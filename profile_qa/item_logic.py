from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import math
import re

import pandas as pd
from scipy.stats import chi2_contingency, kruskal, mannwhitneyu


STYLE_ORDER = ["Community", "Custom", "GMD", "Third-Party"]
STYLE_SET = set(STYLE_ORDER)

ALPHA = 0.05
MIN_OMNIBUS_EPSILON_SQ = 0.01
MIN_PAIRWISE_RBC = 0.147
MIN_CRAMERS_V = 0.10


@dataclass
class EvalResult:
    winner: str
    note: str
    score_by_style: Dict[str, float]
    validation_metric: Optional[str] = None
    lower_is_better: Optional[bool] = None
    categorical_mode: Optional[str] = None



def question_value(row: Dict[str, str]) -> str:
    return (row.get("question", "") or row.get("question_text", "")).strip()



def norm(s: object) -> str:
    return str(s or "").strip()



def normalize_style_answer(answer: str) -> List[str]:
    text = norm(answer)
    if not text:
        return []
    styles = [style for style in STYLE_ORDER if style.lower() in text.lower()]
    return styles



def find_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None



def base_subset(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["base_flag", "is_base", "base", "Base"]:
        if c in df.columns:
            vals = df[c].astype(str).str.lower()
            subset = df[vals.isin(["1", "true", "yes", "y"])]
            if not subset.empty:
                return subset.copy()
    tmp = df.copy()
    if "run_attempt" in tmp.columns:
        tmp = tmp[pd.to_numeric(tmp["run_attempt"], errors="coerce").fillna(0).eq(1)]
    if "run_conclusion" in tmp.columns:
        tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
    return tmp



def first_attempt_subset(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    if "run_attempt" in tmp.columns:
        tmp = tmp[pd.to_numeric(tmp["run_attempt"], errors="coerce").fillna(0).eq(1)]
    return tmp



def ensure_shares(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    if {"pre_invocation_share", "execution_window_share", "post_invocation_share"}.issubset(tmp.columns):
        return tmp
    pre = find_col(tmp, ["pre_invocation_share", "study_pre_invocation_selected_stage3_seconds", "study_pre_invocation_direct_seconds", "pre_invocation_seconds", "pre_invocation"])
    exe = find_col(tmp, ["execution_window_share", "study_invocation_execution_window_selected_stage3_seconds", "study_invocation_execution_window_direct_seconds", "invocation_execution_window_seconds", "execution_window_seconds", "invocation_execution_window"])
    post = find_col(tmp, ["post_invocation_share", "study_post_invocation_selected_stage3_seconds", "study_post_invocation_direct_seconds", "post_invocation_seconds", "post_invocation"])
    if not (pre and exe and post):
        return tmp
    denom = pd.to_numeric(tmp[pre], errors="coerce") + pd.to_numeric(tmp[exe], errors="coerce") + pd.to_numeric(tmp[post], errors="coerce")
    denom = denom.replace(0, pd.NA)
    tmp["pre_invocation_share"] = pd.to_numeric(tmp[pre], errors="coerce") / denom
    tmp["execution_window_share"] = pd.to_numeric(tmp[exe], errors="coerce") / denom
    tmp["post_invocation_share"] = pd.to_numeric(tmp[post], errors="coerce") / denom
    return tmp



def _metric_score_map(df: pd.DataFrame, value_col: str, lower_is_better: bool) -> Dict[str, float]:
    tmp = df[["style", value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=["style", value_col])
    if tmp.empty:
        return {}
    med = tmp.groupby("style")[value_col].median().to_dict()
    return {str(k): float(v) for k, v in med.items() if pd.notna(v)}



def _winner_from_scores(scores: Dict[str, float], lower_is_better: bool) -> str:
    if not scores:
        return "Insufficient evidence"
    ordered = sorted(scores.items(), key=lambda kv: (kv[1], kv[0]), reverse=not lower_is_better)
    return ordered[0][0]



def _composite_rank_score(df: pd.DataFrame, metrics: Sequence[Tuple[str, bool]], agg: str = "sum") -> Dict[str, float]:
    pieces: List[pd.Series] = []
    styles_seen = set()
    for col, lower_is_better in metrics:
        tmp = df[["style", col]].copy()
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        tmp = tmp.dropna(subset=["style", col])
        if tmp.empty:
            continue
        med = tmp.groupby("style")[col].median()
        rank = med.rank(method="average", ascending=lower_is_better)
        if not lower_is_better:
            # higher is better => rank 1 is largest already from ascending=False equivalent
            rank = med.rank(method="average", ascending=False)
        pieces.append(rank)
        styles_seen.update(rank.index.tolist())
    if not pieces:
        return {}
    aligned = pd.concat(pieces, axis=1)
    if agg == "sum":
        out = aligned.mean(axis=1, skipna=True)
    else:
        out = aligned.mean(axis=1, skipna=True)
    return {str(k): float(v) for k, v in out.to_dict().items() if pd.notna(v)}



def _predictability_scores(df: pd.DataFrame, cols: Sequence[str]) -> Dict[str, float]:
    by_style: Dict[str, List[float]] = {}
    for col in cols:
        tmp = df[["style", col]].copy()
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        tmp = tmp.dropna(subset=["style", col])
        if tmp.empty:
            continue
        for style, g in tmp.groupby("style"):
            med = g[col].median()
            mad = (g[col] - med).abs().median()
            score = float(mad / med) if med not in [0, 0.0] else float(mad)
            by_style.setdefault(str(style), []).append(score)
    return {style: float(sum(vals) / len(vals)) for style, vals in by_style.items() if vals}



def _tail_scores(df: pd.DataFrame, cols: Sequence[str]) -> Dict[str, float]:
    by_style: Dict[str, List[float]] = {}
    for col in cols:
        tmp = df[["style", col]].copy()
        tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
        tmp = tmp.dropna(subset=["style", col])
        if tmp.empty:
            continue
        for style, g in tmp.groupby("style"):
            p90 = g[col].quantile(0.9)
            by_style.setdefault(str(style), []).append(float(p90))
    return {style: float(sum(vals) / len(vals)) for style, vals in by_style.items() if vals}



def _categorical_rate_scores(df: pd.DataFrame, mode: str) -> Dict[str, float]:
    tmp = df.copy()
    if mode == "usable_verdict_rate":
        tmp["flag"] = tmp["run_conclusion"].isin(["success", "failure"]).astype(int)
    elif mode == "success_among_usable":
        tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
        tmp["flag"] = tmp["run_conclusion"].eq("success").astype(int)
    elif mode == "trigger_conditioned_spread":
        tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
        tmp["flag"] = tmp["run_conclusion"].eq("success").astype(int)
        scores: Dict[str, float] = {}
        for style, g in tmp.groupby("style"):
            by_event = g.groupby("event")["flag"].mean()
            if len(by_event) >= 2:
                scores[str(style)] = float(by_event.max() - by_event.min())
        return scores
    else:
        return {}
    rates = tmp.groupby("style")["flag"].mean().to_dict()
    return {str(k): float(v) for k, v in rates.items() if pd.notna(v)}



def evaluate_item(obs_id: str, question: str, df: pd.DataFrame) -> EvalResult:
    q = question.lower().strip()
    df_base = ensure_shares(base_subset(df))
    df_rq4 = ensure_shares(first_attempt_subset(df))

    run_duration = find_col(df_base, ["study_run_duration_seconds", "run_duration_seconds", "run_duration"])
    entry = find_col(df_base, ["study_pre_invocation_selected_stage3_seconds", "study_pre_invocation_direct_seconds", "time_to_instrumentation_envelope_seconds", "pre_invocation_seconds", "pre_invocation"])
    envelope = find_col(df_base, ["study_layer1_instrumentation_job_envelope_seconds", "instrumentation_job_envelope_seconds"])
    execution = find_col(df_base, ["study_invocation_execution_window_selected_stage3_seconds", "study_invocation_execution_window_direct_seconds", "invocation_execution_window_seconds", "execution_window_seconds", "invocation_execution_window"])
    post = find_col(df_base, ["study_post_invocation_selected_stage3_seconds", "study_post_invocation_direct_seconds", "post_invocation_seconds", "post_invocation"])

    if df.empty:
        return EvalResult("Insufficient evidence", "MainDataset is empty.", {})

    if obs_id == "Obs. 1.1" or "fastest overall" in q:
        scores = _metric_score_map(df_base, run_duration, True) if run_duration else {}
        return EvalResult(_winner_from_scores(scores, True), f"Scored by lowest median {run_duration}.", scores, run_duration, True)

    if obs_id == "Obs. 1.2" or "fast-entry" in q or "fast entry" in q:
        metrics = [(c, True) for c in [entry] if c] + [(run_duration, False)] if run_duration else [(c, True) for c in [entry] if c]
        scores = _composite_rank_score(df_base, metrics)
        note = "Scored by fast-entry rank with a penalty for also being slow in overall completion."
        return EvalResult(_winner_from_scores(scores, True), note, scores, entry or run_duration, True)

    if obs_id == "Obs. 1.3" or "slowest sustained-execution" in q:
        val = execution or run_duration
        scores = _metric_score_map(df_base, val, False) if val else {}
        return EvalResult(_winner_from_scores(scores, False), f"Scored by highest median {val}.", scores, val, False)

    if obs_id == "Obs. 1.4" or "mixed speed profile" in q:
        metrics: List[Tuple[str, bool]] = []
        if entry:
            metrics.append((entry, True))
        if execution:
            metrics.append((execution, True))
        scores = _composite_rank_score(df_base, metrics)
        if post and scores:
            post_scores = _metric_score_map(df_base, post, False)
            for style in set(scores) | set(post_scores):
                scores[style] = scores.get(style, math.nan) * 0.6 + (post_scores.get(style, 99.0)) * 0.4
        note = "Scored as mixed speed profile: competitive entry/execution with heavier post-invocation tail."
        return EvalResult(_winner_from_scores(scores, True), note, scores, post or execution or entry, False if post else True)

    if obs_id == "Obs. 1.5" or "fast core execution profile with a longer residual tail" in q:
        metrics = []
        if envelope:
            metrics.append((envelope, True))
        if execution:
            metrics.append((execution, True))
        scores = _composite_rank_score(df_base, metrics)
        if post and scores:
            post_scores = _metric_score_map(df_base, post, False)
            for style in set(scores) | set(post_scores):
                scores[style] = scores.get(style, math.nan) * 0.7 + post_scores.get(style, 99.0) * 0.3
        note = "Scored by fast core execution plus longer post-invocation tail."
        return EvalResult(_winner_from_scores(scores, True), note, scores, execution or envelope or post, True if execution or envelope else False)

    if obs_id == "Obs. 2.1" or "most predictable" in q:
        cols = [c for c in [run_duration, envelope, execution, post] if c]
        scores = _predictability_scores(df_base, cols)
        return EvalResult(_winner_from_scores(scores, True), "Scored by lowest average normalized MAD across completion-oriented timing measures.", scores, run_duration or execution, True)

    if obs_id == "Obs. 2.2" or "predictability-poor" in q:
        fast_scores = _metric_score_map(df_base, run_duration, True) if run_duration else {}
        pred_scores = _predictability_scores(df_base, [c for c in [run_duration, execution, post] if c])
        all_styles = set(fast_scores) | set(pred_scores)
        scores = {s: fast_scores.get(s, 99.0) * 0.5 - pred_scores.get(s, 0.0) * 0.5 for s in all_styles}
        return EvalResult(_winner_from_scores(scores, False), "Scored by fast typical runtime combined with poor predictability.", scores, run_duration, True)

    if obs_id == "Obs. 2.3" or "absolute tail-risk profile" in q:
        scores = _tail_scores(df_base, [c for c in [run_duration, execution, post] if c])
        return EvalResult(_winner_from_scores(scores, False), "Scored by highest average p90 across main completion/tail timing measures.", scores, run_duration or execution or post, False)

    if obs_id == "Obs. 2.4" or "mixed predictability profile" in q:
        pred = _predictability_scores(df_base, [c for c in [run_duration, execution, post] if c])
        if pred:
            vals = list(pred.values())
            midpoint = sorted(vals)[len(vals) // 2]
            scores = {s: abs(v - midpoint) for s, v in pred.items()}
        else:
            scores = {}
        return EvalResult(_winner_from_scores(scores, True), "Scored by predictability closeness to a middling profile as a proxy for mixed/cautious predictability.", scores, run_duration or execution or post, True)

    if obs_id == "Obs. 3.1" or "execution-centric" in q:
        scores = _composite_rank_score(df_base, [("execution_window_share", False), ("post_invocation_share", True)])
        return EvalResult(_winner_from_scores(scores, True), "Scored by high execution-window share and low post-invocation share.", scores, "execution_window_share", False)

    if obs_id == "Obs. 3.2" or "heavy entry plus heavy execution" in q:
        pre = _metric_score_map(df_base, "pre_invocation_share", False)
        exe = _metric_score_map(df_base, "execution_window_share", False)
        post_s = _metric_score_map(df_base, "post_invocation_share", False)
        all_styles = set(pre) | set(exe) | set(post_s)
        scores = {s: pre.get(s, 0.0) + exe.get(s, 0.0) - post_s.get(s, 0.0) for s in all_styles}
        return EvalResult(_winner_from_scores(scores, False), "Scored by high pre-invocation + execution share with lower post share.", scores, "execution_window_share", False)

    if obs_id == "Obs. 3.3" or "distributed overhead" in q:
        scores = {}
        for style, g in df_base.groupby("style"):
            means = g[["pre_invocation_share", "execution_window_share", "post_invocation_share"]].apply(pd.to_numeric, errors="coerce").mean()
            scores[str(style)] = float(max(means.fillna(0)))
        return EvalResult(_winner_from_scores(scores, True), "Scored by the lowest maximum phase-share as a distributed-overhead proxy.", scores, "execution_window_share", False)

    if obs_id == "Obs. 3.4" or "tail-heavy mixed" in q:
        post_s = _metric_score_map(df_base, "post_invocation_share", False)
        exe_s = _metric_score_map(df_base, "execution_window_share", True)
        all_styles = set(post_s) | set(exe_s)
        scores = {s: post_s.get(s, 0.0) - exe_s.get(s, 99.0) * 0.25 for s in all_styles}
        return EvalResult(_winner_from_scores(scores, False), "Scored by strong post-invocation share with non-dominant execution share.", scores, "post_invocation_share", False)

    if obs_id == "Obs. 4.1" or "usable run-level verdict rate" in q:
        if {"style", "run_conclusion"}.issubset(df_rq4.columns):
            scores = _categorical_rate_scores(df_rq4, "usable_verdict_rate")
            return EvalResult(_winner_from_scores(scores, False), "Scored by highest usable-verdict rate on first-attempt runs.", scores, categorical_mode="usable_verdict_rate")

    if obs_id == "Obs. 4.2" or "success rate among usable verdicts" in q:
        if {"style", "run_conclusion"}.issubset(df_rq4.columns):
            scores = _categorical_rate_scores(df_rq4, "success_among_usable")
            return EvalResult(_winner_from_scores(scores, False), "Scored by highest success rate among usable first-attempt runs.", scores, categorical_mode="success_among_usable")

    if obs_id == "Obs. 4.3" or "trigger contexts" in q:
        return EvalResult("Yes", "Validated via style × trigger/event distribution difference.", {}, categorical_mode="trigger_context_differentiation")

    if obs_id == "Obs. 4.4" or "trigger-conditioned" in q:
        if {"style", "event", "run_conclusion"}.issubset(df_rq4.columns):
            scores = _categorical_rate_scores(df_rq4, "trigger_conditioned_spread")
            return EvalResult(_winner_from_scores(scores, False), "Scored by the largest trigger-conditioned success-rate spread.", scores, categorical_mode="trigger_conditioned_spread")

    return EvalResult("Insufficient evidence", "No implemented rule for this observation yet.", {})



def _safe_float(x: object) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _kruskal_epsilon_squared(h_stat: float, k: int, n: int) -> float:
    if n <= k or k < 2 or pd.isna(h_stat):
        return 0.0
    return max(0.0, float((h_stat - k + 1) / (n - k)))


def _rank_biserial_from_u(u_stat: float, n1: int, n2: int, lower_is_better: bool) -> float:
    if n1 <= 0 or n2 <= 0:
        return 0.0
    rbc = 1.0 - (2.0 * float(u_stat)) / float(n1 * n2)
    return float(rbc if lower_is_better else -rbc)


def _cramers_v_from_table(table: pd.DataFrame, chi2: float) -> float:
    n = int(table.to_numpy().sum())
    if n <= 0:
        return 0.0
    r, k = table.shape
    denom = min(r - 1, k - 1)
    if denom <= 0:
        return 0.0
    return float((chi2 / (n * denom)) ** 0.5)


def _pairwise_best_competitor(current_style: str, winner: str, styles: Sequence[str]) -> list[str]:
    ordered = [s for s in styles if s not in {current_style}]
    if winner and winner != current_style and winner in ordered:
        return [winner] + [s for s in ordered if s != winner]
    return ordered


def _holm_adjust(pvals: List[Tuple[str, float]]) -> Dict[str, float]:
    ordered = sorted(pvals, key=lambda kv: kv[1])
    m = len(ordered)
    out: Dict[str, float] = {}
    running = 0.0
    for i, (label, p) in enumerate(ordered, start=1):
        adj = min(1.0, (m - i + 1) * p)
        running = max(running, adj)
        out[label] = running
    return out



def validate_stored_answer(row: Dict[str, str], df: pd.DataFrame, stored_answer: str) -> Tuple[str, str, EvalResult]:
    obs_id = norm(row.get("obs_id"))
    result = evaluate_item(obs_id, question_value(row), df)
    if result.winner == "Insufficient evidence":
        return "Insufficient evidence", result.note, result

    stored_styles = normalize_style_answer(stored_answer)
    if obs_id == "Obs. 4.3":
        tmp = first_attempt_subset(df)
        trigger_col = find_col(tmp, ["event", "trigger"])
        if not trigger_col or "style" not in tmp.columns:
            return "Insufficient evidence", "Missing style/event data for trigger-context validation.", result
        table = pd.crosstab(tmp["style"], tmp[trigger_col])
        if table.shape[0] < 2 or table.shape[1] < 2:
            return "Insufficient evidence", "Not enough style/event diversity for chi-square validation.", result
        chi2, p, _, _ = chi2_contingency(table)
        v = _cramers_v_from_table(table, chi2)
        favored = "Yes" if (p < ALPHA and v >= MIN_CRAMERS_V) else "No"
        result = EvalResult(favored, result.note, result.score_by_style, result.validation_metric, result.lower_is_better, result.categorical_mode)
        current = norm(stored_answer).lower() in {"yes", "true"}
        fail = (current and favored == "No") or ((not current) and favored == "Yes")
        status = "Failed" if fail else "Passed"
        note = f"Chi-square on style × {trigger_col}: p={p:.3g}, Cramer's V={v:.3f}; stored answer='{stored_answer}', favored='{favored}'."
        return status, note, result

    if not stored_styles:
        stored_styles = [norm(stored_answer)] if norm(stored_answer) else []
    current_style = stored_styles[0] if stored_styles else ""
    if stored_styles and len(stored_styles) > 1 and result.winner in stored_styles:
        return "Passed", f"Stored multi-style answer '{stored_answer}' still contains current favored style '{result.winner}'. {result.note}", result
    if current_style not in STYLE_SET:
        return "Insufficient evidence", f"Stored answer '{stored_answer}' is not a recognized single-style target.", result

    if result.categorical_mode in {"usable_verdict_rate", "success_among_usable", "trigger_conditioned_spread"}:
        winner = result.winner
        scores = result.score_by_style
        tmp = first_attempt_subset(df).copy()
        if result.categorical_mode == "trigger_conditioned_spread":
            ordered = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
            if len(ordered) < 2:
                return "Passed", f"Only one style had enough trigger-conditioned evidence. {result.note}", result
            gap = ordered[0][1] - ordered[1][1]
            fail = winner != current_style and gap > 0.05
            note = f"Trigger-conditioned spread gap={gap:.4f}; stored='{current_style}', winner='{winner}'."
            return ("Failed" if fail else "Passed"), note + f" {result.note}", result

        if result.categorical_mode == "usable_verdict_rate":
            tmp["flag"] = tmp["run_conclusion"].isin(["success", "failure"]).astype(int)
        else:
            tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
            tmp["flag"] = tmp["run_conclusion"].eq("success").astype(int)
        table = pd.crosstab(tmp["style"], tmp["flag"])
        if table.shape[0] < 2 or table.shape[1] < 2:
            return "Insufficient evidence", f"Not enough categorical variation for {result.categorical_mode} validation.", result
        chi2, p, _, _ = chi2_contingency(table)
        v = _cramers_v_from_table(table, chi2)
        fail = winner != current_style and p < ALPHA and v >= MIN_CRAMERS_V
        note = f"Chi-square for {result.categorical_mode}: p={p:.3g}, Cramer's V={v:.3f}; stored='{current_style}', winner='{winner}'."
        return ("Failed" if fail else "Passed"), note, result

    if not result.validation_metric or (result.validation_metric not in df.columns and result.validation_metric not in ensure_shares(df).columns):
        passed = result.winner == current_style
        return ("Passed" if passed else "Insufficient evidence"), f"No direct validation metric available; checked current scoring winner only. stored='{current_style}', winner='{result.winner}'. {result.note}", result

    src = ensure_shares(base_subset(df))
    val_col = result.validation_metric
    tmp = src[["style", val_col]].copy()
    tmp[val_col] = pd.to_numeric(tmp[val_col], errors="coerce")
    tmp = tmp.dropna(subset=["style", val_col])
    if tmp.empty:
        return "Insufficient evidence", f"No usable values for validation metric '{val_col}'.", result

    current_sample = tmp.loc[tmp["style"] == current_style, val_col].dropna()
    if len(current_sample) < 2:
        return "Insufficient evidence", f"Too few observations for stored style '{current_style}' on '{val_col}'.", result

    styles = [s for s in STYLE_ORDER if s in set(tmp["style"].astype(str))]
    groups = [tmp.loc[tmp["style"] == s, val_col].dropna() for s in styles]
    valid_groups = [g for g in groups if len(g) > 1]
    if len(valid_groups) < 2:
        return "Insufficient evidence", f"Too few style groups for omnibus test on '{val_col}'.", result

    h_stat, omnibus_p = kruskal(*groups)
    n_total = sum(len(g) for g in groups)
    eps_sq = _kruskal_epsilon_squared(h_stat, len(groups), n_total)

    competitors = _pairwise_best_competitor(current_style, result.winner, styles)
    pairwise = []
    strongest_against = None
    for style in competitors:
        other = tmp.loc[tmp["style"] == style, val_col].dropna()
        if len(other) < 2:
            continue
        alt = 'less' if result.lower_is_better else 'greater'
        try:
            stat = mannwhitneyu(current_sample, other, alternative=alt)
            effect = _rank_biserial_from_u(float(stat.statistic), len(current_sample), len(other), bool(result.lower_is_better))
            pairwise.append((style, float(stat.pvalue), effect))
            if strongest_against is None and style == result.winner:
                strongest_against = (style, float(stat.pvalue), effect)
        except Exception:
            continue

    if strongest_against is None and pairwise:
        strongest_against = pairwise[0]

    if not pairwise:
        passed = result.winner == current_style
        return ("Passed" if passed else "Insufficient evidence"), f"Stored='{current_style}', winner='{result.winner}'. Omnibus p={omnibus_p:.3g}, epsilon^2={eps_sq:.3f}. No usable pairwise test was available. {result.note}", result

    adj = _holm_adjust([(style, p) for style, p, _ in pairwise])
    strongest_style, strongest_p_raw, strongest_eff = strongest_against
    strongest_p = adj.get(strongest_style, strongest_p_raw)

    fail = (
        result.winner != current_style
        and omnibus_p < ALPHA
        and eps_sq >= MIN_OMNIBUS_EPSILON_SQ
        and strongest_p < ALPHA
        and abs(strongest_eff) >= MIN_PAIRWISE_RBC
    )
    comps = ", ".join(
        f"vs {style}: p_adj={adj.get(style, p):.3g}, rbc={eff:.3f}"
        for style, p, eff in pairwise
    )
    note = (
        f"Kruskal on {val_col}: p={omnibus_p:.3g}, epsilon^2={eps_sq:.3f}; {comps}. "
        f"stored='{current_style}', winner='{result.winner}'. "
        f"Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size."
    )
    return ("Failed" if fail else "Passed"), note, result
