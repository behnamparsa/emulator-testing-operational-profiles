from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Tuple, Optional
import pandas as pd

from .io_utils import read_csv_rows, write_csv_rows, snapshot_tag

try:
    from scipy.stats import kruskal
except Exception:  # pragma: no cover
    kruskal = None


def _question_value(row: Dict[str, str]) -> str:
    return (row.get("question", "") or row.get("question_text", "")).strip()


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _base_subset(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["base_flag", "is_base", "base", "Base"]:
        if c in df.columns:
            tmp = df.copy()
            vals = tmp[c].astype(str).str.lower()
            return tmp[vals.isin(["1", "true", "yes", "y"])]
    tmp = df.copy()
    if "run_attempt" in tmp.columns:
        tmp = tmp[pd.to_numeric(tmp["run_attempt"], errors="coerce").fillna(0).eq(1)]
    if "run_conclusion" in tmp.columns:
        tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
    return tmp


def _first_attempt_subset(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    if "run_attempt" in tmp.columns:
        tmp = tmp[pd.to_numeric(tmp["run_attempt"], errors="coerce").fillna(0).eq(1)]
    return tmp


def _ensure_shares(df: pd.DataFrame) -> pd.DataFrame:
    tmp = df.copy()
    if "pre_invocation_share" not in tmp.columns or "execution_window_share" not in tmp.columns or "post_invocation_share" not in tmp.columns:
        pre = _find_col(tmp, ["pre_invocation_share", "pre_invocation_seconds", "pre_invocation"])
        exe = _find_col(tmp, ["execution_window_share", "invocation_execution_window_seconds", "execution_window_seconds", "invocation_execution_window"])
        post = _find_col(tmp, ["post_invocation_share", "post_invocation_seconds", "post_invocation"])
        if pre and exe and post and not {"pre_invocation_share", "execution_window_share", "post_invocation_share"}.issubset(tmp.columns):
            denom = (
                pd.to_numeric(tmp[pre], errors="coerce").fillna(0)
                + pd.to_numeric(tmp[exe], errors="coerce").fillna(0)
                + pd.to_numeric(tmp[post], errors="coerce").fillna(0)
            )
            denom = denom.replace(0, pd.NA)
            tmp["pre_invocation_share"] = pd.to_numeric(tmp[pre], errors="coerce") / denom
            tmp["execution_window_share"] = pd.to_numeric(tmp[exe], errors="coerce") / denom
            tmp["post_invocation_share"] = pd.to_numeric(tmp[post], errors="coerce") / denom
    return tmp


def _rank_styles_metric(df: pd.DataFrame, value_col: str, lower_is_better: bool = True) -> Tuple[str, str, str]:
    tmp = df.copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=[value_col, "style"])
    if tmp.empty:
        return "Conditional", "", f"No usable values for {value_col}."
    med = tmp.groupby("style")[value_col].median().sort_values(ascending=not lower_is_better)
    styles = list(med.index)
    if not styles:
        return "Conditional", "", f"No style medians for {value_col}."
    first = str(styles[0])
    second = str(styles[1]) if len(styles) > 1 else ""
    note = f"Ranked by median {value_col} ({'lower' if lower_is_better else 'higher'} is better)."
    if kruskal is not None and len(styles) >= 2:
        samples = [tmp.loc[tmp["style"] == s, value_col].dropna().tolist() for s in styles if len(tmp.loc[tmp["style"] == s, value_col].dropna()) > 0]
        if len(samples) >= 2:
            try:
                _, p = kruskal(*samples)
                note += f" Kruskal p={p:.3g}."
            except Exception:
                pass
    return first, second, note


def _rank_styles_predictability(df: pd.DataFrame, value_col: str) -> Tuple[str, str, str]:
    tmp = df.copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=[value_col, "style"])
    if tmp.empty:
        return "Conditional", "", f"No usable values for {value_col}."
    scores = {}
    for style, g in tmp.groupby("style"):
        med = g[value_col].median()
        mad = (g[value_col] - med).abs().median()
        norm = float(mad / med) if med not in [0, 0.0] else float(mad)
        scores[str(style)] = norm
    if not scores:
        return "Conditional", "", f"No predictability scores for {value_col}."
    ordered = sorted(scores.items(), key=lambda kv: kv[1])
    first = ordered[0][0]
    second = ordered[1][0] if len(ordered) > 1 else ""
    return first, second, f"Ranked by normalized MAD on {value_col} (lower is more predictable)."


def _rank_styles_distributed_overhead(df: pd.DataFrame) -> Tuple[str, str, str]:
    tmp = _ensure_shares(df)
    need = {"pre_invocation_share", "execution_window_share", "post_invocation_share", "style"}
    if not need.issubset(tmp.columns):
        return "Conditional", "", "Missing share columns for distributed-overhead ranking."
    rows = []
    for style, g in tmp.groupby("style"):
        means = g[["pre_invocation_share", "execution_window_share", "post_invocation_share"]].apply(pd.to_numeric, errors="coerce").mean()
        vals = [float(x) for x in means.fillna(0)]
        rows.append((str(style), max(vals)))
    rows.sort(key=lambda x: x[1])
    first = rows[0][0] if rows else "Conditional"
    second = rows[1][0] if len(rows) > 1 else ""
    return first, second, "Ranked by lowest maximum phase share as a proxy for distributed overhead."


def _rank_styles_heavy_entry_execution(df: pd.DataFrame) -> Tuple[str, str, str]:
    tmp = _ensure_shares(df)
    need = {"pre_invocation_share", "execution_window_share", "post_invocation_share", "style"}
    if not need.issubset(tmp.columns):
        return "Conditional", "", "Missing share columns for heavy-entry/execution ranking."
    rows = []
    for style, g in tmp.groupby("style"):
        pre = pd.to_numeric(g["pre_invocation_share"], errors="coerce").mean()
        exe = pd.to_numeric(g["execution_window_share"], errors="coerce").mean()
        post = pd.to_numeric(g["post_invocation_share"], errors="coerce").mean()
        rows.append((str(style), float(pre + exe - post)))
    rows.sort(key=lambda x: x[1], reverse=True)
    first = rows[0][0] if rows else "Conditional"
    second = rows[1][0] if len(rows) > 1 else ""
    return first, second, "Ranked by high pre-invocation + execution share and low post-invocation share."


def _rank_trigger_conditioned(df: pd.DataFrame) -> Tuple[str, str, str]:
    if not {"style", "event", "run_conclusion"}.issubset(df.columns):
        return "Conditional", "", "Missing event/run_conclusion columns for trigger-conditioned ranking."
    tmp = df.copy()
    tmp["usable"] = tmp["run_conclusion"].isin(["success", "failure"])
    tmp = tmp[tmp["usable"]]
    tmp["success_flag"] = tmp["run_conclusion"].eq("success").astype(int)
    rows = []
    for style, g in tmp.groupby("style"):
        by_event = g.groupby("event")["success_flag"].mean()
        if len(by_event) >= 2:
            rows.append((str(style), float(by_event.max() - by_event.min())))
    rows.sort(key=lambda x: x[1], reverse=True)
    first = rows[0][0] if rows else "Conditional"
    second = rows[1][0] if len(rows) > 1 else ""
    return first, second, "Ranked by largest trigger-conditioned success-rate spread."


def _refresh_answer(obs_id: str, question: str, df: pd.DataFrame) -> tuple[str, str, str]:
    q = question.lower().strip()
    df_base = _base_subset(df)
    df_rq4 = _first_attempt_subset(df)

    run_duration = _find_col(df_base, ["run_duration_seconds", "run_duration"])
    fast_entry = _find_col(df_base, ["time_to_instrumentation_envelope_seconds", "pre_invocation_seconds", "pre_invocation"])
    invocation_window = _find_col(df_base, ["invocation_execution_window_seconds", "execution_window_seconds", "invocation_execution_window"])
    post_invocation = _find_col(df_base, ["post_invocation_seconds", "post_invocation"])

    if df.empty:
        return "Insufficient evidence", "", "MainDataset is empty."

    if obs_id == "Obs. 1.1" or "fastest overall operational profile" in q:
        if run_duration:
            return _rank_styles_metric(df_base, run_duration, lower_is_better=True)
    if obs_id == "Obs. 1.2" or "fast-entry" in q or "fast entry" in q:
        if fast_entry:
            return _rank_styles_metric(df_base, fast_entry, lower_is_better=True)
    if obs_id == "Obs. 1.3" or "slowest sustained-execution" in q:
        if invocation_window:
            return _rank_styles_metric(df_base, invocation_window, lower_is_better=False)
        if run_duration:
            return _rank_styles_metric(df_base, run_duration, lower_is_better=False)
    if obs_id == "Obs. 2.1" or "most predictable" in q:
        if run_duration:
            return _rank_styles_predictability(df_base, run_duration)
    if obs_id == "Obs. 3.1" or "execution-centric" in q:
        tmp = _ensure_shares(df_base)
        if "execution_window_share" in tmp.columns:
            return _rank_styles_metric(tmp, "execution_window_share", lower_is_better=False)
    if obs_id == "Obs. 3.2" or "heavy entry plus heavy execution" in q:
        return _rank_styles_heavy_entry_execution(df_base)
    if obs_id == "Obs. 3.3" or "distributed overhead" in q:
        return _rank_styles_distributed_overhead(df_base)
    if obs_id == "Obs. 3.4" or "tail-heavy mixed" in q:
        tmp = _ensure_shares(df_base)
        if "post_invocation_share" in tmp.columns:
            return _rank_styles_metric(tmp, "post_invocation_share", lower_is_better=False)
        if post_invocation:
            return _rank_styles_metric(df_base, post_invocation, lower_is_better=False)
    if obs_id == "Obs. 4.1" or "usable verdict rate" in q:
        if {"style", "run_conclusion"}.issubset(df_rq4.columns):
            tmp = df_rq4.copy()
            tmp["usable"] = tmp["run_conclusion"].isin(["success", "failure"])
            rates = tmp.groupby("style")["usable"].mean().sort_values(ascending=False)
            if len(rates) > 0:
                first = str(rates.index[0])
                second = str(rates.index[1]) if len(rates) > 1 else ""
                return first, second, "Ranked by usable verdict rate on first-attempt observations."
    if obs_id == "Obs. 4.2" or "success rate among usable verdicts" in q:
        if {"style", "run_conclusion"}.issubset(df_rq4.columns):
            tmp = df_rq4.copy()
            tmp = tmp[tmp["run_conclusion"].isin(["success", "failure"])]
            tmp["success_flag"] = tmp["run_conclusion"].eq("success").astype(int)
            rates = tmp.groupby("style")["success_flag"].mean().sort_values(ascending=False)
            if len(rates) > 0:
                first = str(rates.index[0])
                second = str(rates.index[1]) if len(rates) > 1 else ""
                return first, second, "Ranked by success rate among usable verdicts."
    if obs_id == "Obs. 4.3" or "trigger contexts" in q:
        if {"style", "event"}.issubset(df_rq4.columns):
            return "Yes", "", "Trigger/context differentiation retained as a style-level categorical pattern."
    if obs_id == "Obs. 4.4" or "trigger-conditioned" in q:
        return _rank_trigger_conditioned(df_rq4)
    return "Conditional", "", "Layer 2 has no implemented ranking rule for this observation yet."


def run_layer2(validated_catalog_csv: Path, main_dataset_csv: Path, out_csv: Path, snapshot: str | None = None) -> Path:
    tag = snapshot_tag(snapshot)
    answer_col = f"L2_answer_{tag}"
    runner_up_col = f"L2_runner_up_{tag}"
    note_col = f"L2_note_{tag}"

    rows: List[Dict[str, str]] = read_csv_rows(validated_catalog_csv)
    df = pd.read_csv(main_dataset_csv)

    for row in rows:
        question = _question_value(row)
        answer, runner_up, note = _refresh_answer(row.get("obs_id", ""), question, df)
        row[answer_col] = answer
        row[runner_up_col] = runner_up
        row[note_col] = note

    write_csv_rows(out_csv, rows)
    return out_csv
