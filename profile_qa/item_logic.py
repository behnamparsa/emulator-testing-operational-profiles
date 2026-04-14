from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
from scipy.stats import chi2_contingency, kruskal, mannwhitneyu


STYLE_ORDER = ["Community", "GMD", "Third-Party", "Custom"]
STYLE_SET = set(STYLE_ORDER)

ALPHA = 0.05
MIN_OMNIBUS_EPSILON_SQ = 0.01
MIN_PAIRWISE_RBC = 0.147
MIN_CRAMERS_V = 0.10


@dataclass(frozen=True)
class ObservationSpec:
    obs_id: str
    paper_intent: str
    primary_measurement: str
    fallback_measurement: str
    winner_rule: str
    validation_rule: str
    technical_note: str


OBSERVATION_SPECS: Dict[str, ObservationSpec] = {
    "Obs. 1.1": ObservationSpec(
        "Obs. 1.1",
        "Fastest overall operational profile.",
        "Run duration",
        "None",
        "Pick the style with the lowest median run duration.",
        "Kruskal–Wallis on run duration, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Primary metric = study_run_duration_seconds.",
    ),
    "Obs. 1.2": ObservationSpec(
        "Obs. 1.2",
        "Clearest fast-entry profile without claiming fastest overall completion.",
        "Pre-Invocation",
        "Time to Instrumentation Envelope",
        "Pick the style with the lowest median entry metric.",
        "Kruskal–Wallis on the selected entry metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Prefer Layer 2 pre-invocation when available; otherwise fall back to Layer 1 time-to-envelope.",
    ),
    "Obs. 1.3": ObservationSpec(
        "Obs. 1.3",
        "Slowest sustained-execution profile.",
        "Invocation Execution Window",
        "Instrumentation Job Envelope",
        "Pick the style with the highest median sustained-execution metric.",
        "Kruskal–Wallis on the selected sustained-execution metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Prefer Layer 2 execution window when available; otherwise fall back to Layer 1 instrumentation envelope.",
    ),
    "Obs. 1.4": ObservationSpec(
        "Obs. 1.4",
        "Mixed speed profile with a distinctly long completion tail.",
        "Post-Invocation",
        "Post-Instrumentation Tail",
        "Pick the style with the highest median completion-tail metric.",
        "Kruskal–Wallis on the selected tail metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Flattened to the tail metric because it is the clearest automated signature of the mixed-speed profile.",
    ),
    "Obs. 1.5": ObservationSpec(
        "Obs. 1.5",
        "Fast-core profile that still carries a longer residual tail.",
        "Post-Invocation",
        "Post-Instrumentation Tail",
        "Among fast-core candidates (Community, GMD, Third-Party), pick the style with the highest median tail metric.",
        "Kruskal–Wallis on the selected tail metric within the fast-core candidate set, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Flattened to the tail metric, restricted to the fast-core candidate set so the rule does not collapse into the Custom tail-heavy exception.",
    ),
    "Obs. 2.1": ObservationSpec(
        "Obs. 2.1",
        "Most predictable style on the main completion-oriented measures.",
        "Predictability loss on Run Duration",
        "Predictability loss on Instrumentation Job Envelope",
        "Pick the style with the lowest median predictability-loss metric.",
        "Kruskal–Wallis on style-level normalized absolute deviation, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Predictability loss is computed as absolute deviation from the style median, normalized by the style median when non-zero.",
    ),
    "Obs. 2.2": ObservationSpec(
        "Obs. 2.2",
        "Fast in typical terms but predictability-poor.",
        "Predictability loss on Run Duration",
        "Predictability loss on Instrumentation Job Envelope",
        "Restrict to the two fastest styles by median run duration, then pick the style with the highest median predictability-loss metric.",
        "Kruskal–Wallis on the selected predictability-loss metric within the fast-style candidate set, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "This preserves the paper’s speed-versus-stability trade-off while keeping the automated rule single-metric.",
    ),
    "Obs. 2.3": ObservationSpec(
        "Obs. 2.3",
        "Strongest absolute tail-risk profile.",
        "Run Duration upper tail (P90)",
        "Instrumentation Job Envelope upper tail (P90)",
        "Pick the style with the largest P90 on the selected tail metric.",
        "Kruskal–Wallis on the selected raw timing metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer; favored answer is chosen by upper-tail burden.",
        "The repo flattens the paper’s absolute tail-risk idea to one upper-tail timing metric.",
    ),
    "Obs. 2.4": ObservationSpec(
        "Obs. 2.4",
        "Mixed and cautious predictability profile.",
        "Predictability loss on Pre-Invocation",
        "Predictability loss on Run Duration",
        "Pick the style with the highest invocation-level predictability loss.",
        "Kruskal–Wallis on the selected predictability-loss metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Flattened to invocation-level predictability because that is where the paper’s weaker Custom signal is most visible.",
    ),
    "Obs. 3.1": ObservationSpec(
        "Obs. 3.1",
        "Clearest execution-centric overhead profile.",
        "Execution Window Share",
        "None",
        "Pick the style with the highest median execution-window share.",
        "Kruskal–Wallis on execution-window share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Primary metric = execution_window_share.",
    ),
    "Obs. 3.2": ObservationSpec(
        "Obs. 3.2",
        "Heavy entry plus heavy execution profile.",
        "Pre-Invocation Share",
        "Execution Window Share",
        "Pick the style with the highest median pre-invocation share; use execution-window share as fallback if entry share is unavailable.",
        "Kruskal–Wallis on the selected share metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Flattened to entry burden first because that is the clearest differentiator in the paper.",
    ),
    "Obs. 3.3": ObservationSpec(
        "Obs. 3.3",
        "Distributed overhead profile rather than a single dominant source.",
        "Maximum phase share (lower is better)",
        "None",
        "Pick the style with the lowest maximum of pre-, execution-, and post-invocation shares.",
        "Kruskal–Wallis on per-row max phase share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "This is a direct flattening of the paper’s distributed-overhead idea.",
    ),
    "Obs. 3.4": ObservationSpec(
        "Obs. 3.4",
        "Tail-heavy mixed overhead case.",
        "Post-Invocation Share",
        "None",
        "Pick the style with the highest median post-invocation share.",
        "Kruskal–Wallis on post-invocation share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.",
        "Primary metric = post_invocation_share.",
    ),
    "Obs. 4.1": ObservationSpec(
        "Obs. 4.1",
        "Highest usable-verdict rate.",
        "Usable verdict rate",
        "None",
        "Pick the style with the highest usable-verdict rate on first-attempt runs.",
        "Chi-square on style × usable-verdict status, with Cramér’s V as effect size.",
        "Categorical validation on first-attempt instrumentation-executed runs.",
    ),
    "Obs. 4.2": ObservationSpec(
        "Obs. 4.2",
        "Highest success rate among usable verdicts.",
        "Success rate among usable verdicts",
        "None",
        "Pick the style with the highest success rate among usable first-attempt runs.",
        "Chi-square on style × success/failure within the usable-verdict subset, with Cramér’s V as effect size.",
        "Categorical validation on the usable-verdict subset only.",
    ),
    "Obs. 4.3": ObservationSpec(
        "Obs. 4.3",
        "Meaningful trigger-context differentiation exists across styles.",
        "Trigger-context differentiation",
        "None",
        "Validate a Yes/No claim rather than a style winner.",
        "Chi-square on style × event, with Cramér’s V as effect size.",
        "Keep Yes only when the chi-square result supports meaningful style-by-event separation.",
    ),
    "Obs. 4.4": ObservationSpec(
        "Obs. 4.4",
        "Strongest trigger-conditioned success behavior.",
        "Trigger-conditioned success-rate spread",
        "None",
        "Pick the style with the largest difference between its best and worst event-specific success rates.",
        "Chi-square-style categorical interpretation is complemented by the spread proxy for automated winner selection.",
        "This is a flattened proxy for the paper’s trigger-conditioned verdict pattern.",
    ),
}


@dataclass
class EvalResult:
    winner: str
    note: str
    score_by_style: Dict[str, float]
    validation_metric: Optional[str] = None
    lower_is_better: Optional[bool] = None
    categorical_mode: Optional[str] = None
    metric_used: Optional[str] = None
    candidate_styles: Optional[List[str]] = None


def observation_structure_rows() -> List[Dict[str, str]]:
    rows = []
    for obs_id in [f"Obs. {i}.{j}" for i, end in [(1,5),(2,4),(3,4),(4,4)] for j in range(1,end+1)]:
        spec = OBSERVATION_SPECS[obs_id]
        rows.append({
            "obs_id": spec.obs_id,
            "paper_intent": spec.paper_intent,
            "primary_measurement": spec.primary_measurement,
            "fallback_measurement": spec.fallback_measurement,
            "winner_rule": spec.winner_rule,
            "validation_rule": spec.validation_rule,
            "technical_note": spec.technical_note,
        })
    return rows


def observation_logic_rows() -> List[Dict[str, str]]:
    return observation_structure_rows()


def observation_logic_for_obs(obs_id: str) -> str:
    spec = OBSERVATION_SPECS.get(norm(obs_id))
    if not spec:
        return "No observation-specific logic note recorded."
    return f"Primary measurement: {spec.primary_measurement}; fallback: {spec.fallback_measurement}; winner rule: {spec.winner_rule}; validation rule: {spec.validation_rule}"


def observation_structure_for_obs(obs_id: str) -> Dict[str, str]:
    spec = OBSERVATION_SPECS.get(norm(obs_id))
    if not spec:
        return {}
    return {
        "obs_id": spec.obs_id,
        "paper_intent": spec.paper_intent,
        "primary_measurement": spec.primary_measurement,
        "fallback_measurement": spec.fallback_measurement,
        "winner_rule": spec.winner_rule,
        "validation_rule": spec.validation_rule,
        "technical_note": spec.technical_note,
    }


def question_value(row: Dict[str, str]) -> str:
    return (row.get("question", "") or row.get("question_text", "")).strip()


def norm(s: object) -> str:
    return str(s or "").strip()


def normalize_style_answer(answer: str) -> List[str]:
    text = norm(answer)
    if not text:
        return []
    return [style for style in STYLE_ORDER if style.lower() in text.lower()]


def find_col(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def base_subset(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["Base_timing_regime", "base_flag", "is_base", "base", "Base"]:
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
    for c in ["controller_attempt_eq_1", "FirstAttempt", "first_attempt"]:
        if c in tmp.columns:
            vals = tmp[c].astype(str).str.lower()
            subset = tmp[vals.isin(["1", "true", "yes", "y"])]
            if not subset.empty:
                return subset.copy()
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


def _metric_score_map(df: pd.DataFrame, value_col: str, lower_is_better: bool, candidate_styles: Optional[Sequence[str]] = None) -> Dict[str, float]:
    tmp = df[["style", value_col]].copy()
    tmp[value_col] = pd.to_numeric(tmp[value_col], errors="coerce")
    tmp = tmp.dropna(subset=["style", value_col])
    if candidate_styles:
        tmp = tmp[tmp["style"].isin(candidate_styles)]
    if tmp.empty:
        return {}
    med = tmp.groupby("style")[value_col].median().to_dict()
    return {str(k): float(v) for k, v in med.items() if pd.notna(v)}


def _winner_from_scores(scores: Dict[str, float], lower_is_better: bool) -> str:
    if not scores:
        return "Insufficient evidence"
    return sorted(scores.items(), key=lambda kv: (kv[1], kv[0]), reverse=not lower_is_better)[0][0]


def _predictability_row_metric(df: pd.DataFrame, col: str) -> pd.DataFrame:
    tmp = df[["style", col]].copy()
    tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
    tmp = tmp.dropna(subset=["style", col])
    if tmp.empty:
        return pd.DataFrame(columns=["style", "predictability_value"])
    medians = tmp.groupby("style")[col].median().to_dict()
    def calc(row):
        med = medians.get(row["style"])
        val = row[col]
        if pd.isna(med) or pd.isna(val):
            return pd.NA
        if med in [0, 0.0]:
            return abs(val - med)
        return abs(val - med) / abs(med)
    tmp["predictability_value"] = tmp.apply(calc, axis=1)
    return tmp[["style", "predictability_value"]].dropna()


def _predictability_scores_from_metric(df: pd.DataFrame, col: str, candidate_styles: Optional[Sequence[str]] = None) -> Dict[str, float]:
    tmp = _predictability_row_metric(df, col)
    if candidate_styles:
        tmp = tmp[tmp["style"].isin(candidate_styles)]
    if tmp.empty:
        return {}
    med = tmp.groupby("style")["predictability_value"].median().to_dict()
    return {str(k): float(v) for k, v in med.items() if pd.notna(v)}


def _tail_scores_from_metric(df: pd.DataFrame, col: str, candidate_styles: Optional[Sequence[str]] = None) -> Dict[str, float]:
    tmp = df[["style", col]].copy()
    tmp[col] = pd.to_numeric(tmp[col], errors="coerce")
    tmp = tmp.dropna(subset=["style", col])
    if candidate_styles:
        tmp = tmp[tmp["style"].isin(candidate_styles)]
    if tmp.empty:
        return {}
    return {str(style): float(g[col].quantile(0.9)) for style, g in tmp.groupby("style")}


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
        out: Dict[str, float] = {}
        for style, g in tmp.groupby("style"):
            by_event = g.groupby("event")["flag"].mean()
            if len(by_event) >= 2:
                out[str(style)] = float(by_event.max() - by_event.min())
        return out
    else:
        return {}
    rates = tmp.groupby("style")["flag"].mean().to_dict()
    return {str(k): float(v) for k, v in rates.items() if pd.notna(v)}


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


def _metric_columns(df_base: pd.DataFrame) -> Dict[str, Optional[str]]:
    return {
        "run_duration": find_col(df_base, ["study_run_duration_seconds", "run_duration_seconds", "run_duration"]),
        "entry": find_col(df_base, ["study_pre_invocation_selected_stage3_seconds", "study_pre_invocation_direct_seconds", "pre_invocation_seconds", "pre_invocation"]),
        "time_to_envelope": find_col(df_base, ["study_layer1_time_to_instrumentation_envelope_seconds", "study_time_to_instrumentation_envelope_seconds", "time_to_instrumentation_envelope_seconds"]),
        "envelope": find_col(df_base, ["study_layer1_instrumentation_job_envelope_seconds", "instrumentation_job_envelope_seconds"]),
        "execution": find_col(df_base, ["study_invocation_execution_window_selected_stage3_seconds", "study_invocation_execution_window_direct_seconds", "invocation_execution_window_seconds", "execution_window_seconds", "invocation_execution_window"]),
        "post": find_col(df_base, ["study_post_invocation_selected_stage3_seconds", "study_post_invocation_direct_seconds", "post_invocation_seconds", "post_invocation"]),
        "post_tail": find_col(df_base, ["study_layer1_post_instrumentation_tail_seconds", "post_instrumentation_tail_seconds"]),
    }


def evaluate_item(obs_id: str, question: str, df: pd.DataFrame) -> EvalResult:
    df_base = ensure_shares(base_subset(df))
    df_rq4 = ensure_shares(first_attempt_subset(df))
    cols = _metric_columns(df_base)

    if df.empty:
        return EvalResult("Insufficient evidence", "MainDataset is empty.", {})

    # RQ1
    if obs_id == "Obs. 1.1":
        metric = cols["run_duration"]
        scores = _metric_score_map(df_base, metric, True) if metric else {}
        return EvalResult(_winner_from_scores(scores, True), f"Primary measurement: {metric}; winner = lowest median run duration.", scores, metric, True, metric_used=metric)

    if obs_id == "Obs. 1.2":
        metric = cols["entry"] or cols["time_to_envelope"]
        scores = _metric_score_map(df_base, metric, True) if metric else {}
        return EvalResult(_winner_from_scores(scores, True), f"Primary measurement: {metric}; winner = lowest median fast-entry metric.", scores, metric, True, metric_used=metric)

    if obs_id == "Obs. 1.3":
        metric = cols["execution"] or cols["envelope"]
        scores = _metric_score_map(df_base, metric, False) if metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: {metric}; winner = highest median sustained-execution burden.", scores, metric, False, metric_used=metric)

    if obs_id == "Obs. 1.4":
        metric = cols["post"] or cols["post_tail"]
        scores = _metric_score_map(df_base, metric, False) if metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: {metric}; winner = highest median completion-tail metric.", scores, metric, False, metric_used=metric)

    if obs_id == "Obs. 1.5":
        metric = cols["post"] or cols["post_tail"]
        candidates = ["Community", "GMD", "Third-Party"]
        scores = _metric_score_map(df_base, metric, False, candidate_styles=candidates) if metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: {metric}; winner = highest median tail metric within the fast-core candidate set.", scores, metric, False, metric_used=metric, candidate_styles=list(scores.keys()) or candidates)

    # RQ2
    if obs_id == "Obs. 2.1":
        metric = cols["run_duration"] or cols["envelope"]
        scores = _predictability_scores_from_metric(df_base, metric) if metric else {}
        return EvalResult(_winner_from_scores(scores, True), f"Primary measurement: predictability loss on {metric}; winner = lowest median normalized deviation.", scores, f"predictability::{metric}" if metric else None, True, metric_used=metric)

    if obs_id == "Obs. 2.2":
        speed_metric = cols["run_duration"]
        pred_metric = cols["run_duration"] or cols["envelope"]
        speed_scores = _metric_score_map(df_base, speed_metric, True) if speed_metric else {}
        fastest_two = [s for s, _ in sorted(speed_scores.items(), key=lambda kv: kv[1])[:2]] if speed_scores else STYLE_ORDER[:2]
        scores = _predictability_scores_from_metric(df_base, pred_metric, candidate_styles=fastest_two) if pred_metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: predictability loss on {pred_metric} within the two fastest styles by run duration; winner = highest median normalized deviation.", scores, f"predictability::{pred_metric}" if pred_metric else None, False, metric_used=pred_metric, candidate_styles=fastest_two)

    if obs_id == "Obs. 2.3":
        metric = cols["run_duration"] or cols["envelope"]
        scores = _tail_scores_from_metric(df_base, metric) if metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: upper-tail burden on {metric}; winner = highest P90.", scores, metric, False, metric_used=metric)

    if obs_id == "Obs. 2.4":
        metric = cols["entry"] or cols["run_duration"]
        scores = _predictability_scores_from_metric(df_base, metric) if metric else {}
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: predictability loss on {metric}; winner = highest median normalized deviation.", scores, f"predictability::{metric}" if metric else None, False, metric_used=metric)

    # RQ3
    if obs_id == "Obs. 3.1":
        metric = "execution_window_share"
        scores = _metric_score_map(df_base, metric, False)
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: execution_window_share; winner = highest median execution share.", scores, metric, False, metric_used=metric)

    if obs_id == "Obs. 3.2":
        metric = "pre_invocation_share" if "pre_invocation_share" in df_base.columns else "execution_window_share"
        scores = _metric_score_map(df_base, metric, False)
        return EvalResult(_winner_from_scores(scores, False), f"Primary measurement: {metric}; winner = highest median heavy-entry/heavy-execution proxy.", scores, metric, False, metric_used=metric)

    if obs_id == "Obs. 3.3":
        tmp = df_base.copy()
        for c in ["pre_invocation_share", "execution_window_share", "post_invocation_share"]:
            tmp[c] = pd.to_numeric(tmp[c], errors="coerce")
        tmp["max_phase_share"] = tmp[["pre_invocation_share", "execution_window_share", "post_invocation_share"]].max(axis=1)
        scores = _metric_score_map(tmp, "max_phase_share", True)
        return EvalResult(_winner_from_scores(scores, True), "Primary measurement: max_phase_share; winner = lowest median maximum phase share.", scores, "max_phase_share", True, metric_used="max_phase_share")

    if obs_id == "Obs. 3.4":
        metric = "post_invocation_share"
        scores = _metric_score_map(df_base, metric, False)
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: post_invocation_share; winner = highest median post-invocation share.", scores, metric, False, metric_used=metric)

    # RQ4
    if obs_id == "Obs. 4.1":
        scores = _categorical_rate_scores(df_rq4, "usable_verdict_rate")
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: usable verdict rate; winner = highest rate.", scores, categorical_mode="usable_verdict_rate", metric_used="usable_verdict_rate")

    if obs_id == "Obs. 4.2":
        scores = _categorical_rate_scores(df_rq4, "success_among_usable")
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: success rate among usable verdicts; winner = highest rate.", scores, categorical_mode="success_among_usable", metric_used="success_among_usable")

    if obs_id == "Obs. 4.3":
        tmp = first_attempt_subset(df).copy()
        trigger_col = find_col(tmp, ["event", "trigger"])
        scores = {}
        if trigger_col and "style" in tmp.columns:
            tmp["__schedule_flag"] = tmp[trigger_col].astype(str).str.strip().str.lower().replace({"workflow_dispatch": "other"}).eq("schedule").astype(int)
            rates = tmp.groupby("style")["__schedule_flag"].mean().to_dict()
            scores = {str(k): float(v) for k, v in rates.items() if pd.notna(v)}
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: schedule-triggered deployment share on first-attempt runs; winner = highest schedule share.", scores, categorical_mode="schedule_trigger_share", metric_used="schedule_trigger_share")

    if obs_id == "Obs. 4.4":
        scores = _categorical_rate_scores(df_rq4, "trigger_conditioned_spread")
        return EvalResult(_winner_from_scores(scores, False), "Primary measurement: trigger-conditioned success-rate spread; winner = largest spread.", scores, categorical_mode="trigger_conditioned_spread", metric_used="trigger_conditioned_spread")

    return EvalResult("Insufficient evidence", "No implemented rule for this observation yet.", {})


def validate_stored_answer(row: Dict[str, str], df: pd.DataFrame, stored_answer: str) -> Tuple[str, str, EvalResult]:
    obs_id = norm(row.get("obs_id"))
    result = evaluate_item(obs_id, question_value(row), df)
    if result.winner == "Insufficient evidence":
        return "Insufficient evidence", result.note, result

    stored_styles = normalize_style_answer(stored_answer)
    current_style = stored_styles[0] if stored_styles else norm(stored_answer)
    if obs_id == "Obs. 4.3":
        tmp = first_attempt_subset(df).copy()
        trigger_col = find_col(tmp, ["event", "trigger"])
        if not trigger_col or "style" not in tmp.columns:
            return "Insufficient evidence", "Missing style/event data for schedule-trigger validation.", result
        tmp["__trigger_norm"] = tmp[trigger_col].astype(str).str.strip().str.lower().replace({"workflow_dispatch": "other"})
        table = pd.crosstab(tmp["style"], tmp["__trigger_norm"])
        if table.shape[0] < 2 or table.shape[1] < 2:
            return "Insufficient evidence", "Not enough style/event diversity for chi-square validation.", result
        chi2, p, _, _ = chi2_contingency(table)
        v = _cramers_v_from_table(table, chi2)
        ordered = sorted(result.score_by_style.items(), key=lambda kv: (-kv[1], kv[0]))
        top_gap = ordered[0][1] - ordered[1][1] if len(ordered) >= 2 else 0.0
        fail = result.winner != current_style and p < ALPHA and v >= MIN_CRAMERS_V and top_gap > 0.01
        note = f"Chi-square on style × {trigger_col}: p={p:.3g}, Cramer's V={v:.3f}; schedule-share winner='{result.winner}', stored='{current_style}', top_gap={top_gap:.4f}."
        return ("Failed" if fail else "Passed"), note, result

    if not stored_styles:
        stored_styles = [norm(stored_answer)] if norm(stored_answer) else []
        current_style = stored_styles[0] if stored_styles else ""
    if current_style not in STYLE_SET:
        return "Insufficient evidence", f"Stored answer '{stored_answer}' is not a recognized single-style target.", result

    # categorical modes
    if result.categorical_mode in {"usable_verdict_rate", "success_among_usable", "trigger_conditioned_spread", "schedule_trigger_share"}:
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
        elif result.categorical_mode == "schedule_trigger_share":
            trigger_col = find_col(tmp, ["event", "trigger"])
            tmp["flag"] = tmp[trigger_col].astype(str).str.strip().str.lower().replace({"workflow_dispatch": "other"}).eq("schedule").astype(int)
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

    src = ensure_shares(base_subset(df))
    if result.candidate_styles:
        src = src[src["style"].isin(result.candidate_styles)].copy()

    val_col = result.validation_metric
    lower_is_better = bool(result.lower_is_better)

    if val_col and val_col.startswith("predictability::"):
        raw = val_col.split("::", 1)[1]
        metric_label = f"predictability loss on {raw}"
        tmp = _predictability_row_metric(src, raw)
        val_col = "predictability_value"
    else:
        metric_label = val_col or "unknown metric"
        tmp = src[["style", val_col]].copy() if val_col and val_col in src.columns else pd.DataFrame(columns=["style", "value"])
        if val_col and val_col in tmp.columns:
            tmp[val_col] = pd.to_numeric(tmp[val_col], errors="coerce")
            tmp = tmp.dropna(subset=["style", val_col])

    if tmp.empty:
        return "Insufficient evidence", f"No usable values for validation metric '{metric_label}'.", result

    current_sample = tmp.loc[tmp["style"] == current_style, val_col].dropna()
    if len(current_sample) < 2:
        return "Insufficient evidence", f"Too few observations for stored style '{current_style}' on '{metric_label}'.", result

    styles = [s for s in STYLE_ORDER if s in set(tmp["style"].astype(str))]
    groups = [tmp.loc[tmp["style"] == s, val_col].dropna() for s in styles]
    groups = [g for g in groups if len(g) > 1]
    styles = [s for s in styles if len(tmp.loc[tmp["style"] == s, val_col].dropna()) > 1]
    if len(groups) < 2:
        return "Insufficient evidence", f"Too few style groups for omnibus test on '{metric_label}'.", result

    h_stat, omnibus_p = kruskal(*groups)
    n_total = sum(len(g) for g in groups)
    eps_sq = _kruskal_epsilon_squared(h_stat, len(groups), n_total)

    competitors = [s for s in styles if s != current_style]
    if result.winner and result.winner != current_style and result.winner in competitors:
        competitors = [result.winner] + [s for s in competitors if s != result.winner]

    pairwise = []
    strongest_against = None
    for style in competitors:
        other = tmp.loc[tmp["style"] == style, val_col].dropna()
        if len(other) < 2:
            continue
        alt = "less" if lower_is_better else "greater"
        try:
            stat = mannwhitneyu(current_sample, other, alternative=alt)
            effect = _rank_biserial_from_u(float(stat.statistic), len(current_sample), len(other), lower_is_better)
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
    comps = ", ".join(f"vs {style}: p_adj={adj.get(style, p):.3g}, rbc={eff:.3f}" for style, p, eff in pairwise)
    note = (
        f"Kruskal on {metric_label}: p={omnibus_p:.3g}, epsilon^2={eps_sq:.3f}; {comps}. "
        f"stored='{current_style}', winner='{result.winner}'. "
        f"Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size."
    )
    return ("Failed" if fail else "Passed"), note, result
