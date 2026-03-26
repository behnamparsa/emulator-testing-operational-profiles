# -*- coding: utf-8 -*-
"""
build_total_dataset.py

Build MainDataset.csv for the study by merging:
- Stage 3C run×style dataset: run_per_style_v1_stage3.csv
- Stage 4 signature dataset: run_workload_signature_v3.csv

This version materializes the study-facing timeline variables:
- study_run_duration_seconds
- study_queue_seconds
- study_ttfts_seconds
- study_instru_test_window_seconds
- study_other_seconds
- study_pre_exec_seconds
- study_exec_span_seconds
- study_post_exec_seconds

It also preserves provenance/source fields and keeps policy-ready
instrumentation-window layering for later RQ1 validation.

MainDataset is explicitly filtered to Stage 3 executed instrumentation runs only:
  instru_job_count > 0
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Set

import numpy as np
import pandas as pd

from config.runtime import get_root_dir

# =========================
# CONFIG
# =========================
ROOT_DIR = get_root_dir()

IN_STAGE2 = ROOT_DIR / "run_inventory_per_style.csv"
IN_STAGE3C = ROOT_DIR / "run_per_style_v1_stage3.csv"
IN_STAGE4 = ROOT_DIR / "run_workload_signature_v3.csv"
OUT_TOTAL = ROOT_DIR / "MainDataset.csv"

SIG_COL_CANDIDATES: List[str] = [
    "signature_hash_base",
    "hash_base",
    "sig_hash_base",
]

RAW_IN_SCOPE_STYLES = {"Community", "Custom", "GMD", "Third-Party"}
TERMINAL_CONCLUSIONS = {"success", "failure"}

MAD_Z_THRESHOLD = 3.5
TUKEY_K = 1.5
EPS = 1e-9

WINDOW_FALLBACK_POLICY = {
    "Community": "pending_rq1_validation",
    "Custom": "pending_rq1_validation",
    "GMD": "pending_rq1_validation",
    "Third-Party": "pending_rq1_validation",
}

# =========================
# HELPERS
# =========================
def pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def ensure_alias(df: pd.DataFrame, target: str, candidates: List[str]) -> None:
    if target in df.columns:
        return
    for c in candidates:
        if c in df.columns:
            df.rename(columns={c: target}, inplace=True)
            return


def to_stripped_str(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()


def to_int(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


def to_num(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")


def mad(x: pd.Series) -> float:
    arr = x.dropna().astype(float).to_numpy()
    if arr.size == 0:
        return float("nan")
    med = float(np.median(arr))
    return float(np.median(np.abs(arr - med)))


def robust_z_mad(x: pd.Series) -> pd.Series:
    xx = x.astype(float)
    med = float(np.nanmedian(xx))
    d = mad(xx)
    if np.isnan(d) or d == 0:
        return pd.Series(np.zeros(len(xx)), index=xx.index, dtype=float)
    return 0.6745 * (xx - med) / d


def clip_tiny_negative_to_zero(series: pd.Series, eps: float = EPS) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.mask((s < 0) & (s > -eps), 0.0)


def canonicalize_ttfts_source_value(x: object) -> Optional[str]:
    if pd.isna(x):
        return None

    s = str(x).strip().lower()
    if s in {"", "none", "nan", "na"}:
        return None

    direct_exact = {
        "explicit_instru_step",
        "emu_runner_action_step",
        "runtime_anchor_job_earliest_step_to_anchor_step",
        "direct_step",
    }
    if s in direct_exact:
        return "direct_step"

    if s.startswith("stage1_anchor_name_match:"):
        return "direct_step"

    direct_tokens = [
        "anchor_step",
        "earliest_step_to_anchor_step",
        "direct",
        "step",
    ]
    if any(tok in s for tok in direct_tokens):
        return "direct_step"

    fallback_exact = {
        "fallback_stage2",
        "s2_time_to_first_instru_from_anchor_job_seconds",
        "s2_time_to_first_instru_seconds",
    }
    if s in fallback_exact:
        return "fallback_stage2"

    fallback_tokens = [
        "fallback",
        "s2",
        "stage2",
        "from_anchor_job_seconds",
    ]
    if any(tok in s for tok in fallback_tokens):
        return "fallback_stage2"

    return "unknown"


def build_ttfts_source_final(df: pd.DataFrame) -> pd.Series:
    out = pd.Series(index=df.index, dtype="object")
    explicit = pd.Series(index=df.index, dtype="object")

    if "ttfts_source" in df.columns:
        explicit = explicit.combine_first(df["ttfts_source"].map(canonicalize_ttfts_source_value))
    if "modified_ttfts_source" in df.columns:
        explicit = explicit.combine_first(df["modified_ttfts_source"].map(canonicalize_ttfts_source_value))

    out = explicit

    if "first_test_step_started_at" in df.columns and "ttfts_seconds" in df.columns:
        mask_direct = df["first_test_step_started_at"].notna() & df["ttfts_seconds"].notna()
        out = out.where(~(out.isna() & mask_direct), "direct_step")

    if "S2_time_to_first_instru_seconds" in df.columns and "ttfts_seconds" in df.columns:
        no_direct_marker = (
            df["first_test_step_started_at"].isna()
            if "first_test_step_started_at" in df.columns
            else True
        )
        mask_fb = no_direct_marker & df["S2_time_to_first_instru_seconds"].notna() & df["ttfts_seconds"].notna()
        out = out.where(~(out.isna() & mask_fb), "fallback_stage2")

    if "ttfts_seconds" in df.columns:
        out = out.where(df["ttfts_seconds"].notna(), "missing")

    return out.fillna("unknown")


def direct_only_source(series: pd.Series, label: str) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return pd.Series(np.where(s.notna(), label, "missing"), index=series.index, dtype="object")


def simple_available_source(series: pd.Series, label: str) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return pd.Series(np.where(s.notna(), label, "missing"), index=series.index, dtype="object")


def canonicalize_study_style(x: object) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    key = s.lower().replace("_", " ").replace("-", " ")
    key = " ".join(key.split())

    mapping = {
        "emu community": "Community",
        "community": "Community",
        "emu custom": "Custom",
        "custom": "Custom",
        "gmd": "GMD",
        "third party": "Third-Party",
        "3p": "Third-Party",
        "real device": "Real-Devices",
        "real devices": "Real-Devices",
        "real devices": "Real-Devices",
    }
    return mapping.get(key, s)


def make_style_key(df: pd.DataFrame) -> pd.Series:
    repo = df["repo_full_name"].astype(str).str.strip()
    run = df["workflow_run_id"].astype(str).str.strip()
    attempt = df["attempt"].astype(str).str.strip()
    style = df["style"].map(canonicalize_study_style)
    return repo + "||" + run + "||" + attempt + "||" + style


# =========================
# MAIN
# =========================
def main() -> None:
    df = pd.read_csv(IN_STAGE3C, low_memory=False)
    stage4 = pd.read_csv(IN_STAGE4, low_memory=False)

    # -------------------------------------------------
    # Harmonize V13-style keys
    # -------------------------------------------------
    ensure_alias(df, "repo_full_name", ["full_name", "repository"])
    ensure_alias(df, "workflow_run_id", ["run_id"])
    ensure_alias(df, "attempt", ["run_attempt", "run_attempt_number"])

    ensure_alias(stage4, "repo_full_name", ["full_name", "repository"])
    ensure_alias(stage4, "workflow_run_id", ["run_id"])
    ensure_alias(stage4, "attempt", ["run_attempt", "run_attempt_number"])
    ensure_alias(stage4, "style", ["execution_style", "provider_category", "style_label"])

    required_stage3 = ["repo_full_name", "workflow_run_id", "attempt"]
    for col in required_stage3:
        if col not in df.columns:
            raise ValueError(f"Stage 3C missing required column: {col}")

    required_stage4 = ["repo_full_name", "workflow_run_id"]
    for col in required_stage4:
        if col not in stage4.columns:
            raise ValueError(f"Stage 4 missing required column: {col}")

    for d in (df, stage4):
        for col in ["repo_full_name", "workflow_run_id", "attempt", "style"]:
            if col in d.columns:
                to_stripped_str(d, col)

    to_int(df, "attempt")
    if "attempt" in stage4.columns:
        to_int(stage4, "attempt")

    # -------------------------------------------------
    # Stage 3 executed instrumentation runs only
    # -------------------------------------------------
    if "instru_job_count" not in df.columns:
        raise ValueError("Stage 3C missing required column: instru_job_count")
    df["instru_job_count"] = pd.to_numeric(df["instru_job_count"], errors="coerce").fillna(0)
    df = df.loc[df["instru_job_count"] > 0].copy()

    # -------------------------------------------------
    # Canonical style
    # -------------------------------------------------
    ensure_alias(df, "style", ["execution_style", "provider_category", "style_label"])
    to_stripped_str(df, "style")
    df["style"] = df["style"].map(canonicalize_study_style)
    df["study_style"] = df["style"]

    if "style" in stage4.columns:
        stage4["style"] = stage4["style"].map(canonicalize_study_style)

    # -------------------------------------------------
    # Stage 4 signature merge
    # Prefer style-level merge when Stage 4 has style; otherwise run-level merge
    # -------------------------------------------------
    sig_col = pick_first_existing(stage4, SIG_COL_CANDIDATES)
    if sig_col is None:
        raise ValueError(f"Stage 4 must contain one of the BASE signature columns: {SIG_COL_CANDIDATES}")
    if sig_col != "signature_hash_base":
        stage4.rename(columns={sig_col: "signature_hash_base"}, inplace=True)

    keep4 = ["repo_full_name", "workflow_run_id", "signature_hash_base"]
    if "attempt" in stage4.columns:
        keep4.append("attempt")
    if "style" in stage4.columns:
        keep4.append("style")

    for extra in ["runner_os_bucket", "job_count_total_bucket", "step_count_total_bucket"]:
        if extra in stage4.columns:
            keep4.append(extra)

    stage4_small = stage4[keep4].copy()

    if "style" in stage4_small.columns and "attempt" in stage4_small.columns:
        df["_style_key"] = make_style_key(df)
        stage4_small["_style_key"] = make_style_key(stage4_small)
        stage4_small = stage4_small.drop_duplicates(subset=["_style_key"])
        df = df.merge(
            stage4_small.drop(columns=["repo_full_name", "workflow_run_id", "attempt", "style"], errors="ignore"),
            on="_style_key",
            how="left",
            validate="m:1",
        )
    else:
        merge4 = stage4_small.drop_duplicates(subset=["repo_full_name", "workflow_run_id"])
        df = df.merge(
            merge4,
            on=["repo_full_name", "workflow_run_id"],
            how="left",
            validate="m:1",
        )

    # -------------------------------------------------
    # Harmonize controller / metadata names
    # -------------------------------------------------
    ensure_alias(df, "event", ["trigger", "event_name", "run_event", "github_event_name"])
    ensure_alias(df, "run_attempt", ["attempt", "run_attempt_number"])
    to_int(df, "run_attempt")

    ensure_alias(df, "run_conclusion", ["conclusion", "workflow_run_conclusion", "run_result"])
    to_stripped_str(df, "run_conclusion")

    if "instru_conclusion" not in df.columns:
        raise ValueError("Stage 3C must contain instru_conclusion.")
    to_stripped_str(df, "instru_conclusion")

    ensure_alias(df, "created_at", ["run_created_at", "workflow_created_at"])
    ensure_alias(df, "run_started_at", ["started_at", "workflow_run_started_at"])
    ensure_alias(df, "run_duration_seconds", ["duration_seconds"])
    ensure_alias(df, "queue_seconds", ["queue_duration_seconds"])

    ensure_alias(
        df,
        "ttfts_seconds",
        [
            "ttfts_seconds_modified",
            "time_to_first_instru_from_run_seconds",
            "modified_ttfts_seconds",
        ],
    )

    # IMPORTANT:
    # - instru_duration_seconds = FULL instrumentation-path window
    # - core_instru_window_seconds / instru_exec_window_seconds = CORE execution span only
    ensure_alias(df, "instru_duration_seconds", ["instrumentation_duration_seconds"])
    ensure_alias(df, "core_instru_window_seconds", ["core_execution_window_seconds"])
    ensure_alias(df, "instru_exec_window_seconds", ["exec_window_seconds", "test_exec_window_seconds"])

    ensure_alias(df, "instru_window_seconds", ["instrumentation_window_seconds", "telemetry_instru_window_seconds"])
    ensure_alias(df, "instru_total_seconds", ["instrumentation_total_seconds"])

    ensure_alias(df, "instru_started_at", ["instrumentation_started_at", "instr_started_at"])
    ensure_alias(df, "instru_ended_at", ["instrumentation_ended_at", "instr_ended_at"])
    ensure_alias(df, "test_exec_started_at", ["first_exec_started_at"])
    ensure_alias(df, "test_exec_ended_at", ["last_exec_ended_at"])
    ensure_alias(df, "pre_test_overhead_seconds", ["pre_exec_overhead_seconds"])
    ensure_alias(df, "post_test_overhead_seconds", ["post_exec_overhead_seconds"])

    for c in [
        "run_duration_seconds",
        "queue_seconds",
        "ttfts_seconds",
        "instru_duration_seconds",
        "core_instru_window_seconds",
        "instru_exec_window_seconds",
        "instru_window_seconds",
        "instru_total_seconds",
        "pre_test_overhead_seconds",
        "post_test_overhead_seconds",
        "instru_exec_sum_seconds",
        "env_setup_sum_seconds",
        "artifact_sum_seconds",
        "time_to_first_instru_from_anchor_job_seconds",
        "S2_time_to_first_instru_seconds",
        "S2_time_to_first_instru_from_anchor_job_seconds",
    ]:
        to_num(df, c)


    # Current Stage 3 compatibility:
    # if the canonical ttfts_seconds still does not exist, materialize it from
    # the current Stage 3 name before downstream study-field construction.
    if "ttfts_seconds" not in df.columns and "time_to_first_instru_from_run_seconds" in df.columns:
        df["ttfts_seconds"] = pd.to_numeric(df["time_to_first_instru_from_run_seconds"], errors="coerce")

    if "core_instru_window_seconds" in df.columns and "instru_exec_window_seconds" in df.columns:
        fill_a = df["core_instru_window_seconds"].isna() & df["instru_exec_window_seconds"].notna()
        df.loc[fill_a, "core_instru_window_seconds"] = df.loc[fill_a, "instru_exec_window_seconds"]

        fill_b = df["instru_exec_window_seconds"].isna() & df["core_instru_window_seconds"].notna()
        df.loc[fill_b, "instru_exec_window_seconds"] = df.loc[fill_b, "core_instru_window_seconds"]

    # -------------------------------------------------
    # STUDY-FACING CANONICAL VARIABLES
    # -------------------------------------------------
    df["study_run_duration_seconds"] = df["run_duration_seconds"]
    df["study_run_duration_source_final"] = simple_available_source(
        df["study_run_duration_seconds"], "run_metadata_duration"
    )

    df["study_queue_seconds"] = df["queue_seconds"]
    df["study_queue_source_final"] = simple_available_source(
        df["study_queue_seconds"], "trigger_to_run_start"
    )

    if "ttfts_seconds" not in df.columns:
        raise ValueError(
            "Stage 3C is missing TTFTS. Expected one of: ttfts_seconds, "
            "ttfts_seconds_modified, time_to_first_instru_from_run_seconds."
        )
    df["study_ttfts_seconds"] = df["ttfts_seconds"]
    df["study_ttfts_source_final"] = build_ttfts_source_final(df)

    df["study_ttfts_direct_seconds"] = np.where(
        df["study_ttfts_source_final"].eq("direct_step"),
        df["study_ttfts_seconds"],
        np.nan,
    )

    df["study_ttfts_fallback_seconds"] = np.where(
        df["study_ttfts_source_final"].eq("fallback_stage2"),
        df["study_ttfts_seconds"],
        np.nan,
    )

    df["study_ttfts_fallback_compare_seconds"] = (
        df["S2_time_to_first_instru_seconds"]
        if "S2_time_to_first_instru_seconds" in df.columns
        else np.nan
    )

    df["study_ttfts_overlap_valid"] = (
        pd.to_numeric(df["study_ttfts_direct_seconds"], errors="coerce").notna()
        & pd.to_numeric(df["study_ttfts_fallback_compare_seconds"], errors="coerce").notna()
    )

    # -------------------------------------------------
    # INSTRUMENTATION-TEST WINDOW
    # -------------------------------------------------
    df["study_instru_test_window_direct_seconds"] = df["instru_duration_seconds"]
    df["study_instru_test_window_direct_source_final"] = direct_only_source(
        df["study_instru_test_window_direct_seconds"], "direct_instru_path_window"
    )

    if "instru_window_seconds" in df.columns:
        df["study_instru_test_window_fallback_candidate_seconds"] = df["instru_window_seconds"]
    else:
        df["study_instru_test_window_fallback_candidate_seconds"] = np.nan

    df["study_instru_test_window_fallback_candidate_source"] = simple_available_source(
        df["study_instru_test_window_fallback_candidate_seconds"],
        "telemetry_instru_window_candidate",
    )

    df["study_instru_test_window_fallback_candidate_name"] = np.where(
        pd.to_numeric(df["study_instru_test_window_fallback_candidate_seconds"], errors="coerce").notna(),
        "instru_window_seconds",
        "missing",
    )

    if "instru_total_seconds" in df.columns:
        df["study_instru_test_window_fallback_alt_candidate_seconds"] = df["instru_total_seconds"]
    else:
        df["study_instru_test_window_fallback_alt_candidate_seconds"] = np.nan

    df["study_instru_test_window_direct_for_validation_seconds"] = df["study_instru_test_window_direct_seconds"]
    df["study_instru_test_window_fallback_compare_seconds"] = df["study_instru_test_window_fallback_candidate_seconds"]

    df["study_instru_test_window_overlap_valid"] = (
        pd.to_numeric(df["study_instru_test_window_direct_for_validation_seconds"], errors="coerce").notna()
        & pd.to_numeric(df["study_instru_test_window_fallback_compare_seconds"], errors="coerce").notna()
    )

    df["study_instru_test_window_resolution_policy"] = df["study_style"].map(
        lambda s: WINDOW_FALLBACK_POLICY.get(s, "pending_rq1_validation")
    )

    df["study_instru_test_window_fallback_eligible"] = "unknown"

    # Direct-only for now
    df["study_instru_test_window_seconds"] = df["study_instru_test_window_direct_seconds"]
    df["study_instru_test_window_source_final"] = np.where(
        pd.to_numeric(df["study_instru_test_window_direct_seconds"], errors="coerce").notna(),
        "direct_instru_path_window",
        "missing",
    )

    df["study_other_seconds"] = np.where(
        df["study_run_duration_seconds"].notna()
        & df["study_ttfts_seconds"].notna()
        & df["study_instru_test_window_seconds"].notna(),
        df["study_run_duration_seconds"]
        - df["study_ttfts_seconds"]
        - df["study_instru_test_window_seconds"],
        np.nan,
    )
    df["study_other_seconds"] = clip_tiny_negative_to_zero(df["study_other_seconds"])
    df["study_other_source_final"] = np.where(
        pd.to_numeric(df["study_other_seconds"], errors="coerce").notna(),
        "derived_from_run_ttfts_resolved_window",
        "missing",
    )

    df["study_pre_exec_seconds"] = (
        df["pre_test_overhead_seconds"] if "pre_test_overhead_seconds" in df.columns else np.nan
    )
    df["study_exec_span_seconds"] = (
        df["instru_exec_window_seconds"] if "instru_exec_window_seconds" in df.columns else np.nan
    )
    df["study_post_exec_seconds"] = (
        df["post_test_overhead_seconds"] if "post_test_overhead_seconds" in df.columns else np.nan
    )

    df["study_pre_exec_source_final"] = direct_only_source(
        df["study_pre_exec_seconds"], "direct_instru_path_window"
    )
    df["study_exec_span_source_final"] = direct_only_source(
        df["study_exec_span_seconds"], "direct_instru_exec_span"
    )
    df["study_post_exec_source_final"] = direct_only_source(
        df["study_post_exec_seconds"], "direct_instru_path_window"
    )

    df["study_window_decomp_sum_seconds"] = np.where(
        pd.to_numeric(df["study_pre_exec_seconds"], errors="coerce").notna()
        | pd.to_numeric(df["study_exec_span_seconds"], errors="coerce").notna()
        | pd.to_numeric(df["study_post_exec_seconds"], errors="coerce").notna(),
        pd.to_numeric(df["study_pre_exec_seconds"], errors="coerce").fillna(0)
        + pd.to_numeric(df["study_exec_span_seconds"], errors="coerce").fillna(0)
        + pd.to_numeric(df["study_post_exec_seconds"], errors="coerce").fillna(0),
        np.nan,
    )

    df["study_window_decomp_diff_seconds"] = np.where(
        pd.to_numeric(df["study_instru_test_window_direct_seconds"], errors="coerce").notna()
        & pd.to_numeric(df["study_window_decomp_sum_seconds"], errors="coerce").notna(),
        pd.to_numeric(df["study_instru_test_window_direct_seconds"], errors="coerce")
        - pd.to_numeric(df["study_window_decomp_sum_seconds"], errors="coerce"),
        np.nan,
    )
    df["study_window_decomp_diff_seconds"] = clip_tiny_negative_to_zero(df["study_window_decomp_diff_seconds"])

    # -------------------------------------------------
    # PAPER-ALIGNED CONTROLLER
    # Base = in-scope style + attempt1 + terminal run conclusion + terminal instru conclusion
    # -------------------------------------------------
    is_in_scope_style = df["style"].isin(RAW_IN_SCOPE_STYLES)
    is_attempt1 = df["run_attempt"].astype("Int64").eq(1)

    run_concl = df["run_conclusion"].fillna("").astype(str).str.lower()
    instr_concl = df["instru_conclusion"].fillna("").astype(str).str.lower()

    is_run_terminal = run_concl.isin(TERMINAL_CONCLUSIONS)
    is_instru_terminal = instr_concl.isin(TERMINAL_CONCLUSIONS)

    df["Base"] = is_in_scope_style & is_attempt1 & is_run_terminal & is_instru_terminal

    # -------------------------------------------------
    # ROBUST FLAG (diagnostic only)
    # -------------------------------------------------
    base_runs = (
        df.loc[
            df["Base"] & df["signature_hash_base"].notna(),
            ["signature_hash_base", "repo_full_name", "workflow_run_id"],
        ]
        .drop_duplicates()
        .copy()
    )

    sig_n_runs = base_runs.groupby("signature_hash_base")["workflow_run_id"].nunique().rename("n_runs")
    sig_n_repos = base_runs.groupby("signature_hash_base")["repo_full_name"].nunique().rename("n_repos")

    sig_stats = pd.concat([sig_n_runs, sig_n_repos], axis=1).reset_index()
    sig_stats["runs_per_repo"] = sig_stats["n_runs"] / sig_stats["n_repos"].replace(0, np.nan)

    q1 = sig_stats["runs_per_repo"].quantile(0.25)
    q3 = sig_stats["runs_per_repo"].quantile(0.75)
    iqr = q3 - q1
    tukey_upper = q3 + TUKEY_K * iqr
    sig_stats["out_tukey"] = sig_stats["runs_per_repo"] > tukey_upper

    sig_stats["z_mad"] = robust_z_mad(sig_stats["runs_per_repo"])
    sig_stats["out_mad"] = sig_stats["z_mad"].abs() > MAD_Z_THRESHOLD

    sig_stats["outlier_signature"] = sig_stats["out_tukey"] & sig_stats["out_mad"]
    outlier_sigs: Set[str] = set(sig_stats.loc[sig_stats["outlier_signature"], "signature_hash_base"].astype(str))

    df["Robust"] = (
        df["Base"]
        & df["signature_hash_base"].notna()
        & (~df["signature_hash_base"].astype(str).isin(outlier_sigs))
    )

    # -------------------------------------------------
    # OUTPUT ORDERING
    # -------------------------------------------------
    id_cols = [
        "repo_full_name",
        "workflow_id",
        "workflow_name",
        "workflow_run_id",
        "attempt",
        "run_attempt",
        "event",
        "head_branch",
        "default_branch",
        "head_sha",
        "style",
        "study_style",
        "styles",
        "invocation_types",
        "third_party_provider_name",
        "instru_job_count",
    ]

    controller_cols = [
        "run_conclusion",
        "instru_conclusion",
        "Base",
        "Robust",
    ]

    core_raw_timeline_cols = [
        "created_at",
        "run_started_at",
        "queue_seconds",
        "run_duration_seconds",
        "ttfts_seconds",
        "instru_duration_seconds",
        "instru_window_seconds",
        "instru_total_seconds",
        "core_instru_window_seconds",
        "instru_exec_window_seconds",
    ]

    ttfts_provenance_cols = [
        "ttfts_source",
        "modified_ttfts_source",
        "modified_ttfts_quality",
        "first_test_step_started_at",
        "anchor_job_name",
        "anchor_job_started_at",
        "anchor_job_start_source",
        "time_to_first_instru_from_anchor_job_seconds",
        "time_to_first_instru_from_anchor_job_quality",
        "S2_time_to_first_instru_seconds",
        "S2_time_to_first_instru_from_anchor_job_seconds",
    ]

    direct_window_cols = [
        "instru_started_at",
        "instru_ended_at",
        "test_exec_started_at",
        "test_exec_ended_at",
        "pre_test_overhead_seconds",
        "post_test_overhead_seconds",
        "instru_exec_sum_seconds",
        "env_setup_sum_seconds",
        "artifact_sum_seconds",
    ]

    study_timeline_cols = [
        "study_run_duration_seconds",
        "study_run_duration_source_final",
        "study_queue_seconds",
        "study_queue_source_final",
        "study_ttfts_seconds",
        "study_ttfts_source_final",
        "study_ttfts_direct_seconds",
        "study_ttfts_fallback_seconds",
        "study_ttfts_fallback_compare_seconds",
        "study_ttfts_overlap_valid",
        "study_instru_test_window_direct_seconds",
        "study_instru_test_window_direct_source_final",
        "study_instru_test_window_fallback_candidate_seconds",
        "study_instru_test_window_fallback_candidate_source",
        "study_instru_test_window_fallback_candidate_name",
        "study_instru_test_window_fallback_alt_candidate_seconds",
        "study_instru_test_window_direct_for_validation_seconds",
        "study_instru_test_window_fallback_compare_seconds",
        "study_instru_test_window_overlap_valid",
        "study_instru_test_window_resolution_policy",
        "study_instru_test_window_fallback_eligible",
        "study_instru_test_window_seconds",
        "study_instru_test_window_source_final",
        "study_other_seconds",
        "study_other_source_final",
        "study_pre_exec_seconds",
        "study_pre_exec_source_final",
        "study_exec_span_seconds",
        "study_exec_span_source_final",
        "study_post_exec_seconds",
        "study_post_exec_source_final",
        "study_window_decomp_sum_seconds",
        "study_window_decomp_diff_seconds",
    ]

    robustness_cols = [
        "signature_hash_base",
        "runner_os_bucket",
        "job_count_total_bucket",
        "step_count_total_bucket",
    ]

    front_cols = [
        c
        for c in (
            id_cols
            + controller_cols
            + core_raw_timeline_cols
            + ttfts_provenance_cols
            + direct_window_cols
            + robustness_cols
        )
        if c in df.columns
    ]

    remaining_cols = [c for c in df.columns if c not in (front_cols + study_timeline_cols)]
    tail_study_cols = [c for c in study_timeline_cols if c in df.columns]

    df_out = df[front_cols + remaining_cols + tail_study_cols].copy()

    if "_style_key" in df_out.columns:
        df_out.drop(columns=["_style_key"], inplace=True, errors="ignore")

    # -------------------------------------------------
    # OUTPUT
    # -------------------------------------------------
    df_out.to_csv(OUT_TOTAL, index=False, encoding="utf-8-sig")

    print(f"[done] wrote {OUT_TOTAL}")
    print(f"[info] rows kept after instru_job_count > 0 filter: {len(df_out)}")
    print(
        f"[info] unique runs kept after instru_job_count > 0 filter: "
        f"{df_out[['repo_full_name', 'workflow_run_id']].drop_duplicates().shape[0]}"
    )
    print(f"[info] four-style emulator rows: {df_out['style'].isin(RAW_IN_SCOPE_STYLES).sum()}")
    print(f"[info] Base rows: {int(df_out['Base'].sum())}")
    print(f"[info] Robust rows: {int(df_out['Robust'].sum())}")

    print(f"[info] study_run_duration_seconds available: {df_out['study_run_duration_seconds'].notna().sum()}")
    print(f"[info] study_queue_seconds available: {df_out['study_queue_seconds'].notna().sum()}")
    print(f"[info] study_ttfts_seconds available: {df_out['study_ttfts_seconds'].notna().sum()}")
    print(f"[info] study_ttfts_direct_seconds available: {df_out['study_ttfts_direct_seconds'].notna().sum()}")
    print(f"[info] study_ttfts_fallback_seconds available: {df_out['study_ttfts_fallback_seconds'].notna().sum()}")
    print(f"[info] study_ttfts_overlap_valid count: {int(df_out['study_ttfts_overlap_valid'].sum())}")

    print(
        "[info] study_instru_test_window_direct_seconds available: "
        f"{df_out['study_instru_test_window_direct_seconds'].notna().sum()}"
    )
    print(
        "[info] study_instru_test_window_fallback_candidate_seconds available: "
        f"{df_out['study_instru_test_window_fallback_candidate_seconds'].notna().sum()}"
    )
    print(
        "[info] study_instru_test_window_overlap_valid count: "
        f"{int(df_out['study_instru_test_window_overlap_valid'].sum())}"
    )
    print(
        "[info] study_instru_test_window_seconds available (resolved/default): "
        f"{df_out['study_instru_test_window_seconds'].notna().sum()}"
    )

    print(f"[info] study_other_seconds available: {df_out['study_other_seconds'].notna().sum()}")
    print(f"[info] study_pre_exec_seconds available: {df_out['study_pre_exec_seconds'].notna().sum()}")
    print(f"[info] study_exec_span_seconds available: {df_out['study_exec_span_seconds'].notna().sum()}")
    print(f"[info] study_post_exec_seconds available: {df_out['study_post_exec_seconds'].notna().sum()}")

    if "study_window_decomp_diff_seconds" in df_out.columns:
        diff_nonnull = pd.to_numeric(df_out["study_window_decomp_diff_seconds"], errors="coerce").dropna()
        exact_zero = int((diff_nonnull == 0).sum()) if len(diff_nonnull) else 0
        print(f"[info] study_window_decomp_diff_seconds non-null: {len(diff_nonnull)}")
        print(f"[info] exact decomposition matches: {exact_zero}")

    print(f"[info] outlier signatures (Tukey & MAD): {len(outlier_sigs)}")
    if outlier_sigs:
        print("       ", sorted(outlier_sigs))


if __name__ == "__main__":
    main()