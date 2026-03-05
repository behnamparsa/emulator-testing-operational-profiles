# -*- coding: utf-8 -*-
"""
build_total_dataset.py

Build the "Total.csv" base dataset for the study by merging:
- Stage 3A run×style dataset: run_per_style_v1_stage3.csv
- Stage 4 signature dataset: run_workload_signature_v3.csv

Adds:
- signature_hash_base: base workflow-shape signature per record
- Base: True if included under controller filters (emulator-only, attempt=1, success/failure)
- Robust: True if Base==True and NOT excluded by Tukey+MAD outlier detection on signature runs-per-repo

IMPORTANT:
- MainDataset is explicitly filtered to Stage 3 executed instrumentation runs only:
  instru_job_count > 0

Output:
- MainDataset.csv saved to the same folder as the inputs (ROOT_DIR).

ROOT_DIR default:
  C:\Android Mobile App\ICST2026_Ext
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Set

import numpy as np
import pandas as pd

# =========================
# CONFIG
# =========================
from config.runtime import get_root_dir

ROOT_DIR = get_root_dir()

IN_STAGE3A = ROOT_DIR / "run_per_style_v1_stage3.csv"
IN_STAGE4 = ROOT_DIR / "run_workload_signature_v3.csv"
OUT_TOTAL = ROOT_DIR / "MainDataset.csv"

# Stage 4 signature column candidates (we rename the first found to signature_hash_base)
SIG_COL_CANDIDATES: List[str] = [
    "signature_hash_base",
    "hash_base",
    "sig_hash_base",
    "signature_hash",
]

# Controller definitions
VERDICT_COMPLETE = {"success", "failure"}

# Outlier detection settings
MAD_Z_THRESHOLD = 3.5
TUKEY_K = 1.5


# =========================
# Helpers
# =========================
def pick_first_existing(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def to_stripped_str(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].astype(str).str.strip()


def to_int(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")


def mad(x: pd.Series) -> float:
    """Raw median absolute deviation (MAD)."""
    arr = x.dropna().astype(float).to_numpy()
    if arr.size == 0:
        return float("nan")
    med = float(np.median(arr))
    return float(np.median(np.abs(arr - med)))


def robust_z_mad(x: pd.Series) -> pd.Series:
    """Robust z-score using MAD: z = 0.6745 * (x - median) / MAD."""
    xx = x.astype(float)
    med = float(np.nanmedian(xx))
    d = mad(xx)
    if np.isnan(d) or d == 0:
        return pd.Series(np.zeros(len(xx)), index=xx.index, dtype=float)
    return 0.6745 * (xx - med) / d


def is_real_device_style(style_series: pd.Series) -> pd.Series:
    """Heuristic: Real-Device styles contain both 'real' and 'device'."""
    s = style_series.fillna("").astype(str).str.lower()
    return s.str.contains("real", na=False) & s.str.contains("device", na=False)


# =========================
# Main
# =========================
def main() -> None:
    stage3 = pd.read_csv(IN_STAGE3A, low_memory=False)
    stage4 = pd.read_csv(IN_STAGE4, low_memory=False)

    # Normalize key columns in both inputs
    for df in (stage3, stage4):
        if "repo_full_name" in df.columns and "full_name" not in df.columns:
            df.rename(columns={"repo_full_name": "full_name"}, inplace=True)
        if "repository" in df.columns and "full_name" not in df.columns:
            df.rename(columns={"repository": "full_name"}, inplace=True)

    for col in ["full_name", "run_id"]:
        if col not in stage3.columns:
            raise ValueError(f"Stage 3A missing required column: {col}")
        if col not in stage4.columns:
            raise ValueError(f"Stage 4 missing required column: {col}")

    to_stripped_str(stage3, "full_name")
    to_stripped_str(stage3, "run_id")
    to_stripped_str(stage4, "full_name")
    to_stripped_str(stage4, "run_id")

    # =========================
    # Enforce Stage 3 executed instrumentation runs only
    # =========================
    if "instru_job_count" not in stage3.columns:
        raise ValueError("Stage 3A missing required column for filtering: instru_job_count")
    stage3["instru_job_count"] = pd.to_numeric(stage3["instru_job_count"], errors="coerce").fillna(0)
    stage3 = stage3.loc[stage3["instru_job_count"] > 0].copy()

    # Identify and normalize signature column name in Stage 4
    sig_col = pick_first_existing(stage4, SIG_COL_CANDIDATES)
    if sig_col is None:
        raise ValueError(f"Stage 4 must contain one of: {SIG_COL_CANDIDATES}")
    if sig_col != "signature_hash_base":
        stage4.rename(columns={sig_col: "signature_hash_base"}, inplace=True)

    # Keep minimal Stage 4 columns (plus optional buckets for debugging)
    keep4 = ["full_name", "run_id", "signature_hash_base"]
    for extra in ["runner_os_bucket", "job_count_total_bucket", "step_count_total_bucket"]:
        if extra in stage4.columns:
            keep4.append(extra)

    stage4_small = stage4[keep4].drop_duplicates(subset=["full_name", "run_id"])

    # Merge (Stage 3A has many records per run; Stage 4 is per run)
    df = stage3.merge(stage4_small, on=["full_name", "run_id"], how="left", validate="m:1")

    # Harmonize common naming for trigger, attempt, conclusion, style
    if "trigger" not in df.columns:
        for alt in ["event_name", "run_event", "github_event_name"]:
            if alt in df.columns:
                df.rename(columns={alt: "trigger"}, inplace=True)
                break

    if "run_attempt" not in df.columns:
        for alt in ["attempt", "run_attempt_number"]:
            if alt in df.columns:
                df.rename(columns={alt: "run_attempt"}, inplace=True)
                break
    to_int(df, "run_attempt")

    if "run_conclusion" not in df.columns:
        for alt in ["conclusion", "workflow_run_conclusion", "run_result"]:
            if alt in df.columns:
                df.rename(columns={alt: "run_conclusion"}, inplace=True)
                break
    to_stripped_str(df, "run_conclusion")

    if "style" not in df.columns:
        for alt in ["execution_style", "provider_category", "style_label"]:
            if alt in df.columns:
                df.rename(columns={alt: "style"}, inplace=True)
                break
    to_stripped_str(df, "style")

    # =========================
    # Base flag (controllers)
    # =========================
    is_real_device = is_real_device_style(df["style"]) if "style" in df.columns else pd.Series(False, index=df.index)
    is_attempt1 = (df["run_attempt"].astype("Int64") == 1) if "run_attempt" in df.columns else pd.Series(False, index=df.index)
    concl = df["run_conclusion"].fillna("").astype(str).str.lower()
    is_verdict_complete = concl.isin(VERDICT_COMPLETE)

    df["Base"] = (~is_real_device) & is_attempt1 & is_verdict_complete

    # =========================
    # Robust flag (signature-level outlier detection)
    # =========================
    # Compute outliers on Base==True runs, using UNIQUE runs per signature.
    base_runs = (
        df.loc[df["Base"] & df["signature_hash_base"].notna(), ["signature_hash_base", "full_name", "run_id"]]
        .drop_duplicates()
        .copy()
    )

    # Signature-level runs and repos
    sig_n_runs = base_runs.groupby("signature_hash_base")["run_id"].nunique().rename("n_runs")
    sig_n_repos = base_runs.groupby("signature_hash_base")["full_name"].nunique().rename("n_repos")

    sig_stats = pd.concat([sig_n_runs, sig_n_repos], axis=1).reset_index()
    sig_stats["runs_per_repo"] = sig_stats["n_runs"] / sig_stats["n_repos"].replace(0, np.nan)

    # Tukey upper fence on runs_per_repo
    q1 = sig_stats["runs_per_repo"].quantile(0.25)
    q3 = sig_stats["runs_per_repo"].quantile(0.75)
    iqr = q3 - q1
    tukey_upper = q3 + TUKEY_K * iqr
    sig_stats["out_tukey"] = sig_stats["runs_per_repo"] > tukey_upper

    # MAD robust z-score on runs_per_repo
    sig_stats["z_mad"] = robust_z_mad(sig_stats["runs_per_repo"])
    sig_stats["out_mad"] = sig_stats["z_mad"].abs() > MAD_Z_THRESHOLD

    # Conservative: outlier if BOTH Tukey and MAD
    sig_stats["outlier_signature"] = sig_stats["out_tukey"] & sig_stats["out_mad"]
    outlier_sigs: Set[str] = set(sig_stats.loc[sig_stats["outlier_signature"], "signature_hash_base"].astype(str))

    # Record-level Robust flag:
    df["Robust"] = (
        df["Base"]
        & df["signature_hash_base"].notna()
        & (~df["signature_hash_base"].astype(str).isin(outlier_sigs))
    )

    # =========================
    # Output
    # =========================
    front_cols = [
        c for c in [
            "full_name", "run_id", "trigger", "run_attempt", "run_conclusion",
            "style", "instru_job_count", "signature_hash_base", "Base", "Robust"
        ]
        if c in df.columns
    ]

    metric_cols = [
        c for c in [
            "duration_seconds", "run_duration_seconds", "ttfts_seconds", "ttfts_seconds_modified",
            "queue_seconds", "instr_window_seconds", "instrumentation_window_seconds"
        ]
        if c in df.columns and c not in front_cols
    ]

    remaining_cols = [c for c in df.columns if c not in (front_cols + metric_cols)]
    df_out = df[front_cols + metric_cols + remaining_cols]

    df_out.to_csv(OUT_TOTAL, index=False, encoding="utf-8")
    print(f"[done] wrote {OUT_TOTAL}")
    print(f"[info] rows kept after instru_job_count > 0 filter: {len(df_out)}")
    print(f"[info] unique runs kept after instru_job_count > 0 filter: {df_out[['full_name','run_id']].drop_duplicates().shape[0]}")
    print(f"[info] outlier signatures (Tukey & MAD): {len(outlier_sigs)}")
    if outlier_sigs:
        print("       ", sorted(outlier_sigs))


if __name__ == "__main__":
    main()