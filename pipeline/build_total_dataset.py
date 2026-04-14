from __future__ import annotations

# ============================================================
# One-cell Jupyter code to build MainDataset.csv
# CLEAN STUDY-FACING VERSION
#
# Key design:
# - study_signature_hash is defined directly from Stage 4
#   signature_hash_base_exec (no fallback)
# - only exec-signature-related Stage 4 fields are retained
# - regular/full signature fields are dropped from MainDataset
# - proxy fields are dropped
# - likely Stage 3 false positives are removed
# - keeps only clear regime flags at end:
#     Base_timing_regime
#     Layer2_available_in_base
#     Step_telemetry
# - removes:
#     trigger
#     Base
#     Robust
#
# IMPORTANT ADJUSTMENT:
# - Step_telemetry is defined ONLY by the availability of
#   Stage 3 step telemetry for the run×style record:
#       pre_invocation_seconds
#       invocation_execution_window_seconds
#       post_invocation_seconds
# - It does NOT depend on study_signature_hash
# - It does NOT depend on Base_timing_regime
# ============================================================

from pathlib import Path
import numpy as np
import pandas as pd
from config.runtime import get_root_dir

# ----------------------------
# Paths
# ----------------------------
BASE_DIR = get_root_dir()

IN_STAGE1 = BASE_DIR / "verified_workflows_v16.csv"
IN_STAGE2 = BASE_DIR / "run_inventory_per_style.csv"
IN_STAGE3 = BASE_DIR / "run_per_style_v1_stage3.csv"
IN_STAGE4 = BASE_DIR / "run_workload_signature_v3.csv"

OUT_MAIN = BASE_DIR / "MainDataset.csv"

# ----------------------------
# Helpers
# ----------------------------
CANONICAL_STYLES = ["Community", "Custom", "Third-Party", "GMD", "Real-Device"]

def canon_style_token(x):
    if pd.isna(x):
        return np.nan
    s = str(x).strip()
    mapping = {
        "community": "Community",
        "custom": "Custom",
        "third-party": "Third-Party",
        "third party": "Third-Party",
        "gmd": "GMD",
        "real-device": "Real-Device",
        "real device": "Real-Device",
    }
    return mapping.get(s.lower(), s)

def canon_style_list(x):
    if pd.isna(x):
        return np.nan
    parts = [canon_style_token(p) for p in str(x).split(",")]
    parts = [p for p in parts if pd.notna(p) and str(p).strip() != ""]
    return ",".join(parts) if parts else np.nan

def bool_from_series(s):
    return s.fillna(False).astype(bool)

def in_style_scope(styles_csv, target_style):
    if pd.isna(styles_csv) or pd.isna(target_style):
        return False
    parts = [p.strip() for p in str(styles_csv).split(",") if p.strip()]
    return str(target_style).strip() in parts

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def source_col(condition, label):
    return pd.Series(np.where(condition, label, None), index=condition.index, dtype="object")

def safe_series(df, col, dtype=None):
    if col in df.columns:
        return df[col]
    return pd.Series(index=df.index, dtype=dtype)

def coalesce_cols(df, cols, dtype=None):
    available = [c for c in cols if c in df.columns]
    if not available:
        return pd.Series(index=df.index, dtype=dtype)
    out = df[available[0]].copy()
    for c in available[1:]:
        out = out.combine_first(df[c])
    return out

def blank_to_na(series):
    s = series.astype("string")
    return s.mask(s.str.fullmatch(r"\s*", na=False))

def add_cols(df, mapping):
    return pd.concat([df, pd.DataFrame(mapping, index=df.index)], axis=1)

# ----------------------------
# Read inputs
# ----------------------------
print("Reading input files...")
stage1 = pd.read_csv(IN_STAGE1, low_memory=False)
stage2 = pd.read_csv(IN_STAGE2, low_memory=False)
stage3 = pd.read_csv(IN_STAGE3, low_memory=False)
stage4 = pd.read_csv(IN_STAGE4, low_memory=False)

for c in [
    "signature_hash_base_exec",
    "job_count_exec",
    "job_count_exec_bucket",
    "step_count_exec",
    "step_count_exec_bucket",
    "runner_os_bucket",
    "runner_os_source",
    "effective_ref_for_stage4",
    "signature_inputs",
]:
    if c in stage4.columns:
        stage4[c] = blank_to_na(stage4[c])

print("Stage 1 shape:", stage1.shape)
print("Stage 2 shape:", stage2.shape)
print("Stage 3 shape:", stage3.shape)
print("Stage 4 shape:", stage4.shape)

# ----------------------------
# Canonicalize style fields
# ----------------------------
for df_, cols in [
    (stage1, ["styles"]),
    (stage2, ["target_style", "styles_in_run_all"]),
    (stage3, ["target_style", "inferred_styles_all"]),
]:
    for c in cols:
        if c in df_.columns:
            if c == "target_style":
                df_[c] = df_[c].map(canon_style_token)
            else:
                df_[c] = df_[c].map(canon_style_list)

# ----------------------------
# Keep only relevant fields
# ----------------------------
stage2_keep = [
    "full_name","run_id","target_style","workflow_identifier","workflow_id","workflow_path",
    "run_number","run_attempt","created_at","run_started_at","run_updated_at","status",
    "run_conclusion","event","head_branch","head_sha","html_url","styles_in_run_all",
    "multi_style_run_flag","layer1_run_started_at_effective","layer1_run_ended_at_effective",
    "layer1_run_duration_seconds_effective","layer1_run_timing_source","style_instru_job_count",
    "style_instru_job_names","style_first_instru_job_name","style_first_instru_job_started_at",
    "style_first_instru_job_source","style_last_instru_job_name","style_last_instru_job_completed_at",
    "style_last_instru_job_source","style_time_to_instrumentation_envelope_seconds",
    "style_instrumentation_job_envelope_seconds","style_post_instrumentation_tail_seconds",
    "style_layer1_model","style_distinct_job_count","style_distinct_job_base_name_count",
    "style_matrix_like_job_count","style_matrix_expanded_flag","style_parallel_same_style_flag",
    "style_max_parallel_jobs","style_repeated_same_style_flag",
]

stage3_keep = [
    "full_name","workflow_path","workflow_ref","run_id","run_attempt","status","run_conclusion",
    "event","target_style","inferred_styles_all","layer2_measurement_mode",
    "layer2_measurement_quality","run_boundary_start_at","run_boundary_end_at",
    "matched_invocation_step_name","matched_invocation_job_name","matched_invocation_source",
    "matched_invocation_step_started_at","matched_invocation_step_completed_at",
    "matched_invocation_job_ordinal_in_run","matched_invocation_step_ordinal_in_job",
    "invocation_execution_end_step_name","invocation_execution_end_job_name",
    "invocation_execution_end_source","invocation_execution_end_step_started_at",
    "invocation_execution_end_step_completed_at","invocation_execution_end_job_ordinal_in_run",
    "invocation_execution_end_step_ordinal_in_job","invocation_execution_window_started_at",
    "invocation_execution_window_ended_at","pre_invocation_seconds",
    "invocation_execution_window_seconds","post_invocation_seconds","setup_sum_seconds",
    "provision_sum_seconds","test_sum_seconds","artifact_report_sum_seconds",
    "cleanup_teardown_sum_seconds","other_sum_seconds","execution_related_sum_seconds",
    "non_execution_overhead_sum_seconds","pre_test_overhead_sum_seconds",
    "active_test_sum_seconds","post_test_overhead_sum_seconds","setup_step_count",
    "provision_step_count","test_step_count","artifact_report_step_count",
    "cleanup_teardown_step_count","other_step_count","execution_related_step_count",
    "non_execution_overhead_step_count","pre_test_overhead_step_count",
    "active_test_step_count","post_test_overhead_step_count",
    "invocation_candidate_count_total","stage1_anchor_candidate_count",
    "explicit_instru_candidate_count","custom_supported_candidate_count",
    "distinct_invocation_candidate_step_name_count","distinct_invocation_candidate_job_count",
    "invocation_candidate_step_names","invocation_candidate_job_names",
    "selected_invocation_priority_source","execution_window_candidate_count",
    "execution_window_distinct_job_count","execution_window_candidate_job_names",
    "cross_job_execution_window_flag","style_distinct_job_count",
    "style_distinct_job_base_name_count","style_matrix_like_job_count",
    "style_matrix_expanded_flag","style_parallel_same_style_flag","style_max_parallel_jobs",
    "style_repeated_same_style_flag",
]

stage4_keep = [
    "full_name","run_id","workflow_identifier","workflow_path","head_sha",
    "effective_ref_for_stage4","signature_inputs",
    "runner_os_bucket","runner_os_source",
    "signature_hash_base_exec",
    "job_count_exec","job_count_exec_bucket",
    "step_count_exec","step_count_exec_bucket",
]

stage1_keep = [
    "full_name","workflow_identifier","workflow_id","workflow_path","styles",
    "invocation_types","third_party_provider_name",
]

s2 = stage2[[c for c in stage2_keep if c in stage2.columns]].copy()
s3 = stage3[[c for c in stage3_keep if c in stage3.columns]].copy()
s4 = stage4[[c for c in stage4_keep if c in stage4.columns]].copy()
s1 = stage1[[c for c in stage1_keep if c in stage1.columns]].copy()

# ----------------------------
# Deduplicate Stage 1 safely
# ----------------------------
if not s1.empty:
    for c in ["workflow_identifier", "workflow_id", "workflow_path"]:
        if c in s1.columns:
            s1[c] = s1[c].mask(s1[c].astype("string").str.fullmatch(r"\s*", na=False))

    s1_exact = s1.dropna(subset=["workflow_identifier", "workflow_id"]).drop_duplicates(
        subset=["full_name", "workflow_identifier", "workflow_id"], keep="first"
    ).copy()
    s1_by_ident = s1.dropna(subset=["workflow_identifier"]).drop_duplicates(
        subset=["full_name", "workflow_identifier"], keep="first"
    ).copy()
    s1_by_wfid = s1.dropna(subset=["workflow_id"]).drop_duplicates(
        subset=["full_name", "workflow_id"], keep="first"
    ).copy()
else:
    s1_exact = s1.copy()
    s1_by_ident = s1.copy()
    s1_by_wfid = s1.copy()

# ----------------------------
# Rename overlapping columns before merge
# ----------------------------
s2 = s2.rename(columns={
    "workflow_identifier": "workflow_identifier_s2",
    "workflow_id": "workflow_id_s2",
    "workflow_path": "workflow_path_s2",
    "run_number": "run_number_s2",
    "run_attempt": "run_attempt_s2",
    "created_at": "created_at_s2",
    "run_started_at": "run_started_at_s2",
    "run_updated_at": "run_updated_at_s2",
    "status": "status_s2",
    "run_conclusion": "run_conclusion_s2",
    "event": "event_s2",
    "head_branch": "head_branch_s2",
    "head_sha": "head_sha_s2",
    "html_url": "html_url_s2",
    "styles_in_run_all": "styles_in_run_all_s2",
    "multi_style_run_flag": "multi_style_run_flag_s2",
    "layer1_run_started_at_effective": "layer1_run_started_at_effective_s2",
    "layer1_run_ended_at_effective": "layer1_run_ended_at_effective_s2",
    "layer1_run_duration_seconds_effective": "L1_run_duration_seconds_effective_s2",
    "layer1_run_timing_source": "L1_run_timing_source_s2",
    "style_instru_job_count": "style_instru_job_count_s2",
    "style_instru_job_names": "style_instru_job_names_s2",
    "style_first_instru_job_name": "style_first_instru_job_name_s2",
    "style_first_instru_job_started_at": "style_first_instru_job_started_at_s2",
    "style_first_instru_job_source": "style_first_instru_job_source_s2",
    "style_last_instru_job_name": "style_last_instru_job_name_s2",
    "style_last_instru_job_completed_at": "style_last_instru_job_completed_at_s2",
    "style_last_instru_job_source": "style_last_instru_job_source_s2",
    "style_time_to_instrumentation_envelope_seconds": "style_time_to_instrumentation_envelope_seconds_s2",
    "style_instrumentation_job_envelope_seconds": "style_instrumentation_job_envelope_seconds_s2",
    "style_post_instrumentation_tail_seconds": "style_post_instrumentation_tail_seconds_s2",
    "style_layer1_model": "style_layer1_model_s2",
    "style_distinct_job_count": "style_distinct_job_count_s2",
    "style_distinct_job_base_name_count": "style_distinct_job_base_name_count_s2",
    "style_matrix_like_job_count": "style_matrix_like_job_count_s2",
    "style_matrix_expanded_flag": "style_matrix_expanded_flag_s2",
    "style_parallel_same_style_flag": "style_parallel_same_style_flag_s2",
    "style_max_parallel_jobs": "style_max_parallel_jobs_s2",
    "style_repeated_same_style_flag": "style_repeated_same_style_flag_s2",
})

s3 = s3.rename(columns={
    "workflow_path": "workflow_path_s3",
    "run_attempt": "run_attempt_s3",
    "status": "status_s3",
    "run_conclusion": "run_conclusion_s3",
    "event": "event_s3",
    "style_distinct_job_count": "style_distinct_job_count_s3",
    "style_distinct_job_base_name_count": "style_distinct_job_base_name_count_s3",
    "style_matrix_like_job_count": "style_matrix_like_job_count_s3",
    "style_matrix_expanded_flag": "style_matrix_expanded_flag_s3",
    "style_parallel_same_style_flag": "style_parallel_same_style_flag_s3",
    "style_max_parallel_jobs": "style_max_parallel_jobs_s3",
    "style_repeated_same_style_flag": "style_repeated_same_style_flag_s3",
})

s4 = s4.rename(columns={
    "workflow_identifier": "workflow_identifier_s4",
    "workflow_path": "workflow_path_s4",
    "head_sha": "head_sha_s4",
})

s1_exact = s1_exact.rename(columns={
    "workflow_identifier": "workflow_identifier_s1_exact",
    "workflow_id": "workflow_id_s1_exact",
    "workflow_path": "workflow_path_s1_exact",
    "styles": "styles_s1_exact",
    "invocation_types": "invocation_types_s1_exact",
    "third_party_provider_name": "third_party_provider_name_s1_exact",
})

s1_by_ident = s1_by_ident.rename(columns={
    "workflow_identifier": "workflow_identifier_s1_ident",
    "workflow_id": "workflow_id_s1_ident",
    "workflow_path": "workflow_path_s1_ident",
    "styles": "styles_s1_ident",
    "invocation_types": "invocation_types_s1_ident",
    "third_party_provider_name": "third_party_provider_name_s1_ident",
})

s1_by_wfid = s1_by_wfid.rename(columns={
    "workflow_identifier": "workflow_identifier_s1_wfid",
    "workflow_id": "workflow_id_s1_wfid",
    "workflow_path": "workflow_path_s1_wfid",
    "styles": "styles_s1_wfid",
    "invocation_types": "invocation_types_s1_wfid",
    "third_party_provider_name": "third_party_provider_name_s1_wfid",
})

# ----------------------------
# Merge Stage 3 + Stage 2 + Stage 4
# ----------------------------
df = s3.merge(s2, on=["full_name", "run_id", "target_style"], how="left")
df = df.merge(s4, on=["full_name", "run_id"], how="left")
df = df.copy()

# ----------------------------
# Rebuild canonical identity fields BEFORE Stage 1 fallback merges
# ----------------------------
df = add_cols(df, {
    "workflow_id": coalesce_cols(df, ["workflow_id_s2"], dtype="object"),
    "workflow_identifier": coalesce_cols(df, ["workflow_identifier_s2", "workflow_identifier_s4"], dtype="object"),
    "workflow_path": coalesce_cols(df, ["workflow_path_s3", "workflow_path_s2", "workflow_path_s4"], dtype="object"),
    "head_sha": coalesce_cols(df, ["head_sha_s2", "head_sha_s4"], dtype="object"),
    "html_url": coalesce_cols(df, ["html_url_s2"], dtype="object"),
})
df = df.copy()

# ----------------------------
# Stage 1 fallback merges
# ----------------------------
if not s1_exact.empty:
    df = df.merge(
        s1_exact,
        left_on=["full_name", "workflow_identifier", "workflow_id"],
        right_on=["full_name", "workflow_identifier_s1_exact", "workflow_id_s1_exact"],
        how="left",
    )

if not s1_by_ident.empty:
    df = df.merge(
        s1_by_ident,
        left_on=["full_name", "workflow_identifier"],
        right_on=["full_name", "workflow_identifier_s1_ident"],
        how="left",
    )

if not s1_by_wfid.empty:
    df = df.merge(
        s1_by_wfid,
        left_on=["full_name", "workflow_id"],
        right_on=["full_name", "workflow_id_s1_wfid"],
        how="left",
    )

df = df.copy()

# ----------------------------
# Metadata + controller fields
# ----------------------------
meta_cols = {
    "styles": coalesce_cols(
        df,
        ["inferred_styles_all", "styles_in_run_all_s2", "styles_s1_exact", "styles_s1_ident", "styles_s1_wfid"],
        dtype="object",
    ).map(canon_style_list),
    "invocation_types": coalesce_cols(
        df,
        ["invocation_types_s1_exact", "invocation_types_s1_ident", "invocation_types_s1_wfid"],
        dtype="object"
    ),
    "third_party_provider_name": coalesce_cols(
        df,
        ["third_party_provider_name_s1_exact", "third_party_provider_name_s1_ident", "third_party_provider_name_s1_wfid"],
        dtype="object"
    ),
    "workflow_id": coalesce_cols(
        df,
        ["workflow_id", "workflow_id_s1_exact", "workflow_id_s1_ident", "workflow_id_s1_wfid"],
        dtype="object"
    ),
    "workflow_identifier": coalesce_cols(
        df,
        ["workflow_identifier", "workflow_identifier_s1_exact", "workflow_identifier_s1_ident", "workflow_identifier_s1_wfid"],
        dtype="object"
    ),
    "workflow_path": coalesce_cols(
        df,
        ["workflow_path", "workflow_path_s1_exact", "workflow_path_s1_ident", "workflow_path_s1_wfid"],
        dtype="object"
    ),
    "style": safe_series(df, "target_style", dtype="object"),
}
df = add_cols(df, meta_cols)
if "target_style" in df.columns:
    df["target_style"] = df["target_style"].map(canon_style_token)

df = add_cols(df, {
    "run_number": safe_series(df, "run_number_s2"),
    "run_attempt": coalesce_cols(df, ["run_attempt_s3", "run_attempt_s2"]),
    "status": coalesce_cols(df, ["status_s3", "status_s2"], dtype="object"),
    "run_conclusion": coalesce_cols(df, ["run_conclusion_s3", "run_conclusion_s2"], dtype="object"),
    "event": coalesce_cols(df, ["event_s3", "event_s2"], dtype="object"),
    "created_at": safe_series(df, "created_at_s2", dtype="object"),
    "run_started_at": safe_series(df, "run_started_at_s2", dtype="object"),
    "run_updated_at": safe_series(df, "run_updated_at_s2", dtype="object"),
    "head_branch": safe_series(df, "head_branch_s2", dtype="object"),
    "multi_style_run_flag": safe_series(df, "multi_style_run_flag_s2"),
})
df = df.copy()

# ----------------------------
# Exclude Real-Device
# ----------------------------
exclude_mask = safe_series(df, "target_style", dtype="object").eq("Real-Device")
excluded_count = int(exclude_mask.fillna(False).sum())
df = df.loc[~exclude_mask.fillna(False)].copy()

print("Excluded Real-Device rows:", excluded_count)
print("Shape after Real-Device exclusion:", df.shape)

dup_count = int(df.duplicated(subset=["full_name", "run_id", "style"], keep=False).sum())
print("Duplicate rows on (full_name, run_id, style):", dup_count)

# ----------------------------
# Controller regime
# ----------------------------
controller_style_in_scope = [
    in_style_scope(styles_csv, tgt)
    for styles_csv, tgt in zip(
        safe_series(df, "styles", dtype="object"),
        safe_series(df, "target_style", dtype="object")
    )
]
instru_job_count = to_num(safe_series(df, "style_instru_job_count_s2"))
controller_instru_job_count_gt0 = instru_job_count.fillna(0).gt(0)
controller_attempt_eq_1 = to_num(safe_series(df, "run_attempt")).eq(1)

terminal_conclusions = {
    "success", "failure", "cancelled", "timed_out",
    "neutral", "skipped", "startup_failure", "action_required"
}

usable_conclusions = {"success", "failure"}

controller_run_verdict_complete = (
    safe_series(df, "run_conclusion", dtype="object")
    .isin(terminal_conclusions)
)

controller_usable_verdict = (
    safe_series(df, "run_conclusion", dtype="object")
    .isin(usable_conclusions)
)

df = add_cols(df, {
    "controller_style_in_scope": pd.Series(controller_style_in_scope, index=df.index),
    "instru_job_count": instru_job_count,
    "controller_instru_job_count_gt0": controller_instru_job_count_gt0,
    "controller_attempt_eq_1": controller_attempt_eq_1,
    "controller_run_verdict_complete": controller_run_verdict_complete,
    "controller_usable_verdict": controller_usable_verdict,
})
df = df.copy()

# ----------------------------
# Layer 1
# ----------------------------
df = add_cols(df, {
    "study_run_duration_seconds": safe_series(df, "L1_run_duration_seconds_effective_s2"),
    "study_run_duration_source": safe_series(df, "L1_run_timing_source_s2", dtype="object"),
    "study_layer1_time_to_instrumentation_envelope_seconds": safe_series(df, "style_time_to_instrumentation_envelope_seconds_s2"),
    "study_layer1_instrumentation_job_envelope_seconds": safe_series(df, "style_instrumentation_job_envelope_seconds_s2"),
    "study_layer1_post_instrumentation_tail_seconds": safe_series(df, "style_post_instrumentation_tail_seconds_s2"),
    "study_layer1_model": safe_series(df, "style_layer1_model_s2", dtype="object"),
})
df = df.copy()

# ----------------------------
# Stage 2 style structural auxiliaries
# ----------------------------
stage2_aux = {}
for src, dst in [
    ("style_distinct_job_count_s2", "study_style_distinct_job_count"),
    ("style_distinct_job_base_name_count_s2", "study_style_distinct_job_base_name_count"),
    ("style_matrix_like_job_count_s2", "study_style_matrix_like_job_count"),
    ("style_matrix_expanded_flag_s2", "study_style_matrix_expanded_flag"),
    ("style_parallel_same_style_flag_s2", "study_style_parallel_same_style_flag"),
    ("style_max_parallel_jobs_s2", "study_style_max_parallel_jobs"),
    ("style_repeated_same_style_flag_s2", "study_style_repeated_same_style_flag"),
]:
    stage2_aux[dst] = safe_series(df, src, dtype="object")
df = add_cols(df, stage2_aux)
df = df.copy()

# ----------------------------
# Layer 2
# ----------------------------
pre_direct = safe_series(df, "pre_invocation_seconds")
exec_direct = safe_series(df, "invocation_execution_window_seconds")
post_direct = safe_series(df, "post_invocation_seconds")

df = add_cols(df, {
    "study_pre_invocation_direct_seconds": pre_direct,
    "study_pre_invocation_direct_source": source_col(pre_direct.notna(), "measured_step_telemetry"),
    "study_invocation_execution_window_direct_seconds": exec_direct,
    "study_invocation_execution_window_direct_source": source_col(exec_direct.notna(), "measured_step_telemetry"),
    "study_post_invocation_direct_seconds": post_direct,
    "study_post_invocation_direct_source": source_col(post_direct.notna(), "measured_step_telemetry"),
    "study_pre_invocation_selected_stage3_seconds": pre_direct,
    "study_pre_invocation_selected_stage3_source": source_col(pre_direct.notna(), "measured_step_telemetry"),
    "study_invocation_execution_window_selected_stage3_seconds": exec_direct,
    "study_invocation_execution_window_selected_stage3_source": source_col(exec_direct.notna(), "measured_step_telemetry"),
    "study_post_invocation_selected_stage3_seconds": post_direct,
    "study_post_invocation_selected_stage3_source": source_col(post_direct.notna(), "measured_step_telemetry"),
    "study_layer2_measurement_mode": safe_series(df, "layer2_measurement_mode", dtype="object"),
    "study_layer2_measurement_quality": safe_series(df, "layer2_measurement_quality", dtype="object"),
})
df = df.copy()

# ----------------------------
# Stage 3 auxiliaries
# ----------------------------
stage3_aux = {}
for src, dst in [
    ("invocation_candidate_count_total", "study_invocation_candidate_count_total"),
    ("stage1_anchor_candidate_count", "study_stage1_anchor_candidate_count"),
    ("explicit_instru_candidate_count", "study_explicit_instru_candidate_count"),
    ("custom_supported_candidate_count", "study_custom_supported_candidate_count"),
    ("distinct_invocation_candidate_step_name_count", "study_distinct_invocation_candidate_step_name_count"),
    ("distinct_invocation_candidate_job_count", "study_distinct_invocation_candidate_job_count"),
    ("invocation_candidate_step_names", "study_invocation_candidate_step_names"),
    ("invocation_candidate_job_names", "study_invocation_candidate_job_names"),
    ("selected_invocation_priority_source", "study_selected_invocation_priority_source"),
    ("execution_window_candidate_count", "study_execution_window_candidate_count"),
    ("execution_window_distinct_job_count", "study_execution_window_distinct_job_count"),
    ("execution_window_candidate_job_names", "study_execution_window_candidate_job_names"),
    ("cross_job_execution_window_flag", "study_cross_job_execution_window_flag"),
]:
    stage3_aux[dst] = safe_series(df, src, dtype="object")

for dst, cols in [
    ("study_style_distinct_job_count", ["style_distinct_job_count_s3", "study_style_distinct_job_count"]),
    ("study_style_distinct_job_base_name_count", ["style_distinct_job_base_name_count_s3", "study_style_distinct_job_base_name_count"]),
    ("study_style_matrix_like_job_count", ["style_matrix_like_job_count_s3", "study_style_matrix_like_job_count"]),
    ("study_style_matrix_expanded_flag", ["style_matrix_expanded_flag_s3", "study_style_matrix_expanded_flag"]),
    ("study_style_parallel_same_style_flag", ["style_parallel_same_style_flag_s3", "study_style_parallel_same_style_flag"]),
    ("study_style_max_parallel_jobs", ["style_max_parallel_jobs_s3", "study_style_max_parallel_jobs"]),
    ("study_style_repeated_same_style_flag", ["style_repeated_same_style_flag_s3", "study_style_repeated_same_style_flag"]),
]:
    stage3_aux[dst] = coalesce_cols(df, cols, dtype="object")

df = add_cols(df, stage3_aux)
df = df.copy()

# ----------------------------
# Cutpoints and timing diagnostics
# ----------------------------
cutpoint_cols = {
    "study_run_boundary_start_at": safe_series(df, "run_boundary_start_at", dtype="object"),
    "study_run_boundary_end_at": safe_series(df, "run_boundary_end_at", dtype="object"),
    "study_matched_invocation_step_name": safe_series(df, "matched_invocation_step_name", dtype="object"),
    "study_matched_invocation_job_name": safe_series(df, "matched_invocation_job_name", dtype="object"),
    "study_matched_invocation_source": safe_series(df, "matched_invocation_source", dtype="object"),
    "study_matched_invocation_step_started_at": safe_series(df, "matched_invocation_step_started_at", dtype="object"),
    "study_matched_invocation_step_completed_at": safe_series(df, "matched_invocation_step_completed_at", dtype="object"),
    "study_matched_invocation_job_ordinal_in_run": safe_series(df, "matched_invocation_job_ordinal_in_run", dtype="object"),
    "study_matched_invocation_step_ordinal_in_job": safe_series(df, "matched_invocation_step_ordinal_in_job", dtype="object"),
    "study_invocation_execution_end_step_name": safe_series(df, "invocation_execution_end_step_name", dtype="object"),
    "study_invocation_execution_end_job_name": safe_series(df, "invocation_execution_end_job_name", dtype="object"),
    "study_invocation_execution_end_source": safe_series(df, "invocation_execution_end_source", dtype="object"),
    "study_invocation_execution_end_step_started_at": safe_series(df, "invocation_execution_end_step_started_at", dtype="object"),
    "study_invocation_execution_end_step_completed_at": safe_series(df, "invocation_execution_end_step_completed_at", dtype="object"),
    "study_invocation_execution_end_job_ordinal_in_run": safe_series(df, "invocation_execution_end_job_ordinal_in_run", dtype="object"),
    "study_invocation_execution_end_step_ordinal_in_job": safe_series(df, "invocation_execution_end_step_ordinal_in_job", dtype="object"),
    "study_invocation_execution_window_started_at": safe_series(df, "invocation_execution_window_started_at", dtype="object"),
    "study_invocation_execution_window_ended_at": safe_series(df, "invocation_execution_window_ended_at", dtype="object"),
}
df = add_cols(df, cutpoint_cols)
df = df.copy()

group_cols = {}
for src, dst in [
    ("setup_sum_seconds", "study_setup_sum_seconds"),
    ("provision_sum_seconds", "study_provision_sum_seconds"),
    ("test_sum_seconds", "study_test_sum_seconds"),
    ("artifact_report_sum_seconds", "study_artifact_report_sum_seconds"),
    ("cleanup_teardown_sum_seconds", "study_cleanup_teardown_sum_seconds"),
    ("other_sum_seconds", "study_other_sum_seconds"),
    ("execution_related_sum_seconds", "study_execution_related_sum_seconds"),
    ("non_execution_overhead_sum_seconds", "study_non_execution_overhead_sum_seconds"),
    ("pre_test_overhead_sum_seconds", "study_pre_test_overhead_sum_seconds"),
    ("active_test_sum_seconds", "study_active_test_sum_seconds"),
    ("post_test_overhead_sum_seconds", "study_post_test_overhead_sum_seconds"),
    ("setup_step_count", "study_setup_step_count"),
    ("provision_step_count", "study_provision_step_count"),
    ("test_step_count", "study_test_step_count"),
    ("artifact_report_step_count", "study_artifact_report_step_count"),
    ("cleanup_teardown_step_count", "study_cleanup_teardown_step_count"),
    ("other_step_count", "study_other_step_count"),
    ("execution_related_step_count", "study_execution_related_step_count"),
    ("non_execution_overhead_step_count", "study_non_execution_overhead_step_count"),
    ("pre_test_overhead_step_count", "study_pre_test_overhead_step_count"),
    ("active_test_step_count", "study_active_test_step_count"),
    ("post_test_overhead_step_count", "study_post_test_overhead_step_count"),
]:
    if src in df.columns:
        group_cols[dst] = safe_series(df, src)
df = add_cols(df, group_cols)
df = df.copy()

run_start_dt = pd.to_datetime(df["study_run_boundary_start_at"], errors="coerce")
run_end_dt = pd.to_datetime(df["study_run_boundary_end_at"], errors="coerce")
inv_start_dt = pd.to_datetime(df["study_matched_invocation_step_started_at"], errors="coerce")
exec_end_dt = pd.to_datetime(df["study_invocation_execution_end_step_completed_at"], errors="coerce")

pre_diff = (inv_start_dt - run_start_dt).dt.total_seconds() - to_num(df["study_pre_invocation_selected_stage3_seconds"])
exec_diff = (exec_end_dt - inv_start_dt).dt.total_seconds() - to_num(df["study_invocation_execution_window_selected_stage3_seconds"])
post_diff = (run_end_dt - exec_end_dt).dt.total_seconds() - to_num(df["study_post_invocation_selected_stage3_seconds"])

for s in [pre_diff, exec_diff, post_diff]:
    s.loc[s.between(-1e-9, 1e-9, inclusive="both")] = 0.0

have_cutpoints = run_start_dt.notna() & inv_start_dt.notna() & exec_end_dt.notna() & run_end_dt.notna()
cutpoints_ok = pre_diff.abs().fillna(np.inf).le(1e-9) & exec_diff.abs().fillna(np.inf).le(1e-9) & post_diff.abs().fillna(np.inf).le(1e-9)
temporal_ok = (run_start_dt <= inv_start_dt) & (inv_start_dt <= exec_end_dt) & (exec_end_dt <= run_end_dt)

cutpoint_flag = pd.Series("missing", index=df.index, dtype="object")
cutpoint_flag.loc[have_cutpoints] = "mismatch"
cutpoint_flag.loc[have_cutpoints & cutpoints_ok] = "ok"

temporal_flag = pd.Series("missing", index=df.index, dtype="object")
temporal_flag.loc[have_cutpoints] = "mismatch"
temporal_flag.loc[have_cutpoints & temporal_ok] = "ok"

df = add_cols(df, {
    "study_cutpoint_pre_invocation_diff_seconds": pre_diff,
    "study_cutpoint_execution_window_diff_seconds": exec_diff,
    "study_cutpoint_post_invocation_diff_seconds": post_diff,
    "study_cutpoint_consistency_flag": cutpoint_flag,
    "study_temporal_order_flag": temporal_flag,
})
df = df.copy()

# ----------------------------
# Signature / workload fields
# ----------------------------
df = add_cols(df, {
    "study_effective_ref_for_stage4": safe_series(df, "effective_ref_for_stage4", dtype="object"),
    "study_signature_inputs": safe_series(df, "signature_inputs", dtype="object"),
    "study_signature_hash": safe_series(df, "signature_hash_base_exec", dtype="object"),
    "study_runner_os_bucket": safe_series(df, "runner_os_bucket", dtype="object"),
    "study_runner_os_source": safe_series(df, "runner_os_source", dtype="object"),
    "study_job_count_exec": safe_series(df, "job_count_exec"),
    "study_job_count_exec_bucket": safe_series(df, "job_count_exec_bucket", dtype="object"),
    "study_step_count_exec": safe_series(df, "step_count_exec"),
    "study_step_count_exec_bucket": safe_series(df, "step_count_exec_bucket", dtype="object"),
})
df = df.copy()

# ----------------------------
# Add study-facing regime flags
# ----------------------------
base_timing_regime = (
    bool_from_series(df["controller_attempt_eq_1"]) &
    bool_from_series(df["controller_usable_verdict"])
)

# Step telemetry availability for the run×style record:
# defined ONLY by presence of the three Stage 3 timing components.
step_telemetry = (
    df["study_pre_invocation_selected_stage3_seconds"].notna() &
    df["study_invocation_execution_window_selected_stage3_seconds"].notna() &
    df["study_post_invocation_selected_stage3_seconds"].notna()
)

# Base-regime subset of the above coverage
layer2_available_in_base = (
    base_timing_regime &
    step_telemetry
)

df = add_cols(df, {
    "Base_timing_regime": base_timing_regime,
    "Layer2_available_in_base": layer2_available_in_base,
    "Step_telemetry": step_telemetry,
})
df = df.copy()

# ----------------------------
# Remove likely Stage 3 false positives
# ----------------------------
suspicious_false_positive = (
    pd.to_numeric(df["study_invocation_execution_window_direct_seconds"], errors="coerce").fillna(-1).eq(0) &
    df["study_matched_invocation_source"].astype(str).eq("stage1_anchor_match") &
    df["study_invocation_execution_end_source"].astype(str).eq("invocation_step_terminal") &
    pd.to_numeric(df["study_explicit_instru_candidate_count"], errors="coerce").fillna(0).eq(0) &
    pd.to_numeric(df["study_execution_window_candidate_count"], errors="coerce").fillna(0).eq(0)
)

print("Rows removed as likely Stage 3 false positives:", int(suspicious_false_positive.sum()))
df = df.loc[~suspicious_false_positive].copy()

# ----------------------------
# Final output
# ----------------------------
final_cols = [
    "full_name","run_id","workflow_id","workflow_identifier","workflow_path","workflow_ref","html_url",
    "run_number","run_attempt","status","run_conclusion","event","head_branch","head_sha",
    "style","styles","target_style","multi_style_run_flag","invocation_types","third_party_provider_name",
    "instru_job_count","controller_style_in_scope","controller_instru_job_count_gt0",
    "controller_attempt_eq_1","controller_run_verdict_complete","controller_usable_verdict",
    "created_at","run_started_at","run_updated_at",

    "study_run_duration_seconds","study_run_duration_source",
    "study_layer1_time_to_instrumentation_envelope_seconds",
    "study_layer1_instrumentation_job_envelope_seconds",
    "study_layer1_post_instrumentation_tail_seconds","study_layer1_model",

    "study_style_distinct_job_count","study_style_distinct_job_base_name_count",
    "study_style_matrix_like_job_count","study_style_matrix_expanded_flag",
    "study_style_parallel_same_style_flag","study_style_max_parallel_jobs",
    "study_style_repeated_same_style_flag",

    "study_pre_invocation_direct_seconds",
    "study_pre_invocation_direct_source","study_invocation_execution_window_direct_seconds",
    "study_invocation_execution_window_direct_source","study_post_invocation_direct_seconds",
    "study_post_invocation_direct_source","study_pre_invocation_selected_stage3_seconds",
    "study_pre_invocation_selected_stage3_source",
    "study_invocation_execution_window_selected_stage3_seconds",
    "study_invocation_execution_window_selected_stage3_source",
    "study_post_invocation_selected_stage3_seconds","study_post_invocation_selected_stage3_source",
    "study_layer2_measurement_mode","study_layer2_measurement_quality",

    "study_invocation_candidate_count_total","study_stage1_anchor_candidate_count",
    "study_explicit_instru_candidate_count","study_custom_supported_candidate_count",
    "study_distinct_invocation_candidate_step_name_count",
    "study_distinct_invocation_candidate_job_count","study_invocation_candidate_step_names",
    "study_invocation_candidate_job_names","study_selected_invocation_priority_source",
    "study_execution_window_candidate_count","study_execution_window_distinct_job_count",
    "study_execution_window_candidate_job_names","study_cross_job_execution_window_flag",

    "study_run_boundary_start_at","study_run_boundary_end_at","study_matched_invocation_step_name",
    "study_matched_invocation_job_name","study_matched_invocation_source",
    "study_matched_invocation_step_started_at","study_matched_invocation_step_completed_at",
    "study_matched_invocation_job_ordinal_in_run","study_matched_invocation_step_ordinal_in_job",
    "study_invocation_execution_end_step_name","study_invocation_execution_end_job_name",
    "study_invocation_execution_end_source","study_invocation_execution_end_step_started_at",
    "study_invocation_execution_end_step_completed_at","study_invocation_execution_end_job_ordinal_in_run",
    "study_invocation_execution_end_step_ordinal_in_job","study_invocation_execution_window_started_at",
    "study_invocation_execution_window_ended_at","study_cutpoint_pre_invocation_diff_seconds",
    "study_cutpoint_execution_window_diff_seconds","study_cutpoint_post_invocation_diff_seconds",
    "study_cutpoint_consistency_flag","study_temporal_order_flag",

    "study_setup_sum_seconds","study_provision_sum_seconds","study_test_sum_seconds",
    "study_artifact_report_sum_seconds","study_cleanup_teardown_sum_seconds","study_other_sum_seconds",
    "study_execution_related_sum_seconds","study_non_execution_overhead_sum_seconds",
    "study_pre_test_overhead_sum_seconds","study_active_test_sum_seconds",
    "study_post_test_overhead_sum_seconds","study_setup_step_count",
    "study_provision_step_count","study_test_step_count","study_artifact_report_step_count",
    "study_cleanup_teardown_step_count","study_other_step_count","study_execution_related_step_count",
    "study_non_execution_overhead_step_count","study_pre_test_overhead_step_count",
    "study_active_test_step_count","study_post_test_overhead_step_count",

    "study_effective_ref_for_stage4","study_signature_inputs",
    "study_signature_hash",
    "study_runner_os_bucket","study_runner_os_source",
    "study_job_count_exec","study_job_count_exec_bucket",
    "study_step_count_exec","study_step_count_exec_bucket",

    # clear regime/coverage flags at the end
    "Base_timing_regime","Layer2_available_in_base","Step_telemetry",
]

main_df = df[[c for c in final_cols if c in df.columns]].copy()
main_df = main_df.drop_duplicates(subset=["full_name", "run_id", "style"], keep="first").copy()
main_df = main_df.loc[:, ~pd.Index(main_df.columns).duplicated(keep="first")].copy()

main_df.to_csv(OUT_MAIN, index=False)

print("\nSaved:", OUT_MAIN)
print("MainDataset shape:", main_df.shape)
print("Unique run×style rows:", main_df[["full_name", "run_id", "style"]].drop_duplicates().shape[0])
print("Base_timing_regime True:", int(main_df["Base_timing_regime"].fillna(False).sum()))
print("Layer2_available_in_base True:", int(main_df["Layer2_available_in_base"].fillna(False).sum()))
print("Step_telemetry True:", int(main_df["Step_telemetry"].fillna(False).sum()))
print("Non-null study_signature_hash:", int(main_df["study_signature_hash"].notna().sum()))
print("Non-null study_job_count_exec:", int(main_df["study_job_count_exec"].notna().sum()))
print("Non-null study_step_count_exec:", int(main_df["study_step_count_exec"].notna().sum()))

try:
    display(main_df.head(3))
except Exception:
    print(main_df.head(3))

# ===============================
# Eligible signature augmentation
# ===============================

# -*- coding: utf-8 -*-
"""
Add robustness flags directly from MainDataset using the current strict _exec signature.

What this script adds
---------------------
1) eligible_signature
   True only for records whose exact study_signature_hash is eligible
   under the Step 3 thresholds.

2) One individual Tier-2 coarsened-family column per eligible signature:
   coarsened_family__<signature_hash>

   Each such column is True for rows that belong to the Tier-2 family
   defined around that eligible exact signature.

Tier-2 coarsening rule
----------------------
For each eligible exact signature:
- keep runner OS bucket EXACT
- allow job-count bucket EQUAL or ADJACENT
- allow step-count bucket EQUAL or ADJACENT

Adjacency
---------
Job buckets:
- 1 <-> 2_3
- 2_3 <-> 1, 4_6
- 4_6 <-> 2_3, >6
- >6 <-> 4_6

Step buckets:
- <=20 <-> 21_40
- 21_40 <-> <=20, 41_80
- 41_80 <-> 21_40, >80
- >80 <-> 41_80

Input
-----
C:\Android Mobile App\ICST2026_Ext\MainDataset.csv

Output
------
C:\Android Mobile App\ICST2026_Ext\MainDataset.csv
"""

from pathlib import Path
import pandas as pd
import numpy as np
from config.runtime import get_root_dir

# ============================================================
# CONFIG
# ============================================================
BASE_DIR = get_root_dir()
IN_MAIN = BASE_DIR / "MainDataset.csv"
OUT_MAIN = BASE_DIR / "MainDataset.csv"

MIN_SIGNATURE_TOTAL_N = 80
MIN_SIGNATURE_STYLE_N = 15
MIN_SIGNATURE_USABLE_STYLES = 2

STYLE_ORDER_ALL = ["Community", "Third-Party", "GMD", "Custom"]

SIGNATURE_HASH_COL = "study_signature_hash"

COARSE_FAMILY_FIELDS = [
    "study_runner_os_bucket",
    "study_job_count_exec_bucket",
    "study_step_count_exec_bucket",
]

JOB_BUCKET_ADJ = {
    "1": {"1", "2_3"},
    "2_3": {"1", "2_3", "4_6"},
    "4_6": {"2_3", "4_6", ">6"},
    ">6": {"4_6", ">6"},
}

STEP_BUCKET_ADJ = {
    "<=20": {"<=20", "21_40"},
    "21_40": {"<=20", "21_40", "41_80"},
    "41_80": {"21_40", "41_80", ">80"},
    ">80": {"41_80", ">80"},
}

# ============================================================
# HELPERS
# ============================================================
def norm_bool(series: pd.Series) -> pd.Series:
    s = series.copy()
    if pd.api.types.is_bool_dtype(s):
        return s.astype("boolean")
    s = s.astype(str).str.strip().str.lower()
    mapping = {
        "true": True, "false": False,
        "1": True, "0": False,
        "yes": True, "no": False,
        "y": True, "n": False,
    }
    out = s.map(mapping)
    return out.astype("boolean")

def blank_to_na(series: pd.Series) -> pd.Series:
    return series.replace(r"^\s*$", np.nan, regex=True)

def is_known_bucket(x) -> bool:
    return pd.notna(x) and str(x).strip() != "" and str(x).strip().lower() != "unknown"

def job_family_set(bucket: str) -> set:
    b = str(bucket).strip()
    return JOB_BUCKET_ADJ.get(b, {b})

def step_family_set(bucket: str) -> set:
    b = str(bucket).strip()
    return STEP_BUCKET_ADJ.get(b, {b})

def make_family_col_name(sig_hash: str) -> str:
    return f"coarsened_family__{sig_hash}"

# ============================================================
# LOAD
# ============================================================
if not IN_MAIN.exists():
    raise FileNotFoundError(f"MainDataset.csv not found at: {IN_MAIN}")

df = pd.read_csv(IN_MAIN, low_memory=False)

for c in [
    "Base_timing_regime", "Layer2_available_in_base",
    "controller_attempt_eq_1",
    "controller_style_in_scope", "controller_instru_job_count_gt0",
    "eligible_signature",
]:
    if c in df.columns:
        try:
            df[c] = norm_bool(df[c])
        except Exception:
            pass

if "run_attempt" in df.columns:
    df["run_attempt"] = pd.to_numeric(df["run_attempt"], errors="coerce")

required_cols = [SIGNATURE_HASH_COL, "style"] + COARSE_FAMILY_FIELDS
missing_cols = [c for c in required_cols if c not in df.columns]
if missing_cols:
    raise KeyError(f"MainDataset is missing required columns: {missing_cols}")

df[SIGNATURE_HASH_COL] = blank_to_na(df[SIGNATURE_HASH_COL]).astype("object")
for c in COARSE_FAMILY_FIELDS:
    df[c] = blank_to_na(df[c]).astype("object")

# remove old shared family columns if present
drop_old_cols = [
    "Coarsened-family",
    "coarsened_family_anchor_signature",
]
existing_drop = [c for c in drop_old_cols if c in df.columns]
if existing_drop:
    df = df.drop(columns=existing_drop)

# also remove previously generated per-signature family cols to avoid stale columns
old_family_cols = [c for c in df.columns if c.startswith("coarsened_family__")]
if old_family_cols:
    df = df.drop(columns=old_family_cols)

# ============================================================
# SAME POOL SELECTION AS STEP 3
# ============================================================
if "Layer2_available_in_base" in df.columns and df["Layer2_available_in_base"].notna().any():
    signature_pool_mask = df["Layer2_available_in_base"].fillna(False)
    selected_pool_name = "robust"
elif "Base_timing_regime" in df.columns:
    signature_pool_mask = df["Base_timing_regime"].fillna(False)
    selected_pool_name = "controlled_subset"
else:
    raise ValueError(
    "Neither 'Layer2_available_in_base' nor 'Base_timing_regime' is available to define the Step 3 signature pool."
    )

sig_pool = df.loc[
    signature_pool_mask &
    df[SIGNATURE_HASH_COL].notna()
].copy()

# ============================================================
# REPEAT ELIGIBILITY DETECTION DIRECTLY
# ============================================================
sig_counts = (
    sig_pool.groupby([SIGNATURE_HASH_COL, "style"], dropna=False)
    .size()
    .rename("n")
    .reset_index()
)

sig_wide = sig_counts.pivot_table(
    index=SIGNATURE_HASH_COL,
    columns="style",
    values="n",
    fill_value=0
).reset_index()

for s in STYLE_ORDER_ALL:
    if s not in sig_wide.columns:
        sig_wide[s] = 0

sig_wide["total_n"] = sig_wide[STYLE_ORDER_ALL].sum(axis=1)
sig_wide["usable_style_count"] = (sig_wide[STYLE_ORDER_ALL] >= MIN_SIGNATURE_STYLE_N).sum(axis=1)

rep_cols = [SIGNATURE_HASH_COL] + COARSE_FAMILY_FIELDS + [
    c for c in ["study_signature_inputs"] if c in sig_pool.columns
]

sig_meta = (
    sig_pool[rep_cols]
    .drop_duplicates(subset=[SIGNATURE_HASH_COL])
    .copy()
)

sig_candidates = sig_wide.merge(sig_meta, on=SIGNATURE_HASH_COL, how="left")

sig_candidates["eligible"] = (
    (sig_candidates["total_n"] >= MIN_SIGNATURE_TOTAL_N) &
    (sig_candidates["usable_style_count"] >= MIN_SIGNATURE_USABLE_STYLES)
)

eligible_signatures = sig_candidates.loc[sig_candidates["eligible"]].copy()

# ============================================================
# COLUMN 1: eligible_signature
# ============================================================
eligible_signature_set = set(eligible_signatures[SIGNATURE_HASH_COL].astype(str))

df["eligible_signature"] = (
    df[SIGNATURE_HASH_COL].notna() &
    df[SIGNATURE_HASH_COL].astype(str).isin(eligible_signature_set)
).astype(bool)

# ============================================================
# Tier-2: one separate coarsened-family column per eligible signature
# ============================================================
family_specs = []
for _, row in eligible_signatures.iterrows():
    sig_hash = str(row[SIGNATURE_HASH_COL]).strip()
    os_bucket = row["study_runner_os_bucket"]
    job_bucket = row["study_job_count_exec_bucket"]
    step_bucket = row["study_step_count_exec_bucket"]

    if not (is_known_bucket(os_bucket) and is_known_bucket(job_bucket) and is_known_bucket(step_bucket)):
        continue

    family_specs.append({
        "anchor_signature": sig_hash,
        "runner_os_bucket": str(os_bucket).strip(),
        "job_bucket_anchor": str(job_bucket).strip(),
        "step_bucket_anchor": str(step_bucket).strip(),
        "job_family": job_family_set(job_bucket),
        "step_family": step_family_set(step_bucket),
        "col_name": make_family_col_name(sig_hash),
    })

# initialize all family columns to False
for spec in family_specs:
    df[spec["col_name"]] = False

# fill each family column independently
os_vals = df["study_runner_os_bucket"].astype(str).str.strip()
job_vals = df["study_job_count_exec_bucket"].astype(str).str.strip()
step_vals = df["study_step_count_exec_bucket"].astype(str).str.strip()
sig_notna = df[SIGNATURE_HASH_COL].notna()

for spec in family_specs:
    mask = (
        sig_notna &
        (os_vals == spec["runner_os_bucket"]) &
        (job_vals.isin(spec["job_family"])) &
        (step_vals.isin(spec["step_family"]))
    )
    df[spec["col_name"]] = mask.astype(bool)

# optional shared summary boolean: row belongs to at least one coarsened family
family_cols = [spec["col_name"] for spec in family_specs]
if family_cols:
    df["Coarsened-family"] = df[family_cols].any(axis=1).astype(bool)
else:
    df["Coarsened-family"] = False

# consistency check:
# every exact eligible signature row should be included in its own per-signature family column
eligible_not_in_own_family = 0
for spec in family_specs:
    own_exact_mask = df[SIGNATURE_HASH_COL].astype(str).eq(spec["anchor_signature"]) & df["eligible_signature"]
    if spec["col_name"] in df.columns:
        eligible_not_in_own_family += int((own_exact_mask & (~df[spec["col_name"]])).sum())

# ============================================================
# SAVE
# ============================================================
df.to_csv(OUT_MAIN, index=False)

# ============================================================
# OPTIONAL CONSOLE SUMMARY
# ============================================================
print(f"Selected pool: {selected_pool_name}")
print(f"Rows in pool with nonblank strict _exec signature: {len(sig_pool)}")
print(f"Eligible exact signatures: {len(eligible_signatures)}")
print(f"Defined individual coarsened families: {len(family_specs)}")

if not eligible_signatures.empty:
    cols_to_show = [
        SIGNATURE_HASH_COL,
        "total_n",
        "usable_style_count",
        "study_runner_os_bucket",
        "study_job_count_exec_bucket",
        "study_step_count_exec_bucket",
    ]
    if "study_sig_basis_base_exec" in eligible_signatures.columns:
        cols_to_show.append("study_sig_basis_base_exec")

    print("\nEligible signatures:")
    print(eligible_signatures[cols_to_show].to_string(index=False))

if family_specs:
    fam_df = pd.DataFrame([{
        "anchor_signature": f["anchor_signature"],
        "family_column": f["col_name"],
        "runner_os_bucket": f["runner_os_bucket"],
        "job_bucket_anchor": f["job_bucket_anchor"],
        "job_family": ",".join(sorted(f["job_family"])),
        "step_bucket_anchor": f["step_bucket_anchor"],
        "step_family": ",".join(sorted(f["step_family"])),
    } for f in family_specs])

    print("\nIndividual coarsened family definitions:")
    print(fam_df.to_string(index=False))

print(f"\nSaved: {OUT_MAIN}")
print(f"eligible_signature=True rows: {int(df['eligible_signature'].sum())}")
print(f"Coarsened-family=True rows: {int(df['Coarsened-family'].sum())}")
print(f"eligible_signature=True but not in own coarsened family rows: {eligible_not_in_own_family}")

if family_cols:
    print("\nPer-family row counts:")
    for c in family_cols:
        print(f"{c}: {int(df[c].sum())}")
