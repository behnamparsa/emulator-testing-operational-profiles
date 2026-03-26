# ============================================================
# One-cell Jupyter code to build MainDataset.csv
# V18-adjusted for latest uploaded schemas
#
# - Stage 2 input: run_inventory_per_style.csv
# - Stage 3 input: run_per_style_v1_stage3.csv
# - Stage 4 input: run_workload_signature_v3.csv
# - FIXED: style-aware merge on full_name + run_id + target_style
# - FIXED: preserves workflow_identifier and workflow_path
# - FIXED: avoids Stage 1 row explosion
# - Includes controller fields needed for controlled subset filtering
# - Controlled subset is defined only by:
#     run_attempt == 1
#     run verdict complete
# - Excludes Real-Device rows
#
# V18 additions
# - Carries Stage 2 V18 run-level auxiliary fields
# - Carries Stage 2 V18 style-level auxiliary fields
# - Carries Stage 3 V18 auxiliary invocation/window fields
#
# FIX IN THIS VERSION
# - Drops non-study residual timing diagnostics that were causing noise:
#       study_layer2_total_seconds
#       study_grouped_known_sum_seconds
#       study_other_raw_seconds
#       study_other_seconds
#       study_other_source
#       study_window_decomp_sum_seconds
#       study_window_decomp_diff_seconds
#       study_timing_consistency_flag
# - Keeps the actual study-facing Layer 1 / Layer 2 variables and cutpoint checks
# - Robust now requires complete Stage 3 Layer 2 triplet:
#       pre + execution window + post
# ============================================================

from pathlib import Path
import numpy as np
from config.runtime import get_root_dir
import pandas as pd

# ----------------------------
# Paths
# ----------------------------
ROOT_DIR = get_root_dir()

IN_STAGE1 = ROOT_DIR / "verified_workflows_v16.csv"
IN_STAGE2 = ROOT_DIR / "run_inventory_per_style.csv"
IN_STAGE3 = ROOT_DIR / "run_per_style_v1_stage3.csv"
IN_STAGE4 = ROOT_DIR / "run_workload_signature_v3.csv"

OUT_MAIN = ROOT_DIR / "MainDataset.csv"

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

# ----------------------------
# Read inputs
# ----------------------------
print("Reading input files...")
stage1 = pd.read_csv(IN_STAGE1, low_memory=False)
stage2 = pd.read_csv(IN_STAGE2, low_memory=False)
stage3 = pd.read_csv(IN_STAGE3, low_memory=False)
stage4 = pd.read_csv(IN_STAGE4, low_memory=False)

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
    "full_name",
    "run_id",
    "target_style",
    "workflow_identifier",
    "workflow_id",
    "workflow_path",
    "run_number",
    "run_attempt",
    "created_at",
    "run_started_at",
    "run_updated_at",
    "status",
    "run_conclusion",
    "event",
    "head_branch",
    "head_sha",
    "html_url",
    "styles_in_run_all",
    "multi_style_run_flag",

    "layer1_run_started_at_effective",
    "layer1_run_ended_at_effective",
    "layer1_run_duration_seconds_effective",
    "layer1_run_timing_source",

    "style_instru_job_count",
    "style_instru_job_names",
    "style_first_instru_job_name",
    "style_first_instru_job_started_at",
    "style_first_instru_job_source",
    "style_last_instru_job_name",
    "style_last_instru_job_completed_at",
    "style_last_instru_job_source",

    "style_time_to_instrumentation_envelope_seconds",
    "style_instrumentation_job_envelope_seconds",
    "style_post_instrumentation_tail_seconds",
    "style_layer1_model",

    # V18 Stage 2 style auxiliary fields
    "style_distinct_job_count",
    "style_distinct_job_base_name_count",
    "style_matrix_like_job_count",
    "style_matrix_expanded_flag",
    "style_parallel_same_style_flag",
    "style_max_parallel_jobs",
    "style_repeated_same_style_flag",
    "style_invocation_candidate_step_count_proxy",
    "style_distinct_invocation_step_name_count_proxy",
    "style_invocation_candidate_step_names_proxy",
    "style_same_style_complexity_class",
]

stage3_keep = [
    "full_name",
    "workflow_path",
    "workflow_ref",
    "run_id",
    "run_attempt",
    "status",
    "run_conclusion",
    "event",
    "trigger",
    "target_style",
    "inferred_styles_all",

    "layer2_measurement_mode",
    "layer2_measurement_quality",

    "run_boundary_start_at",
    "run_boundary_end_at",

    "matched_invocation_step_name",
    "matched_invocation_job_name",
    "matched_invocation_source",
    "matched_invocation_step_started_at",
    "matched_invocation_step_completed_at",
    "matched_invocation_job_ordinal_in_run",
    "matched_invocation_step_ordinal_in_job",

    "invocation_execution_end_step_name",
    "invocation_execution_end_job_name",
    "invocation_execution_end_source",
    "invocation_execution_end_step_started_at",
    "invocation_execution_end_step_completed_at",
    "invocation_execution_end_job_ordinal_in_run",
    "invocation_execution_end_step_ordinal_in_job",

    "invocation_execution_window_started_at",
    "invocation_execution_window_ended_at",

    "pre_invocation_seconds",
    "invocation_execution_window_seconds",
    "post_invocation_seconds",

    "setup_sum_seconds",
    "provision_sum_seconds",
    "test_sum_seconds",
    "artifact_report_sum_seconds",
    "cleanup_teardown_sum_seconds",
    "other_sum_seconds",
    "execution_related_sum_seconds",
    "non_execution_overhead_sum_seconds",
    "pre_test_overhead_sum_seconds",
    "active_test_sum_seconds",
    "post_test_overhead_sum_seconds",

    "setup_step_count",
    "provision_step_count",
    "test_step_count",
    "artifact_report_step_count",
    "cleanup_teardown_step_count",
    "other_step_count",
    "execution_related_step_count",
    "non_execution_overhead_step_count",
    "pre_test_overhead_step_count",
    "active_test_step_count",
    "post_test_overhead_step_count",

    # V18 Stage 3 auxiliary fields
    "invocation_candidate_count_total",
    "stage1_anchor_candidate_count",
    "explicit_instru_candidate_count",
    "custom_supported_candidate_count",
    "distinct_invocation_candidate_step_name_count",
    "distinct_invocation_candidate_job_count",
    "invocation_candidate_step_names",
    "invocation_candidate_job_names",
    "selected_invocation_priority_source",
    "execution_window_candidate_count",
    "execution_window_distinct_job_count",
    "execution_window_candidate_job_names",
    "cross_job_execution_window_flag",

    # carried Stage 2 style auxiliary fields in Stage 3 output
    "style_distinct_job_count",
    "style_distinct_job_base_name_count",
    "style_matrix_like_job_count",
    "style_matrix_expanded_flag",
    "style_parallel_same_style_flag",
    "style_max_parallel_jobs",
    "style_repeated_same_style_flag",
    "style_invocation_candidate_step_count_proxy",
    "style_distinct_invocation_step_name_count_proxy",
    "style_invocation_candidate_step_names_proxy",
    "style_same_style_complexity_class",
]

stage4_keep = [
    "full_name",
    "run_id",
    "workflow_identifier",
    "workflow_path",
    "head_sha",
    "effective_ref_for_stage4",
    "signature_inputs",
    "runner_os_bucket",
    "runner_os_source",
    "job_count_total",
    "job_count_total_bucket",
    "step_count_exec",
    "step_count_exec_bucket",
    "step_count_decl",
    "step_count_decl_bucket",
    "step_count_total_bucket",
    "step_count_source",
    "junit_cases",
    "junit_source",
    "test_suite_size_bucket",
    "sig_basis_base",
    "signature_hash_base",
    "sig_basis_full",
    "signature_hash_full",
    "signature_hash",
]

stage1_keep = [
    "full_name",
    "workflow_identifier",
    "workflow_id",
    "workflow_path",
    "styles",
    "invocation_types",
    "third_party_provider_name",
]

s2 = stage2[[c for c in stage2_keep if c in stage2.columns]].copy()
s3 = stage3[[c for c in stage3_keep if c in stage3.columns]].copy()
s4 = stage4[[c for c in stage4_keep if c in stage4.columns]].copy()
s1 = stage1[[c for c in stage1_keep if c in stage1.columns]].copy()

# ----------------------------
# Deduplicate Stage 1 safely to avoid row explosion
# ----------------------------
if not s1.empty:
    if "workflow_identifier" in s1.columns:
        s1["workflow_identifier"] = s1["workflow_identifier"].replace("", np.nan)
    if "workflow_id" in s1.columns:
        s1["workflow_id"] = s1["workflow_id"].replace("", np.nan)
    if "workflow_path" in s1.columns:
        s1["workflow_path"] = s1["workflow_path"].replace("", np.nan)

    s1_exact = s1.dropna(subset=["workflow_identifier", "workflow_id"]).copy()
    s1_exact = s1_exact.drop_duplicates(
        subset=["full_name", "workflow_identifier", "workflow_id"], keep="first"
    )

    s1_by_ident = s1.dropna(subset=["workflow_identifier"]).copy()
    s1_by_ident = s1_by_ident.drop_duplicates(
        subset=["full_name", "workflow_identifier"], keep="first"
    )

    s1_by_wfid = s1.dropna(subset=["workflow_id"]).copy()
    s1_by_wfid = s1_by_wfid.drop_duplicates(
        subset=["full_name", "workflow_id"], keep="first"
    )
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

    # V18 Stage 2 style auxiliaries
    "style_distinct_job_count": "style_distinct_job_count_s2",
    "style_distinct_job_base_name_count": "style_distinct_job_base_name_count_s2",
    "style_matrix_like_job_count": "style_matrix_like_job_count_s2",
    "style_matrix_expanded_flag": "style_matrix_expanded_flag_s2",
    "style_parallel_same_style_flag": "style_parallel_same_style_flag_s2",
    "style_max_parallel_jobs": "style_max_parallel_jobs_s2",
    "style_repeated_same_style_flag": "style_repeated_same_style_flag_s2",
    "style_invocation_candidate_step_count_proxy": "style_invocation_candidate_step_count_proxy_s2",
    "style_distinct_invocation_step_name_count_proxy": "style_distinct_invocation_step_name_count_proxy_s2",
    "style_invocation_candidate_step_names_proxy": "style_invocation_candidate_step_names_proxy_s2",
    "style_same_style_complexity_class": "style_same_style_complexity_class_s2",
})

s3 = s3.rename(columns={
    "workflow_path": "workflow_path_s3",
    "run_attempt": "run_attempt_s3",
    "status": "status_s3",
    "run_conclusion": "run_conclusion_s3",
    "event": "event_s3",
    "trigger": "trigger_s3",

    # carried Stage 2 style auxiliaries in Stage 3 output
    "style_distinct_job_count": "style_distinct_job_count_s3",
    "style_distinct_job_base_name_count": "style_distinct_job_base_name_count_s3",
    "style_matrix_like_job_count": "style_matrix_like_job_count_s3",
    "style_matrix_expanded_flag": "style_matrix_expanded_flag_s3",
    "style_parallel_same_style_flag": "style_parallel_same_style_flag_s3",
    "style_max_parallel_jobs": "style_max_parallel_jobs_s3",
    "style_repeated_same_style_flag": "style_repeated_same_style_flag_s3",
    "style_invocation_candidate_step_count_proxy": "style_invocation_candidate_step_count_proxy_s3",
    "style_distinct_invocation_step_name_count_proxy": "style_distinct_invocation_step_name_count_proxy_s3",
    "style_invocation_candidate_step_names_proxy": "style_invocation_candidate_step_names_proxy_s3",
    "style_same_style_complexity_class": "style_same_style_complexity_class_s3",
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
# Merge Stage 3 + Stage 2 (style-aware) + Stage 4
# ----------------------------
df = s3.merge(
    s2,
    on=["full_name", "run_id", "target_style"],
    how="left",
)

df = df.merge(
    s4,
    on=["full_name", "run_id"],
    how="left",
)

# ----------------------------
# Rebuild canonical identity fields BEFORE Stage 1 fallback merges
# ----------------------------
df["workflow_id"] = coalesce_cols(df, ["workflow_id_s2"], dtype="object")
df["workflow_identifier"] = coalesce_cols(df, ["workflow_identifier_s2", "workflow_identifier_s4"], dtype="object")
df["workflow_path"] = coalesce_cols(df, ["workflow_path_s3", "workflow_path_s2", "workflow_path_s4"], dtype="object")
df["head_sha"] = coalesce_cols(df, ["head_sha_s2", "head_sha_s4"], dtype="object")
df["html_url"] = coalesce_cols(df, ["html_url_s2"], dtype="object")

# ----------------------------
# Stage 1 fallback merges WITHOUT explosion
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
# Metadata fallback
# ----------------------------
df["styles"] = coalesce_cols(
    df,
    ["inferred_styles_all", "styles_in_run_all_s2", "styles_s1_exact", "styles_s1_ident", "styles_s1_wfid"],
    dtype="object",
).map(canon_style_list)

df["invocation_types"] = coalesce_cols(
    df,
    ["invocation_types_s1_exact", "invocation_types_s1_ident", "invocation_types_s1_wfid"],
    dtype="object"
)

df["third_party_provider_name"] = coalesce_cols(
    df,
    [
        "third_party_provider_name_s1_exact",
        "third_party_provider_name_s1_ident",
        "third_party_provider_name_s1_wfid",
    ],
    dtype="object"
)

df["workflow_id"] = coalesce_cols(
    df,
    ["workflow_id", "workflow_id_s1_exact", "workflow_id_s1_ident", "workflow_id_s1_wfid"],
    dtype="object"
)

df["workflow_identifier"] = coalesce_cols(
    df,
    ["workflow_identifier", "workflow_identifier_s1_exact", "workflow_identifier_s1_ident", "workflow_identifier_s1_wfid"],
    dtype="object"
)

df["workflow_path"] = coalesce_cols(
    df,
    ["workflow_path", "workflow_path_s1_exact", "workflow_path_s1_ident", "workflow_path_s1_wfid"],
    dtype="object"
)

df["style"] = safe_series(df, "target_style", dtype="object")
if "target_style" in df.columns:
    df["target_style"] = df["target_style"].map(canon_style_token)

# ----------------------------
# Study-facing controller/event fields
# ----------------------------
df["run_number"] = safe_series(df, "run_number_s2")
df["run_attempt"] = coalesce_cols(df, ["run_attempt_s3", "run_attempt_s2"])
df["status"] = coalesce_cols(df, ["status_s3", "status_s2"], dtype="object")
df["run_conclusion"] = coalesce_cols(df, ["run_conclusion_s3", "run_conclusion_s2"], dtype="object")
df["event"] = coalesce_cols(df, ["event_s3", "event_s2"], dtype="object")
df["trigger"] = safe_series(df, "trigger_s3", dtype="object")

df["created_at"] = safe_series(df, "created_at_s2", dtype="object")
df["run_started_at"] = safe_series(df, "run_started_at_s2", dtype="object")
df["run_updated_at"] = safe_series(df, "run_updated_at_s2", dtype="object")
df["head_branch"] = safe_series(df, "head_branch_s2", dtype="object")
df["multi_style_run_flag"] = safe_series(df, "multi_style_run_flag_s2")

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
df["controller_style_in_scope"] = [
    in_style_scope(styles_csv, tgt)
    for styles_csv, tgt in zip(
        safe_series(df, "styles", dtype="object"),
        safe_series(df, "target_style", dtype="object")
    )
]

df["instru_job_count"] = to_num(safe_series(df, "style_instru_job_count_s2"))
df["controller_instru_job_count_gt0"] = df["instru_job_count"].fillna(0).gt(0)
df["controller_attempt_eq_1"] = to_num(safe_series(df, "run_attempt")).eq(1)

terminal_conclusions = {
    "success", "failure", "cancelled", "timed_out",
    "neutral", "skipped", "startup_failure", "action_required"
}

df["controller_run_verdict_complete"] = safe_series(df, "run_conclusion", dtype="object").isin(terminal_conclusions)

df["Base"] = (
    bool_from_series(df["controller_attempt_eq_1"]) &
    bool_from_series(df["controller_run_verdict_complete"])
)

df = df.copy()

# ----------------------------
# Layer 1 (from Stage 2 per-style)
# ----------------------------
df["study_run_duration_seconds"] = safe_series(df, "L1_run_duration_seconds_effective_s2")
df["study_run_duration_source"] = safe_series(df, "L1_run_timing_source_s2", dtype="object")

df["study_layer1_time_to_instrumentation_envelope_seconds"] = safe_series(
    df, "style_time_to_instrumentation_envelope_seconds_s2"
)
df["study_layer1_instrumentation_job_envelope_seconds"] = safe_series(
    df, "style_instrumentation_job_envelope_seconds_s2"
)
df["study_layer1_post_instrumentation_tail_seconds"] = safe_series(
    df, "style_post_instrumentation_tail_seconds_s2"
)
df["study_layer1_model"] = safe_series(df, "style_layer1_model_s2", dtype="object")

# ----------------------------
# V18 Stage 2 run/style auxiliary fields
# ----------------------------
for src, dst in [
    ("instru_distinct_job_count", "study_instru_distinct_job_count"),
    ("instru_distinct_job_base_name_count", "study_instru_distinct_job_base_name_count"),
    ("instru_matrix_like_job_count", "study_instru_matrix_like_job_count"),
    ("instru_matrix_expanded_flag", "study_instru_matrix_expanded_flag"),
    ("instru_parallel_jobs_flag", "study_instru_parallel_jobs_flag"),
    ("instru_max_parallel_jobs", "study_instru_max_parallel_jobs"),
]:
    df[dst] = safe_series(df, src, dtype="object")

for src, dst in [
    ("style_distinct_job_count_s2", "study_style_distinct_job_count"),
    ("style_distinct_job_base_name_count_s2", "study_style_distinct_job_base_name_count"),
    ("style_matrix_like_job_count_s2", "study_style_matrix_like_job_count"),
    ("style_matrix_expanded_flag_s2", "study_style_matrix_expanded_flag"),
    ("style_parallel_same_style_flag_s2", "study_style_parallel_same_style_flag"),
    ("style_max_parallel_jobs_s2", "study_style_max_parallel_jobs"),
    ("style_repeated_same_style_flag_s2", "study_style_repeated_same_style_flag"),
    ("style_invocation_candidate_step_count_proxy_s2", "study_style_invocation_candidate_step_count_proxy"),
    ("style_distinct_invocation_step_name_count_proxy_s2", "study_style_distinct_invocation_step_name_count_proxy"),
    ("style_invocation_candidate_step_names_proxy_s2", "study_style_invocation_candidate_step_names_proxy"),
    ("style_same_style_complexity_class_s2", "study_style_same_style_complexity_class"),
]:
    df[dst] = safe_series(df, src, dtype="object")

# ----------------------------
# Layer 2 direct / selected
# ----------------------------
df["study_pre_invocation_direct_seconds"] = safe_series(df, "pre_invocation_seconds")
df["study_pre_invocation_direct_source"] = source_col(
    df["study_pre_invocation_direct_seconds"].notna(), "measured_step_telemetry"
)

df["study_invocation_execution_window_direct_seconds"] = safe_series(df, "invocation_execution_window_seconds")
df["study_invocation_execution_window_direct_source"] = source_col(
    df["study_invocation_execution_window_direct_seconds"].notna(), "measured_step_telemetry"
)

df["study_post_invocation_direct_seconds"] = safe_series(df, "post_invocation_seconds")
df["study_post_invocation_direct_source"] = source_col(
    df["study_post_invocation_direct_seconds"].notna(), "measured_step_telemetry"
)

df["study_pre_invocation_selected_stage3_seconds"] = df["study_pre_invocation_direct_seconds"]
df["study_pre_invocation_selected_stage3_source"] = df["study_pre_invocation_direct_source"]

df["study_invocation_execution_window_selected_stage3_seconds"] = df["study_invocation_execution_window_direct_seconds"]
df["study_invocation_execution_window_selected_stage3_source"] = df["study_invocation_execution_window_direct_source"]

df["study_post_invocation_selected_stage3_seconds"] = df["study_post_invocation_direct_seconds"]
df["study_post_invocation_selected_stage3_source"] = df["study_post_invocation_direct_source"]

df["study_layer2_measurement_mode"] = safe_series(df, "layer2_measurement_mode", dtype="object")
df["study_layer2_measurement_quality"] = safe_series(df, "layer2_measurement_quality", dtype="object")

# ----------------------------
# V18 Stage 3 auxiliary invocation/window fields
# ----------------------------
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
    df[dst] = safe_series(df, src, dtype="object")

for dst, cols in [
    ("study_style_distinct_job_count", ["style_distinct_job_count_s3", "study_style_distinct_job_count"]),
    ("study_style_distinct_job_base_name_count", ["style_distinct_job_base_name_count_s3", "study_style_distinct_job_base_name_count"]),
    ("study_style_matrix_like_job_count", ["style_matrix_like_job_count_s3", "study_style_matrix_like_job_count"]),
    ("study_style_matrix_expanded_flag", ["style_matrix_expanded_flag_s3", "study_style_matrix_expanded_flag"]),
    ("study_style_parallel_same_style_flag", ["style_parallel_same_style_flag_s3", "study_style_parallel_same_style_flag"]),
    ("study_style_max_parallel_jobs", ["style_max_parallel_jobs_s3", "study_style_max_parallel_jobs"]),
    ("study_style_repeated_same_style_flag", ["style_repeated_same_style_flag_s3", "study_style_repeated_same_style_flag"]),
    ("study_style_invocation_candidate_step_count_proxy", ["style_invocation_candidate_step_count_proxy_s3", "study_style_invocation_candidate_step_count_proxy"]),
    ("study_style_distinct_invocation_step_name_count_proxy", ["style_distinct_invocation_step_name_count_proxy_s3", "study_style_distinct_invocation_step_name_count_proxy"]),
    ("study_style_invocation_candidate_step_names_proxy", ["style_invocation_candidate_step_names_proxy_s3", "study_style_invocation_candidate_step_names_proxy"]),
    ("study_style_same_style_complexity_class", ["style_same_style_complexity_class_s3", "study_style_same_style_complexity_class"]),
]:
    df[dst] = coalesce_cols(df, cols, dtype="object")

# ----------------------------
# Layer 2 cutpoints
# ----------------------------
df["study_run_boundary_start_at"] = safe_series(df, "run_boundary_start_at", dtype="object")
df["study_run_boundary_end_at"] = safe_series(df, "run_boundary_end_at", dtype="object")

df["study_matched_invocation_step_name"] = safe_series(df, "matched_invocation_step_name", dtype="object")
df["study_matched_invocation_job_name"] = safe_series(df, "matched_invocation_job_name", dtype="object")
df["study_matched_invocation_source"] = safe_series(df, "matched_invocation_source", dtype="object")
df["study_matched_invocation_step_started_at"] = safe_series(df, "matched_invocation_step_started_at", dtype="object")
df["study_matched_invocation_step_completed_at"] = safe_series(df, "matched_invocation_step_completed_at", dtype="object")
df["study_matched_invocation_job_ordinal_in_run"] = safe_series(df, "matched_invocation_job_ordinal_in_run", dtype="object")
df["study_matched_invocation_step_ordinal_in_job"] = safe_series(df, "matched_invocation_step_ordinal_in_job", dtype="object")

df["study_invocation_execution_end_step_name"] = safe_series(df, "invocation_execution_end_step_name", dtype="object")
df["study_invocation_execution_end_job_name"] = safe_series(df, "invocation_execution_end_job_name", dtype="object")
df["study_invocation_execution_end_source"] = safe_series(df, "invocation_execution_end_source", dtype="object")
df["study_invocation_execution_end_step_started_at"] = safe_series(df, "invocation_execution_end_step_started_at", dtype="object")
df["study_invocation_execution_end_step_completed_at"] = safe_series(df, "invocation_execution_end_step_completed_at", dtype="object")
df["study_invocation_execution_end_job_ordinal_in_run"] = safe_series(df, "invocation_execution_end_job_ordinal_in_run", dtype="object")
df["study_invocation_execution_end_step_ordinal_in_job"] = safe_series(df, "invocation_execution_end_step_ordinal_in_job", dtype="object")

df["study_invocation_execution_window_started_at"] = safe_series(df, "invocation_execution_window_started_at", dtype="object")
df["study_invocation_execution_window_ended_at"] = safe_series(df, "invocation_execution_window_ended_at", dtype="object")

df = df.copy()

# ----------------------------
# Grouping / decomposition outputs
# ----------------------------
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
        df[dst] = safe_series(df, src)

# ----------------------------
# Automated validation diagnostics from cutpoints
# ----------------------------
run_start_dt = pd.to_datetime(df["study_run_boundary_start_at"], errors="coerce")
run_end_dt = pd.to_datetime(df["study_run_boundary_end_at"], errors="coerce")
inv_start_dt = pd.to_datetime(df["study_matched_invocation_step_started_at"], errors="coerce")
exec_end_dt = pd.to_datetime(df["study_invocation_execution_end_step_completed_at"], errors="coerce")

df["study_cutpoint_pre_invocation_diff_seconds"] = (
    (inv_start_dt - run_start_dt).dt.total_seconds()
    - to_num(df["study_pre_invocation_selected_stage3_seconds"])
)
df["study_cutpoint_execution_window_diff_seconds"] = (
    (exec_end_dt - inv_start_dt).dt.total_seconds()
    - to_num(df["study_invocation_execution_window_selected_stage3_seconds"])
)
df["study_cutpoint_post_invocation_diff_seconds"] = (
    (run_end_dt - exec_end_dt).dt.total_seconds()
    - to_num(df["study_post_invocation_selected_stage3_seconds"])
)

for c in [
    "study_cutpoint_pre_invocation_diff_seconds",
    "study_cutpoint_execution_window_diff_seconds",
    "study_cutpoint_post_invocation_diff_seconds",
]:
    df.loc[df[c].between(-1e-9, 1e-9, inclusive="both"), c] = 0.0

df["study_cutpoint_consistency_flag"] = "missing"
have_cutpoints = (
    run_start_dt.notna() &
    inv_start_dt.notna() &
    exec_end_dt.notna() &
    run_end_dt.notna()
)
cutpoints_ok = (
    df["study_cutpoint_pre_invocation_diff_seconds"].abs().fillna(np.inf).le(1e-9) &
    df["study_cutpoint_execution_window_diff_seconds"].abs().fillna(np.inf).le(1e-9) &
    df["study_cutpoint_post_invocation_diff_seconds"].abs().fillna(np.inf).le(1e-9)
)
df.loc[have_cutpoints, "study_cutpoint_consistency_flag"] = "mismatch"
df.loc[have_cutpoints & cutpoints_ok, "study_cutpoint_consistency_flag"] = "ok"

df["study_temporal_order_flag"] = "missing"
temporal_ok = (
    (run_start_dt <= inv_start_dt) &
    (inv_start_dt <= exec_end_dt) &
    (exec_end_dt <= run_end_dt)
)
df.loc[have_cutpoints, "study_temporal_order_flag"] = "mismatch"
df.loc[have_cutpoints & temporal_ok, "study_temporal_order_flag"] = "ok"

df = df.copy()

# ----------------------------
# Signature / workload fields
# ----------------------------
df["study_effective_ref_for_stage4"] = safe_series(df, "effective_ref_for_stage4", dtype="object")
df["study_signature_inputs"] = safe_series(df, "signature_inputs", dtype="object")
df["study_sig_basis_base"] = safe_series(df, "sig_basis_base", dtype="object")
df["study_signature_hash_base"] = safe_series(df, "signature_hash_base", dtype="object")
df["study_sig_basis_full"] = safe_series(df, "sig_basis_full", dtype="object")
df["study_signature_hash_full"] = safe_series(df, "signature_hash_full", dtype="object")
df["study_signature_hash"] = safe_series(df, "signature_hash", dtype="object")
df["study_runner_os_bucket"] = safe_series(df, "runner_os_bucket", dtype="object")
df["study_runner_os_source"] = safe_series(df, "runner_os_source", dtype="object")
df["study_job_count_total"] = safe_series(df, "job_count_total")
df["study_job_count_total_bucket"] = safe_series(df, "job_count_total_bucket", dtype="object")
df["study_step_count_exec"] = safe_series(df, "step_count_exec")
df["study_step_count_exec_bucket"] = safe_series(df, "step_count_exec_bucket", dtype="object")
df["study_step_count_decl"] = safe_series(df, "step_count_decl")
df["study_step_count_decl_bucket"] = safe_series(df, "step_count_decl_bucket", dtype="object")
df["study_step_count_total_bucket"] = safe_series(df, "step_count_total_bucket", dtype="object")
df["study_step_count_source"] = safe_series(df, "step_count_source", dtype="object")
df["study_junit_cases"] = safe_series(df, "junit_cases")
df["study_junit_source"] = safe_series(df, "junit_source", dtype="object")
df["study_test_suite_size_bucket"] = safe_series(df, "test_suite_size_bucket", dtype="object")

# ----------------------------
# Robust flag
# ----------------------------
df["Robust"] = (
    bool_from_series(df["Base"]) &
    df["study_pre_invocation_selected_stage3_seconds"].notna() &
    df["study_invocation_execution_window_selected_stage3_seconds"].notna() &
    df["study_post_invocation_selected_stage3_seconds"].notna()
)

df = df.copy()

# ----------------------------
# Final column order
# ----------------------------
final_cols = [
    "full_name",
    "run_id",
    "workflow_id",
    "workflow_identifier",
    "workflow_path",
    "workflow_ref",
    "html_url",

    "run_number",
    "run_attempt",
    "status",
    "run_conclusion",
    "event",
    "trigger",

    "head_branch",
    "head_sha",

    "style",
    "styles",
    "target_style",
    "multi_style_run_flag",
    "invocation_types",
    "third_party_provider_name",
    "instru_job_count",

    "controller_style_in_scope",
    "controller_instru_job_count_gt0",
    "controller_attempt_eq_1",
    "controller_run_verdict_complete",
    "Base",
    "Robust",

    "created_at",
    "run_started_at",
    "run_updated_at",

    "study_run_duration_seconds",
    "study_run_duration_source",
    "study_layer1_time_to_instrumentation_envelope_seconds",
    "study_layer1_instrumentation_job_envelope_seconds",
    "study_layer1_post_instrumentation_tail_seconds",
    "study_layer1_model",

    # V18 Stage 2 run/style auxiliaries
    "study_instru_distinct_job_count",
    "study_instru_distinct_job_base_name_count",
    "study_instru_matrix_like_job_count",
    "study_instru_matrix_expanded_flag",
    "study_instru_parallel_jobs_flag",
    "study_instru_max_parallel_jobs",

    "study_style_distinct_job_count",
    "study_style_distinct_job_base_name_count",
    "study_style_matrix_like_job_count",
    "study_style_matrix_expanded_flag",
    "study_style_parallel_same_style_flag",
    "study_style_max_parallel_jobs",
    "study_style_repeated_same_style_flag",
    "study_style_invocation_candidate_step_count_proxy",
    "study_style_distinct_invocation_step_name_count_proxy",
    "study_style_invocation_candidate_step_names_proxy",
    "study_style_same_style_complexity_class",

    "study_pre_invocation_direct_seconds",
    "study_pre_invocation_direct_source",
    "study_invocation_execution_window_direct_seconds",
    "study_invocation_execution_window_direct_source",
    "study_post_invocation_direct_seconds",
    "study_post_invocation_direct_source",
    "study_pre_invocation_selected_stage3_seconds",
    "study_pre_invocation_selected_stage3_source",
    "study_invocation_execution_window_selected_stage3_seconds",
    "study_invocation_execution_window_selected_stage3_source",
    "study_post_invocation_selected_stage3_seconds",
    "study_post_invocation_selected_stage3_source",
    "study_layer2_measurement_mode",
    "study_layer2_measurement_quality",

    # V18 Stage 3 auxiliaries
    "study_invocation_candidate_count_total",
    "study_stage1_anchor_candidate_count",
    "study_explicit_instru_candidate_count",
    "study_custom_supported_candidate_count",
    "study_distinct_invocation_candidate_step_name_count",
    "study_distinct_invocation_candidate_job_count",
    "study_invocation_candidate_step_names",
    "study_invocation_candidate_job_names",
    "study_selected_invocation_priority_source",
    "study_execution_window_candidate_count",
    "study_execution_window_distinct_job_count",
    "study_execution_window_candidate_job_names",
    "study_cross_job_execution_window_flag",

    "study_run_boundary_start_at",
    "study_run_boundary_end_at",
    "study_matched_invocation_step_name",
    "study_matched_invocation_job_name",
    "study_matched_invocation_source",
    "study_matched_invocation_step_started_at",
    "study_matched_invocation_step_completed_at",
    "study_matched_invocation_job_ordinal_in_run",
    "study_matched_invocation_step_ordinal_in_job",
    "study_invocation_execution_end_step_name",
    "study_invocation_execution_end_job_name",
    "study_invocation_execution_end_source",
    "study_invocation_execution_end_step_started_at",
    "study_invocation_execution_end_step_completed_at",
    "study_invocation_execution_end_job_ordinal_in_run",
    "study_invocation_execution_end_step_ordinal_in_job",
    "study_invocation_execution_window_started_at",
    "study_invocation_execution_window_ended_at",

    "study_cutpoint_pre_invocation_diff_seconds",
    "study_cutpoint_execution_window_diff_seconds",
    "study_cutpoint_post_invocation_diff_seconds",
    "study_cutpoint_consistency_flag",
    "study_temporal_order_flag",

    "study_setup_sum_seconds",
    "study_provision_sum_seconds",
    "study_test_sum_seconds",
    "study_artifact_report_sum_seconds",
    "study_cleanup_teardown_sum_seconds",
    "study_other_sum_seconds",
    "study_execution_related_sum_seconds",
    "study_non_execution_overhead_sum_seconds",
    "study_pre_test_overhead_sum_seconds",
    "study_active_test_sum_seconds",
    "study_post_test_overhead_sum_seconds",
    "study_setup_step_count",
    "study_provision_step_count",
    "study_test_step_count",
    "study_artifact_report_step_count",
    "study_cleanup_teardown_step_count",
    "study_other_step_count",
    "study_execution_related_step_count",
    "study_non_execution_overhead_step_count",
    "study_pre_test_overhead_step_count",
    "study_active_test_step_count",
    "study_post_test_overhead_step_count",

    "study_effective_ref_for_stage4",
    "study_signature_inputs",
    "study_sig_basis_base",
    "study_signature_hash_base",
    "study_sig_basis_full",
    "study_signature_hash_full",
    "study_signature_hash",
    "study_runner_os_bucket",
    "study_runner_os_source",
    "study_job_count_total",
    "study_job_count_total_bucket",
    "study_step_count_exec",
    "study_step_count_exec_bucket",
    "study_step_count_decl",
    "study_step_count_decl_bucket",
    "study_step_count_total_bucket",
    "study_step_count_source",
    "study_junit_cases",
    "study_junit_source",
    "study_test_suite_size_bucket",
]

main_df = df[[c for c in final_cols if c in df.columns]].copy()
main_df = main_df.drop_duplicates(subset=["full_name", "run_id", "style"], keep="first").copy()

main_df.to_csv(OUT_MAIN, index=False)

print("\nSaved:", OUT_MAIN)
print("MainDataset shape:", main_df.shape)
print("Unique run×style rows:", main_df[["full_name", "run_id", "style"]].drop_duplicates().shape[0])
print("workflow_identifier non-null:", int(main_df["workflow_identifier"].notna().sum()) if "workflow_identifier" in main_df.columns else 0)
print("workflow_path non-null:", int(main_df["workflow_path"].notna().sum()) if "workflow_path" in main_df.columns else 0)

print("\nStyle counts:")
print(main_df["style"].value_counts(dropna=False))

print("\nBase counts:")
print(main_df["Base"].value_counts(dropna=False))

print("\nRobust counts:")
print(main_df["Robust"].value_counts(dropna=False))

print("\nEvent counts:")
if "event" in main_df.columns:
    print(main_df["event"].value_counts(dropna=False).head(20))

print("\nTrigger counts:")
if "trigger" in main_df.columns:
    print(main_df["trigger"].value_counts(dropna=False).head(20))

v18_check_cols = [
    "study_style_distinct_job_count",
    "study_style_matrix_expanded_flag",
    "study_style_repeated_same_style_flag",
    "study_invocation_candidate_count_total",
    "study_selected_invocation_priority_source",
    "study_cross_job_execution_window_flag",
]
print("\nV18 field presence:")
for c in v18_check_cols:
    print(f"{c}: {'yes' if c in main_df.columns else 'no'}")

try:
    display(main_df.head(3))
except Exception:
    print(main_df.head(3))