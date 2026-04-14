from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / "data" / "processed" / "MainDataset.csv"
OUT = REPO_ROOT / "outputs" / "reports"
OUT.mkdir(parents=True, exist_ok=True)
STYLE_ORDER = ["Community", "GMD", "Third-Party", "Custom"]

def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})

def summarize_style(df: pd.DataFrame, value_col: str) -> dict:
    rows = []
    for style in STYLE_ORDER:
        g = df[df["style"] == style] if "style" in df.columns else pd.DataFrame()
        s = pd.to_numeric(g[value_col], errors="coerce").dropna() if value_col in g.columns else pd.Series(dtype=float)
        rows.append({"style": style, "n": int(s.shape[0]), "median": None if s.empty else float(s.median()), "p95": None if s.empty else float(s.quantile(0.95)), "iqr": None if s.empty else float(s.quantile(0.75) - s.quantile(0.25))})
    return {"metric": value_col, "by_style": rows}

def main() -> None:
    df = pd.read_csv(DATA, low_memory=False)
    emulator = df[df["style"].isin(STYLE_ORDER)].copy()
    base = emulator[_truthy(emulator["Base_timing_regime"])] if "Base_timing_regime" in emulator.columns else emulator
    layer2 = base[_truthy(base["Layer2_available_in_base"])] if "Layer2_available_in_base" in base.columns else base.iloc[0:0].copy()
    first_attempt = emulator[_truthy(emulator["controller_attempt_eq_1"])] if "controller_attempt_eq_1" in emulator.columns else emulator
    payload = {
        "dataset_rows": int(df.shape[0]),
        "emulator_rows": int(emulator.shape[0]),
        "base_timing_rows": int(base.shape[0]),
        "layer2_available_in_base_rows": int(layer2.shape[0]),
        "first_attempt_rows": int(first_attempt.shape[0]),
        "run_duration": summarize_style(base, "study_run_duration_seconds"),
        "layer1_time_to_instr": summarize_style(base, "study_layer1_time_to_instrumentation_envelope_seconds"),
        "layer1_instr_envelope": summarize_style(base, "study_layer1_instrumentation_job_envelope_seconds"),
        "layer2_pre_invocation": summarize_style(layer2, "study_pre_invocation_selected_stage3_seconds"),
        "layer2_execution_window": summarize_style(layer2, "study_invocation_execution_window_selected_stage3_seconds"),
        "layer2_post_invocation": summarize_style(layer2, "study_post_invocation_selected_stage3_seconds"),
    }
    (OUT / "analysis_summary.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

if __name__ == "__main__":
    main()
