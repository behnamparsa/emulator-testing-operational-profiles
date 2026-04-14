from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / "data" / "processed" / "MainDataset.csv"
OUT_REPORTS = REPO_ROOT / "outputs" / "reports"
OUT_REPORTS.mkdir(parents=True, exist_ok=True)
STYLE_ORDER = ["Community", "GMD", "Third-Party", "Custom"]

def _truthy(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().isin({"1", "true", "yes", "y"})

def median_or_none(s: pd.Series):
    s = pd.to_numeric(s, errors="coerce").dropna()
    return None if s.empty else float(s.median())

def main() -> None:
    df = pd.read_csv(DATA, low_memory=False)
    df = df[df["style"].isin(STYLE_ORDER)].copy()
    if "Base_timing_regime" in df.columns:
        df = df[_truthy(df["Base_timing_regime"])].copy()
    rows = []
    for style in STYLE_ORDER:
        g = df[df["style"] == style]
        rows.append({"style": style, "n": int(g.shape[0]), "run_median_seconds": median_or_none(g["study_run_duration_seconds"]) if not g.empty else None, "entry_median_seconds": median_or_none(g["study_pre_invocation_selected_stage3_seconds"]) if "study_pre_invocation_selected_stage3_seconds" in g.columns else None, "window_median_seconds": median_or_none(g["study_invocation_execution_window_selected_stage3_seconds"]) if "study_invocation_execution_window_selected_stage3_seconds" in g.columns else None})
    (OUT_REPORTS / "section_v_summary.json").write_text(json.dumps(rows, indent=2), encoding="utf-8")
    (OUT_REPORTS / "latest_refresh_report.md").write_text("# Refresh complete\n\nGenerated refreshed outputs from MainDataset.csv using the current paper-aligned analytical pipeline.\n", encoding="utf-8")

if __name__ == "__main__":
    main()
