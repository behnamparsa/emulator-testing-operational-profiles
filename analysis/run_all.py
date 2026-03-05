from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / 'data' / 'processed' / 'MainDataset.csv'
OUT = REPO_ROOT / 'outputs' / 'reports'
OUT.mkdir(parents=True, exist_ok=True)


def summarize_style(df: pd.DataFrame, value_col: str) -> dict:
    rows = []
    for style, g in df.groupby('style', dropna=False):
        s = pd.to_numeric(g[value_col], errors='coerce').dropna()
        rows.append({
            'style': style,
            'n': int(s.shape[0]),
            'median': None if s.empty else float(s.median()),
            'p95': None if s.empty else float(s.quantile(0.95)),
            'iqr': None if s.empty else float(s.quantile(0.75) - s.quantile(0.25)),
        })
    return {'metric': value_col, 'by_style': rows}


def main() -> None:
    df = pd.read_csv(DATA, low_memory=False)
    emulator = df[df['style'].isin(['Community', 'Custom', 'GMD', 'Third-Party'])].copy()
    controlled = emulator[(emulator['Base'] == True)] if 'Base' in emulator.columns else emulator
    payload = {
        'dataset_rows': int(df.shape[0]),
        'emulator_rows': int(emulator.shape[0]),
        'controlled_rows': int(controlled.shape[0]),
        'run_duration': summarize_style(controlled, 'run_duration_seconds'),
        'ttfts': summarize_style(controlled, 'ttfts_seconds'),
        'window': summarize_style(controlled, 'instru_window_seconds'),
    }
    (OUT / 'analysis_summary.json').write_text(json.dumps(payload, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
