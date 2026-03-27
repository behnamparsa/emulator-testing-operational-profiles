"""Starter Layer 1 validator.

This is intentionally a scaffold. It appends dated validation columns to the
observation-question catalog and leaves room for the exact paper-grade test
logic to be implemented observation by observation.
"""
from __future__ import annotations

from pathlib import Path
from typing import List, Dict
import pandas as pd

from .io_utils import read_csv_rows, write_csv_rows, snapshot_tag


def _starter_validate_answer(question: str, released_answer: str, df: pd.DataFrame) -> tuple[str, str]:
    if df.empty:
        return 'Insufficient evidence', 'MainDataset is empty.'

    # Starter deterministic placeholders. Replace RQ by RQ with the same paper logic.
    q = question.lower()
    if 'fastest overall operational profile' in q:
        if 'run_duration_seconds' in df.columns and 'style' in df.columns:
            med = df.groupby('style')['run_duration_seconds'].median(numeric_only=True).sort_values()
            if len(med) > 0:
                leader = str(med.index[0])
                return ('Yes', f'Starter check agrees with released answer ({leader}).') if leader == released_answer else ('No', f'Starter check points to {leader}.')
    if 'highest usable verdict rate' in q:
        needed = {'style', 'run_conclusion'}
        if needed.issubset(df.columns):
            tmp = df.copy()
            tmp['usable'] = tmp['run_conclusion'].isin(['success', 'failure'])
            rates = tmp.groupby('style')['usable'].mean().sort_values(ascending=False)
            if len(rates) > 0:
                leader = str(rates.index[0])
                return ('Yes', f'Starter check agrees with released answer ({leader}).') if leader == released_answer else ('No', f'Starter check points to {leader}.')
    return 'Mixed', 'Starter scaffold does not yet implement the full paper-grade statistical validation for this observation.'


def run_layer1(catalog_csv: Path, main_dataset_csv: Path, out_csv: Path, snapshot: str | None = None) -> Path:
    tag = snapshot_tag(snapshot)
    validate_col = f'L1_validate_{tag}'
    note_col = f'L1_note_{tag}'
    rows: List[Dict[str, str]] = read_csv_rows(catalog_csv)
    df = pd.read_csv(main_dataset_csv)
    for row in rows:
        status, note = _starter_validate_answer(row['question_text'], row['released_answer'], df)
        row[validate_col] = status
        row[note_col] = note
    write_csv_rows(out_csv, rows)
    return out_csv
