from __future__ import annotations

from pathlib import Path
from typing import List, Dict
import pandas as pd

from .io_utils import read_csv_rows, write_csv_rows, snapshot_tag


def _question_value(row: Dict[str, str]) -> str:
    return (row.get("question", "") or row.get("question_text", "")).strip()


def _starter_refresh_answer(question: str, df: pd.DataFrame) -> tuple[str, str]:
    q = question.lower().strip()

    if df.empty:
        return "Insufficient evidence", "MainDataset is empty."

    if "fastest overall operational profile" in q and {"style", "run_duration_seconds"}.issubset(df.columns):
        med = df.groupby("style")["run_duration_seconds"].median(numeric_only=True).sort_values()
        if len(med) > 0:
            return str(med.index[0]), "Starter refresh based on smallest median run duration."

    if "highest usable run-level verdict rate" in q and {"style", "run_conclusion"}.issubset(df.columns):
        tmp = df.copy()
        tmp["usable"] = tmp["run_conclusion"].isin(["success", "failure"])
        rates = tmp.groupby("style")["usable"].mean().sort_values(ascending=False)
        if len(rates) > 0:
            return str(rates.index[0]), "Starter refresh based on highest usable verdict rate."

    if "highest usable verdict rate" in q and {"style", "run_conclusion"}.issubset(df.columns):
        tmp = df.copy()
        tmp["usable"] = tmp["run_conclusion"].isin(["success", "failure"])
        rates = tmp.groupby("style")["usable"].mean().sort_values(ascending=False)
        if len(rates) > 0:
            return str(rates.index[0]), "Starter refresh based on highest usable verdict rate."

    return "Conditional", "Starter scaffold does not yet implement the full paper-grade refresh logic for this observation."


def run_layer2(
    validated_catalog_csv: Path,
    main_dataset_csv: Path,
    out_csv: Path,
    snapshot: str | None = None,
) -> Path:
    tag = snapshot_tag(snapshot)
    answer_col = f"L2_answer_{tag}"
    note_col = f"L2_note_{tag}"

    rows: List[Dict[str, str]] = read_csv_rows(validated_catalog_csv)
    df = pd.read_csv(main_dataset_csv)

    for row in rows:
        question = _question_value(row)
        answer, note = _starter_refresh_answer(question, df)
        row[answer_col] = answer
        row[note_col] = note

    write_csv_rows(out_csv, rows)
    return out_csv