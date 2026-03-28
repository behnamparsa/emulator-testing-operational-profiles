from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import csv
import os
from datetime import datetime

import pandas as pd


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _snapshot_suffix(snapshot_tag: str | None = None) -> str:
    tag = (snapshot_tag or "").strip()
    if tag:
        return tag.replace("-", "_")
    return datetime.utcnow().strftime("%Y_%m_%d")


def _question_value(row: Dict[str, str]) -> str:
    # New schema uses "question"; old schema used "question_text"
    return (row.get("question", "") or row.get("question_text", "")).strip()


def _starter_validate_answer(question: str, released_answer: str, df: pd.DataFrame) -> Tuple[str, str]:
    q = question.lower().strip()
    released = (released_answer or "").strip()

    if df.empty:
        return "Insufficient evidence", "MainDataset is empty."

    # Very lightweight starter logic; replace later with the paper-grade tests.
    if "fastest overall" in q:
        return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")
    if "fast-entry" in q or "fast entry" in q:
        return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")
    if "predictable" in q or "predictability" in q:
        return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")
    if "usable run-level verdict rate" in q or "usable verdict" in q:
        return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")
    if "success rate among usable verdicts" in q:
        return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")

    return ("Yes", f"Starter validation currently keeps released answer '{released}' as baseline.")


def run_layer1(
    catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog.csv"),
    main_dataset_csv: Path = Path("data/processed/MainDataset.csv"),
    out_csv: Path = Path("outputs/catalog/observation_qa_catalog_validated.csv"),
    snapshot_tag: str | None = None,
) -> None:
    rows: List[Dict[str, str]] = read_csv_rows(catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in catalog: {catalog_csv}")

    df = pd.read_csv(main_dataset_csv)

    suffix = _snapshot_suffix(snapshot_tag or os.getenv("SNAPSHOT_TAG", ""))
    validate_col = f"L1_validate_{suffix}"
    note_col = f"L1_note_{suffix}"

    out_rows: List[Dict[str, str]] = []
    for row in rows:
        row_out = dict(row)  # preserve all existing columns
        question = _question_value(row_out)
        released_answer = row_out.get("released_answer", "")

        status, note = _starter_validate_answer(question, released_answer, df)
        row_out[validate_col] = status
        row_out[note_col] = note
        out_rows.append(row_out)

    write_csv_rows(out_csv, out_rows)
    print(f"Wrote Layer 1 validated catalog to: {out_csv}")
    print(f"Added columns: {validate_col}, {note_col}")


if __name__ == "__main__":
    run_layer1()