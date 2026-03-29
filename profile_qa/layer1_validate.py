from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import os

import pandas as pd

from .io_utils import snapshot_tag
from .item_logic import validate_stored_answer


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))



def write_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)



def _latest_active_answer(row: Dict[str, str], current_tag: str) -> str:
    candidates = []
    for key, value in row.items():
        value_text = str(value or "").strip()
        if key.startswith("L2_answer_") and value_text and not key.endswith(current_tag):
            candidates.append((key, value_text))
    for key, value_text in sorted(candidates, reverse=True):
        if value_text.lower() not in {"conditional", "insufficient evidence", "", "n/a"}:
            return value_text
    return str(row.get("released_answer", "") or "").strip()



def run_layer1(
    catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog.csv"),
    main_dataset_csv: Path = Path("data/processed/MainDataset.csv"),
    out_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
    snapshot_tag_value: str | None = None,
) -> None:
    rows: List[Dict[str, str]] = read_csv_rows(catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in catalog: {catalog_csv}")

    df = pd.read_csv(main_dataset_csv)

    suffix = snapshot_tag(snapshot_tag_value or os.getenv("SNAPSHOT_TAG", ""), main_dataset_csv)
    target_col = f"L1_target_answer_{suffix}"
    validate_col = f"L1_validate_{suffix}"
    note_col = f"L1_note_{suffix}"

    out_rows: List[Dict[str, str]] = []
    for row in rows:
        row_out = dict(row)
        target_answer = _latest_active_answer(row_out, suffix)
        status, note, _ = validate_stored_answer(row_out, df, target_answer)
        for redundant in ["released_observation_text", "source_section", "source_paper_path", "test_scope", "primary_metric", "primary_metrics", "statistical_test_plan"]:
            row_out.pop(redundant, None)
        row_out[target_col] = target_answer
        row_out[validate_col] = status
        row_out[note_col] = note
        out_rows.append(row_out)

    write_csv_rows(out_csv, out_rows)
    print(f"Wrote Layer 1 state catalog to: {out_csv}")
    print(f"Added columns: {target_col}, {validate_col}, {note_col}")


if __name__ == "__main__":
    run_layer1()
