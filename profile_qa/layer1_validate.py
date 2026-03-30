from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import os

import pandas as pd

from .io_utils import read_csv_rows, write_csv_rows, snapshot_tag
from .item_logic import validate_stored_answer

REFRESHED_CATALOG = Path("outputs/catalog/observation_qa_catalog_refreshed.csv")
BASE_CATALOG = Path("outputs/catalog/observation_qa_catalog.csv")
MAIN_DATASET = Path("data/processed/MainDataset.csv")

DROP_PREFIXES = (
    "L1_target_answer_",
    "L1_validate_",
    "L1_note_",
    "L1_favored_answer_",
    "L1_favored_note_",
    "ACTIVE_",
    "L1_target_source_",
)


def _latest_column(prefix: str, fieldnames: List[str], exclude_suffix: str | None = None) -> str | None:
    matches = [c for c in fieldnames if c.startswith(prefix)]
    if exclude_suffix:
        suffix = f"_{exclude_suffix}"
        matches = [c for c in matches if not c.endswith(suffix)]
    return sorted(matches)[-1] if matches else None


def _source_catalog(path_hint: Path | None = None) -> Path:
    if path_hint and path_hint.exists():
        return path_hint
    if REFRESHED_CATALOG.exists():
        return REFRESHED_CATALOG
    return BASE_CATALOG


def _stored_answer_for_row(row: Dict[str, str], fieldnames: List[str], current_tag: str) -> Tuple[str, str]:
    """Use the latest ACTIVE_* from an older snapshot only.

    If no older ACTIVE_* exists, fall back to released_answer so the first
    stable refreshed snapshot is validated against the paper baseline.
    """
    latest_active = _latest_column("ACTIVE_", fieldnames, exclude_suffix=current_tag)
    if latest_active and str(row.get(latest_active, "")).strip():
        return str(row.get(latest_active, "")).strip(), latest_active
    return str(row.get("released_answer", "")).strip(), "released_answer"


def _clean_row(row: Dict[str, str]) -> Dict[str, str]:
    return {k: v for k, v in row.items() if not any(k.startswith(prefix) for prefix in DROP_PREFIXES)}


def run_layer1(
    catalog_csv: Path | None = None,
    main_dataset_csv: Path = MAIN_DATASET,
    out_csv: Path = REFRESHED_CATALOG,
    snapshot: str | None = None,
) -> None:
    source_csv = _source_catalog(catalog_csv)
    rows: List[Dict[str, str]] = read_csv_rows(source_csv)
    if not rows:
        raise RuntimeError(f"No rows found in source catalog: {source_csv}")

    df = pd.read_csv(main_dataset_csv)
    tag = snapshot_tag(snapshot or os.getenv("SNAPSHOT_TAG", ""), dataset_path=main_dataset_csv)

    target_col = f"L1_target_answer_{tag}"
    validate_col = f"L1_validate_{tag}"
    note_col = f"L1_note_{tag}"
    favored_col = f"L1_favored_answer_{tag}"
    favored_note_col = f"L1_favored_note_{tag}"
    active_col = f"ACTIVE_{tag}"
    target_source_col = f"L1_target_source_{tag}"

    fieldnames = list(rows[0].keys())
    out_rows: List[Dict[str, str]] = []
    for raw_row in rows:
        row = _clean_row(dict(raw_row))
        stored_answer, stored_from = _stored_answer_for_row(raw_row, fieldnames, tag)
        status, note, eval_result = validate_stored_answer(raw_row, df, stored_answer)
        favored_answer = str(eval_result.winner or "").strip()
        favored_note = str(eval_result.note or "").strip()
        active_answer = stored_answer if status in {"Passed", "Insufficient evidence"} else favored_answer

        row[target_col] = stored_answer
        row[validate_col] = status
        row[note_col] = note
        row[favored_col] = favored_answer
        row[favored_note_col] = favored_note
        row[active_col] = active_answer
        if stored_from and stored_from != "released_answer":
            row[target_source_col] = stored_from
        out_rows.append(row)

    write_csv_rows(out_csv, out_rows)
    print(f"Wrote refreshed catalog with single-layer validation/update to: {out_csv}")
    print(
        "Added columns: "
        f"{target_col}, {validate_col}, {note_col}, {favored_col}, {favored_note_col}, {active_col}"
    )


if __name__ == "__main__":
    run_layer1()
