from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .io_utils import read_csv_rows, snapshot_tag, write_csv_rows
from .item_logic import validate_stored_answer

REFRESHED_CATALOG = Path("outputs/catalog/observation_qa_catalog_refreshed.csv")
BASE_CATALOG = Path("outputs/catalog/observation_qa_catalog.csv")
MAIN_DATASET = Path("data/processed/MainDataset.csv")

# Old two-layer columns are removed from the refreshed state table on every run.
DROP_PREFIXES = (
    "L2_used_",
    "L2_answer_",
    "L2_note_",
    "L2_runner_up_",
)


def _latest_column(prefix: str, fieldnames: List[str]) -> str | None:
    matches = [c for c in fieldnames if c.startswith(prefix)]
    return sorted(matches)[-1] if matches else None


def _source_catalog(path_hint: Path | None = None) -> Path:
    if path_hint and path_hint.exists():
        return path_hint
    if REFRESHED_CATALOG.exists():
        return REFRESHED_CATALOG
    return BASE_CATALOG


def _stored_answer_for_row(row: Dict[str, str], fieldnames: List[str]) -> Tuple[str, str]:
    """Return the latest active answer to validate.

    Single-layer rule:
    1) latest ACTIVE_* answer from the refreshed state table
    2) otherwise released_answer from the baseline paper catalog

    Intentionally does NOT look at old L2_* columns anymore.
    """
    latest_active = _latest_column("ACTIVE_", fieldnames)
    if latest_active and str(row.get(latest_active, "")).strip():
        return str(row.get(latest_active, "")).strip(), latest_active
    return str(row.get("released_answer", "")).strip(), "released_answer"


def _clean_row(row: Dict[str, str]) -> Dict[str, str]:
    return {
        k: v
        for k, v in row.items()
        if not any(k.startswith(prefix) for prefix in DROP_PREFIXES)
    }


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
    source_col = f"L1_target_source_{tag}"

    fieldnames = list(rows[0].keys())
    out_rows: List[Dict[str, str]] = []

    for raw_row in rows:
        row = _clean_row(dict(raw_row))
        stored_answer, stored_from = _stored_answer_for_row(raw_row, fieldnames)
        status, note, eval_result = validate_stored_answer(raw_row, df, stored_answer)

        favored_answer = str(eval_result.winner or "").strip()
        favored_note = str(eval_result.note or "").strip()
        active_answer = stored_answer if status == "Passed" else favored_answer

        row[target_col] = stored_answer
        row[validate_col] = status
        row[note_col] = note
        row[favored_col] = favored_answer
        row[favored_note_col] = favored_note
        row[active_col] = active_answer
        if stored_from and stored_from != "released_answer":
            row[source_col] = stored_from

        out_rows.append(row)

    write_csv_rows(out_csv, out_rows)
    print(f"Wrote refreshed catalog with single-layer validation/update to: {out_csv}")
    print(
        "Added columns: "
        f"{target_col}, {validate_col}, {note_col}, {favored_col}, {favored_note_col}, {active_col}"
    )


if __name__ == "__main__":
    run_layer1()
