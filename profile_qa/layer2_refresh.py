from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import pandas as pd

from .io_utils import read_csv_rows, write_csv_rows, snapshot_tag
from .item_logic import evaluate_item



def run_layer2(validated_catalog_csv: Path, main_dataset_csv: Path, out_csv: Path, snapshot: str | None = None) -> Path:
<<<<<<< HEAD
    tag = snapshot_tag(snapshot, main_dataset_csv)
=======
<<<<<<< HEAD
    tag = snapshot_tag(snapshot, main_dataset_csv)
=======
<<<<<<< HEAD
    tag = snapshot_tag(snapshot, main_dataset_csv)
=======
    tag = snapshot_tag(snapshot)
>>>>>>> bdbb4671bb11383b148a4bdec0e9019fca11d952
>>>>>>> 60171f0709520c501f15dc9d53f6aaa811fc4b9f
>>>>>>> 8facab844a5d2b233e3ccf38e6172fecda81f057
    target_col = f"L1_target_answer_{tag}"
    validate_col = f"L1_validate_{tag}"
    answer_col = f"L2_answer_{tag}"
    note_col = f"L2_note_{tag}"
    used_col = f"L2_used_{tag}"

    rows: List[Dict[str, str]] = read_csv_rows(validated_catalog_csv)
    df = pd.read_csv(main_dataset_csv)

    for row in rows:
        target_answer = str(row.get(target_col, "") or row.get("released_answer", "")).strip()
        l1_status = str(row.get(validate_col, "")).strip()
        if l1_status == "Passed":
            row[used_col] = "No"
            row[answer_col] = target_answer
            row[note_col] = f"Layer 1 passed; retained stored answer '{target_answer}'."
            continue

        eval_result = evaluate_item(str(row.get("obs_id", "")), str(row.get("question", "")), df)
        row[used_col] = "Yes"
        row[answer_col] = eval_result.winner
        row[note_col] = eval_result.note

    write_csv_rows(out_csv, rows)
    return out_csv
