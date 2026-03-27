from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Dict
import csv


def snapshot_tag(value: str | None = None) -> str:
    if value:
        return value.replace('-', '_')
    return datetime.utcnow().strftime('%Y_%m_%d')


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open('r', encoding='utf-8', newline='') as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, rows: Iterable[Dict[str, str]]) -> None:
    rows = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
