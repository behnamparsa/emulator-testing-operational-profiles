from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


def read_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Read a CSV into a list of dict rows and return (rows, fieldnames).
    All values are coerced to strings (or empty string) for robust downstream use.
    """
    if not Path(path).exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")

    with Path(path).open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        fieldnames = list(rdr.fieldnames or [])
        rows: List[Dict[str, str]] = []
        for r in rdr:
            rows.append({(k or ""): ("" if v is None else str(v)) for k, v in r.items()})
    return rows, fieldnames


def write_csv(path: Path, fieldnames: List[str], rows: Iterable[Dict]) -> None:
    """
    Write rows to CSV (overwrite). Ensures directory exists and writes header.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def ensure_csv(path: Path, fieldnames: List[str]) -> None:
    """
    Create/overwrite a CSV file with the given header.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


# Alias used by some stages
def ensure_csv_header(path: Path, fieldnames: List[str]) -> None:
    return ensure_csv(path, fieldnames)


def append_row(path: Path, fieldnames: List[str], row: Dict) -> None:
    """
    Append a single row to a CSV, assuming the file already exists with header.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)


def append_rows(path: Path, fieldnames: List[str], rows: Iterable[Dict]) -> None:
    """
    Append multiple rows to a CSV.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for row in rows:
            w.writerow(row)


def load_existing_keys(csv_path: Path, key_field: str) -> Set[str]:
    """
    Load a set of unique keys from an existing CSV column.
    Useful for de-duplication when appending.
    """
    csv_path = Path(csv_path)
    keys: Set[str] = set()
    if not csv_path.exists():
        return keys

    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            k = (row.get(key_field) or "").strip()
            if k:
                keys.add(k)
    return keys


def unique_preserve(items: Sequence[str]) -> List[str]:
    """
    Return a list of unique items preserving first-seen order.
    """
    seen: Set[str] = set()
    out: List[str] = []
    for x in items:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def safe_join_names(items: Optional[Sequence[str]], sep: str = ",") -> str:
    """
    Join a list of names (job/step names) into a single CSV-safe string:
    - strips whitespace
    - drops empties
    - de-dupes preserving order
    """
    if not items:
        return ""
    cleaned: List[str] = []
    for x in items:
        if x is None:
            continue
        s = str(x).strip()
        if s:
            cleaned.append(s)
    cleaned = unique_preserve(cleaned)
    return sep.join(cleaned)