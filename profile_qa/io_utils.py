from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Dict
import csv
import subprocess


def _normalize_tag(value: str) -> str:
    return str(value or '').strip().replace('-', '_')


def _git_snapshot_tag(dataset_path: Path) -> str | None:
    try:
        resolved = dataset_path.resolve()
        repo_root = subprocess.check_output(
            ['git', '-C', str(resolved.parent), 'rev-parse', '--show-toplevel'],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        rel = resolved.relative_to(Path(repo_root))
        commit_date = subprocess.check_output(
            ['git', '-C', repo_root, 'log', '-1', '--format=%cs', '--', str(rel)],
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
        return _normalize_tag(commit_date) if commit_date else None
    except Exception:
        return None


def _filesystem_snapshot_tag(dataset_path: Path) -> str:
    ts = dataset_path.stat().st_mtime
    return datetime.fromtimestamp(ts).strftime('%Y_%m_%d')


def snapshot_tag(value: str | None = None, dataset_path: str | Path | None = None) -> str:
    if value:
        return _normalize_tag(value)
    if dataset_path:
        dataset_path = Path(dataset_path)
        if dataset_path.exists():
            git_tag = _git_snapshot_tag(dataset_path)
            if git_tag:
                return git_tag
            return _filesystem_snapshot_tag(dataset_path)
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
