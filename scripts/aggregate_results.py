from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path
from typing import Dict, List, Tuple, Iterable


CSV_DEFS = {
    "verified_workflows_v16.csv": ["repo_full_name", "workflow_id", "workflow_path"],
    "run_inventory.csv": ["repo_full_name", "workflow_run_id", "attempt"],
    "run_metrics_v16_stage3_enhanced.csv": ["repo_full_name", "workflow_run_id", "attempt"],
    "run_steps_v16_stage3_breakdown.csv": ["repo_full_name", "workflow_run_id", "attempt", "job_name", "step_number", "step_name"],
    "run_per_style_v1_stage3.csv": ["repo_full_name", "workflow_run_id", "attempt", "style"],
    "run_workload_signature_v3.csv": ["repo_full_name", "workflow_run_id", "attempt", "style"],
}


def read_csv(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def ordered_union(all_headers: Iterable[List[str]]) -> List[str]:
    seen = set()
    out: List[str] = []
    for headers in all_headers:
        for col in headers:
            if col not in seen:
                seen.add(col)
                out.append(col)
    return out


def dedupe_rows(rows: List[Dict[str, str]], dedupe_keys: List[str]) -> List[Dict[str, str]]:
    if not rows or not dedupe_keys:
        return rows

    if not all(k in rows[0] or any(k in r for r in rows) for k in dedupe_keys):
        return rows

    out: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        key = tuple((row.get(k, "") or "").strip() for k in dedupe_keys)
        if not any(key):
            out.append(row)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def collect_shard_dirs(base: Path) -> List[Path]:
    out: List[Path] = []
    for child in sorted(base.iterdir()):
        if child.is_dir() and child.name.startswith("shard-"):
            out.append(child)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    shard_dirs = collect_shard_dirs(input_dir)
    if not shard_dirs:
        raise ValueError(f"No shard-* artifact directories found under {input_dir}")

    manifest = {"shards": [], "merged_files": {}}

    for filename, dedupe_keys in CSV_DEFS.items():
        file_headers: List[List[str]] = []
        combined_rows: List[Dict[str, str]] = []
        contributing = 0

        for shard_dir in shard_dirs:
            shard_file = shard_dir / filename
            if not shard_file.exists():
                continue
            headers, rows = read_csv(shard_file)
            file_headers.append(headers)
            combined_rows.extend(rows)
            contributing += 1

        if not combined_rows:
            continue

        merged_headers = ordered_union(file_headers)
        merged_rows = dedupe_rows(combined_rows, dedupe_keys)
        write_csv(output_dir / filename, merged_headers, merged_rows)
        manifest["merged_files"][filename] = {
            "input_shards": contributing,
            "rows_before_dedupe": len(combined_rows),
            "rows_after_dedupe": len(merged_rows),
            "dedupe_keys": dedupe_keys,
        }

    for shard_dir in shard_dirs:
        shard_manifest = shard_dir / "manifest.json"
        if shard_manifest.exists():
            try:
                manifest["shards"].append(json.loads(shard_manifest.read_text(encoding="utf-8")))
            except Exception:
                manifest["shards"].append({"artifact_dir": shard_dir.name, "manifest": "unreadable"})
        else:
            manifest["shards"].append({"artifact_dir": shard_dir.name})

    (output_dir / "aggregate_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # convenience mirror directory for inspection
    merged_dir = output_dir / "merged_snapshot"
    merged_dir.mkdir(exist_ok=True)
    for filename in CSV_DEFS:
        src = output_dir / filename
        if src.exists():
            shutil.copy2(src, merged_dir / filename)


if __name__ == "__main__":
    main()
