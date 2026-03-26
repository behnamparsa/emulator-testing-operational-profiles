from __future__ import annotations

import argparse, csv, json, shutil
from pathlib import Path
from typing import Dict, Iterable, List

DEFAULT_KEY_MAP = {
    'verified_workflows_v16.csv': ['full_name', 'workflow_identifier', 'workflow_id', 'workflow_path'],
    'run_inventory.csv': ['full_name', 'run_id', 'run_attempt'],
    'run_inventory_per_style.csv': ['full_name', 'run_id', 'run_attempt', 'target_style'],
    'run_metrics_v16_stage3_enhanced.csv': ['full_name', 'run_id', 'run_attempt'],
    'run_steps_v16_stage3_breakdown.csv': ['full_name', 'run_id', 'run_attempt', 'job_name', 'step_number', 'step_name'],
    'run_per_style_v1_stage3.csv': ['full_name', 'run_id', 'run_attempt', 'target_style'],
    'run_workload_signature_v3.csv': ['full_name', 'run_id', 'run_attempt', 'target_style'],
}
FALLBACK_KEYS = ['repo_full_name', 'workflow_run_id', 'attempt', 'style', 'workflow_id', 'workflow_path']


def read_csv(path: Path) -> tuple[list[str], list[Dict[str, str]]]:
    with path.open('r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        return reader.fieldnames or [], [dict(row) for row in reader]


def write_csv(path: Path, fieldnames: list[str], rows: list[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def ordered_union(headers_iter: Iterable[list[str]]) -> list[str]:
    out, seen = [], set()
    for headers in headers_iter:
        for h in headers:
            if h not in seen:
                seen.add(h)
                out.append(h)
    return out


def collect_shard_dirs(base: Path) -> list[Path]:
    return [p for p in sorted(base.iterdir()) if p.is_dir() and p.name.startswith('stage') or p.name.startswith('shard-')]


def dedupe_keys_for(filename: str, merged_headers: list[str]) -> list[str]:
    preferred = DEFAULT_KEY_MAP.get(filename, [])
    keys = [k for k in preferred if k in merged_headers]
    if keys:
        return keys
    return [k for k in FALLBACK_KEYS if k in merged_headers]


def dedupe_rows(rows: list[Dict[str, str]], keys: list[str]) -> list[Dict[str, str]]:
    if not rows or not keys:
        return rows
    out, seen = [], set()
    for row in rows:
        key = tuple((row.get(k, '') or '').strip() for k in keys)
        if not any(key):
            out.append(row)
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--input-dir', required=True)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--file', action='append', required=True, help='Filename to merge; repeat for each CSV')
    args = p.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    shard_dirs = [p for p in sorted(input_dir.iterdir()) if p.is_dir()]
    if not shard_dirs:
        raise ValueError(f'No artifact directories found under {input_dir}')

    manifest = {'merged_files': {}}

    for filename in args.file:
        file_headers: List[List[str]] = []
        combined_rows: List[Dict[str, str]] = []
        found_paths: List[str] = []
        input_shards = 0
        for shard_dir in shard_dirs:
            matches = list(shard_dir.rglob(filename))
            if not matches:
                continue
            match = matches[0]
            headers, rows = read_csv(match)
            file_headers.append(headers)
            combined_rows.extend(rows)
            found_paths.append(str(match))
            input_shards += 1

        if not file_headers:
            continue

        merged_headers = ordered_union(file_headers)
        keys = dedupe_keys_for(filename, merged_headers)
        merged_rows = dedupe_rows(combined_rows, keys)
        write_csv(output_dir / filename, merged_headers, merged_rows)
        manifest['merged_files'][filename] = {
            'input_shards': input_shards,
            'rows_before_dedupe': len(combined_rows),
            'rows_after_dedupe': len(merged_rows),
            'dedupe_keys': keys,
            'found_paths': found_paths,
        }
        shutil.copy2(output_dir / filename, output_dir / f'merged_{filename}')

    (output_dir / 'aggregate_manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
