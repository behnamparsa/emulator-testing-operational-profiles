from __future__ import annotations

import argparse, csv, json, math, shutil
from pathlib import Path
from typing import Dict, List


def read_rows(path: Path) -> tuple[list[str], list[Dict[str, str]]]:
    with path.open('r', encoding='utf-8', errors='ignore', newline='') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = [dict(row) for row in reader]
    return fieldnames, rows


def write_rows(path: Path, fieldnames: list[str], rows: list[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument('--run-inventory', required=True)
    p.add_argument('--run-per-style', required=True)
    p.add_argument('--chunk-size', type=int, required=True)
    p.add_argument('--out-dir', required=True)
    args = p.parse_args()

    if args.chunk_size <= 0:
        raise ValueError('--chunk-size must be > 0')

    run_inventory = Path(args.run_inventory)
    run_per_style = Path(args.run_per_style)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    ri_fields, _ = read_rows(run_inventory)
    rps_fields, rps_rows = read_rows(run_per_style)
    if not ri_fields or not rps_fields:
        raise ValueError('Stage 3 inputs must both have headers')
    if not rps_rows:
        raise ValueError('run_inventory_per_style.csv has no data rows')

    num_chunks = int(math.ceil(len(rps_rows) / args.chunk_size))
    manifest = []

    for idx in range(num_chunks):
        start = idx * args.chunk_size
        end = min(start + args.chunk_size, len(rps_rows))
        chunk_rows = rps_rows[start:end]
        shard_id = f'shard_{idx + 1:04d}'
        shard_dir = out_dir / shard_id
        shard_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(run_inventory, shard_dir / 'run_inventory.csv')
        write_rows(shard_dir / 'run_inventory_per_style.csv', rps_fields, chunk_rows)
        manifest.append({
            'shard_id': shard_id,
            'start_index': start,
            'end_index': end - 1,
            'row_count': len(chunk_rows),
            'reference_file': 'run_inventory.csv',
            'driver_file': 'run_inventory_per_style.csv',
        })

    (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps({'num_shards': num_chunks, 'total_rows': len(rps_rows)}, indent=2))


if __name__ == '__main__':
    main()
