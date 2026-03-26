from __future__ import annotations

import argparse, csv, json, math
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
    p.add_argument('--input', required=True)
    p.add_argument('--chunk-size', type=int, required=True)
    p.add_argument('--out-dir', required=True)
    p.add_argument('--filename', default=None, help='Optional filename for each shard copy; defaults to input basename')
    args = p.parse_args()

    if args.chunk_size <= 0:
        raise ValueError('--chunk-size must be > 0')

    in_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    fieldnames, rows = read_rows(in_path)
    if not fieldnames:
        raise ValueError(f'Input CSV has no header: {in_path}')
    if not rows:
        raise ValueError(f'Input CSV has no data rows: {in_path}')

    shard_filename = args.filename or in_path.name
    num_chunks = int(math.ceil(len(rows) / args.chunk_size))
    manifest = []

    for idx in range(num_chunks):
        start = idx * args.chunk_size
        end = min(start + args.chunk_size, len(rows))
        chunk_rows = rows[start:end]
        shard_id = f'shard_{idx + 1:04d}'
        shard_dir = out_dir / shard_id
        write_rows(shard_dir / shard_filename, fieldnames, chunk_rows)
        manifest.append({
            'shard_id': shard_id,
            'filename': shard_filename,
            'start_index': start,
            'end_index': end - 1,
            'row_count': len(chunk_rows),
        })

    (out_dir / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(json.dumps({'num_shards': num_chunks, 'total_rows': len(rows), 'filename': shard_filename}, indent=2))


if __name__ == '__main__':
    main()
