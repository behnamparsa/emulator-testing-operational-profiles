from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import List, Dict


BOM = "\ufeff"


def _clean_key(k: str) -> str:
    return (k or "").replace(BOM, "").strip()


def read_url_list(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"URL list not found: {path}")

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"URL list has no header: {path}")

        fieldnames = [_clean_key(x) for x in reader.fieldnames]
        if "repo_url" not in fieldnames:
            raise ValueError("URL_List.csv must include a 'repo_url' column")

        rows: List[Dict[str, str]] = []
        for row in reader:
            clean = {_clean_key(k): (v or "").strip() for k, v in row.items()}
            if clean.get("repo_url", ""):
                rows.append(clean)
        return rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to URL_List.csv")
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be > 0")

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = read_url_list(input_path)
    fieldnames = ["repo_url"]

    if not rows:
        raise ValueError("URL_List.csv contains no repo_url rows")

    num_chunks = int(math.ceil(len(rows) / args.chunk_size))
    manifest = []

    for idx in range(num_chunks):
        start = idx * args.chunk_size
        end = min(start + args.chunk_size, len(rows))
        chunk_rows = rows[start:end]
        shard_id = f"shard_{idx + 1:04d}"
        shard_filename = f"{shard_id}.csv"
        shard_path = out_dir / shard_filename
        write_csv(shard_path, fieldnames, chunk_rows)
        manifest.append(
            {
                "shard_id": shard_id,
                "shard_file": shard_filename,
                "start_index": start,
                "end_index": end - 1,
                "row_count": len(chunk_rows),
            }
        )

    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps({"num_shards": num_chunks, "total_rows": len(rows)}, indent=2))


if __name__ == "__main__":
    main()
