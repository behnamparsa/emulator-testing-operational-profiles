import argparse
import math
import os
from pathlib import Path

def read_urls(path: str):
    with open(path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]
    return urls

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    urls = read_urls(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total = len(urls)
    num_chunks = math.ceil(total / args.chunk_size)

    for i in range(num_chunks):
        chunk = urls[i * args.chunk_size:(i + 1) * args.chunk_size]
        shard_name = f"shard_{i+1:04d}.txt"
        (out_dir / shard_name).write_text("\n".join(chunk) + "\n", encoding="utf-8")

    print(f"Created {num_chunks} shards from {total} URLs.")

if __name__ == "__main__":
    main()