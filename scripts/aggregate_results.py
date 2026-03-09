import argparse
import json
from pathlib import Path
import shutil

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    merged_dir = output_dir / "merged_shards"
    merged_dir.mkdir(exist_ok=True)

    manifests = []

    for artifact_dir in input_dir.iterdir():
        if not artifact_dir.is_dir():
            continue

        manifest = artifact_dir / "manifest.json"
        if manifest.exists():
            manifests.append(json.loads(manifest.read_text(encoding="utf-8")))

        for item in artifact_dir.rglob("*"):
            if item.is_file() and item.name != "manifest.json":
                target = merged_dir / f"{artifact_dir.name}__{item.name}"
                shutil.copy2(item, target)

    (output_dir / "aggregate_manifest.json").write_text(
        json.dumps({"num_shards": len(manifests), "shards": manifests}, indent=2),
        encoding="utf-8"
    )

if __name__ == "__main__":
    main()