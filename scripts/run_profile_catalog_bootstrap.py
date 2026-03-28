from pathlib import Path
import argparse
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.observation_catalog import (
    locate_source_paper,
    write_catalog_csv,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--paper",
        type=Path,
        default=None,
        help="Optional explicit path to the source paper PDF.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=REPO_ROOT / "outputs" / "catalog" / "observation_qa_catalog.csv",
        help="Output catalog CSV path.",
    )
    args = parser.parse_args()

    paper_path = args.paper or locate_source_paper()
    out_csv = args.out

    write_catalog_csv(out_csv=out_csv, source_paper_path=paper_path)
    print(f"Wrote catalog to: {out_csv}")
    print(f"Source paper: {paper_path}")


if __name__ == "__main__":
    main()