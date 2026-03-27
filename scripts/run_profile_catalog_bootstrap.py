from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.observation_catalog import catalog_rows, locate_source_paper
from profile_qa.io_utils import write_csv_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Bootstrap the observation QA catalog from the paper PDF stored in the repository.'
    )
    parser.add_argument(
        '--paper',
        type=Path,
        default=None,
        help='Optional explicit path to the source paper PDF. Defaults to data/Source_Paper/*.pdf',
    )
    parser.add_argument(
        '--out',
        type=Path,
        default=Path('outputs/catalog/observation_qa_catalog.csv'),
        help='Output CSV path for the bootstrapped observation catalog.',
    )
    args = parser.parse_args()

    paper_path = args.paper or locate_source_paper(REPO_ROOT)
    rows = catalog_rows(paper_path)
    write_csv_rows(args.out, rows)
    print(f'Source paper: {paper_path}')
    print(f'Wrote catalog: {args.out}')
    print(f'Rows: {len(rows)}')


if __name__ == '__main__':
    main()
