from pathlib import Path
import sys
import os

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.layer1_validate import run_layer1


if __name__ == "__main__":
    baseline = Path("outputs/catalog/observation_qa_catalog_refreshed.csv")
    if not baseline.exists():
        baseline = Path("outputs/catalog/observation_qa_catalog.csv")
    run_layer1(
        catalog_csv=baseline,
        main_dataset_csv=Path("data/processed/MainDataset.csv"),
        out_csv=Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
        snapshot_tag_value=os.getenv("SNAPSHOT_TAG", ""),
    )
