import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.layer2_refresh import run_layer2


if __name__ == "__main__":
    snapshot_tag = os.getenv("SNAPSHOT_TAG", "")

    run_layer2(
        validated_catalog_csv=Path("outputs/catalog/observation_qa_catalog_validated.csv"),
        main_dataset_csv=Path("data/processed/MainDataset.csv"),
        out_csv=Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
        snapshot=snapshot_tag,
    )

    print("outputs/catalog/observation_qa_catalog_refreshed.csv")