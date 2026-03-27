import os
from pathlib import Path
from profile_qa.layer2_refresh import run_layer2

snapshot = os.getenv('SNAPSHOT_TAG')
run_layer2(
    validated_catalog_csv=Path('outputs/catalog/observation_qa_catalog_validated.csv'),
    main_dataset_csv=Path('data/processed/MainDataset.csv'),
    out_csv=Path('outputs/catalog/observation_qa_catalog_refreshed.csv'),
    snapshot=snapshot,
)
print('outputs/catalog/observation_qa_catalog_refreshed.csv')
