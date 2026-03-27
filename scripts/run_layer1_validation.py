import os
from pathlib import Path
from profile_qa.layer1_validate import run_layer1

snapshot = os.getenv('SNAPSHOT_TAG')
run_layer1(
    catalog_csv=Path('outputs/catalog/observation_qa_catalog.csv'),
    main_dataset_csv=Path('data/processed/MainDataset.csv'),
    out_csv=Path('outputs/catalog/observation_qa_catalog_validated.csv'),
    snapshot=snapshot,
)
print('outputs/catalog/observation_qa_catalog_validated.csv')
