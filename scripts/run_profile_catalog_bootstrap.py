from pathlib import Path
from profile_qa.observation_catalog import catalog_rows
from profile_qa.io_utils import write_csv_rows

out = Path('outputs/catalog/observation_qa_catalog.csv')
write_csv_rows(out, catalog_rows())
print(out)
