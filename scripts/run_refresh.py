from __future__ import annotations
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

def run(script: str) -> None:
    subprocess.run([PYTHON, str(REPO_ROOT / 'scripts' / script)], check=True)

def copy_main_dataset_if_needed() -> None:
    import os
    root_dir = Path(os.getenv('ROOT_DIR', str(REPO_ROOT / 'data' / 'processed')))
    src = root_dir / 'MainDataset.csv'
    dst = REPO_ROOT / 'data' / 'processed' / 'MainDataset.csv'
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.exists() and src.resolve() != dst.resolve():
        shutil.copy2(src, dst)

def main() -> None:
    copy_main_dataset_if_needed()
    run('run_analysis.py')
    run('run_section_v.py')

if __name__ == '__main__':
    main()
