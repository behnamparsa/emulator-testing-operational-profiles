from __future__ import annotations
import subprocess, sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

MODULES = [
    'pipeline.stage1_verified_workflows',
    'pipeline.stage2_run_inventory',
    'pipeline.stage3_run_telemetry',
    'pipeline.stage4_workload_signature',
    'pipeline.build_total_dataset',
]

for module in MODULES:
    print(f"\n=== Running {module} ===")
    subprocess.run([PYTHON, '-m', module], check=True, cwd=REPO_ROOT)
