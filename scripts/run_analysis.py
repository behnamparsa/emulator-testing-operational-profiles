from pathlib import Path
import sys
import subprocess

REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable

def run(script: str) -> None:
    subprocess.run([PYTHON, str(REPO_ROOT / "scripts" / script)], check=True)

if __name__ == "__main__":
    run("run_profile_catalog_bootstrap.py")
    run("run_layer1_validation.py")
    run("run_profile_regeneration.py")
    run("run_robustness_check.py")
    subprocess.run([PYTHON, str(REPO_ROOT / "analysis" / "run_all.py")], check=True)
