from pathlib import Path
import sys

# Add repo root to Python path so "analysis" and other top-level packages are importable
REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from analysis.run_all import main

if __name__ == "__main__":
    main()