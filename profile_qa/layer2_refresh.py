"""Deprecated.

Layer 2 has been retired. The refreshed catalog is now fully produced by
single-layer Layer 1 validation + favored-answer update.
"""

from __future__ import annotations

from pathlib import Path


def run_layer2(*args, **kwargs) -> None:
    raise RuntimeError(
        "Layer 2 has been retired. Use scripts/run_layer1_validation.py and "
        "outputs/catalog/observation_qa_catalog_refreshed.csv as the single state table."
    )


if __name__ == "__main__":
    run_layer2()
