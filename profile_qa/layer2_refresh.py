from __future__ import annotations

from pathlib import Path


def run_layer2(*args, **kwargs) -> None:
    raise RuntimeError(
        "Layer 2 has been removed from the analytical flow. "
        "Use profile_qa.layer1_validate.run_layer1() to validate and refresh the active answer."
    )


if __name__ == "__main__":
    run_layer2(Path())
