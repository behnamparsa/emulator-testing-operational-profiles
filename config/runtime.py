from __future__ import annotations
import os
from pathlib import Path
from typing import List, Optional


def get_repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


# config/runtime.py
def get_root_dir() -> Path:
    value = os.getenv("ROOT_DIR")
    if value:
        return Path(value).expanduser().resolve()
    return (get_repo_root() / "data" / "processed").resolve()


def get_tokens_env_path() -> Path:
    value = os.getenv("TOKENS_ENV_PATH")
    if value:
        return Path(value).expanduser().resolve()
    return get_repo_root() / ".github_tokens.env"


def load_github_tokens(env_path: Optional[Path] = None, max_tokens: int = 3) -> List[str]:
    tokens: List[str] = []

    for key in ["GH_PAT", "GITHUB_TOKEN"]:
        val = os.getenv(key, "").strip()
        if val:
            tokens.append(val)

    numbered = []
    for key, value in os.environ.items():
        if key.startswith("GITHUB_TOKEN_") and value.strip():
            try:
                order = int(key.split("_")[-1])
            except Exception:
                order = 9999
            numbered.append((order, value.strip()))
    numbered.sort(key=lambda x: x[0])
    tokens.extend(v for _, v in numbered)

    path = env_path or get_tokens_env_path()
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if (key in {"GH_PAT", "GITHUB_TOKEN"} or key.startswith("GITHUB_TOKEN_")) and value:
                tokens.append(value)

    # de-duplicate, preserve order
    deduped: List[str] = []
    seen = set()
    for token in tokens:
        if token not in seen:
            seen.add(token)
            deduped.append(token)

    if not deduped:
        raise ValueError(
            "No GitHub token found. Set GH_PAT or GITHUB_TOKEN in repository secrets or local env, "
            "or provide TOKENS_ENV_PATH/.github_tokens.env with GITHUB_TOKEN_1=..."
        )
    return deduped[:max_tokens]
