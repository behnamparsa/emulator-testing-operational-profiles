from __future__ import annotations
import os
import re
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


def load_github_tokens(env_path: Optional[Path] = None, max_tokens: Optional[int] = None) -> List[str]:
    """Load a cross-repo GitHub token pool with PATs first, then fallbacks."""
    tokens: List[str] = []

    def add_token(value: str) -> None:
        sval = (value or "").strip()
        if sval:
            tokens.append(sval)

    # Preferred PAT pool first
    for key in [f"GH_PAT_{i}" for i in range(1, 6)]:
        add_token(os.getenv(key, ""))
    add_token(os.getenv("GH_PAT", ""))

    # Additional numbered GitHub tokens if present
    numbered_gh_tokens = []
    for key, value in os.environ.items():
        sval = (value or "").strip()
        if not sval:
            continue
        if re.match(r"^GITHUB_TOKEN_\d+$", key):
            try:
                order = int(key.split("_")[-1])
            except Exception:
                order = 9999
            numbered_gh_tokens.append((order, sval))
    for _, val in sorted(numbered_gh_tokens):
        add_token(val)

    # Repo-scoped workflow token last
    add_token(os.getenv("GITHUB_TOKEN", ""))

    path = env_path or get_tokens_env_path()
    if path.exists():
        env_values = {}
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            env_values[key.strip()] = value.strip().strip('"').strip("'")

        for key in [f"GH_PAT_{i}" for i in range(1, 6)]:
            add_token(env_values.get(key, ""))
        add_token(env_values.get("GH_PAT", ""))

        numbered_env_gh = []
        for key, value in env_values.items():
            if re.match(r"^GITHUB_TOKEN_\d+$", key):
                try:
                    order = int(key.split("_")[-1])
                except Exception:
                    order = 9999
                numbered_env_gh.append((order, value))
        for _, val in sorted(numbered_env_gh):
            add_token(val)

        add_token(env_values.get("GITHUB_TOKEN", ""))

    deduped: List[str] = []
    seen = set()
    for token in tokens:
        if token not in seen:
            seen.add(token)
            deduped.append(token)

    if not deduped:
        raise ValueError(
            "No GitHub token found. Set GH_PAT_1..GH_PAT_5, GH_PAT, or GITHUB_TOKEN in repository secrets or local env, "
            "or provide TOKENS_ENV_PATH/.github_tokens.env with GH_PAT_1=..."
        )

    if max_tokens is None or max_tokens <= 0:
        return deduped
    return deduped[:max_tokens]
