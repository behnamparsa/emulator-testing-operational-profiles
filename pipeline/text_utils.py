from __future__ import annotations

import re
from typing import List

_STYLE_SPLIT_RE = re.compile(r"[,\|;/]+")

def split_styles(styles_text: str) -> List[str]:
    """
    Parse the Stage-1/Stage-2 'styles' field into a list of styles.
    Accepts comma/pipe/semicolon separated values and de-dupes preserving order.
    """
    if not styles_text:
        return []
    parts = [p.strip() for p in _STYLE_SPLIT_RE.split(str(styles_text)) if p.strip()]
    seen = set()
    out: List[str] = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out