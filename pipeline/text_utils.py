from __future__ import annotations

import re
from typing import List, Optional
from typing import Any, Dict, Iterable, Optional

_STYLE_SPLIT_RE = re.compile(r"[,\|;/]+")
_INT_RE = re.compile(r"-?\d+")

def first_nonempty(row: Dict[str, Any], keys: Iterable[str]) -> str:
    """
    Return the first non-empty string value from row[k] across keys, else ''.
    """
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""

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


def safe_int_from_str(x: object) -> Optional[int]:
    """
    Robust integer parser for CSV fields:
    '', None, '12', '12.0', '1,234', '1_234', 'jobs: 8', etc.
    Returns None if no integer can be parsed.
    """
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None

    s = s.replace(",", "").replace("_", "")

    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    except Exception:
        pass

    try:
        f = float(s)
        if f.is_integer():
            return int(f)
    except Exception:
        pass

    m = _INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None