import re
from typing import Optional

_INT_RE = re.compile(r"-?\d+")

def safe_int_from_str(x: object) -> Optional[int]:
    """
    Robust integer parser used for CSV fields that may contain:
    '', None, '12', '12.0', '1,234', '1_234', 'jobs: 8', etc.
    Returns None if no integer can be parsed.
    """
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None

    # normalize common formats
    s = s.replace(",", "").replace("_", "")

    # fast path
    try:
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return int(s)
    except Exception:
        pass

    # handle float-looking ints like '12.0'
    try:
        f = float(s)
        if f.is_integer():
            return int(f)
    except Exception:
        pass

    # extract first integer substring
    m = _INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None