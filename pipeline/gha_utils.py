import re

_EXPR_WS = re.compile(r"\s+")
_EXPR_COMMENTS = re.compile(r"#.*?$", re.MULTILINE)

def sanitize_gha_expr(expr: str) -> str:
    """
    Conservative normalizer for GitHub Actions expressions / 'if:' conditions.
    - strips surrounding whitespace
    - removes YAML-style inline comments
    - collapses internal whitespace
    - keeps content otherwise unchanged (no evaluation)
    """
    if expr is None:
        return ""
    s = str(expr).strip()
    s = _EXPR_COMMENTS.sub("", s).strip()
    s = _EXPR_WS.sub(" ", s)
    return s