# ============================================================
# Stage 3 (FULLY ADJUSTED): timing model + S2_ fallback integration
#
# Adjustments (this version)
# 1) Keeps GHA expression sanitization
# 2) Keeps Detox + Flutter Integration Test execution detection (Android-gated)
# 3) Keeps Stage-1 test_invocation_step_names priority anchor hint
# 4) Adds MODIFIED TTFTS support (anchor job start -> anchoring step start)
#    with proper fallback to S2_ fields from Stage 2:
#      - time_to_first_instru_from_anchor_job_seconds
#      - S2_time_to_first_instru_from_anchor_job_seconds
#      - anchor_job_started_at / S2_anchor_job_started_at
# 5) Adds runtime count metric for jobs from run start to anchor-step job (inclusive):
#      - jobs_to_anchor_job_count
#    and source field:
#      - jobs_to_anchor_job_count_source
# 6) Includes the Stage-1 pass-through field (if present in Stage 2 input):
#      - jobs_before_anchor_count
#    in run and run-per-style outputs automatically via preserved columns
# 7) FIX (this version ONLY): Constrain instrumentation window end (instru_end)
#    to instrumentation-job scope so unrelated tail jobs do not extend the
#    instrumentation window. Preference:
#      - anchor job if known
#      - else exec-step job(s)
#    (No other changes.)
# ============================================================

import base64
import csv
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from pipeline.gha_utils import sanitize_gha_expr
from pipeline.text_utils import split_styles, safe_int_from_str
from pipeline.csv_utils import read_csv_rows, ensure_csv, ensure_csv_header, append_row, load_existing_keys, safe_join_names, unique_preserve, write_csv


import requests

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

# =========================
# CONFIG
# =========================
from config.runtime import get_root_dir, get_tokens_env_path, load_github_tokens

TOKENS_ENV_PATH = get_tokens_env_path()
ROOT_DIR = get_root_dir()

IN_STAGE2_CSV = ROOT_DIR / "run_inventory.csv"

OUT_STAGE3A_RUNS_CSV = ROOT_DIR / "run_metrics_v16_stage3_enhanced.csv"          # Run metrics
OUT_STAGE3B_STEPS_CSV = ROOT_DIR / "run_steps_v16_stage3_breakdown.csv"          # Step metrics
OUT_STAGE3C_RUN_PER_STYLE_CSV = ROOT_DIR / "run_per_style_v1_stage3.csv"         # Run metrics per style

MAX_TOKENS_TO_USE = 7
PROCESS_ONLY_RELEVANT_ROWS = True

CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 60
MAX_RETRIES_PER_REQUEST = 8
BACKOFF_BASE_S = 1.7
BACKOFF_CAP_S = 60
MAX_PAGES_PER_LIST = 2000

FETCH_WORKFLOW_YAML = True
WORKFLOW_YAML_CACHE_MAX = 7000

# =========================
# Helpers
# =========================
BOM = "\ufeff"
GHA_EXPR_RE = re.compile(r"\${{\s*[^}]+}}")  # sanitize GitHub Actions expressions


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

import csv
from pathlib import Path
from typing import List


import re
from typing import List

_STYLE_SPLIT_RE = re.compile(r"[,\|;/]+")

def split_styles(styles_text: str) -> List[str]:
    """
    Parse the style label string into a list of styles.
    Accepts comma/pipe/semicolon separated values.
    Preserves canonical names used in the paper.
    """
    if not styles_text:
        return []
    parts = [p.strip() for p in _STYLE_SPLIT_RE.split(str(styles_text)) if p.strip()]
    # de-dupe preserving order
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out

def ensure_csv(path: Path, fieldnames: List[str]) -> None:
    """
    Create/overwrite a CSV file with the given header.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


import csv
from pathlib import Path
from typing import Dict, List, Tuple

def read_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Read a CSV into a list of dict rows and return (rows, fieldnames).
    Matches how Stage 1/2 typically load CSVs.
    """
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        rows = [{(k or ""): (v or "") for k, v in r.items()} for r in rdr]
        fieldnames = list(rdr.fieldnames or [])
    return rows, fieldnames

def iso_to_dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00"))
    except Exception:
        return None


def dt_to_seconds(a: Optional[datetime], b: Optional[datetime]) -> Optional[int]:
    if not a or not b:
        return None
    try:
        sec = int((b - a).total_seconds())
        return sec if sec >= 0 else None
    except Exception:
        return None


def unique_preserve(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def safe_join_names(names: List[str], max_len: int = 800) -> str:
    s = ",".join(unique_preserve([n.strip() for n in names if n and n.strip()]))
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def load_tokens_from_env_file(env_path: Optional[Path], max_tokens: int = 3) -> List[str]:
    tokens = load_github_tokens(env_path=env_path, max_tokens=max_tokens)
    if not tokens:
        raise RuntimeError(
            "No GitHub tokens found. Provide All_Tokens.env with GITHUB_TOKEN_1..7 "
            "or set TOKENS_ENV_PATH to its location."
        )
    return tokens

# =========================
# Normalization + matching
# =========================
_norm_ws_re = re.compile(r"\s+")
_norm_punct_re = re.compile(r"[^a-z0-9]+")


def normalize_step_key(s: str) -> str:
    s = (s or "").lower().strip()
    s = sanitize_gha_expr(s)
    s = _norm_ws_re.sub(" ", s)
    s = _norm_punct_re.sub(" ", s)
    s = _norm_ws_re.sub(" ", s).strip()
    return s


def token_set(s: str) -> Set[str]:
    return set([t for t in normalize_step_key(s).split(" ") if t])


def jaccard(a: Set[str], b: Set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    return inter / uni if uni else 0.0


def is_runner_injected_step(step_name: str) -> bool:
    s = (step_name or "").strip()
    if not s:
        return True
    if s.lower() in ("set up job", "complete job"):
        return True
    if s.startswith("Post "):
        return True
    return False


_GENERIC_TOKENS = {"set", "up", "install", "setup", "cache", "checkout", "post", "complete", "job"}


def is_too_generic_for_fuzzy(name: str) -> bool:
    toks = token_set(name)
    if not toks:
        return True
    non_generic = [t for t in toks if t not in _GENERIC_TOKENS]
    return len(non_generic) <= 1


def best_yaml_step_match_with_reason(step_name: str, yaml_steps: Dict[str, Dict[str, str]]) -> Tuple[Optional[Dict[str, str]], str]:
    if not step_name or not yaml_steps:
        return None, "no_match"

    key = normalize_step_key(step_name)
    if key in yaml_steps:
        return yaml_steps[key], "exact_norm"

    candidates = []
    for k in yaml_steps.keys():
        if not k:
            continue
        if key and (key in k or k in key):
            candidates.append((len(k), k))
    if candidates:
        candidates.sort(reverse=True)
        return yaml_steps[candidates[0][1]], "substring_norm"

    if is_too_generic_for_fuzzy(step_name):
        return None, "no_match_generic_step"

    import difflib
    s_tokens = token_set(step_name)
    best_score = 0.0
    best_k = None
    best_j = 0.0
    best_seq = 0.0

    for k in yaml_steps.keys():
        if not k:
            continue
        if is_too_generic_for_fuzzy(k):
            continue

        k_tokens = set(k.split(" "))
        jac = jaccard(s_tokens, k_tokens)
        if jac < 0.60:
            continue
        seq = difflib.SequenceMatcher(None, key, k).ratio()
        if seq < 0.60:
            continue
        score = 0.65 * jac + 0.35 * seq
        if score > best_score:
            best_score = score
            best_k = k
            best_j = jac
            best_seq = seq

    if best_k is not None:
        return yaml_steps[best_k], f"fuzzy_strict:{best_score:.3f}|j={best_j:.3f}|s={best_seq:.3f}"

    return None, "no_match"


# =========================
# GitHub API client
# =========================
@dataclass
class TokenState:
    token: str
    remaining: Optional[int] = None
    reset_epoch: Optional[int] = None


class GitHubClient:
    def __init__(self, tokens: List[str]) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "stage3-v16-adjusted/1.1",
        })
        self.tokens = [TokenState(t) for t in tokens]

    def _pick_idx(self) -> int:
        now = int(time.time())
        candidates = []
        for i, st in enumerate(self.tokens):
            if st.remaining is None or st.remaining > 0:
                candidates.append((0, i))
            else:
                if st.reset_epoch is not None and st.reset_epoch <= now:
                    candidates.append((0, i))
                else:
                    candidates.append((1, i))
        candidates.sort()
        return candidates[0][1]

    def _sleep_until_reset(self) -> None:
        now = int(time.time())
        resets = [st.reset_epoch for st in self.tokens if st.reset_epoch]
        if not resets:
            time.sleep(5)
            return
        soonest = min(resets)
        sleep_s = max(1, soonest - now + 2)
        print(f"[rate-limit] sleeping {sleep_s}s until reset...")
        time.sleep(sleep_s)

    def _backoff(self, attempt: int) -> None:
        sleep_s = min(BACKOFF_CAP_S, (BACKOFF_BASE_S ** attempt)) + random.random()
        time.sleep(sleep_s)

    def request_json(self, method: str, url: str, params: Optional[Dict] = None) -> Optional[Union[Dict, List]]:
        last_status = None
        for attempt in range(1, MAX_RETRIES_PER_REQUEST + 1):
            idx = self._pick_idx()
            st = self.tokens[idx]
            self.session.headers["Authorization"] = f"Bearer {st.token}"
            try:
                resp = self.session.request(method, url, params=params, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
            except requests.exceptions.RequestException:
                self._backoff(attempt)
                continue

            last_status = resp.status_code

            rem = resp.headers.get("X-RateLimit-Remaining")
            if rem is not None:
                try:
                    st.remaining = int(rem)
                except Exception:
                    pass
            rst = resp.headers.get("X-RateLimit-Reset")
            if rst is not None:
                try:
                    st.reset_epoch = int(rst)
                except Exception:
                    pass

            if resp.status_code == 404:
                return None

            retry_after = resp.headers.get("Retry-After")
            if resp.status_code in (403, 429) and retry_after:
                try:
                    ra = int(retry_after)
                    time.sleep(min(BACKOFF_CAP_S, max(1, ra)) + random.random())
                    continue
                except Exception:
                    pass

            text_l = (resp.text or "").lower()
            if resp.status_code in (403, 429) and (
                "rate limit" in text_l
                or "secondary rate limit" in text_l
                or "abuse detection" in text_l
                or "too many requests" in text_l
            ):
                if any((t.remaining is None) or (t.remaining > 0) for t in self.tokens):
                    self._backoff(attempt)
                    continue
                self._sleep_until_reset()
                continue

            if resp.status_code in (500, 502, 503, 504):
                self._backoff(attempt)
                continue

            if resp.status_code >= 400:
                return None

            try:
                return resp.json()
            except Exception:
                return None

        print(f"[giveup] {method} {url} after {MAX_RETRIES_PER_REQUEST} tries (last_status={last_status})")
        return None

    def paginate(self, url: str, params: Optional[Dict], item_key: str) -> Iterable[Dict]:
        page = 1
        while page <= MAX_PAGES_PER_LIST:
            p = dict(params or {})
            p.update({"per_page": 100, "page": page})
            data = self.request_json("GET", url, params=p)
            if data is None:
                return
            items = data if isinstance(data, list) else data.get(item_key, [])
            if not items:
                return
            for it in items:
                yield it
            if isinstance(items, list) and len(items) < 100:
                return
            page += 1


# =========================
# GitHub endpoints
# =========================
def list_run_jobs(gh: GitHubClient, full_name: str, run_id: int) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/runs/{run_id}/jobs"
    return list(gh.paginate(url, params={}, item_key="jobs"))


def fetch_workflow_yaml(gh: GitHubClient, full_name: str, workflow_path: str, ref: str) -> str:
    url = f"https://api.github.com/repos/{full_name}/contents/{workflow_path.lstrip('/')}"
    data = gh.request_json("GET", url, params={"ref": ref})
    if not data or not isinstance(data, dict):
        return ""
    if data.get("encoding") == "base64" and data.get("content"):
        try:
            return base64.b64decode(data["content"]).decode("utf-8", errors="ignore")
        except Exception:
            return ""
    dl = data.get("download_url")
    if dl:
        try:
            r = gh.session.get(dl, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
            if r.status_code == 200:
                return r.text or ""
        except requests.exceptions.RequestException:
            return ""
    return ""


# =========================
# YAML step extraction
# =========================
def _count_leading_spaces(s: str) -> int:
    return len(s) - len(s.lstrip(" "))


def parse_workflow_steps(yaml_text: str) -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not yaml_text:
        return out

    lines = yaml_text.splitlines()
    n = len(lines)
    i = 0

    while i < n:
        line = lines[i]
        m = re.match(r"^(\s*)-\s*name\s*:\s*(.+?)\s*$", line)
        if not m:
            i += 1
            continue

        base_indent = len(m.group(1))
        step_name = m.group(2).strip().strip('"').strip("'")
        key = normalize_step_key(step_name)

        j = i + 1
        block_lines = [line]
        while j < n:
            nxt = lines[j]
            m2 = re.match(r"^(\s*)-\s*name\s*:\s*(.+?)\s*$", nxt)
            if m2 and len(m2.group(1)) == base_indent:
                break
            block_lines.append(nxt)
            j += 1

        block = "\n".join(block_lines)

        uses_val = ""
        m_uses = re.search(r"(?mi)^\s*uses\s*:\s*([^\n\r#]+)", block)
        if m_uses:
            uses_val = m_uses.group(1).strip().strip('"').strip("'")

        run_val = ""
        m_run = re.search(r"(?mi)^\s*run\s*:\s*(.*)$", block)
        if m_run:
            run_line_text = m_run.group(0)
            run_start_idx = None
            for idx, bl in enumerate(block_lines):
                if bl.strip() == run_line_text.strip():
                    run_start_idx = idx
                    break
            if run_start_idx is not None:
                run_indent = _count_leading_spaces(block_lines[run_start_idx])
                rhs = block_lines[run_start_idx].split("run:", 1)[1].strip()
                if rhs in ("|", ">"):
                    k = run_start_idx + 1
                    acc = []
                    while k < len(block_lines):
                        l = block_lines[k]
                        if l.strip() == "":
                            acc.append("")
                            k += 1
                            continue
                        if _count_leading_spaces(l) <= run_indent:
                            break
                        acc.append(l.strip("\n"))
                        k += 1
                    run_val = "\n".join(acc).strip()
                else:
                    run_val = rhs.strip()

        with_script = ""
        with_line = None
        for idx, bl in enumerate(block_lines):
            if re.match(r"^\s*with\s*:\s*$", bl):
                with_line = idx
                break
        if with_line is not None:
            with_indent = _count_leading_spaces(block_lines[with_line])
            k = with_line + 1
            while k < len(block_lines):
                l = block_lines[k]
                if l.strip() == "":
                    k += 1
                    continue
                if _count_leading_spaces(l) <= with_indent:
                    break
                m_script = re.match(r"^\s*script\s*:\s*(.*)\s*$", l)
                if m_script:
                    rhs = m_script.group(1).strip()
                    script_indent = _count_leading_spaces(l)
                    if rhs in ("|", ">"):
                        kk = k + 1
                        acc = []
                        while kk < len(block_lines):
                            ll = block_lines[kk]
                            if ll.strip() == "":
                                acc.append("")
                                kk += 1
                                continue
                            if _count_leading_spaces(ll) <= script_indent:
                                break
                            acc.append(ll.strip("\n"))
                            kk += 1
                        with_script = "\n".join(acc).strip()
                    else:
                        with_script = rhs
                    break
                k += 1

        out[key] = {
            "name": step_name,
            "run": run_val or "",
            "uses": uses_val or "",
            "with_script": with_script or "",
            "blob": block,
        }
        i = j

    return out


def parse_job_reusable_uses(yaml_text: str) -> Dict[str, str]:
    if not yaml_text:
        return {}
    out: Dict[str, str] = {}

    lines = yaml_text.splitlines()
    n = len(lines)

    jobs_i = None
    for i, line in enumerate(lines):
        if re.match(r"^\s*jobs\s*:\s*$", line):
            jobs_i = i
            break
    if jobs_i is None:
        return {}

    jobs_indent = _count_leading_spaces(lines[jobs_i])
    i = jobs_i + 1

    while i < n:
        line = lines[i]
        if line.strip() == "":
            i += 1
            continue

        indent = _count_leading_spaces(line)
        if indent <= jobs_indent:
            break

        m_job = re.match(r"^\s*([A-Za-z0-9_.-]+)\s*:\s*$", line)
        if not m_job or indent != jobs_indent + 2:
            i += 1
            continue

        job_id = m_job.group(1).strip()
        block_indent = indent
        j = i + 1
        block_lines = []
        while j < n:
            nxt = lines[j]
            if nxt.strip() == "":
                block_lines.append(nxt)
                j += 1
                continue
            nxt_indent = _count_leading_spaces(nxt)
            if nxt_indent <= block_indent:
                break
            block_lines.append(nxt)
            j += 1

        block = "\n".join(block_lines)
        m_name = re.search(r"(?mi)^\s*name\s*:\s*(.+?)\s*$", block)
        job_display = (m_name.group(1).strip().strip('"').strip("'") if m_name else "")

        m_uses = re.search(r"(?mi)^\s*uses\s*:\s*(.+?)\s*$", block)
        uses_val = (m_uses.group(1).strip().strip('"').strip("'") if m_uses else "")

        if uses_val and uses_val.startswith("./.github/workflows/"):
            out[job_id] = uses_val
            if job_display:
                out[job_display] = uses_val

        i = j

    return out


# =========================
# Patterns
# =========================
GRADLE_INVOKE_PREFIX = r"(?:^\s*|[ \t\r\n;&|()\"'`])"

THIRD_PARTY_PROVIDER_RE = re.compile(
    r"(?ism)\b("
    r"hub\.browserstack\.com|browserstack|bstack|"
    r"sauce(labs)?|saucectl|"
    r"\bappcenter\b|microsoft/appcenter|"
    r"emulator\.wtf|"
    r"maestro\s+cloud"
    r")\b"
)

THIRD_PARTY_INSTRU_INVOKE_RE = re.compile(
    r"(?ism)\b("
    r"(gcloud\s+firebase\s+test\s+android\s+run\b[\s\S]*?--type\s+instrumentation)|"
    r"(firebase\s+test\s+android\s+run\b[\s\S]*?--type\s+instrumentation)|"
    r"(flank\s+android\s+run\b)|"
    r"(appcenter\s+test\s+run\s+espresso\b)|"
    r"(appcenter\s+test\s+run\s+android\b[\s\S]*?\bespresso\b)|"
    r"(appcenter\s+test\s+run\s+android\b[\s\S]*?\binstrumentation\b)"
    r")\b"
)

LOCAL_INSTRU_INVOKE_RE = re.compile(
    r"(?ism)\b("
    r"adb\s+shell\s+am\s+instrument|"
    r"\bconnected\w*androidtest\b|\bconnectedcheck\b|\bdevicecheck\b|\balldevicescheck\b|"
    r"\bmanageddevice\w*check\b|\bmanageddevice\w*androidtest\b|"
    r"\bdevice\w*androidtest\b"
    r")\b"
)

FLUTTER_IT_RE = re.compile(r"(?is)\bflutter\s+(?:test|drive)\b")
FLUTTER_IT_ANDROID_HINT_RE = re.compile(r"(?is)\b(integration_test|--driver\b|test_driver)\b")
FLUTTER_DEVICE_FLAG_RE = re.compile(r"(?is)\s+-d\s+(?P<dev>\"[^\"]+\"|'[^']+'|\S+)")
FLUTTER_DEVICE_IS_ANDROID_RE = re.compile(r"(?is)\b(android|emulator-\d+|sdk\s+gphone|android\s+sdk\s+built\s+for|pixel)\b")

DETOX_INVOKE_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}("
    r"(?:yarn|npm|pnpm)\s+[^\n\r]*\bdetox(?::[a-z0-9:_-]+)?\b|"
    r"(?:npx\s+detox\s+test\b)|"
    r"(?:detox\s+test\b)"
    r")"
)

EMU_COMMUNITY_ACTION_RE = re.compile(
    r"(reactivecircus/android-emulator-runner|android-emulator-runner|malinskiy/action-android)",
    re.IGNORECASE,
)

EMU_CUSTOM_SCRIPT_RE = re.compile(
    r"(?ism)\b(avdmanager|sdkmanager|emulator\b|start[-_ ]emulator|adb\s+wait[- ]?for[- ]?device)\b"
)

REAL_DEVICE_ADB_RE = re.compile(r"(?mi)\badb\s+-s\s+(?!emulator-\d+\b)(?!localhost:\d+\b)(?!127\.0\.0\.1:\d+\b)\S+\b")

ENV_ANY_RE = re.compile(
    r"(reactivecircus|android-emulator-runner|malinskiy/action-android|emulator\b|avd\b|avdmanager|sdkmanager|"
    r"start[-_ ]emulator|android-wait-for-emulator|adb\s+wait[- ]?for[- ]?device|kvm|"
    r"android-actions/setup-android|"
    r"browserstack/github-actions|saucelabs/sauce-connect-action|microsoft/appcenter|"
    r"google-github-actions/(auth|setup-gcloud)"
    r")",
    re.IGNORECASE,
)

ARTIFACT_RE = re.compile(r"(upload[- ]artifact|actions/upload-artifact)", re.IGNORECASE)

GRADLE_CMD_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradlew\.bat\b|gradle\s+)"
)

GMD_SETUP_TASK_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r":[\w:-]*api\d+setup|"
    r":[\w:-]*pixel[\w-]*api\d+setup|"
    r"manageddevice[\w-]*setup|"
    r"pixel[\w-]*api\d+setup"
    r")\b"
)

GMD_LIFECYCLE_TASK_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"manageddevice[\w:-]*(check|androidtest|test)|"
    r":[\w:-]*manageddevice[\w:-]*(check|androidtest|test)"
    r")\b"
)

BASELINE_PROFILE_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"generatebaselineprofile|"
    r"baselineprofile"
    r")\b"
)

GMD_VARIANT_DEVICE_ANDROIDTEST_RE = re.compile(
    rf"(?ism){GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b([a-z0-9]+api\d+\w*androidtest|pixel[a-z0-9_-]*api\d+\w*androidtest)\b"
)

INSTRU_TASK_NAME_HINT_RE = re.compile(
    r"(?i)\b("
    r"androidtest|connectedcheck|devicecheck|alldevicescheck|"
    r"manageddevice|instrumentation|am\s+instrument|"
    r"firebase\s+test|flank|"
    r"detox|flutter.*(integration|drive)|integration_test"
    r")\b"
)

_SPLIT_COMMA_RE = re.compile(r"\s*,\s*")


# =========================
# Android-ish gating helpers
# =========================
def _runtime_evidence_from_text(text: str) -> Dict[str, bool]:
    t = sanitize_gha_expr(text or "")
    low = t.lower()
    return {
        "emu_comm": bool(EMU_COMMUNITY_ACTION_RE.search(low)),
        "emu_custom": bool(EMU_CUSTOM_SCRIPT_RE.search(low)),
        "real_device": bool(REAL_DEVICE_ADB_RE.search(low)),
        "third_party_invoke": bool(THIRD_PARTY_INSTRU_INVOKE_RE.search(low)),
    }


def _flutter_androidish_from_text(text: str, runtime_ev: Dict[str, bool]) -> bool:
    t = sanitize_gha_expr(text or "")
    low = t.lower()

    if not (FLUTTER_IT_RE.search(low) and FLUTTER_IT_ANDROID_HINT_RE.search(low)):
        return False

    targeted = False
    for m in FLUTTER_DEVICE_FLAG_RE.finditer(low):
        dev = (m.group("dev") or "").strip().strip('"').strip("'").lower()
        if FLUTTER_DEVICE_IS_ANDROID_RE.search(dev):
            targeted = True
            break

    if FLUTTER_DEVICE_IS_ANDROID_RE.search(low):
        targeted = True

    if targeted:
        return True

    if runtime_ev.get("emu_comm") or runtime_ev.get("emu_custom") or runtime_ev.get("real_device") or runtime_ev.get("third_party_invoke"):
        return True

    return False


def _detox_androidish_from_text(text: str, runtime_ev: Dict[str, bool]) -> bool:
    t = sanitize_gha_expr(text or "")
    low = t.lower()

    if not DETOX_INVOKE_RE.search(low):
        return False

    if runtime_ev.get("emu_comm") or runtime_ev.get("emu_custom") or runtime_ev.get("real_device") or runtime_ev.get("third_party_invoke"):
        return True

    if re.search(r"(?is)\b(android|emulator|avd|adb)\b", low):
        return True

    return False


# =========================
# Stage-1 anchor name matching
# =========================
def parse_stage1_anchor_names(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [p.strip() for p in _SPLIT_COMMA_RE.split(raw) if p.strip()]
    return unique_preserve(parts)


def step_matches_stage1_anchor(step_name: str, stage1_anchor_names: List[str]) -> Tuple[bool, str]:
    if not step_name or not stage1_anchor_names:
        return False, ""

    key = normalize_step_key(step_name)
    s_tok = token_set(step_name)

    for n in stage1_anchor_names:
        if key == normalize_step_key(n):
            return True, "stage1_anchor_exact_norm"

    for n in stage1_anchor_names:
        nk = normalize_step_key(n)
        if nk and (nk in key or key in nk):
            return True, "stage1_anchor_substring_norm"

    best_score = 0.0
    for n in stage1_anchor_names:
        nt = token_set(n)
        if not nt:
            continue
        jac = jaccard(s_tok, nt)
        if jac > best_score:
            best_score = jac
    if best_score >= 0.80:
        return True, f"stage1_anchor_jaccard:{best_score:.3f}"

    return False, ""


# =========================
# Category detection (YAML-based)
# =========================
def compute_category_from_yaml(step_name: str, y: Optional[Dict[str, str]]) -> Tuple[str, str]:
    if not y:
        if is_runner_injected_step(step_name):
            return "other", "runner_injected_no_yaml"
        return "other", "no_yaml_block"

    run_txt = y.get("run", "") or ""
    uses_txt = y.get("uses", "") or ""
    script_txt = y.get("with_script", "") or ""
    blob = y.get("blob", "") or ""

    combined_raw = "\n".join([step_name or "", run_txt, uses_txt, script_txt, blob])
    step_local_raw = "\n".join([step_name or "", run_txt, uses_txt, script_txt])

    combined_raw = sanitize_gha_expr(combined_raw)
    step_local_raw = sanitize_gha_expr(step_local_raw)

    if ARTIFACT_RE.search(combined_raw):
        return "artifact", "artifact_upload_signal"

    runtime_ev = _runtime_evidence_from_text(combined_raw)
    flutter_androidish = _flutter_androidish_from_text(step_local_raw, runtime_ev)
    detox_androidish = _detox_androidish_from_text(step_local_raw, runtime_ev)

    if THIRD_PARTY_INSTRU_INVOKE_RE.search(step_local_raw):
        return "test", "third_party_instru_invoke"

    if flutter_androidish:
        return "test", "flutter_integration_androidish"
    if detox_androidish:
        return "test", "detox_androidish"

    is_gradle = bool(GRADLE_CMD_RE.search(step_local_raw))
    if LOCAL_INSTRU_INVOKE_RE.search(step_local_raw) and (("adb shell am instrument" in step_local_raw.lower()) or is_gradle):
        return "test", "local_instru_invoke"

    is_androidtest = bool(re.search(r"(?i)\b\w*androidtest\b", step_local_raw)) and is_gradle
    if is_androidtest:
        return "test", "gradle_androidtest_invocation"

    if ENV_ANY_RE.search(combined_raw):
        return "env_setup", "env_setup_signal"

    if is_gradle:
        return "gradle", "gradle_non_test"

    return "other", "no_phase_signal"


def _snip(s: str, n: int = 240) -> str:
    s = (s or "").replace("\r", "")
    s = _norm_ws_re.sub(" ", s).strip()
    return s if len(s) <= n else (s[: n - 3] + "...")


# =========================
# Step classification + style tagging
# =========================
def classify_step(
    step_name: str,
    y: Optional[Dict[str, str]],
    styles_text: str,
    workflow_identifier: str,
    workflow_path: str,
    job_name: str,
    stage1_anchor_names: List[str],
) -> Dict[str, Union[bool, str]]:
    y = y or {}
    run_txt = y.get("run", "") or ""
    uses_txt = y.get("uses", "") or ""
    script_txt = y.get("with_script", "") or ""
    blob = y.get("blob", "") or ""

    combined_raw = "\n".join([step_name or "", run_txt, uses_txt, script_txt, blob])
    step_local_raw = "\n".join([step_name or "", run_txt, uses_txt, script_txt])

    combined = sanitize_gha_expr(combined_raw)
    step_local = sanitize_gha_expr(step_local_raw)

    styles_l = (styles_text or "").lower()
    wi_l = (workflow_identifier or "").lower()
    wp_l = (workflow_path or "").lower()
    jn_l = (job_name or "").lower()

    runtime_ev = _runtime_evidence_from_text(combined)
    flutter_androidish = _flutter_androidish_from_text(step_local, runtime_ev)
    detox_androidish = _detox_androidish_from_text(step_local, runtime_ev)

    third_party_provider = bool(THIRD_PARTY_PROVIDER_RE.search(step_local)) or bool(THIRD_PARTY_PROVIDER_RE.search(combined))
    third_party_instru_invoke = bool(THIRD_PARTY_INSTRU_INVOKE_RE.search(step_local))

    local_instru_invoke = bool(LOCAL_INSTRU_INVOKE_RE.search(step_local))
    is_gradle = bool(GRADLE_CMD_RE.search(step_local))
    gradle_androidtest = bool(re.search(r"(?i)\b(\w*androidtest)\b", step_local)) and is_gradle

    explicit_instru_exec = bool(
        (local_instru_invoke and (("adb shell am instrument" in step_local.lower()) or is_gradle))
        or gradle_androidtest
        or third_party_instru_invoke
        or flutter_androidish
        or detox_androidish
    )

    has_gmd_context = ("gmd" in styles_l) or ("gmd" in wi_l) or ("gmd" in wp_l) or ("gmd" in jn_l)
    gmd_setup = bool(GMD_SETUP_TASK_RE.search(step_local)) or bool(re.search(r"(?i)\bsetup\s+gmd\b", step_name or ""))
    gmd_lifecycle_task = bool(GMD_LIFECYCLE_TASK_RE.search(step_local))

    gmd_variant_device_androidtest = has_gmd_context and bool(GMD_VARIANT_DEVICE_ANDROIDTEST_RE.search(step_local))
    if gmd_variant_device_androidtest:
        gmd_lifecycle_task = True

    baseline_profile = bool(BASELINE_PROFILE_RE.search(step_local)) or bool(re.search(r"(?i)\bbaseline\s*profile\b", step_name or ""))

    env_setup = bool(ENV_ANY_RE.search(combined))
    artifact = bool(ARTIFACT_RE.search(combined))

    emu_community_action = bool(EMU_COMMUNITY_ACTION_RE.search(combined))
    emu_custom_script = bool(EMU_CUSTOM_SCRIPT_RE.search(step_local)) and not emu_community_action
    real_device = bool(REAL_DEVICE_ADB_RE.search(combined))

    stage1_anchor_match, stage1_anchor_match_reason = step_matches_stage1_anchor(step_name, stage1_anchor_names)

    return {
        "explicit_instru": explicit_instru_exec,
        "third_party_provider": third_party_provider,
        "third_party_instru_invoke": third_party_instru_invoke,
        "flutter_integration_androidish": flutter_androidish,
        "detox_androidish": detox_androidish,
        "gmd_setup": gmd_setup,
        "gmd_lifecycle_task": gmd_lifecycle_task,
        "baseline_profile": baseline_profile,
        "env_setup": env_setup,
        "gradle": is_gradle,
        "gradle_androidtest": gradle_androidtest,
        "artifact": artifact,
        "emu_community_action": emu_community_action,
        "emu_custom_script": emu_custom_script,
        "real_device": real_device,
        "stage1_anchor_match": stage1_anchor_match,
        "stage1_anchor_match_reason": stage1_anchor_match_reason,
    }


def is_exec_step(flags: Dict[str, Union[bool, str]]) -> bool:
    return bool(
        flags.get("explicit_instru")
        or flags.get("third_party_instru_invoke")
        or flags.get("gmd_lifecycle_task")
        or flags.get("emu_community_action")
        or flags.get("gradle_androidtest")
        or flags.get("baseline_profile")
        or flags.get("flutter_integration_androidish")
        or flags.get("detox_androidish")
    )


# =========================
# Anchor selection
# =========================
def pick_instru_anchor_from_candidates(
    cands: List[Tuple[datetime, str, str, Dict[str, Union[bool, str]]]]
) -> Tuple[Optional[datetime], str, str, str, Dict[str, Union[bool, str]]]:
    """
    Returns:
      (anchor_step_start_dt, anchor_step_name, anchor_job_name, anchor_source, anchor_flags)
    """
    if not cands:
        return None, "", "", "missing", {}

    tier0 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("stage1_anchor_match")]
    if tier0:
        tier0_exec = [(t, n, jn, f) for (t, n, jn, f) in tier0 if is_exec_step(f)]
        src = "stage1_anchor_name_match"
        pick_from = tier0_exec if tier0_exec else tier0
        pick_from.sort(key=lambda x: x[0])
        t, n, jn, f = pick_from[0]
        reason = f.get("stage1_anchor_match_reason") or ""
        return t, n, jn, (f"{src}:{reason}" if reason else src), f

    tier1 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("explicit_instru") or f.get("third_party_instru_invoke")]
    if tier1:
        tier1.sort(key=lambda x: x[0])
        t, n, jn, f = tier1[0]
        return t, n, jn, "explicit_instru_step", f

    tier2 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("gmd_setup") or f.get("gmd_lifecycle_task")]
    if tier2:
        tier2.sort(key=lambda x: x[0])
        t, n, jn, f = tier2[0]
        return t, n, jn, "gmd_setup_step", f

    tier3 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("emu_community_action")]
    if tier3:
        tier3.sort(key=lambda x: x[0])
        t, n, jn, f = tier3[0]
        return t, n, jn, "emu_runner_action_step", f

    tier4 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("emu_custom_script")]
    if tier4:
        tier4.sort(key=lambda x: x[0])
        t, n, jn, f = tier4[0]
        return t, n, jn, "scripted_emulator_step", f

    tier5 = [(t, n, jn, f) for (t, n, jn, f) in cands if f.get("baseline_profile")]
    if tier5:
        tier5.sort(key=lambda x: x[0])
        t, n, jn, f = tier5[0]
        return t, n, jn, "baseline_profile_step", f

    return None, "", "", "missing", {}


# =========================
# Metric computation
# =========================
STYLE_METRIC_KEYS = [
    # existing TTFTS (run_started -> anchor step start)
    "first_test_step_started_at",
    "ttfts_seconds",
    "ttfts_source",

    # NEW modified TTFTS (anchor job start -> anchoring step start)
    "modified_ttfts_seconds",
    "modified_ttfts_source",
    "modified_ttfts_quality",

    # NEW job-count-to-anchor metric (inclusive count)
    "jobs_to_anchor_job_count",
    "jobs_to_anchor_job_count_source",

    "instru_started_at",
    "instru_ended_at",
    "test_exec_started_at",
    "test_exec_ended_at",

    "instru_duration_seconds",
    "pre_test_overhead_seconds",
    "core_instru_window_seconds",
    "post_test_overhead_seconds",
    "instru_exec_sum_seconds",
    "instru_exec_window_seconds",
    "instru_exec_step_count",

    "env_setup_sum_seconds",
    "artifact_sum_seconds",
]


def compute_metrics_for_event_set(
    base_start: Optional[datetime],
    events: List[Tuple[Optional[datetime], Optional[datetime], Optional[int], str, str, Dict[str, Union[bool, str]]]],
    stage2_instru_detect_method: str,
    s2_fallback: Dict[str, str],
) -> Dict[str, Union[str, int, None]]:

    out: Dict[str, Union[str, int, None]] = {
        "first_test_step_started_at": "",
        "ttfts_seconds": None,
        "ttfts_source": "missing",

        "modified_ttfts_seconds": None,
        "modified_ttfts_source": "missing",
        "modified_ttfts_quality": "missing",

        "jobs_to_anchor_job_count": None,
        "jobs_to_anchor_job_count_source": "missing",

        "instru_started_at": "",
        "instru_ended_at": "",
        "test_exec_started_at": "",
        "test_exec_ended_at": "",

        "instru_duration_seconds": None,
        "pre_test_overhead_seconds": None,
        "core_instru_window_seconds": None,
        "post_test_overhead_seconds": None,
        "instru_exec_sum_seconds": None,
        "instru_exec_window_seconds": None,
        "instru_exec_step_count": None,

        "env_setup_sum_seconds": None,
        "artifact_sum_seconds": None,
    }

    if not events:
        # S2 fallbacks
        s2_first = (s2_fallback.get("S2_instru_first_started_at") or "").strip()
        s2_ttfi = (s2_fallback.get("S2_time_to_first_instru_seconds") or "").strip()

        s2_mod_ttfts = (s2_fallback.get("S2_time_to_first_instru_from_anchor_job_seconds") or "").strip()
        s2_mod_ttfts_quality = (s2_fallback.get("S2_time_to_first_instru_from_anchor_job_quality") or "").strip()
        s2_anchor_job_start = (s2_fallback.get("S2_anchor_job_started_at") or "").strip()
        stage2_mod_ttfts = (s2_fallback.get("time_to_first_instru_from_anchor_job_seconds") or "").strip()
        stage2_mod_ttfts_quality = (s2_fallback.get("time_to_first_instru_from_anchor_job_quality") or "").strip()

        stage1_jobs_before_anchor_count = safe_int_from_str(s2_fallback.get("jobs_before_anchor_count", ""))

        # legacy ttfts fallback
        if s2_ttfi:
            try:
                out["ttfts_seconds"] = int(float(s2_ttfi))
                out["ttfts_source"] = "S2_time_to_first_instru_seconds"
            except Exception:
                pass

        if out["ttfts_seconds"] is None and base_start and s2_first:
            tt = dt_to_seconds(base_start, iso_to_dt(s2_first))
            if tt is not None:
                out["ttfts_seconds"] = tt
                out["ttfts_source"] = "S2_instru_first_started_at"

        # NEW modified TTFTS fallback order:
        # 1) S2 mirrored field
        # 2) direct Stage2 field (if present)
        # 3) derive from S2 anchor job start + S2 instru first started
        if s2_mod_ttfts:
            try:
                out["modified_ttfts_seconds"] = int(float(s2_mod_ttfts))
                out["modified_ttfts_source"] = "S2_time_to_first_instru_from_anchor_job_seconds"
                out["modified_ttfts_quality"] = s2_mod_ttfts_quality or "S2"
            except Exception:
                pass

        if out["modified_ttfts_seconds"] is None and stage2_mod_ttfts:
            try:
                out["modified_ttfts_seconds"] = int(float(stage2_mod_ttfts))
                out["modified_ttfts_source"] = "time_to_first_instru_from_anchor_job_seconds"
                out["modified_ttfts_quality"] = stage2_mod_ttfts_quality or "stage2"
            except Exception:
                pass

        if out["modified_ttfts_seconds"] is None and s2_anchor_job_start and s2_first:
            d = dt_to_seconds(iso_to_dt(s2_anchor_job_start), iso_to_dt(s2_first))
            if d is not None:
                out["modified_ttfts_seconds"] = d
                out["modified_ttfts_source"] = "S2_anchor_job_started_at_plus_S2_instru_first_started_at"
                out["modified_ttfts_quality"] = "derived"

        # jobs-to-anchor fallback (inclusive) from Stage1 pass-through if present
        if stage1_jobs_before_anchor_count is not None:
            out["jobs_to_anchor_job_count"] = stage1_jobs_before_anchor_count + 1
            out["jobs_to_anchor_job_count_source"] = "stage1_jobs_before_anchor_count_plus1"

        if (stage2_instru_detect_method or "").strip().lower().startswith("workflow_label"):
            if out["ttfts_seconds"] is None:
                out["ttfts_seconds"] = 0
                out["ttfts_source"] = "workflow_label_proxy"
            if out["modified_ttfts_seconds"] is None:
                out["modified_ttfts_seconds"] = 0
                out["modified_ttfts_source"] = "workflow_label_proxy"
                out["modified_ttfts_quality"] = "workflow_label_proxy"

        return out

    total_env = 0
    total_art = 0

    exec_first: Optional[datetime] = None
    exec_last: Optional[datetime] = None
    exec_sum = 0
    exec_count = 0

    cands: List[Tuple[datetime, str, str, Dict[str, Union[bool, str]]]] = []

    # job timing within the current event set (for modified TTFTS + job count)
    job_first_step_start: Dict[str, datetime] = {}

    # ---- FIX support: collect exec job scope and keep step times for constrained instru_end ----
    exec_job_names: Set[str] = set()
    step_times: List[Tuple[Optional[datetime], Optional[datetime], str]] = []  # (st_start, st_end, job_name)
    # ------------------------------------------------------------------------------------------

    for (st_start, st_end, st_dur, step_name, job_name, flags) in events:
        if flags.get("env_setup") and st_dur is not None:
            total_env += st_dur
        if flags.get("artifact") and st_dur is not None:
            total_art += st_dur

        if st_start:
            cands.append((st_start, step_name, job_name, flags))
            if job_name:
                prev = job_first_step_start.get(job_name)
                if prev is None or st_start < prev:
                    job_first_step_start[job_name] = st_start

        if is_exec_step(flags) and st_start and st_end:
            if exec_first is None or st_start < exec_first:
                exec_first = st_start
            if exec_last is None or st_end > exec_last:
                exec_last = st_end
            if st_dur is not None:
                exec_sum += st_dur
            exec_count += 1
            if job_name:
                exec_job_names.add(job_name)

        # FIX support: record step timing for later constrained selection of instru_end
        step_times.append((st_start, st_end, job_name or ""))

    out["env_setup_sum_seconds"] = total_env if total_env > 0 else None
    out["artifact_sum_seconds"] = total_art if total_art > 0 else None

    anchor_dt, _anchor_name, anchor_job_name, anchor_source, _anchor_flags = pick_instru_anchor_from_candidates(cands)

    if anchor_dt is None:
        fallback: List[Tuple[datetime, str, str, Dict[str, Union[bool, str]]]] = []
        for (t, n, jn, f) in cands:
            has_instru_evidence = bool(
                f.get("stage1_anchor_match")
                or f.get("explicit_instru")
                or f.get("third_party_instru_invoke")
                or f.get("gmd_setup")
                or f.get("gmd_lifecycle_task")
                or f.get("baseline_profile")
                or f.get("flutter_integration_androidish")
                or f.get("detox_androidish")
                or INSTRU_TASK_NAME_HINT_RE.search(n or "")
            )
            if has_instru_evidence:
                fallback.append((t, n, jn, f))

        if fallback:
            fallback.sort(key=lambda x: x[0])
            t = fallback[0]
            # support both legacy (4) and newer (5) tuple formats
            if len(t) == 5:
                anchor_dt, _anchor_name, anchor_job_name, _f_source, _f_flags = t
            elif len(t) == 4:
                anchor_dt, _anchor_name, anchor_job_name, _f_source = t
                _f_flags = ""
            else:
                # unexpected shape; treat as missing fallback
                anchor_dt, _anchor_name, anchor_job_name, _f_source, _f_flags = None, "", "", "missing", ""
            anchor_source = "fallback_instru_evidence"

    # legacy ttfts (run_started -> anchor step)
    if anchor_dt and base_start:
        out["instru_started_at"] = anchor_dt.isoformat().replace("+00:00", "Z")
        out["first_test_step_started_at"] = out["instru_started_at"]
        out["ttfts_seconds"] = dt_to_seconds(base_start, anchor_dt)
        out["ttfts_source"] = anchor_source
    else:
        if (stage2_instru_detect_method or "").strip().lower().startswith("workflow_label"):
            out["ttfts_seconds"] = 0
            out["ttfts_source"] = "workflow_label_proxy"

    # NEW modified TTFTS (anchor job start -> anchor step)
    if anchor_dt and anchor_job_name and anchor_job_name in job_first_step_start:
        anchor_job_start_dt = job_first_step_start[anchor_job_name]
        mod = dt_to_seconds(anchor_job_start_dt, anchor_dt)
        out["modified_ttfts_seconds"] = mod
        out["modified_ttfts_source"] = "runtime_anchor_job_earliest_step_to_anchor_step"
        out["modified_ttfts_quality"] = "runtime_observed"
    else:
        # Fallback to Stage2/S2 fields
        s2_mod_ttfts = (s2_fallback.get("S2_time_to_first_instru_from_anchor_job_seconds") or "").strip()
        s2_mod_ttfts_quality = (s2_fallback.get("S2_time_to_first_instru_from_anchor_job_quality") or "").strip()
        stage2_mod_ttfts = (s2_fallback.get("time_to_first_instru_from_anchor_job_seconds") or "").strip()
        stage2_mod_ttfts_quality = (s2_fallback.get("time_to_first_instru_from_anchor_job_quality") or "").strip()
        s2_anchor_job_start = (s2_fallback.get("S2_anchor_job_started_at") or "").strip()
        s2_first = (s2_fallback.get("S2_instru_first_started_at") or "").strip()

        if s2_mod_ttfts:
            try:
                out["modified_ttfts_seconds"] = int(float(s2_mod_ttfts))
                out["modified_ttfts_source"] = "S2_time_to_first_instru_from_anchor_job_seconds"
                out["modified_ttfts_quality"] = s2_mod_ttfts_quality or "S2"
            except Exception:
                pass

        if out["modified_ttfts_seconds"] is None and stage2_mod_ttfts:
            try:
                out["modified_ttfts_seconds"] = int(float(stage2_mod_ttfts))
                out["modified_ttfts_source"] = "time_to_first_instru_from_anchor_job_seconds"
                out["modified_ttfts_quality"] = stage2_mod_ttfts_quality or "stage2"
            except Exception:
                pass

        if out["modified_ttfts_seconds"] is None and s2_anchor_job_start and s2_first:
            d = dt_to_seconds(iso_to_dt(s2_anchor_job_start), iso_to_dt(s2_first))
            if d is not None:
                out["modified_ttfts_seconds"] = d
                out["modified_ttfts_source"] = "S2_anchor_job_started_at_plus_S2_instru_first_started_at"
                out["modified_ttfts_quality"] = "derived"

        if out["modified_ttfts_seconds"] is None and (stage2_instru_detect_method or "").strip().lower().startswith("workflow_label"):
            out["modified_ttfts_seconds"] = 0
            out["modified_ttfts_source"] = "workflow_label_proxy"
            out["modified_ttfts_quality"] = "workflow_label_proxy"

    # NEW jobs-to-anchor (inclusive)
    if anchor_job_name and job_first_step_start:
        ordered_jobs = sorted(job_first_step_start.items(), key=lambda kv: kv[1])
        idx = None
        for i, (jn, _dt) in enumerate(ordered_jobs):
            if jn == anchor_job_name:
                idx = i
                break
        if idx is not None:
            out["jobs_to_anchor_job_count"] = idx + 1
            out["jobs_to_anchor_job_count_source"] = "runtime_observed_job_order"
    if out["jobs_to_anchor_job_count"] is None:
        stage1_jobs_before_anchor_count = safe_int_from_str(s2_fallback.get("jobs_before_anchor_count", ""))
        if stage1_jobs_before_anchor_count is not None:
            out["jobs_to_anchor_job_count"] = stage1_jobs_before_anchor_count + 1
            out["jobs_to_anchor_job_count_source"] = "stage1_jobs_before_anchor_count_plus1"

    if exec_first:
        out["test_exec_started_at"] = exec_first.isoformat().replace("+00:00", "Z")
    if exec_last:
        out["test_exec_ended_at"] = exec_last.isoformat().replace("+00:00", "Z")

    # ------------------------------------------------------------------
    # FIX: constrain instru_end to instrumentation job scope
    #   prefer anchor job (if known), else exec-step job(s)
    # ------------------------------------------------------------------
    allowed_jobs: Optional[Set[str]] = None
    if anchor_job_name:
        allowed_jobs = {anchor_job_name}
    elif exec_job_names:
        allowed_jobs = set(exec_job_names)

    latest_end_in_scope_after_exec: Optional[datetime] = None
    if exec_first and allowed_jobs:
        for (st_start, st_end, jn) in step_times:
            if not st_start or not st_end:
                continue
            if jn not in allowed_jobs:
                continue
            if st_start >= exec_first:
                if latest_end_in_scope_after_exec is None or st_end > latest_end_in_scope_after_exec:
                    latest_end_in_scope_after_exec = st_end

    instru_end: Optional[datetime] = None
    if latest_end_in_scope_after_exec:
        instru_end = latest_end_in_scope_after_exec
    elif exec_last:
        instru_end = exec_last
    # ------------------------------------------------------------------

    if instru_end:
        out["instru_ended_at"] = instru_end.isoformat().replace("+00:00", "Z")

    if anchor_dt and instru_end:
        out["instru_duration_seconds"] = dt_to_seconds(anchor_dt, instru_end)

    if anchor_dt and exec_first:
        out["pre_test_overhead_seconds"] = dt_to_seconds(anchor_dt, exec_first)

    if exec_first and exec_last:
        out["core_instru_window_seconds"] = dt_to_seconds(exec_first, exec_last)
        out["instru_exec_window_seconds"] = out["core_instru_window_seconds"]
        out["instru_exec_sum_seconds"] = exec_sum if exec_sum > 0 else None
        out["instru_exec_step_count"] = exec_count if exec_count > 0 else None

    if exec_last and instru_end:
        out["post_test_overhead_seconds"] = dt_to_seconds(exec_last, instru_end)

    return out


# =========================
# Stage 3 builders
# =========================
def build_stage3_outputs_for_run(
    jobs: List[Dict],
    run_created_at: str,
    run_started_at: str,
    yaml_steps_main: Dict[str, Dict[str, str]],
    yaml_steps_by_job: Dict[str, Dict[str, Dict[str, str]]],
    styles_text: str,
    stage2_instru_detect_method: str,
    s2_fallback: Dict[str, str],
    stage1_anchor_names: List[str],
    full_name: str,
    run_id: str,
    workflow_identifier: str,
    workflow_path: str,
    head_sha: str,
) -> Tuple[Dict[str, Union[str, int, float, None]], List[Dict[str, str]], List[Dict[str, str]]]:

    run_metrics: Dict[str, Union[str, int, float, None]] = {k: ("" if k.endswith("_at") else None) for k in STYLE_METRIC_KEYS}
    run_metrics.update({
        "ttfts_source": "missing",
        "modified_ttfts_source": "missing",
        "modified_ttfts_quality": "missing",
        "jobs_to_anchor_job_count_source": "missing",
        "third_party_job_count": 0,
        "third_party_job_names": "",
        "third_party_provider_job_count": 0,
        "third_party_provider_job_names": "",
    })

    step_rows: List[Dict[str, str]] = []
    per_style_rows: List[Dict[str, str]] = []

    base_start = iso_to_dt(run_started_at) or iso_to_dt(run_created_at)

    if not jobs:
        baseline = compute_metrics_for_event_set(base_start, [], stage2_instru_detect_method, s2_fallback)
        for k in STYLE_METRIC_KEYS:
            run_metrics[k] = baseline.get(k)

        declared = split_styles(styles_text) or [""]
        for s in declared:
            row = {"style": s}
            for k in STYLE_METRIC_KEYS:
                v = run_metrics.get(k)
                row[k] = "" if v is None else str(v)
            per_style_rows.append(row)

        return run_metrics, step_rows, per_style_rows

    tmp_steps: List[
        Tuple[
            str, str, Optional[datetime], Optional[datetime], Optional[int], Dict[str, Union[bool, str]],
            str, str, str, str, str, str, str, str, str, str, str, str
        ]
    ] = []

    third_party_job_names: List[str] = []
    third_party_provider_job_names: List[str] = []
    third_party_count = 0
    third_party_provider_count = 0

    counted_tp_exec_jobs: Set[str] = set()
    counted_tp_provider_jobs: Set[str] = set()

    for j in jobs:
        job_id = str(j.get("id") or "")
        job_name = (j.get("name") or "").strip()
        steps = j.get("steps") if isinstance(j.get("steps"), list) else []

        job_start = iso_to_dt(j.get("started_at"))
        job_end = iso_to_dt(j.get("completed_at"))

        yaml_steps = dict(yaml_steps_main or {})
        extra = yaml_steps_by_job.get(job_name) or {}
        if extra:
            yaml_steps.update(extra)

        for st in steps:
            step_name = (st.get("name") or "").strip()
            if not step_name:
                continue

            st_start = iso_to_dt(st.get("started_at")) or job_start
            st_end = iso_to_dt(st.get("completed_at")) or job_end

            step_norm_key = normalize_step_key(step_name)
            runner_injected = "1" if is_runner_injected_step(step_name) else "0"

            y = None
            yaml_match = "NO"
            yaml_match_reason = ""
            yaml_step_name = ""
            yaml_run_snip = ""
            yaml_uses_snip = ""
            yaml_with_script_snip = ""
            yaml_block_snip = ""

            if runner_injected == "1":
                yaml_match_reason = "skip_runner_injected"
            else:
                y = yaml_steps.get(step_norm_key)
                if y is not None:
                    yaml_match = "YES"
                    yaml_match_reason = "exact_norm"
                else:
                    y, yaml_match_reason = best_yaml_step_match_with_reason(step_name, yaml_steps)
                    if y is not None:
                        yaml_match = "YES"

            if y:
                yaml_step_name = y.get("name", "") or ""
                yaml_run_snip = _snip(y.get("run", "") or "", 220)
                yaml_uses_snip = _snip(y.get("uses", "") or "", 220)
                yaml_with_script_snip = _snip(y.get("with_script", "") or "", 220)
                yaml_block_snip = _snip(y.get("blob", "") or "", 280)

            flags = classify_step(
                step_name=step_name,
                y=y,
                styles_text=styles_text,
                workflow_identifier=workflow_identifier,
                workflow_path=workflow_path,
                job_name=job_name,
                stage1_anchor_names=stage1_anchor_names,
            )

            if flags.get("third_party_provider") and job_name and job_name not in counted_tp_provider_jobs:
                counted_tp_provider_jobs.add(job_name)
                third_party_provider_count += 1
                third_party_provider_job_names.append(job_name)

            if flags.get("third_party_instru_invoke") and job_name and job_name not in counted_tp_exec_jobs:
                counted_tp_exec_jobs.add(job_name)
                third_party_count += 1
                third_party_job_names.append(job_name)

            st_dur = dt_to_seconds(iso_to_dt(st.get("started_at")), iso_to_dt(st.get("completed_at")))
            if st_dur is None:
                st_dur = dt_to_seconds(job_start, job_end)

            category, category_reason = compute_category_from_yaml(step_name, y)

            tmp_steps.append((
                job_id, job_name, st_start, st_end, st_dur, flags, step_name, category, category_reason,
                step_norm_key, runner_injected,
                yaml_match, yaml_match_reason, yaml_step_name,
                yaml_run_snip, yaml_uses_snip, yaml_with_script_snip, yaml_block_snip
            ))

    declared_styles = split_styles(styles_text) or [""]
    is_multi_style = len([s for s in declared_styles if s]) > 1
    first_style = declared_styles[0] if (len(declared_styles) == 1) else ""

    tmp_steps_sorted = sorted(tmp_steps, key=lambda x: (x[2] or datetime.min.replace(tzinfo=timezone.utc)))

    current_style = ""
    seen_first_anchor = False

    all_events: List[Tuple[Optional[datetime], Optional[datetime], Optional[int], str, str, Dict[str, Union[bool, str]]]] = []
    events_by_style: Dict[str, List[Tuple[Optional[datetime], Optional[datetime], Optional[int], str, str, Dict[str, Union[bool, str]]]]] = {}

    for (
        job_id, job_name, st_start, st_end, st_dur, flags, step_name, category, category_reason,
        step_norm_key, runner_injected, yaml_match, yaml_match_reason, yaml_step_name,
        yaml_run_snip, yaml_uses_snip, yaml_with_script_snip, yaml_block_snip
    ) in tmp_steps_sorted:

        if not is_multi_style:
            if not seen_first_anchor:
                if category == "test" or is_exec_step(flags) or bool(flags.get("stage1_anchor_match")):
                    seen_first_anchor = True
                else:
                    step_rows.append({
                        "full_name": full_name,
                        "run_id": run_id,
                        "workflow_identifier": workflow_identifier,
                        "workflow_path": workflow_path,
                        "head_sha": head_sha,
                        "styles": styles_text,
                        "job_id": job_id,
                        "job_name": job_name,
                        "step_name": step_name,
                        "category": category,
                        "category_reason": category_reason,
                        "step_style_tag": "",
                        "step_style_reason": "",
                        "started_at": st_start.isoformat() if st_start else "",
                        "completed_at": st_end.isoformat() if st_end else "",
                        "duration_seconds": "" if st_dur is None else str(st_dur),
                        "step_norm_key": step_norm_key,
                        "runner_injected": runner_injected,
                        "yaml_match": yaml_match,
                        "yaml_match_reason": yaml_match_reason,
                        "yaml_step_name": yaml_step_name,
                        "yaml_run_snip": yaml_run_snip,
                        "yaml_uses_snip": yaml_uses_snip,
                        "yaml_with_script_snip": yaml_with_script_snip,
                        "yaml_block_snip": yaml_block_snip,
                        "stage1_anchor_match": "1" if flags.get("stage1_anchor_match") else "0",
                        "stage1_anchor_match_reason": str(flags.get("stage1_anchor_match_reason") or ""),
                    })
                    continue

            step_style_tag = first_style
            step_style_reason = "single_style_run_override"
        else:
            step_style_tag = current_style
            step_style_reason = "segment_inferred_from_anchor" if current_style else ""

        step_rows.append({
            "full_name": full_name,
            "run_id": run_id,
            "workflow_identifier": workflow_identifier,
            "workflow_path": workflow_path,
            "head_sha": head_sha,
            "styles": styles_text,
            "job_id": job_id,
            "job_name": job_name,
            "step_name": step_name,
            "category": category,
            "category_reason": category_reason,
            "step_style_tag": step_style_tag,
            "step_style_reason": step_style_reason,
            "started_at": st_start.isoformat() if st_start else "",
            "completed_at": st_end.isoformat() if st_end else "",
            "duration_seconds": "" if st_dur is None else str(st_dur),
            "step_norm_key": step_norm_key,
            "runner_injected": runner_injected,
            "yaml_match": yaml_match,
            "yaml_match_reason": yaml_match_reason,
            "yaml_step_name": yaml_step_name,
            "yaml_run_snip": yaml_run_snip,
            "yaml_uses_snip": yaml_uses_snip,
            "yaml_with_script_snip": yaml_with_script_snip,
            "yaml_block_snip": yaml_block_snip,
            "stage1_anchor_match": "1" if flags.get("stage1_anchor_match") else "0",
            "stage1_anchor_match_reason": str(flags.get("stage1_anchor_match_reason") or ""),
        })

        ev = (st_start, st_end, st_dur, step_name, job_name, flags)
        all_events.append(ev)
        if step_style_tag:
            events_by_style.setdefault(step_style_tag, []).append(ev)

    baseline = compute_metrics_for_event_set(base_start, all_events, stage2_instru_detect_method, s2_fallback)
    for k in STYLE_METRIC_KEYS:
        run_metrics[k] = baseline.get(k)

    run_metrics["third_party_job_count"] = third_party_count
    run_metrics["third_party_job_names"] = safe_join_names(third_party_job_names)

    run_metrics["third_party_provider_job_count"] = third_party_provider_count
    run_metrics["third_party_provider_job_names"] = safe_join_names(third_party_provider_job_names)

    if not is_multi_style:
        s = declared_styles[0]
        row = {"style": s}
        for k in STYLE_METRIC_KEYS:
            v = run_metrics.get(k)
            row[k] = "" if v is None else str(v)
        per_style_rows.append(row)
        return run_metrics, step_rows, per_style_rows

    for s in declared_styles:
        row = {"style": s}
        for k in STYLE_METRIC_KEYS:
            v = run_metrics.get(k)
            row[k] = "" if v is None else str(v)

        s_events = events_by_style.get(s, [])
        if s_events:
            s_metrics = compute_metrics_for_event_set(base_start, s_events, stage2_instru_detect_method, s2_fallback)
            for k in STYLE_METRIC_KEYS:
                v = s_metrics.get(k)
                row[k] = "" if v is None else str(v)

        per_style_rows.append(row)

    return run_metrics, step_rows, per_style_rows


# =========================
# MAIN
# =========================
def main() -> None:
    for p in [OUT_STAGE3A_RUNS_CSV, OUT_STAGE3B_STEPS_CSV, OUT_STAGE3C_RUN_PER_STYLE_CSV]:
        if p.exists():
            p.unlink()

    tokens = load_tokens_from_env_file(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
    gh = GitHubClient(tokens)

    rows, in_fields = read_csv_rows(IN_STAGE2_CSV)
    if not rows:
        raise RuntimeError("No rows in Stage-2 input CSV.")

    if PROCESS_ONLY_RELEVANT_ROWS:
        def is_relevant(r: Dict[str, str]) -> bool:
            det = (r.get("instru_detect_method", "") or "").strip().lower()
            styles = (r.get("styles", "") or "").lower()
            inv = (r.get("invocation_types", "") or "").lower()
            looks = (r.get("looks_like_instru", "") or "").strip().lower()
            return (
                looks == "yes"
                or det not in ("", "none", "unknown")
                or ("third-party" in styles)
                or ("gmd" in styles)
                or ("emu_custom" in styles)
                or ("emu_community" in styles)
                or ("real-device" in styles)
                or ("3p-cli" in inv)
                or ("detox" in inv)
                or ("flutter integration test" in inv)
                or ("gradle_connected" in inv)
                or ("gradle_gmd" in inv)
            )
        target = [r for r in rows if is_relevant(r)]
    else:
        target = rows

    print(f"[Stage3] Rows total: {len(rows)} | Rows to enhance: {len(target)}")

    new_cols_3a = (
        STYLE_METRIC_KEYS
        + [
            "third_party_job_count",
            "third_party_job_names",
            "third_party_provider_job_count",
            "third_party_provider_job_names",
            "stage3_extracted_at_utc",
        ]
    )

    out_fieldnames_3a = list(in_fields)
    for c in new_cols_3a:
        if c not in out_fieldnames_3a:
            out_fieldnames_3a.append(c)

    steps_fields = [
        "full_name",
        "run_id",
        "workflow_identifier",
        "workflow_path",
        "head_sha",
        "styles",
        "job_id",
        "job_name",
        "step_name",
        "category",
        "category_reason",
        "step_style_tag",
        "step_style_reason",
        "started_at",
        "completed_at",
        "duration_seconds",
        "step_norm_key",
        "runner_injected",
        "yaml_match",
        "yaml_match_reason",
        "yaml_step_name",
        "yaml_run_snip",
        "yaml_uses_snip",
        "yaml_with_script_snip",
        "yaml_block_snip",
        "stage1_anchor_match",
        "stage1_anchor_match_reason",
        "stage3_extracted_at_utc",
    ]
    ensure_csv(OUT_STAGE3B_STEPS_CSV, steps_fields)

    per_style_fields = list(out_fieldnames_3a)
    if "styles" in per_style_fields:
        i = per_style_fields.index("styles") + 1
        per_style_fields.insert(i, "style")
    else:
        per_style_fields.append("style")
    ensure_csv(OUT_STAGE3C_RUN_PER_STYLE_CSV, per_style_fields)

    yaml_steps_cache: Dict[Tuple[str, str, str], Dict[str, Dict[str, str]]] = {}
    yaml_job_uses_cache: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    it = target
    if tqdm is not None:
        it = tqdm(target, desc="Stage3: build outputs")

    all_per_style_rows: List[Dict[str, str]] = []

    for r in it:
        full_name = (r.get("full_name") or "").strip()
        run_id = (r.get("run_id") or "").strip()
        created_at = r.get("created_at") or ""
        run_started_at = r.get("run_started_at") or ""
        workflow_identifier = (r.get("workflow_identifier") or "").strip()
        workflow_path = (r.get("workflow_path") or "").strip()
        head_sha = (r.get("head_sha") or "").strip()
        styles_text = (r.get("styles") or "")
        stage2_det = (r.get("instru_detect_method") or "")
        stage1_anchor_names = parse_stage1_anchor_names(r.get("test_invocation_step_names", ""))

        if not full_name or not run_id:
            continue

        # include both direct and S2 mirrored fields so compute_metrics can fallback cleanly
        s2_fallback = {
            "S2_instru_first_started_at": r.get("S2_instru_first_started_at", ""),
            "S2_instru_last_completed_at": r.get("S2_instru_last_completed_at", ""),
            "S2_instru_window_seconds": r.get("S2_instru_window_seconds", ""),
            "S2_time_to_first_instru_seconds": r.get("S2_time_to_first_instru_seconds", ""),

            # NEW modified TTFTS fallback fields
            "time_to_first_instru_from_anchor_job_seconds": r.get("time_to_first_instru_from_anchor_job_seconds", ""),
            "time_to_first_instru_from_anchor_job_quality": r.get("time_to_first_instru_from_anchor_job_quality", ""),
            "anchor_job_started_at": r.get("anchor_job_started_at", ""),

            "S2_time_to_first_instru_from_anchor_job_seconds": r.get("S2_time_to_first_instru_from_anchor_job_seconds", ""),
            "S2_time_to_first_instru_from_anchor_job_quality": r.get("S2_time_to_first_instru_from_anchor_job_quality", ""),
            "S2_anchor_job_started_at": r.get("S2_anchor_job_started_at", ""),

            # Stage1 pass-through from Stage2 (if present)
            "jobs_before_anchor_count": r.get("jobs_before_anchor_count", ""),
        }

        yaml_steps_main: Dict[str, Dict[str, str]] = {}
        yaml_steps_by_job: Dict[str, Dict[str, Dict[str, str]]] = {}

        if FETCH_WORKFLOW_YAML and workflow_path and head_sha:
            ck = (full_name, workflow_path, head_sha)

            if ck in yaml_steps_cache:
                yaml_steps_main = yaml_steps_cache[ck]
                job_uses = yaml_job_uses_cache.get(ck, {})
            else:
                yml = fetch_workflow_yaml(gh, full_name, workflow_path, ref=head_sha)
                yaml_steps_main = parse_workflow_steps(yml)
                job_uses = parse_job_reusable_uses(yml)

                if len(yaml_steps_cache) < WORKFLOW_YAML_CACHE_MAX:
                    yaml_steps_cache[ck] = yaml_steps_main
                    yaml_job_uses_cache[ck] = job_uses

            for job_key, uses_path in (job_uses or {}).items():
                ck2 = (full_name, uses_path, head_sha)
                if ck2 in yaml_steps_cache:
                    yaml_steps_by_job[job_key] = yaml_steps_cache[ck2]
                    continue

                yml2 = fetch_workflow_yaml(gh, full_name, uses_path, ref=head_sha)
                steps2 = parse_workflow_steps(yml2)
                if steps2:
                    yaml_steps_by_job[job_key] = steps2
                if len(yaml_steps_cache) < WORKFLOW_YAML_CACHE_MAX:
                    yaml_steps_cache[ck2] = steps2

        jobs = list_run_jobs(gh, full_name, int(run_id)) or []

        run_metrics, step_rows, per_style_rows = build_stage3_outputs_for_run(
            jobs=jobs,
            run_created_at=created_at,
            run_started_at=run_started_at,
            yaml_steps_main=yaml_steps_main,
            yaml_steps_by_job=yaml_steps_by_job,
            styles_text=styles_text,
            stage2_instru_detect_method=stage2_det,
            s2_fallback=s2_fallback,
            stage1_anchor_names=stage1_anchor_names,
            full_name=full_name,
            run_id=run_id,
            workflow_identifier=workflow_identifier,
            workflow_path=workflow_path,
            head_sha=head_sha,
        )

        extracted_ts = now_utc_iso()
        r["stage3_extracted_at_utc"] = extracted_ts

        for k in new_cols_3a:
            if k == "stage3_extracted_at_utc":
                continue
            v = run_metrics.get(k)
            r[k] = "" if v is None else str(v)

        for sr in step_rows:
            sr2 = dict(sr)
            sr2["stage3_extracted_at_utc"] = extracted_ts
            append_row(OUT_STAGE3B_STEPS_CSV, steps_fields, sr2)

        for pr in per_style_rows:
            pr2 = dict(r)  # carries all Stage2 columns too (including jobs_before_anchor_count)
            pr2["style"] = pr.get("style", "")
            for k in STYLE_METRIC_KEYS:
                if k in pr:
                    pr2[k] = pr[k]
            pr2["stage3_extracted_at_utc"] = extracted_ts
            all_per_style_rows.append(pr2)

    write_csv(OUT_STAGE3A_RUNS_CSV, out_fieldnames_3a, rows)
    write_csv(OUT_STAGE3C_RUN_PER_STYLE_CSV, per_style_fields, all_per_style_rows)

    print("[done] Run metrics:", OUT_STAGE3A_RUNS_CSV)
    print("[done] Step breakdown:", OUT_STAGE3B_STEPS_CSV)
    print("[done] Run x style:", OUT_STAGE3C_RUN_PER_STYLE_CSV)


if __name__ == "__main__":
    main()