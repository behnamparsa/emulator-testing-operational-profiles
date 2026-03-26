# ============================================================
# Stage 1 (REWIRED): Fetch workflows from GitHub for URL_List repos,
# follow called scripts/local actions, and emit verified_workflows_v16.csv
#
# CHANGES ONLY (per request):
# 1) FIX missed Gradle connected*AndroidTest when tasks include GHA expressions
#    => sanitize ${{ ... }} expressions before regex matching.
# 2) ADD a column for step name(s) that include the test invocation
#    => test_invocation_step_names
# 3) Flutter integration tests are labeled "Flutter Integration Test" instead of "3P-CLI"
#    => flutter_project_hint dropped
# 4) ADD Detox Android E2E detection
#    => invocation_types includes "Detox"
#    => looks_like_instru=yes only when Detox + Android runtime/style evidence is present
#    => test_invocation_step_names captures the step with yarn/npx detox test
# 5) FIX Python DeprecationWarning ("Flags not at the start...")
#    => no inline flags inside shared regex fragments
# 6) ADD workflow-structure anchor position proxies (NEW)
#    => anchor_job_ordinal
#    => anchor_step_ordinal_in_job
#    (based on declared YAML order of the first detected invocation step)
#
# 7) Add STRICT emulator.wtf "indirect invocation" detection (already in your latest)
# 8) NEW (ONLY): Add STRICT BrowserStack "indirect invocation" detection
#    - Requires BrowserStack signal AND credible execution trigger (API/script/CLI/gradle task)
#    - Prevents FP from mere env/setup mentions
#
# 9) NEW (ONLY): Persist called-file instrumentation evidence for downstream stages
#    => called_instru_signal
#    => called_instru_file_paths
#    => called_instru_origin_refs
#    => called_instru_origin_step_names
#    => called_instru_file_types
# ============================================================

import base64
import os
import csv
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

import requests
from config.runtime import get_root_dir, get_tokens_env_path, load_github_tokens

try:
    import yaml  # PyYAML (not required)
except Exception:
    yaml = None

# =========================
# CONFIG (KEEP THESE AS YOUR STAGE-1 CONTRACT)
# =========================
TOKENS_ENV_PATH = get_tokens_env_path()
ROOT_DIR = get_root_dir()

IN_URL_LIST_CSV = ROOT_DIR / "URL_List.csv"               # input list of repos
OUT_STAGE1_CSV  = ROOT_DIR / "verified_workflows_v16.csv" # Stage-1 output name (original)

MAX_TOKENS_TO_USE = 7

CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 60
MAX_RETRIES_PER_REQUEST = 8
BACKOFF_BASE_S = 1.7
BACKOFF_CAP_S = 60
MAX_PAGES_PER_LIST = 2000

# follow local files referenced by workflow:
FOLLOW_CALLED_FILES = True
MAX_FOLLOW_DEPTH = 2
MAX_FOLLOW_BYTES = 1_500_000  # skip huge files

# =========================
# Helpers
# =========================
BOM = "\ufeff"
GHA_EXPR_RE = re.compile(r"\${{\s*[^}]+}}")  # sanitize expressions like ${{ matrix.flavor }}

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _clean_key(k: str) -> str:
    return (k or "").replace(BOM, "").strip()

def read_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Input CSV not found: {path}")
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        raw_fields = rdr.fieldnames or []
        fields = [_clean_key(x) for x in raw_fields]
        rows: List[Dict[str, str]] = []
        for r in rdr:
            clean_row = {}
            for k, v in r.items():
                ck = _clean_key(k)
                clean_row[ck] = (v or "")
            rows.append(clean_row)
    return rows, fields

def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def unique_preserve(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        x = (x or "").strip()
        if not x:
            continue
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

def safe_join(items: List[str], max_len: int = 1500) -> str:
    s = ",".join(unique_preserve(items))
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."

def safe_join_pipe(items: List[str], max_len: int = 3000) -> str:
    s = "|".join(unique_preserve(items))
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def external_repo_tokens() -> List[Optional[str]]:
    """For cross-repo scans, prefer a PAT and otherwise fall back to
    unauthenticated requests for public repositories. Avoid using the current
    repo's GITHUB_TOKEN for external workflow discovery.
    """
    gh_pat = (os.environ.get("GH_PAT") or "").strip()
    if gh_pat:
        return [gh_pat, None]
    return [None]

def parse_repo_full_name(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if re.match(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", u):
        return u
    if u.startswith("git@github.com:"):
        u2 = u.split("git@github.com:", 1)[1]
        u2 = u2[:-4] if u2.endswith(".git") else u2
        return u2.strip("/")
    if "github.com" in u:
        try:
            p = urlparse(u)
            parts = [x for x in (p.path or "").split("/") if x]
            if len(parts) >= 2:
                return f"{parts[0]}/{parts[1].replace('.git','')}"
        except Exception:
            return ""
    return ""

def load_tokens_from_env_file(env_path: Path, max_tokens: int = 3) -> List[str]:
    if not env_path.exists():
        raise FileNotFoundError(f"Tokens env file not found: {env_path}")
    tokens: List[str] = []
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        if k.startswith("GITHUB_TOKEN_") and v:
            tokens.append(v)
            if len(tokens) >= max_tokens:
                break
    if not tokens:
        raise ValueError(f"No tokens found in {env_path}. Expected keys like GITHUB_TOKEN_1=...")
    return tokens

def sanitize_gha_expr(text: str) -> str:
    # "connected${{ matrix.flavor }}DebugAndroidTest" -> "connectedDebugAndroidTest"
    return GHA_EXPR_RE.sub("", text or "")

def normalize_repo_rel_path(ref: str) -> str:
    rr = (ref or "").replace("\\", "/").strip()
    rr = rr[2:] if rr.startswith("./") else rr
    rr = rr.lstrip("/")
    while "//" in rr:
        rr = rr.replace("//", "/")
    return rr

# =========================
# GitHub API client
# =========================
@dataclass
class TokenState:
    token: Optional[str]
    remaining: Optional[int] = None
    reset_epoch: Optional[int] = None

class GitHubClient:
    def __init__(self, tokens: List[Optional[str]]) -> None:
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "stage1-v16-workflow-scan/1.0",
        })
        cleaned: List[Optional[str]] = []
        seen = set()
        for t in tokens:
            key = (t or "").strip()
            dedupe_key = key if key else "__NONE__"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            cleaned.append(key or None)
        if not cleaned:
            cleaned = [None]
        self.tokens = [TokenState(t) for t in cleaned]

    def _pick_idx(self) -> int:
        now = int(time.time())
        candidates = []
        for i, st in enumerate(self.tokens):
            if st.token is None:
                candidates.append((0, i))
                continue
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
        resets = [st.reset_epoch for st in self.tokens if st.token and st.reset_epoch]
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
            headers = dict(self.session.headers)
            if st.token:
                headers["Authorization"] = f"Bearer {st.token}"
            else:
                headers.pop("Authorization", None)
            try:
                resp = self.session.request(method, url, params=params, headers=headers, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S))
            except requests.exceptions.RequestException:
                self._backoff(attempt)
                continue

            last_status = resp.status_code

            if st.token:
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
                if any((t.token is None) or (t.remaining is None) or (t.remaining > 0) for t in self.tokens):
                    self._backoff(attempt)
                    continue
                self._sleep_until_reset()
                continue

            # Repository-scoped GITHUB_TOKEN often cannot access external repos.
            # If an authenticated request is denied, retry with unauthenticated/public mode.
            if resp.status_code in (401, 403) and st.token:
                if not any(t.token is None for t in self.tokens):
                    self.tokens.append(TokenState(None))
                self._backoff(attempt)
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
def get_repo_meta(gh: GitHubClient, full_name: str) -> Dict[str, str]:
    url = f"https://api.github.com/repos/{full_name}"
    data = gh.request_json("GET", url, params={})
    if not isinstance(data, dict):
        return {}
    return {
        "default_branch": (data.get("default_branch") or "").strip(),
        "archived": str(bool(data.get("archived"))).lower(),
        "private": str(bool(data.get("private"))).lower(),
    }

def list_workflows_via_actions_api(gh: GitHubClient, full_name: str) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/workflows"
    data = gh.request_json("GET", url, params={"per_page": 100})
    if not isinstance(data, dict):
        return []
    wfs = data.get("workflows", [])
    return wfs if isinstance(wfs, list) else []

def list_workflow_files_via_contents_api(gh: GitHubClient, full_name: str, ref: str) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/contents/.github/workflows"
    data = gh.request_json("GET", url, params={"ref": ref} if ref else {})
    if not isinstance(data, list):
        return []
    out: List[Dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        path = (item.get("path") or "").strip()
        name = (item.get("name") or "").strip()
        item_type = (item.get("type") or "").strip()
        if item_type != "file":
            continue
        if not path.lower().endswith((".yml", ".yaml")):
            continue
        out.append({
            "id": "",
            "name": name or path.split("/")[-1],
            "path": path,
            "state": "active",
        })
    return out

def list_workflows(gh: GitHubClient, full_name: str, default_branch: str) -> Tuple[List[Dict], str]:
    api_items = list_workflows_via_actions_api(gh, full_name)
    if api_items:
        return api_items, "actions_api"

    content_items = list_workflow_files_via_contents_api(gh, full_name, default_branch)
    if content_items:
        return content_items, "contents_api"

    return [], "none"

def fetch_file_text_at_ref(gh: GitHubClient, full_name: str, path: str, ref: str) -> str:
    url = f"https://api.github.com/repos/{full_name}/contents/{path.lstrip('/')}"
    data = gh.request_json("GET", url, params={"ref": ref} if ref else {})
    if not isinstance(data, dict):
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

def file_size_at_ref(gh: GitHubClient, full_name: str, path: str, ref: str) -> Optional[int]:
    url = f"https://api.github.com/repos/{full_name}/contents/{path.lstrip('/')}"
    data = gh.request_json("GET", url, params={"ref": ref} if ref else {})
    if not isinstance(data, dict):
        return None
    sz = data.get("size")
    try:
        return int(sz)
    except Exception:
        return None

# =========================
# Signal detection patterns (Stage-1)
# =========================
GRADLE_INVOKE_PREFIX = r"(?:^|[ \t\r\n;&|()\"'`])"

GRADLE_CMD_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradlew\.bat\b|gradle\s+)",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

# Flutter integration tests (hint + android-targeted)
FLUTTER_IT_RE = re.compile(r"\bflutter\s+(?:test|drive)\b", flags=re.IGNORECASE | re.DOTALL)
FLUTTER_IT_ANDROID_HINT_RE = re.compile(r"\b(integration_test|--driver\b|test_driver)\b", flags=re.IGNORECASE | re.DOTALL)
FLUTTER_DEVICE_FLAG_RE = re.compile(r"\s+-d\s+(?P<dev>\"[^\"]+\"|'[^']+'|\S+)", flags=re.IGNORECASE | re.DOTALL)
FLUTTER_DEVICE_IS_ANDROID_RE = re.compile(
    r"\b(android|emulator-\d+|sdk\s+gphone|android\s+sdk\s+built\s+for|pixel)\b",
    flags=re.IGNORECASE | re.DOTALL,
)

# Detox Android E2E invocation (CLI via yarn/npm/pnpm/npx)
DETOX_INVOKE_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}("
    r"(?:yarn|npm|pnpm)\s+[^\n\r]*\bdetox(?::[a-z0-9:_-]+)?\b|"
    r"(?:npx\s+detox\s+test\b)|"
    r"(?:detox\s+test\b)"
    r")",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

# --- GMD tasks ---
GMD_TASK_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"manageddevice[\w:-]*(check|androidtest|test|setup)\b|"
    r":[\w:-]*manageddevice[\w:-]*(check|androidtest|test|setup)\b"
    r")",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

GMD_MANAGEDDEV_PROP_RE = re.compile(
    r"\B-Pandroid\.(?:testoptions\.manageddevices|experimental\.testOptions\.managedDevices)\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

GENERIC_ANDROIDTEST_TASK_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b(?!connected)\w+androidtest\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

CONNECTED_ANDROIDTEST_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"connected\w*androidtest|connectedcheck|devicecheck|alldevicescheck|"
    r"device\w*androidtest"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

BASELINE_PROFILE_TASK_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"generate\w*baselineprofile|collect\w*baselineprofile|baselineprofile"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

ADB_INSTR_RE = re.compile(r"\badb\s+shell\s+am\s+instrument\b|\bam\s+instrument\b", flags=re.IGNORECASE | re.DOTALL)

EMU_COMMUNITY_ACTION_RE = re.compile(
    r"\b("
    r"reactivecircus/android-emulator-runner|"
    r"malinskiy/action-android/emulator-run-cmd|"
    r"android-emulator-runner"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

EMU_CUSTOM_RUNTIME_RE = re.compile(
    r"\b("
    r"\bemulator\b.*\b-avd\b|"
    r"\bavdmanager\b|"
    r"adb\s+wait[- ]?for[- ]?device"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

REAL_DEVICE_ADB_RE = re.compile(
    r"\badb\s+-s\s+(?!emulator-\d+\b)(?!localhost:\d+\b)(?!127\.0\.0\.1:\d+\b)\S+\b",
    flags=re.IGNORECASE | re.MULTILINE,
)

THIRD_PARTY_PROVIDER_NAME_RE = re.compile(
    r"\b("
    r"firebase\s+test\s+lab|gcloud\s+firebase|"
    r"browserstack|bstack|hub\.browserstack\.com|"
    r"sauce(labs)?|saucectl|"
    r"appcenter|microsoft/appcenter|"
    r"emulator\.wtf|"
    r"maestro\s+cloud"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

THIRD_PARTY_INVOKE_RE = re.compile(
    r"\b("
    r"(gcloud\s+firebase\s+test\s+android\s+run\b)|"
    r"(firebase\s+test\s+android\s+run\b)|"
    r"(flank\s+android\s+run\b)|"
    r"(appcenter\s+test\s+run\s+android\b)|"
    r"(appcenter\s+test\s+run\s+espresso\b)|"
    r"(saucectl\s+(run|test)\b)|"
    r"(emulator-wtf/run-tests@)|"
    r"(maestro\s+cloud\b)"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

THIRD_PARTY_SETUP_ONLY_RE = re.compile(
    r"\b("
    r"google-github-actions/(auth|setup-gcloud)|"
    r"gcloud\s+auth|"
    r"gcloud\s+config\s+set"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

# =========================
# STRICT indirect 3P invocation detectors
# =========================

# --- emulator.wtf (already added previously) ---
EMULATOR_WTF_SIGNAL_RE = re.compile(
    r"\b("
    r"emulator\.wtf|"
    r"emulator_wtf|"
    r"ew_api_token|"
    r"emulatorwtf_token|"
    r"emulator_wtf_token"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

EMULATOR_WTF_GRADLE_TASK_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\b("
    r"(?:[:\w.-]+)?emulatorwtf(?:[:\w.-]+)?"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

def _is_emulator_wtf_indirect_invoke(text: str) -> bool:
    low = sanitize_gha_expr(text or "").lower()
    return bool(EMULATOR_WTF_SIGNAL_RE.search(low)) and bool(EMULATOR_WTF_GRADLE_TASK_RE.search(low))

# --- NEW: BrowserStack strict indirect invoke ---
# Signal: domain, bs:// app ids, common secrets/envs, local tunnel, sdk token
BROWSERSTACK_SIGNAL_RE = re.compile(
    r"\b("
    r"api-cloud\.browserstack\.com|"
    r"hub\.browserstack\.com|"
    r"browserstack(local)?|"
    r"\bbs://|"
    r"browserstack_username|browserstack_access(_)?key|"
    r"bstack(_)?(username|access(_)?key)|"
    r"browserstack-sdk"
    r")\b",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

# Execution trigger: API call, bstack CLI, explicit scripts, or gradle tasks containing browserstack
BROWSERSTACK_EXEC_TRIGGER_RE = re.compile(
    rf"{GRADLE_INVOKE_PREFIX}("
    r"curl\s+[^\n\r]*(api-cloud\.browserstack\.com|hub\.browserstack\.com)|"
    r"(?:python|python3)\s+[^\n\r]*browserstack[^\s]*\.(?:py)\b|"
    r"node\s+[^\n\r]*browserstack[^\s]*\.(?:js|mjs|cjs)\b|"
    r"(?:yarn|npm|pnpm)\s+[^\n\r]*\bbrowserstack\b|"
    r"\bbstack\b\s+[^\n\r]*(?:run|execute|test|app-automate|appautomate|espresso|xcuitest|appium)\b|"
    r"(\./gradlew\b|gradle\s+|gradlew\.bat\b)[^\n\r]*\bbrowserstack\b"
    r")",
    flags=re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

def _is_browserstack_indirect_invoke(text: str) -> bool:
    low = sanitize_gha_expr(text or "").lower()
    return bool(BROWSERSTACK_SIGNAL_RE.search(low)) and bool(BROWSERSTACK_EXEC_TRIGGER_RE.search(low))

# =========================
# Follow-called-files extraction (UNCHANGED)
# =========================
LOCAL_USES_RE = re.compile(r'(?mi)^\s*uses\s*:\s*(?P<ref>\./\S+?)(?:\s+#.*)?$')
WORKDIR_RE = re.compile(r'(?mi)^\s*working-directory\s*:\s*(?P<wd>[^\n#]+)')

SCRIPT_CALL_RE = re.compile(r'''(?mix)
(?:^|[;&|()\s"'`])
(?:(?:bash|sh|pwsh|powershell|python|python3|node|ruby)\s+)?
(?P<path>(?:\./|\.\\)?[\w./\\-]+\.(?:sh|ps1|bat|cmd|py|js|rb|pl))
(?:\s|$)
''')

GENERIC_REL_EXEC_RE = re.compile(r'(?m)(?:^|[;&|()\s"\'`])(?P<path>\./[A-Za-z0-9_./\\-]+)(?:\s|$)')
CONFIG_ARG_RE = re.compile(r'(?mi)\b--config(?:=|\s+)(?P<path>[^\s"\']+)')

NO_FOLLOW_BASENAMES = {"gradlew", "gradlew.bat", "gradle", "adb", "flutter", "gcloud", "java", "python", "python3"}

def _strip_quotes(s: str) -> str:
    return (s or "").strip().strip('"').strip("'").strip("`")

def is_dynamic_ref(ref: str) -> bool:
    r = ref or ""
    return ("${{" in r) or ("${" in r) or ("$(" in r) or ("%{" in r)

def extract_workdirs(text: str) -> List[str]:
    wds = []
    for m in WORKDIR_RE.finditer(text or ""):
        wd = _strip_quotes(m.group("wd"))
        if wd:
            wd = wd.replace("\\", "/").lstrip("./")
            wds.append(wd)
    return unique_preserve(wds)

def extract_references(text: str) -> List[str]:
    refs: List[str] = []

    for m in LOCAL_USES_RE.finditer(text or ""):
        ref = _strip_quotes(m.group("ref"))
        if "@" in ref:
            ref = ref.split("@", 1)[0]
        refs.append(ref)

    for m in SCRIPT_CALL_RE.finditer(text or ""):
        refs.append(_strip_quotes(m.group("path")))

    for m in CONFIG_ARG_RE.finditer(text or ""):
        refs.append(_strip_quotes(m.group("path")))

    for m in GENERIC_REL_EXEC_RE.finditer(text or ""):
        p = _strip_quotes(m.group("path"))
        base = Path(p.replace("\\", "/")).name.lower()
        if base in NO_FOLLOW_BASENAMES:
            continue
        refs.append(p)

    out = []
    for r in refs:
        if not r:
            continue
        out.append(r.replace("\\", "/").strip())
    return unique_preserve(out)

def normalize_ref_path(ref: str) -> str:
    rr = (ref or "").replace("\\", "/").strip()
    rr = rr[2:] if rr.startswith("./") else rr
    rr = rr.lstrip("/")
    return rr

def candidate_paths_for_ref(ref: str, workdirs: List[str]) -> List[str]:
    rr = normalize_ref_path(ref)
    prefixes = [""] + [wd.strip("/").replace("\\", "/") for wd in (workdirs or []) if wd.strip()]
    out = []
    for pref in prefixes:
        p = f"{pref}/{rr}" if pref else rr
        out.append(p.strip("/"))
    return unique_preserve(out)

def possible_action_ymls(path: str) -> List[str]:
    p = path.strip("/")
    return unique_preserve([f"{p}/action.yml", f"{p}/action.yaml"])

def classify_called_file_type(path: str) -> str:
    low = (path or "").lower().replace("\\", "/")
    if low.endswith("action.yml") or low.endswith("action.yaml"):
        return "local_action"
    if "/.github/workflows/" in f"/{low}":
        return "local_workflow"
    return "script"

# =========================
# Step parsing for invocation step names + anchor ordinals (UNCHANGED)
# =========================
STEP_NAME_LINE_RE = re.compile(r"^(\s*)-\s*name\s*:\s*(.+?)\s*$", re.MULTILINE)

def _count_leading_spaces(s: str) -> int:
    return len(s) - len(s.lstrip(" "))

def parse_workflow_step_records(yaml_text: str) -> List[Dict[str, Union[str, int]]]:
    if not yaml_text:
        return []

    lines = yaml_text.splitlines()
    n = len(lines)
    out: List[Dict[str, Union[str, int]]] = []

    jobs_idx = None
    for i, line in enumerate(lines):
        if re.match(r"^\s*jobs\s*:\s*$", line):
            jobs_idx = i
            break
    if jobs_idx is None:
        return out

    jobs_indent = _count_leading_spaces(lines[jobs_idx])
    i = jobs_idx + 1
    job_ordinal = 0

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
        job_ordinal += 1

        block_start = i + 1
        j = block_start
        while j < n:
            nxt = lines[j]
            if nxt.strip() == "":
                j += 1
                continue
            nxt_indent = _count_leading_spaces(nxt)
            if nxt_indent <= indent:
                break
            j += 1
        job_block_lines = lines[block_start:j]
        job_block = "\n".join(job_block_lines)

        m_name = re.search(r"(?mi)^\s*name\s*:\s*(.+?)\s*$", job_block)
        job_name = m_name.group(1).strip().strip('"').strip("'") if m_name else ""

        job_lines = job_block_lines
        steps_idx = None
        steps_indent = None
        for k, jl in enumerate(job_lines):
            if re.match(r"^\s*steps\s*:\s*$", jl):
                steps_idx = k
                steps_indent = _count_leading_spaces(jl)
                break

        if steps_idx is not None and steps_indent is not None:
            step_ordinal = 0
            k = steps_idx + 1
            while k < len(job_lines):
                cur = job_lines[k]
                if cur.strip() == "":
                    k += 1
                    continue
                cur_indent = _count_leading_spaces(cur)
                if cur_indent <= steps_indent:
                    break

                m_step = re.match(r"^(\s*)-\s*name\s*:\s*(.+?)\s*$", cur)
                if not m_step:
                    k += 1
                    continue

                base_indent = len(m_step.group(1))
                step_name = m_step.group(2).strip().strip('"').strip("'")
                block = [cur]
                kk = k + 1
                while kk < len(job_lines):
                    nxt = job_lines[kk]
                    m2 = re.match(r"^(\s*)-\s*name\s*:\s*(.+?)\s*$", nxt)
                    if m2 and len(m2.group(1)) == base_indent:
                        break
                    if nxt.strip() and _count_leading_spaces(nxt) <= steps_indent:
                        break
                    block.append(nxt)
                    kk += 1

                step_ordinal += 1
                out.append({
                    "job_id": job_id,
                    "job_name": job_name,
                    "job_ordinal": job_ordinal,
                    "step_name": step_name,
                    "step_ordinal_in_job": step_ordinal,
                    "step_block": "\n".join(block),
                })

                k = kk
        i = j

    return out

def build_origin_ref_to_step_names(workflow_yaml_text: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for rec in parse_workflow_step_records(workflow_yaml_text):
        step_name = str(rec.get("step_name") or "").strip()
        step_block = str(rec.get("step_block") or "")
        refs = extract_references(step_block)
        for r in refs:
            rr = normalize_repo_rel_path(r)
            if not rr:
                continue
            out.setdefault(rr, [])
            out[rr] = unique_preserve(out[rr] + [step_name])
    return out

def _flutter_androidish_from_text(text: str, runtime_ev: Dict[str, bool]) -> bool:
    t = text or ""
    t2 = sanitize_gha_expr(t)
    low = t2.lower()

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
    t = text or ""
    t2 = sanitize_gha_expr(t)
    low = t2.lower()

    if not DETOX_INVOKE_RE.search(low):
        return False

    if runtime_ev.get("emu_comm") or runtime_ev.get("emu_custom") or runtime_ev.get("real_device") or runtime_ev.get("third_party_invoke"):
        return True

    return False

def _is_third_party_invoke_non_flutter(text: str) -> bool:
    low = sanitize_gha_expr(text or "").lower()
    return bool(THIRD_PARTY_INVOKE_RE.search(low)) and not bool(
        THIRD_PARTY_SETUP_ONLY_RE.search(low) and not THIRD_PARTY_INVOKE_RE.search(low)
    )

def extract_test_invocation_step_names_and_anchor(
    gh: GitHubClient,
    full_name: str,
    base_ref: str,
    workflow_yaml_text: str,
) -> Tuple[List[str], Optional[int], Optional[int]]:
    if not workflow_yaml_text:
        return [], None, None

    step_records = parse_workflow_step_records(workflow_yaml_text)
    if not step_records:
        return [], None, None

    step_names: List[str] = []
    anchor_job_ordinal: Optional[int] = None
    anchor_step_ordinal_in_job: Optional[int] = None

    ev_full = scan_text_for_evidence(workflow_yaml_text)
    runtime_ev = {
        "emu_comm": bool(ev_full.get("emu_comm")),
        "emu_custom": bool(ev_full.get("emu_custom")),
        "real_device": bool(ev_full.get("real_device")),
        "third_party_invoke": bool(ev_full.get("third_party_invoke")),
    }

    for rec in step_records:
        step_name = str(rec.get("step_name") or "")
        blk = str(rec.get("step_block") or "")
        blk_s = sanitize_gha_expr(blk)
        low = blk_s.lower()

        direct_connected = bool(CONNECTED_ANDROIDTEST_RE.search(low))
        direct_gmd_task = bool(GMD_TASK_RE.search(low))
        direct_gmd_prop = bool(GMD_MANAGEDDEV_PROP_RE.search(low))
        direct_generic_androidtest = bool(GENERIC_ANDROIDTEST_TASK_RE.search(low))
        direct_baseline = bool(BASELINE_PROFILE_TASK_RE.search(low))
        direct_adb = bool(ADB_INSTR_RE.search(low))

        direct_3p = _is_third_party_invoke_non_flutter(low)
        direct_emu_wtf_indirect = _is_emulator_wtf_indirect_invoke(low)
        direct_bs_indirect = _is_browserstack_indirect_invoke(low)

        direct_flutter_androidish = _flutter_androidish_from_text(low, runtime_ev)
        direct_detox_androidish = _detox_androidish_from_text(low, runtime_ev)

        direct_gmd = bool(direct_gmd_task or (direct_gmd_prop and (direct_baseline or direct_generic_androidtest)))

        matched_here = False
        if (
            direct_connected
            or direct_gmd
            or direct_baseline
            or direct_adb
            or direct_3p
            or direct_emu_wtf_indirect
            or direct_bs_indirect
            or direct_flutter_androidish
            or direct_detox_androidish
        ):
            matched_here = True
        else:
            refs = extract_references(blk)
            wds = extract_workdirs(blk)

            for r in refs:
                if not r or is_dynamic_ref(r):
                    continue
                candidates = candidate_paths_for_ref(r, wds)
                is_prob_action = bool(r.strip().startswith("./")) and (r.endswith("/") or "/" in r)

                found_invoke_in_called = False

                for c in candidates:
                    if is_prob_action and (not c.lower().endswith((".yml", ".yaml", ".sh", ".ps1", ".py", ".js", ".rb", ".pl", ".bat", ".cmd"))):
                        for ay in possible_action_ymls(c):
                            txt = fetch_file_text_at_ref(gh, full_name, ay, base_ref)
                            if not txt:
                                continue
                            ev = scan_text_for_evidence(txt)
                            if compute_looks_like_instru(ev) == "yes":
                                found_invoke_in_called = True
                                break
                        if found_invoke_in_called:
                            break

                    txt = fetch_file_text_at_ref(gh, full_name, c, base_ref)
                    if not txt:
                        continue
                    ev = scan_text_for_evidence(txt)
                    if compute_looks_like_instru(ev) == "yes":
                        found_invoke_in_called = True
                        break

                if found_invoke_in_called:
                    matched_here = True
                    break

        if matched_here:
            step_names.append(step_name)
            if anchor_job_ordinal is None:
                try:
                    anchor_job_ordinal = int(rec.get("job_ordinal"))  # type: ignore[arg-type]
                except Exception:
                    anchor_job_ordinal = None
                try:
                    anchor_step_ordinal_in_job = int(rec.get("step_ordinal_in_job"))  # type: ignore[arg-type]
                except Exception:
                    anchor_step_ordinal_in_job = None

    return unique_preserve(step_names), anchor_job_ordinal, anchor_step_ordinal_in_job

# =========================
# Scan logic
# =========================
def detect_provider_names(text: str) -> List[str]:
    t = (text or "").lower()
    names = []
    if re.search(r"\b(gcloud\s+firebase|firebase\s+test\s+lab|firebase\s+test\s+android\s+run)\b", t):
        names.append("Firebase Test Lab")
    if re.search(r"\bbrowserstack|bstack|hub\.browserstack\.com\b", t):
        names.append("BrowserStack")
    if re.search(r"\bsauce(labs)?|saucectl\b", t):
        names.append("Sauce Labs")
    if re.search(r"\b(appcenter|microsoft/appcenter)\b", t):
        names.append("App Center")
    if re.search(r"\bemulator\.wtf|emulator-wtf/run-tests@\b", t):
        names.append("emulator.wtf")
    if re.search(r"\bmaestro\s+cloud\b", t):
        names.append("Maestro Cloud")
    return unique_preserve(names)

def scan_text_for_evidence(text: str) -> Dict[str, Union[bool, List[str]]]:
    txt = sanitize_gha_expr(text or "")
    low = txt.lower()

    has_gradle = bool(GRADLE_CMD_RE.search(low))

    baseline = bool(BASELINE_PROFILE_TASK_RE.search(low))
    adb = bool(ADB_INSTR_RE.search(low))

    gmd_prop = bool(GMD_MANAGEDDEV_PROP_RE.search(low))
    generic_androidtest = bool(GENERIC_ANDROIDTEST_TASK_RE.search(low))

    gmd_task = bool(GMD_TASK_RE.search(low))
    gmd = bool(gmd_task or (gmd_prop and (baseline or generic_androidtest)))

    connected = bool(CONNECTED_ANDROIDTEST_RE.search(low))

    emu_comm = bool(EMU_COMMUNITY_ACTION_RE.search(low))
    emu_custom = bool(EMU_CUSTOM_RUNTIME_RE.search(low))
    real_device = bool(REAL_DEVICE_ADB_RE.search(low))

    # Direct 3P invoke
    tp_invoke_direct = bool(THIRD_PARTY_INVOKE_RE.search(low)) and not bool(
        THIRD_PARTY_SETUP_ONLY_RE.search(low) and not THIRD_PARTY_INVOKE_RE.search(low)
    )

    # Strict indirect invokes
    tp_invoke_emu_wtf_indirect = _is_emulator_wtf_indirect_invoke(low)
    tp_invoke_bs_indirect = _is_browserstack_indirect_invoke(low)

    tp_invoke = bool(tp_invoke_direct or tp_invoke_emu_wtf_indirect or tp_invoke_bs_indirect)

    tp_providers = detect_provider_names(low) if THIRD_PARTY_PROVIDER_NAME_RE.search(low) else []

    runtime_ev = {
        "emu_comm": emu_comm,
        "emu_custom": emu_custom,
        "real_device": real_device,
        "third_party_invoke": tp_invoke,
    }

    flutter_androidish = _flutter_androidish_from_text(txt, runtime_ev)
    detox_androidish = _detox_androidish_from_text(txt, runtime_ev)

    return {
        "has_gradle": has_gradle,
        "gmd": gmd,
        "connected": connected,
        "baseline": baseline,
        "adb": adb,
        "emu_comm": emu_comm,
        "emu_custom": emu_custom,
        "real_device": real_device,
        "third_party_invoke": tp_invoke,
        "third_party_providers": tp_providers,
        "flutter_androidish_invoke": flutter_androidish,
        "detox_androidish_invoke": detox_androidish,
    }

def merge_evidence(a: Dict, b: Dict) -> Dict:
    out = dict(a)
    for k, v in b.items():
        if isinstance(v, bool):
            out[k] = bool(out.get(k, False) or v)
        elif isinstance(v, list):
            out[k] = unique_preserve((out.get(k, []) or []) + v)
        else:
            out[k] = v
    return out

def compute_invocation_types(ev: Dict) -> List[str]:
    inv: List[str] = []

    if ev.get("detox_androidish_invoke"):
        inv.append("Detox")

    if ev.get("flutter_androidish_invoke"):
        inv.append("Flutter Integration Test")
    else:
        if ev.get("third_party_invoke"):
            inv.append("3P-CLI")

    if ev.get("adb"):
        inv.append("ADB")
    if ev.get("gmd"):
        inv.append("Gradle_GMD")
    if ev.get("connected"):
        inv.append("Gradle_Connected")
    if ev.get("baseline"):
        inv.append("Gradle_BaselineProfile")
    if ev.get("has_gradle") and (ev.get("gmd") or ev.get("connected") or ev.get("baseline")):
        inv.append("Gradle")

    return sorted(set(inv))

def compute_styles(ev: Dict) -> List[str]:
    styles: List[str] = []
    if ev.get("third_party_invoke"):
        styles.append("Third-Party")
    if ev.get("gmd"):
        styles.append("GMD")
    if ev.get("real_device"):
        styles.append("Real-Device")

    if ev.get("emu_comm"):
        styles.append("Community")
    else:
        if ev.get("emu_custom"):
            styles.append("Custom")

    return sorted(set(styles))

def compute_looks_like_instru(ev: Dict) -> str:
    if (
        ev.get("gmd")
        or ev.get("connected")
        or ev.get("baseline")
        or ev.get("adb")
        or ev.get("third_party_invoke")
        or ev.get("flutter_androidish_invoke")
        or ev.get("detox_androidish_invoke")
    ):
        return "yes"
    return "no"

def infer_instru_detect_method(styles: List[str], inv: List[str]) -> str:
    if "Third-Party" in styles:
        return "third_party_cli"
    if "GMD" in styles:
        return "gradle_gmd"
    if "Community" in styles or "Custom" in styles:
        if any(x in inv for x in ["Gradle_Connected", "Gradle"]):
            return "gradle_connected"
        if "Detox" in inv:
            return "invocation_signal"
    if "Real-Device" in styles:
        return "real_device_adb"
    if inv:
        return "invocation_signal"
    return "none"

# =========================
# Called-file following via GitHub API (ADJUSTED ONLY TO PERSIST CALLED-FILE INSTRU EVIDENCE)
# =========================
def follow_called_files(
    gh: GitHubClient,
    full_name: str,
    base_ref: str,
    root_text: str,
    origin_ref_to_step_names: Optional[Dict[str, List[str]]] = None,
    max_depth: int = MAX_FOLLOW_DEPTH,
) -> Tuple[Dict, int, int, List[str], bool, List[str], List[str], List[str], List[str]]:
    if not FOLLOW_CALLED_FILES:
        return scan_text_for_evidence(root_text), 0, 0, [], False, [], [], [], []

    agg_evidence = scan_text_for_evidence(root_text)
    unresolved_dynamic = 0
    followed_paths: List[str] = []
    visited: Set[str] = set()

    called_instru_signal = False
    called_instru_file_paths: List[str] = []
    called_instru_origin_refs: List[str] = []
    called_instru_origin_step_names: List[str] = []
    called_instru_file_types: List[str] = []

    origin_ref_to_step_names = origin_ref_to_step_names or {}

    def fetch_and_scan(path: str) -> Optional[Tuple[str, Dict]]:
        sz = file_size_at_ref(gh, full_name, path, base_ref)
        if sz is not None and sz > MAX_FOLLOW_BYTES:
            return None
        txt = fetch_file_text_at_ref(gh, full_name, path, base_ref)
        if not txt:
            return None
        ev = scan_text_for_evidence(txt)
        return txt, ev

    def register_called_instru(path: str, origin_ref: str) -> None:
        nonlocal called_instru_signal, called_instru_file_paths, called_instru_origin_refs, called_instru_origin_step_names, called_instru_file_types
        called_instru_signal = True
        norm_path = normalize_repo_rel_path(path)
        norm_origin = normalize_repo_rel_path(origin_ref)
        called_instru_file_paths.append(norm_path)
        called_instru_origin_refs.append(norm_origin)
        called_instru_file_types.append(classify_called_file_type(norm_path))
        called_instru_origin_step_names.extend(origin_ref_to_step_names.get(norm_origin, []))

    def walk(text: str, depth: int) -> None:
        nonlocal agg_evidence, unresolved_dynamic, followed_paths, visited
        if depth > max_depth:
            return

        refs = extract_references(text)
        wds = extract_workdirs(text)

        for r in refs:
            if not r:
                continue
            if is_dynamic_ref(r):
                unresolved_dynamic += 1
                continue

            norm_origin_ref = normalize_repo_rel_path(r)
            candidates = candidate_paths_for_ref(r, wds)
            is_prob_action = bool(r.strip().startswith("./")) and (r.endswith("/") or "/" in r)

            for c in candidates:
                if c in visited:
                    continue

                if is_prob_action and (not c.lower().endswith((".yml", ".yaml", ".sh", ".ps1", ".py", ".js", ".rb", ".pl", ".bat", ".cmd"))):
                    for ay in possible_action_ymls(c):
                        if ay in visited:
                            continue
                        got = fetch_and_scan(ay)
                        if got:
                            visited.add(ay)
                            followed_paths.append(ay)
                            txt2, ev2 = got
                            agg_evidence = merge_evidence(agg_evidence, ev2)
                            if compute_looks_like_instru(ev2) == "yes":
                                register_called_instru(ay, norm_origin_ref)
                            walk(txt2, depth + 1)

                got = fetch_and_scan(c)
                if got:
                    visited.add(c)
                    followed_paths.append(c)
                    txt2, ev2 = got
                    agg_evidence = merge_evidence(agg_evidence, ev2)
                    if compute_looks_like_instru(ev2) == "yes":
                        register_called_instru(c, norm_origin_ref)
                    walk(txt2, depth + 1)

    walk(root_text, 0)
    return (
        agg_evidence,
        len(unique_preserve(followed_paths)),
        int(unresolved_dynamic),
        unique_preserve(followed_paths),
        bool(called_instru_signal),
        unique_preserve(called_instru_file_paths),
        unique_preserve(called_instru_origin_refs),
        unique_preserve(called_instru_origin_step_names),
        unique_preserve(called_instru_file_types),
    )

# =========================
# Stage-1 processing
# =========================
def build_stage1_rows_for_repo(gh: GitHubClient, full_name: str, repo_url: str) -> Tuple[List[Dict[str, str]], Dict[str, object]]:
    meta = get_repo_meta(gh, full_name)
    default_branch = meta.get("default_branch") or "main"

    workflows, workflow_source = list_workflows(gh, full_name, default_branch)
    out_rows: List[Dict[str, str]] = []
    stats: Dict[str, object] = {
        "repo": full_name,
        "default_branch": default_branch,
        "workflow_source": workflow_source,
        "workflows_listed": len(workflows),
        "workflow_files_seen": 0,
        "workflow_yaml_loaded": 0,
        "rows_written": 0,
        "yaml_fetch_failures": [],
    }

    for wf in workflows:
        wf_name = (wf.get("name") or "").strip()
        wf_path = (wf.get("path") or "").strip()
        wf_state = (wf.get("state") or "").strip()
        wf_id = str(wf.get("id") or "")

        if not wf_path:
            continue
        stats["workflow_files_seen"] = int(stats["workflow_files_seen"]) + 1

        yaml_text = fetch_file_text_at_ref(gh, full_name, wf_path, default_branch)
        if not yaml_text:
            stats["yaml_fetch_failures"].append(wf_path)
            continue
        stats["workflow_yaml_loaded"] = int(stats["workflow_yaml_loaded"]) + 1

        origin_ref_to_step_names = build_origin_ref_to_step_names(yaml_text)

        (
            ev0,
            followed_count,
            unresolved_dyn,
            followed_paths,
            called_instru_signal,
            called_instru_file_paths,
            called_instru_origin_refs,
            called_instru_origin_step_names,
            called_instru_file_types,
        ) = follow_called_files(
            gh=gh,
            full_name=full_name,
            base_ref=default_branch,
            root_text=yaml_text,
            origin_ref_to_step_names=origin_ref_to_step_names,
            max_depth=MAX_FOLLOW_DEPTH,
        )

        invocation_types = compute_invocation_types(ev0)
        styles = compute_styles(ev0)
        looks_like = compute_looks_like_instru(ev0)

        tp_names = ev0.get("third_party_providers", []) if isinstance(ev0.get("third_party_providers"), list) else []
        tp_name_str = safe_join(tp_names) if ("Third-Party" in styles) else ""

        step_inv_names, anchor_job_ordinal, anchor_step_ordinal_in_job = extract_test_invocation_step_names_and_anchor(
            gh=gh,
            full_name=full_name,
            base_ref=default_branch,
            workflow_yaml_text=yaml_text,
        )

        row = {
            "repo_url": repo_url,
            "full_name": full_name,
            "workflow_id": wf_id,
            "workflow_identifier": wf_name,
            "workflow_path": wf_path,
            "workflow_state": wf_state,
            "styles": ",".join(styles),
            "invocation_types": ",".join(invocation_types),
            "looks_like_instru": looks_like,
            "instru_detect_method": infer_instru_detect_method(styles, invocation_types),
            "third_party_provider_name": tp_name_str,
            "test_invocation_step_names": safe_join(step_inv_names, max_len=1200),
            "anchor_job_ordinal": "" if anchor_job_ordinal is None else str(anchor_job_ordinal),
            "anchor_step_ordinal_in_job": "" if anchor_step_ordinal_in_job is None else str(anchor_step_ordinal_in_job),
            "followed_files_count": str(followed_count),
            "unresolved_dynamic_refs_count": str(unresolved_dyn),
            "followed_paths": safe_join(followed_paths, max_len=1500),

            "called_instru_signal": "True" if called_instru_signal else "False",
            "called_instru_file_paths": safe_join_pipe(called_instru_file_paths, max_len=3000),
            "called_instru_origin_refs": safe_join_pipe(called_instru_origin_refs, max_len=3000),
            "called_instru_origin_step_names": safe_join_pipe(called_instru_origin_step_names, max_len=3000),
            "called_instru_file_types": safe_join_pipe(called_instru_file_types, max_len=1000),

            "stage1_extracted_at_utc": now_utc_iso(),
        }
        out_rows.append(row)
        stats["rows_written"] = int(stats["rows_written"]) + 1

    return out_rows, stats

def main() -> None:
    loaded_tokens = load_github_tokens(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
    tokens = external_repo_tokens()
    gh = GitHubClient(tokens)

    print(f"Stage1 ROOT_DIR: {ROOT_DIR}")
    print(f"Stage1 input CSV: {IN_URL_LIST_CSV}")
    print(f"Stage1 output CSV: {OUT_STAGE1_CSV}")
    print(f"Stage1 token sources loaded: {len(loaded_tokens)}")
    print(f"Stage1 cross-repo auth modes: {["GH_PAT" if t else "unauthenticated" for t in tokens]}")

    url_rows, url_fields = read_csv_rows(IN_URL_LIST_CSV)
    if not url_rows:
        raise RuntimeError("URL_List.csv is empty.")

    print(f"Stage1 loaded repo rows: {len(url_rows)}")
    print(f"Stage1 input columns: {url_fields}")

    candidates = ["repo_urls", "repo_url", "url", "repo"]

    def get_url(r: Dict[str, str]) -> str:
        for c in candidates:
            if (r.get(c) or "").strip():
                return (r.get(c) or "").strip()
        if url_fields:
            return (r.get(url_fields[0]) or "").strip()
        return ""

    repo_urls = unique_preserve([get_url(r) for r in url_rows])
    print(f"Stage1 normalized repo URLs: {len(repo_urls)}")
    print(f"Stage1 first repo URLs: {repo_urls[:3]}")

    stage1_rows: List[Dict[str, str]] = []

    repos_seen = 0
    repos_failed = 0
    repos_with_any_workflows_listed = 0
    workflow_files_seen = 0
    workflow_yaml_loaded = 0
    workflow_rows_written = 0

    for u in repo_urls:
        full_name = parse_repo_full_name(u)
        if not full_name:
            print(f"[skip] invalid repo url: {u}")
            continue
        repos_seen += 1
        try:
            rows, stats = build_stage1_rows_for_repo(gh, full_name, u)
            stage1_rows.extend(rows)

            if int(stats.get("workflows_listed", 0) or 0) > 0:
                repos_with_any_workflows_listed += 1
            workflow_files_seen += int(stats.get("workflow_files_seen", 0) or 0)
            workflow_yaml_loaded += int(stats.get("workflow_yaml_loaded", 0) or 0)
            workflow_rows_written += int(stats.get("rows_written", 0) or 0)

            print(
                f"[repo] {full_name} "
                f"source={stats.get('workflow_source')} "
                f"listed={stats.get('workflows_listed')} "
                f"yaml_loaded={stats.get('workflow_yaml_loaded')} "
                f"rows_written={stats.get('rows_written')}"
            )
            yaml_failures = stats.get("yaml_fetch_failures") or []
            if yaml_failures:
                print(f"[repo-yaml-miss] {full_name}: {yaml_failures[:5]}")
        except Exception as e:
            repos_failed += 1
            stage1_rows.append({
                "repo_url": u,
                "full_name": full_name,
                "workflow_id": "",
                "workflow_identifier": "",
                "workflow_path": "",
                "workflow_state": "",
                "styles": "",
                "invocation_types": "",
                "looks_like_instru": "no",
                "instru_detect_method": "error",
                "third_party_provider_name": "",
                "test_invocation_step_names": "",
                "anchor_job_ordinal": "",
                "anchor_step_ordinal_in_job": "",
                "followed_files_count": "0",
                "unresolved_dynamic_refs_count": "0",
                "followed_paths": "",
                "called_instru_signal": "False",
                "called_instru_file_paths": "",
                "called_instru_origin_refs": "",
                "called_instru_origin_step_names": "",
                "called_instru_file_types": "",
                "stage1_extracted_at_utc": now_utc_iso(),
            })
            print(f"[warn] {full_name}: {e}")

    out_fields = [
        "repo_url",
        "full_name",
        "workflow_id",
        "workflow_identifier",
        "workflow_path",
        "workflow_state",
        "styles",
        "invocation_types",
        "looks_like_instru",
        "instru_detect_method",
        "third_party_provider_name",
        "test_invocation_step_names",
        "anchor_job_ordinal",
        "anchor_step_ordinal_in_job",
        "followed_files_count",
        "unresolved_dynamic_refs_count",
        "followed_paths",
        "called_instru_signal",
        "called_instru_file_paths",
        "called_instru_origin_refs",
        "called_instru_origin_step_names",
        "called_instru_file_types",
        "stage1_extracted_at_utc",
    ]

    write_csv(OUT_STAGE1_CSV, out_fields, stage1_rows)
    print(f"Stage1 repos seen: {repos_seen}")
    print(f"Stage1 repos failed: {repos_failed}")
    print(f"Stage1 repos with any workflows listed: {repos_with_any_workflows_listed}")
    print(f"Stage1 workflow files seen: {workflow_files_seen}")
    print(f"Stage1 workflow YAML loaded: {workflow_yaml_loaded}")
    print(f"Stage1 rows written: {workflow_rows_written}")
    print("[done] Stage 1:", OUT_STAGE1_CSV, f"(rows={len(stage1_rows)})")

if __name__ == "__main__":
    main()