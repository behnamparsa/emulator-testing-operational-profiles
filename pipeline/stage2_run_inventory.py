# ============================================================
# Stage 2 V18 (CURRENT STUDY ALIGNED, AUXILIARY COMPLEXITY ENHANCED)
# Durable Layer-1 instrumentation-envelope inventory + run×style inventory
#
# V18 purpose
# ------------------------------------------------------------
# Keep the current study unit unchanged:
#   - Stage 2 = durable Layer-1 run inventory + run×style inventory
#   - Stage 3 = precise Layer-2 measured from step telemetry
#
# New in V18
# ------------------------------------------------------------
# Adds auxiliary fields to better expose and control for:
#   - repeated same-style execution within a run
#   - matrix-expanded jobs
#   - parallel same-style execution
#   - invocation-like multiplicity proxies at Stage 2
#
# These fields are descriptive/supportive only.
# They do NOT change the primary Stage-2 Layer-1 decomposition.
#
# Core study structure
# 1) Layer 1 decomposition  (Stage 2 durable, job-level)
# 2) Layer 2 decomposition  (Stage 3 precise, step-level; revised)
# 3) Auxiliary labeling / complexity fields
#
# ------------------------------------------------------------
# Layer 1 (Stage 2, durable, broad, job-level)
#
#   Run Duration
# = Time to Instrumentation Envelope
# + Instrumentation Job Envelope
# + Post-Instrumentation Tail
#
# where:
#   Time to Instrumentation Envelope
#     = run start -> first instrumentation-related job start
#
#   Instrumentation Job Envelope
#     = first instrumentation-related job start -> last instrumentation-related job end
#
#   Post-Instrumentation Tail
#     = last instrumentation-related job end -> run end
#
# ------------------------------------------------------------
# Layer 2 (Stage 3 onward, revised, precise, step-level)
#
#   Run Duration
# = Pre-Invocation
# + Invocation Execution Window
# + Post-Invocation
#
# where:
#   Pre-Invocation
#     = run start -> matched invocation step start
#
#   Invocation Execution Window
#     = style-relevant execution interval centered on the invocation anchor
#
#   Post-Invocation
#     = instrumentation execution end -> run completion
#
# IMPORTANT ALIGNMENT NOTE
# - Stage 2 does NOT directly measure Layer 2.
# - Stage 2 remains a durable Layer-1 inventory only.
# - However, Stage 2 exposes broad job-level proxy aliases aligned to the
#   revised Layer 2 naming for downstream compatibility:
#
#     pre_invocation_seconds_proxy
#     invocation_execution_window_seconds_proxy
#     post_invocation_seconds_proxy
#
# - These proxies are BROAD JOB-LEVEL approximations, not precise step-level values.
#
# Anchor concept
# - "Anchor job" is retained as the earliest strongest invocation-carrying job
# - It remains a semantic reference point only
# - The Layer-1 middle component is the broader instrumentation envelope,
#   not necessarily the anchor job alone
#
# Outputs
# - run_inventory.csv                (run-level durable inventory)
# - run_inventory_per_style.csv      (run × style durable Layer-1 inventory)
# ============================================================

import csv
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union

import requests
from config.runtime import get_root_dir, get_tokens_env_path, load_github_tokens

try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


# =========================
# CONFIG
# =========================
TOKENS_ENV_PATH = get_tokens_env_path()

ROOT_DIR = get_root_dir()

IN_VERIFIED_WORKFLOWS_CSV = ROOT_DIR / "verified_workflows_v16.csv"

OUT_RUN_INVENTORY_CSV = ROOT_DIR / "run_inventory.csv"
OUT_RUN_PER_STYLE_CSV = ROOT_DIR / "run_inventory_per_style.csv"

DEFAULT_BRANCH_ONLY = True
PROCESS_ONLY_LOOKS_LIKE_INSTRU = True
FETCH_JOBS_FOR_EACH_RUN = True

MAX_RUNS_PER_WORKFLOW: Optional[int] = None
RUN_CREATED_AT_AFTER: Optional[str] = None

CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 60
MAX_RETRIES_PER_REQUEST = 8
BACKOFF_BASE_S = 1.7
BACKOFF_CAP_S = 60
MAX_PAGES_PER_LIST = 2000

MAX_TOKENS_TO_USE = 5
SLEEP_BETWEEN_WORKFLOWS_SEC = 0.05


# =========================
# Helpers
# =========================
STYLE_CANONICAL = ["Community", "Custom", "GMD", "Third-Party", "Real-Device"]

STYLE_ALIASES = {
    "community": "Community",
    "custom": "Custom",
    "gmd": "GMD",
    "third party": "Third-Party",
    "third-party": "Third-Party",
    "third_party": "Third-Party",
    "thirdparty": "Third-Party",
    "3p": "Third-Party",
    "real device": "Real-Device",
    "real-device": "Real-Device",
    "real_device": "Real-Device",
    "realdevice": "Real-Device",

    "emu community": "Community",
    "emulator community": "Community",
    "emu_custom": "Custom",
    "emu custom": "Custom",
    "emulator custom": "Custom",
    "real devices": "Real-Device",
    "real-devices": "Real-Device",
    "real_devices": "Real-Device",
    "realdevices": "Real-Device",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def norm(s: Optional[str]) -> str:
    return (s or "").strip()


def low(s: Optional[str]) -> str:
    return norm(s).lower()


def canon_key(s: Optional[str]) -> str:
    x = low(s).replace("_", " ").replace("-", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x


def normalize_style_label(s: Optional[str]) -> str:
    return STYLE_ALIASES.get(canon_key(s), norm(s))


def split_styles(s: Optional[str]) -> List[str]:
    raw = norm(s)
    if not raw:
        return []
    vals = [normalize_style_label(x) for x in re.split(r"[|,;/]+", raw) if norm(x)]
    return unique_preserve([v for v in vals if v in STYLE_CANONICAL])


def iso_to_dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
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


def ensure_csv_header(csv_path: Path, fieldnames: List[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def append_row(csv_path: Path, fieldnames: List[str], row: Dict) -> None:
    with csv_path.open("a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writerow({k: row.get(k, "") for k in fieldnames})


def load_existing_keys(csv_path: Path, key_field: str) -> Set[str]:
    keys: Set[str] = set()
    if not csv_path.exists():
        return keys
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            k = norm(row.get(key_field))
            if k:
                keys.add(k)
    return keys


def unique_preserve(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        s = norm(str(x))
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def safe_join_names(names: List[str], max_len: int = 800) -> str:
    s = ",".join(unique_preserve(names))
    return s if len(s) <= max_len else s[: max_len - 3] + "..."


def read_env_tokens(path: Path) -> List[str]:
    if not path.exists():
        raise FileNotFoundError(f"Tokens env file not found: {path}")
    toks: List[str] = []
    accepted_prefixes = ("GH_PAT", "GITHUB_TOKEN")
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        val = v.strip().strip('"').strip("'")
        if any(key == prefix or key.startswith(prefix + "_") for prefix in accepted_prefixes):
            if val and val not in toks:
                toks.append(val)
                if len(toks) >= MAX_TOKENS_TO_USE:
                    break
    if not toks:
        raise ValueError(f"No GitHub tokens found in {path}")
    return toks


# =========================
# Name normalization / matching
# =========================
_NORM_WS_RE = re.compile(r"\s+")
_NORM_PUNCT_RE = re.compile(r"[\[\]\(\)\{\}:;|]+")


def normalize_name(s: str) -> str:
    x = (s or "").strip().strip('"').strip("'").lower()
    x = _NORM_PUNCT_RE.sub(" ", x)
    x = _NORM_WS_RE.sub(" ", x).strip()
    return x


def parse_anchor_step_names(csv_value: str) -> List[str]:
    if not csv_value:
        return []
    raw = [p.strip() for p in str(csv_value).split(",")]
    return unique_preserve([r for r in raw if r])


def anchored_step_match(runtime_step_name: str, anchor_names: List[str]) -> bool:
    rn = normalize_name(runtime_step_name)
    if not rn:
        return False
    for a in anchor_names:
        an = normalize_name(a)
        if not an:
            continue
        if rn == an or an in rn or rn in an:
            return True
    return False


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
            "User-Agent": "run-inventory-stage2-v18-layer1-envelope-auxiliary/1.0",
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

            text_l = low(resp.text or "")
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
def get_repo_default_branch(gh: GitHubClient, full_name: str) -> str:
    data = gh.request_json("GET", f"https://api.github.com/repos/{full_name}")
    if not data or not isinstance(data, dict):
        return ""
    return norm(data.get("default_branch"))


def list_workflow_runs(gh: GitHubClient, full_name: str, workflow_id_or_file: str, branch: Optional[str]) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/workflows/{workflow_id_or_file}/runs"
    params = {"branch": branch} if branch else {}
    return list(gh.paginate(url, params=params, item_key="workflow_runs"))


def list_run_jobs(gh: GitHubClient, full_name: str, run_id: int) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/runs/{run_id}/jobs"
    return list(gh.paginate(url, params={}, item_key="jobs"))


# =========================
# Job / step heuristics
# =========================
THIRD_PARTY_PROVIDER_RE = re.compile(
    r"(browserstack|sauce\s*labs|saucelabs|kobiton|headspin|bitbar|perfecto|lambdatest|genymotion\s*cloud|firebase\s*test\s*lab)",
    re.I,
)
THIRD_PARTY_LIFECYCLE_RE = re.compile(
    r"(start|stop|upload|download|results?|report|reports|session|app url|build id|device logs?|summary|finali[sz]e)",
    re.I,
)
THIRD_PARTY_INVOKE_RE = re.compile(
    r"(firebase\s+test\s+android\s+run|gcloud\s+firebase\s+test\s+android\s+run|flank\s+android\s+run|appcenter\s+test\s+run|saucectl\s+(run|test)|browserstack|maestro\s+cloud)",
    re.I,
)
GMD_RE = re.compile(r"(manageddevice|gmd|gradle managed device|managed device)", re.I)
COMMUNITY_RE = re.compile(
    r"(android-emulator-runner|emulator runner|create avd|avd|start emulator|emulator|connectedcheck|connectedandroidtest|androidtest)",
    re.I,
)
CUSTOM_RE = re.compile(
    r"(detox|flutter.*integration|integration test|baseline.?profile|macrobenchmark|uiautomator|espresso|instrumentation)",
    re.I,
)
GENERIC_INSTRU_RE = re.compile(
    r"(connectedcheck|connectedandroidtest|androidtest|instrumentation|manageddevice|gmd|detox|flutter.*integration|integration test|baseline.?profile|macrobenchmark|uiautomator|espresso|emulator|avd|firebase test|test lab|device farm)",
    re.I,
)
ARTIFACT_STEP_RE = re.compile(
    r"(upload-artifact|download-artifact|artifact|test-results|test results|results|report|reports|logs?|summary)",
    re.I,
)


def get_job_runtime_text(job: Dict) -> str:
    vals: List[str] = []
    vals.append(norm(job.get("name")))
    for st in (job.get("steps") or []):
        vals.append(norm(st.get("name")))
    return " | ".join([v for v in vals if v])


def job_base_name(job_name: str) -> str:
    """
    Collapse common matrix-expanded job suffixes:
      build (Foss) -> build
      test [api35] -> test
    """
    x = norm(job_name)
    if not x:
        return ""
    x = re.sub(r"\s+\([^)]*\)\s*$", "", x).strip()
    x = re.sub(r"\s+\[[^\]]*\]\s*$", "", x).strip()
    return x


def is_matrix_like_job_name(job_name: str) -> bool:
    x = norm(job_name)
    if not x:
        return False
    base = job_base_name(x)
    return bool(base and base != x)


def detect_job_style_tags(job: Dict, declared_styles: List[str], anchor_step_names: List[str]) -> Tuple[bool, List[str], str]:
    """
    Returns:
      is_instru_related_job, matched_styles, detect_method

    V18 framing:
    - Stage 2 is still a broad Layer-1 detector of instrumentation-related jobs
    - continuation/finalization jobs may still belong to the broad instrumentation envelope
    - auxiliary fields now expose multiplicity/complexity for repeated same-style cases
    """
    text = get_job_runtime_text(job)
    steps = job.get("steps") if isinstance(job.get("steps"), list) else []

    anchor_hit = False
    for st in steps:
        step_name = norm(st.get("name"))
        if step_name and anchored_step_match(step_name, anchor_step_names):
            anchor_hit = True
            break

    generic_instru = bool(GENERIC_INSTRU_RE.search(text))
    tp_hit = bool(THIRD_PARTY_PROVIDER_RE.search(text))
    tp_lifecycle = bool(THIRD_PARTY_LIFECYCLE_RE.search(text))
    gmd_hit = bool(GMD_RE.search(text))
    community_hit = bool(COMMUNITY_RE.search(text))
    custom_hit = bool(CUSTOM_RE.search(text))
    artifact_hit = bool(ARTIFACT_STEP_RE.search(text))
    real_device_hit = bool(re.search(r"(firebase test lab|device farm|real device)", text, re.I))

    matched_styles: List[str] = []

    if "Third-Party" in declared_styles and (tp_hit or (artifact_hit and tp_lifecycle)):
        matched_styles.append("Third-Party")
    if "GMD" in declared_styles and gmd_hit:
        matched_styles.append("GMD")
    if "Community" in declared_styles and community_hit:
        matched_styles.append("Community")
    if "Custom" in declared_styles and custom_hit:
        matched_styles.append("Custom")
    if "Real-Device" in declared_styles and real_device_hit:
        matched_styles.append("Real-Device")

    matched_styles = unique_preserve(matched_styles)

    is_instru_related_job = False
    detect_method = "none"

    if anchor_hit:
        is_instru_related_job = True
        detect_method = "step_name_anchor"
    elif generic_instru or tp_hit:
        is_instru_related_job = True
        detect_method = "job_or_step_text_third_party" if tp_hit else "job_or_step_text_regex"
    elif artifact_hit and tp_hit and tp_lifecycle:
        is_instru_related_job = True
        detect_method = "third_party_lifecycle_artifact"
    elif artifact_hit and len(declared_styles) == 1 and generic_instru:
        is_instru_related_job = True
        detect_method = "artifact_plus_instru_text"

    if is_instru_related_job and not matched_styles and len(declared_styles) == 1:
        matched_styles = declared_styles[:]
        detect_method = detect_method + "_single_declared_style"

    return is_instru_related_job, matched_styles, detect_method


def step_is_invocation_candidate(step_name: str, target_style: str, anchor_step_names: List[str]) -> bool:
    s = norm(step_name)
    if not s:
        return False
    if anchored_step_match(s, anchor_step_names):
        return True

    style = normalize_style_label(target_style)
    if style == "Third-Party":
        return bool(THIRD_PARTY_INVOKE_RE.search(s) or THIRD_PARTY_PROVIDER_RE.search(s))
    if style == "GMD":
        return bool(GMD_RE.search(s))
    if style == "Community":
        return bool(COMMUNITY_RE.search(s))
    if style == "Custom":
        return bool(CUSTOM_RE.search(s))

    return bool(GENERIC_INSTRU_RE.search(s))


def compute_parallel_overlap_stats(windows: List[Tuple[Optional[datetime], Optional[datetime]]]) -> Tuple[bool, int]:
    """
    Returns:
      has_overlap, max_parallel
    """
    events: List[Tuple[datetime, int]] = []
    for sdt, edt in windows:
        if not sdt or not edt:
            continue
        if edt < sdt:
            continue
        events.append((sdt, +1))
        events.append((edt, -1))

    if not events:
        return False, 0

    # Start before end on ties
    events.sort(key=lambda x: (x[0], -x[1]))

    current = 0
    max_parallel = 0
    has_overlap = False
    for _t, delta in events:
        current += delta
        if current > max_parallel:
            max_parallel = current
        if current >= 2:
            has_overlap = True

    return has_overlap, max_parallel


# =========================
# Timing helpers
# =========================
def compute_run_window_from_jobs(
    jobs: List[Dict],
    run_started_at: str,
    run_updated_at: str,
) -> Tuple[str, str, Optional[int], str]:
    starts: List[datetime] = []
    ends: List[datetime] = []

    for j in jobs:
        sdt = iso_to_dt(j.get("started_at"))
        edt = iso_to_dt(j.get("completed_at"))
        if sdt:
            starts.append(sdt)
        if edt:
            ends.append(edt)

    run_start_dt = iso_to_dt(run_started_at)
    run_end_dt = iso_to_dt(run_updated_at)

    if not run_start_dt and starts:
        run_start_dt = min(starts)
    if not run_end_dt and ends:
        run_end_dt = max(ends)

    source = "run_api"
    if run_start_dt and run_end_dt and not (iso_to_dt(run_started_at) and iso_to_dt(run_updated_at)):
        source = "hybrid_run_api_jobs"
    elif not (iso_to_dt(run_started_at) and iso_to_dt(run_updated_at)) and starts and ends:
        source = "jobs_window"

    return (
        run_start_dt.isoformat().replace("+00:00", "Z") if run_start_dt else "",
        run_end_dt.isoformat().replace("+00:00", "Z") if run_end_dt else "",
        dt_to_seconds(run_start_dt, run_end_dt),
        source if run_start_dt and run_end_dt else "missing",
    )


def pick_anchor_job(
    jobs: List[Dict],
    declared_styles: List[str],
    anchor_step_names: List[str],
) -> Tuple[Optional[Dict], str, List[str]]:
    """
    Pick the earliest instrumentation-related job in the run.
    Returns:
      anchor_job, detect_method, all_instru_related_job_names
    """
    candidates: List[Tuple[datetime, Dict, str]] = []
    all_instru_related_job_names: List[str] = []

    for j in jobs:
        is_instru_related_job, _matched_styles, detect_method = detect_job_style_tags(
            job=j,
            declared_styles=declared_styles,
            anchor_step_names=anchor_step_names,
        )
        if not is_instru_related_job:
            continue

        jname = norm(j.get("name"))
        if jname:
            all_instru_related_job_names.append(jname)

        js = iso_to_dt(j.get("started_at"))
        if js:
            candidates.append((js, j, detect_method))

    if not candidates:
        return None, "none", unique_preserve(all_instru_related_job_names)

    candidates.sort(key=lambda x: x[0])
    _dt, anchor_job, detect_method = candidates[0]
    return anchor_job, detect_method, unique_preserve(all_instru_related_job_names)


# =========================
# Inventory builders
# =========================
def build_run_level_metrics(
    jobs: List[Dict],
    run_created_at: str,
    run_started_at: str,
    run_updated_at: str,
    anchor_step_names: List[str],
    declared_styles: List[str],
) -> Dict[str, Union[str, int, None]]:
    """
    Run-level durable Layer-1 metrics for V18:
    - broad instrumentation job envelope
    - anchor job retained as semantic reference
    - Layer-2-aligned proxy aliases exposed for downstream convenience,
      but still based on Layer-1 broad job telemetry
    - auxiliary multiplicity/matrix/parallel complexity fields added
    """
    out: Dict[str, Union[str, int, None]] = {
        "queue_seconds": None,
        "run_duration_seconds": None,
        "runner_labels_union": "",
        "instru_job_count": 0,
        "instru_job_names": "",
        "instru_detect_method": "none",

        # broad instrumentation envelope
        "instru_first_started_at": "",
        "instru_last_completed_at": "",
        "instru_window_seconds": None,

        "time_to_instrumentation_envelope_seconds": None,
        "instrumentation_job_envelope_seconds": None,
        "post_instrumentation_tail_seconds": None,
        "layer1_model": "instrumentation_job_envelope",
        "layer1_proxy_quality": "missing",

        # anchor job retained as reference point
        "anchor_job_name": "",
        "anchor_job_started_at": "",
        "anchor_job_completed_at": "",
        "anchor_job_source": "missing",

        # revised broad proxy aliases
        "pre_invocation_seconds_proxy": None,
        "invocation_execution_window_seconds_proxy": None,
        "post_invocation_seconds_proxy": None,

        # backward-friendly legacy aliases
        "time_to_invocation_seconds_proxy": None,
        "invocation_tail_seconds_proxy": None,
        "time_to_first_instru_seconds": None,
        "anchor_job_start_source": "missing",
        "time_to_first_instru_from_anchor_job_seconds": None,
        "time_to_first_instru_from_anchor_job_quality": "missing",

        # new V18 run-level auxiliary fields
        "instru_distinct_job_count": 0,
        "instru_distinct_job_base_name_count": 0,
        "instru_matrix_like_job_count": 0,
        "instru_matrix_expanded_flag": "false",
        "instru_parallel_jobs_flag": "false",
        "instru_max_parallel_jobs": 0,
    }

    out["queue_seconds"] = dt_to_seconds(iso_to_dt(run_created_at), iso_to_dt(run_started_at))

    run_start_eff, run_end_eff, run_dur_eff, _ = compute_run_window_from_jobs(
        jobs=jobs,
        run_started_at=run_started_at,
        run_updated_at=run_updated_at,
    )
    out["run_duration_seconds"] = run_dur_eff

    labels_union: Set[str] = set()
    instru_related_job_names: List[str] = []
    instru_related_job_base_names: List[str] = []
    instru_starts: List[Tuple[datetime, str, str]] = []
    instru_ends: List[Tuple[datetime, str, str]] = []
    instru_windows: List[Tuple[Optional[datetime], Optional[datetime]]] = []

    for j in jobs:
        for lab in (j.get("labels") or []):
            if isinstance(lab, str) and norm(lab):
                labels_union.add(norm(lab))

        is_instru_related_job, _styles, detect_method = detect_job_style_tags(
            job=j,
            declared_styles=declared_styles,
            anchor_step_names=anchor_step_names,
        )
        if not is_instru_related_job:
            continue

        jname = norm(j.get("name"))
        jbase = job_base_name(jname)
        js = iso_to_dt(j.get("started_at"))
        je = iso_to_dt(j.get("completed_at"))

        if jname:
            instru_related_job_names.append(jname)
            instru_related_job_base_names.append(jbase or jname)
        if js:
            instru_starts.append((js, jname, detect_method))
        if je:
            instru_ends.append((je, jname, detect_method))
        instru_windows.append((js, je))

    out["runner_labels_union"] = ",".join(sorted(labels_union))
    out["instru_job_names"] = safe_join_names(instru_related_job_names)
    out["instru_job_count"] = len(unique_preserve(instru_related_job_names))
    out["instru_distinct_job_count"] = len(unique_preserve(instru_related_job_names))
    out["instru_distinct_job_base_name_count"] = len(unique_preserve(instru_related_job_base_names))
    out["instru_matrix_like_job_count"] = sum(1 for n in unique_preserve(instru_related_job_names) if is_matrix_like_job_name(n))

    if (
        out["instru_distinct_job_count"] is not None
        and out["instru_distinct_job_base_name_count"] is not None
        and int(out["instru_distinct_job_count"]) > int(out["instru_distinct_job_base_name_count"])
    ):
        out["instru_matrix_expanded_flag"] = "true"

    has_parallel, max_parallel = compute_parallel_overlap_stats(instru_windows)
    out["instru_parallel_jobs_flag"] = "true" if has_parallel else "false"
    out["instru_max_parallel_jobs"] = max_parallel

    run_start_dt = iso_to_dt(run_start_eff)
    run_end_dt = iso_to_dt(run_end_eff)

    if instru_starts:
        instru_starts.sort(key=lambda x: x[0])
        first_dt, _first_name, first_method = instru_starts[0]
        out["instru_first_started_at"] = first_dt.isoformat().replace("+00:00", "Z")
        out["time_to_instrumentation_envelope_seconds"] = dt_to_seconds(run_start_dt, first_dt)
        out["instru_detect_method"] = first_method or "job_text"

    if instru_ends:
        instru_ends.sort(key=lambda x: x[0])
        last_dt, _last_name, _last_method = instru_ends[-1]
        out["instru_last_completed_at"] = last_dt.isoformat().replace("+00:00", "Z")
        out["post_instrumentation_tail_seconds"] = dt_to_seconds(last_dt, run_end_dt)

    if out["instru_first_started_at"] and out["instru_last_completed_at"]:
        first_dt = iso_to_dt(str(out["instru_first_started_at"]))
        last_dt = iso_to_dt(str(out["instru_last_completed_at"]))
        out["instru_window_seconds"] = dt_to_seconds(first_dt, last_dt)
        out["instrumentation_job_envelope_seconds"] = out["instru_window_seconds"]

    anchor_job, detect_method, all_instru_job_names = pick_anchor_job(
        jobs=jobs,
        declared_styles=declared_styles,
        anchor_step_names=anchor_step_names,
    )
    if all_instru_job_names:
        out["instru_job_names"] = safe_join_names(all_instru_job_names)
        out["instru_job_count"] = len(all_instru_job_names)
        out["instru_distinct_job_count"] = len(unique_preserve(all_instru_job_names))
        out["instru_matrix_like_job_count"] = sum(1 for n in unique_preserve(all_instru_job_names) if is_matrix_like_job_name(n))
    if detect_method and detect_method != "none" and out["instru_detect_method"] == "none":
        out["instru_detect_method"] = detect_method

    if anchor_job is not None:
        anchor_name = norm(anchor_job.get("name"))
        anchor_start_dt = iso_to_dt(anchor_job.get("started_at"))
        anchor_end_dt = iso_to_dt(anchor_job.get("completed_at"))

        out["anchor_job_name"] = anchor_name
        out["anchor_job_started_at"] = anchor_start_dt.isoformat().replace("+00:00", "Z") if anchor_start_dt else ""
        out["anchor_job_completed_at"] = anchor_end_dt.isoformat().replace("+00:00", "Z") if anchor_end_dt else ""
        out["anchor_job_source"] = detect_method or "job_text"

        out["anchor_job_start_source"] = out["anchor_job_source"]
        out["time_to_first_instru_from_anchor_job_seconds"] = 0 if anchor_start_dt else None
        out["time_to_first_instru_from_anchor_job_quality"] = "anchor_job_reference" if anchor_start_dt else "missing"

    out["pre_invocation_seconds_proxy"] = out["time_to_instrumentation_envelope_seconds"]
    out["invocation_execution_window_seconds_proxy"] = out["instrumentation_job_envelope_seconds"]
    out["post_invocation_seconds_proxy"] = out["post_instrumentation_tail_seconds"]

    out["time_to_invocation_seconds_proxy"] = out["time_to_instrumentation_envelope_seconds"]
    out["invocation_tail_seconds_proxy"] = out["post_instrumentation_tail_seconds"]
    out["time_to_first_instru_seconds"] = out["time_to_instrumentation_envelope_seconds"]

    if (
        out["time_to_instrumentation_envelope_seconds"] is not None
        and out["instrumentation_job_envelope_seconds"] is not None
        and out["post_instrumentation_tail_seconds"] is not None
    ):
        out["layer1_proxy_quality"] = "broad_job_envelope"
    elif out["instrumentation_job_envelope_seconds"] is not None:
        out["layer1_proxy_quality"] = "partial_broad_job_envelope"

    return out


def build_run_per_style_rows(
    full_name: str,
    workflow_identifier: str,
    workflow_id: str,
    workflow_path: str,
    run: Dict,
    declared_styles: List[str],
    anchor_step_names: List[str],
    jobs: List[Dict],
    run_start_eff: str,
    run_end_eff: str,
    run_duration_eff: Optional[int],
    run_timing_source: str,
) -> List[Dict[str, object]]:
    """
    Build durable Layer-1 run × style rows for V18.
    Core Stage-2 middle component remains the broad instrumentation envelope.
    Revised Layer-2-aligned proxy names are exposed as broad job-level proxies only.
    New auxiliary fields expose same-style multiplicity / matrix / parallel complexity.
    """
    run_start_dt = iso_to_dt(run_start_eff)
    run_end_dt = iso_to_dt(run_end_eff)

    style_to_jobs: Dict[str, List[Dict]] = {s: [] for s in declared_styles}
    ambiguous_job_names: List[str] = []
    all_instru_job_names_any: List[str] = []

    for j in jobs:
        is_instru_related_job, matched_styles, detect_method = detect_job_style_tags(
            job=j,
            declared_styles=declared_styles,
            anchor_step_names=anchor_step_names,
        )
        if not is_instru_related_job:
            continue

        jname = norm(j.get("name"))
        if jname:
            all_instru_job_names_any.append(jname)

        payload = {
            "job": j,
            "detect_method": detect_method,
        }

        if len(matched_styles) == 1:
            style_to_jobs[matched_styles[0]].append(payload)
        elif len(matched_styles) > 1:
            if jname:
                ambiguous_job_names.append(jname)
        else:
            if len(declared_styles) == 1:
                style_to_jobs[declared_styles[0]].append({
                    "job": j,
                    "detect_method": detect_method + "_fallback_single_style",
                })
            else:
                if jname:
                    ambiguous_job_names.append(jname)

    rows: List[Dict[str, object]] = []

    multi_style_run_flag = len(declared_styles) > 1
    all_styles_in_run = safe_join_names(declared_styles)
    all_instru_job_names_any_s = safe_join_names(all_instru_job_names_any)
    ambiguous_jobs_s = safe_join_names(ambiguous_job_names)

    for style in declared_styles:
        assigned = style_to_jobs.get(style, [])

        assigned_job_names: List[str] = []
        assigned_job_base_names: List[str] = []
        detect_methods: List[str] = []

        anchor_job = None
        anchor_job_name = ""
        anchor_job_started_at = ""
        anchor_job_completed_at = ""
        anchor_job_source = "missing"

        anchor_job_start_dt: Optional[datetime] = None
        anchor_job_end_dt: Optional[datetime] = None

        style_first_instru_job_name = ""
        style_first_instru_job_started_at = ""
        style_first_instru_job_source = "missing"
        style_last_instru_job_name = ""
        style_last_instru_job_completed_at = ""
        style_last_instru_job_source = "missing"

        first_any_dt: Optional[datetime] = None
        last_any_dt: Optional[datetime] = None

        candidates: List[Tuple[datetime, Dict, str]] = []
        end_candidates: List[Tuple[datetime, Dict, str]] = []

        # New V18 auxiliary trackers
        style_windows: List[Tuple[Optional[datetime], Optional[datetime]]] = []
        style_invocation_candidate_step_names: List[str] = []
        style_invocation_candidate_step_count = 0

        for item in assigned:
            j = item["job"]
            detect_method = norm(item["detect_method"])
            detect_methods.append(detect_method)

            jname = norm(j.get("name"))
            jbase = job_base_name(jname)
            js = iso_to_dt(j.get("started_at"))
            je = iso_to_dt(j.get("completed_at"))

            if jname:
                assigned_job_names.append(jname)
                assigned_job_base_names.append(jbase or jname)

            style_windows.append((js, je))

            # job-level timing
            if js:
                candidates.append((js, j, detect_method))
                if first_any_dt is None or js < first_any_dt:
                    first_any_dt = js
                    style_first_instru_job_name = jname
                    style_first_instru_job_started_at = js.isoformat().replace("+00:00", "Z")
                    style_first_instru_job_source = detect_method or "job_text"

            if je:
                end_candidates.append((je, j, detect_method))
                if last_any_dt is None or je > last_any_dt:
                    last_any_dt = je
                    style_last_instru_job_name = jname
                    style_last_instru_job_completed_at = je.isoformat().replace("+00:00", "Z")
                    style_last_instru_job_source = detect_method or "job_text"

            # step-level invocation candidate proxy counting
            steps = j.get("steps") if isinstance(j.get("steps"), list) else []
            for st in steps:
                step_name = norm(st.get("name"))
                if step_is_invocation_candidate(step_name, style, anchor_step_names):
                    style_invocation_candidate_step_count += 1
                    style_invocation_candidate_step_names.append(step_name)

        if candidates:
            candidates.sort(key=lambda x: x[0])
            _dt, anchor_job, anchor_method = candidates[0]
            anchor_job_name = norm(anchor_job.get("name"))
            anchor_job_start_dt = iso_to_dt(anchor_job.get("started_at"))
            anchor_job_end_dt = iso_to_dt(anchor_job.get("completed_at"))
            anchor_job_started_at = anchor_job_start_dt.isoformat().replace("+00:00", "Z") if anchor_job_start_dt else ""
            anchor_job_completed_at = anchor_job_end_dt.isoformat().replace("+00:00", "Z") if anchor_job_end_dt else ""
            anchor_job_source = anchor_method or "job_text"

        time_to_instrumentation_envelope = dt_to_seconds(run_start_dt, first_any_dt)
        instrumentation_job_envelope = dt_to_seconds(first_any_dt, last_any_dt)
        post_instrumentation_tail = dt_to_seconds(last_any_dt, run_end_dt)

        segmentation_confidence = (
            "high" if first_any_dt is not None and last_any_dt is not None and not ambiguous_job_names else
            "medium" if first_any_dt is not None else
            "missing"
        )

        style_distinct_job_count = len(unique_preserve(assigned_job_names))
        style_distinct_job_base_name_count = len(unique_preserve(assigned_job_base_names))
        style_matrix_like_job_count = sum(1 for n in unique_preserve(assigned_job_names) if is_matrix_like_job_name(n))
        style_matrix_expanded_flag = "true" if style_distinct_job_count > style_distinct_job_base_name_count else "false"

        style_parallel_same_style, style_max_parallel_jobs = compute_parallel_overlap_stats(style_windows)
        style_parallel_same_style_flag = "true" if style_parallel_same_style else "false"

        style_repeated_same_style_flag = "true" if (
            style_distinct_job_count > 1
            or style_invocation_candidate_step_count > 1
            or style_matrix_expanded_flag == "true"
        ) else "false"

        if style_distinct_job_count <= 1 and style_invocation_candidate_step_count <= 1:
            style_same_style_complexity_class = "single_path"
        elif style_matrix_expanded_flag == "true" and style_parallel_same_style_flag == "true":
            style_same_style_complexity_class = "matrix_parallel_repeated"
        elif style_matrix_expanded_flag == "true":
            style_same_style_complexity_class = "matrix_repeated"
        elif style_distinct_job_count > 1 and style_parallel_same_style_flag == "true":
            style_same_style_complexity_class = "parallel_repeated"
        elif style_distinct_job_count > 1 or style_invocation_candidate_step_count > 1:
            style_same_style_complexity_class = "serial_or_multi_candidate_repeated"
        else:
            style_same_style_complexity_class = "single_path"

        row = {
            "full_name": full_name,
            "workflow_identifier": workflow_identifier,
            "workflow_id": workflow_id,
            "workflow_path": workflow_path,

            "run_id": norm(str(run.get("id") or "")),
            "run_number": norm(str(run.get("run_number") or "")),
            "run_attempt": norm(str(run.get("run_attempt") or "")),
            "created_at": norm(run.get("created_at")),
            "run_started_at": norm(run.get("run_started_at")),
            "run_updated_at": norm(run.get("updated_at")),
            "status": norm(run.get("status")),
            "run_conclusion": norm(run.get("conclusion")),
            "event": norm(run.get("event")),
            "head_branch": norm(run.get("head_branch")),
            "head_sha": norm(run.get("head_sha")),
            "html_url": norm(run.get("html_url")),

            "target_style": style,
            "styles_in_run_all": all_styles_in_run,
            "multi_style_run_flag": "true" if multi_style_run_flag else "false",

            "layer1_run_started_at_effective": run_start_eff,
            "layer1_run_ended_at_effective": run_end_eff,
            "layer1_run_duration_seconds_effective": "" if run_duration_eff is None else str(run_duration_eff),
            "layer1_run_timing_source": run_timing_source,

            "style_instru_job_count": str(style_distinct_job_count),
            "style_instru_job_names": safe_join_names(assigned_job_names),

            "style_first_instru_job_name": style_first_instru_job_name,
            "style_first_instru_job_started_at": style_first_instru_job_started_at,
            "style_first_instru_job_source": style_first_instru_job_source,
            "style_last_instru_job_name": style_last_instru_job_name,
            "style_last_instru_job_completed_at": style_last_instru_job_completed_at,
            "style_last_instru_job_source": style_last_instru_job_source,

            # V18 new auxiliary complexity fields
            "style_distinct_job_count": str(style_distinct_job_count),
            "style_distinct_job_base_name_count": str(style_distinct_job_base_name_count),
            "style_matrix_like_job_count": str(style_matrix_like_job_count),
            "style_matrix_expanded_flag": style_matrix_expanded_flag,
            "style_parallel_same_style_flag": style_parallel_same_style_flag,
            "style_max_parallel_jobs": str(style_max_parallel_jobs),
            "style_repeated_same_style_flag": style_repeated_same_style_flag,
            "style_invocation_candidate_step_count_proxy": str(style_invocation_candidate_step_count),
            "style_distinct_invocation_step_name_count_proxy": str(len(unique_preserve(style_invocation_candidate_step_names))),
            "style_invocation_candidate_step_names_proxy": safe_join_names(style_invocation_candidate_step_names),
            "style_same_style_complexity_class": style_same_style_complexity_class,

            # preferred Layer-1 fields
            "style_time_to_instrumentation_envelope_seconds": "" if time_to_instrumentation_envelope is None else str(time_to_instrumentation_envelope),
            "style_instrumentation_job_envelope_seconds": "" if instrumentation_job_envelope is None else str(instrumentation_job_envelope),
            "style_post_instrumentation_tail_seconds": "" if post_instrumentation_tail is None else str(post_instrumentation_tail),
            "style_layer1_model": "instrumentation_job_envelope",

            # anchor reference fields
            "style_anchor_job_name": anchor_job_name,
            "style_anchor_job_started_at": anchor_job_started_at,
            "style_anchor_job_completed_at": anchor_job_completed_at,
            "style_anchor_job_source": anchor_job_source,

            # broad proxy aliases
            "style_pre_invocation_seconds_proxy": "" if time_to_instrumentation_envelope is None else str(time_to_instrumentation_envelope),
            "style_invocation_execution_window_seconds_proxy": "" if instrumentation_job_envelope is None else str(instrumentation_job_envelope),
            "style_post_invocation_seconds_proxy": "" if post_instrumentation_tail is None else str(post_instrumentation_tail),

            # backward-friendly legacy aliases
            "style_time_to_invocation_seconds_proxy": "" if time_to_instrumentation_envelope is None else str(time_to_instrumentation_envelope),
            "style_invocation_tail_seconds_proxy": "" if post_instrumentation_tail is None else str(post_instrumentation_tail),
            "style_time_to_instru_job_start_seconds": "" if time_to_instrumentation_envelope is None else str(time_to_instrumentation_envelope),
            "style_instru_job_envelope_seconds": "" if instrumentation_job_envelope is None else str(instrumentation_job_envelope),
            "style_post_instru_job_tail_seconds": "" if post_instrumentation_tail is None else str(post_instrumentation_tail),

            "style_job_detection_methods": safe_join_names(detect_methods),
            "style_job_segmentation_confidence": segmentation_confidence,
            "style_overlap_with_other_styles_flag": "true" if ambiguous_job_names else "false",
            "ambiguous_instru_job_names_in_run": ambiguous_jobs_s,
            "all_instru_job_names_in_run": all_instru_job_names_any_s,

            "extracted_at_utc": now_utc_iso(),
        }
        rows.append(row)

    return rows


# =========================
# Read verified workflows
# =========================
def load_verified_workflows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Verified workflows CSV not found: {path}")
    rows: List[Dict[str, str]] = []
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({(k or ""): (v or "") for k, v in r.items()})
    return rows


# =========================
# MAIN
# =========================
def main() -> None:
    if not IN_VERIFIED_WORKFLOWS_CSV.exists():
        raise FileNotFoundError(f"Missing input: {IN_VERIFIED_WORKFLOWS_CSV}")

    if OUT_RUN_INVENTORY_CSV.exists():
        OUT_RUN_INVENTORY_CSV.unlink()
    if OUT_RUN_PER_STYLE_CSV.exists():
        OUT_RUN_PER_STYLE_CSV.unlink()

    try:
        tokens = load_github_tokens(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
        print(f"Loaded GitHub token pool size: {len(tokens)}")
    except Exception:
        tokens = read_env_tokens(TOKENS_ENV_PATH)
    gh = GitHubClient(tokens)

    rows = load_verified_workflows(IN_VERIFIED_WORKFLOWS_CSV)
    if PROCESS_ONLY_LOOKS_LIKE_INSTRU:
        rows = [r for r in rows if low(r.get("looks_like_instru")) == "yes"]

    if not rows:
        raise RuntimeError("No workflows found to process (check verified CSV or filter).")

    after_dt = iso_to_dt(RUN_CREATED_AT_AFTER) if RUN_CREATED_AT_AFTER else None

    out_run_fields = [
        # workflow identity
        "full_name",
        "default_branch",
        "workflow_identifier",
        "workflow_id",
        "workflow_path",

        # Stage 1 labels / pass-through
        "looks_like_instru",
        "styles",
        "invocation_types",
        "third_party_provider_name",
        "test_invocation_step_names",
        "anchor_job_ordinal",
        "anchor_step_ordinal_in_job",
        "called_instru_signal",
        "called_instru_file_paths",
        "called_instru_origin_refs",
        "called_instru_origin_step_names",
        "called_instru_file_types",

        # run metadata
        "run_id",
        "run_number",
        "run_attempt",
        "head_sha",
        "created_at",
        "run_started_at",
        "run_updated_at",
        "status",
        "run_conclusion",
        "event",
        "head_branch",
        "html_url",
        "extracted_at_utc",

        # durable run timing
        "L1_run_started_at_effective",
        "L1_run_ended_at_effective",
        "L1_run_duration_seconds_effective",
        "L1_run_timing_source",

        # broad inventory fields
        "queue_seconds",
        "instru_detect_method",
        "instru_job_count",
        "instru_job_names",
        "instru_first_started_at",
        "instru_last_completed_at",
        "instru_window_seconds",

        # new V18 run-level auxiliary complexity fields
        "instru_distinct_job_count",
        "instru_distinct_job_base_name_count",
        "instru_matrix_like_job_count",
        "instru_matrix_expanded_flag",
        "instru_parallel_jobs_flag",
        "instru_max_parallel_jobs",

        # preferred Layer-1 fields
        "time_to_instrumentation_envelope_seconds",
        "instrumentation_job_envelope_seconds",
        "post_instrumentation_tail_seconds",
        "layer1_model",
        "layer1_proxy_quality",

        # anchor reference fields
        "anchor_job_name",
        "anchor_job_started_at",
        "anchor_job_completed_at",
        "anchor_job_source",

        # broad proxy aliases
        "pre_invocation_seconds_proxy",
        "invocation_execution_window_seconds_proxy",
        "post_invocation_seconds_proxy",

        # backward-friendly legacy aliases
        "time_to_invocation_seconds_proxy",
        "invocation_tail_seconds_proxy",
        "time_to_first_instru_seconds",
        "anchor_job_start_source",
        "time_to_first_instru_from_anchor_job_seconds",
        "time_to_first_instru_from_anchor_job_quality",
    ]

    out_style_fields = [
        "full_name",
        "workflow_identifier",
        "workflow_id",
        "workflow_path",

        "run_id",
        "run_number",
        "run_attempt",
        "created_at",
        "run_started_at",
        "run_updated_at",
        "status",
        "run_conclusion",
        "event",
        "head_branch",
        "head_sha",
        "html_url",

        "target_style",
        "styles_in_run_all",
        "multi_style_run_flag",

        "layer1_run_started_at_effective",
        "layer1_run_ended_at_effective",
        "layer1_run_duration_seconds_effective",
        "layer1_run_timing_source",

        "style_instru_job_count",
        "style_instru_job_names",
        "style_first_instru_job_name",
        "style_first_instru_job_started_at",
        "style_first_instru_job_source",
        "style_last_instru_job_name",
        "style_last_instru_job_completed_at",
        "style_last_instru_job_source",

        # new V18 auxiliary complexity fields
        "style_distinct_job_count",
        "style_distinct_job_base_name_count",
        "style_matrix_like_job_count",
        "style_matrix_expanded_flag",
        "style_parallel_same_style_flag",
        "style_max_parallel_jobs",
        "style_repeated_same_style_flag",
        "style_invocation_candidate_step_count_proxy",
        "style_distinct_invocation_step_name_count_proxy",
        "style_invocation_candidate_step_names_proxy",
        "style_same_style_complexity_class",

        # preferred Layer-1 fields
        "style_time_to_instrumentation_envelope_seconds",
        "style_instrumentation_job_envelope_seconds",
        "style_post_instrumentation_tail_seconds",
        "style_layer1_model",

        # anchor reference fields
        "style_anchor_job_name",
        "style_anchor_job_started_at",
        "style_anchor_job_completed_at",
        "style_anchor_job_source",

        # broad proxy aliases
        "style_pre_invocation_seconds_proxy",
        "style_invocation_execution_window_seconds_proxy",
        "style_post_invocation_seconds_proxy",

        # backward-friendly legacy aliases
        "style_time_to_invocation_seconds_proxy",
        "style_invocation_tail_seconds_proxy",
        "style_time_to_instru_job_start_seconds",
        "style_instru_job_envelope_seconds",
        "style_post_instru_job_tail_seconds",

        "style_job_detection_methods",
        "style_job_segmentation_confidence",
        "style_overlap_with_other_styles_flag",
        "ambiguous_instru_job_names_in_run",
        "all_instru_job_names_in_run",

        "extracted_at_utc",
    ]

    ensure_csv_header(OUT_RUN_INVENTORY_CSV, out_run_fields)
    ensure_csv_header(OUT_RUN_PER_STYLE_CSV, out_style_fields)

    existing_run_ids = load_existing_keys(OUT_RUN_INVENTORY_CSV, "run_id")
    default_branch_cache: Dict[str, str] = {}

    wf_iter = rows
    if tqdm is not None:
        wf_iter = tqdm(rows, desc="Stage2 V18: workflows -> runs")

    for wf in wf_iter:
        full_name = norm(wf.get("full_name"))
        workflow_identifier = norm(wf.get("workflow_identifier"))
        workflow_id = norm(wf.get("workflow_id"))
        workflow_path = norm(wf.get("workflow_path"))

        if not full_name:
            continue

        workflow_key_for_runs = workflow_id or workflow_identifier or workflow_path
        if not workflow_key_for_runs:
            continue

        if DEFAULT_BRANCH_ONLY:
            if full_name not in default_branch_cache:
                default_branch_cache[full_name] = get_repo_default_branch(gh, full_name)
            default_branch = default_branch_cache[full_name]
            if not default_branch:
                continue
        else:
            default_branch = ""

        branch = default_branch if DEFAULT_BRANCH_ONLY else None

        runs = list_workflow_runs(gh, full_name, workflow_key_for_runs, branch=branch) or []
        if MAX_RUNS_PER_WORKFLOW is not None:
            runs = runs[:MAX_RUNS_PER_WORKFLOW]

        declared_styles = split_styles(wf.get("styles") or wf.get("inferred_styles") or "")
        if not declared_styles:
            one_style = normalize_style_label(wf.get("inferred_style"))
            declared_styles = [one_style] if one_style in STYLE_CANONICAL else []

        anchor_step_names = parse_anchor_step_names(wf.get("test_invocation_step_names") or "")

        for run in runs:
            run_id = norm(str(run.get("id") or ""))
            if not run_id:
                continue
            if run_id in existing_run_ids:
                continue

            created_at = norm(run.get("created_at"))
            if after_dt:
                cdt = iso_to_dt(created_at)
                if cdt and cdt < after_dt:
                    continue

            head_branch = norm(run.get("head_branch"))
            if DEFAULT_BRANCH_ONLY and head_branch and head_branch != default_branch:
                continue

            jobs = list_run_jobs(gh, full_name, int(run_id)) if FETCH_JOBS_FOR_EACH_RUN else []

            run_started_at = norm(run.get("run_started_at"))
            run_updated_at = norm(run.get("updated_at"))

            run_start_eff, run_end_eff, run_dur_eff, run_timing_source = compute_run_window_from_jobs(
                jobs=jobs,
                run_started_at=run_started_at,
                run_updated_at=run_updated_at,
            )

            run_metrics = build_run_level_metrics(
                jobs=jobs,
                run_created_at=created_at,
                run_started_at=run_started_at,
                run_updated_at=run_updated_at,
                anchor_step_names=anchor_step_names,
                declared_styles=declared_styles,
            )

            append_row(OUT_RUN_INVENTORY_CSV, out_run_fields, {
                "full_name": full_name,
                "default_branch": default_branch,
                "workflow_identifier": workflow_identifier,
                "workflow_id": workflow_id,
                "workflow_path": workflow_path,

                "looks_like_instru": norm(wf.get("looks_like_instru")),
                "styles": norm(wf.get("styles")),
                "invocation_types": norm(wf.get("invocation_types")),
                "third_party_provider_name": norm(wf.get("third_party_provider_name")),
                "test_invocation_step_names": norm(wf.get("test_invocation_step_names")),
                "anchor_job_ordinal": norm(wf.get("anchor_job_ordinal")),
                "anchor_step_ordinal_in_job": norm(wf.get("anchor_step_ordinal_in_job")),
                "called_instru_signal": norm(wf.get("called_instru_signal")),
                "called_instru_file_paths": norm(wf.get("called_instru_file_paths")),
                "called_instru_origin_refs": norm(wf.get("called_instru_origin_refs")),
                "called_instru_origin_step_names": norm(wf.get("called_instru_origin_step_names")),
                "called_instru_file_types": norm(wf.get("called_instru_file_types")),

                "run_id": run_id,
                "run_number": norm(str(run.get("run_number") or "")),
                "run_attempt": norm(str(run.get("run_attempt") or "")),
                "head_sha": norm(run.get("head_sha")),
                "created_at": created_at,
                "run_started_at": run_started_at,
                "run_updated_at": run_updated_at,
                "status": norm(run.get("status")),
                "run_conclusion": norm(run.get("conclusion")),
                "event": norm(run.get("event")),
                "head_branch": head_branch,
                "html_url": norm(run.get("html_url")),
                "extracted_at_utc": now_utc_iso(),

                "L1_run_started_at_effective": run_start_eff,
                "L1_run_ended_at_effective": run_end_eff,
                "L1_run_duration_seconds_effective": "" if run_dur_eff is None else str(run_dur_eff),
                "L1_run_timing_source": run_timing_source,

                "queue_seconds": "" if run_metrics["queue_seconds"] is None else str(run_metrics["queue_seconds"]),
                "instru_detect_method": run_metrics["instru_detect_method"],
                "instru_job_count": "" if run_metrics["instru_job_count"] is None else str(run_metrics["instru_job_count"]),
                "instru_job_names": run_metrics["instru_job_names"],
                "instru_first_started_at": run_metrics["instru_first_started_at"],
                "instru_last_completed_at": run_metrics["instru_last_completed_at"],
                "instru_window_seconds": "" if run_metrics["instru_window_seconds"] is None else str(run_metrics["instru_window_seconds"]),

                "instru_distinct_job_count": "" if run_metrics["instru_distinct_job_count"] is None else str(run_metrics["instru_distinct_job_count"]),
                "instru_distinct_job_base_name_count": "" if run_metrics["instru_distinct_job_base_name_count"] is None else str(run_metrics["instru_distinct_job_base_name_count"]),
                "instru_matrix_like_job_count": "" if run_metrics["instru_matrix_like_job_count"] is None else str(run_metrics["instru_matrix_like_job_count"]),
                "instru_matrix_expanded_flag": run_metrics["instru_matrix_expanded_flag"],
                "instru_parallel_jobs_flag": run_metrics["instru_parallel_jobs_flag"],
                "instru_max_parallel_jobs": "" if run_metrics["instru_max_parallel_jobs"] is None else str(run_metrics["instru_max_parallel_jobs"]),

                "time_to_instrumentation_envelope_seconds": "" if run_metrics["time_to_instrumentation_envelope_seconds"] is None else str(run_metrics["time_to_instrumentation_envelope_seconds"]),
                "instrumentation_job_envelope_seconds": "" if run_metrics["instrumentation_job_envelope_seconds"] is None else str(run_metrics["instrumentation_job_envelope_seconds"]),
                "post_instrumentation_tail_seconds": "" if run_metrics["post_instrumentation_tail_seconds"] is None else str(run_metrics["post_instrumentation_tail_seconds"]),
                "layer1_model": run_metrics["layer1_model"],
                "layer1_proxy_quality": run_metrics["layer1_proxy_quality"],

                "anchor_job_name": run_metrics["anchor_job_name"],
                "anchor_job_started_at": run_metrics["anchor_job_started_at"],
                "anchor_job_completed_at": run_metrics["anchor_job_completed_at"],
                "anchor_job_source": run_metrics["anchor_job_source"],

                "pre_invocation_seconds_proxy": "" if run_metrics["pre_invocation_seconds_proxy"] is None else str(run_metrics["pre_invocation_seconds_proxy"]),
                "invocation_execution_window_seconds_proxy": "" if run_metrics["invocation_execution_window_seconds_proxy"] is None else str(run_metrics["invocation_execution_window_seconds_proxy"]),
                "post_invocation_seconds_proxy": "" if run_metrics["post_invocation_seconds_proxy"] is None else str(run_metrics["post_invocation_seconds_proxy"]),

                "time_to_invocation_seconds_proxy": "" if run_metrics["time_to_invocation_seconds_proxy"] is None else str(run_metrics["time_to_invocation_seconds_proxy"]),
                "invocation_tail_seconds_proxy": "" if run_metrics["invocation_tail_seconds_proxy"] is None else str(run_metrics["invocation_tail_seconds_proxy"]),
                "time_to_first_instru_seconds": "" if run_metrics["time_to_first_instru_seconds"] is None else str(run_metrics["time_to_first_instru_seconds"]),
                "anchor_job_start_source": run_metrics["anchor_job_start_source"],
                "time_to_first_instru_from_anchor_job_seconds": "" if run_metrics["time_to_first_instru_from_anchor_job_seconds"] is None else str(run_metrics["time_to_first_instru_from_anchor_job_seconds"]),
                "time_to_first_instru_from_anchor_job_quality": run_metrics["time_to_first_instru_from_anchor_job_quality"],
            })

            style_rows = build_run_per_style_rows(
                full_name=full_name,
                workflow_identifier=workflow_identifier,
                workflow_id=workflow_id,
                workflow_path=workflow_path,
                run=run,
                declared_styles=declared_styles,
                anchor_step_names=anchor_step_names,
                jobs=jobs,
                run_start_eff=run_start_eff,
                run_end_eff=run_end_eff,
                run_duration_eff=run_dur_eff,
                run_timing_source=run_timing_source,
            )
            for sr in style_rows:
                append_row(OUT_RUN_PER_STYLE_CSV, out_style_fields, sr)

            existing_run_ids.add(run_id)

        time.sleep(SLEEP_BETWEEN_WORKFLOWS_SEC)

    print("Done.")
    print("Wrote run-level inventory:", OUT_RUN_INVENTORY_CSV)
    print("Wrote run×style Layer-1 envelope inventory:", OUT_RUN_PER_STYLE_CSV)


if __name__ == "__main__":
    main()