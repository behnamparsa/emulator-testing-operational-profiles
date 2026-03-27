# ============================================================
# Stage 3 V18 — Current Study Plan Aligned + Auxiliary Complexity Support
# ADJUSTED FOR MULTI-STYLE STYLE-SCOPED LAYER-2 ANCHORING
#
# Current study alignment
# ------------------------------------------------------------
# Layer 1 (broad, carried from Stage 2)
#   Run Duration
# = Time to Instrumentation Envelope
# + Instrumentation Job Envelope
# + Post-Instrumentation Tail
#
# Layer 2 (precise, measured here from step telemetry)
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
#     = matched invocation step start
#       -> last execution-related step end in the invocation-centered path
#
#   Post-Invocation
#     = invocation execution end -> run completion
#
# Current study scope
# - Stage 2 = instrumentation-capable inventory
# - Stage 3 = instrumentation-executed subset only
# - authoritative Stage 3 gate:
#     style_instru_job_count > 0
# - study-facing styles only:
#     Community, Custom, GMD, Third-Party
# - Real-Device excluded from Stage 3 study outputs
#
# V18 additions
# ------------------------------------------------------------
# - Carries Stage 2 V18 auxiliary complexity fields into Stage 3 outputs
# - Adds Stage 3 auxiliary fields for:
#     * invocation candidate multiplicity
#     * selected invocation priority source
#     * execution-window candidate multiplicity
#     * cross-job same-style execution window visibility
#
# Multi-style adjustment in this version
# ------------------------------------------------------------
# - Invocation-step selection is style-scoped using Stage 2 style job inventory
# - Execution-related classification is style-scoped for multi-style runs
# - Workflow YAML step matching is made more robust for nested runtime job names
#   such as "caller / android" -> "android"
#
# Outputs
# - run_metrics_v16_stage3_enhanced.csv
# - run_steps_v16_stage3_breakdown.csv
# - run_per_style_v1_stage3.csv
#
# Notes
# - No TTFTS
# - No fallback timing fields
# - No legacy measured aliases
# - run_per_style explicitly includes the exact cutpoints used
#   for Layer 2 so validation can be performed directly
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

IN_STAGE2_RUN_CSV = ROOT_DIR / "run_inventory.csv"
IN_STAGE2_PER_STYLE_CSV = ROOT_DIR / "run_inventory_per_style.csv"

OUT_STAGE3A_RUNS_CSV = ROOT_DIR / "run_metrics_v16_stage3_enhanced.csv"
OUT_STAGE3B_STEPS_CSV = ROOT_DIR / "run_steps_v16_stage3_breakdown.csv"
OUT_STAGE3C_RUN_PER_STYLE_CSV = ROOT_DIR / "run_per_style_v1_stage3.csv"

MAX_TOKENS_TO_USE = 5
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
GHA_EXPR_RE = re.compile(r"\${{\s*[^}]+}}")


STYLE_CANONICAL = ["Community", "Custom", "GMD", "Third-Party"]
STYLE_ALIASES = {
    "community": "Community",
    "emu community": "Community",
    "emulator community": "Community",

    "custom": "Custom",
    "emu custom": "Custom",
    "emulator custom": "Custom",

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
    s2 = low(s)
    s2 = s2.replace("_", " ").replace("-", " ")
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2


def normalize_style_label(s: Optional[str]) -> str:
    return STYLE_ALIASES.get(canon_key(s), norm(s))


def split_styles(s: Optional[str]) -> List[str]:
    raw = norm(s)
    if not raw:
        return []
    parts = [normalize_style_label(x) for x in re.split(r"[|,;/]+", raw) if norm(x)]
    return unique_preserve([p for p in parts if p])


def first_nonempty_value(row: Dict[str, str], keys: List[str]) -> str:
    for k in keys:
        v = norm(row.get(k))
        if v:
            return v
    return ""


def first_nonempty_value_no_fallback(style_row: Dict[str, str], run_row: Dict[str, str], keys: List[str]) -> str:
    """
    Prefer style-row value. If absent, use run-row value only for the SAME field family.
    No semantic cross-field fallback.
    """
    for k in keys:
        v = norm(style_row.get(k))
        if v:
            return v
    for k in keys:
        v = norm(run_row.get(k))
        if v:
            return v
    return ""


def sanitize_gha_expr(text: str) -> str:
    return GHA_EXPR_RE.sub("", text or "")


def iso_to_dt(iso: Optional[str]) -> Optional[datetime]:
    if not iso:
        return None
    try:
        return datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
    except Exception:
        return None


def dt_to_iso_z(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.isoformat().replace("+00:00", "Z")


def dt_to_seconds(a: Optional[datetime], b: Optional[datetime]) -> Optional[int]:
    if not a or not b:
        return None
    try:
        sec = int((b - a).total_seconds())
        return sec if sec >= 0 else None
    except Exception:
        return None


def safe_int_from_str(x: Optional[str]) -> Optional[int]:
    try:
        if x is None or str(x).strip() == "":
            return None
        return int(float(str(x).strip()))
    except Exception:
        return None


def unique_preserve(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        sx = norm(str(x))
        if not sx:
            continue
        if sx not in seen:
            seen.add(sx)
            out.append(sx)
    return out


def safe_join_names(names: List[str], max_len: int = 1000) -> str:
    s = ",".join(unique_preserve(names))
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def read_env_tokens(path: Path) -> List[str]:
    toks: List[str] = []
    if not path.exists():
        return toks
    accepted_prefixes = ("GH_PAT", "GITHUB_TOKEN")
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        key = k.strip()
        if any(key == prefix or key.startswith(prefix + "_") for prefix in accepted_prefixes):
            tok = v.strip().strip('"').strip("'")
            if tok and tok not in toks:
                toks.append(tok)
    return toks[:MAX_TOKENS_TO_USE]


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        out = []
        for r in rdr:
            clean = {}
            for k, v in r.items():
                kk = (k or "").replace(BOM, "").strip()
                clean[kk] = v or ""
            out.append(clean)
        return out


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def parse_repo(repo_full_name: str) -> Tuple[str, str]:
    parts = repo_full_name.split("/")
    if len(parts) != 2:
        raise ValueError(f"Bad full_name: {repo_full_name}")
    return parts[0], parts[1]


def resolve_run_attempt(style_row: Dict[str, str], run_row: Dict[str, str]) -> str:
    return first_nonempty_value_no_fallback(style_row, run_row, ["run_attempt"])


def resolve_run_status(style_row: Dict[str, str], run_row: Dict[str, str]) -> str:
    return first_nonempty_value_no_fallback(style_row, run_row, ["status"])


def resolve_run_conclusion(style_row: Dict[str, str], run_row: Dict[str, str]) -> str:
    return first_nonempty_value_no_fallback(
        style_row,
        run_row,
        ["run_conclusion", "conclusion", "workflow_conclusion"],
    )


def resolve_event(style_row: Dict[str, str], run_row: Dict[str, str]) -> str:
    return first_nonempty_value_no_fallback(style_row, run_row, ["event"])


def resolve_trigger(style_row: Dict[str, str], run_row: Dict[str, str]) -> str:
    return first_nonempty_value_no_fallback(style_row, run_row, ["trigger"])


# =========================
# Executed-run gating
# =========================
def row_is_instru_executed(row: Dict[str, str]) -> bool:
    c = safe_int_from_str(row.get("style_instru_job_count"))
    return c is not None and c > 0


# =========================
# Stage 4 compatibility support
# =========================
def detect_runner_os_from_job(job: dict) -> str:
    labels = job.get("labels") or []
    if isinstance(labels, list):
        labels_joined = " ".join(str(x).strip() for x in labels if str(x).strip())
        if re.search(r"\bubuntu\b|\blinux\b", labels_joined, re.I):
            return "ubuntu"
        if re.search(r"\bmacos\b|\bosx\b|\bmac\b", labels_joined, re.I):
            return "macos"
        if re.search(r"\bwindows\b|\bwin(dows)?\b", labels_joined, re.I):
            return "windows"

    runner_name = norm(job.get("runner_name"))
    if re.search(r"\bubuntu\b|\blinux\b", runner_name, re.I):
        return "ubuntu"
    if re.search(r"\bmacos\b|\bosx\b|\bmac\b", runner_name, re.I):
        return "macos"
    if re.search(r"\bwindows\b|\bwin(dows)?\b", runner_name, re.I):
        return "windows"
    return ""


def stage4_compatible_run_support_fields(row: Dict[str, str], jobs: List[dict]) -> Dict[str, object]:
    out: Dict[str, object] = {}

    workflow_identifier = first_nonempty_value(row, ["workflow_identifier", "workflow_id", "workflow_path"])
    head_sha = first_nonempty_value(row, ["head_sha"])
    effective_ref = head_sha

    style_instru_job_count = first_nonempty_value(row, ["style_instru_job_count"])
    run_style_instru_job_count = first_nonempty_value(row, ["instru_job_count"])

    runner_os = first_nonempty_value(row, ["runner_os", "runs_on", "os"])
    runs_on = first_nonempty_value(row, ["runs_on"])
    os_val = first_nonempty_value(row, ["os"])
    runner_labels = first_nonempty_value(row, ["runner_labels"])

    if not runner_os:
        derived_runner_os_vals = unique_preserve([detect_runner_os_from_job(j) for j in jobs if detect_runner_os_from_job(j)])
        if len(derived_runner_os_vals) == 1:
            runner_os = derived_runner_os_vals[0]
        elif len(derived_runner_os_vals) > 1:
            runner_os = "mixed_or_unknown"

    if not runner_labels:
        all_labels: List[str] = []
        for j in jobs:
            labels = j.get("labels") or []
            if isinstance(labels, list):
                all_labels.extend([str(x).strip() for x in labels if str(x).strip()])
        runner_labels = safe_join_names(unique_preserve(all_labels), max_len=800)

    derived_job_count = len(jobs) if jobs else ""

    job_count_total = first_nonempty_value(row, ["job_count_total", "jobs_total", "total_jobs", "jobs_count"])
    if not job_count_total and derived_job_count != "":
        job_count_total = str(derived_job_count)

    out["workflow_identifier"] = workflow_identifier
    out["head_sha"] = head_sha
    out["effective_ref_for_stage4"] = effective_ref
    out["instru_job_count"] = run_style_instru_job_count or style_instru_job_count

    out["runner_os"] = runner_os
    out["runs_on"] = runs_on
    out["os"] = os_val
    out["runner_labels"] = runner_labels

    out["job_count_total"] = job_count_total
    out["jobs_total"] = first_nonempty_value(row, ["jobs_total"]) or job_count_total
    out["total_jobs"] = first_nonempty_value(row, ["total_jobs"]) or job_count_total
    out["jobs_count"] = first_nonempty_value(row, ["jobs_count"]) or job_count_total

    return out


# =========================
# GitHub client
# =========================
@dataclass
class GitHubClient:
    tokens: List[str]
    idx: int = 0

    def __post_init__(self):
        if not self.tokens:
            raise RuntimeError("No GitHub tokens found.")

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"token {self.tokens[self.idx]}",
            "Accept": "application/vnd.github+json",
            "User-Agent": "ICST2026-Stage3-V18",
        }

    def _rotate(self):
        self.idx = (self.idx + 1) % len(self.tokens)

    def get(self, url: str, stream: bool = False) -> requests.Response:
        last_exc = None
        for attempt in range(MAX_RETRIES_PER_REQUEST):
            try:
                r = requests.get(
                    url,
                    headers=self._headers(),
                    timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                    stream=stream,
                )
                if r.status_code in (403, 429):
                    self._rotate()
                    time.sleep(min(BACKOFF_CAP_S, BACKOFF_BASE_S ** attempt) + random.random())
                    continue
                if r.status_code >= 500:
                    time.sleep(min(BACKOFF_CAP_S, BACKOFF_BASE_S ** attempt) + random.random())
                    continue
                return r
            except requests.RequestException as e:
                last_exc = e
                time.sleep(min(BACKOFF_CAP_S, BACKOFF_BASE_S ** attempt) + random.random())
        if last_exc:
            raise last_exc
        raise RuntimeError(f"Failed GET: {url}")


# =========================
# YAML / API caches
# =========================
WORKFLOW_YAML_CACHE: Dict[Tuple[str, str, str], str] = {}
JOBS_CACHE: Dict[Tuple[str, str], List[dict]] = {}


def gh_contents_raw(gh: GitHubClient, owner: str, repo: str, path: str, ref: str) -> Optional[str]:
    key = (f"{owner}/{repo}", path, ref)
    if key in WORKFLOW_YAML_CACHE:
        return WORKFLOW_YAML_CACHE[key]

    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"
    r = gh.get(url)
    if r.status_code == 404:
        return None
    if r.status_code != 200:
        return None

    js = r.json()
    if not isinstance(js, dict):
        return None
    if js.get("type") != "file":
        return None

    content = js.get("content", "")
    encoding = js.get("encoding", "")
    if encoding == "base64":
        try:
            txt = base64.b64decode(content).decode("utf-8", errors="ignore")
        except Exception:
            return None
    else:
        dl = js.get("download_url")
        if not dl:
            return None
        rr = gh.get(dl)
        if rr.status_code != 200:
            return None
        txt = rr.text

    if len(WORKFLOW_YAML_CACHE) < WORKFLOW_YAML_CACHE_MAX:
        WORKFLOW_YAML_CACHE[key] = txt
    return txt


# =========================
# Stage 1 / called-file parsing helpers
# =========================
def split_multi_value_cell(s: Optional[str]) -> List[str]:
    raw = norm(s)
    if not raw:
        return []
    if "||" in raw:
        parts = [x.strip() for x in raw.split("||")]
    elif "|" in raw:
        parts = [x.strip() for x in raw.split("|")]
    elif ";" in raw:
        parts = [x.strip() for x in raw.split(";")]
    else:
        parts = [x.strip() for x in raw.split(",")]
    return [p for p in parts if p]


def parse_stage1_confirmed_called_file_paths(row: Dict[str, str]) -> List[str]:
    signal = low(row.get("called_instru_signal"))
    if signal not in {"true", "1", "yes", "y"}:
        return []
    paths = split_multi_value_cell(row.get("called_instru_file_paths"))
    cleaned: List[str] = []
    for p in paths:
        p2 = p.strip().replace("\\", "/")
        if p2.startswith("./"):
            p2 = p2[2:]
        if p2:
            cleaned.append(p2)
    return unique_preserve(cleaned)


def parse_stage1_confirmed_called_origins(row: Dict[str, str]) -> Set[str]:
    vals = split_multi_value_cell(row.get("called_instru_origin_step_names"))
    return set(low(v) for v in vals if norm(v))


# =========================
# Workflow YAML extraction
# =========================
STEP_NAME_RE = re.compile(r"^\s*-\s*name\s*:\s*(.+?)\s*$")
STEP_USES_RE = re.compile(r"^\s*uses\s*:\s*(.+?)\s*$")
STEP_RUN_RE = re.compile(r"^\s*run\s*:\s*(.*)$")
JOB_ID_RE = re.compile(r"^\s{2}([A-Za-z0-9_.-]+)\s*:\s*$")


def strip_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
        return s[1:-1]
    return s


def extract_steps_from_workflow_yaml(text: str) -> List[Dict[str, str]]:
    lines = text.splitlines()
    out: List[Dict[str, str]] = []
    current_job = ""
    i = 0
    in_steps_block = False

    while i < len(lines):
        line = lines[i]
        m_job = JOB_ID_RE.match(line)
        if m_job:
            current_job = m_job.group(1).strip()
            in_steps_block = False

        if re.match(r"^\s*steps\s*:\s*$", line):
            in_steps_block = True
            i += 1
            continue

        if in_steps_block:
            m_name = STEP_NAME_RE.match(line)
            if m_name:
                current_step_name = strip_quotes(m_name.group(1).strip())
                out.append({"job_name": current_job, "step_name": current_step_name, "uses": "", "run": ""})
                i += 1
                continue

            if out:
                m_uses = STEP_USES_RE.match(line.strip())
                if m_uses:
                    out[-1]["uses"] = strip_quotes(m_uses.group(1).strip())
                m_run = STEP_RUN_RE.match(line.strip())
                if m_run and out[-1].get("run", "") == "":
                    out[-1]["run"] = m_run.group(1)
        i += 1

    return out


# =========================
# Heuristics / classification
# =========================
_NORM_WS_RE = re.compile(r"\s+")
_NORM_PUNCT_RE = re.compile(r"[\[\]\(\)\{\}:;|]+")
_JOB_MATRIX_SUFFIX_RE = re.compile(r"\s*\([^)]*\)\s*$")


def normalize_name(s: str) -> str:
    x = (s or "").strip().strip('"').strip("'").lower()
    x = _NORM_PUNCT_RE.sub(" ", x)
    x = _NORM_WS_RE.sub(" ", x).strip()
    return x


def job_base_name(s: str) -> str:
    return _JOB_MATRIX_SUFFIX_RE.sub("", norm(s)).strip()


def runtime_job_matches_yaml_job(runtime_job_name: str, yaml_job_name: str) -> bool:
    rt = low(runtime_job_name)
    y = low(yaml_job_name)
    if not rt or not y:
        return False
    if rt == y:
        return True
    rt_parts = [p.strip() for p in rt.split("/") if p.strip()]
    if rt_parts and rt_parts[-1] == y:
        return True
    return False


def step_in_style_job_scope(step_job_name: str, style_job_names: Set[str], invocation_job_name: str = "") -> bool:
    sj = norm(step_job_name)
    if not style_job_names:
        return True

    if sj and sj in style_job_names:
        return True

    sj_base = job_base_name(sj)
    style_bases = {job_base_name(x) for x in style_job_names if norm(x)}
    if sj_base and sj_base in style_bases:
        return True

    if invocation_job_name:
        ij = norm(invocation_job_name)
        if sj and ij and (sj == ij or job_base_name(sj) == job_base_name(ij)):
            return True

    return False


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


ANDROIDISH_RE = re.compile(
    r"(adb|am instrument|avd|emulator|uiautomator|espresso|androidtest|connectedcheck|connectedandroidtest|manageddevice|gmd|"
    r"detox|baseline profile|baselineprofile|macrobenchmark|instrumentation|integration[\s_-]*test)",
    re.I,
)

THIRD_PARTY_PROVIDER_RE = re.compile(
    r"(browserstack|bstack|sauce\s*labs|saucelabs|kobiton|headspin|bitbar|perfecto|lambdatest|genymotion\s*cloud|firebase\s*test\s*lab|gcloud\s+firebase|emulator\.wtf|maestro\s+cloud)",
    re.I,
)

THIRD_PARTY_INVOKE_RE = re.compile(
    r"(firebase\s+test\s+android\s+run|gcloud\s+firebase\s+test\s+android\s+run|flank\s+android\s+run|"
    r"appcenter\s+test\s+run|saucectl\s+(run|test)|maestro\s+cloud|detox\s+test|browserstack)",
    re.I,
)

SETUP_HINT_RE = re.compile(
    r"(checkout|setup-java|setup java|setup-jdk|setup android|sdkmanager|cache|restore cache|install dependencies|npm ci|yarn install|bundle install|gradle dependencies)",
    re.I,
)

PROVISION_HINT_RE = re.compile(
    r"(create avd|avdmanager|start emulator|launch emulator|boot emulator|wait[- ]for[- ]device|provision|device farm|test lab|browserstack local|sauce connect)",
    re.I,
)

ARTIFACT_HINT_RE = re.compile(
    r"(upload-artifact|download-artifact|artifact|test-results|results|report|reports|junit|coverage|logs?)",
    re.I,
)

CLEANUP_HINT_RE = re.compile(
    r"(cleanup|tear\s*down|teardown|stop emulator|kill emulator|shutdown emulator|remove avd|delete avd|close session|stop session)",
    re.I,
)

TEST_HINT_RE = re.compile(
    r"(connectedcheck|connectedandroidtest|androidtest|am instrument|instrumentation|manageddevice|gmd|"
    r"detox|baselineprofile|baseline profile|macrobenchmark|uiautomator|espresso|integration[\s_-]*test|firebase\s+test\s+android\s+run|gcloud\s+firebase)",
    re.I,
)

FILE_HINT_INSTRU_RE = re.compile(
    r"(androidtest|connectedcheck|connectedandroidtest|detox|integration[\s_-]*test|managed[\s_-]*device|gmd|baseline[\s_-]*profile|macrobenchmark|espresso|uiautomator|instrumentation)",
    re.I,
)

CUSTOM_SCRIPT_HINT_RE = re.compile(
    r"(./gradlew|gradlew|python|bash|sh |pwsh|powershell|node |npm |yarn |ruby |bundle exec)",
    re.I,
)

INVOCATION_START_STRONG_RE = re.compile(
    r"(adb.*am instrument|am instrument|connectedcheck|connectedandroidtest|androidtest|"
    r"run flutter integration tests|run android emulator|integration[\s_-]*tests?|"
    r"detox\s+test|instrumentation|manageddevice|gmd|baseline[\s_-]*profile|"
    r"macrobenchmark|uiautomator|espresso|firebase\s+test\s+android\s+run|"
    r"gcloud\s+firebase\s+test\s+android\s+run)",
    re.I,
)

INVOCATION_WRAPPER_RE = re.compile(
    r"(cache|restore cache|save cache|avd cache|download artifacts?|download artifact|"
    r"prepare test apk|prepare apk|prepare|setup ssh|wait[- ]for[- ]device|"
    r"boot emulator|start emulator|launch emulator|create avd|provision)",
    re.I,
)

POST_PHASE_HINT_RE = re.compile(
    r"(^post\b|post |complete job|upload artifacts?|download artifacts?|upload|download|artifact|report|reports|teardown|tear down|cleanup|close session)",
    re.I,
)

JOB_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")

JOB_PATH_GENERIC_TOKENS = {
    "call",
    "run",
    "test",
    "tests",
    "job",
    "jobs",
    "setup",
    "build",
    "debug",
    "release",
    "ci",
    "workflow",
    "workflows",
    "min",
    "max",
}


def infer_flags_from_step(step_name: str, uses: str, run_cmd: str, target_style: str = "") -> Dict[str, Union[bool, str]]:
    sname = norm(step_name)
    suse = norm(uses)
    srun = sanitize_gha_expr(norm(run_cmd))
    combo = " | ".join([sname, suse, srun])
    style = normalize_style_label(target_style)

    flags: Dict[str, Union[bool, str]] = {
        "stage1_anchor_match": False,
        "stage1_anchor_match_reason": "",

        "explicit_instru": False,
        "explicit_instru_reason": "",

        "setup": False,
        "provision": False,
        "artifact_report": False,
        "cleanup_teardown": False,

        "third_party_provider": False,
        "third_party_provider_name": "",
        "third_party_invoke": False,

        "gmd": False,
        "community": False,
        "custom": False,

        "custom_followed_file_instru": False,
        "custom_stage1_supported_exec": False,
    }

    if SETUP_HINT_RE.search(combo):
        flags["setup"] = True
    if PROVISION_HINT_RE.search(combo):
        flags["provision"] = True
    if ARTIFACT_HINT_RE.search(combo):
        flags["artifact_report"] = True
    if CLEANUP_HINT_RE.search(combo):
        flags["cleanup_teardown"] = True

    if THIRD_PARTY_PROVIDER_RE.search(combo):
        flags["third_party_provider"] = True
        m = THIRD_PARTY_PROVIDER_RE.search(combo)
        flags["third_party_provider_name"] = m.group(1) if m else ""
    if THIRD_PARTY_INVOKE_RE.search(combo):
        flags["third_party_invoke"] = True

    if re.search(r"(manageddevice|gmd|gradle managed device)", combo, re.I):
        flags["gmd"] = True
    if re.search(r"(android-emulator-runner|emulator runner|create avd|start emulator|connectedcheck|connectedandroidtest)", combo, re.I):
        flags["community"] = True
    if re.search(r"(detox|integration[\s_-]*test|baseline profile|baselineprofile|macrobenchmark|uiautomator|espresso|instrumentation)", combo, re.I):
        flags["custom"] = True

    if TEST_HINT_RE.search(combo) or ANDROIDISH_RE.search(combo):
        flags["explicit_instru"] = True
        flags["explicit_instru_reason"] = "androidish_or_test_hint"

    if style == "Third-Party":
        if bool(flags.get("third_party_provider")) or bool(flags.get("third_party_invoke")):
            flags["explicit_instru"] = True
            if not flags["explicit_instru_reason"]:
                flags["explicit_instru_reason"] = "third_party"
    elif style == "GMD":
        if bool(flags.get("gmd")):
            flags["explicit_instru"] = True
            if not flags["explicit_instru_reason"]:
                flags["explicit_instru_reason"] = "gmd"
    elif style == "Community":
        if bool(flags.get("community")):
            flags["explicit_instru"] = True
            if not flags["explicit_instru_reason"]:
                flags["explicit_instru_reason"] = "community"
    elif style == "Custom":
        if bool(flags.get("custom")):
            flags["explicit_instru"] = True
            if not flags["explicit_instru_reason"]:
                flags["explicit_instru_reason"] = "custom"

    return flags


def build_step_combo(step_name: str, uses: str, run_cmd: str) -> str:
    return " | ".join([
        norm(step_name),
        norm(uses),
        sanitize_gha_expr(norm(run_cmd)),
    ])


def is_strong_invocation_candidate(
    flags: Dict[str, Union[bool, str]],
    step_name: str,
    uses: str,
    run_cmd: str,
) -> bool:
    combo = build_step_combo(step_name, uses, run_cmd)

    if bool(flags.get("stage1_anchor_match")):
        return True

    if bool(flags.get("custom_stage1_supported_exec")):
        return True

    if INVOCATION_START_STRONG_RE.search(combo):
        return True

    if INVOCATION_WRAPPER_RE.search(combo):
        return False

    if bool(flags.get("explicit_instru")) and not (
        bool(flags.get("setup"))
        or bool(flags.get("provision"))
        or bool(flags.get("artifact_report"))
        or bool(flags.get("cleanup_teardown"))
    ):
        return True

    return False


def tokenize_job_for_path(job_name: str) -> List[str]:
    toks: List[str] = []
    for tok in JOB_TOKEN_RE.findall(low(job_name)):
        if not tok or tok.isdigit():
            continue
        if tok in JOB_PATH_GENERIC_TOKENS:
            continue
        toks.append(tok)
    return unique_preserve(toks)


def job_path_overlap_count(job_a: str, job_b: str) -> int:
    a = set(tokenize_job_for_path(job_a))
    b = set(tokenize_job_for_path(job_b))
    if not a or not b:
        return 0
    return len(a & b)


def execution_candidate_path_linked(step_job_name: str, invocation_job_name: str) -> bool:
    sj = norm(step_job_name)
    ij = norm(invocation_job_name)
    if not sj or not ij:
        return False

    if sj == ij or job_base_name(sj) == job_base_name(ij):
        return True

    return job_path_overlap_count(sj, ij) >= 1


def normalize_platform_value(s: str) -> str:
    x = low(s)
    if not x:
        return ""

    # Prefer target/test family first
    if "android" in x:
        return "android"
    if "ios" in x:
        return "ios"
    if "web" in x:
        return "web"

    # Fallback host runner family
    if "ubuntu" in x or "linux" in x:
        return "linux"
    if "windows" in x or re.search(r"\bwin\b", x):
        return "windows"
    if "macos" in x or "osx" in x or re.search(r"\bmac\b", x):
        return "macos"

    return x


def infer_platform_from_job_name(job_name: str) -> str:
    j = low(job_name)
    if not j:
        return ""

    # Target platform first
    if "android" in j:
        return "android"
    if "ios" in j:
        return "ios"
    if "web" in j:
        return "web"

    # Host fallback only if target family is absent
    if "windows" in j or re.search(r"\bwin\b", j):
        return "windows"
    if "macos" in j or "osx" in j or re.search(r"\bmac\b", j):
        return "macos"
    if "linux" in j or "ubuntu" in j:
        return "linux"

    return ""


def get_step_platform(step_row: Dict[str, object]) -> str:
    """
    For Layer 2 continuity, prefer the TEST TARGET family
    (android / ios / web) over the host runner family.
    """
    job_platform = infer_platform_from_job_name(str(step_row.get("job_name", "")))
    if job_platform:
        return job_platform

    runner_os = normalize_platform_value(str(step_row.get("runner_os", "")))
    if runner_os:
        return runner_os

    runner_name = normalize_platform_value(str(step_row.get("runner_name", "")))
    if runner_name:
        return runner_name

    runner_labels = normalize_platform_value(str(step_row.get("runner_labels", "")))
    if runner_labels:
        return runner_labels

    return ""


def same_platform_as_invocation(step_row: Dict[str, object], invocation_platform: str) -> bool:
    if not invocation_platform:
        return False
    return get_step_platform(step_row) == invocation_platform


def mark_stage1_anchor_matches(
    steps: List[Dict[str, object]],
    stage1_anchor_names: List[str],
    called_origin_step_names: Set[str],
) -> None:
    for st in steps:
        step_low = low(str(st.get("step_name", "")))
        if stage1_anchor_names and anchored_step_match(str(st.get("step_name", "")), stage1_anchor_names):
            st["stage1_anchor_match"] = True
            st["stage1_anchor_match_reason"] = "stage1_test_invocation_step_names"
        elif step_low and step_low in called_origin_step_names:
            st["stage1_anchor_match"] = True
            st["stage1_anchor_match_reason"] = "matched_stage1_called_origin"
        else:
            st["stage1_anchor_match"] = False
            st["stage1_anchor_match_reason"] = ""


def enrich_with_called_file_support(
    gh: GitHubClient,
    owner: str,
    repo: str,
    ref: str,
    steps: List[Dict[str, object]],
    confirmed_called_paths: List[str],
    called_origin_step_names: Set[str],
    target_style: str,
) -> None:
    if not confirmed_called_paths:
        return

    file_evidence_map: Dict[str, bool] = {}
    for p in confirmed_called_paths:
        txt = gh_contents_raw(gh, owner, repo, p, ref)
        if not txt:
            continue
        file_evidence_map[p] = bool(FILE_HINT_INSTRU_RE.search(txt))

    if not file_evidence_map:
        return

    for st in steps:
        step_low = low(str(st.get("step_name", "")))
        uses_low = low(str(st.get("uses", "")))
        run_low = low(str(st.get("run", "")))
        combo = " | ".join([step_low, uses_low, run_low])

        matched_origin = step_low in called_origin_step_names if step_low else False
        matched_local_ref = any(p in combo for p in file_evidence_map.keys())

        if matched_origin or matched_local_ref:
            if any(file_evidence_map.values()):
                st["custom_followed_file_instru"] = True
                if normalize_style_label(target_style) == "Custom":
                    if CUSTOM_SCRIPT_HINT_RE.search(combo) or FILE_HINT_INSTRU_RE.search(combo):
                        st["custom_stage1_supported_exec"] = True


# =========================
# Jobs / steps API
# =========================
def list_jobs_for_run(gh: GitHubClient, owner: str, repo: str, run_id: str) -> List[dict]:
    cache_key = (f"{owner}/{repo}", run_id)
    if cache_key in JOBS_CACHE:
        return JOBS_CACHE[cache_key]

    jobs: List[dict] = []
    page = 1
    while page <= MAX_PAGES_PER_LIST:
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}/jobs?per_page=100&page={page}"
        r = gh.get(url)
        if r.status_code != 200:
            break
        js = r.json() if r.text else {}
        arr = js.get("jobs", [])
        if not arr:
            break
        jobs.extend(arr)
        if len(arr) < 100:
            break
        page += 1

    JOBS_CACHE[cache_key] = jobs
    return jobs


def step_rows_from_jobs(jobs: List[dict]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for job_idx, job in enumerate(jobs, start=1):
        jname = norm(job.get("name"))
        job_id = str(job.get("id") or "")
        job_url = norm(job.get("url"))
        job_html_url = norm(job.get("html_url"))
        runner_name = norm(job.get("runner_name"))
        runner_os = detect_runner_os_from_job(job)

        runner_labels = ""
        if isinstance(job.get("labels"), list):
            runner_labels = ",".join(str(x).strip() for x in job.get("labels", []) if str(x).strip())

        job_started_at = norm(job.get("started_at"))
        job_completed_at = norm(job.get("completed_at"))

        for step_idx, st in enumerate(job.get("steps") or [], start=1):
            started = st.get("started_at") or ""
            completed = st.get("completed_at") or ""
            sdt = iso_to_dt(started)
            edt = iso_to_dt(completed)
            dur = dt_to_seconds(sdt, edt)

            rows.append({
                "job_name": jname,
                "job_id": job_id,
                "job_url": job_url,
                "job_html_url": job_html_url,
                "job_started_at": job_started_at,
                "job_completed_at": job_completed_at,
                "job_ordinal_in_run": str(job_idx),
                "step_ordinal_in_job": str(step_idx),
                "runner_os": runner_os,
                "runner_labels": runner_labels,
                "runner_name": runner_name,
                "step_name": norm(st.get("name")),
                "status": norm(st.get("status")),
                "conclusion": norm(st.get("conclusion")),
                "started_at": started,
                "completed_at": completed,
                "duration_seconds": dur,
                "uses": "",
                "run": "",
            })
    return rows


# =========================
# Layer 2 classification
# =========================
def is_execution_related(
    flags: Dict[str, Union[bool, str]],
    target_style: str,
    step_job_name: str = "",
    style_job_names: Optional[Set[str]] = None,
    invocation_job_name: str = "",
) -> bool:
    style_job_names = style_job_names or set()

    if not step_in_style_job_scope(step_job_name, style_job_names, invocation_job_name):
        return False

    if bool(flags.get("stage1_anchor_match")):
        return True
    if bool(flags.get("explicit_instru")):
        return True
    if normalize_style_label(target_style) == "Custom" and bool(flags.get("custom_stage1_supported_exec")):
        return True
    return False


def classify_step_activity_group(flags: Dict[str, Union[bool, str]]) -> str:
    if bool(flags.get("artifact_report")):
        return "Artifact/Report"
    if bool(flags.get("cleanup_teardown")):
        return "Cleanup/Teardown"
    if bool(flags.get("setup")):
        return "Setup"
    if bool(flags.get("provision")):
        return "Provision"
    if bool(flags.get("explicit_instru")) or bool(flags.get("stage1_anchor_match")) or bool(flags.get("custom_stage1_supported_exec")):
        return "Test"
    return "Other"


def classify_execution_role(
    flags: Dict[str, Union[bool, str]],
    target_style: str,
    step_job_name: str = "",
    style_job_names: Optional[Set[str]] = None,
    invocation_job_name: str = "",
) -> str:
    return (
        "Execution-related"
        if is_execution_related(
            flags,
            target_style,
            step_job_name=step_job_name,
            style_job_names=style_job_names,
            invocation_job_name=invocation_job_name,
        )
        else "Non-execution overhead"
    )


def classify_overhead_phase(
    step_start: Optional[datetime],
    invocation_start: Optional[datetime],
    execution_window_end: Optional[datetime],
) -> str:
    if not step_start or not invocation_start:
        return "Pre-test overhead"
    if step_start < invocation_start:
        return "Pre-test overhead"
    if execution_window_end and step_start <= execution_window_end:
        return "Active test"
    return "Post-test overhead"


def infer_phase_metadata(
    step_row: Dict[str, object],
    flags: Dict[str, Union[bool, str]],
    step_activity_group: str,
    invocation_job_name: str,
    invocation_start_dt: Optional[datetime],
) -> Dict[str, object]:
    step_name = str(step_row.get("step_name", ""))
    uses = str(step_row.get("uses", ""))
    run_cmd = str(step_row.get("run", ""))
    job_name = str(step_row.get("job_name", ""))
    step_start = iso_to_dt(str(step_row.get("started_at", "")))

    combo = build_step_combo(step_name, uses, run_cmd)

    path_linked = False
    if invocation_job_name:
        path_linked = execution_candidate_path_linked(job_name, invocation_job_name)
    else:
        path_linked = bool(flags.get("stage1_anchor_match")) or bool(flags.get("explicit_instru")) or bool(flags.get("custom_stage1_supported_exec"))

    if not path_linked:
        phase_guess = "outside_selected_path"
        phase_reason = "not_selected_path"
        phase_score = 0
    elif bool(flags.get("artifact_report")) or bool(flags.get("cleanup_teardown")) or POST_PHASE_HINT_RE.search(combo):
        if bool(flags.get("artifact_report")) and bool(flags.get("cleanup_teardown")):
            phase_reason = "artifact+cleanup"
        elif bool(flags.get("artifact_report")):
            phase_reason = "artifact"
        elif bool(flags.get("cleanup_teardown")):
            phase_reason = "cleanup_teardown"
        else:
            phase_reason = "post_hint"
        phase_guess = "post_invocation"
        phase_score = 4
    elif bool(flags.get("stage1_anchor_match")):
        phase_guess = "invocation_execution"
        phase_reason = "stage1_anchor_match"
        phase_score = 5
    elif bool(flags.get("custom_stage1_supported_exec")):
        phase_guess = "invocation_execution"
        phase_reason = "custom_supported_exec"
        phase_score = 5
    elif bool(flags.get("explicit_instru")) and not (
        bool(flags.get("artifact_report")) or bool(flags.get("cleanup_teardown"))
    ):
        phase_guess = "invocation_execution"
        phase_reason = "explicit_instru"
        phase_score = 4
    elif bool(flags.get("setup")) or bool(flags.get("provision")) or INVOCATION_WRAPPER_RE.search(combo):
        if bool(flags.get("setup")) and bool(flags.get("provision")):
            phase_reason = "setup+provision"
        elif bool(flags.get("setup")):
            phase_reason = "setup"
        elif bool(flags.get("provision")):
            phase_reason = "provision"
        else:
            phase_reason = "wrapper_hint"
        phase_guess = "pre_invocation"
        phase_score = 3
    elif invocation_start_dt is not None and step_start is not None and step_start < invocation_start_dt:
        phase_guess = "pre_invocation"
        phase_reason = "before_invocation_start"
        phase_score = 2
    else:
        phase_guess = "pre_invocation"
        phase_reason = "path_scoped_fallback"
        phase_score = 1

    if phase_guess == "outside_selected_path":
        phase_group_consistency = "outside_selected_path"
    elif phase_guess == "pre_invocation":
        if step_activity_group in {"Setup", "Provision"}:
            phase_group_consistency = "consistent"
        elif step_activity_group in {"Other", "Test"}:
            phase_group_consistency = "soft_conflict"
        else:
            phase_group_consistency = "hard_conflict"
    elif phase_guess == "invocation_execution":
        if step_activity_group == "Test":
            phase_group_consistency = "consistent"
        elif step_activity_group in {"Setup", "Provision", "Other"}:
            phase_group_consistency = "soft_conflict"
        else:
            phase_group_consistency = "hard_conflict"
    else:  # post_invocation
        if step_activity_group in {"Artifact/Report", "Cleanup/Teardown", "Other"}:
            phase_group_consistency = "consistent"
        elif step_activity_group == "Test":
            phase_group_consistency = "soft_conflict"
        else:
            phase_group_consistency = "hard_conflict"

    return {
        "phase_guess": phase_guess,
        "phase_reason": phase_reason,
        "phase_score": phase_score,
        "phase_group_consistency": phase_group_consistency,
        "path_linked_to_selected_invocation": "true" if path_linked else "false",
    }


# =========================
# Layer 2 precise boundaries
# =========================
def parse_style_job_names(style_row: Dict[str, str]) -> Set[str]:
    names = split_multi_value_cell(style_row.get("style_instru_job_names"))
    return set(norm(x) for x in names if norm(x))


def make_cutpoint_record(
    step_row: Dict[str, object],
    source_type: str,
) -> Dict[str, object]:
    return {
        "step_name": str(step_row.get("step_name", "")),
        "job_name": str(step_row.get("job_name", "")),
        "source": source_type,
        "step_started_at": str(step_row.get("started_at", "")),
        "step_completed_at": str(step_row.get("completed_at", "")),
        "job_ordinal_in_run": str(step_row.get("job_ordinal_in_run", "")),
        "step_ordinal_in_job": str(step_row.get("step_ordinal_in_job", "")),
    }


def pick_measured_invocation_step(
    merged_steps: List[Dict[str, object]],
    target_style: str,
    style_job_names: Set[str],
) -> Tuple[Optional[datetime], Optional[datetime], Dict[str, object], Dict[str, Union[bool, str]]]:
    candidates_stage1: List[Tuple[datetime, datetime, Dict[str, object], Dict[str, Union[bool, str]]]] = []
    candidates_explicit_strong: List[Tuple[datetime, datetime, Dict[str, object], Dict[str, Union[bool, str]]]] = []
    candidates_explicit_weak: List[Tuple[datetime, datetime, Dict[str, object], Dict[str, Union[bool, str]]]] = []
    candidates_custom_supported: List[Tuple[datetime, datetime, Dict[str, object], Dict[str, Union[bool, str]]]] = []

    candidate_pool = [
        st for st in merged_steps
        if step_in_style_job_scope(str(st.get("job_name", "")), style_job_names)
    ]
    if not style_job_names:
        candidate_pool = merged_steps

    for st in candidate_pool:
        st_start = iso_to_dt(str(st.get("started_at", "")))
        st_end = iso_to_dt(str(st.get("completed_at", "")))
        if not st_start:
            continue

        step_name = str(st.get("step_name", ""))
        uses = str(st.get("uses", ""))
        run_cmd = str(st.get("run", ""))

        flags = infer_flags_from_step(
            step_name=step_name,
            uses=uses,
            run_cmd=run_cmd,
            target_style=target_style,
        )

        if bool(st.get("stage1_anchor_match")):
            flags["stage1_anchor_match"] = True
            flags["stage1_anchor_match_reason"] = st.get("stage1_anchor_match_reason", "")
        if bool(st.get("custom_followed_file_instru")):
            flags["custom_followed_file_instru"] = True
        if bool(st.get("custom_stage1_supported_exec")):
            flags["custom_stage1_supported_exec"] = True

        cutpoint = make_cutpoint_record(st, "")
        rec = (
            st_start,
            st_end if st_end else st_start,
            cutpoint,
            flags,
        )

        if bool(flags.get("stage1_anchor_match")):
            cutpoint["source"] = "stage1_anchor_match"
            candidates_stage1.append(rec)
            continue

        if normalize_style_label(target_style) == "Custom" and bool(flags.get("custom_stage1_supported_exec")):
            cutpoint["source"] = "stage1_supported_custom_exec"
            candidates_custom_supported.append(rec)
            continue

        if bool(flags.get("explicit_instru")):
            if is_strong_invocation_candidate(flags, step_name, uses, run_cmd):
                cutpoint["source"] = "explicit_instru_execution_start"
                candidates_explicit_strong.append(rec)
            else:
                cutpoint["source"] = "explicit_instru_step_fallback"
                candidates_explicit_weak.append(rec)

    if candidates_stage1:
        candidates_stage1.sort(key=lambda x: x[0])
        sdt, edt, cutpoint, flags = candidates_stage1[0]
        return sdt, edt, cutpoint, flags

    if candidates_explicit_strong:
        candidates_explicit_strong.sort(key=lambda x: x[0])
        sdt, edt, cutpoint, flags = candidates_explicit_strong[0]
        return sdt, edt, cutpoint, flags

    if candidates_custom_supported:
        candidates_custom_supported.sort(key=lambda x: x[0])
        sdt, edt, cutpoint, flags = candidates_custom_supported[0]
        return sdt, edt, cutpoint, flags

    if candidates_explicit_weak:
        candidates_explicit_weak.sort(key=lambda x: x[0])
        sdt, edt, cutpoint, flags = candidates_explicit_weak[0]
        return sdt, edt, cutpoint, flags

    return None, None, {
        "step_name": "",
        "job_name": "",
        "source": "missing",
        "step_started_at": "",
        "step_completed_at": "",
        "job_ordinal_in_run": "",
        "step_ordinal_in_job": "",
    }, {}


def is_execution_window_candidate(
    flags: Dict[str, Union[bool, str]],
    target_style: str,
    step_job_name: str,
    invocation_job_name: str,
    style_job_names: Set[str],
) -> bool:
    if not is_execution_related(
        flags=flags,
        target_style=target_style,
        step_job_name=step_job_name,
        style_job_names=style_job_names,
        invocation_job_name=invocation_job_name,
    ):
        return False

    style = normalize_style_label(target_style)
    same_job = bool(invocation_job_name) and (
        norm(step_job_name) == norm(invocation_job_name)
        or job_base_name(step_job_name) == job_base_name(invocation_job_name)
    )
    in_style_job = step_in_style_job_scope(step_job_name, style_job_names, invocation_job_name)

    if same_job:
        return True
    if in_style_job:
        return True
    if style == "Third-Party" and (bool(flags.get("third_party_provider")) or bool(flags.get("third_party_invoke"))):
        return True
    if style == "Custom" and bool(flags.get("custom_stage1_supported_exec")):
        return True

    return False


def collect_execution_window_candidates(
    merged_steps: List[Dict[str, object]],
    target_style: str,
    invocation_start_dt: Optional[datetime],
    invocation_job_name: str,
    style_job_names: Set[str],
    invocation_platform: str = "",
) -> List[Dict[str, object]]:
    if invocation_start_dt is None:
        return []

    candidates: List[Dict[str, object]] = []

    for st in merged_steps:
        st_start = iso_to_dt(str(st.get("started_at", "")))
        st_end = iso_to_dt(str(st.get("completed_at", "")))
        if not st_start or not st_end:
            continue
        if st_start < invocation_start_dt:
            continue

        step_name = str(st.get("step_name", ""))
        job_name = str(st.get("job_name", ""))

        flags = infer_flags_from_step(
            step_name=step_name,
            uses=str(st.get("uses", "")),
            run_cmd=str(st.get("run", "")),
            target_style=target_style,
        )
        if bool(st.get("stage1_anchor_match")):
            flags["stage1_anchor_match"] = True
            flags["stage1_anchor_match_reason"] = st.get("stage1_anchor_match_reason", "")
        if bool(st.get("custom_followed_file_instru")):
            flags["custom_followed_file_instru"] = True
        if bool(st.get("custom_stage1_supported_exec")):
            flags["custom_stage1_supported_exec"] = True

        if not is_execution_window_candidate(
            flags=flags,
            target_style=target_style,
            step_job_name=job_name,
            invocation_job_name=invocation_job_name,
            style_job_names=style_job_names,
        ):
            continue

        step_activity_group = classify_step_activity_group(flags)
        phase_meta = infer_phase_metadata(
            step_row=st,
            flags=flags,
            step_activity_group=step_activity_group,
            invocation_job_name=invocation_job_name,
            invocation_start_dt=invocation_start_dt,
        )

        if phase_meta["phase_guess"] in {"post_invocation", "outside_selected_path"}:
            continue

        path_linked = execution_candidate_path_linked(job_name, invocation_job_name)
        same_platform = same_platform_as_invocation(st, invocation_platform)

        cutpoint = make_cutpoint_record(
            st,
            "last_execution_related_step_path_linked_same_platform"
            if (path_linked and same_platform)
            else (
                "last_execution_related_step_path_linked"
                if path_linked
                else "last_execution_related_step_broad_scope"
            ),
        )

        candidates.append({
            "end_dt": st_end,
            "job_name": job_name,
            "path_linked": path_linked,
            "same_platform": same_platform,
            "platform": get_step_platform(st),
            "phase_guess": phase_meta["phase_guess"],
            "phase_score": int(phase_meta["phase_score"]),
            "phase_group_consistency": phase_meta["phase_group_consistency"],
            "cutpoint": cutpoint,
        })

    return candidates


def select_preferred_execution_window_candidates(
    candidates: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    if not candidates:
        return []

    same_platform_and_path_exec = [
        c for c in candidates
        if bool(c.get("same_platform")) and bool(c.get("path_linked")) and c.get("phase_guess") == "invocation_execution"
    ]
    if same_platform_and_path_exec:
        return same_platform_and_path_exec

    same_platform_exec = [
        c for c in candidates
        if bool(c.get("same_platform")) and c.get("phase_guess") == "invocation_execution"
    ]
    if same_platform_exec:
        return same_platform_exec

    path_linked_exec = [
        c for c in candidates
        if bool(c.get("path_linked")) and c.get("phase_guess") == "invocation_execution"
    ]
    if path_linked_exec:
        return path_linked_exec

    same_platform_and_path = [
        c for c in candidates
        if bool(c.get("same_platform")) and bool(c.get("path_linked"))
    ]
    if same_platform_and_path:
        return same_platform_and_path

    same_platform_only = [
        c for c in candidates
        if bool(c.get("same_platform"))
    ]
    if same_platform_only:
        return same_platform_only

    path_linked_only = [
        c for c in candidates
        if bool(c.get("path_linked"))
    ]
    if path_linked_only:
        return path_linked_only

    return candidates


def pick_invocation_execution_end(
    merged_steps: List[Dict[str, object]],
    target_style: str,
    invocation_start_dt: Optional[datetime],
    invocation_end_dt: Optional[datetime],
    invocation_job_name: str,
    style_job_names: Set[str],
) -> Tuple[Optional[datetime], Dict[str, object]]:
    if invocation_start_dt is None:
        return None, {
            "step_name": "",
            "job_name": "",
            "source": "missing",
            "step_started_at": "",
            "step_completed_at": "",
            "job_ordinal_in_run": "",
            "step_ordinal_in_job": "",
        }

    invocation_platform = ""
    for st in merged_steps:
        if (
            norm(str(st.get("job_name", ""))) == norm(invocation_job_name)
            or job_base_name(str(st.get("job_name", ""))) == job_base_name(invocation_job_name)
        ):
            invocation_platform = get_step_platform(st)
            if invocation_platform:
                break

    all_candidates = collect_execution_window_candidates(
        merged_steps=merged_steps,
        target_style=target_style,
        invocation_start_dt=invocation_start_dt,
        invocation_job_name=invocation_job_name,
        style_job_names=style_job_names,
        invocation_platform=invocation_platform,
    )
    preferred_candidates = select_preferred_execution_window_candidates(all_candidates)

    if preferred_candidates:
        preferred_candidates.sort(key=lambda x: (x["phase_score"], x["end_dt"]))
        chosen = preferred_candidates[-1]
        return chosen["end_dt"], chosen["cutpoint"]

    if invocation_end_dt is not None:
        return invocation_end_dt, {
            "step_name": "",
            "job_name": invocation_job_name,
            "source": "invocation_step_terminal",
            "step_started_at": "",
            "step_completed_at": dt_to_iso_z(invocation_end_dt),
            "job_ordinal_in_run": "",
            "step_ordinal_in_job": "",
        }

    return None, {
        "step_name": "",
        "job_name": "",
        "source": "missing",
        "step_started_at": "",
        "step_completed_at": "",
        "job_ordinal_in_run": "",
        "step_ordinal_in_job": "",
    }


# =========================
# V18 auxiliary summary helpers
# =========================
def summarize_invocation_candidates(
    merged_steps: List[Dict[str, object]],
    target_style: str,
    style_job_names: Set[str],
) -> Dict[str, object]:
    stage1_candidates: List[Dict[str, object]] = []
    explicit_candidates: List[Dict[str, object]] = []
    custom_candidates: List[Dict[str, object]] = []

    candidate_pool = [
        st for st in merged_steps
        if step_in_style_job_scope(str(st.get("job_name", "")), style_job_names)
    ]
    if not style_job_names:
        candidate_pool = merged_steps

    for st in candidate_pool:
        st_start = iso_to_dt(str(st.get("started_at", "")))
        if not st_start:
            continue

        step_name = str(st.get("step_name", ""))
        uses = str(st.get("uses", ""))
        run_cmd = str(st.get("run", ""))

        flags = infer_flags_from_step(
            step_name=step_name,
            uses=uses,
            run_cmd=run_cmd,
            target_style=target_style,
        )
        if bool(st.get("stage1_anchor_match")):
            flags["stage1_anchor_match"] = True
            flags["stage1_anchor_match_reason"] = st.get("stage1_anchor_match_reason", "")
        if bool(st.get("custom_followed_file_instru")):
            flags["custom_followed_file_instru"] = True
        if bool(st.get("custom_stage1_supported_exec")):
            flags["custom_stage1_supported_exec"] = True

        rec = {
            "job_name": str(st.get("job_name", "")),
            "step_name": str(st.get("step_name", "")),
            "started_at": str(st.get("started_at", "")),
        }

        if bool(flags.get("stage1_anchor_match")):
            stage1_candidates.append(rec)
        elif normalize_style_label(target_style) == "Custom" and bool(flags.get("custom_stage1_supported_exec")):
            custom_candidates.append(rec)
        elif bool(flags.get("explicit_instru")):
            explicit_candidates.append(rec)

    all_candidates = stage1_candidates + explicit_candidates + custom_candidates
    distinct_step_names = unique_preserve([str(x["step_name"]) for x in all_candidates if norm(str(x["step_name"]))])
    distinct_jobs = unique_preserve([str(x["job_name"]) for x in all_candidates if norm(str(x["job_name"]))])

    return {
        "invocation_candidate_count_total": len(all_candidates),
        "stage1_anchor_candidate_count": len(stage1_candidates),
        "explicit_instru_candidate_count": len(explicit_candidates),
        "custom_supported_candidate_count": len(custom_candidates),
        "distinct_invocation_candidate_step_name_count": len(distinct_step_names),
        "distinct_invocation_candidate_job_count": len(distinct_jobs),
        "invocation_candidate_step_names": safe_join_names(distinct_step_names),
        "invocation_candidate_job_names": safe_join_names(distinct_jobs),
    }


def summarize_execution_window_candidates(
    merged_steps: List[Dict[str, object]],
    target_style: str,
    invocation_start_dt: Optional[datetime],
    invocation_job_name: str,
    style_job_names: Set[str],
) -> Dict[str, object]:
    if invocation_start_dt is None:
        return {
            "execution_window_candidate_count": 0,
            "execution_window_distinct_job_count": 0,
            "execution_window_candidate_job_names": "",
            "cross_job_execution_window_flag": "false",
        }

    invocation_platform = ""
    for st in merged_steps:
        if (
            norm(str(st.get("job_name", ""))) == norm(invocation_job_name)
            or job_base_name(str(st.get("job_name", ""))) == job_base_name(invocation_job_name)
        ):
            invocation_platform = get_step_platform(st)
            if invocation_platform:
                break

    all_candidates = collect_execution_window_candidates(
        merged_steps=merged_steps,
        target_style=target_style,
        invocation_start_dt=invocation_start_dt,
        invocation_job_name=invocation_job_name,
        style_job_names=style_job_names,
        invocation_platform=invocation_platform,
    )
    preferred_candidates = select_preferred_execution_window_candidates(all_candidates)

    cand_jobs = unique_preserve([
        str(c.get("job_name", ""))
        for c in preferred_candidates
        if norm(str(c.get("job_name", "")))
    ])
    cross_job = "true" if len(cand_jobs) >= 2 else "false"

    return {
        "execution_window_candidate_count": len(preferred_candidates),
        "execution_window_distinct_job_count": len(cand_jobs),
        "execution_window_candidate_job_names": safe_join_names(cand_jobs),
        "cross_job_execution_window_flag": cross_job,
    }


# =========================
# Aggregation helpers
# =========================
def init_duration_buckets() -> Dict[str, int]:
    return {
        "Setup": 0,
        "Provision": 0,
        "Test": 0,
        "Artifact/Report": 0,
        "Cleanup/Teardown": 0,
        "Other": 0,
        "Execution-related": 0,
        "Non-execution overhead": 0,
        "Pre-test overhead": 0,
        "Active test": 0,
        "Post-test overhead": 0,
    }


def init_count_buckets() -> Dict[str, int]:
    return {
        "Setup": 0,
        "Provision": 0,
        "Test": 0,
        "Artifact/Report": 0,
        "Cleanup/Teardown": 0,
        "Other": 0,
        "Execution-related": 0,
        "Non-execution overhead": 0,
        "Pre-test overhead": 0,
        "Active test": 0,
        "Post-test overhead": 0,
    }


# =========================
# Metrics keys
# =========================
STYLE_METRIC_KEYS = [
    "layer2_measurement_mode",
    "layer2_measurement_quality",

    "run_boundary_start_at",
    "run_boundary_end_at",

    "matched_invocation_step_name",
    "matched_invocation_job_name",
    "matched_invocation_source",
    "matched_invocation_step_started_at",
    "matched_invocation_step_completed_at",
    "matched_invocation_job_ordinal_in_run",
    "matched_invocation_step_ordinal_in_job",

    "invocation_execution_end_step_name",
    "invocation_execution_end_job_name",
    "invocation_execution_end_source",
    "invocation_execution_end_step_started_at",
    "invocation_execution_end_step_completed_at",
    "invocation_execution_end_job_ordinal_in_run",
    "invocation_execution_end_step_ordinal_in_job",

    "invocation_execution_window_started_at",
    "invocation_execution_window_ended_at",
    "pre_invocation_seconds",
    "invocation_execution_window_seconds",
    "post_invocation_seconds",

    "setup_sum_seconds",
    "provision_sum_seconds",
    "test_sum_seconds",
    "artifact_report_sum_seconds",
    "cleanup_teardown_sum_seconds",
    "other_sum_seconds",

    "execution_related_sum_seconds",
    "non_execution_overhead_sum_seconds",

    "pre_test_overhead_sum_seconds",
    "active_test_sum_seconds",
    "post_test_overhead_sum_seconds",

    "setup_step_count",
    "provision_step_count",
    "test_step_count",
    "artifact_report_step_count",
    "cleanup_teardown_step_count",
    "other_step_count",

    "execution_related_step_count",
    "non_execution_overhead_step_count",

    "pre_test_overhead_step_count",
    "active_test_step_count",
    "post_test_overhead_step_count",

    # V18 new Stage 3 auxiliary fields
    "invocation_candidate_count_total",
    "stage1_anchor_candidate_count",
    "explicit_instru_candidate_count",
    "custom_supported_candidate_count",
    "distinct_invocation_candidate_step_name_count",
    "distinct_invocation_candidate_job_count",
    "invocation_candidate_step_names",
    "invocation_candidate_job_names",
    "selected_invocation_priority_source",
    "execution_window_candidate_count",
    "execution_window_distinct_job_count",
    "execution_window_candidate_job_names",
    "cross_job_execution_window_flag",

    # V18 carried Stage 2 style auxiliary fields
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
]


# =========================
# Stage 3 core builder
# =========================
def build_stage3_outputs_for_style(
    gh: GitHubClient,
    run_row: Dict[str, str],
    style_row: Dict[str, str],
) -> Tuple[Dict[str, object], List[Dict[str, object]], Dict[str, object]]:
    full_name = norm(style_row.get("full_name") or run_row.get("full_name"))
    workflow_path = norm(style_row.get("workflow_path") or run_row.get("workflow_path"))
    workflow_ref = first_nonempty_value(style_row, ["head_sha"]) or first_nonempty_value(run_row, ["head_sha"])
    run_id = norm(style_row.get("run_id"))
    target_style = normalize_style_label(style_row.get("target_style"))

    run_attempt = resolve_run_attempt(style_row, run_row)
    run_status = resolve_run_status(style_row, run_row)
    run_conclusion = resolve_run_conclusion(style_row, run_row)
    event = resolve_event(style_row, run_row)
    trigger = resolve_trigger(style_row, run_row)

    run_started_at = iso_to_dt(
        first_nonempty_value(style_row, ["layer1_run_started_at_effective", "run_started_at"])
        or first_nonempty_value(run_row, ["L1_run_started_at_effective", "run_started_at"])
    )
    run_ended_at = iso_to_dt(
        first_nonempty_value(style_row, ["layer1_run_ended_at_effective", "run_updated_at"])
        or first_nonempty_value(run_row, ["L1_run_ended_at_effective", "run_updated_at"])
    )

    owner, repo = parse_repo(full_name)
    jobs = list_jobs_for_run(gh, owner, repo, run_id)
    step_rows = step_rows_from_jobs(jobs)

    workflow_yaml = ""
    if FETCH_WORKFLOW_YAML and workflow_path and workflow_ref:
        workflow_yaml = gh_contents_raw(gh, owner, repo, workflow_path, workflow_ref) or ""

    yaml_steps = extract_steps_from_workflow_yaml(workflow_yaml) if workflow_yaml else []

    stage1_anchor_names = parse_anchor_step_names(first_nonempty_value(run_row, ["test_invocation_step_names"]))
    called_paths = parse_stage1_confirmed_called_file_paths(run_row)
    called_origin_step_names = parse_stage1_confirmed_called_origins(run_row)

    yaml_by_stepname: Dict[str, List[Dict[str, str]]] = {}
    for ys in yaml_steps:
        yaml_by_stepname.setdefault(low(ys.get("step_name")), []).append(ys)

    merged_steps: List[Dict[str, object]] = []
    for sr in step_rows:
        rt_job_raw = str(sr["job_name"])
        rt_step = low(str(sr["step_name"]))
        matched_yaml = None

        for ys in yaml_steps:
            if runtime_job_matches_yaml_job(rt_job_raw, ys.get("job_name", "")) and low(ys.get("step_name")) == rt_step:
                matched_yaml = ys
                break

        if matched_yaml is None:
            cands = yaml_by_stepname.get(rt_step, [])
            matched_yaml = cands[0] if cands else {"job_name": sr["job_name"], "step_name": sr["step_name"], "uses": "", "run": ""}

        merged = dict(sr)
        merged["uses"] = matched_yaml.get("uses", "")
        merged["run"] = matched_yaml.get("run", "")
        merged["stage1_anchor_match"] = False
        merged["stage1_anchor_match_reason"] = ""
        merged["custom_followed_file_instru"] = False
        merged["custom_stage1_supported_exec"] = False
        merged_steps.append(merged)

    if called_paths:
        enrich_with_called_file_support(
            gh=gh,
            owner=owner,
            repo=repo,
            ref=workflow_ref,
            steps=merged_steps,
            confirmed_called_paths=called_paths,
            called_origin_step_names=called_origin_step_names,
            target_style=target_style,
        )

    mark_stage1_anchor_matches(
        steps=merged_steps,
        stage1_anchor_names=stage1_anchor_names,
        called_origin_step_names=called_origin_step_names,
    )

    style_job_names = parse_style_job_names(style_row)

    invocation_start_dt, invocation_step_end_dt, invocation_cutpoint, _inv_flags = pick_measured_invocation_step(
        merged_steps=merged_steps,
        target_style=target_style,
        style_job_names=style_job_names,
    )

    execution_end_dt, execution_end_cutpoint = pick_invocation_execution_end(
        merged_steps=merged_steps,
        target_style=target_style,
        invocation_start_dt=invocation_start_dt,
        invocation_end_dt=invocation_step_end_dt,
        invocation_job_name=str(invocation_cutpoint.get("job_name", "")),
        style_job_names=style_job_names,
    )

    pre_invocation_seconds = dt_to_seconds(run_started_at, invocation_start_dt)
    invocation_execution_window_seconds = dt_to_seconds(invocation_start_dt, execution_end_dt)
    post_invocation_seconds = dt_to_seconds(execution_end_dt, run_ended_at)

    if invocation_start_dt and execution_end_dt:
        layer2_mode = "measured_step_based"
        layer2_quality = "direct_step_plus_last_execution_related_step"
    elif invocation_start_dt:
        layer2_mode = "partial_step_based"
        layer2_quality = "direct_step_missing_execution_end"
    else:
        layer2_mode = "missing"
        layer2_quality = "missing"

    candidate_summary = summarize_invocation_candidates(
        merged_steps=merged_steps,
        target_style=target_style,
        style_job_names=style_job_names,
    )
    execution_window_summary = summarize_execution_window_candidates(
        merged_steps=merged_steps,
        target_style=target_style,
        invocation_start_dt=invocation_start_dt,
        invocation_job_name=str(invocation_cutpoint.get("job_name", "")),
        style_job_names=style_job_names,
    )

    selected_invocation_priority_source = str(invocation_cutpoint.get("source", "missing"))
    selected_invocation_job_name = str(invocation_cutpoint.get("job_name", ""))

    duration_buckets = init_duration_buckets()
    count_buckets = init_count_buckets()

    step_breakdown_rows: List[Dict[str, object]] = []

    selected_inv_step = (
        normalize_name(str(invocation_cutpoint.get("step_name", ""))),
        normalize_name(str(invocation_cutpoint.get("job_name", ""))),
        norm(str(invocation_cutpoint.get("step_started_at", ""))),
    )
    selected_end_step = (
        normalize_name(str(execution_end_cutpoint.get("step_name", ""))),
        normalize_name(str(execution_end_cutpoint.get("job_name", ""))),
        norm(str(execution_end_cutpoint.get("step_completed_at", ""))),
    )

    for st in merged_steps:
        flags = infer_flags_from_step(
            step_name=str(st.get("step_name", "")),
            uses=str(st.get("uses", "")),
            run_cmd=str(st.get("run", "")),
            target_style=target_style,
        )

        if bool(st.get("stage1_anchor_match")):
            flags["stage1_anchor_match"] = True
            flags["stage1_anchor_match_reason"] = st.get("stage1_anchor_match_reason", "")
        if bool(st.get("custom_followed_file_instru")):
            flags["custom_followed_file_instru"] = True
        if bool(st.get("custom_stage1_supported_exec")):
            flags["custom_stage1_supported_exec"] = True

        st_start = iso_to_dt(str(st.get("started_at", "")))
        st_dur = st.get("duration_seconds")
        st_dur_i = st_dur if isinstance(st_dur, int) else safe_int_from_str(str(st_dur))

        step_activity_group = classify_step_activity_group(flags)
        execution_role = classify_execution_role(
            flags,
            target_style,
            step_job_name=str(st.get("job_name", "")),
            style_job_names=style_job_names,
            invocation_job_name=selected_invocation_job_name,
        )
        overhead_phase = classify_overhead_phase(
            step_start=st_start,
            invocation_start=invocation_start_dt,
            execution_window_end=execution_end_dt,
        )

        phase_meta = infer_phase_metadata(
            step_row=st,
            flags=flags,
            step_activity_group=step_activity_group,
            invocation_job_name=selected_invocation_job_name,
            invocation_start_dt=invocation_start_dt,
        )

        inside_execution_window = (
            "true"
            if (st_start is not None and invocation_start_dt is not None and execution_end_dt is not None and invocation_start_dt <= st_start <= execution_end_dt)
            else "false"
        )

        if st_dur_i is not None:
            duration_buckets[step_activity_group] += st_dur_i
            duration_buckets[execution_role] += st_dur_i
            duration_buckets[overhead_phase] += st_dur_i

        count_buckets[step_activity_group] += 1
        count_buckets[execution_role] += 1
        count_buckets[overhead_phase] += 1

        is_selected_invocation_cutpoint = (
            normalize_name(str(st.get("step_name", ""))) == selected_inv_step[0]
            and normalize_name(str(st.get("job_name", ""))) == selected_inv_step[1]
            and norm(str(st.get("started_at", ""))) == selected_inv_step[2]
        )

        is_selected_execution_end_cutpoint = (
            normalize_name(str(st.get("step_name", ""))) == selected_end_step[0]
            and normalize_name(str(st.get("job_name", ""))) == selected_end_step[1]
            and norm(str(st.get("completed_at", ""))) == selected_end_step[2]
        )

        step_breakdown_rows.append({
            "full_name": full_name,
            "workflow_path": workflow_path,
            "workflow_ref": workflow_ref,
            "run_id": run_id,
            "run_attempt": run_attempt,
            "status": run_status,
            "run_conclusion": run_conclusion,
            "event": event,
            "trigger": trigger,
            "target_style": target_style,

            "job_id": st.get("job_id", ""),
            "job_url": st.get("job_url", ""),
            "job_html_url": st.get("job_html_url", ""),
            "job_ordinal_in_run": st.get("job_ordinal_in_run", ""),
            "step_ordinal_in_job": st.get("step_ordinal_in_job", ""),
            "runner_os": st.get("runner_os", ""),
            "runner_labels": st.get("runner_labels", ""),
            "runner_name": st.get("runner_name", ""),
            "job_name": st.get("job_name", ""),
            "step_name": st.get("step_name", ""),
            "status_step": st.get("status", ""),
            "conclusion_step": st.get("conclusion", ""),
            "started_at": st.get("started_at", ""),
            "completed_at": st.get("completed_at", ""),
            "duration_seconds": st_dur_i if st_dur_i is not None else "",
            "uses": st.get("uses", ""),
            "run": st.get("run", ""),

            "step_activity_group": step_activity_group,
            "execution_role": execution_role,
            "overhead_phase": overhead_phase,
            "phase_guess": phase_meta["phase_guess"],
            "phase_reason": phase_meta["phase_reason"],
            "phase_score": phase_meta["phase_score"],
            "phase_group_consistency": phase_meta["phase_group_consistency"],
            "path_linked_to_selected_invocation": phase_meta["path_linked_to_selected_invocation"],
            "inside_invocation_execution_window": inside_execution_window,

            "selected_invocation_cutpoint": "true" if is_selected_invocation_cutpoint else "false",
            "selected_execution_end_cutpoint": "true" if is_selected_execution_end_cutpoint else "false",

            "stage1_anchor_match": str(bool(flags.get("stage1_anchor_match"))).lower(),
            "stage1_anchor_match_reason": flags.get("stage1_anchor_match_reason", ""),
            "explicit_instru": str(bool(flags.get("explicit_instru"))).lower(),
            "explicit_instru_reason": flags.get("explicit_instru_reason", ""),
            "custom_followed_file_instru": str(bool(flags.get("custom_followed_file_instru"))).lower(),
            "custom_stage1_supported_exec": str(bool(flags.get("custom_stage1_supported_exec"))).lower(),

            "setup_flag": str(bool(flags.get("setup"))).lower(),
            "provision_flag": str(bool(flags.get("provision"))).lower(),
            "artifact_report_flag": str(bool(flags.get("artifact_report"))).lower(),
            "cleanup_teardown_flag": str(bool(flags.get("cleanup_teardown"))).lower(),
            "third_party_provider_flag": str(bool(flags.get("third_party_provider"))).lower(),
            "third_party_provider_name": flags.get("third_party_provider_name", ""),
        })

    per_style_row: Dict[str, object] = {
        "full_name": full_name,
        "workflow_path": workflow_path,
        "workflow_ref": workflow_ref,
        "run_id": run_id,
        "run_attempt": run_attempt,
        "status": run_status,
        "run_conclusion": run_conclusion,
        "event": event,
        "trigger": trigger,
        "target_style": target_style,
        "inferred_styles_all": safe_join_names([s for s in split_styles(first_nonempty_value(run_row, ["styles"])) if s in STYLE_CANONICAL]),

        "layer2_measurement_mode": layer2_mode,
        "layer2_measurement_quality": layer2_quality,

        "run_boundary_start_at": dt_to_iso_z(run_started_at),
        "run_boundary_end_at": dt_to_iso_z(run_ended_at),

        "matched_invocation_step_name": invocation_cutpoint["step_name"],
        "matched_invocation_job_name": invocation_cutpoint["job_name"],
        "matched_invocation_source": invocation_cutpoint["source"],
        "matched_invocation_step_started_at": invocation_cutpoint["step_started_at"],
        "matched_invocation_step_completed_at": invocation_cutpoint["step_completed_at"],
        "matched_invocation_job_ordinal_in_run": invocation_cutpoint["job_ordinal_in_run"],
        "matched_invocation_step_ordinal_in_job": invocation_cutpoint["step_ordinal_in_job"],

        "invocation_execution_end_step_name": execution_end_cutpoint["step_name"],
        "invocation_execution_end_job_name": execution_end_cutpoint["job_name"],
        "invocation_execution_end_source": execution_end_cutpoint["source"],
        "invocation_execution_end_step_started_at": execution_end_cutpoint["step_started_at"],
        "invocation_execution_end_step_completed_at": execution_end_cutpoint["step_completed_at"],
        "invocation_execution_end_job_ordinal_in_run": execution_end_cutpoint["job_ordinal_in_run"],
        "invocation_execution_end_step_ordinal_in_job": execution_end_cutpoint["step_ordinal_in_job"],

        "invocation_execution_window_started_at": dt_to_iso_z(invocation_start_dt),
        "invocation_execution_window_ended_at": dt_to_iso_z(execution_end_dt),
        "pre_invocation_seconds": "" if pre_invocation_seconds is None else str(pre_invocation_seconds),
        "invocation_execution_window_seconds": "" if invocation_execution_window_seconds is None else str(invocation_execution_window_seconds),
        "post_invocation_seconds": "" if post_invocation_seconds is None else str(post_invocation_seconds),

        "setup_sum_seconds": str(duration_buckets["Setup"]),
        "provision_sum_seconds": str(duration_buckets["Provision"]),
        "test_sum_seconds": str(duration_buckets["Test"]),
        "artifact_report_sum_seconds": str(duration_buckets["Artifact/Report"]),
        "cleanup_teardown_sum_seconds": str(duration_buckets["Cleanup/Teardown"]),
        "other_sum_seconds": str(duration_buckets["Other"]),

        "execution_related_sum_seconds": str(duration_buckets["Execution-related"]),
        "non_execution_overhead_sum_seconds": str(duration_buckets["Non-execution overhead"]),

        "pre_test_overhead_sum_seconds": str(duration_buckets["Pre-test overhead"]),
        "active_test_sum_seconds": str(duration_buckets["Active test"]),
        "post_test_overhead_sum_seconds": str(duration_buckets["Post-test overhead"]),

        "setup_step_count": str(count_buckets["Setup"]),
        "provision_step_count": str(count_buckets["Provision"]),
        "test_step_count": str(count_buckets["Test"]),
        "artifact_report_step_count": str(count_buckets["Artifact/Report"]),
        "cleanup_teardown_step_count": str(count_buckets["Cleanup/Teardown"]),
        "other_step_count": str(count_buckets["Other"]),

        "execution_related_step_count": str(count_buckets["Execution-related"]),
        "non_execution_overhead_step_count": str(count_buckets["Non-execution overhead"]),

        "pre_test_overhead_step_count": str(count_buckets["Pre-test overhead"]),
        "active_test_step_count": str(count_buckets["Active test"]),
        "post_test_overhead_step_count": str(count_buckets["Post-test overhead"]),

        # V18 new Stage 3 auxiliary fields
        "invocation_candidate_count_total": str(candidate_summary["invocation_candidate_count_total"]),
        "stage1_anchor_candidate_count": str(candidate_summary["stage1_anchor_candidate_count"]),
        "explicit_instru_candidate_count": str(candidate_summary["explicit_instru_candidate_count"]),
        "custom_supported_candidate_count": str(candidate_summary["custom_supported_candidate_count"]),
        "distinct_invocation_candidate_step_name_count": str(candidate_summary["distinct_invocation_candidate_step_name_count"]),
        "distinct_invocation_candidate_job_count": str(candidate_summary["distinct_invocation_candidate_job_count"]),
        "invocation_candidate_step_names": str(candidate_summary["invocation_candidate_step_names"]),
        "invocation_candidate_job_names": str(candidate_summary["invocation_candidate_job_names"]),
        "selected_invocation_priority_source": selected_invocation_priority_source,
        "execution_window_candidate_count": str(execution_window_summary["execution_window_candidate_count"]),
        "execution_window_distinct_job_count": str(execution_window_summary["execution_window_distinct_job_count"]),
        "execution_window_candidate_job_names": str(execution_window_summary["execution_window_candidate_job_names"]),
        "cross_job_execution_window_flag": str(execution_window_summary["cross_job_execution_window_flag"]),

        # carried Stage 2 V18 style auxiliary fields
        "style_distinct_job_count": first_nonempty_value(style_row, ["style_distinct_job_count"]),
        "style_distinct_job_base_name_count": first_nonempty_value(style_row, ["style_distinct_job_base_name_count"]),
        "style_matrix_like_job_count": first_nonempty_value(style_row, ["style_matrix_like_job_count"]),
        "style_matrix_expanded_flag": first_nonempty_value(style_row, ["style_matrix_expanded_flag"]),
        "style_parallel_same_style_flag": first_nonempty_value(style_row, ["style_parallel_same_style_flag"]),
        "style_max_parallel_jobs": first_nonempty_value(style_row, ["style_max_parallel_jobs"]),
        "style_repeated_same_style_flag": first_nonempty_value(style_row, ["style_repeated_same_style_flag"]),
        "style_invocation_candidate_step_count_proxy": first_nonempty_value(style_row, ["style_invocation_candidate_step_count_proxy"]),
        "style_distinct_invocation_step_name_count_proxy": first_nonempty_value(style_row, ["style_distinct_invocation_step_name_count_proxy"]),
        "style_invocation_candidate_step_names_proxy": first_nonempty_value(style_row, ["style_invocation_candidate_step_names_proxy"]),
        "style_same_style_complexity_class": first_nonempty_value(style_row, ["style_same_style_complexity_class"]),
    }

    run_support_fields = stage4_compatible_run_support_fields(row=run_row, jobs=jobs)

    # Carry these as raw fields only, no semantic fallback.
    run_support_fields["run_attempt"] = run_attempt
    run_support_fields["status"] = run_status
    run_support_fields["run_conclusion"] = run_conclusion
    run_support_fields["event"] = event
    run_support_fields["trigger"] = trigger

    # Carry Stage 2 V18 run-level auxiliary fields into run-level Stage 3 output
    for k in [
        "instru_distinct_job_count",
        "instru_distinct_job_base_name_count",
        "instru_matrix_like_job_count",
        "instru_matrix_expanded_flag",
        "instru_parallel_jobs_flag",
        "instru_max_parallel_jobs",
    ]:
        run_support_fields[k] = first_nonempty_value(run_row, [k])

    return per_style_row, step_breakdown_rows, run_support_fields


# =========================
# CSV fields
# =========================
BASE_RUN_KEEP_FIELDS = [
    "full_name",
    "default_branch",
    "workflow_path",
    "workflow_id",
    "workflow_identifier",
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
    "trigger",
    "head_branch",
    "html_url",
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
    "instru_job_count",
    "runner_os",
    "runs_on",
    "os",
    "runner_labels",
    "job_count_total",
    "jobs_total",
    "total_jobs",
    "jobs_count",

    # Stage 2 V18 run-level auxiliary fields
    "instru_distinct_job_count",
    "instru_distinct_job_base_name_count",
    "instru_matrix_like_job_count",
    "instru_matrix_expanded_flag",
    "instru_parallel_jobs_flag",
    "instru_max_parallel_jobs",
]

out_fieldnames_3a = BASE_RUN_KEEP_FIELDS + [
    "styles_count_stage3",
    "styles_stage3",
]
for sty, pref in [
    ("Community", "community"),
    ("Custom", "custom"),
    ("GMD", "gmd"),
    ("Third-Party", "third_party"),
]:
    for k in STYLE_METRIC_KEYS:
        out_fieldnames_3a.append(f"{pref}_{k}")

out_fieldnames_3a += [
    "stage3_extracted_at_utc",
    "stage3_error",
]

out_fieldnames_3b = [
    "full_name", "workflow_path", "workflow_ref", "run_id",
    "run_attempt", "status", "run_conclusion", "event", "trigger",
    "target_style",
    "job_id", "job_url", "job_html_url", "job_ordinal_in_run", "step_ordinal_in_job",
    "runner_os", "runner_labels", "runner_name",
    "job_name", "step_name", "status_step", "conclusion_step",
    "started_at", "completed_at", "duration_seconds",
    "uses", "run",
    "step_activity_group", "execution_role", "overhead_phase",
    "phase_guess", "phase_reason", "phase_score", "phase_group_consistency",
    "path_linked_to_selected_invocation",
    "inside_invocation_execution_window",
    "selected_invocation_cutpoint", "selected_execution_end_cutpoint",
    "stage1_anchor_match", "stage1_anchor_match_reason",
    "explicit_instru", "explicit_instru_reason",
    "custom_followed_file_instru", "custom_stage1_supported_exec",
    "setup_flag", "provision_flag", "artifact_report_flag", "cleanup_teardown_flag",
    "third_party_provider_flag", "third_party_provider_name",
    "stage3_extracted_at_utc",
]

per_style_fields = [
    "full_name",
    "workflow_path",
    "workflow_ref",
    "run_id",
    "run_attempt",
    "status",
    "run_conclusion",
    "event",
    "trigger",
    "target_style",
    "inferred_styles_all",
] + STYLE_METRIC_KEYS + ["stage3_extracted_at_utc"]


# =========================
# Main
# =========================
def main():
    try:
        tokens = load_github_tokens(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
        print(f"Loaded GitHub token pool size: {len(tokens)}")
    except Exception:
        tokens = read_env_tokens(TOKENS_ENV_PATH)
    gh = GitHubClient(tokens=tokens)

    run_rows = read_csv_rows(IN_STAGE2_RUN_CSV)
    style_rows = read_csv_rows(IN_STAGE2_PER_STYLE_CSV)

    total_rows_before_filter = len(style_rows)

    if PROCESS_ONLY_RELEVANT_ROWS:
        style_rows = [
            r for r in style_rows
            if norm(r.get("run_id"))
            and normalize_style_label(r.get("target_style")) in STYLE_CANONICAL
        ]

    total_rows_after_runid_filter = len(style_rows)

    style_rows = [r for r in style_rows if row_is_instru_executed(r)]

    total_rows_after_exec_filter = len(style_rows)

    if total_rows_after_exec_filter == 0:
        raise RuntimeError(
            "Stage 3 executed-run filter removed all rows. "
            "Check that run_inventory_per_style.csv contains style_instru_job_count and that it is populated correctly."
        )

    run_index: Dict[str, Dict[str, str]] = {}
    for r in run_rows:
        rid = norm(r.get("run_id"))
        if rid:
            run_index[rid] = r

    all_step_rows: List[Dict[str, object]] = []
    all_per_style_rows: List[Dict[str, object]] = []
    run_level_rows: List[Dict[str, object]] = []
    extracted_ts = now_utc_iso()

    style_to_pref = {
        "Community": "community",
        "Custom": "custom",
        "GMD": "gmd",
        "Third-Party": "third_party",
    }

    iterator = tqdm(style_rows, desc="Stage3 V18") if tqdm else style_rows

    current_run_id = None
    current_run_out: Optional[Dict[str, object]] = None
    current_seen_styles: List[str] = []

    def flush_current_run():
        nonlocal current_run_out, current_seen_styles
        if current_run_out is None:
            return
        current_run_out["styles_count_stage3"] = len(unique_preserve(current_seen_styles))
        current_run_out["styles_stage3"] = safe_join_names(current_seen_styles)
        current_run_out["stage3_extracted_at_utc"] = extracted_ts
        current_run_out["stage3_error"] = current_run_out.get("stage3_error", "")
        run_level_rows.append(current_run_out)
        current_run_out = None
        current_seen_styles = []

    for style_row in iterator:
        run_id = norm(style_row.get("run_id"))
        run_row = run_index.get(run_id, {})

        try:
            per_style_row, step_rows, run_support_fields = build_stage3_outputs_for_style(
                gh=gh,
                run_row=run_row,
                style_row=style_row,
            )
            per_style_row["stage3_extracted_at_utc"] = extracted_ts
            all_per_style_rows.append(per_style_row)

            for sr in step_rows:
                sr["stage3_extracted_at_utc"] = extracted_ts
                all_step_rows.append(sr)

            if current_run_id != run_id:
                flush_current_run()
                current_run_id = run_id

                base_run = dict(run_row)
                base_run.update(run_support_fields)

                for sty, pref in style_to_pref.items():
                    for k in STYLE_METRIC_KEYS:
                        base_run[f"{pref}_{k}"] = ""
                base_run["stage3_error"] = ""
                current_run_out = base_run

            current_seen_styles.append(str(per_style_row["target_style"]))

            pref = style_to_pref[str(per_style_row["target_style"])]
            for k in STYLE_METRIC_KEYS:
                current_run_out[f"{pref}_{k}"] = per_style_row.get(k, "")

        except Exception as e:
            if current_run_id != run_id:
                flush_current_run()
                current_run_id = run_id

                base_run = dict(run_row) if run_row else dict(style_row)
                base_run.update(stage4_compatible_run_support_fields(row=base_run, jobs=[]))

                # raw carry only, no semantic fallback
                base_run["run_attempt"] = resolve_run_attempt(style_row, run_row)
                base_run["status"] = resolve_run_status(style_row, run_row)
                base_run["run_conclusion"] = resolve_run_conclusion(style_row, run_row)
                base_run["event"] = resolve_event(style_row, run_row)
                base_run["trigger"] = resolve_trigger(style_row, run_row)

                for k in [
                    "instru_distinct_job_count",
                    "instru_distinct_job_base_name_count",
                    "instru_matrix_like_job_count",
                    "instru_matrix_expanded_flag",
                    "instru_parallel_jobs_flag",
                    "instru_max_parallel_jobs",
                ]:
                    base_run[k] = first_nonempty_value(run_row, [k]) or first_nonempty_value(style_row, [k])

                for sty, pref in style_to_pref.items():
                    for k in STYLE_METRIC_KEYS:
                        base_run[f"{pref}_{k}"] = ""
                base_run["stage3_error"] = str(e)
                current_run_out = base_run
                current_seen_styles = []

    flush_current_run()

    write_csv(OUT_STAGE3B_STEPS_CSV, out_fieldnames_3b, all_step_rows)
    write_csv(OUT_STAGE3A_RUNS_CSV, out_fieldnames_3a, run_level_rows)
    write_csv(OUT_STAGE3C_RUN_PER_STYLE_CSV, per_style_fields, all_per_style_rows)

    print("[info] Stage 3 input rows before any filter:", total_rows_before_filter)
    print("[info] Stage 3 rows after run_id/style filter:", total_rows_after_runid_filter)
    print("[info] Stage 3 rows after executed-run filter (style_instru_job_count > 0):", total_rows_after_exec_filter)

    print("[done] Run metrics:", OUT_STAGE3A_RUNS_CSV)
    print("[done] Step breakdown:", OUT_STAGE3B_STEPS_CSV)
    print("[done] Run x style:", OUT_STAGE3C_RUN_PER_STYLE_CSV)


if __name__ == "__main__":
    main()