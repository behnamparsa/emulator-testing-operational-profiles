# ============================================================
# Stage 3 (FULLY ADJUSTED: canonical style names + stronger 3P alignment
#            + Stage1-confirmed called-file recovery only)
#
# Main adjustments in this version
# 1) Preserves existing output file names / locations
# 2) Preserves TTFTS logic and provenance behavior
# 3) Preserves run-level output and run×style output
# 4) Keeps:
#      - instru_duration_seconds = FULL instrumentation-path window
#      - core_instru_window_seconds / instru_exec_window_seconds = CORE execution span only
# 5) Uses canonical style names everywhere:
#      - Community
#      - Custom
#      - GMD
#      - Third-Party
#      - Real-Devices
# 6) Fixes multi-style segmentation by normalizing inferred style names
#    before comparing against declared styles
# 7) Strengthens Third-Party detection to align better with Stage 1
# 8) Keeps style-aware instrumentation end selection
# 9) IMPORTANT CHANGE:
#      - Stage 3 no longer blindly re-follows local files for every candidate step
#      - It now uses Stage 1 / Stage 2 pass-through fields:
#           called_instru_signal
#           called_instru_file_paths
#           called_instru_origin_refs
#           called_instru_origin_step_names
#           called_instru_file_types
#      - Called-file recovery is attempted ONLY when Stage 1 already confirmed
#        instrumentation evidence in followed files
# 10) Keeps Option B only for Custom-capable workflows:
#      - guarded Stage-1-supported fallback to rescue Custom wrapper anchor/execution detection
#
# 11) ONLY NEW CHANGE IN THIS REVISION:
#      - Custom no longer uses the same-job constrained instrumentation-end rule
#      - Custom now uses the broader continuation logic like Third-Party
#      - Community / GMD / Real-Devices remain same-job constrained
# ============================================================

from __future__ import annotations

import base64
import csv
import random
import re
import time
import sys
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

MAX_TOKENS_TO_USE = 10
PROCESS_ONLY_RELEVANT_ROWS = True

CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 60
MAX_RETRIES_PER_REQUEST = 8
BACKOFF_BASE_S = 1.7
BACKOFF_CAP_S = 60
MAX_PAGES_PER_LIST = 2000

FETCH_WORKFLOW_YAML = True
WORKFLOW_YAML_CACHE_MAX = 7000

# targeted Stage1-confirmed followed-file fetch only
MAX_FOLLOW_BYTES_STAGE3 = 1_500_000

# =========================
# Helpers
# =========================
BOM = "\ufeff"
GHA_EXPR_RE = re.compile(r"\${{\s*[^}]+}}")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    s = ",".join(unique_preserve([n for n in names if n]))
    return s[:max_len]


def safe_int_from_str(x: Optional[str]) -> Optional[int]:
    try:
        if x is None or str(x).strip() == "":
            return None
        return int(float(str(x).strip()))
    except Exception:
        return None


def norm(s: Optional[str]) -> str:
    return (s or "").strip()


def low(s: Optional[str]) -> str:
    return norm(s).lower()


def canon_key(s: Optional[str]) -> str:
    s = low(s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def bool_from_any(v) -> bool:
    if isinstance(v, bool):
        return v
    s = low(str(v))
    return s in {"1", "true", "yes", "y"}


def read_env_tokens(path: Path) -> List[str]:
    toks: List[str] = []
    if not path.exists():
        return toks
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        if k.strip().startswith("GITHUB_TOKEN"):
            tok = v.strip().strip('"').strip("'")
            if tok:
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
                clean[kk] = v
            out.append(clean)
        return out


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


# =========================
# Final output schemas / cleanup
# =========================
RUN_OUTPUT_COLUMNS = [
    "full_name",
    "default_branch",
    "workflow_identifier",
    "workflow_id",
    "workflow_path",
    "looks_like_instru",
    "styles",
    "invocation_types",
    "third_party_provider_name",
    "test_invocation_step_names",
    "jobs_before_anchor_count",
    "called_instru_signal",
    "called_instru_file_paths",
    "called_instru_origin_refs",
    "called_instru_origin_step_names",
    "called_instru_file_types",
    "run_id",
    "run_number",
    "run_attempt",
    "head_sha",
    "created_at",
    "run_started_at",
    "status",
    "run_conclusion",
    "event",
    "head_branch",
    "html_url",
    "extracted_at_utc",
    "queue_seconds",
    "time_to_first_instru_from_run_seconds",
    "instru_duration_seconds",
    "core_instru_window_seconds",
    "instru_exec_window_seconds",
    "instru_job_count",
]

STEP_OUTPUT_COLUMNS = [
    "full_name",
    "workflow_id",
    "run_id",
    "run_attempt",
    "declared_styles",
    "job_name",
    "step_number",
    "step_name",
    "status",
    "conclusion",
    "step_started_at",
    "step_completed_at",
    "seconds_from_run_start",
    "global_step_index",
    "anchor_match",
    "explicit_instru",
    "buildish",
    "real_deviceish",
    "thirdparty_marker",
    "gmd_marker",
    "androidish",
    "custom_followed_file_support",
    "called_origin_match",
]

PER_STYLE_OUTPUT_COLUMNS = [
    "full_name",
    "default_branch",
    "workflow_identifier",
    "workflow_id",
    "workflow_path",
    "looks_like_instru",
    "styles",
    "invocation_types",
    "third_party_provider_name",
    "test_invocation_step_names",
    "jobs_before_anchor_count",
    "called_instru_signal",
    "called_instru_file_paths",
    "called_instru_origin_refs",
    "called_instru_origin_step_names",
    "called_instru_file_types",
    "run_id",
    "run_number",
    "run_attempt",
    "head_sha",
    "created_at",
    "run_started_at",
    "status",
    "run_conclusion",
    "event",
    "head_branch",
    "html_url",
    "extracted_at_utc",
    "queue_seconds",
    "target_style",
    "style_instru_job_count",
    "time_to_first_instru_from_run_seconds",
    "instru_duration_seconds",
    "core_instru_window_seconds",
    "instru_exec_window_seconds",
]

def _clean_output_row(row: Dict[str, object]) -> Dict[str, object]:
    clean = {}
    for k, v in row.items():
        kk = (k or "").replace(BOM, "").strip()
        # drop legacy compatibility-only aliases from final emitted schema
        if kk in {"repo_full_name", "workflow_run_id", "attempt", "Inferred_Label", "style"}:
            continue
        clean[kk] = v
    return clean

def _project_rows(rows: List[Dict[str, object]], columns: List[str]) -> List[Dict[str, object]]:
    return [{c: r.get(c, "") for c in columns} for r in rows]


# =========================
# Canonical style normalization
# =========================
STYLE_CANONICAL = ["Community", "Custom", "GMD", "Third-Party", "Real-Devices"]

STYLE_ALIASES = {
    "community": "Community",
    "custom": "Custom",
    "gmd": "GMD",
    "third party": "Third-Party",
    "third-party": "Third-Party",
    "third_party": "Third-Party",
    "thirdparty": "Third-Party",
    "3p": "Third-Party",
    "real devices": "Real-Devices",
    "real-devices": "Real-Devices",
    "real_devices": "Real-Devices",
    "realdevices": "Real-Devices",
}


def normalize_style_label(s: Optional[str]) -> str:
    key = canon_key(s)
    return STYLE_ALIASES.get(key, norm(s))


def split_styles(s: Optional[str]) -> List[str]:
    raw = norm(s)
    if not raw:
        return []
    parts = [normalize_style_label(x) for x in re.split(r"[|,;/]+", raw) if norm(x)]
    return unique_preserve([p for p in parts if p in STYLE_CANONICAL])


# =========================
# GitHub client
# =========================
class GitHubClient:
    def __init__(self, tokens: List[str]):
        self.tokens = unique_preserve([t for t in tokens if t])
        self._idx = 0
        self._session = requests.Session()

    def _headers(self, token: str) -> Dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "stage3-run-telemetry-v13",
        }

    def _sleep_backoff(self, attempt: int, resp: Optional[requests.Response]) -> None:
        if resp is not None:
            ra = resp.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(min(int(ra), BACKOFF_CAP_S))
                    return
                except Exception:
                    pass
        delay = min(BACKOFF_BASE_S ** max(0, attempt - 1), BACKOFF_CAP_S)
        time.sleep(delay + random.uniform(0, 0.5))

    def get_json(self, url: str, params: Optional[dict] = None) -> Optional[Union[dict, list]]:
        if not self.tokens:
            return None
        last_resp = None
        for attempt in range(1, MAX_RETRIES_PER_REQUEST + 1):
            token = self.tokens[self._idx % len(self.tokens)]
            self._idx += 1
            try:
                resp = self._session.get(
                    url,
                    headers=self._headers(token),
                    params=params,
                    timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                )
                last_resp = resp
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (403, 429, 500, 502, 503, 504):
                    self._sleep_backoff(attempt, resp)
                    continue
                return None
            except requests.RequestException:
                self._sleep_backoff(attempt, last_resp)
        return None

    def get_text(self, url: str) -> Optional[str]:
        if not self.tokens:
            return None
        last_resp = None
        for attempt in range(1, MAX_RETRIES_PER_REQUEST + 1):
            token = self.tokens[self._idx % len(self.tokens)]
            self._idx += 1
            try:
                resp = self._session.get(
                    url,
                    headers=self._headers(token),
                    timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
                )
                last_resp = resp
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code in (403, 429, 500, 502, 503, 504):
                    self._sleep_backoff(attempt, resp)
                    continue
                return None
            except requests.RequestException:
                self._sleep_backoff(attempt, last_resp)
        return None


def parse_owner_repo(repo_full_name: str) -> Tuple[Optional[str], Optional[str]]:
    s = norm(repo_full_name)
    if not s or "/" not in s:
        return None, None
    owner, repo = s.split("/", 1)
    owner = owner.strip()
    repo = repo.strip()
    if not owner or not repo:
        return None, None
    return owner, repo


# =========================
# Current Stage 2 schema compatibility
# =========================
def get_repo_full_name(row: Dict[str, str]) -> str:
    return norm(row.get("repo_full_name") or row.get("full_name"))

def get_run_id(row: Dict[str, str]) -> str:
    v = row.get("workflow_run_id")
    if v in (None, ""):
        v = row.get("run_id")
    return norm(v)

def get_attempt(row: Dict[str, str]) -> str:
    v = row.get("attempt")
    if v in (None, ""):
        v = row.get("run_attempt")
    return norm(v)

def get_declared_styles(row: Dict[str, str]) -> List[str]:
    return split_styles(
        row.get("target_style")
        or row.get("Inferred_Label")
        or row.get("styles")
        or row.get("style")
    )

def adapt_base_row_for_stage3(row: Dict[str, str]) -> Dict[str, str]:
    out = dict(row)
    # carry old aliases expected by downstream Stage 3 logic while preserving current fields
    out["repo_full_name"] = get_repo_full_name(row)
    out["workflow_run_id"] = get_run_id(row)
    out["attempt"] = get_attempt(row)
    # Stage 3 legacy style reads from Inferred_Label/style; normalize to current style fields
    if not norm(out.get("Inferred_Label")):
        out["Inferred_Label"] = row.get("target_style") or row.get("styles") or row.get("style") or ""
    if not norm(out.get("style")) and norm(row.get("target_style")):
        out["style"] = row.get("target_style")
    return out



def gh_contents_raw(gh: GitHubClient, owner: str, repo: str, path: str, ref: str) -> Optional[str]:
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path.lstrip('/')}"
    js = gh.get_json(url, params={"ref": ref})
    if not isinstance(js, dict):
        return None
    if js.get("type") == "file":
        enc = js.get("encoding")
        content = js.get("content")
        if enc == "base64" and isinstance(content, str):
            try:
                return base64.b64decode(content).decode("utf-8", errors="ignore")
            except Exception:
                return None
        durl = js.get("download_url")
        if durl:
            return gh.get_text(durl)
    return None


_workflow_yaml_cache: Dict[Tuple[str, str, str, str], Optional[str]] = {}


def fetch_workflow_yaml_if_possible(gh: GitHubClient, repo_full_name: str, path: str, ref: str) -> Optional[str]:
    if not FETCH_WORKFLOW_YAML:
        return None
    owner, repo = parse_owner_repo(repo_full_name)
    if not owner or not repo or not path or not ref:
        return None
    key = (owner, repo, path, ref)
    if key in _workflow_yaml_cache:
        return _workflow_yaml_cache[key]
    if len(_workflow_yaml_cache) >= WORKFLOW_YAML_CACHE_MAX:
        _workflow_yaml_cache.clear()
    txt = gh_contents_raw(gh, owner, repo, path, ref)
    _workflow_yaml_cache[key] = txt
    return txt


# =========================
# Stage1 / Stage2 carried fields
# =========================
def parse_stage1_confirmed_called_file_paths(row: Dict[str, str]) -> List[str]:
    txt = norm(row.get("called_instru_file_paths") or row.get("called_files_followed"))
    if not txt:
        return []
    parts = [norm(x) for x in re.split(r"[|,;]+", txt) if norm(x)]
    return unique_preserve(parts)


def parse_stage1_confirmed_called_origins(row: Dict[str, str]) -> List[str]:
    txt = norm(row.get("called_instru_origin_step_names"))
    if not txt:
        return []
    parts = [norm(x) for x in re.split(r"[|,;]+", txt) if norm(x)]
    return unique_preserve(parts)


def parse_stage1_confirmed_called_refs(row: Dict[str, str]) -> List[str]:
    txt = norm(row.get("called_instru_origin_refs"))
    if not txt:
        return []
    parts = [norm(x) for x in re.split(r"[|,;]+", txt) if norm(x)]
    return unique_preserve(parts)


def stage1_confirmed_called_signal(row: Dict[str, str]) -> bool:
    return bool_from_any(row.get("called_instru_signal"))


# =========================
# Workflow YAML extraction
# =========================
def sanitize_gha_text(txt: str) -> str:
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    return GHA_EXPR_RE.sub("<GHA_EXPR>", txt)


def extract_jobs_and_steps_from_yaml(yaml_text: Optional[str]) -> List[Dict[str, str]]:
    if not yaml_text:
        return []
    txt = sanitize_gha_text(yaml_text)
    lines = txt.splitlines()
    out: List[Dict[str, str]] = []

    in_jobs = False
    jobs_indent = None
    current_job = None
    current_job_indent = None
    step_idx = 0

    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        indent = len(line) - len(line.lstrip(" "))
        stripped = line.strip()

        if not in_jobs:
            if re.match(r"^jobs\s*:\s*$", stripped):
                in_jobs = True
                jobs_indent = indent
            continue

        if jobs_indent is not None and indent <= jobs_indent and re.match(r"^[A-Za-z0-9_\-]+\s*:", stripped):
            in_jobs = False
            current_job = None
            current_job_indent = None
            continue

        if current_job is None:
            m_job = re.match(r"^([A-Za-z0-9_\-]+)\s*:\s*$", stripped)
            if m_job:
                current_job = m_job.group(1)
                current_job_indent = indent
                step_idx = 0
            continue

        if current_job_indent is not None and indent <= current_job_indent:
            m_job = re.match(r"^([A-Za-z0-9_\-]+)\s*:\s*$", stripped)
            if m_job:
                current_job = m_job.group(1)
                current_job_indent = indent
                step_idx = 0
            else:
                current_job = None
                current_job_indent = None
            continue

        if re.match(r"^-\s*(name|uses|run)\s*:", stripped):
            step_idx += 1
            name = ""
            uses = ""
            runv = ""
            m_name = re.match(r"^-\s*name\s*:\s*(.+)$", stripped)
            m_uses = re.match(r"^-\s*uses\s*:\s*(.+)$", stripped)
            m_run = re.match(r"^-\s*run\s*:\s*(.+)$", stripped)
            if m_name:
                name = m_name.group(1).strip().strip('"').strip("'")
            elif m_uses:
                uses = m_uses.group(1).strip().strip('"').strip("'")
            elif m_run:
                runv = m_run.group(1).strip().strip('"').strip("'")
            out.append(
                {
                    "job_name": current_job or "",
                    "step_index": str(step_idx),
                    "yaml_step_name": name,
                    "yaml_uses": uses,
                    "yaml_run": runv,
                }
            )
            continue

        if out:
            last = out[-1]
            if last.get("job_name") != (current_job or ""):
                continue
            if stripped.startswith("name:") and not last.get("yaml_step_name"):
                last["yaml_step_name"] = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("uses:") and not last.get("yaml_uses"):
                last["yaml_uses"] = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            elif stripped.startswith("run:") and not last.get("yaml_run"):
                last["yaml_run"] = stripped.split(":", 1)[1].strip().strip('"').strip("'")


    return out


# =========================
# Heuristics
# =========================
EMU_KWS = [
    "emulator",
    "avd",
    "android-emulator",
    "sdkmanager",
    "avdmanager",
    "reactivecircus/android-emulator-runner",
    "enable-kvm",
    "qemu",
    "genymotion",
]
TEST_KWS = [
    "connectedandroidtest",
    "androidtest",
    "instrumentation",
    "espresso",
    "gradlew connected",
    "adb shell am instrument",
    "detox test",
    "flutter drive",
    "integrationtest",
    "integration test",
    "xcodebuild test",
    "maestro test",
]
BUILDISH_KWS = ["assemble", "build", "compile", "dependencies", "cache", "setup java", "checkout"]
DEVICE_KWS = ["real device", "physical device"]

THIRDPARTY_USES_MARKERS = [
    "reactivecircus/android-emulator-runner",
    "reactivecircus/android-emulator-runner@",
    "mobile-dev-inc/action-maestro-cloud",
    "genymotion",
    "maestro-cloud",
    "browserstack",
    "saucelabs",
    "firebase test lab",
    "bitrise-io",
]

GMD_MARKERS = [
    "gmd",
    "gradle managed device",
    "gradle managed devices",
    "manageddevice",
    "managed device",
]

ANDROIDISH_MARKERS = [
    "android-emulator-runner",
    "avd",
    "emulator",
    "sdkmanager",
    "avdmanager",
    "connectedandroidtest",
    "adb shell am instrument",
    "uiautomator",
    "espresso",
]

CUSTOM_FOLLOWED_FILE_MARKERS = [
    ".sh",
    ".bash",
    ".zsh",
    ".ps1",
    ".py",
    ".rb",
    ".js",
    ".ts",
    ".kts",
    ".gradle",
    ".yml",
    ".yaml",
]


def text_blob(*parts: Optional[str]) -> str:
    return "\n".join([p for p in parts if norm(p)])


def has_any_kw(txt: str, kws: List[str]) -> bool:
    t = low(txt)
    return any(k in t for k in kws)


def looks_emulatorish(txt: str) -> bool:
    return has_any_kw(txt, EMU_KWS)


def looks_testish(txt: str) -> bool:
    return has_any_kw(txt, TEST_KWS)


def looks_buildish(txt: str) -> bool:
    return has_any_kw(txt, BUILDISH_KWS)


def looks_real_deviceish(txt: str) -> bool:
    return has_any_kw(txt, DEVICE_KWS)


def is_thirdparty_marker(txt: str) -> bool:
    return has_any_kw(txt, THIRDPARTY_USES_MARKERS)


def is_gmd_marker(txt: str) -> bool:
    return has_any_kw(txt, GMD_MARKERS)


def is_androidish(txt: str) -> bool:
    return has_any_kw(txt, ANDROIDISH_MARKERS)


def looks_custom_followed_file(path_or_text: str) -> bool:
    t = low(path_or_text)
    return any(m in t for m in CUSTOM_FOLLOWED_FILE_MARKERS)


# =========================
# Step flag inference
# =========================
def infer_flags_from_step(step: Dict[str, str], row: Dict[str, str]) -> Dict[str, bool]:
    nm = norm(step.get("name"))
    run_cmd = norm(step.get("run"))
    uses = norm(step.get("uses"))
    blob = text_blob(nm, uses, run_cmd)

    declared_styles = get_declared_styles(row)

    thirdparty_declared = "Third-Party" in declared_styles
    custom_declared = "Custom" in declared_styles
    gmd_declared = "GMD" in declared_styles
    community_declared = "Community" in declared_styles
    real_declared = "Real-Devices" in declared_styles

    anchorish = looks_emulatorish(blob) or is_thirdparty_marker(blob) or is_gmd_marker(blob)
    explicit_test = looks_testish(blob)
    buildish = looks_buildish(blob)
    realish = looks_real_deviceish(blob)
    thirdpartyish = is_thirdparty_marker(blob)
    gmdish = is_gmd_marker(blob)
    androidish = is_androidish(blob)

    s1_called_signal = stage1_confirmed_called_signal(row)
    called_origins = [low(x) for x in parse_stage1_confirmed_called_origins(row)]
    called_refs = [low(x) for x in parse_stage1_confirmed_called_refs(row)]
    step_name_l = low(nm)
    uses_l = low(uses)
    run_l = low(run_cmd)

    called_origin_match = False
    if s1_called_signal:
        if step_name_l and any(step_name_l == x or step_name_l in x or x in step_name_l for x in called_origins):
            called_origin_match = True
        elif uses_l and any(ref and ref in uses_l for ref in called_refs):
            called_origin_match = True
        elif run_l and any(ref and ref in run_l for ref in called_refs):
            called_origin_match = True

    called_file_support = False
    if s1_called_signal:
        for p in parse_stage1_confirmed_called_file_paths(row):
            if looks_custom_followed_file(p):
                called_file_support = True
                break

    return {
        "anchor_match": anchorish,
        "explicit_instru": explicit_test,
        "buildish": buildish,
        "real_deviceish": realish,
        "thirdparty_marker": thirdpartyish or thirdparty_declared,
        "gmd_marker": gmdish or gmd_declared,
        "androidish": androidish,
        "custom_followed_file_support": called_file_support,
        "called_origin_match": called_origin_match,
        "community_declared": community_declared,
        "custom_declared": custom_declared,
        "gmd_declared": gmd_declared,
        "thirdparty_declared": thirdparty_declared,
        "real_declared": real_declared,
    }


# =========================
# Job / step materialization
# =========================
def gh_list_all_jobs_for_run(gh: GitHubClient, owner: str, repo: str, run_id: str) -> List[dict]:
    out: List[dict] = []
    if not owner or not repo or not run_id:
        return out
    for page in range(1, MAX_PAGES_PER_LIST + 1):
        url = f"https://api.github.com/repos/{owner}/{repo}/actions/runs/{run_id}/jobs"
        js = gh.get_json(url, params={"per_page": 100, "page": page})
        if not isinstance(js, dict):
            break
        jobs = js.get("jobs") or []
        if not isinstance(jobs, list) or not jobs:
            break
        out.extend([j for j in jobs if isinstance(j, dict)])
        if len(jobs) < 100:
            break
    return out


def jobs_to_step_rows(jobs: List[dict], row: Dict[str, str]) -> List[Dict[str, object]]:
    out: List[Dict[str, object]] = []
    run_started = iso_to_dt(row.get("run_started_at"))
    declared_styles = get_declared_styles(row)

    for job in jobs:
        job_name = norm(job.get("name"))
        job_started_at = iso_to_dt(job.get("started_at"))
        job_completed_at = iso_to_dt(job.get("completed_at"))
        steps = job.get("steps") or []
        if not isinstance(steps, list):
            steps = []
        for idx, st in enumerate(steps, start=1):
            if not isinstance(st, dict):
                continue
            nm = norm(st.get("name"))
            st_started = iso_to_dt(st.get("started_at"))
            st_completed = iso_to_dt(st.get("completed_at"))
            run_cmd = norm(st.get("run"))
            uses = norm(st.get("uses"))
            inferred_styles = declared_styles[:]
            flags = infer_flags_from_step({"name": nm, "run": run_cmd, "uses": uses}, row)
            out.append(
                {
                    "job_name": job_name,
                    "job_started_at": job_started_at,
                    "job_completed_at": job_completed_at,
                    "step_number": idx,
                    "step_name": nm,
                    "step_started_at": st_started,
                    "step_completed_at": st_completed,
                    "status": norm(st.get("status")),
                    "conclusion": norm(st.get("conclusion")),
                    "run": run_cmd,
                    "uses": uses,
                    "declared_styles": inferred_styles,
                    **flags,
                }
            )

    out.sort(
        key=lambda r: (
            r.get("job_started_at") or datetime.max.replace(tzinfo=timezone.utc),
            r.get("step_started_at") or datetime.max.replace(tzinfo=timezone.utc),
            int(r.get("step_number") or 0),
        )
    )

    for i, r in enumerate(out):
        r["global_step_index"] = i + 1
        r["seconds_from_run_start"] = dt_to_seconds(run_started, r.get("step_started_at"))
    return out


# =========================
# Anchor + execution selection
# =========================
@dataclass
class BoundaryStub:
    seconds: Optional[int]
    source_type: str
    source_layer: str
    observational_quality: str
    semantic_quality: str
    step_name: str
    job_name: str
    rank: Optional[int] = None


def build_boundary_stub(
    run_started_at: Optional[datetime],
    step: Optional[Dict[str, object]],
    source_type: str,
    source_layer: str,
    observational_quality: str,
    semantic_quality: str,
    rank: Optional[int] = None,
) -> Optional[BoundaryStub]:
    if not step:
        return None
    sec = dt_to_seconds(run_started_at, step.get("step_started_at"))
    return BoundaryStub(
        seconds=sec,
        source_type=source_type,
        source_layer=source_layer,
        observational_quality=observational_quality,
        semantic_quality=semantic_quality,
        step_name=norm(step.get("step_name")),
        job_name=norm(step.get("job_name")),
        rank=rank,
    )


def style_allows_broad_continuation(style: str) -> bool:
    s = normalize_style_label(style)
    return s in {"Third-Party", "Custom"}


def choose_anchor_step(step_rows: List[Dict[str, object]], style: str) -> Optional[Dict[str, object]]:
    style = normalize_style_label(style)
    candidates = []
    for st in step_rows:
        if style == "Community":
            ok = bool(st.get("anchor_match")) and not bool(st.get("thirdparty_marker")) and not bool(st.get("gmd_marker"))
        elif style == "Custom":
            ok = bool(st.get("anchor_match")) or bool(st.get("called_origin_match")) or bool(st.get("custom_followed_file_support"))
        elif style == "GMD":
            ok = bool(st.get("gmd_marker")) or (bool(st.get("anchor_match")) and bool(st.get("gmd_declared")))
        elif style == "Third-Party":
            ok = bool(st.get("thirdparty_marker")) or (bool(st.get("anchor_match")) and bool(st.get("thirdparty_declared")))
        elif style == "Real-Devices":
            ok = bool(st.get("real_deviceish"))
        else:
            ok = False
        if ok:
            candidates.append(st)
    if not candidates:
        return None
    candidates.sort(
        key=lambda r: (
            r.get("step_started_at") or datetime.max.replace(tzinfo=timezone.utc),
            int(r.get("global_step_index") or 0),
        )
    )
    return candidates[0]


def choose_explicit_execution_start(
    step_rows: List[Dict[str, object]], style: str, anchor: Optional[Dict[str, object]]
) -> Optional[Dict[str, object]]:
    style = normalize_style_label(style)
    if not step_rows:
        return None
    anchor_idx = int(anchor.get("global_step_index") or 0) if anchor else 0
    anchor_job = norm(anchor.get("job_name")) if anchor else ""
    candidates = []
    for st in step_rows:
        idx = int(st.get("global_step_index") or 0)
        if idx < anchor_idx:
            continue
        if not bool(st.get("explicit_instru")):
            continue
        if style in {"Community", "GMD", "Real-Devices"} and anchor_job and norm(st.get("job_name")) != anchor_job:
            continue
        if style == "Community" and (bool(st.get("thirdparty_marker")) or bool(st.get("gmd_marker"))):
            continue
        if style == "GMD" and not bool(st.get("gmd_marker") or st.get("gmd_declared") or st.get("androidish")):
            continue
        if style == "Third-Party" and not bool(
            st.get("thirdparty_marker") or st.get("thirdparty_declared") or st.get("androidish")
        ):
            continue
        candidates.append(st)
    if not candidates:
        return None
    candidates.sort(
        key=lambda r: (
            r.get("step_started_at") or datetime.max.replace(tzinfo=timezone.utc),
            int(r.get("global_step_index") or 0),
        )
    )
    return candidates[0]


def choose_execution_end(
    step_rows: List[Dict[str, object]],
    style: str,
    exec_start: Optional[Dict[str, object]],
    anchor: Optional[Dict[str, object]],
) -> Optional[Dict[str, object]]:
    style = normalize_style_label(style)
    if not exec_start:
        return None
    start_idx = int(exec_start.get("global_step_index") or 0)
    anchor_job = norm(anchor.get("job_name")) if anchor else ""
    start_job = norm(exec_start.get("job_name"))
    broad = style_allows_broad_continuation(style)

    candidates = []
    for st in step_rows:
        idx = int(st.get("global_step_index") or 0)
        if idx < start_idx:
            continue
        if not broad:
            if anchor_job and norm(st.get("job_name")) != anchor_job:
                continue
        else:
            if start_job and norm(st.get("job_name")) and norm(st.get("job_name")) != start_job:
                if idx != start_idx and not bool(
                    st.get("explicit_instru")
                    or st.get("anchor_match")
                    or st.get("thirdparty_marker")
                    or st.get("called_origin_match")
                ):
                    continue
        if style == "Community" and (bool(st.get("thirdparty_marker")) or bool(st.get("gmd_marker"))):
            continue
        if style == "GMD" and not bool(
            st.get("gmd_marker") or st.get("gmd_declared") or st.get("androidish") or st.get("explicit_instru")
        ):
            continue
        if style == "Third-Party" and not bool(
            st.get("thirdparty_marker")
            or st.get("thirdparty_declared")
            or st.get("androidish")
            or st.get("explicit_instru")
        ):
            continue
        if style == "Custom" and not bool(
            st.get("explicit_instru")
            or st.get("anchor_match")
            or st.get("called_origin_match")
            or st.get("custom_followed_file_support")
            or st.get("androidish")
        ):
            continue
        candidates.append(st)
    if not candidates:
        return exec_start
    candidates.sort(
        key=lambda r: (
            r.get("step_started_at") or datetime.min.replace(tzinfo=timezone.utc),
            int(r.get("global_step_index") or 0),
        )
    )
    return candidates[-1]


def find_terminal_step_details(
    step_rows: List[Dict[str, object]],
    style: str,
    anchor: Optional[Dict[str, object]],
    exec_start: Optional[Dict[str, object]],
    run_started_at: Optional[datetime],
) -> Dict[str, object]:
    direct_anchor = (
        build_boundary_stub(
            run_started_at,
            anchor,
            source_type="direct" if anchor else "missing",
            source_layer="step",
            observational_quality="observed" if anchor else "missing",
            semantic_quality="semantic-anchor" if anchor else "missing",
        )
        if anchor
        else None
    )

    direct_exec = (
        build_boundary_stub(
            run_started_at,
            exec_start,
            source_type="direct" if exec_start else "missing",
            source_layer="step",
            observational_quality="observed" if exec_start else "missing",
            semantic_quality="semantic-execution" if exec_start else "missing",
        )
        if exec_start
        else None
    )

    exec_end = choose_execution_end(step_rows, style, exec_start, anchor)
    direct_end = (
        build_boundary_stub(
            run_started_at,
            exec_end,
            source_type="direct" if exec_end else "missing",
            source_layer="step",
            observational_quality="observed" if exec_end else "missing",
            semantic_quality="semantic-end" if exec_end else "missing",
        )
        if exec_end
        else None
    )

    ambiguity_count = 0
    if anchor:
        ambiguity_count += 1
    if exec_start:
        ambiguity_count += 1
    if exec_end:
        ambiguity_count += 1

    ambiguity_level = "none"
    if ambiguity_count == 1:
        ambiguity_level = "low"
    elif ambiguity_count == 2:
        ambiguity_level = "medium"
    elif ambiguity_count >= 3:
        ambiguity_level = "low"

    return {
        "anchor": direct_anchor,
        "exec_start": direct_exec,
        "exec_end": direct_end,
        "ambiguity_count": ambiguity_count,
        "ambiguity_level": ambiguity_level,
    }


# =========================
# CSV field construction
# =========================
STYLE_METRIC_KEYS = [
    "timeline_evidence_mode",
    "anchor_direct_seconds",
    "anchor_direct_source_type",
    "anchor_direct_source_layer",
    "anchor_direct_observational_quality",
    "anchor_direct_semantic_quality",
    "anchor_direct_step_name",
    "anchor_direct_job_name",
    "anchor_fallback1_seconds",
    "anchor_fallback1_source_type",
    "anchor_fallback1_source_layer",
    "anchor_fallback1_rank",
    "anchor_fallback2_seconds",
    "anchor_fallback2_source_type",
    "anchor_fallback2_source_layer",
    "anchor_fallback2_rank",
    "anchor_selected_seconds",
    "anchor_selected_source_type",
    "anchor_selected_source_layer",
    "anchor_selected_observational_quality",
    "anchor_selected_semantic_quality",
    "anchor_selected_step_name",
    "anchor_selected_job_name",
    "anchor_ambiguity_count",
    "anchor_ambiguity_level",
    "exec_start_direct_seconds",
    "exec_start_direct_source_type",
    "exec_start_direct_source_layer",
    "exec_start_direct_observational_quality",
    "exec_start_direct_semantic_quality",
    "exec_start_direct_step_name",
    "exec_start_direct_job_name",
    "exec_start_fallback1_seconds",
    "exec_start_fallback1_source_type",
    "exec_start_fallback1_source_layer",
    "exec_start_fallback1_rank",
    "exec_start_fallback2_seconds",
    "exec_start_fallback2_source_type",
    "exec_start_fallback2_source_layer",
    "exec_start_fallback2_rank",
    "exec_start_selected_seconds",
    "exec_start_selected_source_type",
    "exec_start_selected_source_layer",
    "exec_start_selected_observational_quality",
    "exec_start_selected_semantic_quality",
    "exec_start_selected_step_name",
    "exec_start_selected_job_name",
    "exec_start_ambiguity_count",
    "exec_start_ambiguity_level",
    "exec_end_direct_seconds",
    "exec_end_direct_source_type",
    "exec_end_direct_source_layer",
    "exec_end_direct_observational_quality",
    "exec_end_direct_semantic_quality",
    "exec_end_direct_step_name",
    "exec_end_direct_job_name",
    "exec_end_fallback1_seconds",
    "exec_end_fallback1_source_type",
    "exec_end_fallback1_source_layer",
    "exec_end_fallback1_rank",
    "exec_end_fallback2_seconds",
    "exec_end_fallback2_source_type",
    "exec_end_fallback2_source_layer",
    "exec_end_fallback2_rank",
    "exec_end_selected_seconds",
    "exec_end_selected_source_type",
    "exec_end_selected_source_layer",
    "exec_end_selected_observational_quality",
    "exec_end_selected_semantic_quality",
    "exec_end_selected_step_name",
    "exec_end_selected_job_name",
    "exec_end_ambiguity_count",
    "exec_end_ambiguity_level",
    "time_to_first_instru_from_run_seconds",
    "instru_duration_seconds",
    "core_instru_window_seconds",
    "instru_exec_window_seconds",
]


def empty_style_metric_dict() -> Dict[str, object]:
    return {k: "" for k in STYLE_METRIC_KEYS}


def apply_boundary(
    prefix: str,
    payload: Dict[str, object],
    stub: Optional[BoundaryStub],
    ambiguity_count: int,
    ambiguity_level: str,
) -> None:
    if not stub:
        payload[f"{prefix}_direct_seconds"] = ""
        payload[f"{prefix}_direct_source_type"] = "missing"
        payload[f"{prefix}_direct_source_layer"] = "missing"
        payload[f"{prefix}_direct_observational_quality"] = "missing"
        payload[f"{prefix}_direct_semantic_quality"] = "missing"
        payload[f"{prefix}_direct_step_name"] = ""
        payload[f"{prefix}_direct_job_name"] = ""
        payload[f"{prefix}_selected_seconds"] = ""
        payload[f"{prefix}_selected_source_type"] = "missing"
        payload[f"{prefix}_selected_source_layer"] = "missing"
        payload[f"{prefix}_selected_observational_quality"] = "missing"
        payload[f"{prefix}_selected_semantic_quality"] = "missing"
        payload[f"{prefix}_selected_step_name"] = ""
        payload[f"{prefix}_selected_job_name"] = ""
    else:
        payload[f"{prefix}_direct_seconds"] = stub.seconds if stub.seconds is not None else ""
        payload[f"{prefix}_direct_source_type"] = stub.source_type
        payload[f"{prefix}_direct_source_layer"] = stub.source_layer
        payload[f"{prefix}_direct_observational_quality"] = stub.observational_quality
        payload[f"{prefix}_direct_semantic_quality"] = stub.semantic_quality
        payload[f"{prefix}_direct_step_name"] = stub.step_name
        payload[f"{prefix}_direct_job_name"] = stub.job_name
        payload[f"{prefix}_selected_seconds"] = stub.seconds if stub.seconds is not None else ""
        payload[f"{prefix}_selected_source_type"] = stub.source_type
        payload[f"{prefix}_selected_source_layer"] = stub.source_layer
        payload[f"{prefix}_selected_observational_quality"] = stub.observational_quality
        payload[f"{prefix}_selected_semantic_quality"] = stub.semantic_quality
        payload[f"{prefix}_selected_step_name"] = stub.step_name
        payload[f"{prefix}_selected_job_name"] = stub.job_name

    payload[f"{prefix}_fallback1_seconds"] = ""
    payload[f"{prefix}_fallback1_source_type"] = ""
    payload[f"{prefix}_fallback1_source_layer"] = ""
    payload[f"{prefix}_fallback1_rank"] = ""
    payload[f"{prefix}_fallback2_seconds"] = ""
    payload[f"{prefix}_fallback2_source_type"] = ""
    payload[f"{prefix}_fallback2_source_layer"] = ""
    payload[f"{prefix}_fallback2_rank"] = ""
    payload[f"{prefix}_ambiguity_count"] = ambiguity_count
    payload[f"{prefix}_ambiguity_level"] = ambiguity_level


def compute_style_metrics_for_run(row: Dict[str, str], step_rows: List[Dict[str, object]], style: str) -> Dict[str, object]:
    payload = empty_style_metric_dict()
    style = normalize_style_label(style)
    run_started_at = iso_to_dt(row.get("run_started_at"))

    anchor = choose_anchor_step(step_rows, style)
    exec_start = choose_explicit_execution_start(step_rows, style, anchor)
    det = find_terminal_step_details(step_rows, style, anchor, exec_start, run_started_at)

    payload["timeline_evidence_mode"] = "direct-step-boundaries"

    apply_boundary("anchor", payload, det.get("anchor"), det.get("ambiguity_count", 0), det.get("ambiguity_level", "none"))
    apply_boundary(
        "exec_start",
        payload,
        det.get("exec_start"),
        det.get("ambiguity_count", 0),
        det.get("ambiguity_level", "none"),
    )
    apply_boundary("exec_end", payload, det.get("exec_end"), det.get("ambiguity_count", 0), det.get("ambiguity_level", "none"))

    anchor_sec = det.get("anchor").seconds if det.get("anchor") else None
    start_sec = det.get("exec_start").seconds if det.get("exec_start") else None
    end_sec = det.get("exec_end").seconds if det.get("exec_end") else None

    payload["time_to_first_instru_from_run_seconds"] = anchor_sec if anchor_sec is not None else ""
    payload["instru_duration_seconds"] = (
        end_sec - anchor_sec if (anchor_sec is not None and end_sec is not None and end_sec >= anchor_sec) else ""
    )
    payload["core_instru_window_seconds"] = (
        end_sec - start_sec if (start_sec is not None and end_sec is not None and end_sec >= start_sec) else ""
    )
    payload["instru_exec_window_seconds"] = payload["core_instru_window_seconds"]
    return payload


def style_has_executed_instru(payload: Dict[str, object]) -> bool:
    """
    Stage 3 authoritative executed-subset gate.
    Treat a style as executed only when we can identify an instrumentation path,
    i.e. at least an anchor and an execution window signal.
    """
    for k in [
        "instru_duration_seconds",
        "core_instru_window_seconds",
        "instru_exec_window_seconds",
        "time_to_first_instru_from_run_seconds",
        "anchor_selected_seconds",
        "exec_end_selected_seconds",
    ]:
        v = payload.get(k, "")
        if v not in ("", None):
            return True
    return False



def build_run_level_row(base_row: Dict[str, str], style_payloads: Dict[str, Dict[str, object]]) -> Dict[str, object]:
    out: Dict[str, object] = _clean_output_row(dict(base_row))

    all_styles = list(style_payloads.keys()) or get_declared_styles(base_row)
    picked = all_styles[0] if all_styles else ""
    primary = style_payloads.get(picked) if picked else None

    if primary:
        out["time_to_first_instru_from_run_seconds"] = primary.get("time_to_first_instru_from_run_seconds", "")
        out["instru_duration_seconds"] = primary.get("instru_duration_seconds", "")
        out["core_instru_window_seconds"] = primary.get("core_instru_window_seconds", "")
        out["instru_exec_window_seconds"] = primary.get("instru_exec_window_seconds", "")
        # Make Stage 3 the authoritative executed subset, like V20.
        out["instru_job_count"] = str(max(1, len(style_payloads)))
    else:
        out["time_to_first_instru_from_run_seconds"] = ""
        out["instru_duration_seconds"] = ""
        out["core_instru_window_seconds"] = ""
        out["instru_exec_window_seconds"] = ""
        out["instru_job_count"] = "0"

    # final emitted naming
    out["full_name"] = out.get("full_name") or base_row.get("repo_full_name", "")
    out["run_id"] = out.get("run_id") or base_row.get("workflow_run_id", "")
    out["run_attempt"] = out.get("run_attempt") or base_row.get("attempt", "")
    return out


def build_step_breakdown_rows(base_row: Dict[str, str], step_rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    out = []
    declared = get_declared_styles(base_row)
    for st in step_rows:
        out.append(
            {
                "full_name": base_row.get("repo_full_name", ""),
                "workflow_id": base_row.get("workflow_id", ""),
                "run_id": base_row.get("workflow_run_id", ""),
                "run_attempt": base_row.get("attempt", ""),
                "declared_styles": "|".join(declared),
                "job_name": st.get("job_name", ""),
                "step_number": st.get("step_number", ""),
                "step_name": st.get("step_name", ""),
                "status": st.get("status", ""),
                "conclusion": st.get("conclusion", ""),
                "step_started_at": st.get("step_started_at").isoformat() if st.get("step_started_at") else "",
                "step_completed_at": st.get("step_completed_at").isoformat() if st.get("step_completed_at") else "",
                "seconds_from_run_start": st.get("seconds_from_run_start", ""),
                "global_step_index": st.get("global_step_index", ""),
                "anchor_match": st.get("anchor_match", False),
                "explicit_instru": st.get("explicit_instru", False),
                "buildish": st.get("buildish", False),
                "real_deviceish": st.get("real_deviceish", False),
                "thirdparty_marker": st.get("thirdparty_marker", False),
                "gmd_marker": st.get("gmd_marker", False),
                "androidish": st.get("androidish", False),
                "custom_followed_file_support": st.get("custom_followed_file_support", False),
                "called_origin_match": st.get("called_origin_match", False),
            }
        )
    return out


def build_run_per_style_rows(base_row: Dict[str, str], style_payloads: Dict[str, Dict[str, object]]) -> List[Dict[str, object]]:
    out = []
    styles_to_emit = list(style_payloads.keys())
    for style in styles_to_emit:
        p = style_payloads.get(style, empty_style_metric_dict())
        row = _clean_output_row(dict(base_row))
        row["target_style"] = style
        row["style_instru_job_count"] = "1" if style_has_executed_instru(p) else "0"
        for k in STYLE_METRIC_KEYS:
            row[k] = p.get(k, "")
        row["time_to_first_instru_from_run_seconds"] = p.get("time_to_first_instru_from_run_seconds", "")
        row["instru_duration_seconds"] = p.get("instru_duration_seconds", "")
        row["core_instru_window_seconds"] = p.get("core_instru_window_seconds", "")
        row["instru_exec_window_seconds"] = p.get("instru_exec_window_seconds", "")
        row["full_name"] = row.get("full_name") or base_row.get("repo_full_name", "")
        row["run_id"] = row.get("run_id") or base_row.get("workflow_run_id", "")
        row["run_attempt"] = row.get("run_attempt") or base_row.get("attempt", "")
        out.append(row)
    return out


# =========================
# Main
# =========================

def main() -> None:
    tokens = []
    try:
        tokens = load_github_tokens(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
        print(f"Loaded GitHub token pool size: {len(tokens)}")
    except Exception:
        tokens = read_env_tokens(TOKENS_ENV_PATH)
    gh = GitHubClient(tokens)

    run_rows = read_csv_rows(IN_STAGE2_RUN_CSV)
    per_style_driver_rows = read_csv_rows(IN_STAGE2_PER_STYLE_CSV)

    print(f"Stage3 run input rows: {len(run_rows)} from {IN_STAGE2_RUN_CSV}")
    print(f"Stage3 per-style input rows: {len(per_style_driver_rows)} from {IN_STAGE2_PER_STYLE_CSV}")

    # Build run-level reference by (repo, run_id, attempt) so each per-style row can inherit rich Stage2 fields.
    run_ref: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for r in run_rows:
        rr = adapt_base_row_for_stage3(r)
        key = (rr.get("repo_full_name",""), rr.get("workflow_run_id",""), rr.get("attempt",""))
        run_ref[key] = rr

    driver_rows: List[Dict[str, str]] = []
    for r in per_style_driver_rows:
        rr = adapt_base_row_for_stage3(r)
        key = (rr.get("repo_full_name",""), rr.get("workflow_run_id",""), rr.get("attempt",""))
        merged = dict(run_ref.get(key, {}))
        merged.update(rr)  # per-style row should win for style field
        driver_rows.append(merged)

    if PROCESS_ONLY_RELEVANT_ROWS:
        driver_rows = [r for r in driver_rows if get_declared_styles(r)]

    run_rows_out: List[Dict[str, object]] = []
    step_rows_out: List[Dict[str, object]] = []
    per_style_rows_out: List[Dict[str, object]] = []

    iterator = tqdm(driver_rows, desc="Stage3") if tqdm else driver_rows

    # Avoid duplicate expensive job fetches across multiple per-style rows for the same run
    processed_runs: Set[Tuple[str, str, str]] = set()

    for row in iterator:
        repo_full_name = get_repo_full_name(row)
        run_id = get_run_id(row)
        attempt = get_attempt(row)
        owner, repo = parse_owner_repo(repo_full_name)
        if not owner or not repo or not run_id:
            per_style_rows_out.extend(build_run_per_style_rows(row, {}))
            continue

        run_key = (repo_full_name, run_id, attempt)
        if run_key in processed_runs:
            # Still emit per-style row for this driver row using empty payload if duplicate;
            # duplicates are not expected when driver is run_inventory_per_style, but guard anyway.
            per_style_rows_out.extend(build_run_per_style_rows(row, {}))
            continue

        processed_runs.add(run_key)

        jobs = gh_list_all_jobs_for_run(gh, owner, repo, run_id)
        steps = jobs_to_step_rows(jobs, row)

        style_payloads: Dict[str, Dict[str, object]] = {}
        for style in get_declared_styles(row):
            payload = compute_style_metrics_for_run(row, steps, style)
            if style_has_executed_instru(payload):
                style_payloads[style] = payload

        # V20-aligned executed-subset gate:
        # only retain rows/runs where instrumentation actually executed.
        if not style_payloads:
            continue

        run_rows_out.append(build_run_level_row(row, style_payloads))
        step_rows_out.extend(build_step_breakdown_rows(row, steps))
        per_style_rows_out.extend(build_run_per_style_rows(row, style_payloads))

    # De-duplicate final outputs with current schema keys
    def dedupe_rows(rows: List[Dict[str, object]], keys: List[str]) -> List[Dict[str, object]]:
        seen = set()
        out = []
        for r in rows:
            k = tuple(str(r.get(x, "")) for x in keys)
            if k in seen:
                continue
            seen.add(k)
            out.append(r)
        return out

    run_rows_out = dedupe_rows(run_rows_out, ["repo_full_name", "workflow_run_id", "attempt"])
    step_rows_out = dedupe_rows(step_rows_out, ["full_name", "run_id", "run_attempt", "job_name", "global_step_index", "step_name"])
    per_style_rows_out = dedupe_rows(per_style_rows_out, ["repo_full_name", "workflow_run_id", "attempt", "style"])

    run_rows_out = [_clean_output_row(r) for r in run_rows_out]
    step_rows_out = [_clean_output_row(r) for r in step_rows_out]
    per_style_rows_out = [_clean_output_row(r) for r in per_style_rows_out]

    write_csv(OUT_STAGE3A_RUNS_CSV, RUN_OUTPUT_COLUMNS, _project_rows(run_rows_out, RUN_OUTPUT_COLUMNS))
    write_csv(OUT_STAGE3B_STEPS_CSV, STEP_OUTPUT_COLUMNS, _project_rows(step_rows_out, STEP_OUTPUT_COLUMNS))
    write_csv(OUT_STAGE3C_RUN_PER_STYLE_CSV, PER_STYLE_OUTPUT_COLUMNS, _project_rows(per_style_rows_out, PER_STYLE_OUTPUT_COLUMNS))

    print(f"[done] Stage 3 runs rows={len(run_rows_out)} steps rows={len(step_rows_out)} per-style rows={len(per_style_rows_out)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[error] Stage 3 failed: {exc}", file=sys.stderr)
        raise
