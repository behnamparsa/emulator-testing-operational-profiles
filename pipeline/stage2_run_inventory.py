# ============================================================
# Stage 2 (ADJUSTED): Run inventory + anchored fallback metrics (S2_)
#
# What changed in this version
# - Uses workflow_id (NOT workflow_identifier) to list runs (more reliable)
# - Removes Stage-2 fields that are not present in current Stage-1 output
#   (gmd_capable, gmd_reasons, evidence_labels, label_source, workflow_name)
# - Adds anchored runtime-step matching using Stage-1:
#     test_invocation_step_names
#   This improves fallback timing (especially TTFTS)
# - Keeps regex fallback (step/job) for coverage
# - Stage-2 heuristics updated to better align with Stage-1 labels
#   (includes Detox / Flutter wording in runtime step-name heuristics)
#
# NEW (for modified TTFTS readiness)
# - Pass-through Stage-1 extra anchor-position field if present:
#     jobs_before_anchor_count
# - Adds anchor-job timing outputs for job-based TTFTS:
#     anchor_job_name
#     anchor_job_started_at
#     anchor_job_start_source
#     time_to_first_instru_from_anchor_job_seconds
#     time_to_first_instru_from_anchor_job_quality
# - Adds S2_ mirrors for the above so Stage 3 can consume them as fallback
#
# NEW (ONLY): carries Stage-1 called-file instrumentation evidence fields
#   so Stage 3 can use them directly without re-following every file:
#     called_instru_signal
#     called_instru_file_paths
#     called_instru_origin_refs
#     called_instru_origin_step_names
#     called_instru_file_types
#
# Output:
# - Keeps legacy metrics columns
# - Adds S2_ mirror/fallback fields for Stage 3 consumption
# ============================================================

import csv
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Union, Tuple

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

MAX_TOKENS_TO_USE = 7
SLEEP_BETWEEN_WORKFLOWS_SEC = 0.05


# =========================
# Helpers
# =========================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

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
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)

def load_existing_keys(csv_path: Path, key_field: str) -> Set[str]:
    keys: Set[str] = set()
    if not csv_path.exists():
        return keys
    with csv_path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            k = (row.get(key_field) or "").strip()
            if k:
                keys.add(k)
    return keys

def load_tokens_from_env_file(env_path: Path, max_tokens: int = 3) -> List[str]:
    """CI-safe token loader.
    - In GitHub Actions, reads GH_PAT/GITHUB_TOKEN from environment.
    - Locally, will also read from env_path if it exists.
    """
    return load_github_tokens(env_path=env_path, max_tokens=max_tokens)

def unique_preserve(seq: Iterable[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for x in seq:
        if x is None:
            continue
        s = str(x)
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def safe_join_names(names: List[str], max_len: int = 700) -> str:
    s = ",".join(unique_preserve([n.strip() for n in names if n and str(n).strip()]))
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


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
    out = [r for r in raw if r]
    return unique_preserve(out)

def anchored_step_match(runtime_step_name: str, anchor_names: List[str]) -> bool:
    """
    Robust but conservative:
    - exact normalized match
    - contains match either direction for matrix/appended labels
    """
    rn = normalize_name(runtime_step_name)
    if not rn:
        return False
    for a in anchor_names:
        an = normalize_name(a)
        if not an:
            continue
        if rn == an:
            return True
        if an in rn or rn in an:
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
            "User-Agent": "run-inventory-stage2-v16/1.5",
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
def get_repo_default_branch(gh: GitHubClient, full_name: str) -> str:
    data = gh.request_json("GET", f"https://api.github.com/repos/{full_name}")
    if not data or not isinstance(data, dict):
        return ""
    return (data.get("default_branch") or "").strip()

def list_workflow_runs(gh: GitHubClient, full_name: str, workflow_id_or_file: str, branch: Optional[str]) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/workflows/{workflow_id_or_file}/runs"
    params = {"branch": branch} if branch else {}
    return list(gh.paginate(url, params=params, item_key="workflow_runs"))

def list_run_jobs(gh: GitHubClient, full_name: str, run_id: int) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/runs/{run_id}/jobs"
    return list(gh.paginate(url, params={}, item_key="jobs"))


# =========================
# Run timing from jobs window
# =========================
def compute_run_window_from_jobs(jobs: List[Dict]) -> Tuple[str, str, Optional[int]]:
    if not jobs:
        return "", "", None
    starts: List[datetime] = []
    ends: List[datetime] = []
    for j in jobs:
        sdt = iso_to_dt(j.get("started_at"))
        edt = iso_to_dt(j.get("completed_at"))
        if sdt:
            starts.append(sdt)
        if edt:
            ends.append(edt)
    if not starts or not ends:
        return "", "", None
    smin = min(starts)
    emax = max(ends)
    return (
        smin.isoformat().replace("+00:00", "Z"),
        emax.isoformat().replace("+00:00", "Z"),
        dt_to_seconds(smin, emax),
    )


# =========================
# Stage-2 runtime heuristics (aligned better with Stage 1)
# =========================
INSTRU_STEP_NAME_RE = re.compile(
    r"("
    r"instrument|"
    r"connected.*androidtest|androidtest|manageddevice|gmd|"
    r"baseline.?profile|"
    r"adb|"
    r"emulator runner|android emulator|avd|"
    r"firebase test|test lab|device farm|"
    r"uiautomator|espresso|"
    r"detox|"
    r"flutter.*(integration|drive)|integration test"
    r")",
    re.IGNORECASE,
)
INSTRU_JOB_NAME_RE = re.compile(
    r"("
    r"instrument|androidtest|connected|manageddevice|gmd|"
    r"baseline|"
    r"emulator|avd|"
    r"firebase|test lab|device farm|"
    r"detox|flutter.*integration"
    r")",
    re.IGNORECASE,
)


# =========================
# Instrumentation metrics from jobs/steps
# =========================
def infer_instru_metrics_from_jobs(
    jobs: List[Dict],
    run_created_at: str,
    run_started_at: str,
    anchor_step_names: Optional[List[str]] = None,
) -> Dict[str, Union[str, int, float, None]]:
    """
    Priority:
      1) Stage-1 anchored runtime step-name match (step_name_anchor)
      2) Regex step-name match (step_regex)
      3) Regex job-name match (job_regex)

    NEW:
      - Captures anchor-job timing for modified TTFTS:
          anchor_job_name
          anchor_job_started_at
          anchor_job_start_source
          time_to_first_instru_from_anchor_job_seconds
          time_to_first_instru_from_anchor_job_quality
    """
    anchor_step_names = anchor_step_names or []

    out = {
        "instru_conclusion": "unknown",
        "instru_detect_method": "none",
        "instru_duration_seconds": None,
        "run_duration_seconds": None,
        "runner_labels_union": "",
        "queue_seconds": None,
        "time_to_first_instru_seconds": None,
        "instru_job_count": 0,
        "instru_step_count": 0,
        "instru_job_names": "",
        "instru_step_names": "",
        "instru_total_seconds": None,
        "instru_window_seconds": None,
        "instru_first_started_at": "",
        "instru_last_completed_at": "",
        "instru_share_of_run": None,

        # NEW anchor-job fields
        "anchor_job_name": "",
        "anchor_job_started_at": "",
        "anchor_job_start_source": "missing",
        "time_to_first_instru_from_anchor_job_seconds": None,
        "time_to_first_instru_from_anchor_job_quality": "missing",
    }

    out["queue_seconds"] = dt_to_seconds(iso_to_dt(run_created_at), iso_to_dt(run_started_at))
    if not jobs:
        return out

    starts, ends = [], []
    labels_union: Set[str] = set()
    for j in jobs:
        sdt = iso_to_dt(j.get("started_at"))
        edt = iso_to_dt(j.get("completed_at"))
        if sdt:
            starts.append(sdt)
        if edt:
            ends.append(edt)
        for lab in (j.get("labels") or []):
            if isinstance(lab, str) and lab.strip():
                labels_union.add(lab.strip())

    run_dur = dt_to_seconds(min(starts) if starts else None, max(ends) if ends else None)
    out["run_duration_seconds"] = run_dur
    out["runner_labels_union"] = ",".join(sorted(labels_union))

    candidates: List[Dict[str, Union[int, str, datetime, None]]] = []
    instru_job_names: List[str] = []
    instru_step_names: List[str] = []

    def add_candidate(
        priority: int,
        method: str,
        conclusion: str,
        dur: Optional[int],
        sdt: Optional[datetime],
        edt: Optional[datetime],
        job_name: str,
        step_name: str,
        job_sdt: Optional[datetime],
        job_earliest_step_sdt: Optional[datetime],
    ) -> None:
        candidates.append({
            "priority": priority,
            "method": method,
            "conclusion": conclusion or "unknown",
            "dur": dur,
            "sdt": sdt,
            "edt": edt,
            "job_name": job_name,
            "step_name": step_name,
            "job_sdt": job_sdt,
            "job_earliest_step_sdt": job_earliest_step_sdt,
        })

    for j in jobs:
        job_name = (j.get("name") or "").strip()
        job_is_instru_regex = bool(INSTRU_JOB_NAME_RE.search(job_name))
        job_sdt = iso_to_dt(j.get("started_at"))
        job_edt = iso_to_dt(j.get("completed_at"))
        job_dur = dt_to_seconds(job_sdt, job_edt)

        steps = j.get("steps") if isinstance(j.get("steps"), list) else []

        # earliest actual step start in this job (fallback if job.started_at missing)
        job_earliest_step_sdt: Optional[datetime] = None
        for stx in steps:
            s0 = iso_to_dt(stx.get("started_at"))
            if s0 and (job_earliest_step_sdt is None or s0 < job_earliest_step_sdt):
                job_earliest_step_sdt = s0

        any_step_candidate_here = False
        for st in steps:
            step_name = (st.get("name") or "").strip()
            if not step_name:
                continue

            is_anchor = anchored_step_match(step_name, anchor_step_names) if anchor_step_names else False
            is_regex = bool(INSTRU_STEP_NAME_RE.search(step_name))

            if not (is_anchor or is_regex):
                continue

            any_step_candidate_here = True
            if job_name:
                instru_job_names.append(job_name)
            instru_step_names.append(step_name)

            st_sdt = iso_to_dt(st.get("started_at")) or job_sdt or job_earliest_step_sdt
            st_edt = iso_to_dt(st.get("completed_at")) or job_edt
            st_dur = dt_to_seconds(iso_to_dt(st.get("started_at")), iso_to_dt(st.get("completed_at")))
            if st_dur is None:
                st_dur = job_dur

            if is_anchor:
                add_candidate(
                    priority=0,
                    method="step_name_anchor",
                    conclusion=(st.get("conclusion") or st.get("status") or "unknown"),
                    dur=st_dur,
                    sdt=st_sdt,
                    edt=st_edt,
                    job_name=job_name,
                    step_name=step_name,
                    job_sdt=job_sdt,
                    job_earliest_step_sdt=job_earliest_step_sdt,
                )
            else:
                add_candidate(
                    priority=1,
                    method="step_regex",
                    conclusion=(st.get("conclusion") or st.get("status") or "unknown"),
                    dur=st_dur,
                    sdt=st_sdt,
                    edt=st_edt,
                    job_name=job_name,
                    step_name=step_name,
                    job_sdt=job_sdt,
                    job_earliest_step_sdt=job_earliest_step_sdt,
                )

        if job_is_instru_regex and not any_step_candidate_here:
            if job_name:
                instru_job_names.append(job_name)
            add_candidate(
                priority=2,
                method="job_regex",
                conclusion=(j.get("conclusion") or j.get("status") or "unknown"),
                dur=job_dur,
                sdt=job_sdt or job_earliest_step_sdt,
                edt=job_edt,
                job_name=job_name,
                step_name="",
                job_sdt=job_sdt,
                job_earliest_step_sdt=job_earliest_step_sdt,
            )

    instru_first_start: Optional[datetime] = None
    instru_last_end: Optional[datetime] = None
    total_seconds = 0
    total_seconds_any = False

    if candidates:
        def _cand_sort_key(c: Dict[str, Union[int, str, datetime, None]]) -> Tuple[int, str]:
            p = int(c.get("priority") or 99)
            sdt = c.get("sdt")
            s = sdt.isoformat() if isinstance(sdt, datetime) else "9999-12-31T23:59:59+00:00"
            return (p, s)

        candidates_sorted = sorted(candidates, key=_cand_sort_key)

        first = candidates_sorted[0]
        out["instru_detect_method"] = str(first.get("method") or "none")
        out["instru_conclusion"] = str(first.get("conclusion") or "unknown")
        out["instru_duration_seconds"] = first.get("dur") if isinstance(first.get("dur"), int) else None

        # NEW: anchor-job timing for modified TTFTS
        anchor_job_name = str(first.get("job_name") or "")
        anchor_step_start = first.get("sdt") if isinstance(first.get("sdt"), datetime) else None
        anchor_job_sdt = first.get("job_sdt") if isinstance(first.get("job_sdt"), datetime) else None
        anchor_job_earliest_step_sdt = first.get("job_earliest_step_sdt") if isinstance(first.get("job_earliest_step_sdt"), datetime) else None

        job_start_basis: Optional[datetime] = None
        job_start_source = "missing"
        quality = "missing"

        if anchor_job_sdt:
            job_start_basis = anchor_job_sdt
            job_start_source = "job_started_at"
            quality = "exact"
        elif anchor_job_earliest_step_sdt:
            job_start_basis = anchor_job_earliest_step_sdt
            job_start_source = "earliest_step_in_job"
            quality = "inferred"

        if anchor_job_name:
            out["anchor_job_name"] = anchor_job_name
        if job_start_basis:
            out["anchor_job_started_at"] = job_start_basis.isoformat().replace("+00:00", "Z")
            out["anchor_job_start_source"] = job_start_source

        if job_start_basis and anchor_step_start:
            out["time_to_first_instru_from_anchor_job_seconds"] = dt_to_seconds(job_start_basis, anchor_step_start)
            out["time_to_first_instru_from_anchor_job_quality"] = quality
        elif anchor_step_start:
            # last-resort if we know the step start but not the job start
            base_run = iso_to_dt(run_started_at) or iso_to_dt(run_created_at)
            if base_run:
                out["time_to_first_instru_from_anchor_job_seconds"] = dt_to_seconds(base_run, anchor_step_start)
                out["time_to_first_instru_from_anchor_job_quality"] = "run_started_fallback"
                out["anchor_job_start_source"] = "run_started_fallback"

        for c in candidates_sorted:
            sdt = c.get("sdt") if isinstance(c.get("sdt"), datetime) else None
            edt = c.get("edt") if isinstance(c.get("edt"), datetime) else None
            dur = c.get("dur") if isinstance(c.get("dur"), int) else None

            if sdt and (instru_first_start is None or sdt < instru_first_start):
                instru_first_start = sdt
            if edt and (instru_last_end is None or edt > instru_last_end):
                instru_last_end = edt

            if dur is not None:
                total_seconds += dur
                total_seconds_any = True

    out["instru_job_names"] = safe_join_names(instru_job_names)
    out["instru_step_names"] = safe_join_names(instru_step_names)
    out["instru_job_count"] = len(unique_preserve(instru_job_names))
    out["instru_step_count"] = len(unique_preserve(instru_step_names))

    if total_seconds_any:
        out["instru_total_seconds"] = total_seconds

    if instru_first_start:
        out["instru_first_started_at"] = instru_first_start.isoformat().replace("+00:00", "Z")
        base_start = iso_to_dt(run_started_at) or iso_to_dt(run_created_at)
        out["time_to_first_instru_seconds"] = dt_to_seconds(base_start, instru_first_start)

    if instru_last_end:
        out["instru_last_completed_at"] = instru_last_end.isoformat().replace("+00:00", "Z")

    out["instru_window_seconds"] = dt_to_seconds(instru_first_start, instru_last_end)

    if out["instru_window_seconds"] is not None and run_dur:
        try:
            out["instru_share_of_run"] = round(float(out["instru_window_seconds"]) / float(run_dur), 6)
        except Exception:
            out["instru_share_of_run"] = None

    return out


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

    tokens = load_github_tokens(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
    gh = GitHubClient(tokens)

    rows = load_verified_workflows(IN_VERIFIED_WORKFLOWS_CSV)
    if PROCESS_ONLY_LOOKS_LIKE_INSTRU:
        rows = [r for r in rows if (r.get("looks_like_instru", "").strip().lower() == "yes")]

    if not rows:
        out_run_fields = ["full_name", "workflow_identifier", "workflow_id", "workflow_path", "run_id", "run_number", "run_attempt"]
        out_style_fields = ["full_name", "workflow_identifier", "workflow_id", "workflow_path", "run_id", "run_number", "run_attempt", "target_style"]
        ensure_csv_header(OUT_RUN_INVENTORY_CSV, out_run_fields)
        ensure_csv_header(OUT_RUN_PER_STYLE_CSV, out_style_fields)
        print("No eligible workflows found for this shard after filtering; wrote header-only Stage 2 outputs.")
        return

    after_dt = iso_to_dt(RUN_CREATED_AT_AFTER) if RUN_CREATED_AT_AFTER else None

    out_fields = [
        # workflow identity (consistent with current Stage 1)
        "full_name",
        "default_branch",
        "workflow_identifier",
        "workflow_id",
        "workflow_path",

        # workflow-level labels from Stage 1 (current schema)
        "looks_like_instru",
        "styles",
        "invocation_types",
        "third_party_provider_name",
        "test_invocation_step_names",

        # NEW: pass-through Stage-1 noise indicator if present
        "jobs_before_anchor_count",

        # NEW: pass-through called-file instrumentation evidence from Stage 1
        "called_instru_signal",
        "called_instru_file_paths",
        "called_instru_origin_refs",
        "called_instru_origin_step_names",
        "called_instru_file_types",

        # run
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

        # legacy metrics (kept)
        "queue_seconds",
        "time_to_first_instru_seconds",
        "instru_conclusion",
        "instru_detect_method",
        "instru_duration_seconds",
        "run_duration_seconds",
        "runner_labels_union",
        "instru_job_count",
        "instru_step_count",
        "instru_job_names",
        "instru_step_names",
        "instru_total_seconds",
        "instru_window_seconds",
        "instru_first_started_at",
        "instru_last_completed_at",
        "instru_share_of_run",

        # NEW: anchor-job timing (modified TTFTS readiness)
        "anchor_job_name",
        "anchor_job_started_at",
        "anchor_job_start_source",
        "time_to_first_instru_from_anchor_job_seconds",
        "time_to_first_instru_from_anchor_job_quality",

        # S2 run timing fallback
        "S2_run_started_at_jobs_min",
        "S2_run_ended_at_jobs_max",
        "S2_run_duration_seconds_jobs_window",
        "S2_run_timing_source",

        # S2 mirrored fallback metrics
        "S2_queue_seconds",
        "S2_time_to_first_instru_seconds",
        "S2_instru_conclusion",
        "S2_instru_detect_method",
        "S2_instru_duration_seconds",
        "S2_run_duration_seconds",
        "S2_runner_labels_union",
        "S2_instru_job_count",
        "S2_instru_step_count",
        "S2_instru_job_names",
        "S2_instru_step_names",
        "S2_instru_total_seconds",
        "S2_instru_window_seconds",
        "S2_instru_first_started_at",
        "S2_instru_last_completed_at",
        "S2_instru_share_of_run",

        # NEW S2 mirrors for modified TTFTS
        "S2_anchor_job_name",
        "S2_anchor_job_started_at",
        "S2_anchor_job_start_source",
        "S2_time_to_first_instru_from_anchor_job_seconds",
        "S2_time_to_first_instru_from_anchor_job_quality",
    ]

    ensure_csv_header(OUT_RUN_INVENTORY_CSV, out_fields)
    existing_run_ids = load_existing_keys(OUT_RUN_INVENTORY_CSV, "run_id")

    default_branch_cache: Dict[str, str] = {}

    wf_iter = rows
    if tqdm is not None:
        wf_iter = tqdm(rows, desc="Stage2: workflows -> runs")

    for wf in wf_iter:
        full_name = (wf.get("full_name") or "").strip()
        workflow_identifier = (wf.get("workflow_identifier") or "").strip()
        workflow_id = (wf.get("workflow_id") or "").strip()
        workflow_path = (wf.get("workflow_path") or "").strip()

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

        anchor_step_names = parse_anchor_step_names(wf.get("test_invocation_step_names") or "")

        for run in runs:
            run_id = str(run.get("id") or "").strip()
            if not run_id or run_id in existing_run_ids:
                continue

            created_at = run.get("created_at") or ""
            if after_dt:
                cdt = iso_to_dt(created_at)
                if cdt and cdt < after_dt:
                    continue

            head_branch = run.get("head_branch") or ""
            if DEFAULT_BRANCH_ONLY and head_branch and head_branch != default_branch:
                continue

            run_started_at = run.get("run_started_at") or ""
            head_sha = run.get("head_sha") or ""

            jobs = list_run_jobs(gh, full_name, int(run_id)) if FETCH_JOBS_FOR_EACH_RUN else []

            metrics = infer_instru_metrics_from_jobs(
                jobs=jobs,
                run_created_at=created_at,
                run_started_at=run_started_at,
                anchor_step_names=anchor_step_names,
            )

            # Jobs-window run timing (S2_)
            s2_run_start, s2_run_end, s2_run_dur = compute_run_window_from_jobs(jobs)
            s2_run_src = "jobs_window" if s2_run_start and s2_run_end else "missing"

            # Broad workflow-label fallback only when no runtime signal exists
            if (
                int(metrics.get("instru_job_count") or 0) == 0
                and int(metrics.get("instru_step_count") or 0) == 0
                and (wf.get("looks_like_instru") or "").strip().lower() == "yes"
            ):
                metrics["instru_detect_method"] = "workflow_label"
                metrics["instru_conclusion"] = (run.get("conclusion") or "unknown")
                if run_started_at:
                    metrics["instru_first_started_at"] = run_started_at
                    metrics["time_to_first_instru_seconds"] = 0

                # Modified TTFTS fallback consistency (explicitly flagged)
                metrics["anchor_job_start_source"] = "run_started_fallback"
                metrics["time_to_first_instru_from_anchor_job_seconds"] = 0
                metrics["time_to_first_instru_from_anchor_job_quality"] = "workflow_label_proxy"
                metrics["anchor_job_started_at"] = run_started_at

            # Build S2_ mirror for Stage 3 fallback
            s2 = {
                "S2_queue_seconds": metrics["queue_seconds"],
                "S2_time_to_first_instru_seconds": metrics["time_to_first_instru_seconds"],
                "S2_instru_conclusion": metrics["instru_conclusion"],
                "S2_instru_detect_method": metrics["instru_detect_method"],
                "S2_instru_duration_seconds": metrics["instru_duration_seconds"],
                "S2_run_duration_seconds": metrics["run_duration_seconds"],
                "S2_runner_labels_union": metrics["runner_labels_union"],
                "S2_instru_job_count": metrics["instru_job_count"],
                "S2_instru_step_count": metrics["instru_step_count"],
                "S2_instru_job_names": metrics["instru_job_names"],
                "S2_instru_step_names": metrics["instru_step_names"],
                "S2_instru_total_seconds": metrics["instru_total_seconds"],
                "S2_instru_window_seconds": metrics["instru_window_seconds"],
                "S2_instru_first_started_at": metrics["instru_first_started_at"],
                "S2_instru_last_completed_at": metrics["instru_last_completed_at"],
                "S2_instru_share_of_run": metrics["instru_share_of_run"],

                # NEW S2 mirrors
                "S2_anchor_job_name": metrics["anchor_job_name"],
                "S2_anchor_job_started_at": metrics["anchor_job_started_at"],
                "S2_anchor_job_start_source": metrics["anchor_job_start_source"],
                "S2_time_to_first_instru_from_anchor_job_seconds": metrics["time_to_first_instru_from_anchor_job_seconds"],
                "S2_time_to_first_instru_from_anchor_job_quality": metrics["time_to_first_instru_from_anchor_job_quality"],
            }

            append_row(OUT_RUN_INVENTORY_CSV, out_fields, {
                # workflow identity
                "full_name": full_name,
                "default_branch": default_branch,
                "workflow_identifier": workflow_identifier,
                "workflow_id": workflow_id,
                "workflow_path": workflow_path,

                # workflow labels from Stage 1
                "looks_like_instru": (wf.get("looks_like_instru") or ""),
                "styles": (wf.get("styles") or ""),
                "invocation_types": (wf.get("invocation_types") or ""),
                "third_party_provider_name": (wf.get("third_party_provider_name") or ""),
                "test_invocation_step_names": (wf.get("test_invocation_step_names") or ""),

                # NEW pass-through field from Stage 1 (if absent, stays blank)
                "jobs_before_anchor_count": (wf.get("jobs_before_anchor_count") or ""),

                # NEW pass-through called-file instrumentation evidence from Stage 1
                "called_instru_signal": (wf.get("called_instru_signal") or ""),
                "called_instru_file_paths": (wf.get("called_instru_file_paths") or ""),
                "called_instru_origin_refs": (wf.get("called_instru_origin_refs") or ""),
                "called_instru_origin_step_names": (wf.get("called_instru_origin_step_names") or ""),
                "called_instru_file_types": (wf.get("called_instru_file_types") or ""),

                # run metadata
                "run_id": run_id,
                "run_number": run.get("run_number") or "",
                "run_attempt": run.get("run_attempt") or "",
                "head_sha": head_sha,
                "created_at": created_at,
                "run_started_at": run_started_at,
                "status": run.get("status") or "",
                "run_conclusion": run.get("conclusion") or "",
                "event": run.get("event") or "",
                "head_branch": head_branch,
                "html_url": run.get("html_url") or "",
                "extracted_at_utc": now_utc_iso(),

                # legacy metrics
                "queue_seconds": "" if metrics["queue_seconds"] is None else str(metrics["queue_seconds"]),
                "time_to_first_instru_seconds": "" if metrics["time_to_first_instru_seconds"] is None else str(metrics["time_to_first_instru_seconds"]),
                "instru_conclusion": metrics["instru_conclusion"],
                "instru_detect_method": metrics["instru_detect_method"],
                "instru_duration_seconds": "" if metrics["instru_duration_seconds"] is None else str(metrics["instru_duration_seconds"]),
                "run_duration_seconds": "" if metrics["run_duration_seconds"] is None else str(metrics["run_duration_seconds"]),
                "runner_labels_union": metrics["runner_labels_union"],
                "instru_job_count": str(metrics["instru_job_count"]),
                "instru_step_count": str(metrics["instru_step_count"]),
                "instru_job_names": metrics["instru_job_names"],
                "instru_step_names": metrics["instru_step_names"],
                "instru_total_seconds": "" if metrics["instru_total_seconds"] is None else str(metrics["instru_total_seconds"]),
                "instru_window_seconds": "" if metrics["instru_window_seconds"] is None else str(metrics["instru_window_seconds"]),
                "instru_first_started_at": metrics["instru_first_started_at"],
                "instru_last_completed_at": metrics["instru_last_completed_at"],
                "instru_share_of_run": "" if metrics["instru_share_of_run"] is None else str(metrics["instru_share_of_run"]),

                # NEW anchor-job timing metrics
                "anchor_job_name": metrics["anchor_job_name"],
                "anchor_job_started_at": metrics["anchor_job_started_at"],
                "anchor_job_start_source": metrics["anchor_job_start_source"],
                "time_to_first_instru_from_anchor_job_seconds": "" if metrics["time_to_first_instru_from_anchor_job_seconds"] is None else str(metrics["time_to_first_instru_from_anchor_job_seconds"]),
                "time_to_first_instru_from_anchor_job_quality": metrics["time_to_first_instru_from_anchor_job_quality"],

                # S2 timing fallback
                "S2_run_started_at_jobs_min": s2_run_start,
                "S2_run_ended_at_jobs_max": s2_run_end,
                "S2_run_duration_seconds_jobs_window": "" if s2_run_dur is None else str(s2_run_dur),
                "S2_run_timing_source": s2_run_src,

                # S2 mirrored metrics
                "S2_queue_seconds": "" if s2["S2_queue_seconds"] is None else str(s2["S2_queue_seconds"]),
                "S2_time_to_first_instru_seconds": "" if s2["S2_time_to_first_instru_seconds"] is None else str(s2["S2_time_to_first_instru_seconds"]),
                "S2_instru_conclusion": s2["S2_instru_conclusion"],
                "S2_instru_detect_method": s2["S2_instru_detect_method"],
                "S2_instru_duration_seconds": "" if s2["S2_instru_duration_seconds"] is None else str(s2["S2_instru_duration_seconds"]),
                "S2_run_duration_seconds": "" if s2["S2_run_duration_seconds"] is None else str(s2["S2_run_duration_seconds"]),
                "S2_runner_labels_union": s2["S2_runner_labels_union"],
                "S2_instru_job_count": "" if s2["S2_instru_job_count"] is None else str(s2["S2_instru_job_count"]),
                "S2_instru_step_count": "" if s2["S2_instru_step_count"] is None else str(s2["S2_instru_step_count"]),
                "S2_instru_job_names": s2["S2_instru_job_names"],
                "S2_instru_step_names": s2["S2_instru_step_names"],
                "S2_instru_total_seconds": "" if s2["S2_instru_total_seconds"] is None else str(s2["S2_instru_total_seconds"]),
                "S2_instru_window_seconds": "" if s2["S2_instru_window_seconds"] is None else str(s2["S2_instru_window_seconds"]),
                "S2_instru_first_started_at": s2["S2_instru_first_started_at"],
                "S2_instru_last_completed_at": s2["S2_instru_last_completed_at"],
                "S2_instru_share_of_run": "" if s2["S2_instru_share_of_run"] is None else str(s2["S2_instru_share_of_run"]),

                # NEW S2 mirrors for modified TTFTS
                "S2_anchor_job_name": s2["S2_anchor_job_name"],
                "S2_anchor_job_started_at": s2["S2_anchor_job_started_at"],
                "S2_anchor_job_start_source": s2["S2_anchor_job_start_source"],
                "S2_time_to_first_instru_from_anchor_job_seconds": "" if s2["S2_time_to_first_instru_from_anchor_job_seconds"] is None else str(s2["S2_time_to_first_instru_from_anchor_job_seconds"]),
                "S2_time_to_first_instru_from_anchor_job_quality": s2["S2_time_to_first_instru_from_anchor_job_quality"],
            })

            existing_run_ids.add(run_id)

        time.sleep(SLEEP_BETWEEN_WORKFLOWS_SEC)

    print("Done.")
    print("Wrote:", OUT_RUN_INVENTORY_CSV)


if __name__ == "__main__":
    main()