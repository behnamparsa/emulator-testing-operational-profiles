# -*- coding: utf-8 -*-
"""
Stage 4 (MIN SIGNATURE, FIXED + STAGE3-ONLY) — Style-agnostic workload signature for normalization

Fixes included:
1) full_name normalization (handle bad/missing columns / URLs / BOM / whitespace)
2) job_count_total_bucket "strange values" (e.g., dates) => stricter numeric parsing + safer source selection
3) Improve runner_os_bucket detection (prefer run/job telemetry if present, else step rows, else YAML runs-on parse)
4) Restore test_suite_size_bucket by extracting junit_cases via bounded artifact parsing (optional but enabled by default)
5) **NEW (critical): Stage 4 now fingerprints ONLY Stage 3 executed runs**
   - Stage 3 condition: instru_job_count > 0 (executed instrumentation evidence)
6) **NEW (recommended): emit BOTH base and full signature hashes**
   - base: OS + jobs + steps  (robust when suite size is unknown)
   - full: OS + jobs + steps + suite size (refines when known)
   - signature_hash column remains the FULL hash for backward compatibility

Signature hash inputs (style-agnostic):
- runner_os_bucket
- job_count_total_bucket
- step_count_total_bucket (executed if available else declared from YAML else unknown)
- test_suite_size_bucket (from parsed junit_cases if available else unknown)

Provenance/support fields included:
- signature_inputs (steps/yaml/artifacts presence)
- step_count_exec + declared + source
- junit_cases + source

Inputs (ROOT_DIR):
- run_metrics_v16_stage3_enhanced.csv
- run_steps_v16_stage3_breakdown.csv (must be CSV; unzip if needed)

Output:
- run_workload_signature_v2_min_fixed.csv
"""

import base64
import csv
import hashlib
import random
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple, Union
import xml.etree.ElementTree as ET

import requests

# =========================
# CONFIG
# =========================
from config.runtime import get_root_dir, get_tokens_env_path, load_github_tokens

TOKENS_ENV_PATH = get_tokens_env_path()
ROOT_DIR = get_root_dir()

IN_RUN_METRICS_CSV = ROOT_DIR / "run_metrics_v16_stage3_enhanced.csv"
IN_RUN_STEPS_CSV   = ROOT_DIR / "run_steps_v16_stage3_breakdown.csv"

OUT_STAGE4_SIGNATURE_CSV = ROOT_DIR / "run_workload_signature_v3.csv"

MAX_TOKENS_TO_USE = 7
CONNECT_TIMEOUT_S = 10
READ_TIMEOUT_S = 90
MAX_RETRIES_PER_REQUEST = 8
BACKOFF_BASE_S = 1.7
BACKOFF_CAP_S = 60
MAX_PAGES_PER_LIST = 2000

# YAML fetching (for declared step counts + runs-on fallback)
FETCH_WORKFLOW_YAML = True
WORKFLOW_YAML_CACHE_MAX = 5000

# Artifact parsing (to get junit_cases)
DOWNLOAD_AND_PARSE_ARTIFACTS = True
MAX_ARTIFACT_ZIP_BYTES = 15 * 1024 * 1024   # 15MB cap
MAX_ARTIFACTS_TO_PARSE = 3
MAX_XMLS_PER_ARTIFACT = 40   # slightly higher than before (still bounded)

# =========================
# Helpers
# =========================
BOM = "\ufeff"
GHA_EXPR_RE = re.compile(r"\${{\s*[^}]+}}", re.MULTILINE)

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def safe_lower(s: str) -> str:
    return (s or "").strip().lower()

def _clean_key(k: str) -> str:
    return (k or "").replace(BOM, "").strip()

def read_csv_rows(path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input CSV: {path}")
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        rdr = csv.DictReader(f)
        raw_fields = rdr.fieldnames or []
        fields = [_clean_key(x) for x in raw_fields]
        rows: List[Dict[str, str]] = []
        for r in rdr:
            row = {}
            for k, v in r.items():
                row[_clean_key(k)] = (v or "")
            rows.append(row)
    return rows, fields

def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def load_tokens_from_env_file(env_path: Optional[Path], max_tokens: int = 3) -> List[str]:
    tokens = load_github_tokens(env_path=env_path, max_tokens=max_tokens)
    if not tokens:
        raise RuntimeError(
            "No GitHub tokens found. Provide All_Tokens.env with GITHUB_TOKEN_1..7 "
            "or set TOKENS_ENV_PATH to its location."
        )
    return tokens

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
            "User-Agent": "stage4-signature-min-fixed/1.2",
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

    def request(self, method: str, url: str, params: Optional[Dict] = None, stream: bool = False) -> Optional[requests.Response]:
        last_status = None
        for attempt in range(1, MAX_RETRIES_PER_REQUEST + 1):
            idx = self._pick_idx()
            st = self.tokens[idx]
            self.session.headers["Authorization"] = f"Bearer {st.token}"
            try:
                resp = self.session.request(method, url, params=params, timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S), stream=stream)
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

            return resp

        print(f"[giveup] {method} {url} after {MAX_RETRIES_PER_REQUEST} tries (last_status={last_status})")
        return None

    def request_json(self, method: str, url: str, params: Optional[Dict] = None) -> Optional[Union[Dict, List]]:
        resp = self.request(method, url, params=params, stream=False)
        if resp is None:
            return None
        try:
            return resp.json()
        except Exception:
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
        resp = gh.request("GET", dl, params=None, stream=False)
        if resp and resp.status_code == 200:
            return resp.text or ""
    return ""

def list_run_artifacts(gh: GitHubClient, full_name: str, run_id: int) -> List[Dict]:
    url = f"https://api.github.com/repos/{full_name}/actions/runs/{run_id}/artifacts"
    return list(gh.paginate(url, params={}, item_key="artifacts"))

def download_artifact_zip(gh: GitHubClient, full_name: str, artifact_id: int) -> Optional[bytes]:
    url = f"https://api.github.com/repos/{full_name}/actions/artifacts/{artifact_id}/zip"
    resp = gh.request("GET", url, params=None, stream=True)
    if resp is None or resp.status_code != 200:
        return None
    data = bytearray()
    try:
        for chunk in resp.iter_content(chunk_size=1024 * 128):
            if not chunk:
                continue
            data.extend(chunk)
            if len(data) > MAX_ARTIFACT_ZIP_BYTES:
                return None
    except Exception:
        return None
    return bytes(data)

# =========================
# Declared step counting (conservative)
# =========================
def count_declared_steps_from_yaml(yaml_text: str) -> Optional[int]:
    """
    Conservative YAML step count:
    - Does NOT expand matrix
    - Does NOT follow reusable workflows
    - Counts only literal list items under "steps:" blocks
    """
    if not yaml_text or not yaml_text.strip():
        return None
    y = sanitize_gha_expr(yaml_text)
    lines = y.splitlines()

    total_steps = 0
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r"^(\s*)steps\s*:\s*$", line)
        if not m:
            i += 1
            continue
        base_indent = len(m.group(1))
        i += 1
        while i < len(lines):
            ln = lines[i]
            if not ln.strip():
                i += 1
                continue
            indent = len(ln) - len(ln.lstrip(" "))
            if indent <= base_indent:
                break
            if re.match(r"^\s*-\s+(name|uses|run)\s*:", ln):
                total_steps += 1
            i += 1
    return total_steps if total_steps > 0 else None

def parse_runs_on_from_yaml(yaml_text: str) -> str:
    """
    Very simple runs-on detector:
    - collects values of `runs-on:` occurrences
    - buckets them to ubuntu/macos/windows/mixed_or_unknown/unknown
    """
    if not yaml_text or not yaml_text.strip():
        return "unknown"
    y = sanitize_gha_expr(yaml_text)
    vals = re.findall(r"(?im)^\s*runs-on\s*:\s*([^\n#]+)", y)
    buckets: Set[str] = set()
    for v in vals:
        v = v.strip().strip('"').strip("'")
        if not v:
            continue
        buckets.add(bucket_runner_os(v))
    if not buckets:
        return "unknown"
    if len(buckets) == 1:
        return list(buckets)[0]
    return "mixed_or_unknown"

# =========================
# JUnit count parsing (bounded)
# =========================
_JUNIT_XML_HINTS = (
    "junit", "test", "tests", "result", "results", "report", "reports",
    "androidtest", "instrumentation", "connected", "surefire", "TEST-"
)

def _try_parse_junit_xml_counts(xml_bytes: bytes) -> int:
    """Return number of testcases from JUnit XML; 0 if not JUnit or cannot parse."""
    try:
        root = ET.fromstring(xml_bytes)
    except Exception:
        return 0
    tag = (root.tag or "").lower()
    if not (tag.endswith("testsuite") or tag.endswith("testsuites")):
        return 0

    if tag.endswith("testsuite"):
        nodes = [root]
    else:
        nodes = list(root.findall(".//testsuite"))

    total = 0
    for n in nodes:
        t = n.attrib.get("tests")
        if t and re.fullmatch(r"\d+", t.strip()):
            total += int(t.strip())
    if total > 0:
        return total

    tcs = root.findall(".//testcase")
    return len(tcs) if tcs else 0

def extract_junit_cases_from_artifacts(gh: GitHubClient, full_name: str, run_id: int) -> Tuple[Optional[int], str]:
    """
    Returns (junit_cases, source):
      source in {artifacts, none}
    """
    artifacts = list_run_artifacts(gh, full_name, run_id) or []
    if not artifacts:
        return None, "none"

    def score_name(name: str) -> int:
        n = safe_lower(name)
        score = 0
        for kw in ["junit", "test-results", "test_results", "test-result",
                   "androidtest", "instrumentation", "connected",
                   "reports", "report", "results", "surefire"]:
            if kw in n:
                score += 3
        return score

    scored = []
    for a in artifacts:
        nm = a.get("name") or ""
        scored.append((score_name(nm), a))
    scored.sort(key=lambda x: x[0], reverse=True)

    parse_list = [(s, a) for (s, a) in scored if s > 0][:MAX_ARTIFACTS_TO_PARSE]
    if not parse_list:
        return None, "none"

    total_cases = 0
    parsed_any = False

    for _, a in parse_list:
        try:
            aid = int(a.get("id"))
        except Exception:
            continue
        zip_bytes = download_artifact_zip(gh, full_name, aid)
        if not zip_bytes:
            continue
        try:
            zf = zipfile.ZipFile(BytesIO(zip_bytes))
            names = zf.namelist()
        except Exception:
            continue

        xmls = []
        for n in names:
            nl = (n or "").lower()
            if not nl.endswith(".xml"):
                continue
            if any(h.lower() in nl for h in _JUNIT_XML_HINTS) or re.search(r"(?i)/TEST-[^/]+\.xml$", n):
                xmls.append(n)

        seen = set()
        xmls2 = []
        for x in xmls:
            if x in seen:
                continue
            seen.add(x)
            xmls2.append(x)

        for xn in xmls2[:MAX_XMLS_PER_ARTIFACT]:
            try:
                raw = zf.read(xn)
            except Exception:
                continue
            if not raw or len(raw) > 2_000_000:
                continue
            c = _try_parse_junit_xml_counts(raw)
            if c > 0:
                total_cases += c
                parsed_any = True

    if parsed_any and total_cases > 0:
        return total_cases, "artifacts"
    return None, "none"

# =========================
# MAIN
# =========================
def main() -> None:
    tokens = load_tokens_from_env_file(TOKENS_ENV_PATH, max_tokens=MAX_TOKENS_TO_USE)
    gh = GitHubClient(tokens)

    run_rows, _ = read_csv_rows(IN_RUN_METRICS_CSV)
    step_rows, _ = read_csv_rows(IN_RUN_STEPS_CSV)

    # Index step rows by (full_name, run_id)
    steps_by_run: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    jobs_by_run: Dict[Tuple[str, str], Set[str]] = {}
    runner_os_by_run: Dict[Tuple[str, str], Set[str]] = {}

    step_runner_keys = ["runner_os", "runs_on", "runner_labels", "runner_name", "os"]
    for s in step_rows:
        fn_raw = first_nonempty(s, ["full_name", "repo_full_name", "repository", "repo", "repo_name"])
        fn = normalize_full_name(fn_raw)
        rid = (s.get("run_id") or "").strip()
        if not fn or not rid:
            continue
        key = (fn, rid)
        steps_by_run.setdefault(key, []).append(s)

        # --- ONLY CHANGE HERE: use stable identity instead of (job_id or job_name) ---
        jid = _job_identity_from_step_row(s)
        if jid:
            jobs_by_run.setdefault(key, set()).add(jid)

        ros_raw = first_nonempty(s, step_runner_keys)
        if ros_raw:
            runner_os_by_run.setdefault(key, set()).add(bucket_runner_os(ros_raw))

    yaml_cache: Dict[Tuple[str, str, str], str] = {}
    out_rows: List[Dict[str, str]] = []

    for r in run_rows:
        instru_job_count = to_int_loose(r.get("instru_job_count", ""), 0)
        if instru_job_count <= 0:
            continue

        fn_raw = first_nonempty(r, ["full_name", "repo_full_name", "repository", "repo", "repo_name", "repo_url", "html_url"])
        full_name = normalize_full_name(fn_raw)
        run_id_s = (r.get("run_id") or "").strip()

        workflow_path = (r.get("workflow_path") or "").strip()
        head_sha = (r.get("head_sha") or "").strip()

        if not full_name or not run_id_s:
            continue

        run_key = (full_name, run_id_s)
        sr_list = steps_by_run.get(run_key, [])
        has_steps_rows = bool(sr_list)

        yaml_text = ""
        has_yaml = False
        step_count_decl = None
        yaml_runner_bucket = "unknown"

        if FETCH_WORKFLOW_YAML and workflow_path and head_sha:
            ck = (full_name, workflow_path, head_sha)
            if ck in yaml_cache:
                yaml_text = yaml_cache[ck]
            else:
                yaml_text = fetch_workflow_yaml(gh, full_name, workflow_path, head_sha)
                if len(yaml_cache) < WORKFLOW_YAML_CACHE_MAX:
                    yaml_cache[ck] = yaml_text
            has_yaml = bool((yaml_text or "").strip())
            if has_yaml:
                step_count_decl = count_declared_steps_from_yaml(yaml_text)
                yaml_runner_bucket = parse_runs_on_from_yaml(yaml_text)

        run_os_raw = first_nonempty(r, ["runner_os", "runs_on", "os", "runner_labels"])
        if run_os_raw:
            runner_os_bucket = bucket_runner_os(run_os_raw)
            runner_os_source = "run_metrics"
        else:
            os_set = runner_os_by_run.get(run_key, set())
            if len(os_set) == 1:
                runner_os_bucket = list(os_set)[0]
                runner_os_source = "steps"
            elif len(os_set) > 1:
                runner_os_bucket = "mixed_or_unknown"
                runner_os_source = "steps"
            else:
                runner_os_bucket = yaml_runner_bucket
                runner_os_source = "yaml" if yaml_runner_bucket != "unknown" else "unknown"

        # job_count_total (parse formatted; else compute from step rows unique jobs)
        job_count_total = None
        job_count_raw = first_nonempty(r, ["job_count_total", "jobs_total", "total_jobs", "jobs_count"])
        job_count_total = parse_int_strict(job_count_raw)

        if job_count_total is None:
            if jobs_by_run.get(run_key):
                job_count_total = len(jobs_by_run[run_key])

        job_count_total_bucket = bucket_job_count(job_count_total)

        step_count_exec = len(sr_list) if has_steps_rows else None
        step_count_exec_bucket = bucket_step_count(step_count_exec) if step_count_exec is not None else "unknown"
        step_count_decl_bucket = bucket_step_count(step_count_decl) if step_count_decl is not None else "unknown"

        if step_count_exec is not None:
            step_count_total_bucket = step_count_exec_bucket
            step_count_source = "executed"
        elif step_count_decl is not None:
            step_count_total_bucket = step_count_decl_bucket
            step_count_source = "declared"
        else:
            step_count_total_bucket = "unknown"
            step_count_source = "unknown"

        junit_cases = None
        junit_source = "none"
        if DOWNLOAD_AND_PARSE_ARTIFACTS:
            try:
                junit_cases, junit_source = extract_junit_cases_from_artifacts(gh, full_name, int(run_id_s))
            except Exception:
                junit_cases, junit_source = (None, "none")
        test_suite_size_bucket = bucket_suite_size(junit_cases)

        signature_inputs_parts = []
        if has_steps_rows:
            signature_inputs_parts.append("steps")
        if has_yaml:
            signature_inputs_parts.append("yaml")
        if junit_source == "artifacts":
            signature_inputs_parts.append("artifacts")
        signature_inputs = "+".join(signature_inputs_parts) if signature_inputs_parts else ""

        sig_basis_base = "\n".join([
            f"runner_os_bucket={runner_os_bucket}",
            f"job_count_total_bucket={job_count_total_bucket}",
            f"step_count_total_bucket={step_count_total_bucket}",
        ])
        signature_hash_base = hashlib.sha256(sig_basis_base.encode("utf-8", errors="ignore")).hexdigest()[:16]

        sig_basis_full = "\n".join([
            f"runner_os_bucket={runner_os_bucket}",
            f"job_count_total_bucket={job_count_total_bucket}",
            f"step_count_total_bucket={step_count_total_bucket}",
            f"test_suite_size_bucket={test_suite_size_bucket}",
        ])
        signature_hash_full = hashlib.sha256(sig_basis_full.encode("utf-8", errors="ignore")).hexdigest()[:16]

        signature_hash = signature_hash_full

        out_rows.append({
            "full_name": full_name,
            "run_id": run_id_s,
            "workflow_identifier": r.get("workflow_identifier", ""),
            "workflow_path": workflow_path,
            "head_sha": head_sha,

            "signature_inputs": signature_inputs,

            "runner_os_bucket": runner_os_bucket,
            "runner_os_source": runner_os_source,

            "job_count_total": str(job_count_total) if job_count_total is not None else "",
            "job_count_total_bucket": job_count_total_bucket,

            "step_count_exec": str(step_count_exec) if step_count_exec is not None else "",
            "step_count_exec_bucket": step_count_exec_bucket,
            "step_count_decl": str(step_count_decl) if step_count_decl is not None else "",
            "step_count_decl_bucket": step_count_decl_bucket,
            "step_count_source": step_count_source,
            "step_count_total_bucket": step_count_total_bucket,

            "junit_cases": str(junit_cases) if junit_cases is not None else "",
            "junit_source": junit_source,
            "test_suite_size_bucket": test_suite_size_bucket,

            "sig_basis_base": sig_basis_base,
            "signature_hash_base": signature_hash_base,
            "sig_basis_full": sig_basis_full,
            "signature_hash_full": signature_hash_full,

            "signature_hash": signature_hash,

            "stage4_extracted_at_utc": now_utc_iso(),
        })

    out_fields = [
        "full_name","run_id","workflow_identifier","workflow_path","head_sha",
        "signature_inputs",
        "runner_os_bucket","runner_os_source",
        "job_count_total","job_count_total_bucket",
        "step_count_exec","step_count_exec_bucket",
        "step_count_decl","step_count_decl_bucket",
        "step_count_source","step_count_total_bucket",
        "junit_cases","junit_source","test_suite_size_bucket",
        "sig_basis_base","signature_hash_base",
        "sig_basis_full","signature_hash_full",
        "signature_hash",
        "stage4_extracted_at_utc",
    ]

    write_csv(OUT_STAGE4_SIGNATURE_CSV, out_fields, out_rows)
    print("[done] Stage 4 signature (Stage3-only, base+full):", OUT_STAGE4_SIGNATURE_CSV)

if __name__ == "__main__":
    main()