from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List, Tuple

from config.runtime import get_root_dir

ROOT_DIR = get_root_dir()

IN_STAGE1 = ROOT_DIR / "verified_workflows.csv"
IN_STAGE2 = ROOT_DIR / "run_inventory.csv"
IN_STAGE3A = ROOT_DIR / "run_metrics_v16_stage3_enhanced.csv"
IN_STAGE3C = ROOT_DIR / "run_per_style_v1_stage3.csv"
IN_STAGE4 = ROOT_DIR / "fingerprint_output_v5.csv"
OUT_MAIN = ROOT_DIR / "MainDataset.csv"

BOM = "\ufeff"


def _clean_key(k: str) -> str:
    return (k or "").replace(BOM, "").strip()


def read_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        return [], []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.DictReader(f)
        fieldnames = [_clean_key(k) for k in (rdr.fieldnames or [])]
        rows = []
        for row in rdr:
            clean = {}
            for k, v in row.items():
                clean[_clean_key(k)] = v
            rows.append(clean)
        return fieldnames, rows


def write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def low(s: str) -> str:
    return (s or "").strip().lower()


def norm(s: str) -> str:
    return (s or "").strip()


def canon_key(s: str) -> str:
    s = low(s).replace("_", " ").replace("-", " ")
    return " ".join(s.split())


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


def normalize_style_label(s: str) -> str:
    return STYLE_ALIASES.get(canon_key(s), norm(s))


def split_styles(s: str) -> List[str]:
    raw = norm(s)
    if not raw:
        return []
    out = []
    seen = set()
    for p in __import__("re").split(r"[|,;/]+", raw):
        pp = normalize_style_label(p)
        if pp and pp not in seen:
            seen.add(pp)
            out.append(pp)
    return out


def join_styles(styles: List[str]) -> str:
    return "|".join(styles)


def make_run_key(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        norm(row.get("repo_full_name")),
        norm(row.get("workflow_run_id")),
        norm(row.get("attempt")),
    )


def make_style_key(row: Dict[str, str]) -> Tuple[str, str, str, str]:
    return (
        norm(row.get("repo_full_name")),
        norm(row.get("workflow_run_id")),
        norm(row.get("attempt")),
        normalize_style_label(row.get("style") or row.get("Inferred_Label") or row.get("styles") or ""),
    )


def merge_preferring_right(left: Dict[str, object], right: Dict[str, object]) -> Dict[str, object]:
    out = dict(left)
    for k, v in right.items():
        if v not in (None, ""):
            out[k] = v
        elif k not in out:
            out[k] = v
    return out


def main() -> None:
    _, s1_rows = read_csv_rows(IN_STAGE1)
    _, s2_rows = read_csv_rows(IN_STAGE2)
    _, s3a_rows = read_csv_rows(IN_STAGE3A)
    _, s3c_rows = read_csv_rows(IN_STAGE3C)
    _, s4_rows = read_csv_rows(IN_STAGE4)

    s1_by_repo: Dict[str, Dict[str, str]] = {}
    for r in s1_rows:
        repo = norm(r.get("repo_full_name"))
        if repo and repo not in s1_by_repo:
            s1_by_repo[repo] = r

    s2_by_run: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for r in s2_rows:
        s2_by_run[make_run_key(r)] = r

    s3a_by_run: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    for r in s3a_rows:
        s3a_by_run[make_run_key(r)] = r

    s4_by_style: Dict[Tuple[str, str, str, str], Dict[str, str]] = {}
    for r in s4_rows:
        s4_by_style[make_style_key(r)] = r

    run_per_style_rows: List[Dict[str, object]] = []

    for r in s3c_rows:
        repo = norm(r.get("repo_full_name"))
        run_key = make_run_key(r)
        style = normalize_style_label(r.get("style") or "")
        style_key = (run_key[0], run_key[1], run_key[2], style)

        merged: Dict[str, object] = {}
        if repo in s1_by_repo:
            merged = merge_preferring_right(merged, s1_by_repo[repo])
        if run_key in s2_by_run:
            merged = merge_preferring_right(merged, s2_by_run[run_key])
        if run_key in s3a_by_run:
            merged = merge_preferring_right(merged, s3a_by_run[run_key])
        merged = merge_preferring_right(merged, r)
        if style_key in s4_by_style:
            merged = merge_preferring_right(merged, s4_by_style[style_key])

        merged["style"] = style
        inferred_label = merged.get("Inferred_Label") or merged.get("styles") or merged.get("style") or ""
        merged["Inferred_Label"] = join_styles(split_styles(inferred_label))
        merged["styles"] = merged["Inferred_Label"]
        run_per_style_rows.append(merged)

    fieldnames = []
    seen = set()
    preferred_front = [
        "repo_full_name",
        "workflow_id",
        "workflow_name",
        "workflow_run_id",
        "attempt",
        "created_at",
        "run_started_at",
        "updated_at",
        "style",
        "Inferred_Label",
        "styles",
        "time_to_first_instru_from_run_seconds",
        "instru_duration_seconds",
        "core_instru_window_seconds",
        "instru_exec_window_seconds",
        "signature_hash_base",
        "Base",
        "Robust",
    ]

    for k in preferred_front:
        if k not in seen:
            seen.add(k)
            fieldnames.append(k)

    for row in run_per_style_rows:
        for k in row.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    write_csv(OUT_MAIN, fieldnames, run_per_style_rows)
    print(f"[build_total_dataset] wrote: {OUT_MAIN}")


if __name__ == "__main__":
    main()