from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _latest_column(prefix: str, fieldnames: List[str]) -> str | None:
    matches = [c for c in fieldnames if c.startswith(prefix)]
    return sorted(matches)[-1] if matches else None


def regenerate_from_catalog(
    refreshed_catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
    profile_md: Path = Path("outputs/profiles/operational_profile.md"),
    profile_json: Path = Path("outputs/profiles/operational_profile.json"),
    rules_md: Path = Path("outputs/rules/decision_support_rules.md"),
    rules_json: Path = Path("outputs/rules/decision_support_rules.json"),
    refresh_report_md: Path = Path("outputs/reports/latest_refresh_report.md"),
) -> None:
    rows = _read_csv_rows(refreshed_catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in refreshed catalog: {refreshed_catalog_csv}")

    fieldnames = list(rows[0].keys())
    latest_l1 = _latest_column("L1_validate_", fieldnames)
    latest_l2 = _latest_column("L2_answer_", fieldnames)

    profile_md.parent.mkdir(parents=True, exist_ok=True)
    profile_json.parent.mkdir(parents=True, exist_ok=True)
    rules_md.parent.mkdir(parents=True, exist_ok=True)
    rules_json.parent.mkdir(parents=True, exist_ok=True)
    refresh_report_md.parent.mkdir(parents=True, exist_ok=True)

    profile_lines = ["# Operational profile (regenerated)", ""]
    profile_data = []

    rules_lines = ["# Decision-support rules (regenerated)", ""]
    rules_data = []

    report_lines = ["# Latest refresh report", ""]

    for row in rows:
        question = row.get("question", "")
        released_answer = row.get("released_answer", "")
        l1_value = row.get(latest_l1, "") if latest_l1 else ""
        l2_value = row.get(latest_l2, "") if latest_l2 else ""
        current_answer = l2_value or released_answer

        profile_lines.extend([
            f"## {row.get('obs_id', '')} — {question}",
            "",
            f"- Released answer: **{released_answer}**",
            f"- Latest Layer 1 status: **{l1_value or 'N/A'}**",
            f"- Current answer: **{current_answer or 'N/A'}**",
            "",
        ])

        profile_data.append({
            "rq_id": row.get("rq_id", ""),
            "obs_id": row.get("obs_id", ""),
            "question": question,
            "released_answer": released_answer,
            "latest_layer1_status": l1_value,
            "current_answer": current_answer,
        })

        rules_lines.extend([
            f"## {row.get('obs_id', '')}",
            "",
            f"- Question: {question}",
            f"- Recommended current answer: **{current_answer or 'N/A'}**",
            "",
        ])

        rules_data.append({
            "obs_id": row.get("obs_id", ""),
            "question": question,
            "current_answer": current_answer,
        })

    report_lines.extend([
        f"- Source catalog: `{refreshed_catalog_csv}`",
        f"- Latest Layer 1 column: `{latest_l1 or 'N/A'}`",
        f"- Latest Layer 2 column: `{latest_l2 or 'N/A'}`",
        f"- Total observations: **{len(rows)}**",
        "",
    ])

    profile_md.write_text("\n".join(profile_lines) + "\n", encoding="utf-8")
    profile_json.write_text(json.dumps(profile_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    rules_md.write_text("\n".join(rules_lines) + "\n", encoding="utf-8")
    rules_json.write_text(json.dumps(rules_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    refresh_report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    regenerate_from_catalog()
