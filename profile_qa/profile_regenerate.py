from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json

from .io_utils import read_csv_rows



def _norm(s: object) -> str:
    return str(s or "").strip()



def _latest_column(prefix: str, fieldnames: List[str]) -> str | None:
    matches = sorted([f for f in fieldnames if f.startswith(prefix)])
    return matches[-1] if matches else None



def _rq_heading(row: Dict[str, str]) -> str:
    rq_id = _norm(row.get("rq_id", ""))
    rq_title = _norm(row.get("rq_title", ""))
    return f"{rq_id} - {rq_title}" if rq_id or rq_title else "Ungrouped"



def _obs_number(row: Dict[str, str]) -> str:
    return _norm(row.get("obs_number", ""))



def _obs_question(row: Dict[str, str]) -> str:
    return _norm(row.get("question", "") or row.get("obs_title", ""))



def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    return read_csv_rows(path)



def _write_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)



def _style_cell(style: str, current_answers: Dict[str, str]) -> Dict[str, str]:
    speed = []
    if current_answers.get("Obs. 1.1") == style:
        speed.append("Fastest overall")
    if current_answers.get("Obs. 1.2") == style:
        speed.append("Fast entry")
    if current_answers.get("Obs. 1.3") == style:
        speed.append("Slow sustained execution")
    if current_answers.get("Obs. 1.4") == style:
        speed.append("Mixed speed profile")
    if current_answers.get("Obs. 1.5") == style:
        speed.append("Fast core with longer tail")

    pred = []
    if current_answers.get("Obs. 2.1") == style:
        pred.append("Most predictable")
    if current_answers.get("Obs. 2.2") == style:
        pred.append("Fast but predictability-poor")
    if current_answers.get("Obs. 2.3") == style:
        pred.append("Strong absolute tail risk")
    if current_answers.get("Obs. 2.4") == style:
        pred.append("Mixed/cautious predictability")

    overhead = []
    if current_answers.get("Obs. 3.1") == style:
        overhead.append("Execution-centric")
    if current_answers.get("Obs. 3.2") == style:
        overhead.append("Heavy entry + execution")
    if current_answers.get("Obs. 3.3") == style:
        overhead.append("Distributed overhead")
    if current_answers.get("Obs. 3.4") == style:
        overhead.append("Tail-heavy mixed")

    verdict = []
    if current_answers.get("Obs. 4.1") == style:
        verdict.append("Best usable verdict rate")
    if current_answers.get("Obs. 4.2") == style:
        verdict.append("Best success among usable verdicts")
    if current_answers.get("Obs. 4.4") == style:
        verdict.append("Strongest trigger-conditioned behavior")

    return {
        "Style": style,
        "Speed profile": "; ".join(speed) or "Not the current lead style in speed-focused items",
        "Predictability": "; ".join(pred) or "Not the current lead style in predictability-focused items",
        "Overhead source & lever": "; ".join(overhead) or "Not the current lead style in overhead-focused items",
        "Verdict & deployment": "; ".join(verdict) or "Not the current lead style in verdict/deployment items",
    }


PAPER_BASELINE_RULES = [
    {"objective": "Predictable feedback", "paper_recommendation": "GMD", "basis_obs": "Obs. 2.1"},
    {"objective": "Fast first signal", "paper_recommendation": "GMD", "basis_obs": "Obs. 1.2"},
    {"objective": "Fastest typical end-to-end completion", "paper_recommendation": "Community", "basis_obs": "Obs. 1.1"},
    {"objective": "Usable and successful run outcomes", "paper_recommendation": "GMD", "basis_obs": "Obs. 4.2"},
    {"objective": "Overhead-placement-led optimization", "paper_recommendation": "GMD", "basis_obs": "Obs. 3.1"},
]



def _build_current_answer_map(rows: List[Dict[str, str]], latest_l2: str | None) -> Dict[str, str]:
    current_answers: Dict[str, str] = {}
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = _norm(row.get(latest_l2, "")) if latest_l2 else _norm(row.get("released_answer", ""))
    return current_answers



def _update_decision_support_table(guide_table_csv: Path, snapshot_col: str, current_answers: Dict[str, str]) -> List[Dict[str, str]]:
    if guide_table_csv.exists():
        existing = _read_csv_rows(guide_table_csv)
        by_obj = {row["objective"]: row for row in existing}
        rows = []
        for rule in PAPER_BASELINE_RULES:
            row = by_obj.get(rule["objective"], {}).copy()
            if not row:
                row = {"objective": rule["objective"]}
            row["paper_recommendation"] = _norm(row.get("paper_recommendation", "")) or _norm(row.get("paper_first", "")) or rule["paper_recommendation"]
            row[snapshot_col] = _norm(current_answers.get(rule["basis_obs"], "")) or rule["paper_recommendation"]
            rows.append(row)
    else:
        rows = []
        for rule in PAPER_BASELINE_RULES:
            rows.append({
                "objective": rule["objective"],
                "paper_recommendation": rule["paper_recommendation"],
                snapshot_col: _norm(current_answers.get(rule["basis_obs"], "")) or rule["paper_recommendation"],
            })
    _write_csv_rows(guide_table_csv, rows)
    return rows



def _make_guide_table_md(rows: List[Dict[str, str]]) -> str:
    headers = list(rows[0].keys())
    lines = ["# Decision-support guide table", "", "|" + "|".join(headers) + "|", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        lines.append("|" + "|".join(_norm(row.get(h, "")) for h in headers) + "|")
    lines.append("")
    return "\n".join(lines)



def _make_profile_table_md(table_rows: List[Dict[str, str]]) -> str:
    headers = ["Style", "Speed profile", "Predictability", "Overhead source & lever", "Verdict & deployment"]
    lines = ["# Compact operational profile table", "", "|" + "|".join(headers) + "|", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in table_rows:
        lines.append("|" + "|".join(row[h].replace("\n", " ") for h in headers) + "|")
    lines.append("")
    return "\n".join(lines)



def _make_decision_support_guide_md(guide_rows: List[Dict[str, str]]) -> str:
    snapshot_cols = sorted([k for k in guide_rows[0].keys() if k.startswith('snapshot_')])
    latest_snapshot_col = snapshot_cols[-1] if snapshot_cols else 'paper_recommendation'
    lines = [
        "# Decision-support guide (profile-derived)",
        "",
        "The table below keeps the paper baseline recommendation and appends the latest single recommended style for each objective.",
        "",
        "|Primary objective|Paper baseline|Latest snapshot recommendation|",
        "|---|---|---|",
    ]
    for row in guide_rows:
        lines.append(f"|{_norm(row.get('objective',''))}|{_norm(row.get('paper_recommendation',''))}|{_norm(row.get(latest_snapshot_col,''))}|")
    lines.append("")
    return "\n".join(lines)



def _make_narrative_md(current_answers: Dict[str, str]) -> str:
    lines = [
        "# Operational performance profile summary (regenerated)",
        "",
        f"**{_norm(current_answers.get('Obs. 1.1','Community'))}** is currently the best-supported answer for the fastest overall profile.",
        f"**{_norm(current_answers.get('Obs. 2.1','GMD'))}** is currently the best-supported predictability-first profile.",
        f"**{_norm(current_answers.get('Obs. 3.1','GMD'))}** remains the clearest execution-centric overhead case, while **{_norm(current_answers.get('Obs. 3.3','Community'))}** remains the clearest distributed-overhead case.",
        f"For practice-facing outcomes, **{_norm(current_answers.get('Obs. 4.1','GMD'))}** leads usable-verdict rate and **{_norm(current_answers.get('Obs. 4.2','GMD'))}** leads success among usable verdicts.",
        "",
    ]
    return "\n".join(lines)



def regenerate_from_catalog(
    refreshed_catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
    profile_md: Path = Path("outputs/profiles/operational_profile.md"),
    profile_json: Path = Path("outputs/profiles/operational_profile.json"),
    rules_md: Path = Path("outputs/rules/decision_support_rules.md"),
    rules_json: Path = Path("outputs/rules/decision_support_rules.json"),
    refresh_report_md: Path = Path("outputs/reports/latest_refresh_report.md"),
    profile_table_md: Path = Path("outputs/profiles/operational_profile_table.md"),
    profile_table_csv: Path = Path("outputs/profiles/operational_profile_table.csv"),
    profile_narrative_md: Path = Path("outputs/profiles/operational_profile_narrative.md"),
    decision_guide_md: Path = Path("outputs/rules/decision_support_guide.md"),
    decision_guide_table_csv: Path = Path("outputs/rules/decision_support_guide_table.csv"),
    decision_guide_table_md: Path = Path("outputs/rules/decision_support_guide_table.md"),
) -> None:
    rows = _read_csv_rows(refreshed_catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in refreshed catalog: {refreshed_catalog_csv}")

    fieldnames = list(rows[0].keys())
    latest_l1 = _latest_column("L1_validate_", fieldnames)
    latest_l2 = _latest_column("L2_answer_", fieldnames)
    latest_l1_target = _latest_column("L1_target_answer_", fieldnames)
    latest_l2_used = _latest_column("L2_used_", fieldnames)

    for path in [profile_md, profile_json, rules_md, rules_json, refresh_report_md, profile_table_md, profile_table_csv, profile_narrative_md, decision_guide_md, decision_guide_table_csv, decision_guide_table_md]:
        path.parent.mkdir(parents=True, exist_ok=True)

    profile_lines = ["# Operational profile (regenerated)", ""]
    profile_data = []
    rules_lines = ["# Decision-support rules (regenerated)", ""]
    rules_data = []
    report_lines = ["# Latest refresh report", ""]

    current_answers = _build_current_answer_map(rows, latest_l2)
    current_rq_heading: str | None = None

    for row in rows:
        question = _norm(row.get("question", ""))
        released_answer = _norm(row.get("released_answer", ""))
        l1_value = _norm(row.get(latest_l1, "")) if latest_l1 else ""
        l1_target = _norm(row.get(latest_l1_target, "")) if latest_l1_target else released_answer
        l2_value = _norm(row.get(latest_l2, "")) if latest_l2 else released_answer
        l2_used = _norm(row.get(latest_l2_used, "")) if latest_l2_used else ""

        rq_heading = _rq_heading(row)
        obs_number = _obs_number(row)
        obs_question = _obs_question(row)
        if rq_heading != current_rq_heading:
            profile_lines.extend([f"## {rq_heading}", ""])
            rules_lines.extend([f"## {rq_heading}", ""])
            current_rq_heading = rq_heading

        obs_heading = f"{obs_number} - {obs_question}" if obs_number and obs_question else obs_question or _norm(row.get("obs_id", ""))
        profile_lines.extend([f"### {obs_heading}", "", f"- Released answer: **{released_answer or 'N/A'}**", f"- Layer 1 validated answer: **{l1_target or 'N/A'}**", f"- Latest Layer 1 status: **{l1_value or 'N/A'}**", f"- Current answer: **{l2_value or 'N/A'}**", f"- Layer 2 used: **{l2_used or 'N/A'}**", ""])
        profile_data.append({"rq_id": _norm(row.get("rq_id", "")), "rq_title": _norm(row.get("rq_title", "")), "obs_id": _norm(row.get("obs_id", "")), "obs_number": obs_number, "question": question, "released_answer": released_answer, "latest_layer1_target": l1_target, "latest_layer1_status": l1_value, "current_answer": l2_value, "latest_layer2_used": l2_used})

        rules_lines.extend([f"### {obs_heading}", "", f"- Question: {question or obs_question}", f"- Recommended current answer: **{l2_value or 'N/A'}**", ""])
        rules_data.append({"rq_id": _norm(row.get("rq_id", "")), "rq_title": _norm(row.get("rq_title", "")), "obs_id": _norm(row.get("obs_id", "")), "obs_number": obs_number, "question": question, "current_answer": l2_value})

    styles = ["Community", "Custom", "GMD", "Third-Party"]
    table_rows = [_style_cell(style, current_answers) for style in styles]
    snapshot_col = latest_l2.replace("L2_answer_", "snapshot_") if latest_l2 else "snapshot_current"
    guide_rows = _update_decision_support_table(decision_guide_table_csv, snapshot_col, current_answers)

    report_lines.extend([
        f"- Source catalog: `{refreshed_catalog_csv}`",
        f"- Latest Layer 1 target column: `{latest_l1_target or 'N/A'}`",
        f"- Latest Layer 1 status column: `{latest_l1 or 'N/A'}`",
        f"- Latest Layer 2 answer column: `{latest_l2 or 'N/A'}`",
        f"- Latest Layer 2 used column: `{latest_l2_used or 'N/A'}`",
        f"- Total observations: **{len(rows)}**",
        "",
    ])

    profile_md.write_text("\n".join(profile_lines) + "\n", encoding="utf-8")
    profile_json.write_text(json.dumps(profile_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    rules_md.write_text("\n".join(rules_lines) + "\n", encoding="utf-8")
    rules_json.write_text(json.dumps(rules_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    refresh_report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    profile_table_md.write_text(_make_profile_table_md(table_rows) + "\n", encoding="utf-8")
    profile_narrative_md.write_text(_make_narrative_md(current_answers) + "\n", encoding="utf-8")
    decision_guide_table_md.write_text(_make_guide_table_md(guide_rows) + "\n", encoding="utf-8")
    decision_guide_md.write_text(_make_decision_support_guide_md(guide_rows) + "\n", encoding="utf-8")

    with profile_table_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["Style", "Speed profile", "Predictability", "Overhead source & lever", "Verdict & deployment"])
        writer.writeheader()
        for row in table_rows:
            writer.writerow(row)
