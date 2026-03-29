from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json
import re


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _write_csv_rows(path: Path, rows: List[Dict[str, str]]) -> None:
    if not rows:
        raise RuntimeError(f"No rows to write: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _latest_column(prefix: str, fieldnames: List[str]) -> str | None:
    matches = [c for c in fieldnames if c.startswith(prefix)]
    return sorted(matches)[-1] if matches else None


def _norm(text: str) -> str:
    return (text or "").strip()


def _obs_number(row: Dict[str, str]) -> str:
    val = _norm(row.get("obs_number", ""))
    if val:
        return val
    obs_id = _norm(row.get("obs_id", ""))
    return obs_id.replace("Obs.", "").strip()


def _strip_leading_obs_number(text: str) -> str:
    text = _norm(text)
    return re.sub(r"^(Obs\.\s*)?\d+\.\d+\s*[-:]\s*", "", text, flags=re.IGNORECASE).strip()


def _obs_question(row: Dict[str, str]) -> str:
    q = _strip_leading_obs_number(_norm(row.get("question", "")))
    if q:
        return q
    t = _strip_leading_obs_number(_norm(row.get("obs_title", "")))
    if t:
        return t
    return _norm(row.get("obs_id", ""))


def _rq_heading(row: Dict[str, str]) -> str:
    rq_id = _norm(row.get("rq_id", ""))
    rq_title = _norm(row.get("rq_title", ""))
    if rq_id and rq_title:
        return f"{rq_id} — {rq_title}"
    if rq_id:
        return rq_id
    if rq_title:
        return rq_title
    return "Uncategorized"


def _style_cell(style: str, current_answers: Dict[str, str]) -> Dict[str, str]:
    style_l = style.lower()

    def ans(obs_id: str, fallback: str = "") -> str:
        return _norm(current_answers.get(obs_id, "")) or fallback

    if style_l == "community":
        speed_parts = []
        if ans("Obs. 1.1").lower() == "community":
            speed_parts.append("Fastest overall completion")
        if ans("Obs. 1.5").lower() == "community":
            speed_parts.append("fast-core / longer-tail profile")
        if not speed_parts:
            speed_parts.append("Competitive fast-path profile")
        speed = "; ".join(speed_parts)
        pred = "Fast but more variable than predictability-first alternatives"
        overhead = "Distributed overhead profile; optimize entry/setup and execution path before residual tail"
        verdict = "Generally usable with healthier success profile; mainly push-associated"
    elif style_l == "gmd":
        speed_parts = []
        if ans("Obs. 1.2").lower() == "gmd":
            speed_parts.append("Clearest fast-entry profile")
        if ans("Obs. 1.1").lower() == "gmd":
            speed_parts.append("Fastest overall completion")
        if not speed_parts:
            speed_parts.append("Stable but not always fastest end-to-end")
        speed = "; ".join(speed_parts)
        pred = "Strongest predictability-first profile"
        overhead = "Execution-centric profile; first optimization target is the main execution path"
        verdict = "Most usable and success-oriented outcomes; mainly schedule-associated"
    elif style_l == "third-party":
        speed = "Slow sustained-execution profile with heavy entry burden"
        pred = "Strongest absolute tail-risk profile"
        overhead = "Heavy entry plus heavy execution; optimize provisioning delay and provider-side execution cost"
        verdict = "Usable verdicts common, but success strongly trigger-conditioned; mainly schedule-associated"
    else:
        speed = "Mixed-speed case: early entry can be competitive, but completion tail is heavy"
        pred = "Mixed and cautious profile"
        overhead = "Tail-heavy mixed case; reduce bespoke post-execution work and standardize orchestration"
        verdict = "Sparse and context-sensitive; low observed success and often pull_request-associated"
    return {
        "Style": style,
        "Speed profile": speed,
        "Predictability": pred,
        "Overhead source & lever": overhead,
        "Verdict & deployment": verdict,
    }


PAPER_BASELINE_RULES = [
    {"objective": "Predictable feedback", "paper_recommendation": "GMD", "basis_obs": "Obs. 2.1"},
    {"objective": "Fast first signal", "paper_recommendation": "GMD", "basis_obs": "Obs. 1.2"},
    {"objective": "Fastest typical end-to-end completion", "paper_recommendation": "Community", "basis_obs": "Obs. 1.1"},
    {"objective": "Usable and successful run outcomes", "paper_recommendation": "GMD", "basis_obs": "Obs. 4.2"},
    {"objective": "Overhead-placement-led optimization", "paper_recommendation": "GMD", "basis_obs": "Obs. 3.1"},
]


def _build_current_answer_map(rows: List[Dict[str, str]], latest_active: str | None) -> Dict[str, str]:
    current_answers: Dict[str, str] = {}
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = _norm(row.get(latest_active, "")) if latest_active else ""
    return current_answers


def _update_decision_support_table(guide_table_csv: Path, snapshot_col: str, current_answers: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for rule in PAPER_BASELINE_RULES:
        rows.append(
            {
                "objective": rule["objective"],
                "paper_recommendation": rule["paper_recommendation"],
                snapshot_col: _norm(current_answers.get(rule["basis_obs"], "")) or rule["paper_recommendation"],
            }
        )
    _write_csv_rows(guide_table_csv, rows)
    return rows


def _make_guide_table_md(rows: List[Dict[str, str]]) -> str:
    headers = list(rows[0].keys())
    lines = [
        "# Decision-support guide table",
        "",
        "|" + "|".join(headers) + "|",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in rows:
        lines.append("|" + "|".join(_norm(row.get(h, "")) for h in headers) + "|")
    lines.append("")
    return "\n".join(lines)


def _make_profile_table_md(table_rows: List[Dict[str, str]]) -> str:
    headers = ["Style", "Speed profile", "Predictability", "Overhead source & lever", "Verdict & deployment"]
    lines = [
        "# Compact operational profile table",
        "",
        "|" + "|".join(headers) + "|",
        "|" + "|".join(["---"] * len(headers)) + "|",
    ]
    for row in table_rows:
        lines.append("|" + "|".join(row[h].replace("\n", " ") for h in headers) + "|")
    lines.append("")
    return "\n".join(lines)


def _make_decision_support_guide_md(guide_rows: List[Dict[str, str]]) -> str:
    headers = list(guide_rows[0].keys())
    latest_snapshot_col = headers[-1]
    lines = [
        "# Decision-support guide (profile-derived)",
        "",
        "The table below preserves the paper baseline recommendation and appends a single updated recommendation from the latest active answer in the refreshed catalog.",
        "",
        "|Primary objective|Paper baseline|Latest snapshot recommendation|",
        "|---|---|---|",
    ]
    for row in guide_rows:
        baseline = _norm(row.get("paper_recommendation", ""))
        latest = _norm(row.get(latest_snapshot_col, ""))
        lines.append(f"|{_norm(row.get('objective',''))}|{baseline}|{latest}|")
    lines.append("")
    return "\n".join(lines)


def _make_narrative_md(current_answers: Dict[str, str]) -> str:
    fastest = _norm(current_answers.get("Obs. 1.1", "")) or "Community"
    fast_entry = _norm(current_answers.get("Obs. 1.2", "")) or "GMD"
    slow_exec = _norm(current_answers.get("Obs. 1.3", "")) or "Third-Party"
    predict = _norm(current_answers.get("Obs. 2.1", "")) or "GMD"
    heavy_entry_exec = _norm(current_answers.get("Obs. 3.2", "")) or "Third-Party"
    distributed = _norm(current_answers.get("Obs. 3.3", "")) or "Community"
    tail_heavy = _norm(current_answers.get("Obs. 3.4", "")) or "Custom"
    usable = _norm(current_answers.get("Obs. 4.1", "")) or "GMD"
    success = _norm(current_answers.get("Obs. 4.2", "")) or "GMD"
    lines = [
        "# Operational performance profile summary (regenerated)",
        "",
        f"**{fastest}** is the current active answer for the fastest overall operational profile question.",
        f"**{fast_entry}** is the current active answer for the fast-entry profile question, while **{slow_exec}** remains the clearest slow sustained-execution profile.",
        f"On predictability, **{predict}** is the current active predictability-first profile.",
        f"For overhead composition, **{distributed}** is the distributed-overhead case, **{heavy_entry_exec}** is the heavy-entry plus heavy-execution case, and **{tail_heavy}** is the tail-heavy mixed case.",
        f"For practice-facing outcomes, **{usable}** is the strongest current active answer for usable-verdict rate, and **{success}** is the strongest current active answer for success among usable outcomes.",
        "",
    ]
    return "\n".join(lines)


def regenerate_from_catalog(
    refreshed_catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
    profile_md: Path = Path("outputs/profiles/operational_profile.md"),
    profile_json: Path = Path("outputs/profiles/operational_profile.json"),
    rules_json: Path = Path("outputs/rules/decision_support_rules.json"),
    refresh_report_md: Path = Path("outputs/reports/latest_refresh_report.md"),
    profile_table_md: Path = Path("outputs/profiles/operational_profile_table.md"),
    profile_table_csv: Path = Path("outputs/profiles/operational_profile_table.csv"),
    profile_narrative_md: Path = Path("outputs/profiles/operational_profile_narrative.md"),
    decision_guide_md: Path = Path("outputs/rules/decision_support_guide.md"),
    decision_guide_table_csv: Path = Path("outputs/rules/decision_support_guide_table.csv"),
) -> None:
    rows = _read_csv_rows(refreshed_catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in refreshed catalog: {refreshed_catalog_csv}")

    for path in [
        profile_md,
        profile_json,
        rules_json,
        refresh_report_md,
        profile_table_md,
        profile_table_csv,
        profile_narrative_md,
        decision_guide_md,
        decision_guide_table_csv,
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(rows[0].keys())
    latest_active = _latest_column("ACTIVE_", fieldnames)
    latest_validate = _latest_column("L1_validate_", fieldnames)
    latest_favored = _latest_column("L1_favored_answer_", fieldnames)
    snapshot_col = latest_active or latest_validate or "latest_snapshot"
    current_answers = _build_current_answer_map(rows, latest_active)

    profile_table_rows = [
        _style_cell(style, current_answers)
        for style in ["Community", "Custom", "GMD", "Third-Party"]
    ]
    _write_csv_rows(profile_table_csv, profile_table_rows)
    profile_table_md.write_text(_make_profile_table_md(profile_table_rows), encoding="utf-8")
    profile_narrative_md.write_text(_make_narrative_md(current_answers), encoding="utf-8")

    guide_rows = _update_decision_support_table(
        decision_guide_table_csv,
        snapshot_col,
        current_answers,
    )
    decision_guide_md.write_text(_make_decision_support_guide_md(guide_rows), encoding="utf-8")

    profile_sections = ["# Refreshed operational profile", ""]
    current_rq = None
    note_col = _latest_column("L1_note_", fieldnames)
    favored_note_col = _latest_column("L1_favored_note_", fieldnames)

    for row in rows:
        rq = _rq_heading(row)
        if rq != current_rq:
            profile_sections.append(f"## {rq}")
            profile_sections.append("")
            current_rq = rq

        profile_sections.append(f"### Obs. {_obs_number(row)} — {_obs_question(row)}")
        profile_sections.append("")
        profile_sections.append(f"- Released answer: `{_norm(row.get('released_answer', ''))}`")

        if latest_validate:
            profile_sections.append(f"- Latest Layer 1 status: `{_norm(row.get(latest_validate, ''))}`")
        if latest_favored:
            profile_sections.append(f"- Statistically favored answer: `{_norm(row.get(latest_favored, ''))}`")
        if latest_active:
            profile_sections.append(f"- Current active answer: `{_norm(row.get(latest_active, ''))}`")
        if note_col:
            profile_sections.append(f"- Validation note: {_norm(row.get(note_col, ''))}")
        if favored_note_col:
            profile_sections.append(f"- Favored-answer note: {_norm(row.get(favored_note_col, ''))}")

        profile_sections.append("")

    profile_md.write_text("\n".join(profile_sections), encoding="utf-8")

    profile_json.write_text(
        json.dumps(
            {
                "profile_table": profile_table_rows,
                "observations": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    rules_json.write_text(
        json.dumps({"rules": guide_rows}, indent=2),
        encoding="utf-8",
    )

    report_lines = [
        "# Latest refresh report",
        "",
        "This report is generated from the single-layer refreshed catalog.",
        f"Latest active-answer column: `{latest_active or 'N/A'}`",
        f"Latest validation column: `{latest_validate or 'N/A'}`",
        f"Latest favored-answer column: `{latest_favored or 'N/A'}`",
        "",
        "## Snapshot-level summary",
        "",
    ]

    passed = 0
    failed = 0
    for row in rows:
        status = _norm(row.get(latest_validate, "")) if latest_validate else ""
        if status == "Passed":
            passed += 1
        elif status == "Failed":
            failed += 1

    report_lines.append(f"- Passed observations: {passed}")
    report_lines.append(f"- Failed observations: {failed}")
    report_lines.append("")
    report_lines.append("## Active answers")
    report_lines.append("")

    for row in rows:
        report_lines.append(f"- {_norm(row.get('obs_id', ''))}: `{_norm(row.get(latest_active, ''))}`")

    report_lines.append("")
    refresh_report_md.write_text("\n".join(report_lines), encoding="utf-8")