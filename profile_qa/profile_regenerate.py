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
    return obs_id.replace("Obs. ", "").strip()


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
            speed_parts.append("fastest overall completion")
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
    {"objective": "Predictable feedback", "paper_first": "GMD", "paper_second": "Community", "basis_first_obs": "Obs. 2.1", "basis_second_obs": "Obs. 1.1"},
    {"objective": "Fast first signal", "paper_first": "GMD", "paper_second": "Community", "basis_first_obs": "Obs. 1.2", "basis_second_obs": "Obs. 1.1"},
    {"objective": "Fastest typical end-to-end completion", "paper_first": "Community", "paper_second": "GMD", "basis_first_obs": "Obs. 1.1", "basis_second_obs": "Obs. 2.1"},
    {"objective": "Usable and successful run outcomes", "paper_first": "GMD", "paper_second": "Community", "basis_first_obs": "Obs. 4.2", "basis_second_obs": "Obs. 1.1"},
    {"objective": "Overhead-placement-led optimization", "paper_first": "GMD", "paper_second": "Community", "basis_first_obs": "Obs. 3.1", "basis_second_obs": "Obs. 3.3"},
]


def _build_current_answer_maps(rows: List[Dict[str, str]], latest_l2: str | None, latest_runner: str | None) -> tuple[Dict[str, str], Dict[str, str]]:
    current_answers: Dict[str, str] = {}
    runner_ups: Dict[str, str] = {}
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = _norm(row.get(latest_l2, "")) if latest_l2 else ""
        runner_ups[obs_id] = _norm(row.get(latest_runner, "")) if latest_runner else ""
    return current_answers, runner_ups


def _resolve_first_second(rule: Dict[str, str], current_answers: Dict[str, str], runner_ups: Dict[str, str]) -> str:
    first_obs = rule["basis_first_obs"]
    second_obs = rule["basis_second_obs"]

    first = _norm(current_answers.get(first_obs, "")) or rule["paper_first"]
    second = _norm(runner_ups.get(first_obs, ""))

    if not second:
        alt = _norm(current_answers.get(second_obs, ""))
        if alt and alt != first:
            second = alt

    if not second:
        second = rule["paper_second"]

    return f"1st: {first} | 2nd: {second}"


def _update_decision_support_table(guide_table_csv: Path, snapshot_col: str, current_answers: Dict[str, str], runner_ups: Dict[str, str]) -> List[Dict[str, str]]:
    if guide_table_csv.exists():
        existing = _read_csv_rows(guide_table_csv)
        by_obj = {row["objective"]: row for row in existing}
        rows = []
        for rule in PAPER_BASELINE_RULES:
            row = by_obj.get(
                rule["objective"],
                {
                    "objective": rule["objective"],
                    "paper_first": rule["paper_first"],
                    "paper_second": rule["paper_second"],
                },
            )
            row[snapshot_col] = _resolve_first_second(rule, current_answers, runner_ups)
            rows.append(row)
    else:
        rows = []
        for rule in PAPER_BASELINE_RULES:
            row = {
                "objective": rule["objective"],
                "paper_first": rule["paper_first"],
                "paper_second": rule["paper_second"],
                snapshot_col: _resolve_first_second(rule, current_answers, runner_ups),
            }
            rows.append(row)

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
        "The table below mirrors the paper-style objective-driven guide. The baseline paper recommendations are preserved, and each new snapshot appends an updated recommendation column based on the latest Layer 2 first/second style detections.",
        "",
        "|Primary objective|Paper baseline|Latest snapshot recommendation|",
        "|---|---|---|",
    ]
    for row in guide_rows:
        baseline = f"1st: {_norm(row.get('paper_first',''))} | 2nd: {_norm(row.get('paper_second',''))}"
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
        f"**{fastest}** is currently the best-supported answer for the fastest overall operational profile question.",
        f"**{fast_entry}** remains the clearest fast-entry style, while **{slow_exec}** remains the clearest slow sustained-execution profile.",
        f"On predictability, **{predict}** remains the strongest predictability-first profile.",
        f"For overhead composition, **{distributed}** remains the distributed-overhead case, **{heavy_entry_exec}** remains the heavy-entry plus heavy-execution case, and **{tail_heavy}** remains the tail-heavy mixed case.",
        f"For practice-facing outcome context, **{usable}** remains the strongest answer for usable-verdict rate, and **{success}** remains the strongest answer for success among usable outcomes.",
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
    latest_runner = _latest_column("L2_runner_up_", fieldnames)

    for path in [
        profile_md,
        profile_json,
        rules_md,
        rules_json,
        refresh_report_md,
        profile_table_md,
        profile_table_csv,
        profile_narrative_md,
        decision_guide_md,
        decision_guide_table_csv,
        decision_guide_table_md,
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)

    profile_lines = ["# Operational profile (regenerated)", ""]
    profile_data = []
    rules_lines = ["# Decision-support rules (regenerated)", ""]
    rules_data = []
    report_lines = ["# Latest refresh report", ""]

    current_answers, runner_ups = _build_current_answer_maps(rows, latest_l2, latest_runner)
    current_rq_heading: str | None = None

    for row in rows:
        question = _norm(row.get("question", ""))
        released_answer = _norm(row.get("released_answer", ""))
        l1_value = _norm(row.get(latest_l1, "")) if latest_l1 else ""
        l2_value = _norm(row.get(latest_l2, "")) if latest_l2 else ""
        runner = _norm(row.get(latest_runner, "")) if latest_runner else ""
        current_answer = l2_value or released_answer

        rq_heading = _rq_heading(row)
        obs_number = _obs_number(row)
        obs_question = _obs_question(row)

        if rq_heading != current_rq_heading:
            profile_lines.extend([f"## {rq_heading}", ""])
            rules_lines.extend([f"## {rq_heading}", ""])
            current_rq_heading = rq_heading

        obs_heading = f"{obs_number} - {obs_question}" if obs_number and obs_question else obs_question or _norm(row.get("obs_id", ""))

        profile_lines.extend(
            [
                f"### {obs_heading}",
                "",
                f"- Released answer: **{released_answer or 'N/A'}**",
                f"- Latest Layer 1 status: **{l1_value or 'N/A'}**",
                f"- Current answer: **{current_answer or 'N/A'}**",
                f"- Current runner-up: **{runner or 'N/A'}**",
                "",
            ]
        )

        profile_data.append(
            {
                "rq_id": _norm(row.get("rq_id", "")),
                "rq_title": _norm(row.get("rq_title", "")),
                "obs_id": _norm(row.get("obs_id", "")),
                "obs_number": obs_number,
                "question": question,
                "released_answer": released_answer,
                "latest_layer1_status": l1_value,
                "current_answer": current_answer,
                "current_runner_up": runner,
            }
        )

        rules_lines.extend(
            [
                f"### {obs_heading}",
                "",
                f"- Question: {question or obs_question}",
                f"- Recommended current answer: **{current_answer or 'N/A'}**",
                f"- Recommended second answer: **{runner or 'N/A'}**",
                "",
            ]
        )

        rules_data.append(
            {
                "rq_id": _norm(row.get("rq_id", "")),
                "rq_title": _norm(row.get("rq_title", "")),
                "obs_id": _norm(row.get("obs_id", "")),
                "obs_number": obs_number,
                "question": question,
                "current_answer": current_answer,
                "current_runner_up": runner,
            }
        )

    styles = ["Community", "Custom", "GMD", "Third-Party"]
    table_rows = [_style_cell(style, current_answers) for style in styles]

    snapshot_col = latest_l2.replace("L2_answer_", "snapshot_") if latest_l2 else "snapshot_current"
    guide_rows = _update_decision_support_table(decision_guide_table_csv, snapshot_col, current_answers, runner_ups)

    report_lines.extend(
        [
            f"- Source catalog: `{refreshed_catalog_csv}`",
            f"- Latest Layer 1 column: `{latest_l1 or 'N/A'}`",
            f"- Latest Layer 2 column: `{latest_l2 or 'N/A'}`",
            f"- Latest Layer 2 runner-up column: `{latest_runner or 'N/A'}`",
            f"- Total observations: **{len(rows)}**",
            f"- Generated compact profile table: `{profile_table_md}`",
            f"- Generated decision guide: `{decision_guide_md}`",
            f"- Generated decision guide table: `{decision_guide_table_csv}`",
            "",
        ]
    )

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
        writer = csv.DictWriter(
            f,
            fieldnames=["Style", "Speed profile", "Predictability", "Overhead source & lever", "Verdict & deployment"],
        )
        writer.writeheader()
        for row in table_rows:
            writer.writerow(row)
