from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json
import re


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


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
    # Removes prefixes like:
    # 1.1 -
    # 1.1:
    # Obs. 1.1 -
    # Obs. 1.1:
    text = re.sub(r"^(Obs\.\s*)?\d+\.\d+\s*[-:]\s*", "", text, flags=re.IGNORECASE)
    return text.strip()


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

    else:  # Custom
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


def _make_decision_support_guide_md(current_answers: Dict[str, str]) -> str:
    predictable = _norm(current_answers.get("Obs. 2.1", "")) or "GMD"
    fastest = _norm(current_answers.get("Obs. 1.1", "")) or "Community"
    fast_entry = _norm(current_answers.get("Obs. 1.2", "")) or "GMD"
    usable = _norm(current_answers.get("Obs. 4.1", "")) or "GMD"
    success = _norm(current_answers.get("Obs. 4.2", "")) or "GMD"
    exec_centric = _norm(current_answers.get("Obs. 3.1", "")) or "GMD"
    distributed = _norm(current_answers.get("Obs. 3.3", "")) or "Community"
    heavy_entry_exec = _norm(current_answers.get("Obs. 3.2", "")) or "Third-Party"
    tail_heavy = _norm(current_answers.get("Obs. 3.4", "")) or "Custom"

    lines = [
        "# Decision-support guide (profile-derived)",
        "",
        "## If the primary objective is predictable feedback",
        f"- Prefer **{predictable}** when feasible.",
        f"- If {predictable} is not feasible, use **Community** as the practical fallback and treat it as the higher-tail-risk option.",
        "",
        "## If the primary objective is fast first signal",
        f"- Prefer **{fast_entry}** when the earliest entry into the instrumentation path matters most.",
        f"- For fastest typical end-to-end completion, prefer **{fastest}**.",
        "",
        "## If the objective includes usable and successful run outcomes",
        f"- Prefer **{usable}** for usable-verdict rate and **{success}** for success among usable outcomes when those remain aligned.",
        "",
        "## If the dominant problem is overhead placement rather than median speed",
        f"- Treat **{exec_centric}** as the execution-centric case: optimize the main execution path.",
        f"- Treat **{distributed}** as the distributed-overhead case: inspect entry, execution, and residual tail together.",
        f"- Treat **{heavy_entry_exec}** as the heavy-entry plus heavy-execution case: optimize provisioning/orchestration and provider-side execution cost.",
        f"- Treat **{tail_heavy}** as the tail-heavy mixed case: reduce post-execution cleanup/reporting and bespoke orchestration.",
        "",
    ]
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
) -> None:
    rows = _read_csv_rows(refreshed_catalog_csv)
    if not rows:
        raise RuntimeError(f"No rows found in refreshed catalog: {refreshed_catalog_csv}")

    fieldnames = list(rows[0].keys())
    latest_l1 = _latest_column("L1_validate_", fieldnames)
    latest_l2 = _latest_column("L2_answer_", fieldnames)

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
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)

    profile_lines = ["# Operational profile (regenerated)", ""]
    profile_data = []

    rules_lines = ["# Decision-support rules (regenerated)", ""]
    rules_data = []

    report_lines = ["# Latest refresh report", ""]

    current_answers: Dict[str, str] = {}
    current_rq_heading: str | None = None

    for row in rows:
        question = _norm(row.get("question", ""))
        released_answer = _norm(row.get("released_answer", ""))
        l1_value = _norm(row.get(latest_l1, "")) if latest_l1 else ""
        l2_value = _norm(row.get(latest_l2, "")) if latest_l2 else ""
        current_answer = l2_value or released_answer

        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = current_answer

        rq_heading = _rq_heading(row)
        obs_number = _obs_number(row)
        obs_question = _obs_question(row)

        if rq_heading != current_rq_heading:
            profile_lines.extend([
                f"## {rq_heading}",
                "",
            ])
            rules_lines.extend([
                f"## {rq_heading}",
                "",
            ])
            current_rq_heading = rq_heading

        obs_heading = f"{obs_number} - {obs_question}" if obs_number and obs_question else obs_question or obs_id

        profile_lines.extend([
            f"### {obs_heading}",
            "",
            f"- Released answer: **{released_answer or 'N/A'}**",
            f"- Latest Layer 1 status: **{l1_value or 'N/A'}**",
            f"- Current answer: **{current_answer or 'N/A'}**",
            "",
        ])

        profile_data.append({
            "rq_id": _norm(row.get("rq_id", "")),
            "rq_title": _norm(row.get("rq_title", "")),
            "obs_id": obs_id,
            "obs_number": obs_number,
            "question": question,
            "released_answer": released_answer,
            "latest_layer1_status": l1_value,
            "current_answer": current_answer,
        })

        rules_lines.extend([
            f"### {obs_heading}",
            "",
            f"- Question: {question or obs_question}",
            f"- Recommended current answer: **{current_answer or 'N/A'}**",
            "",
        ])

        rules_data.append({
            "rq_id": _norm(row.get("rq_id", "")),
            "rq_title": _norm(row.get("rq_title", "")),
            "obs_id": obs_id,
            "obs_number": obs_number,
            "question": question,
            "current_answer": current_answer,
        })

    styles = ["Community", "Custom", "GMD", "Third-Party"]
    table_rows = [_style_cell(style, current_answers) for style in styles]

    report_lines.extend([
        f"- Source catalog: `{refreshed_catalog_csv}`",
        f"- Latest Layer 1 column: `{latest_l1 or 'N/A'}`",
        f"- Latest Layer 2 column: `{latest_l2 or 'N/A'}`",
        f"- Total observations: **{len(rows)}**",
        f"- Generated compact profile table: `{profile_table_md}`",
        f"- Generated decision guide: `{decision_guide_md}`",
        "",
    ])

    profile_md.write_text("\n".join(profile_lines) + "\n", encoding="utf-8")
    profile_json.write_text(json.dumps(profile_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    rules_md.write_text("\n".join(rules_lines) + "\n", encoding="utf-8")
    rules_json.write_text(json.dumps(rules_data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    refresh_report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    profile_table_md.write_text(_make_profile_table_md(table_rows) + "\n", encoding="utf-8")
    profile_narrative_md.write_text(_make_narrative_md(current_answers) + "\n", encoding="utf-8")
    decision_guide_md.write_text(_make_decision_support_guide_md(current_answers) + "\n", encoding="utf-8")

    with profile_table_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Style", "Speed profile", "Predictability", "Overhead source & lever", "Verdict & deployment"],
        )
        writer.writeheader()
        for row in table_rows:
            writer.writerow(row)


if __name__ == "__main__":
    regenerate_from_catalog()