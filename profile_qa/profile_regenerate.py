from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import csv
import json
import re

from .item_logic import observation_structure_for_obs


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


RULE_OBJECTIVES = [
    {
        "rule_id": "predictable_feedback",
        "objective": "Predictable feedback",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 2.1", "Obs. 2.2"],
        "paper_rationale": "The paper positions GMD as the predictability-first profile and Community as the faster but more variable alternative.",
        "latest_rule": "Prefer the current active answer from Obs. 2.1; use Obs. 2.2 as supporting context for the speed-versus-variability trade-off.",
    },
    {
        "rule_id": "fast_first_signal",
        "objective": "Fast first signal",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 1.2"],
        "paper_rationale": "The paper treats GMD as the clearest fast-entry style under the speed profile.",
        "latest_rule": "Use the current active answer from Obs. 1.2.",
    },
    {
        "rule_id": "fastest_typical_completion",
        "objective": "Fastest typical end-to-end completion",
        "paper_recommendation": "Community",
        "basis_obs": ["Obs. 1.1"],
        "paper_rationale": "The paper treats Community as the fastest overall completion profile.",
        "latest_rule": "Use the current active answer from Obs. 1.1.",
    },
    {
        "rule_id": "usable_successful_outcomes",
        "objective": "Usable and successful run outcomes",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 4.1", "Obs. 4.2", "Obs. 4.4"],
        "paper_rationale": "The paper combines usable-verdict rate, success rate among usable outcomes, and trigger-conditioned outcome context.",
        "latest_rule": "Prefer the current active answer from Obs. 4.2, use Obs. 4.1 as supporting context, and use Obs. 4.4 to identify trigger-conditioned reliability issues.",
    },
    {
        "rule_id": "overhead_led_optimization",
        "objective": "Overhead-placement-led optimization",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 3.1", "Obs. 3.2", "Obs. 3.3", "Obs. 3.4"],
        "paper_rationale": "The paper maps styles to dominant overhead placements and then translates those placements into first optimization targets.",
        "latest_rule": "Use the current active answer across the RQ3 observations to identify the dominant style-level bottleneck driving the optimization recommendation.",
    },
]

STYLE_DEFAULT_BOTTLENECK = {
    "Community": "distributed_overhead",
    "GMD": "execution_path",
    "Third-Party": "entry_setup",
    "Custom": "post_execution_tail",
}


def _build_current_answer_map(rows: List[Dict[str, str]], latest_active: str | None) -> Dict[str, str]:
    current_answers: Dict[str, str] = {}
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = _norm(row.get(latest_active, "")) if latest_active else ""
    return current_answers


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


def _validation_interpretation(status: str, target: str, favored: str, active: str) -> str:
    status = _norm(status)
    target = _norm(target)
    favored = _norm(favored)
    active = _norm(active)
    if status == "Passed":
        if favored and target and favored != target:
            return f"Current data shows {favored} as the nominally favored answer, but the evidence was not strong enough to replace the current answer {target}."
        return f"Current data still supports {active or target} as the answer for this observation."
    if status == "Failed":
        if active and favored and active == favored:
            return f"Current data no longer supported {target}; the answer was updated to {active}."
        return f"Current data no longer supported the previous answer {target}."
    if status == "Insufficient evidence":
        return f"Current data was not sufficient to replace the current answer {active or target}."
    return "Validation status requires manual interpretation."


def _make_observation_logic_md(rows: List[Dict[str, str]]) -> str:
    lines = [
        "# Observation measurement structure",
        "",
        "This file documents the normalized repo-side measurement structure used to automate observation validation and answer refresh.",
        "",
    ]
    for row in rows:
        if not row:
            continue
        lines.append(f"## {row['obs_id']}")
        lines.append("")
        lines.append(f"- Paper intent: {row.get('paper_intent', '')}")
        lines.append(f"- Primary measurement: {row.get('primary_measurement', '')}")
        lines.append(f"- Fallback measurement: {row.get('fallback_measurement', '')}")
        lines.append(f"- Winner rule: {row.get('winner_rule', '')}")
        lines.append(f"- Validation rule: {row.get('validation_rule', '')}")
        lines.append(f"- Technical note: {row.get('technical_note', '')}")
        lines.append("")
    return "\n".join(lines)


def _make_validation_notes_md(rows: List[Dict[str, str]], latest_cols: Dict[str, str | None]) -> str:
    latest_target = latest_cols.get("target")
    latest_validate = latest_cols.get("validate")
    latest_note = latest_cols.get("note")
    latest_favored = latest_cols.get("favored")
    latest_favored_note = latest_cols.get("favored_note")
    latest_active = latest_cols.get("active")

    lines = [
        "# Observation validation notes",
        "",
        "This file keeps the technical validation notes and favored-answer notes separate from the main operational profile.",
        "",
    ]
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        lines.append(f"## {obs_id} — {_obs_question(row)}")
        lines.append("")
        if latest_target:
            lines.append(f"- Current baseline under validation: `{_norm(row.get(latest_target, ''))}`")
        if latest_validate:
            lines.append(f"- Validation status: `{_norm(row.get(latest_validate, ''))}`")
        if latest_favored:
            lines.append(f"- Favored answer: `{_norm(row.get(latest_favored, ''))}`")
        if latest_active:
            lines.append(f"- Active answer: `{_norm(row.get(latest_active, ''))}`")
        if latest_note:
            lines.append(f"- Validation note: {_norm(row.get(latest_note, ''))}")
        if latest_favored_note:
            lines.append(f"- Favored-answer note: {_norm(row.get(latest_favored_note, ''))}")
        lines.append("")
    return "\n".join(lines)


def _style_order_key(style: str) -> tuple[int, str]:
    order = {"Community": 0, "Custom": 1, "GMD": 2, "Third-Party": 3}
    return (order.get(style, 999), style)


def _is_truthy(value: str) -> bool:
    return _norm(value).lower() in {"1", "true", "yes", "y"}


def _make_coverage_snapshot_md(main_rows: List[Dict[str, str]]) -> str:
    styles = sorted({_norm(r.get("style", "")) for r in main_rows if _norm(r.get("style", ""))}, key=_style_order_key)

    def is_base(row: Dict[str, str]) -> bool:
        return _is_truthy(row.get("Base", ""))

    def is_layer2(row: Dict[str, str]) -> bool:
        return _norm(row.get("study_invocation_execution_window_selected_stage3_source", "")) != ""

    full_rows = [r for r in main_rows if _norm(r.get("style", "")) in styles]
    base_rows = [r for r in full_rows if is_base(r)]
    layer2_rows = [r for r in base_rows if is_layer2(r)]

    full_total = len(full_rows)
    base_total = len(base_rows)
    layer2_total = len(layer2_rows)

    def pct(n: int, d: int) -> str:
        return f"{(100.0 * n / d):.2f}%" if d else "N/A"

    lines = [
        "# Coverage snapshot",
        "",
        "This report summarizes the current processed-dataset coverage in the same spirit as the paper's coverage snapshot, with an added four-style breakdown.",
        "",
        "## Overall coverage",
        "",
        f"- Four-style analysis dataset: **{full_total}** executed run×style records.",
        f"- Base controlled subset: **{base_total}** records (`Base = True`, first-attempt usable-verdict records).",
        f"- Layer 1 coverage: effectively **{full_total}/{full_total} (100.00%)** on the four-style dataset because Layer 1 is derived from run/job telemetry.",
        f"- Layer 2 observable within Base: **{layer2_total}/{base_total} ({pct(layer2_total, base_total)})**.",
        "",
        "## Breakdown by style",
        "",
        "|Style|Four-style dataset|Base subset|Base as % of style total|Layer 2 observable in Base|Layer 2 coverage within Base|",
        "|---|---:|---:|---:|---:|---:|",
    ]

    for style in styles:
        full_n = sum(1 for r in full_rows if _norm(r.get("style", "")) == style)
        base_n = sum(1 for r in base_rows if _norm(r.get("style", "")) == style)
        layer2_n = sum(1 for r in layer2_rows if _norm(r.get("style", "")) == style)
        lines.append(f"|{style}|{full_n}|{base_n}|{pct(base_n, full_n)}|{layer2_n}|{pct(layer2_n, base_n)}|")

    lines.extend([
        "",
        "## Interpretation",
        "",
        "- The four-style dataset counts all executed run×style records currently represented in `MainDataset.csv` for Community, Custom, GMD, and Third-Party.",
        "- The Base subset matches the repo's controlled timing-comparison regime (`Base = True`).",
        "- Layer 2 observability is counted using the presence of the selected Stage 3 invocation-window telemetry source, which is the practical repo-side indicator that the step-level timing decomposition is available.",
        "",
    ])
    return "\n".join(lines)


def _load_guidance_rows(path: Path) -> List[Dict[str, str]]:
    return _read_csv_rows(path)


def _detect_rule_recommendation(rule: Dict[str, object], current_answers: Dict[str, str]) -> str:
    rule_id = rule["rule_id"]
    if rule_id == "predictable_feedback":
        return _norm(current_answers.get("Obs. 2.1", "")) or rule["paper_recommendation"]
    if rule_id == "fast_first_signal":
        return _norm(current_answers.get("Obs. 1.2", "")) or rule["paper_recommendation"]
    if rule_id == "fastest_typical_completion":
        return _norm(current_answers.get("Obs. 1.1", "")) or rule["paper_recommendation"]
    if rule_id == "usable_successful_outcomes":
        return _norm(current_answers.get("Obs. 4.2", "")) or _norm(current_answers.get("Obs. 4.1", "")) or rule["paper_recommendation"]
    if rule_id == "overhead_led_optimization":
        for obs in ["Obs. 3.1", "Obs. 3.2", "Obs. 3.3", "Obs. 3.4"]:
            val = _norm(current_answers.get(obs, ""))
            if val:
                return val
        return rule["paper_recommendation"]
    return rule["paper_recommendation"]


def _detect_bottleneck(rule_id: str, style: str, current_answers: Dict[str, str]) -> str:
    style = _norm(style)
    if rule_id == "predictable_feedback":
        if style == "GMD":
            return "execution_path"
        if style == "Community":
            return "distributed_overhead"
        if style == "Third-Party":
            return "execution_path"
        if style == "Custom":
            return "mixed_profile"
    if rule_id == "fast_first_signal":
        return "entry_setup"
    if rule_id == "fastest_typical_completion":
        if style == "Community":
            return "distributed_overhead"
        if style == "GMD":
            return "execution_path"
        if style == "Third-Party":
            return "execution_path"
        if style == "Custom":
            return "post_execution_tail"
    if rule_id == "usable_successful_outcomes":
        if _norm(current_answers.get("Obs. 4.4", "")) == style:
            return "trigger_context"
        return "reliability_outcome"
    if rule_id == "overhead_led_optimization":
        if _norm(current_answers.get("Obs. 3.1", "")) == style:
            return "execution_path"
        if _norm(current_answers.get("Obs. 3.2", "")) == style:
            return "entry_setup"
        if _norm(current_answers.get("Obs. 3.3", "")) == style:
            return "distributed_overhead"
        if _norm(current_answers.get("Obs. 3.4", "")) == style:
            return "post_execution_tail"
    return STYLE_DEFAULT_BOTTLENECK.get(style, "mixed_profile")


def _lookup_guidance(guidance_rows: List[Dict[str, str]], rule_id: str, style: str, bottleneck: str) -> Dict[str, str]:
    candidates = [
        (rule_id, style, bottleneck),
        ("*", style, bottleneck),
        (rule_id, style, "*"),
        ("*", style, "*"),
    ]
    for r, s, b in candidates:
        for row in guidance_rows:
            if _norm(row.get("rule_id", "")) == r and _norm(row.get("style", "")) == s and _norm(row.get("bottleneck", "")) == b:
                return row
    return {
        "guidance_title": "General improvement focus",
        "guidance_text": "Use the style's dominant current bottleneck as the first improvement focus.",
        "literature_basis": "Profile-derived default",
    }


def _build_rule_rows(current_answers: Dict[str, str], snapshot_col: str, guidance_rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for rule in RULE_OBJECTIVES:
        latest_style = _detect_rule_recommendation(rule, current_answers)
        bottleneck = _detect_bottleneck(rule["rule_id"], latest_style, current_answers)
        guidance = _lookup_guidance(guidance_rows, rule["rule_id"], latest_style, bottleneck)
        rows.append(
            {
                "rule_id": rule["rule_id"],
                "objective": rule["objective"],
                "paper_recommendation": rule["paper_recommendation"],
                "latest_snapshot_recommendation": latest_style,
                "latest_bottleneck": bottleneck,
                "paper_rationale": rule["paper_rationale"],
                "latest_rationale": f"Latest recommendation comes from {', '.join(rule['basis_obs'])} using the current active answers in the refreshed catalog.",
                "first_optimization_target": _norm(guidance.get("guidance_title", "")),
                "improvement_suggestion": _norm(guidance.get("guidance_text", "")),
                "basis_observations": ", ".join(rule["basis_obs"]),
                "literature_basis": _norm(guidance.get("literature_basis", "")),
                "snapshot_column": snapshot_col,
            }
        )
    return rows


def _make_decision_support_guide_md(rule_rows: List[Dict[str, str]]) -> str:
    lines = [
        "# Decision-support guide (profile-derived)",
        "",
        "The guidance below preserves the paper baseline recommendation and appends a refreshed recommendation from the latest active answers, together with the currently detected bottleneck and the first improvement focus for that rule–style combination.",
        "",
    ]

    for row in rule_rows:
        lines.append(f"## If the primary objective is {row['objective'].lower()}")
        lines.append("")
        lines.append(f"- Paper baseline recommendation: **{row['paper_recommendation']}**")
        lines.append(f"- Latest snapshot recommendation: **{row['latest_snapshot_recommendation']}**")
        lines.append(f"- Why this recommendation: {row['latest_rationale']}")
        lines.append(f"- Current bottleneck emphasis: **{row['latest_bottleneck']}**")
        lines.append(f"- First optimization target: **{row['first_optimization_target']}**")
        lines.append(f"- Improvement suggestion: {row['improvement_suggestion']}")
        lines.append("")

    return "\n".join(lines)


def _make_decision_support_rule_structure_md() -> str:
    lines = [
        "# Decision-support rule structure",
        "",
        "This file documents the structural logic used by the repo to refresh the five decision-support rules.",
        "",
        "Priority notation used below:",
        "- `A` = primary recommendation source",
        "- `A > B` = use `A` first; use `B` only if `A` is unavailable",
        "- `A + B (support)` = `A` determines the recommendation; `B` is supporting context for interpretation",
        "- `A > B + C (support)` = use `A` first, then `B` as fallback; use `C` only as supporting context",
        "",
    ]

    structures = {
        "predictable_feedback": {
            "priority": "**Obs. 2.1 + Obs. 2.2 (support)**",
            "primary": "**Obs. 2.1**",
            "fallback": "",
            "support": "**Obs. 2.2**",
            "latest_rule_text": "Use the current active answer from **Obs. 2.1** as the latest recommendation. Use **Obs. 2.2** only to explain the speed-versus-variability trade-off; it does not override the recommendation on its own.",
        },
        "fast_first_signal": {
            "priority": "**Obs. 1.2**",
            "primary": "**Obs. 1.2**",
            "fallback": "",
            "support": "",
            "latest_rule_text": "Use the current active answer from **Obs. 1.2**.",
        },
        "fastest_typical_completion": {
            "priority": "**Obs. 1.1**",
            "primary": "**Obs. 1.1**",
            "fallback": "",
            "support": "",
            "latest_rule_text": "Use the current active answer from **Obs. 1.1**.",
        },
        "usable_successful_outcomes": {
            "priority": "**Obs. 4.2 > Obs. 4.1 + Obs. 4.4 (support)**",
            "primary": "**Obs. 4.2**",
            "fallback": "**Obs. 4.1**",
            "support": "**Obs. 4.4**",
            "latest_rule_text": "Prefer the current active answer from **Obs. 4.2**. If **Obs. 4.2** is unavailable, use **Obs. 4.1** as the fallback recommendation source. Use **Obs. 4.4** only as supporting context to describe trigger-conditioned reliability issues; it does not override the recommendation on its own.",
        },
        "overhead_led_optimization": {
            "priority": "**Obs. 3.1 > Obs. 3.2 > Obs. 3.3 > Obs. 3.4**",
            "primary": "**Obs. 3.1**",
            "fallback": "**Obs. 3.2**, then **Obs. 3.3**, then **Obs. 3.4**",
            "support": "",
            "latest_rule_text": "Use the first non-empty current active answer in this order: **Obs. 3.1**, then **Obs. 3.2**, then **Obs. 3.3**, then **Obs. 3.4**. Earlier observations in the sequence have higher priority than later ones.",
        },
    }

    for rule in RULE_OBJECTIVES:
        s = structures[rule["rule_id"]]
        lines.append(f"## {rule['objective']}")
        lines.append("")
        lines.append(f"- Priority structure: {s['priority']}")
        lines.append(f"- Primary recommendation source: {s['primary']}")
        if s["fallback"]:
            lines.append(f"- Fallback recommendation source{'s' if ', then ' in s['fallback'] else ''}: {s['fallback']}")
        if s["support"]:
            lines.append(f"- Supporting context: {s['support']}")
        lines.append(f"- Paper rationale: {rule['paper_rationale']}")
        lines.append(f"- Latest recommendation rule: {s['latest_rule_text']}")
        lines.append("- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.")
        lines.append("- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.")
        lines.append("")

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
    validation_notes_md: Path = Path("outputs/reports/observation_validation_notes.md"),
    measurement_structure_md: Path = Path("outputs/reports/observation_measurement_structure.md"),
    coverage_snapshot_md: Path = Path("outputs/reports/coverage_snapshot.md"),
    decision_support_rule_structure_md: Path = Path("outputs/reports/decision_support_rule_structure.md"),
    guidance_csv: Path = Path("config/rule_style_bottleneck_guidance.csv"),
    main_dataset_csv: Path = Path("data/processed/MainDataset.csv"),
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
        validation_notes_md,
        measurement_structure_md,
        coverage_snapshot_md,
        decision_support_rule_structure_md,
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = list(rows[0].keys())
    latest_active = _latest_column("ACTIVE_", fieldnames)
    latest_validate = _latest_column("L1_validate_", fieldnames)
    latest_favored = _latest_column("L1_favored_answer_", fieldnames)
    latest_target = _latest_column("L1_target_answer_", fieldnames)
    latest_note = _latest_column("L1_note_", fieldnames)
    latest_favored_note = _latest_column("L1_favored_note_", fieldnames)
    snapshot_col = latest_active or latest_validate or "latest_snapshot"
    current_answers = _build_current_answer_map(rows, latest_active)

    profile_table_rows = [_style_cell(style, current_answers) for style in ["Community", "Custom", "GMD", "Third-Party"]]
    _write_csv_rows(profile_table_csv, profile_table_rows)
    profile_table_md.write_text(_make_profile_table_md(profile_table_rows), encoding="utf-8")
    profile_narrative_md.write_text(_make_narrative_md(current_answers), encoding="utf-8")

    guidance_rows = _load_guidance_rows(guidance_csv)
    rule_rows = _build_rule_rows(current_answers, snapshot_col, guidance_rows)
    table_rows = [
        {
            "objective": r["objective"],
            "paper_recommendation": r["paper_recommendation"],
            "latest_snapshot_recommendation": r["latest_snapshot_recommendation"],
            "latest_bottleneck": r["latest_bottleneck"],
            "first_optimization_target": r["first_optimization_target"],
            "improvement_suggestion": r["improvement_suggestion"],
            "basis_observations": r["basis_observations"],
        }
        for r in rule_rows
    ]
    _write_csv_rows(decision_guide_table_csv, table_rows)
    decision_guide_md.write_text(_make_decision_support_guide_md(rule_rows), encoding="utf-8")
    decision_support_rule_structure_md.write_text(_make_decision_support_rule_structure_md(), encoding="utf-8")

    logic_rows = [observation_structure_for_obs(_norm(row.get("obs_id", ""))) for row in rows]
    measurement_structure_md.write_text(_make_observation_logic_md(logic_rows), encoding="utf-8")

    main_rows = _read_csv_rows(main_dataset_csv)
    coverage_snapshot_md.write_text(_make_coverage_snapshot_md(main_rows), encoding="utf-8")

    validation_notes_md.write_text(
        _make_validation_notes_md(
            rows,
            {
                "target": latest_target,
                "validate": latest_validate,
                "note": latest_note,
                "favored": latest_favored,
                "favored_note": latest_favored_note,
                "active": latest_active,
            },
        ),
        encoding="utf-8",
    )

    profile_sections = ["# Refreshed operational profile", ""]
    current_rq = None

    for row in rows:
        rq = _rq_heading(row)
        if rq != current_rq:
            profile_sections.append(f"## {rq}")
            profile_sections.append("")
            current_rq = rq
        profile_sections.append(f"### Obs. {_obs_number(row)} — {_obs_question(row)}")
        profile_sections.append("")
        profile_sections.append(f"- Paper baseline answer: `{_norm(row.get('released_answer', ''))}`")
        if latest_target:
            profile_sections.append(f"- Current baseline under validation: `{_norm(row.get(latest_target, ''))}`")
        if latest_validate:
            profile_sections.append(f"- Latest Layer 1 status: `{_norm(row.get(latest_validate, ''))}`")
        if latest_favored:
            profile_sections.append(f"- Statistically favored answer: `{_norm(row.get(latest_favored, ''))}`")
        if latest_active:
            profile_sections.append(f"- Current active answer: `{_norm(row.get(latest_active, ''))}`")
        profile_sections.append(f"- Validation interpretation: {_validation_interpretation(_norm(row.get(latest_validate, '')) if latest_validate else '', _norm(row.get(latest_target, '')) if latest_target else '', _norm(row.get(latest_favored, '')) if latest_favored else '', _norm(row.get(latest_active, '')) if latest_active else '')}")
        profile_sections.append("- Measurement structure reference: see `outputs/reports/observation_measurement_structure.md`.")
        profile_sections.append("- Technical validation notes: see `outputs/reports/observation_validation_notes.md`.")
        profile_sections.append("")
    profile_md.write_text("\n".join(profile_sections), encoding="utf-8")

    profile_json.write_text(json.dumps({"profile_table": profile_table_rows, "observations": rows}, indent=2), encoding="utf-8")
    rules_json.write_text(json.dumps({"rules": rule_rows}, indent=2), encoding="utf-8")

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
        report_lines.append(f"- {_norm(row.get('obs_id',''))}: `{_norm(row.get(latest_active, ''))}`")
    report_lines.append("")
    report_lines.append("## Companion references")
    report_lines.append("")
    report_lines.append("- Observation measurement structure: `outputs/reports/observation_measurement_structure.md`")
    report_lines.append("- Coverage snapshot: `outputs/reports/coverage_snapshot.md`")
    report_lines.append("- Technical validation notes: `outputs/reports/observation_validation_notes.md`")
    report_lines.append("- Decision-support rule structure: `outputs/reports/decision_support_rule_structure.md`")
    report_lines.append("")
    refresh_report_md.write_text("\n".join(report_lines), encoding="utf-8")
