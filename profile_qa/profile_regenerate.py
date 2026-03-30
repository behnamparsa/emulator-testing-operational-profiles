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


RULE_SPECS = [
    {
        "objective": "Predictable feedback",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 2.1", "Obs. 2.2"],
        "paper_rationale": "The paper pairs predictability-first guidance with GMD's stability profile and uses Community as the fast-but-variable counterpoint.",
        "latest_recommendation_rule": "Use the current active answer from Obs. 2.1 as the primary recommendation; use Obs. 2.2 to explain the trade-off against the fast-but-variable alternative.",
        "fallback_rule": "If the latest recommendation is not feasible, fall back to the fast-but-variable alternative indicated by Obs. 2.2 when it differs from the recommendation; otherwise keep the current recommended style.",
    },
    {
        "objective": "Fast first signal",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 1.2", "Obs. 1.1"],
        "paper_rationale": "The paper associates fast first signal with GMD's entry advantage, while still distinguishing it from overall completion speed.",
        "latest_recommendation_rule": "Use the current active answer from Obs. 1.2 as the primary recommendation; use Obs. 1.1 to explain whether that style also wins or loses on overall completion.",
        "fallback_rule": "If the latest recommendation is not feasible, fall back to the fastest-overall style from Obs. 1.1 when it differs; otherwise keep the current recommended style.",
    },
    {
        "objective": "Fastest typical end-to-end completion",
        "paper_recommendation": "Community",
        "basis_obs": ["Obs. 1.1", "Obs. 2.1"],
        "paper_rationale": "The paper ties fastest typical completion to Community, while using GMD as the safer trade-off when predictability matters almost as much as speed.",
        "latest_recommendation_rule": "Use the current active answer from Obs. 1.1 as the primary recommendation; use Obs. 2.1 to describe the predictability trade-off.",
        "fallback_rule": "If the latest recommendation is not feasible, fall back to the predictability-first style from Obs. 2.1 when it differs; otherwise keep the current recommended style.",
    },
    {
        "objective": "Usable and successful run outcomes",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 4.2", "Obs. 4.1", "Obs. 4.4"],
        "paper_rationale": "The paper prefers GMD for usable and successful outcomes, while also using verdict- and trigger-conditioned observations to qualify that recommendation.",
        "latest_recommendation_rule": "Use the current active answer from Obs. 4.2 as the primary recommendation; use Obs. 4.1 and Obs. 4.4 to explain usable-verdict and trigger-conditioned context.",
        "fallback_rule": "If the latest recommendation is not feasible, fall back to the strongest usable-verdict style from Obs. 4.1 when it differs; otherwise keep the current recommended style and inspect Obs. 4.4 for trigger-conditioned caveats.",
    },
    {
        "objective": "Overhead-placement-led optimization",
        "paper_recommendation": "GMD",
        "basis_obs": ["Obs. 3.1", "Obs. 3.2", "Obs. 3.3", "Obs. 3.4"],
        "paper_rationale": "The paper's overhead guide is style-by-bottleneck rather than a single global winner; the repo flattens that into a primary recommended style plus a refreshed bottleneck-based lever.",
        "latest_recommendation_rule": "Use the current active answer from Obs. 3.1 as the primary recommendation, but derive the current bottleneck from the latest active overhead observations across Obs. 3.1–3.4.",
        "fallback_rule": "If the latest recommendation is not feasible, fall back to the style whose current overhead observation best matches the same bottleneck family; otherwise keep the current recommended style.",
    },
]


def _build_current_answer_map(rows: List[Dict[str, str]], latest_active: str | None) -> Dict[str, str]:
    current_answers: Dict[str, str] = {}
    for row in rows:
        obs_id = _norm(row.get("obs_id", ""))
        current_answers[obs_id] = _norm(row.get(latest_active, "")) if latest_active else ""
    return current_answers


def _rule_spec_by_objective(objective: str) -> Dict[str, object]:
    for spec in RULE_SPECS:
        if spec["objective"] == objective:
            return spec
    raise KeyError(objective)


def _style_bottleneck_label(style: str, objective: str, current_answers: Dict[str, str]) -> str:
    style = _norm(style)
    objective = _norm(objective)

    if objective == "Usable and successful run outcomes":
        if style in {"Third-Party", "Custom"} and _norm(current_answers.get("Obs. 4.4", "")) == style:
            return "trigger_conditioning"
        return "reliability_outcome"

    if objective == "Fast first signal":
        if style in {"GMD", "Community"}:
            return "entry_setup"
        if style == "Third-Party":
            return "entry_execution"
        return "post_execution_tail"

    if objective == "Fastest typical end-to-end completion":
        if style == "Community":
            return "distributed_overhead" if _norm(current_answers.get("Obs. 3.3", "")) == "Community" else "post_execution_tail"
        if style == "GMD":
            return "execution_path"
        if style == "Third-Party":
            return "entry_execution"
        return "post_execution_tail"

    if objective == "Predictable feedback":
        if style == "GMD":
            return "execution_path"
        if style == "Community":
            return "distributed_overhead"
        if style == "Third-Party":
            return "entry_execution"
        return "mixed_predictability"

    if objective == "Overhead-placement-led optimization":
        if style == "GMD":
            return "execution_path"
        if style == "Third-Party":
            return "entry_execution"
        if style == "Community":
            return "distributed_overhead"
        return "post_execution_tail"

    if style == "GMD":
        return "execution_path"
    if style == "Third-Party":
        return "entry_execution"
    if style == "Community":
        return "distributed_overhead"
    return "post_execution_tail"


IMPROVEMENT_SUGGESTIONS = {
    "Community": {
        "entry_setup": "Reduce entry/setup variability, trim repeated orchestration, and simplify the path into instrumentation execution.",
        "execution_path": "Reduce execution-path cost through test-efficiency improvements, parallelism tuning, and flake reduction.",
        "post_execution_tail": "Inspect residual tail work after execution, especially reporting, artifact handling, and late cleanup steps.",
        "distributed_overhead": "Treat Community as a distributed bottleneck: tighten entry/setup, execution-path cost, and residual tail together rather than optimizing only one phase.",
        "reliability_outcome": "Prioritize stable actionable outcomes by reducing cancellation-prone setup paths and clarifying failure handling in the dominant trigger regime.",
        "trigger_conditioning": "Review trigger-specific workflow branches because Community's operational behavior may differ between push and other event contexts.",
        "mixed_predictability": "Reduce spread across the full path by stabilizing both setup and execution rather than chasing only median speed.",
        "entry_execution": "Shorten the path into execution and remove provisioning or orchestration work that delays the first useful signal.",
    },
    "GMD": {
        "entry_setup": "Keep the fast-entry advantage by minimizing pre-test provisioning churn and avoiding unnecessary environment work before the managed-device path starts.",
        "execution_path": "Optimize the execution path itself: improve test efficiency, reduce flakes, simplify execution, and tune parallelization inside the managed-device workflow.",
        "post_execution_tail": "Inspect any residual completion tail after execution, especially result collection and teardown that should stay small for GMD.",
        "distributed_overhead": "Even if multiple phases contribute, start with the execution path because that remains GMD's clearest controllable bottleneck family.",
        "reliability_outcome": "Preserve the strong outcome profile by focusing on reliable execution and stable environment setup in the trigger regimes where GMD is currently used.",
        "trigger_conditioning": "Review trigger-specific verdict behavior and keep the healthier event regime as the default deployment context when possible.",
        "mixed_predictability": "Reduce outlier execution windows rather than chasing additional median-speed gains.",
        "entry_execution": "Shorten the handoff from environment startup into active execution and keep the managed-device path tightly bounded.",
    },
    "Third-Party": {
        "entry_setup": "Reduce provisioning and provider entry delay before the observable invocation begins.",
        "execution_path": "Shorten provider-side execution cost and remove unnecessary in-window work once the external execution path starts.",
        "post_execution_tail": "Inspect completion tail work only after entry and execution bottlenecks are under control, because they are usually the dominant cost.",
        "distributed_overhead": "Treat Third-Party as a combined entry-plus-execution problem rather than a single tail bottleneck.",
        "reliability_outcome": "Review verdict usability and success behavior first, especially if the current recommendation is driven by outcome rather than speed.",
        "trigger_conditioning": "Treat trigger policy as the first improvement target: Third-Party's outcome behavior is strongly conditioned by event regime, so review schedule vs. push deployment before deeper timing tuning.",
        "mixed_predictability": "Reduce variability in provider queueing, environment allocation, and execution stability before optimizing smaller tail segments.",
        "entry_execution": "Third-Party's current refreshed bottleneck is the heavy entry plus heavy execution path; start with provisioning delay and provider-side execution cost.",
    },
    "Custom": {
        "entry_setup": "Trim bespoke setup steps and remove repository-specific orchestration that delays reaching the instrumentation path.",
        "execution_path": "Simplify custom execution logic and eliminate avoidable in-window work that prolongs the core path.",
        "post_execution_tail": "Reduce bespoke post-execution work, reporting, and cleanup that create Custom's heavy completion tail.",
        "distributed_overhead": "Standardize the custom workflow first, then inspect whether the largest remaining cost sits in entry, execution, or tail work.",
        "reliability_outcome": "Review failure-dominant workflow branches and the operational contexts where Custom is being used before optimizing for raw speed.",
        "trigger_conditioning": "Custom's current refreshed bottleneck is trigger-conditioned behavior; inspect event-specific success patterns and narrow the trigger regimes where it is relied on.",
        "mixed_predictability": "Stabilize the custom workflow end to end because predictability loss can arise from both bespoke setup and long residual tail work.",
        "entry_execution": "Reduce both the path into execution and the execution window itself when Custom behaves like a mixed entry/execution bottleneck.",
    },
}


def _style_suggestion(style: str, bottleneck_label: str) -> str:
    style = _norm(style)
    bottleneck_label = _norm(bottleneck_label)
    return IMPROVEMENT_SUGGESTIONS.get(style, {}).get(
        bottleneck_label,
        "Use the latest refreshed bottleneck for this style as the first optimization target."
    )


def _objective_fallback_note(objective: str, latest_style: str, current_answers: Dict[str, str], bottleneck_label: str) -> str:
    objective = _norm(objective)
    latest_style = _norm(latest_style)
    bottleneck_phrase = bottleneck_label.replace("_", " ")

    def alt_phrase(style: str) -> str:
        alt_bottleneck = _style_bottleneck_label(style, objective, current_answers)
        return alt_bottleneck.replace("_", " ")

    if objective == "Predictable feedback":
        alt = _norm(current_answers.get("Obs. 2.2", ""))
        if alt and alt != latest_style:
            return f"If {latest_style} is not feasible, use {alt} as the practical fallback, but treat it as the higher-variability alternative and focus first on its {alt_phrase(alt)} bottleneck."
    if objective == "Fast first signal":
        alt = _norm(current_answers.get("Obs. 1.1", ""))
        if alt and alt != latest_style:
            return f"If {latest_style} is not feasible, fall back to {alt} as the fastest-overall style, but accept that its first-signal profile differs and focus on its {alt_phrase(alt)} bottleneck."
    if objective == "Fastest typical end-to-end completion":
        alt = _norm(current_answers.get("Obs. 2.1", ""))
        if alt and alt != latest_style:
            return f"If {latest_style} is not feasible or predictability matters nearly as much as speed, use {alt} as the safer fallback and tune its {alt_phrase(alt)} bottleneck first."
    if objective == "Usable and successful run outcomes":
        alt = _norm(current_answers.get("Obs. 4.1", ""))
        if alt and alt != latest_style:
            return f"If {latest_style} is not feasible, use {alt} as the fallback style with the strongest usable-verdict support, and review trigger-conditioned caveats before relying on it."
    if objective == "Overhead-placement-led optimization":
        return f"If {latest_style} is not feasible, choose the style that shows the same bottleneck family in the latest overhead observations and focus on that {bottleneck_phrase} path first."
    return f"If {latest_style} is not feasible, keep the next closest style-level profile in mind and focus on the currently detected {bottleneck_phrase} bottleneck."


def _basis_note(current_answers: Dict[str, str], basis_obs: List[str]) -> str:
    parts = []
    for obs in basis_obs:
        ans = _norm(current_answers.get(obs, ""))
        if ans:
            parts.append(f"{obs} → {ans}")
        else:
            parts.append(f"{obs} → unavailable")
    return "; ".join(parts)


def _latest_rule_recommendation(spec: Dict[str, object], current_answers: Dict[str, str]) -> str:
    primary_obs = spec["basis_obs"][0]
    return _norm(current_answers.get(primary_obs, "")) or _norm(spec.get("paper_recommendation", ""))


def _latest_rule_rationale(objective: str, latest_style: str, current_answers: Dict[str, str], basis_obs: List[str], bottleneck_label: str) -> str:
    basis_note = _basis_note(current_answers, basis_obs)
    if objective == "Overhead-placement-led optimization":
        return f"Latest recommendation follows the refreshed overhead profile, with {latest_style} selected from the current active overhead observation set. Structural basis: {basis_note}. Current bottleneck family: {bottleneck_label.replace('_', ' ')}."
    return f"Latest recommendation follows the current active answer(s) behind this objective. Structural basis: {basis_note}. Current bottleneck family for {latest_style}: {bottleneck_label.replace('_', ' ')}."


def _update_decision_support_table(guide_table_csv: Path, snapshot_col: str, current_answers: Dict[str, str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for spec in RULE_SPECS:
        latest = _latest_rule_recommendation(spec, current_answers)
        bottleneck = _style_bottleneck_label(latest, _norm(spec["objective"]), current_answers)
        rows.append(
            {
                "objective": _norm(spec["objective"]),
                "paper_recommendation": _norm(spec["paper_recommendation"]),
                "latest_snapshot_recommendation": latest,
                "current_bottleneck": bottleneck,
                "paper_rationale": _norm(spec["paper_rationale"]),
                "latest_rationale": _latest_rule_rationale(_norm(spec["objective"]), latest, current_answers, list(spec["basis_obs"]), bottleneck),
                "first_optimization_target": _style_suggestion(latest, bottleneck),
                "fallback_feasibility_note": _objective_fallback_note(_norm(spec["objective"]), latest, current_answers, bottleneck),
                "structural_basis_observations": ", ".join(spec["basis_obs"]),
            }
        )
    _write_csv_rows(guide_table_csv, rows)
    return rows


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
    lines = [
        "# Decision-support guide (profile-derived)",
        "",
        "This guide preserves the paper baseline recommendation, adds the latest refreshed recommendation, and pairs it with the latest refreshed bottleneck and first optimization target.",
        "",
    ]
    for row in guide_rows:
        lines.append(f"## {row['objective']}")
        lines.append("")
        lines.append(f"- Paper baseline recommendation: **{_norm(row.get('paper_recommendation', ''))}**")
        lines.append(f"- Latest snapshot recommendation: **{_norm(row.get('latest_snapshot_recommendation', ''))}**")
        lines.append(f"- Current bottleneck behind the recommendation: {_norm(row.get('current_bottleneck', '')).replace('_', ' ')}")
        lines.append(f"- Why this recommendation: {_norm(row.get('latest_rationale', ''))}")
        lines.append(f"- First optimization target: {_norm(row.get('first_optimization_target', ''))}")
        lines.append(f"- Fallback / feasibility note: {_norm(row.get('fallback_feasibility_note', ''))}")
        lines.append("")
    return "\n".join(lines)


def _make_decision_support_rule_structure_md(current_answers: Dict[str, str]) -> str:
    lines = [
        "# Decision-support rule structure",
        "",
        "This file documents the structural schema used to regenerate the five decision-support rules from the latest refreshed profile.",
        "",
    ]
    for spec in RULE_SPECS:
        objective = _norm(spec['objective'])
        latest = _latest_rule_recommendation(spec, current_answers)
        bottleneck = _style_bottleneck_label(latest, objective, current_answers)
        lines.append(f"## {objective}")
        lines.append("")
        lines.append(f"- Basis observations: {', '.join(spec['basis_obs'])}")
        lines.append(f"- Paper rationale: {_norm(spec['paper_rationale'])}")
        lines.append(f"- Latest recommendation rule: {_norm(spec['latest_recommendation_rule'])}")
        lines.append(f"- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `{bottleneck}` for latest recommendation `{latest}`.")
        lines.append(f"- First optimization target rule: Map (`{latest}`, `{bottleneck}`) to the style-and-bottleneck suggestion dictionary.")
        lines.append(f"- Fallback / feasibility rule: {_norm(spec['fallback_rule'])}")
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
        return "Current data was not sufficient to confirm or replace the previous answer."
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
    styles = sorted({ _norm(r.get("style", "")) for r in main_rows if _norm(r.get("style", "")) }, key=_style_order_key)

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

    guide_rows = _update_decision_support_table(decision_guide_table_csv, snapshot_col, current_answers)
    decision_guide_md.write_text(_make_decision_support_guide_md(guide_rows), encoding="utf-8")
    decision_support_rule_structure_md.write_text(_make_decision_support_rule_structure_md(current_answers), encoding="utf-8")

    logic_rows = []
    for row in rows:
        logic_rows.append(observation_structure_for_obs(_norm(row.get("obs_id", ""))))
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
        profile_sections.append(f"- Released answer: `{_norm(row.get('released_answer', ''))}`")
        if latest_validate:
            profile_sections.append(f"- Latest Layer 1 status: `{_norm(row.get(latest_validate, ''))}`")
        if latest_favored:
            profile_sections.append(f"- Statistically favored answer: `{_norm(row.get(latest_favored, ''))}`")
        if latest_active:
            profile_sections.append(f"- Current active answer: `{_norm(row.get(latest_active, ''))}`")
        profile_sections.append(
            f"- Validation interpretation: {_validation_interpretation(_norm(row.get(latest_validate, '')) if latest_validate else '', _norm(row.get(latest_target, '')) if latest_target else '', _norm(row.get(latest_favored, '')) if latest_favored else '', _norm(row.get(latest_active, '')) if latest_active else '')}"
        )
        profile_sections.append(f"- Measurement structure reference: see `outputs/reports/observation_measurement_structure.md`.")
        profile_sections.append(f"- Technical validation notes: see `outputs/reports/observation_validation_notes.md`.")
        profile_sections.append("")
    profile_md.write_text("\n".join(profile_sections), encoding="utf-8")

    profile_json.write_text(json.dumps({"profile_table": profile_table_rows, "observations": rows}, indent=2), encoding="utf-8")
    rules_json.write_text(json.dumps({"rules": guide_rows}, indent=2), encoding="utf-8")

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
    report_lines.append("- Decision-support rule structure: `outputs/reports/decision_support_rule_structure.md`")
    report_lines.append("- Technical validation notes: `outputs/reports/observation_validation_notes.md`")
    report_lines.append("")
    refresh_report_md.write_text("\n".join(report_lines), encoding="utf-8")
