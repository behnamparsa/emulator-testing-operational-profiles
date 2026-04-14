from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import csv

SOURCE_PAPER_CANDIDATES = [
    Path("data/Source_Paper/Emulator_Testing_Paper2_IPM_latest.pdf"),
    Path("data/Source_Paper/Emulator_Testing_Paper2_IPM_latest.PDF"),
    Path("data/Source_Paper/Emulator_Testing_MSR_2026_Modified_RQ3B_Extension.pdf"),
    Path("data/Source_Paper/Emulator_Testing_MSR_2026_Modified_RQ3B_Extension.PDF"),
]

OBSERVATION_DEFS: List[Dict[str, str]] = [
    {"rq_id": "RQ1", "rq_title": "Speed profiling", "obs_id": "Obs. 1.1", "obs_number": "1.1", "obs_title": "Community is the fastest overall operational profile", "question": "Which style is the fastest overall operational profile?", "released_answer": "Community"},
    {"rq_id": "RQ1", "rq_title": "Speed profiling", "obs_id": "Obs. 1.2", "obs_number": "1.2", "obs_title": "GMD reaches the instrumentation path fastest, but does not finish fastest", "question": "Which style shows the clearest fast-entry profile without being the fastest overall finisher?", "released_answer": "GMD"},
    {"rq_id": "RQ1", "rq_title": "Speed profiling", "obs_id": "Obs. 1.3", "obs_number": "1.3", "obs_title": "Third-Party is the slowest sustained-execution profile", "question": "Which style is the slowest sustained-execution profile?", "released_answer": "Third-Party"},
    {"rq_id": "RQ1", "rq_title": "Speed profiling", "obs_id": "Obs. 1.4", "obs_number": "1.4", "obs_title": "Custom shows a mixed speed profile", "question": "Which style shows a mixed speed profile with competitive entry, middling core path, and a long completion tail?", "released_answer": "Custom"},
    {"rq_id": "RQ1", "rq_title": "Speed profiling", "obs_id": "Obs. 1.5", "obs_number": "1.5", "obs_title": "Community’s speed advantage is concentrated in the core execution windows and comes with a longer tail", "question": "Which style combines a fast core execution profile with a longer residual tail?", "released_answer": "Community"},
    {"rq_id": "RQ2", "rq_title": "Predictability and tail risk", "obs_id": "Obs. 2.1", "obs_number": "2.1", "obs_title": "GMD is the most predictable style on the main completion-oriented measures", "question": "Which style is the most predictable on the main completion-oriented measures?", "released_answer": "GMD"},
    {"rq_id": "RQ2", "rq_title": "Predictability and tail risk", "obs_id": "Obs. 2.2", "obs_number": "2.2", "obs_title": "Community is fast in typical terms, but predictability-poor", "question": "Which style is fast in typical terms but predictability-poor?", "released_answer": "Community"},
    {"rq_id": "RQ2", "rq_title": "Predictability and tail risk", "obs_id": "Obs. 2.3", "obs_number": "2.3", "obs_title": "Third-Party carries the strongest absolute tail-risk profile", "question": "Which style carries the strongest absolute tail-risk profile?", "released_answer": "Third-Party"},
    {"rq_id": "RQ2", "rq_title": "Predictability and tail risk", "obs_id": "Obs. 2.4", "obs_number": "2.4", "obs_title": "Custom has a mixed predictability profile and should be interpreted cautiously", "question": "Which style shows a mixed predictability profile that should be interpreted cautiously?", "released_answer": "Custom"},
    {"rq_id": "RQ3", "rq_title": "Overhead composition and actionable levers", "obs_id": "Obs. 3.1", "obs_number": "3.1", "obs_title": "GMD is the clearest execution-centric style", "question": "Which style is the clearest execution-centric overhead profile?", "released_answer": "GMD"},
    {"rq_id": "RQ3", "rq_title": "Overhead composition and actionable levers", "obs_id": "Obs. 3.2", "obs_number": "3.2", "obs_title": "Third-Party is best characterized by heavy entry and heavy execution, not by a dominant completion tail", "question": "Which style is best characterized by heavy entry plus heavy execution rather than a dominant completion tail?", "released_answer": "Third-Party"},
    {"rq_id": "RQ3", "rq_title": "Overhead composition and actionable levers", "obs_id": "Obs. 3.3", "obs_number": "3.3", "obs_title": "Community has a distributed overhead profile rather than a single dominant overhead source", "question": "Which style has a distributed overhead profile rather than a single dominant overhead source?", "released_answer": "Community"},
    {"rq_id": "RQ3", "rq_title": "Overhead composition and actionable levers", "obs_id": "Obs. 3.4", "obs_number": "3.4", "obs_title": "Custom remains a cautious, tail-heavy mixed case", "question": "Which style remains a cautious tail-heavy mixed overhead case?", "released_answer": "Custom"},
    {"rq_id": "RQ4", "rq_title": "Deployment context and run-level verdict usability", "obs_id": "Obs. 4.1", "obs_number": "4.1", "obs_title": "Styles differ in usable run-level verdict rates, although the overall separation is modest", "question": "Which style currently has the strongest usable run-level verdict rate?", "released_answer": "GMD"},
    {"rq_id": "RQ4", "rq_title": "Deployment context and run-level verdict usability", "obs_id": "Obs. 4.2", "obs_number": "4.2", "obs_title": "Among usable verdicts, styles differ sharply in success rate", "question": "Which style currently has the strongest success rate among usable verdicts?", "released_answer": "GMD"},
    {"rq_id": "RQ4", "rq_title": "Deployment context and run-level verdict usability", "obs_id": "Obs. 4.3", "obs_number": "4.3", "obs_title": "GMD is the clearest schedule-triggered deployment profile", "question": "Which style is most strongly associated with schedule-triggered deployment?", "released_answer": "GMD"},
    {"rq_id": "RQ4", "rq_title": "Deployment context and run-level verdict usability", "obs_id": "Obs. 4.4", "obs_number": "4.4", "obs_title": "Third-Party shows the clearest trigger-conditioned outcome behavior", "question": "Which style remains most strongly trigger-conditioned in outcome behavior?", "released_answer": "Third-Party"},
]

def locate_source_paper() -> Path:
    for candidate in SOURCE_PAPER_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not locate source paper PDF.")

def catalog_rows(source_paper_path: Optional[Path] = None) -> List[Dict[str, str]]:
    _ = source_paper_path or locate_source_paper()
    return [dict(row) for row in OBSERVATION_DEFS]

def write_catalog_csv(out_csv: Path, source_paper_path: Optional[Path] = None) -> None:
    rows = catalog_rows(source_paper_path=source_paper_path)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
