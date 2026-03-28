from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
import csv
import re

try:
    from pypdf import PdfReader
    _IMPORT_ERROR = None
except Exception as exc:  # pragma: no cover
    PdfReader = None
    _IMPORT_ERROR = exc


SOURCE_PAPER_CANDIDATES = [
    Path("data/Source_Paper/Emulator_Testing_MSR_2026_Modified_RQ3B_Extension.pdf"),
    Path("data/Source_Paper/Emulator_Testing_MSR_2026_Modified_RQ3B_Extension.PDF"),
]


OBSERVATION_DEFS: List[Dict[str, str]] = [
    {
        "rq_id": "RQ1",
        "rq_title": "Speed profiling",
        "obs_id": "Obs. 1.1",
        "obs_number": "1.1",
        "obs_title": "Community is the fastest overall operational profile",
        "question": "Which style is the fastest overall operational profile?",
        "released_answer": "Community",
        "test_scope": "RQ1",
        "primary_metric": "Run duration; instrumentation job envelope; invocation execution window",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ1",
        "rq_title": "Speed profiling",
        "obs_id": "Obs. 1.2",
        "obs_number": "1.2",
        "obs_title": "GMD reaches the instrumentation path fastest, but does not finish fastest",
        "question": "Which style shows the clearest fast-entry profile without being the fastest overall finisher?",
        "released_answer": "GMD",
        "test_scope": "RQ1",
        "primary_metric": "Time to instrumentation envelope; pre-invocation; run duration",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ1",
        "rq_title": "Speed profiling",
        "obs_id": "Obs. 1.3",
        "obs_number": "1.3",
        "obs_title": "Third-Party is the slowest sustained-execution profile",
        "question": "Which style is the slowest sustained-execution profile?",
        "released_answer": "Third-Party",
        "test_scope": "RQ1",
        "primary_metric": "Run duration; pre-invocation; invocation execution window",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ1",
        "rq_title": "Speed profiling",
        "obs_id": "Obs. 1.4",
        "obs_number": "1.4",
        "obs_title": "Custom shows a mixed speed profile",
        "question": "Which style shows a mixed speed profile with competitive entry, middling core path, and a long completion tail?",
        "released_answer": "Custom",
        "test_scope": "RQ1",
        "primary_metric": "Time to instrumentation envelope; pre-invocation; invocation execution window; post-invocation",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ1",
        "rq_title": "Speed profiling",
        "obs_id": "Obs. 1.5",
        "obs_number": "1.5",
        "obs_title": "Community’s speed advantage is concentrated in the core execution windows and comes with a longer tail",
        "question": "Which style combines a fast core execution profile with a longer residual tail?",
        "released_answer": "Community",
        "test_scope": "RQ1",
        "primary_metric": "Instrumentation job envelope; invocation execution window; post-invocation",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ2",
        "rq_title": "Predictability and tail risk",
        "obs_id": "Obs. 2.1",
        "obs_number": "2.1",
        "obs_title": "GMD is the most predictable style on the main completion-oriented measures",
        "question": "Which style is the most predictable on the main completion-oriented measures?",
        "released_answer": "GMD",
        "test_scope": "RQ2",
        "primary_metric": "Predictability summaries for run duration and completion-oriented components",
        "statistical_test_plan": "Non-parametric comparison of normalized dispersion/tail summaries with pairwise follow-up",
    },
    {
        "rq_id": "RQ2",
        "rq_title": "Predictability and tail risk",
        "obs_id": "Obs. 2.2",
        "obs_number": "2.2",
        "obs_title": "Community is fast in typical terms, but predictability-poor",
        "question": "Which style is fast in typical terms but predictability-poor?",
        "released_answer": "Community",
        "test_scope": "RQ2",
        "primary_metric": "Predictability summaries for run duration and completion-oriented components",
        "statistical_test_plan": "Non-parametric comparison of normalized dispersion/tail summaries with pairwise follow-up",
    },
    {
        "rq_id": "RQ2",
        "rq_title": "Predictability and tail risk",
        "obs_id": "Obs. 2.3",
        "obs_number": "2.3",
        "obs_title": "Third-Party carries the strongest absolute tail-risk profile",
        "question": "Which style carries the strongest absolute tail-risk profile?",
        "released_answer": "Third-Party",
        "test_scope": "RQ2",
        "primary_metric": "Absolute completion/tail behavior across main timing measures",
        "statistical_test_plan": "Tail summary comparison with non-parametric pairwise interpretation",
    },
    {
        "rq_id": "RQ2",
        "rq_title": "Predictability and tail risk",
        "obs_id": "Obs. 2.4",
        "obs_number": "2.4",
        "obs_title": "Custom has a mixed predictability profile and should be interpreted cautiously",
        "question": "Which style shows a mixed predictability profile that should be interpreted cautiously?",
        "released_answer": "Custom",
        "test_scope": "RQ2",
        "primary_metric": "Layer 1 and Layer 2 predictability summaries",
        "statistical_test_plan": "Non-parametric comparison of normalized dispersion/tail summaries with cautious interpretation",
    },
    {
        "rq_id": "RQ3",
        "rq_title": "Overhead composition and actionable levers",
        "obs_id": "Obs. 3.1",
        "obs_number": "3.1",
        "obs_title": "GMD is the clearest execution-centric style",
        "question": "Which style is the clearest execution-centric overhead profile?",
        "released_answer": "GMD",
        "test_scope": "RQ3",
        "primary_metric": "Invocation execution window share; post-invocation share",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ3",
        "rq_title": "Overhead composition and actionable levers",
        "obs_id": "Obs. 3.2",
        "obs_number": "3.2",
        "obs_title": "Third-Party is best characterized by heavy entry and heavy execution, not by a dominant completion tail",
        "question": "Which style is best characterized by heavy entry plus heavy execution rather than a dominant completion tail?",
        "released_answer": "Third-Party",
        "test_scope": "RQ3",
        "primary_metric": "Pre-invocation share; invocation execution window share; post-invocation share",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ3",
        "rq_title": "Overhead composition and actionable levers",
        "obs_id": "Obs. 3.3",
        "obs_number": "3.3",
        "obs_title": "Community has a distributed overhead profile rather than a single dominant overhead source",
        "question": "Which style has a distributed overhead profile rather than a single dominant overhead source?",
        "released_answer": "Community",
        "test_scope": "RQ3",
        "primary_metric": "Pre-invocation share; invocation execution window share; post-invocation share",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ3",
        "rq_title": "Overhead composition and actionable levers",
        "obs_id": "Obs. 3.4",
        "obs_number": "3.4",
        "obs_title": "Custom remains a cautious, tail-heavy mixed case",
        "question": "Which style remains a cautious tail-heavy mixed overhead case?",
        "released_answer": "Custom",
        "test_scope": "RQ3",
        "primary_metric": "Post-invocation share; execution-window share",
        "statistical_test_plan": "Kruskal–Wallis + Mann–Whitney with Holm correction + Cliff's delta",
    },
    {
        "rq_id": "RQ4",
        "rq_title": "Deployment context and run-level verdict usability",
        "obs_id": "Obs. 4.1",
        "obs_number": "4.1",
        "obs_title": "Styles differ in usable run-level verdict rates, although the overall separation is modest",
        "question": "Which style currently has the strongest usable run-level verdict rate?",
        "released_answer": "GMD",
        "test_scope": "RQ4",
        "primary_metric": "Usable verdict rate",
        "statistical_test_plan": "Chi-square + Cramer's V with proportion comparison",
    },
    {
        "rq_id": "RQ4",
        "rq_title": "Deployment context and run-level verdict usability",
        "obs_id": "Obs. 4.2",
        "obs_number": "4.2",
        "obs_title": "Among usable verdicts, styles differ sharply in success rate",
        "question": "Which style currently has the strongest success rate among usable verdicts?",
        "released_answer": "GMD",
        "test_scope": "RQ4",
        "primary_metric": "Success rate among usable verdicts",
        "statistical_test_plan": "Chi-square + Cramer's V with proportion comparison",
    },
    {
        "rq_id": "RQ4",
        "rq_title": "Deployment context and run-level verdict usability",
        "obs_id": "Obs. 4.3",
        "obs_number": "4.3",
        "obs_title": "Styles are deployed in markedly different CI trigger contexts",
        "question": "Do the styles remain deployed in markedly different CI trigger contexts?",
        "released_answer": "Yes",
        "test_scope": "RQ4",
        "primary_metric": "Trigger distribution by style",
        "statistical_test_plan": "Chi-square + Cramer's V",
    },
    {
        "rq_id": "RQ4",
        "rq_title": "Deployment context and run-level verdict usability",
        "obs_id": "Obs. 4.4",
        "obs_number": "4.4",
        "obs_title": "For Third-Party and Custom, success rate is strongly trigger-conditioned",
        "question": "Which styles remain strongly trigger-conditioned in success behavior?",
        "released_answer": "Third-Party and Custom",
        "test_scope": "RQ4",
        "primary_metric": "Trigger-conditioned success behavior",
        "statistical_test_plan": "Chi-square + Cramer's V with stratified interpretation",
    },
]


def locate_source_paper() -> Path:
    for candidate in SOURCE_PAPER_CANDIDATES:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate the source paper PDF. Expected one of: "
        + ", ".join(str(p) for p in SOURCE_PAPER_CANDIDATES)
    )


def _extract_pdf_text(pdf_path: Path) -> str:
    if PdfReader is None:  # pragma: no cover
        raise RuntimeError(
            "pypdf is required to bootstrap the observation catalog from the paper PDF. "
            f"Original import error: {_IMPORT_ERROR}"
        )
    reader = PdfReader(str(pdf_path))
    texts: List[str] = []
    for page in reader.pages:
        texts.append(page.extract_text() or "")
    text = "\n".join(texts)
    text = text.replace("\u00ad", "")
    text = re.sub(r"\r", "\n", text)
    return text


def _normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _extract_observation_text(full_text: str, obs_id: str) -> str:
    full_text = full_text.replace("￾", "")
    pattern = re.compile(
        rf"(?P<label>{re.escape(obs_id)})\s*\((?P<title>.*?)\)\s*:?\.?\s*(?P<body>.*?)(?=(Obs\.\s*\d+\.\d+\s*\(|RQ\d+\s*Summary|V\. OPERATIONAL PERFORMANCE PROFILE SUMMARY|VI\. IMPLICATIONS|VII\. THREATS TO VALIDITY|VIII\. CONCLUSION|IX\. APPENDIX))",
        re.DOTALL,
    )
    m = pattern.search(full_text)
    if not m:
        return ""
    title = _normalize_space(m.group("title"))
    body = _normalize_space(m.group("body"))
    return f"{obs_id} ({title}). {body}".strip()


def _base_row(defn: Dict[str, str], source_paper_path: Path, released_observation_text: str) -> Dict[str, str]:
    return {
        "rq_id": defn["rq_id"],
        "rq_title": defn["rq_title"],
        "obs_id": defn["obs_id"],
        "obs_number": defn["obs_number"],
        "obs_title": defn["obs_title"],
        "question": defn["question"],
        "released_answer": defn["released_answer"],
        "released_observation_text": released_observation_text,
        "source_section": "Section IV",
        "source_paper_path": str(source_paper_path),
        "test_scope": defn["test_scope"],
        "primary_metric": defn["primary_metric"],
        "statistical_test_plan": defn["statistical_test_plan"],
    }


def catalog_rows(source_paper_path: Optional[Path] = None) -> List[Dict[str, str]]:
    pdf_path = source_paper_path or locate_source_paper()
    full_text = _extract_pdf_text(pdf_path)

    rows: List[Dict[str, str]] = []
    for defn in OBSERVATION_DEFS:
        released_text = _extract_observation_text(full_text, defn["obs_id"])
        rows.append(_base_row(defn, pdf_path, released_text))
    return rows


def write_catalog_csv(out_csv: Path, source_paper_path: Optional[Path] = None) -> None:
    rows = catalog_rows(source_paper_path=source_paper_path)
    if not rows:
        raise RuntimeError("No observation rows were generated for the QA catalog.")

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)