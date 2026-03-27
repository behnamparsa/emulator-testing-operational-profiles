from __future__ import annotations

"""Build the released observation-question catalog from the source paper PDF.

The first bootstrap should come from the paper stored in the repository,
not from a hand-maintained starter list. This module:
1. locates the PDF under data/Source_Paper/
2. extracts the named observations (Obs. 1.1 .. Obs. 4.4)
3. maps each observation to a short question and released answer
4. emits the initial catalog rows used by Layer 1 and Layer 2
"""

from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List
import re

try:
    from pypdf import PdfReader
except Exception as exc:  # pragma: no cover
    PdfReader = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


@dataclass(frozen=True)
class ObservationQuestion:
    rq_id: str
    obs_id: str
    question_text: str
    released_answer: str
    released_observation_text: str
    analysis_regime: str
    primary_metric: str
    test_spec: str
    effect_size_spec: str
    robustness_spec: str
    release_snapshot: str
    source_paper_path: str


OBSERVATION_SPECS: Dict[str, Dict[str, str]] = {
    '1.1': {
        'rq_id': 'RQ1',
        'question_text': 'Which style is the fastest overall operational profile?',
        'released_answer': 'Community',
        'analysis_regime': 'Base',
        'primary_metric': 'run_duration_seconds median + execution-oriented timing components',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '1.2': {
        'rq_id': 'RQ1',
        'question_text': 'Which style reaches the instrumentation path fastest?',
        'released_answer': 'GMD',
        'analysis_regime': 'Base',
        'primary_metric': 'time-to-envelope + pre-invocation median',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '1.3': {
        'rq_id': 'RQ1',
        'question_text': 'Which style is the slowest sustained-execution profile?',
        'released_answer': 'Third-Party',
        'analysis_regime': 'Base',
        'primary_metric': 'run_duration_seconds + pre-invocation + invocation-execution median',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '1.4': {
        'rq_id': 'RQ1',
        'question_text': 'Which style shows the mixed speed profile with competitive entry but a very long completion tail?',
        'released_answer': 'Custom',
        'analysis_regime': 'Base',
        'primary_metric': 'time-to-envelope + run duration + post-invocation median',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'directional workflow-shape support where available',
    },
    '1.5': {
        'rq_id': 'RQ1',
        'question_text': 'Which style has a fast-core but longer-tail speed profile?',
        'released_answer': 'Community',
        'analysis_regime': 'Base',
        'primary_metric': 'execution-window advantage + post-tail disadvantage',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '2.1': {
        'rq_id': 'RQ2',
        'question_text': 'Which style is the most predictable on the main completion-oriented measures?',
        'released_answer': 'GMD',
        'analysis_regime': 'Base',
        'primary_metric': 'normalized deviation + spread + relative tail summaries',
        'test_spec': 'study-defined predictability comparisons on timing measures',
        'effect_size_spec': 'pairwise effect interpretation',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '2.2': {
        'rq_id': 'RQ2',
        'question_text': 'Which style is fast in typical terms but predictability-poor?',
        'released_answer': 'Community',
        'analysis_regime': 'Base',
        'primary_metric': 'speed medians + normalized variability',
        'test_spec': 'study-defined predictability comparisons on timing measures',
        'effect_size_spec': 'pairwise effect interpretation',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '2.3': {
        'rq_id': 'RQ2',
        'question_text': 'Which style carries the strongest absolute tail-risk profile?',
        'released_answer': 'Third-Party',
        'analysis_regime': 'Base',
        'primary_metric': 'absolute tail summaries + normalized deviation',
        'test_spec': 'study-defined predictability comparisons on timing measures',
        'effect_size_spec': 'pairwise effect interpretation',
        'robustness_spec': 'two-tier workflow-shape follow-up',
    },
    '2.4': {
        'rq_id': 'RQ2',
        'question_text': 'Which style has a mixed and cautious predictability profile?',
        'released_answer': 'Custom',
        'analysis_regime': 'Base',
        'primary_metric': 'Layer 1 stability vs Layer 2 predictability contrast',
        'test_spec': 'study-defined predictability comparisons on timing measures',
        'effect_size_spec': 'pairwise effect interpretation',
        'robustness_spec': 'narrower support; interpret cautiously',
    },
    '3.1': {
        'rq_id': 'RQ3',
        'question_text': 'Which style is the clearest execution-centric style?',
        'released_answer': 'GMD',
        'analysis_regime': 'Base step-observable subset',
        'primary_metric': 'invocation execution share dominance',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'interpretive consistency across overhead lenses',
    },
    '3.2': {
        'rq_id': 'RQ3',
        'question_text': 'Which style is best characterized by heavy entry plus heavy execution rather than by a dominant completion tail?',
        'released_answer': 'Third-Party',
        'analysis_regime': 'Base step-observable subset',
        'primary_metric': 'pre-invocation + execution share dominance',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'interpretive consistency across overhead lenses',
    },
    '3.3': {
        'rq_id': 'RQ3',
        'question_text': 'Which style has a distributed overhead profile rather than a single dominant overhead source?',
        'released_answer': 'Community',
        'analysis_regime': 'Base step-observable subset',
        'primary_metric': 'distributed placement across pre, execution, and post',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'interpretive consistency across overhead lenses',
    },
    '3.4': {
        'rq_id': 'RQ3',
        'question_text': 'Which style remains a cautious, tail-heavy mixed case in overhead composition?',
        'released_answer': 'Custom',
        'analysis_regime': 'Base step-observable subset',
        'primary_metric': 'post-invocation share dominance with cautious support',
        'test_spec': 'Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
        'effect_size_spec': 'epsilon^2 + Cliff delta',
        'robustness_spec': 'interpret cautiously due to smaller support',
    },
    '4.1': {
        'rq_id': 'RQ4',
        'question_text': 'Which style has the highest usable run-level verdict rate?',
        'released_answer': 'GMD',
        'analysis_regime': 'instrumentation-executed first attempts',
        'primary_metric': 'usable verdict rate',
        'test_spec': 'chi-square + pairwise proportion contrasts where relevant',
        'effect_size_spec': 'Cramer V',
        'robustness_spec': 'descriptive trigger-conditioned inspection where relevant',
    },
    '4.2': {
        'rq_id': 'RQ4',
        'question_text': 'Which style has the highest success rate among usable verdicts?',
        'released_answer': 'GMD',
        'analysis_regime': 'usable-verdict subset of instrumentation-executed first attempts',
        'primary_metric': 'success rate among usable verdicts',
        'test_spec': 'chi-square + pairwise proportion contrasts where relevant',
        'effect_size_spec': 'Cramer V',
        'robustness_spec': 'practical significance emphasized',
    },
    '4.3': {
        'rq_id': 'RQ4',
        'question_text': 'How are styles deployed across CI trigger contexts?',
        'released_answer': 'Community=push; GMD=schedule; Third-Party=schedule; Custom=pull_request',
        'analysis_regime': 'instrumentation-executed first attempts',
        'primary_metric': 'trigger-event distribution by style',
        'test_spec': 'chi-square',
        'effect_size_spec': 'Cramer V',
        'robustness_spec': 'descriptive deployment-context interpretation',
    },
    '4.4': {
        'rq_id': 'RQ4',
        'question_text': 'Which styles show strongly trigger-conditioned success behavior?',
        'released_answer': 'Third-Party and Custom',
        'analysis_regime': 'instrumentation-executed first attempts',
        'primary_metric': 'trigger-conditioned verdict pattern',
        'test_spec': 'contingency-table analysis within style where relevant',
        'effect_size_spec': 'Cramer V',
        'robustness_spec': 'style-specific event-conditioned interpretation',
    },
}

EXPECTED_OBS_ORDER: List[str] = [
    '1.1','1.2','1.3','1.4','1.5',
    '2.1','2.2','2.3','2.4',
    '3.1','3.2','3.3','3.4',
    '4.1','4.2','4.3','4.4',
]


def locate_source_paper(repo_root: Path | None = None) -> Path:
    root = repo_root or Path.cwd()
    paper_dir = root / 'data' / 'Source_Paper'
    pdfs = sorted(paper_dir.glob('*.pdf'))
    if not pdfs:
        raise FileNotFoundError(f'No source paper PDF found under {paper_dir}')
    return pdfs[0]


def _extract_pdf_text(pdf_path: Path) -> str:
    if PdfReader is None:  # pragma: no cover
        raise RuntimeError(
            'pypdf is required to bootstrap the observation catalog from the paper PDF. '
            f'Original import error: {_IMPORT_ERROR}'
        )
    reader = PdfReader(str(pdf_path))
    texts: List[str] = []
    for page in reader.pages:
        texts.append(page.extract_text() or '')
    text = '
'.join(texts)
    text = text.replace('­', '')
    text = re.sub(r'', '
', text)
    return text


def _extract_observations(text: str) -> Dict[str, str]:
    flat = re.sub(r'\s+', ' ', text)
    pattern = re.compile(
        r'Obs\.\s*(?P<obs>\d+\.\d+)\s*\((?P<title>.*?)\)\.:\s*(?P<body>.*?)(?=(?:Obs\.\s*\d+\.\d+\s*\()|(?:RQ\d Summary)|(?:V\. OPERATIONAL PERFORMANCE PROFILE SUMMARY)|$)'
    )
    obs_map: Dict[str, str] = {}
    for match in pattern.finditer(flat):
        obs_id = match.group('obs')
        title = match.group('title').strip()
        body = match.group('body').strip()
        text_value = f'{title}. {body}'
        text_value = re.sub(r'\s+', ' ', text_value).strip()
        obs_map[obs_id] = text_value
    missing = [obs for obs in EXPECTED_OBS_ORDER if obs not in obs_map]
    if missing:
        raise RuntimeError(f'Failed to extract observation text for: {missing}')
    return obs_map


def build_catalog_from_paper(pdf_path: Path) -> List[ObservationQuestion]:
    obs_map = _extract_observations(_extract_pdf_text(pdf_path))
    rows: List[ObservationQuestion] = []
    for obs_id in EXPECTED_OBS_ORDER:
        spec = OBSERVATION_SPECS[obs_id]
        rows.append(
            ObservationQuestion(
                rq_id=spec['rq_id'],
                obs_id=obs_id,
                question_text=spec['question_text'],
                released_answer=spec['released_answer'],
                released_observation_text=obs_map[obs_id],
                analysis_regime=spec['analysis_regime'],
                primary_metric=spec['primary_metric'],
                test_spec=spec['test_spec'],
                effect_size_spec=spec['effect_size_spec'],
                robustness_spec=spec['robustness_spec'],
                release_snapshot='paper_pdf_bootstrap',
                source_paper_path=str(pdf_path.as_posix()),
            )
        )
    return rows


def catalog_rows(pdf_path: Path | None = None) -> List[Dict[str, str]]:
    actual_pdf = pdf_path or locate_source_paper()
    return [asdict(q) for q in build_catalog_from_paper(actual_pdf)]
