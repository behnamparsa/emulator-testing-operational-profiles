"""Released observation-question catalog for Layer 1 / Layer 2 refresh.

This starter catalog mirrors the paper's released observation structure.
It is intentionally compact and should be extended RQ by RQ with the exact
statistical logic used in the study.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Dict


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


def default_catalog() -> List[ObservationQuestion]:
    return [
        ObservationQuestion(
            rq_id='RQ1',
            obs_id='1.1',
            question_text='Which style is the fastest overall operational profile?',
            released_answer='Community',
            released_observation_text='Community is the fastest overall operational profile.',
            analysis_regime='Base',
            primary_metric='run_duration_seconds median',
            test_spec='Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
            effect_size_spec='epsilon^2 + Cliff delta',
            robustness_spec='workflow-shape follow-up where applicable',
            release_snapshot='paper_snapshot',
        ),
        ObservationQuestion(
            rq_id='RQ1',
            obs_id='1.2',
            question_text='Which style reaches the instrumentation path fastest?',
            released_answer='GMD',
            released_observation_text='GMD reaches the instrumentation path fastest, but does not finish fastest.',
            analysis_regime='Base',
            primary_metric='time_to_instru envelope / pre-invocation',
            test_spec='Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
            effect_size_spec='epsilon^2 + Cliff delta',
            robustness_spec='workflow-shape follow-up where applicable',
            release_snapshot='paper_snapshot',
        ),
        ObservationQuestion(
            rq_id='RQ2',
            obs_id='2.1',
            question_text='Which style is the most predictable on the main completion-oriented measures?',
            released_answer='GMD',
            released_observation_text='GMD is the most predictable style on the main completion-oriented measures.',
            analysis_regime='Base',
            primary_metric='normalized deviation / spread / tail summaries',
            test_spec='study-defined predictability comparison',
            effect_size_spec='pairwise effect interpretation',
            robustness_spec='workflow-shape follow-up where applicable',
            release_snapshot='paper_snapshot',
        ),
        ObservationQuestion(
            rq_id='RQ3',
            obs_id='3.1',
            question_text='Which style is the clearest execution-centric style?',
            released_answer='GMD',
            released_observation_text='GMD is the clearest execution-centric style.',
            analysis_regime='Base step-observable subset',
            primary_metric='invocation execution share dominance',
            test_spec='Kruskal-Wallis + pairwise Mann-Whitney (Holm)',
            effect_size_spec='epsilon^2 + Cliff delta',
            robustness_spec='interpretive consistency across overhead lenses',
            release_snapshot='paper_snapshot',
        ),
        ObservationQuestion(
            rq_id='RQ4',
            obs_id='4.1',
            question_text='Which style has the highest usable verdict rate?',
            released_answer='GMD',
            released_observation_text='Styles differ in usable run-level verdict rates, with GMD highest.',
            analysis_regime='instrumentation-executed first attempts',
            primary_metric='usable verdict rate',
            test_spec='chi-square',
            effect_size_spec='Cramer V',
            robustness_spec='descriptive trigger-conditioned inspection where relevant',
            release_snapshot='paper_snapshot',
        ),
    ]


def catalog_rows() -> List[Dict[str, str]]:
    return [asdict(q) for q in default_catalog()]
