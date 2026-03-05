from __future__ import annotations
import json
from pathlib import Path
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA = REPO_ROOT / 'data' / 'processed' / 'MainDataset.csv'
OUT_PROFILES = REPO_ROOT / 'outputs' / 'profiles'
OUT_RULES = REPO_ROOT / 'outputs' / 'rules'
OUT_REPORTS = REPO_ROOT / 'outputs' / 'reports'
for p in [OUT_PROFILES, OUT_RULES, OUT_REPORTS]:
    p.mkdir(parents=True, exist_ok=True)

STYLE_ORDER = ['Community', 'Custom', 'GMD', 'Third-Party']
LEVER_MAP = {
    'pre_test_overhead_seconds': 'Optimize setup/provisioning first',
    'instru_exec_window_seconds': 'Optimize test execution first',
    'post_test_overhead_seconds': 'Optimize post-test follow-up first',
}


def median_or_none(s: pd.Series):
    s = pd.to_numeric(s, errors='coerce').dropna()
    return None if s.empty else float(s.median())


def build_profiles(df: pd.DataFrame):
    profiles = []
    for style in STYLE_ORDER:
        g = df[df['style'] == style].copy()
        if g.empty:
            continue
        comps = {k: median_or_none(g[k]) for k in LEVER_MAP if k in g.columns}
        valid = {k: v for k, v in comps.items() if v is not None}
        dominant_component = max(valid, key=valid.get) if valid else None
        profile = {
            'style': style,
            'n': int(g.shape[0]),
            'run_median_seconds': median_or_none(g['run_duration_seconds']) if 'run_duration_seconds' in g.columns else None,
            'ttfts_median_seconds': median_or_none(g['ttfts_seconds']) if 'ttfts_seconds' in g.columns else None,
            'window_median_seconds': median_or_none(g['instru_window_seconds']) if 'instru_window_seconds' in g.columns else None,
            'verdict_success_rate': None if 'instru_conclusion' not in g.columns else float((g['instru_conclusion'] == 'success').mean()),
            'dominant_component': dominant_component,
            'first_optimization_target': None if dominant_component is None else LEVER_MAP[dominant_component],
        }
        profiles.append(profile)
    return profiles


def main() -> None:
    df = pd.read_csv(DATA, low_memory=False)
    df = df[df['style'].isin(STYLE_ORDER)].copy()
    if 'Base' in df.columns:
        df = df[df['Base'] == True].copy()
    profiles = build_profiles(df)
    rules = []
    for p in profiles:
        rules.append({
            'style': p['style'],
            'if_you_prioritize': 'first optimization target',
            'recommended_first_target': p['first_optimization_target'],
            'evidence': {
                'dominant_component': p['dominant_component'],
                'run_median_seconds': p['run_median_seconds'],
                'ttfts_median_seconds': p['ttfts_median_seconds'],
                'window_median_seconds': p['window_median_seconds'],
                'success_rate': p['verdict_success_rate'],
                'n': p['n'],
            }
        })

    (OUT_PROFILES / 'operational_profile.json').write_text(json.dumps(profiles, indent=2), encoding='utf-8')
    (OUT_RULES / 'decision_support_rules.json').write_text(json.dumps(rules, indent=2), encoding='utf-8')

    md = ['# Operational Profiles', '']
    for p in profiles:
        md += [
            f"## {p['style']}",
            f"- n: {p['n']}",
            f"- Run median: {p['run_median_seconds']}",
            f"- TTFTS median: {p['ttfts_median_seconds']}",
            f"- Window median: {p['window_median_seconds']}",
            f"- Dominant component: {p['dominant_component']}",
            f"- First optimization target: {p['first_optimization_target']}",
            '',
        ]
    (OUT_PROFILES / 'operational_profile.md').write_text('\n'.join(md), encoding='utf-8')

    md = ['# Decision Support Rules', '']
    for r in rules:
        md += [
            f"## {r['style']}",
            f"- Recommended first target: {r['recommended_first_target']}",
            f"- Based on dominant component: {r['evidence']['dominant_component']}",
            f"- n: {r['evidence']['n']}",
            '',
        ]
    (OUT_RULES / 'decision_support_rules.md').write_text('\n'.join(md), encoding='utf-8')

    (OUT_REPORTS / 'latest_refresh_report.md').write_text(
        '# Refresh complete\n\nGenerated dataset, operational profiles, and decision-support rules.\n',
        encoding='utf-8',
    )


if __name__ == '__main__':
    main()
