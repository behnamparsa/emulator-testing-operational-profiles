from pathlib import Path
from profile_qa.profile_regenerate import regenerate_from_catalog

regenerate_from_catalog(
    refreshed_catalog_csv=Path('outputs/catalog/observation_qa_catalog_refreshed.csv'),
    profile_md=Path('outputs/profiles/operational_profile.md'),
    profile_json=Path('outputs/profiles/operational_profile.json'),
    rules_md=Path('outputs/rules/decision_support_rules.md'),
    rules_json=Path('outputs/rules/decision_support_rules.json'),
    report_md=Path('outputs/reports/latest_refresh_report.md'),
)
print('regenerated profile and rules')
