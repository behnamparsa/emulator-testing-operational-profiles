from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.profile_regenerate import regenerate_from_catalog


if __name__ == "__main__":
    regenerate_from_catalog(
        refreshed_catalog_csv=Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
        profile_md=Path("outputs/profiles/operational_profile.md"),
        profile_json=Path("outputs/profiles/operational_profile.json"),
        rules_json=Path("outputs/rules/decision_support_rules.json"),
        refresh_report_md=Path("outputs/reports/latest_refresh_report.md"),
        profile_table_md=Path("outputs/profiles/operational_profile_table.md"),
        profile_table_csv=Path("outputs/profiles/operational_profile_table.csv"),
        profile_narrative_md=Path("outputs/profiles/operational_profile_narrative.md"),
        decision_guide_md=Path("outputs/rules/decision_support_guide.md"),
        decision_guide_table_csv=Path("outputs/rules/decision_support_guide_table.csv"),
        observation_logic_md=Path("outputs/reports/observation_logic.md"),
        validation_notes_md=Path("outputs/reports/observation_validation_notes.md"),
    )
