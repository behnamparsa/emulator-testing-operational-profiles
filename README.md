# Emulator Testing Operational Profiles

This repository contains the data-processing and analytical pipeline for the operational-profile extension of the Android emulator testing study.

## Current analytical model

The analytical pipeline now uses a **single-layer** refresh model.

- `outputs/catalog/observation_qa_catalog.csv` stores the paper-derived baseline catalog.
- `outputs/catalog/observation_qa_catalog_refreshed.csv` is the evolving state table.
- `profile_qa/layer1_validate.py` both validates the latest stored answer and computes the currently favored answer from the latest processed dataset snapshot.
- `profile_qa/profile_regenerate.py` regenerates profiles, rules, and reports from the refreshed catalog only.

## Major outputs

The two main analytical outputs of this repository are the refreshable counterparts of the paper's Section V deliverables.

### 1. Refreshable operational profile

Primary output:
- `outputs/profiles/operational_profile.md`

Companion outputs:
- `outputs/profiles/operational_profile_table.md`
- `outputs/profiles/operational_profile_table.csv`
- `outputs/profiles/operational_profile_narrative.md`

Structural support:
- `outputs/reports/observation_measurement_structure.md`
- `outputs/reports/observation_validation_notes.md`
- `outputs/reports/coverage_snapshot.md`

### 2. Refreshable decision-support rule set

Primary output:
- `outputs/rules/decision_support_guide.md`

Companion outputs:
- `outputs/rules/decision_support_guide_table.csv`
- `outputs/rules/decision_support_rules.json`

Structural support:
- `outputs/reports/decision_support_rule_structure.md`

Together, these outputs let the repository regenerate both the operational profile and the decision-support guidance from the latest refreshed catalog state.

## State fields in the refreshed catalog

For each processed-data snapshot, the refreshed catalog appends:

- `L1_target_answer_`
- `L1_validate_`
- `L1_note_`
- `L1_favored_answer_`
- `L1_favored_note_`
- `ACTIVE_`

Interpretation:

- If `L1_validate = Passed`, then `ACTIVE = L1_target_answer`.
- If `L1_validate = Failed`, then `ACTIVE = L1_favored_answer`.

This keeps one analytical basis for both validation and update.

## Main workflow

Run:

- `.github/workflows/3_00_profile_refresh_orchestrator.yml`

This executes:

1. Layer 1 validation/update refresh
2. Profile and rule regeneration
