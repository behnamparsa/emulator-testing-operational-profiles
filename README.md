# Emulator Testing Operational Profiles

This repository contains the data-processing and analytical pipeline for the operational-profile extension of the Android emulator testing study.

## Current analytical model

The analytical pipeline uses a **single-layer** refresh model.

- `outputs/catalog/observation_qa_catalog.csv` stores the paper-derived baseline catalog.
- `outputs/catalog/observation_qa_catalog_refreshed.csv` is the evolving state table.
- `profile_qa/layer1_validate.py` validates the latest stored answer and computes the currently favored answer from the latest processed dataset snapshot.
- `profile_qa/profile_regenerate.py` regenerates profiles, rules, and reports from the refreshed catalog only.
- `scripts/run_robustness_check.py` regenerates the lightweight robustness companion from the latest processed dataset and refreshed catalog.

## Major outputs

The two main analytical outputs of this repository are the refreshable counterparts of the paper's Section V deliverables.

### 1. Refreshable operational profile

Primary output:
- `outputs/profiles/operational_profile.md`

Companion outputs:
- `outputs/profiles/operational_profile_table.md`
- `outputs/profiles/operational_profile_table.csv`
- `outputs/profiles/operational_profile_narrative.md`

Validation companions:
- `outputs/reports/observation_measurement_structure.md`
- `outputs/reports/observation_validation_notes.md`
- `outputs/reports/coverage_snapshot.md`

### 2. Refreshable decision-support rule set

Primary output:
- `outputs/rules/decision_support_guide.md`

Companion outputs:
- `outputs/rules/decision_support_guide_table.csv`
- `outputs/rules/decision_support_rules.json`

Structural companion:
- `outputs/reports/decision_support_rule_structure.md`

### Secondary validation companion: robustness check

The robustness companion is not treated as a third headline output. Instead, it provides a lighter refreshable validation layer that re-checks key findings under the paper's two-tier signature logic using structural support, directional support, and conditional statistical support when subset size is sufficient.

Outputs:
- `outputs/robustness_check/robustness_summary.md`
- `outputs/robustness_check/signature_inventory.csv`
- `outputs/robustness_check/observation_robustness_check.csv`
- `outputs/robustness_check/coarsened_family_inventory.csv`

## Refreshed catalog state fields

For each processed-data snapshot, the refreshed catalog appends dated state fields such as:

- `L1_target_answer_<snapshot>`
- `L1_validate_<snapshot>`
- `L1_note_<snapshot>`
- `L1_favored_answer_<snapshot>`
- `L1_favored_note_<snapshot>`
- `ACTIVE_<snapshot>`

Interpretation:
- If `L1_validate = Passed`, then `ACTIVE = L1_target_answer`.
- If `L1_validate = Failed`, then `ACTIVE = L1_favored_answer`.
- If `L1_validate = Insufficient evidence`, the active answer remains unchanged.

## Main pipelines and orchestrators

### 1. Data-processing pipeline

This pipeline rebuilds the processed telemetry layers and the study-facing dataset.

Main orchestrator:
- `.github/workflows/0_00_dataprocess_orchestrator_single1234_sharded3.yml`

This orchestrator can be run independently whenever you want to refresh the processed dataset without regenerating the analytical outputs yet.

### 2. Analytical pipeline

This pipeline validates the observations, regenerates the operational profile and decision-support rule set, and then refreshes the lightweight robustness companion.

Main orchestrator:
- `.github/workflows/3_00_profile_refresh_orchestrator.yml`

This orchestrator can be run independently whenever the processed dataset is already up to date and only the analytical outputs need to be refreshed.

Its internal sequence is:
1. `3_11_layer1_observation_validation.yml`
2. `3_12_profile_rule_regeneration.yml`
3. `3_13_robustness_check.yml`

### 3. Scheduled master orchestrator

This workflow is designed to refresh the whole repository on a quarterly schedule by running the two main pipelines in sequence.

Master orchestrator:
- `.github/workflows/9_00_scheduled_master_refresh.yml`

Its role is:
1. dispatch the data-processing orchestrator
2. wait for successful completion
3. dispatch the analytical orchestrator

This makes the repository suitable for scheduled end-to-end refresh of the processed dataset, refreshed catalog, operational profile, decision-support rule set, and robustness companion.
