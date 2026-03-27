# Emulator Testing Operational Profiles

This repository now supports **two connected capabilities**:

1. **Extraction pipeline refresh**
   - Stage 1-4 extraction
   - `MainDataset.csv` build
   - Stage 3 step telemetry production
2. **Question-answer evidence refresh** for the released study findings
   - Layer 1: validate whether each released observation from the paper is still supported by the newest snapshot
   - Layer 2: refresh the best-supported answer to each observation-question when support changes
   - regenerate the operational profile and the decision-support rule set from the validated or refreshed answers

The repository therefore acts as a **two-layer evidence-refresh system**:
- **Layer 1: observation validation** — rerun the same analysis logic used in the study and record whether each released answer is still supported by the latest snapshot.
- **Layer 2: answer refresh** — when a released answer is no longer adequately supported, compute the current best-supported answer for that same question using the predefined scoring and evidence-selection policy.

## Study-aligned design

The extension treats the paper's released findings as **question-answer units** rather than as free-form narrative text.

Example:
- **Question:** Which style is the fastest overall operational profile?
- **Released answer:** Community
- **Layer 1 validation on a new snapshot:** Yes / No / Partial / Mixed / Insufficient evidence
- **Layer 2 refreshed answer on a new snapshot:** Community / GMD / Third-Party / Custom / Tie / Insufficient evidence

This design keeps Layer 1 and Layer 2 connected:
- Layer 1 checks whether the released answer still holds.
- Layer 2 updates the answer itself when needed.

## Main repository areas

### Extraction pipeline
- `pipeline/`
  - `stage1_verified_workflows.py`
  - `stage2_run_inventory.py`
  - `stage3_run_telemetry.py`
  - `stage4_workload_signature.py`
  - `build_total_dataset.py`

### Question-answer evidence refresh extension
- `profile_qa/observation_catalog.py`
  - released observation-question catalog
- `profile_qa/layer1_validate.py`
  - snapshot validation of released answers
- `profile_qa/layer2_refresh.py`
  - refreshed answer selection for the same questions
- `profile_qa/profile_regenerate.py`
  - regenerate profile and rules from the latest validated/refreshed catalog
- `profile_qa/io_utils.py`
  - catalog reading, snapshot-column append, artifact path helpers

### Wrapper scripts
- `scripts/run_profile_catalog_bootstrap.py`
- `scripts/run_layer1_validation.py`
- `scripts/run_layer2_refresh.py`
- `scripts/run_profile_regeneration.py`

### Outputs
- `outputs/catalog/observation_qa_catalog.csv`
  - master released question-answer catalog
- `outputs/catalog/observation_qa_catalog_validated.csv`
  - same catalog with dated Layer 1 validation columns
- `outputs/catalog/observation_qa_catalog_refreshed.csv`
  - same catalog with dated Layer 2 refreshed-answer columns
- `outputs/profiles/operational_profile.md`
- `outputs/profiles/operational_profile.json`
- `outputs/rules/decision_support_rules.md`
- `outputs/rules/decision_support_rules.json`
- `outputs/reports/latest_refresh_report.md`

## Core inputs for the extension

The evidence-refresh extension is driven by the latest snapshot outputs:
- `data/processed/MainDataset.csv`
- Stage 3 step telemetry artifact:
  - `run_steps_v16_stage3_breakdown.zip`

The current starter implementation uses `MainDataset.csv` as the primary input and is structured so that deeper RQ-specific logic can also consume the Stage 3 step telemetry artifact when needed.

## Observation-question catalog

Each row in the catalog represents one released observation from the paper reformulated as a short question.

Recommended base columns:
- `rq_id`
- `obs_id`
- `question_text`
- `released_answer`
- `released_observation_text`
- `analysis_regime`
- `primary_metric`
- `test_spec`
- `effect_size_spec`
- `robustness_spec`
- `release_snapshot`

Then each new snapshot appends new columns, for example:
- `L1_validate_2026_03_27`
- `L1_note_2026_03_27`
- `L2_answer_2026_03_27`
- `L2_note_2026_03_27`

This preserves history across snapshots and supports change tracking.

## Layer 1: observation validation

Layer 1 reruns the same study-aligned logic for each released question-answer pair and records whether the released answer remains supported by the newest snapshot.

Recommended validation outcomes:
- `Yes`
- `No`
- `Partial`
- `Mixed`
- `Insufficient evidence`

Layer 1 should be implemented RQ by RQ, using the same study logic where applicable:
- same regime/subset
- same metrics
- same statistical tests
- same effect-size interpretation
- same robustness logic when relevant

## Layer 2: answer refresh

Layer 2 operates on the **same questions** as Layer 1.

For each question:
- if Layer 1 says `Yes`, the released answer can stand
- if Layer 1 says `No`, `Partial`, or `Mixed`, Layer 2 computes the current best-supported answer

Recommended answer outcomes:
- one style label, such as `Community` or `GMD`
- `Tie`
- `Conditional`
- `Insufficient evidence`

This refreshed answer table becomes the direct input to profile regeneration and rule regeneration.

## Regenerated artifacts

After Layer 1 and Layer 2 run:
- the operational profile is regenerated from the latest validated/refreshed answers
- the decision-support rule set is regenerated from the same answer table
- the refresh report summarizes:
  - which released answers stayed supported
  - which weakened
  - which changed
  - which regenerated profile and rule outputs changed

## Workflows

### Extraction workflows
Existing Stage 1-4 and dataset builder workflows remain the source of the newest snapshot.

### New question-answer extension workflows
- `.github/workflows/3_10_profile_catalog_bootstrap.yml`
  - create or refresh the base observation-question catalog
- `.github/workflows/3_11_layer1_observation_validation.yml`
  - run Layer 1 validation against the latest snapshot
- `.github/workflows/3_12_layer2_answer_refresh.yml`
  - run Layer 2 refreshed-answer selection
- `.github/workflows/3_13_profile_rule_regeneration.yml`
  - regenerate profile and rules from the latest catalog state
- `.github/workflows/3_00_profile_refresh_orchestrator.yml`
  - orchestrate Layer 1 → Layer 2 → regeneration

## Recommended operational flow

1. Run the extraction pipeline and build the newest `MainDataset.csv`.
2. Ensure the latest Stage 3 step telemetry artifact is available.
3. Run Layer 1 observation validation.
4. Run Layer 2 refreshed-answer selection.
5. Regenerate the operational profile and decision-support rules.
6. Review the refresh report and the dated catalog columns.

## Important boundary

This repository can automatically:
- refresh the evidence
- validate released answers
- refresh the current best-supported answers
- regenerate candidate profile and rule outputs

But study-level interpretation should still remain governed by explicit review rules when needed.


## First-time catalog bootstrap

The first catalog bootstrap is sourced from the paper PDF committed in the repository:
- `data/Source_Paper/Emulator_Testing_MSR_2026_Modified_RQ3B_Extension.pdf`

The bootstrap workflow/script does **not** rely on a hand-maintained starter list as its primary source. Instead, it:
1. reads the source paper PDF from `data/Source_Paper/`
2. extracts the released observations (`Obs. 1.1` .. `Obs. 4.4`)
3. maps them into the repository's question-answer catalog format
4. writes `outputs/catalog/observation_qa_catalog.csv`

After this one-time bootstrap, the generated catalog becomes the baseline that later Layer 1 and Layer 2 runs update with dated snapshot columns.
