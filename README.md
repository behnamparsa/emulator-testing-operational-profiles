# Emulator Testing Operational Profiles

This repository contains the data-processing and analytical pipeline for the operational-profile extension of the Android emulator testing study.

## Current analytical model

The analytical pipeline now uses a **single-layer** refresh model.

- `outputs/catalog/observation_qa_catalog.csv` stores the paper-derived baseline catalog.
- `outputs/catalog/observation_qa_catalog_refreshed.csv` is the evolving state table.
- `profile_qa/layer1_validate.py` both validates the latest stored answer and computes the currently favored answer from the latest processed dataset snapshot.
- `profile_qa/profile_regenerate.py` regenerates profiles, rules, and reports from the refreshed catalog only.

## State fields in the refreshed catalog

For each processed-data snapshot, the refreshed catalog appends:

- `L1_target_answer_<snapshot>`
- `L1_validate_<snapshot>`
- `L1_note_<snapshot>`
- `L1_favored_answer_<snapshot>`
- `L1_favored_note_<snapshot>`
- `ACTIVE_<snapshot>`

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




