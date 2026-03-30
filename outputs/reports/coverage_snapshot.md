# Coverage snapshot

This report summarizes the current processed-dataset coverage in the same spirit as the paper's coverage snapshot, with an added four-style breakdown.

## Overall coverage

- Four-style analysis dataset: **8972** executed run×style records.
- Base controlled subset: **8732** records (`Base = True`, first-attempt usable-verdict records).
- Layer 1 coverage: effectively **8972/8972 (100.00%)** on the four-style dataset because Layer 1 is derived from run/job telemetry.
- Layer 1 time coverage: **2025-01-25 to 2026-03-27** (`run_started_at` range for the four-style dataset).
- Layer 2 observable within Base: **5893/8732 (67.49%)**.
- Layer 2 time coverage: **2025-11-26 to 2026-03-27** (`run_started_at` range for the Layer 2-observable Base subset).

## Breakdown by style

|Style|Four-style dataset|Base subset|Base as % of style total|Layer 2 observable in Base|Layer 2 coverage within Base|
|---|---:|---:|---:|---:|---:|
|Community|8107|7891|97.34%|5591|70.85%|
|Custom|51|39|76.47%|34|87.18%|
|GMD|278|278|100.00%|145|52.16%|
|Third-Party|536|524|97.76%|123|23.47%|

## Interpretation

- The four-style dataset counts all executed run×style records currently represented in `MainDataset.csv` for Community, Custom, GMD, and Third-Party.
- The Base subset matches the repo's controlled timing-comparison regime (`Base = True`).
- Layer 1 time coverage is derived from run/job telemetry and therefore spans the full four-style analysis dataset.
- Layer 2 observability is counted using the presence of the selected Stage 3 invocation-window telemetry source, which is the practical repo-side indicator that the step-level timing decomposition is available.
- The narrower Layer 2 time window reflects the fact that directly observable step-level telemetry is available only for a later and smaller subset of Base.
