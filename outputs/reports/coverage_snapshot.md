# Coverage snapshot

This report summarizes the current processed-dataset coverage in the same spirit as the paper's coverage snapshot, with an added four-style breakdown.

## Overall coverage

- Four-style analysis dataset: **8846** executed run×style records.
- Base controlled subset: **7744** records (`Base_timing_regime = True`, first-attempt usable-verdict records).
- Layer 1 coverage: effectively **8846/8846 (100.00%)** on the four-style dataset because Layer 1 is derived from run/job telemetry.
- Layer 1 time coverage: **2025-01-15 to 2026-03-19** (`run_started_at` range for the four-style dataset).
- Layer 2 observable within Base: **4598/7744 (59.38%)**.
- Layer 2 time coverage: **2025-12-08 to 2026-03-19** (`run_started_at` range for the Layer 2-observable Base subset).

## Breakdown by style

|Style|Four-style dataset|Base subset|Base as % of style total|Layer 2 observable in Base|Layer 2 coverage within Base|
|---|---:|---:|---:|---:|---:|
|Community|7982|6921|86.71%|4354|62.91%|
|Custom|51|34|66.67%|28|82.35%|
|GMD|265|263|99.25%|117|44.49%|
|Third-Party|548|526|95.99%|99|18.82%|

## Interpretation

- The four-style dataset counts all executed run×style records currently represented in `MainDataset.csv` for Community, Custom, GMD, and Third-Party.
- The Base subset matches the repo's controlled timing-comparison regime (`Base_timing_regime = True`).
- Layer 1 time coverage is derived from run/job telemetry and therefore spans the full four-style analysis dataset.
- Layer 2 observability is counted using the presence of the selected Stage 3 invocation-window telemetry source, which is the practical repo-side indicator that the step-level timing decomposition is available.
- The narrower Layer 2 time window reflects the fact that directly observable step-level telemetry is available only for a later and smaller subset of Base.
