# Robustness check summary

This folder provides the lightweight refreshable robustness companion for the automated operational profile and decision-support guide.

The repo keeps the **operational profile** and the **decision-support rule set** as the two main outputs, and uses this robustness layer as a methodological companion that re-checks whether key findings remain visible under the paper's two-tier signature logic.

## Current snapshot summary

- Base records evaluated for robustness: **8732**
- Tier 1 exact-signature candidates with usable overlap: **5**
- Tier 2 coarsened-family candidates with usable overlap: **5**

## Two-tier interpretation

- **Tier 1** uses the exact workflow-shape signature (`study_signature_hash`) to locate strict within-signature overlap.
- **Tier 2** uses a coarsened family built from runner OS bucket, job-count bucket, and total step-count bucket.
- The goal is not to replace the main refreshed answers, but to show whether those answers remain visible under narrower workflow-shape conditions.

## Main files

- `signature_inventory.csv` lists the exact signatures, their coarsened family, style counts, and Tier 1 qualification flag.
- `observation_robustness_check.csv` summarizes selected key observations, their current reference answer, and whether the answer remains visible under Tier 1 and Tier 2 overlap.

## Snapshot note

This robustness companion is intentionally lighter than the paper's full robustness section. It is designed to be rerun automatically on each refreshed snapshot so the repo can retain a current robustness view without turning the robustness layer into a third major headline output.
