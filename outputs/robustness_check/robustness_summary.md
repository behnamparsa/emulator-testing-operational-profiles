# Robustness check summary

This folder provides the refreshable robustness companion for the automated operational profile and decision-support guide.

The repo keeps the **operational profile** and the **decision-support rule set** as the two main outputs, and uses this robustness layer as a methodological companion that re-checks whether key findings remain visible under the paper's two-tier signature logic.

## Current snapshot summary

- Base records evaluated for robustness: **8732**
- First-attempt records evaluated for robustness: **8732**
- Tier 1 exact-signature candidates with usable overlap (Base): **5**
- Tier 2 coarsened-family candidates with usable overlap (Base): **5**
- Tier 1 exact-signature candidates with usable overlap (first-attempt): **5**
- Tier 2 coarsened-family candidates with usable overlap (first-attempt): **5**

## Three-level interpretation

- **Structural support** checks whether the current reference answer remains represented inside qualifying Tier 1 exact-signature overlap and Tier 2 coarsened-family overlap.
- **Directional support** checks whether the same answer remains directionally visible when the observation is re-evaluated inside the qualifying Tier 1 and Tier 2 subsets.
- **Statistical support** is performed only when the robustness subset is large enough for a stable automated check; otherwise the result is marked as `not_feasible` rather than forcing a fragile significance claim.

## Two-tier interpretation

- **Tier 1** uses the exact workflow-shape signature (`study_signature_hash`) to locate strict within-signature overlap.
- **Tier 2** uses a coarsened family built from runner OS bucket, job-count bucket, and total step-count bucket.
- The goal is not to replace the main refreshed answers, but to show whether those answers remain visible under narrower workflow-shape conditions.

## Main files

- `signature_inventory.csv` lists the exact signatures, their coarsened family, style counts, regime, and Tier 1 qualification flag.
- `coarsened_family_inventory.csv` lists the coarsened families, style counts, regime, and Tier 2 qualification flag.
- `observation_robustness_check.csv` summarizes selected key observations, their current reference answer, and their structural, directional, and conditional statistical support.

## Snapshot note

This robustness companion is still lighter than the paper's full robustness section, but it now goes beyond pure overlap checking by adding directional re-checks and conditional statistical support where the robustness subset is large enough.
