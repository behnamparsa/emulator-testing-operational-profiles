# Observation logic reference

This file explains the scoring/selection logic used for each observation in the refreshed operational-profile pipeline.

## Obs. 1.1

- Item logic: Pick the style with the lowest median total run duration (`study_run_duration_seconds`). Validate the stored answer with Kruskal omnibus significance/effect size and Holm-corrected Mann-Whitney pairwise evidence against the stored style.

## Obs. 1.2

- Item logic: Pick the style with the best fast-entry composite: lower entry time is preferred, with an overall-run-duration penalty so a style is not rewarded for entering fast while finishing slowly.

## Obs. 1.3

- Item logic: Pick the style with the highest median sustained-execution burden, using the execution-window metric when available and falling back to total run duration when needed.

## Obs. 1.4

- Item logic: Pick the best mixed-speed profile from a composite of fast entry and fast execution, then penalize heavier post-invocation tail to capture the early-entry / long-tail trade-off.

## Obs. 1.5

- Item logic: Pick the style with the strongest fast-core profile from instrumentation-envelope and execution-window speed, while still accounting for longer post-invocation tail as a penalty.

## Obs. 2.1

- Item logic: Pick the most predictable style by the lowest normalized median absolute deviation across the main completion-oriented timing measures.

## Obs. 2.2

- Item logic: Pick the fast-but-variable profile by combining good typical speed with poorer predictability, so the answer reflects speed–stability trade-off rather than speed alone.

## Obs. 2.3

- Item logic: Pick the strongest absolute tail-risk profile by the largest upper-tail burden across the main completion-oriented timing measures.

## Obs. 2.4

- Item logic: Pick the mixed and cautious predictability profile from a broad-layer stability signal combined with weaker invocation-level predictability, reflecting a mixed stability pattern rather than a clean winner.

## Obs. 3.1

- Item logic: Pick the clearest execution-centric profile by the largest execution-window share and the smallest residual post-invocation share.

## Obs. 3.2

- Item logic: Pick the heavy-entry plus heavy-execution profile by combining large pre-invocation share and large execution-window share, rather than rewarding a completion-tail-dominant style.

## Obs. 3.3

- Item logic: Pick the distributed-overhead profile by favoring styles whose observable time is spread more evenly across entry, execution, and tail instead of being dominated by one phase.

## Obs. 3.4

- Item logic: Pick the tail-heavy mixed case by the largest post-invocation share, interpreted cautiously because this profile may be sparse in some snapshots.

## Obs. 4.1

- Item logic: Pick the style with the highest usable-verdict rate among first-attempt instrumentation-executed runs, validated with chi-square and Cramer's V.

## Obs. 4.2

- Item logic: Pick the style with the highest success rate among usable verdicts, validated with chi-square and Cramer's V on the usable-verdict subset.

## Obs. 4.3

- Item logic: Treat the answer as a Yes/No claim about trigger-context differentiation. Validate whether styles are deployed differently across events using chi-square and Cramer's V; keep Yes only when the evidence supports meaningful trigger separation.

## Obs. 4.4

- Item logic: Pick the style with the strongest trigger-conditioned success spread, measured as the largest difference between its best and worst event-specific success rates among usable verdicts.
