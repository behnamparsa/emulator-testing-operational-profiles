# Observation measurement structure

This file documents the normalized repo-side measurement structure used to automate observation validation and answer refresh.

## Obs. 1.1

- Paper intent: Fastest overall operational profile.
- Primary measurement: Run duration
- Fallback measurement: None
- Winner rule: Pick the style with the lowest median run duration.
- Validation rule: Kruskal–Wallis on run duration, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Primary metric = study_run_duration_seconds.

## Obs. 1.2

- Paper intent: Clearest fast-entry profile without claiming fastest overall completion.
- Primary measurement: Pre-Invocation
- Fallback measurement: Time to Instrumentation Envelope
- Winner rule: Pick the style with the lowest median entry metric.
- Validation rule: Kruskal–Wallis on the selected entry metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Prefer Layer 2 pre-invocation when available; otherwise fall back to Layer 1 time-to-envelope.

## Obs. 1.3

- Paper intent: Slowest sustained-execution profile.
- Primary measurement: Invocation Execution Window
- Fallback measurement: Instrumentation Job Envelope
- Winner rule: Pick the style with the highest median sustained-execution metric.
- Validation rule: Kruskal–Wallis on the selected sustained-execution metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Prefer Layer 2 execution window when available; otherwise fall back to Layer 1 instrumentation envelope.

## Obs. 1.4

- Paper intent: Mixed speed profile with a distinctly long completion tail.
- Primary measurement: Post-Invocation
- Fallback measurement: Post-Instrumentation Tail
- Winner rule: Pick the style with the highest median completion-tail metric.
- Validation rule: Kruskal–Wallis on the selected tail metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Flattened to the tail metric because it is the clearest automated signature of the mixed-speed profile.

## Obs. 1.5

- Paper intent: Fast-core profile that still carries a longer residual tail.
- Primary measurement: Post-Invocation
- Fallback measurement: Post-Instrumentation Tail
- Winner rule: Among fast-core candidates (Community, GMD, Third-Party), pick the style with the highest median tail metric.
- Validation rule: Kruskal–Wallis on the selected tail metric within the fast-core candidate set, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Flattened to the tail metric, restricted to the fast-core candidate set so the rule does not collapse into the Custom tail-heavy exception.

## Obs. 2.1

- Paper intent: Most predictable style on the main completion-oriented measures.
- Primary measurement: Predictability loss on Run Duration
- Fallback measurement: Predictability loss on Instrumentation Job Envelope
- Winner rule: Pick the style with the lowest median predictability-loss metric.
- Validation rule: Kruskal–Wallis on style-level normalized absolute deviation, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Predictability loss is computed as absolute deviation from the style median, normalized by the style median when non-zero.

## Obs. 2.2

- Paper intent: Fast in typical terms but predictability-poor.
- Primary measurement: Predictability loss on Run Duration
- Fallback measurement: Predictability loss on Instrumentation Job Envelope
- Winner rule: Restrict to the two fastest styles by median run duration, then pick the style with the highest median predictability-loss metric.
- Validation rule: Kruskal–Wallis on the selected predictability-loss metric within the fast-style candidate set, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: This preserves the paper’s speed-versus-stability trade-off while keeping the automated rule single-metric.

## Obs. 2.3

- Paper intent: Strongest absolute tail-risk profile.
- Primary measurement: Run Duration upper tail (P90)
- Fallback measurement: Instrumentation Job Envelope upper tail (P90)
- Winner rule: Pick the style with the largest P90 on the selected tail metric.
- Validation rule: Kruskal–Wallis on the selected raw timing metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer; favored answer is chosen by upper-tail burden.
- Technical note: The repo flattens the paper’s absolute tail-risk idea to one upper-tail timing metric.

## Obs. 2.4

- Paper intent: Mixed and cautious predictability profile.
- Primary measurement: Predictability loss on Pre-Invocation
- Fallback measurement: Predictability loss on Run Duration
- Winner rule: Pick the style with the highest invocation-level predictability loss.
- Validation rule: Kruskal–Wallis on the selected predictability-loss metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Flattened to invocation-level predictability because that is where the paper’s weaker Custom signal is most visible.

## Obs. 3.1

- Paper intent: Clearest execution-centric overhead profile.
- Primary measurement: Execution Window Share
- Fallback measurement: None
- Winner rule: Pick the style with the highest median execution-window share.
- Validation rule: Kruskal–Wallis on execution-window share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Primary metric = execution_window_share.

## Obs. 3.2

- Paper intent: Heavy entry plus heavy execution profile.
- Primary measurement: Pre-Invocation Share
- Fallback measurement: Execution Window Share
- Winner rule: Pick the style with the highest median pre-invocation share; use execution-window share as fallback if entry share is unavailable.
- Validation rule: Kruskal–Wallis on the selected share metric, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Flattened to entry burden first because that is the clearest differentiator in the paper.

## Obs. 3.3

- Paper intent: Distributed overhead profile rather than a single dominant source.
- Primary measurement: Maximum phase share (lower is better)
- Fallback measurement: None
- Winner rule: Pick the style with the lowest maximum of pre-, execution-, and post-invocation shares.
- Validation rule: Kruskal–Wallis on per-row max phase share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: This is a direct flattening of the paper’s distributed-overhead idea.

## Obs. 3.4

- Paper intent: Tail-heavy mixed overhead case.
- Primary measurement: Post-Invocation Share
- Fallback measurement: None
- Winner rule: Pick the style with the highest median post-invocation share.
- Validation rule: Kruskal–Wallis on post-invocation share, then Holm-corrected Mann–Whitney pairwise checks against the stored answer.
- Technical note: Primary metric = post_invocation_share.

## Obs. 4.1

- Paper intent: Highest usable-verdict rate.
- Primary measurement: Usable verdict rate
- Fallback measurement: None
- Winner rule: Pick the style with the highest usable-verdict rate on first-attempt runs.
- Validation rule: Chi-square on style × usable-verdict status, with Cramér’s V as effect size.
- Technical note: Categorical validation on first-attempt instrumentation-executed runs.

## Obs. 4.2

- Paper intent: Highest success rate among usable verdicts.
- Primary measurement: Success rate among usable verdicts
- Fallback measurement: None
- Winner rule: Pick the style with the highest success rate among usable first-attempt runs.
- Validation rule: Chi-square on style × success/failure within the usable-verdict subset, with Cramér’s V as effect size.
- Technical note: Categorical validation on the usable-verdict subset only.

## Obs. 4.3

- Paper intent: Meaningful trigger-context differentiation exists across styles.
- Primary measurement: Trigger-context differentiation
- Fallback measurement: None
- Winner rule: Validate a Yes/No claim rather than a style winner.
- Validation rule: Chi-square on style × event, with Cramér’s V as effect size.
- Technical note: Keep Yes only when the chi-square result supports meaningful style-by-event separation.

## Obs. 4.4

- Paper intent: Strongest trigger-conditioned success behavior.
- Primary measurement: Trigger-conditioned success-rate spread
- Fallback measurement: None
- Winner rule: Pick the style with the largest difference between its best and worst event-specific success rates.
- Validation rule: Chi-square-style categorical interpretation is complemented by the spread proxy for automated winner selection.
- Technical note: This is a flattened proxy for the paper’s trigger-conditioned verdict pattern.
