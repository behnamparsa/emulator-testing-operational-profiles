# Refreshed operational profile

## RQ1 — Speed profiling

### Obs. 1.1 — Which style is the fastest overall operational profile?

- Released answer: `Community`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Community`
- Current active answer: `Community`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Custom: p_adj=0.000399, rbc=0.311, vs GMD: p_adj=5.04e-11, rbc=0.231, vs Third-Party: p_adj=2.91e-56, rbc=0.412. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 1.2 — Which style shows the clearest fast-entry profile without being the fastest overall finisher?

- Released answer: `GMD`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Community: p_adj=1, rbc=-0.231, vs Custom: p_adj=2.48e-08, rbc=0.551, vs Third-Party: p_adj=3.13e-64, rbc=0.726. stored='GMD', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 1.3 — Which style is the slowest sustained-execution profile?

- Released answer: `Third-Party`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Third-Party`
- Current active answer: `Third-Party`
- Validation note: Kruskal on study_invocation_execution_window_selected_stage3_seconds: p=7.71e-51, epsilon^2=0.040; vs Community: p_adj=8.4e-33, rbc=0.630, vs Custom: p_adj=2.93e-08, rbc=0.609, vs GMD: p_adj=2.22e-11, rbc=0.474. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 1.4 — Which style shows a mixed speed profile with competitive entry, middling core path, and a long completion tail?

- Released answer: `Custom`
- Latest Layer 1 status: `Failed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Kruskal on study_post_invocation_selected_stage3_seconds: p=1.13e-71, epsilon^2=0.056; vs GMD: p_adj=2.27e-13, rbc=0.802, vs Community: p_adj=0.00767, rbc=0.241, vs Third-Party: p_adj=6.45e-08, rbc=0.599. stored='Custom', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 1.5 — Which style combines a fast core execution profile with a longer residual tail?

- Released answer: `Community`
- Latest Layer 1 status: `Failed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Kruskal on study_invocation_execution_window_selected_stage3_seconds: p=7.71e-51, epsilon^2=0.040; vs GMD: p_adj=4.57e-22, rbc=0.469, vs Custom: p_adj=0.0173, rbc=0.210, vs Third-Party: p_adj=8.4e-33, rbc=0.630. stored='Community', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

## RQ2 — Predictability and tail risk

### Obs. 2.1 — Which style is the most predictable on the main completion-oriented measures?

- Released answer: `GMD`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Community: p_adj=1, rbc=-0.231, vs Custom: p_adj=2.48e-08, rbc=0.551, vs Third-Party: p_adj=3.13e-64, rbc=0.726. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 2.2 — Which style is fast in typical terms but predictability-poor?

- Released answer: `Community`
- Latest Layer 1 status: `Failed`
- Statistically favored answer: `Third-Party`
- Current active answer: `Third-Party`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Third-Party: p_adj=2.91e-56, rbc=0.412, vs Custom: p_adj=0.000399, rbc=0.311, vs GMD: p_adj=5.04e-11, rbc=0.231. stored='Community', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 2.3 — Which style carries the strongest absolute tail-risk profile?

- Released answer: `Third-Party`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Third-Party`
- Current active answer: `Third-Party`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Community: p_adj=1.94e-56, rbc=0.412, vs Custom: p_adj=0.074, rbc=0.139, vs GMD: p_adj=3.13e-64, rbc=0.726. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 2.4 — Which style shows a mixed predictability profile that should be interpreted cautiously?

- Released answer: `Custom`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Custom`
- Current active answer: `Custom`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Community: p_adj=1, rbc=-0.311, vs GMD: p_adj=1, rbc=-0.551, vs Third-Party: p_adj=0.222, rbc=0.139. stored='Custom', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

## RQ3 — Overhead composition and actionable levers

### Obs. 3.1 — Which style is the clearest execution-centric overhead profile?

- Released answer: `GMD`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Kruskal on execution_window_share: p=1.23e-52, epsilon^2=0.041; vs Community: p_adj=4.03e-47, rbc=0.702, vs Custom: p_adj=7.66e-16, rbc=0.880, vs Third-Party: p_adj=8.66e-28, rbc=0.775. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 3.2 — Which style is best characterized by heavy entry plus heavy execution rather than a dominant completion tail?

- Released answer: `Third-Party`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Third-Party`
- Current active answer: `Third-Party`
- Validation note: Kruskal on execution_window_share: p=1.23e-52, epsilon^2=0.041; vs GMD: p_adj=1, rbc=-0.775, vs Community: p_adj=3.39e-09, rbc=0.315, vs Custom: p_adj=0.000535, rbc=0.389. stored='Third-Party', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 3.3 — Which style has a distributed overhead profile rather than a single dominant overhead source?

- Released answer: `Community`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Community`
- Current active answer: `Community`
- Validation note: Kruskal on execution_window_share: p=1.23e-52, epsilon^2=0.041; vs Custom: p_adj=1, rbc=0.012, vs GMD: p_adj=1, rbc=-0.702, vs Third-Party: p_adj=1, rbc=-0.315. stored='Community', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 3.4 — Which style remains a cautious tail-heavy mixed overhead case?

- Released answer: `Custom`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `Custom`
- Current active answer: `Custom`
- Validation note: Kruskal on post_invocation_share: p=5.47e-89, epsilon^2=0.069; vs Community: p_adj=0.0194, rbc=0.205, vs GMD: p_adj=1.73e-09, rbc=0.665, vs Third-Party: p_adj=3.91e-11, rbc=0.748. stored='Custom', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

## RQ4 — Deployment context and run-level verdict usability

### Obs. 4.1 — Which style currently has the strongest usable run-level verdict rate?

- Released answer: `GMD`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Chi-square for usable_verdict_rate: p=4.89e-17, Cramer's V=0.095; stored='GMD', winner='GMD'.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 4.2 — Which style currently has the strongest success rate among usable verdicts?

- Released answer: `GMD`
- Latest Layer 1 status: `Passed`
- Statistically favored answer: `GMD`
- Current active answer: `GMD`
- Validation note: Chi-square for success_among_usable: p=6.2e-252, Cramer's V=0.386; stored='GMD', winner='GMD'.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 4.3 — Do the styles remain deployed in markedly different CI trigger contexts?

- Released answer: `Yes`
- Latest Layer 1 status: `Failed`
- Statistically favored answer: `Yes`
- Current active answer: `Yes`
- Validation note: Chi-square on style × event: p=0, Cramer's V=0.395; stored answer='Yes'.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.

### Obs. 4.4 — Which styles remain strongly trigger-conditioned in success behavior?

- Released answer: `Third-Party and Custom`
- Latest Layer 1 status: `Failed`
- Statistically favored answer: `Third-Party`
- Current active answer: `Third-Party`
- Validation note: Trigger-conditioned spread gap=0.3188; stored='Custom', winner='Third-Party'. Scored by the largest trigger-conditioned success-rate spread.
- Favored-answer note: Migrated during Layer 2 retirement; rerun Layer 1 to recompute favored answer strictly from the single-layer logic.
