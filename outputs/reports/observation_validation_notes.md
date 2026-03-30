# Observation validation notes

This file keeps the technical validation notes and favored-answer notes separate from the main operational profile.

## Obs. 1.1 — Which style is the fastest overall operational profile?

- Current baseline under validation: `Community`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Community`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Custom: p_adj=0.000399, rbc=0.311, vs GMD: p_adj=5.04e-11, rbc=0.231, vs Third-Party: p_adj=2.91e-56, rbc=0.412. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_run_duration_seconds; winner = lowest median run duration.

## Obs. 1.2 — Which style shows the clearest fast-entry profile without being the fastest overall finisher?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on study_pre_invocation_selected_stage3_seconds: p=8.92e-14, epsilon^2=0.010; vs Community: p_adj=0.000469, rbc=0.170, vs Custom: p_adj=0.0141, rbc=0.242, vs Third-Party: p_adj=7.9e-41, rbc=0.951. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_pre_invocation_selected_stage3_seconds; winner = lowest median fast-entry metric.

## Obs. 1.3 — Which style is the slowest sustained-execution profile?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on study_invocation_execution_window_selected_stage3_seconds: p=7.71e-51, epsilon^2=0.040; vs Community: p_adj=8.4e-33, rbc=0.630, vs Custom: p_adj=2.93e-08, rbc=0.609, vs GMD: p_adj=2.22e-11, rbc=0.474. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_invocation_execution_window_selected_stage3_seconds; winner = highest median sustained-execution burden.

## Obs. 1.4 — Which style shows a mixed speed profile with competitive entry, middling core path, and a long completion tail?

- Current baseline under validation: `Custom`
- Validation status: `Passed`
- Favored answer: `Custom`
- Active answer: `Custom`
- Validation note: Kruskal on study_post_invocation_selected_stage3_seconds: p=1.13e-71, epsilon^2=0.056; vs Community: p_adj=0.00767, rbc=0.241, vs GMD: p_adj=2.27e-13, rbc=0.802, vs Third-Party: p_adj=6.45e-08, rbc=0.599. stored='Custom', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_post_invocation_selected_stage3_seconds; winner = highest median completion-tail metric.

## Obs. 1.5 — Which style combines a fast core execution profile with a longer residual tail?

- Current baseline under validation: `Community`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Community`
- Validation note: Kruskal on study_post_invocation_selected_stage3_seconds: p=1.54e-71, epsilon^2=0.055; vs GMD: p_adj=7.33e-50, rbc=0.721, vs Third-Party: p_adj=9.75e-27, rbc=0.560. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_post_invocation_selected_stage3_seconds; winner = highest median tail metric within the fast-core candidate set.

## Obs. 2.1 — Which style is the most predictable on the main completion-oriented measures?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on predictability loss on study_run_duration_seconds: p=7.34e-120, epsilon^2=0.063; vs Community: p_adj=6.43e-102, rbc=0.756, vs Custom: p_adj=1.66e-09, rbc=0.585, vs Third-Party: p_adj=2.88e-45, rbc=0.605. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_run_duration_seconds; winner = lowest median normalized deviation.

## Obs. 2.2 — Which style is fast in typical terms but predictability-poor?

- Current baseline under validation: `Community`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Community`
- Validation note: Kruskal on predictability loss on study_run_duration_seconds: p=4.28e-102, epsilon^2=0.056; vs GMD: p_adj=2.14e-102, rbc=0.756. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_run_duration_seconds within the two fastest styles by run duration; winner = highest median normalized deviation.

## Obs. 2.3 — Which style carries the strongest absolute tail-risk profile?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on study_run_duration_seconds: p=6.32e-66, epsilon^2=0.035; vs Community: p_adj=1.94e-56, rbc=0.412, vs Custom: p_adj=0.074, rbc=0.139, vs GMD: p_adj=3.13e-64, rbc=0.726. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: upper-tail burden on study_run_duration_seconds; winner = highest P90.

## Obs. 2.4 — Which style shows a mixed predictability profile that should be interpreted cautiously?

- Current baseline under validation: `Custom`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Custom`
- Validation note: Kruskal on predictability loss on study_pre_invocation_selected_stage3_seconds: p=3.5e-100, epsilon^2=0.078; vs Community: p_adj=1, rbc=-0.332, vs GMD: p_adj=3.93e-11, rbc=0.735, vs Third-Party: p_adj=1e-08, rbc=0.643. stored='Custom', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_pre_invocation_selected_stage3_seconds; winner = highest median normalized deviation.

## Obs. 3.1 — Which style is the clearest execution-centric overhead profile?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on execution_window_share: p=1.23e-52, epsilon^2=0.041; vs Community: p_adj=4.03e-47, rbc=0.702, vs Custom: p_adj=7.66e-16, rbc=0.880, vs Third-Party: p_adj=8.66e-28, rbc=0.775. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: execution_window_share; winner = highest median execution share.

## Obs. 3.2 — Which style is best characterized by heavy entry plus heavy execution rather than a dominant completion tail?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on pre_invocation_share: p=4.47e-14, epsilon^2=0.011; vs Community: p_adj=4.66e-06, rbc=0.233, vs Custom: p_adj=7.09e-08, rbc=0.605, vs GMD: p_adj=3.36e-24, rbc=0.722. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: pre_invocation_share; winner = highest median heavy-entry/heavy-execution proxy.

## Obs. 3.3 — Which style has a distributed overhead profile rather than a single dominant overhead source?

- Current baseline under validation: `Community`
- Validation status: `Insufficient evidence`
- Favored answer: `Custom`
- Active answer: `Community`
- Validation note: No usable values for validation metric 'max_phase_share'.
- Favored-answer note: Primary measurement: max_phase_share; winner = lowest median maximum phase share.

## Obs. 3.4 — Which style remains a cautious tail-heavy mixed overhead case?

- Current baseline under validation: `Custom`
- Validation status: `Passed`
- Favored answer: `Custom`
- Active answer: `Custom`
- Validation note: Kruskal on post_invocation_share: p=5.47e-89, epsilon^2=0.069; vs Community: p_adj=0.0194, rbc=0.205, vs GMD: p_adj=1.73e-09, rbc=0.665, vs Third-Party: p_adj=3.91e-11, rbc=0.748. stored='Custom', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: post_invocation_share; winner = highest median post-invocation share.

## Obs. 4.1 — Which style currently has the strongest usable run-level verdict rate?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Chi-square for usable_verdict_rate: p=4.89e-17, Cramer's V=0.095; stored='GMD', winner='GMD'.
- Favored-answer note: Primary measurement: usable verdict rate; winner = highest rate.

## Obs. 4.2 — Which style currently has the strongest success rate among usable verdicts?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Chi-square for success_among_usable: p=6.2e-252, Cramer's V=0.386; stored='GMD', winner='GMD'.
- Favored-answer note: Primary measurement: success rate among usable verdicts; winner = highest rate.

## Obs. 4.3 — Do the styles remain deployed in markedly different CI trigger contexts?

- Current baseline under validation: `Yes`
- Validation status: `Passed`
- Favored answer: `Yes`
- Active answer: `Yes`
- Validation note: Chi-square on style × event: p=0, Cramer's V=0.395; stored answer='Yes'.
- Favored-answer note: Primary measurement: trigger-context differentiation; validate a Yes/No claim with chi-square.

## Obs. 4.4 — Which styles remain strongly trigger-conditioned in success behavior?

- Current baseline under validation: `Third-Party and Custom`
- Validation status: `Failed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Trigger-conditioned spread gap=0.3188; stored='Custom', winner='Third-Party'. Primary measurement: trigger-conditioned success-rate spread; winner = largest spread.
- Favored-answer note: Primary measurement: trigger-conditioned success-rate spread; winner = largest spread.
