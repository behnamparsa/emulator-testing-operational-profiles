# Observation validation notes

This file keeps the technical validation notes and favored-answer notes separate from the main operational profile.

## Obs. 1.1 — Which style is the fastest overall operational profile?

- Current baseline under validation: `Community`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Community`
- Validation note: Kruskal on study_run_duration_seconds: p=1.37e-63, epsilon^2=0.038; vs GMD: p_adj=1.9e-11, rbc=0.244, vs Third-Party: p_adj=2.57e-54, rbc=0.406, vs Custom: p_adj=0.00214, rbc=0.284. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_run_duration_seconds; winner = lowest median run duration.

## Obs. 1.2 — Which style shows the clearest fast-entry profile without being the fastest overall finisher?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on study_pre_invocation_selected_stage3_seconds: p=1.49e-10, epsilon^2=0.010; vs Community: p_adj=0.000334, rbc=0.194, vs Third-Party: p_adj=2.08e-33, rbc=0.954, vs Custom: p_adj=0.0784, rbc=0.173. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_pre_invocation_selected_stage3_seconds; winner = lowest median fast-entry metric.

## Obs. 1.3 — Which style is the slowest sustained-execution profile?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on study_invocation_execution_window_selected_stage3_seconds: p=3.28e-38, epsilon^2=0.038; vs Community: p_adj=1.38e-25, rbc=0.616, vs GMD: p_adj=3.11e-09, rbc=0.468, vs Custom: p_adj=2.49e-08, rbc=0.677. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_invocation_execution_window_selected_stage3_seconds; winner = highest median sustained-execution burden.

## Obs. 1.4 — Which style shows a mixed speed profile with competitive entry, middling core path, and a long completion tail?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `Custom`
- Active answer: `GMD`
- Validation note: Kruskal on study_post_invocation_selected_stage3_seconds: p=7.14e-60, epsilon^2=0.060; vs Custom: p_adj=1, rbc=-0.827, vs Community: p_adj=1, rbc=-0.731, vs Third-Party: p_adj=1, rbc=-0.012. stored='GMD', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_post_invocation_selected_stage3_seconds; winner = highest median completion-tail metric.

## Obs. 1.5 — Which style combines a fast core execution profile with a longer residual tail?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `GMD`
- Validation note: Kruskal on study_post_invocation_selected_stage3_seconds: p=1.43e-59, epsilon^2=0.059; vs Community: p_adj=1, rbc=-0.731, vs Third-Party: p_adj=1, rbc=-0.012. stored='GMD', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: study_post_invocation_selected_stage3_seconds; winner = highest median tail metric within the fast-core candidate set.

## Obs. 2.1 — Which style is the most predictable on the main completion-oriented measures?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on predictability loss on study_run_duration_seconds: p=9.76e-111, epsilon^2=0.066; vs Community: p_adj=2.71e-100, rbc=0.772, vs Third-Party: p_adj=7.95e-48, rbc=0.634, vs Custom: p_adj=1.07e-10, rbc=0.669. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_run_duration_seconds; winner = lowest median normalized deviation.

## Obs. 2.2 — Which style is fast in typical terms but predictability-poor?

- Current baseline under validation: `Community`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Community`
- Validation note: Kruskal on predictability loss on study_run_duration_seconds: p=1.81e-100, epsilon^2=0.063; vs GMD: p_adj=9.05e-101, rbc=0.772. stored='Community', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_run_duration_seconds within the two fastest styles by run duration; winner = highest median normalized deviation.

## Obs. 2.3 — Which style carries the strongest absolute tail-risk profile?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on study_run_duration_seconds: p=1.37e-63, epsilon^2=0.038; vs Community: p_adj=1.71e-54, rbc=0.406, vs GMD: p_adj=1.37e-60, rbc=0.717, vs Custom: p_adj=0.0747, rbc=0.147. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: upper-tail burden on study_run_duration_seconds; winner = highest P90.

## Obs. 2.4 — Which style shows a mixed predictability profile that should be interpreted cautiously?

- Current baseline under validation: `Custom`
- Validation status: `Passed`
- Favored answer: `Community`
- Active answer: `Custom`
- Validation note: Kruskal on predictability loss on study_pre_invocation_selected_stage3_seconds: p=3.51e-77, epsilon^2=0.077; vs Community: p_adj=0.997, rbc=-0.299, vs GMD: p_adj=3.17e-10, rbc=0.774, vs Third-Party: p_adj=5.25e-07, rbc=0.623. stored='Custom', winner='Community'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: predictability loss on study_pre_invocation_selected_stage3_seconds; winner = highest median normalized deviation.

## Obs. 3.1 — Which style is the clearest execution-centric overhead profile?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Kruskal on execution_window_share: p=1.97e-43, epsilon^2=0.043; vs Community: p_adj=2.08e-39, rbc=0.712, vs Third-Party: p_adj=1.06e-24, rbc=0.811, vs Custom: p_adj=5.01e-14, rbc=0.907. stored='GMD', winner='GMD'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: execution_window_share; winner = highest median execution share.

## Obs. 3.2 — Which style is best characterized by heavy entry plus heavy execution rather than a dominant completion tail?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Kruskal on pre_invocation_share: p=3.47e-12, epsilon^2=0.012; vs Community: p_adj=9.16e-06, rbc=0.251, vs GMD: p_adj=2.55e-22, rbc=0.771, vs Custom: p_adj=8.1e-08, rbc=0.666. stored='Third-Party', winner='Third-Party'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: pre_invocation_share; winner = highest median heavy-entry/heavy-execution proxy.

## Obs. 3.3 — Which style has a distributed overhead profile rather than a single dominant overhead source?

- Current baseline under validation: `Custom`
- Validation status: `Insufficient evidence`
- Favored answer: `Custom`
- Active answer: `Custom`
- Validation note: No usable values for validation metric 'max_phase_share'.
- Favored-answer note: Primary measurement: max_phase_share; winner = lowest median maximum phase share.

## Obs. 3.4 — Which style remains a cautious tail-heavy mixed overhead case?

- Current baseline under validation: `Custom`
- Validation status: `Passed`
- Favored answer: `Custom`
- Active answer: `Custom`
- Validation note: Kruskal on post_invocation_share: p=1.91e-76, epsilon^2=0.076; vs Community: p_adj=0.00189, rbc=0.317, vs GMD: p_adj=8.9e-11, rbc=0.798, vs Third-Party: p_adj=8.9e-11, rbc=0.812. stored='Custom', winner='Custom'. Fail only when winner changes with significant omnibus + pairwise support and meaningful effect size.
- Favored-answer note: Primary measurement: post_invocation_share; winner = highest median post-invocation share.

## Obs. 4.1 — Which style currently has the strongest usable run-level verdict rate?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Chi-square for usable_verdict_rate: p=2.25e-15, Cramer's V=0.091; stored='GMD', winner='GMD'.
- Favored-answer note: Primary measurement: usable verdict rate; winner = highest rate.

## Obs. 4.2 — Which style currently has the strongest success rate among usable verdicts?

- Current baseline under validation: `GMD`
- Validation status: `Passed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Chi-square for success_among_usable: p=2.03e-247, Cramer's V=0.384; stored='GMD', winner='GMD'.
- Favored-answer note: Primary measurement: success rate among usable verdicts; winner = highest rate.

## Obs. 4.3 — Do the styles remain deployed in markedly different CI trigger contexts?

- Current baseline under validation: `Yes`
- Validation status: `Failed`
- Favored answer: `GMD`
- Active answer: `GMD`
- Validation note: Chi-square on style × event: p=0, Cramer's V=0.397; schedule-share winner='GMD', stored='Yes', top_gap=0.0288.
- Favored-answer note: Primary measurement: schedule-triggered deployment share on first-attempt runs; winner = highest schedule share.

## Obs. 4.4 — Which styles remain strongly trigger-conditioned in success behavior?

- Current baseline under validation: `Third-Party`
- Validation status: `Passed`
- Favored answer: `Third-Party`
- Active answer: `Third-Party`
- Validation note: Trigger-conditioned spread gap=0.3200; stored='Third-Party', winner='Third-Party'. Primary measurement: trigger-conditioned success-rate spread; winner = largest spread.
- Favored-answer note: Primary measurement: trigger-conditioned success-rate spread; winner = largest spread.
