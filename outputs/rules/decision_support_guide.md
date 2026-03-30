# Decision-support guide (profile-derived)

This guide preserves the paper baseline recommendation, appends the latest snapshot recommendation, and records the first optimization target and feasibility note behind each primary objective.

## Predictable feedback

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Paper rationale: The paper’s predictability-first guide prefers the style with the tightest dispersion and lightest relative tails on the main completion-oriented measures.
- Latest rationale: Latest recommendation comes from Obs. 2.1 (most predictable style). Obs. 2.2 still marks Community as the fast-but-less-predictable trade-off.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If GMD is not feasible, Community remains the practical fallback but should be treated as higher tail-risk.
- Structural basis: Obs. 2.1, Obs. 2.2

## Fast first signal

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Paper rationale: The paper’s guide prefers the clearest fast-entry style when early developer feedback is the main objective.
- Latest rationale: Latest recommendation comes from Obs. 1.2, which captures the clearest fast-entry profile under the normalized entry metric.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If the recommended fast-entry style is not feasible, use Community as the practical fallback; do not choose Third-Party on early-feedback grounds alone.
- Structural basis: Obs. 1.2

## Fastest typical end-to-end completion

- Paper baseline recommendation: `Community`
- Latest snapshot recommendation: `Community`
- Paper rationale: The paper’s guide treats the fastest typical end-to-end completion objective as the headline overall-speed recommendation.
- Latest rationale: Latest recommendation comes from Obs. 1.1, which captures the fastest overall operational profile on the repo’s normalized overall-speed metric.
- First optimization target: Stabilize entry/setup variability and reduce execution-path cost; then inspect the remaining residual tail.
- Fallback / feasibility note: If predictability matters almost as much as median speed and GMD is feasible, prefer GMD as the safer trade-off.
- Structural basis: Obs. 1.1

## Usable and successful run outcomes

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Paper rationale: The paper’s guide combines usable-verdict rate and success rate among usable outcomes when actionability of CI results is the main objective.
- Latest rationale: Latest recommendation is shared by Obs. 4.1 and Obs. 4.2, so GMD remains the strongest actionability-oriented choice.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: Community remains the general-purpose fallback with broader practical coverage; treat Third-Party and Custom as trigger-sensitive.
- Structural basis: Obs. 4.1, Obs. 4.2

## Overhead-placement-led optimization

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Paper rationale: The paper’s guide uses the overhead profile to map an optimization objective to the style whose dominant bottleneck best matches the intended intervention.
- Latest rationale: Latest recommendation is anchored by Obs. 3.1 (GMD) for the execution-centric case, with structural support from Obs. 3.2 (Third-Party), Obs. 3.3 (Custom), and Obs. 3.4 (Custom).
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If the local bottleneck is not execution-centric, consult the structural notes for Third-Party (entry + execution), Community (distributed), and Custom (tail-heavy) before applying the recommendation.
- Structural basis: Obs. 3.1, Obs. 3.2, Obs. 3.3, Obs. 3.4
