# Decision-support guide (profile-derived)

This guide keeps the paper baseline recommendation and the latest snapshot recommendation, then presents the practical guidance in a bulletpoint style closer to the paper's decision-support figure.

## Predictable feedback

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Why this recommendation: Latest recommendation comes from Obs. 2.1 (most predictable style). Obs. 2.2 still marks Community as the fast-but-less-predictable trade-off.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If GMD is not feasible, Community remains the practical fallback but should be treated as higher tail-risk.

## Fast first signal

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Why this recommendation: Latest recommendation comes from Obs. 1.2, which captures the clearest fast-entry profile under the normalized entry metric.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If the recommended fast-entry style is not feasible, use Community as the practical fallback; do not choose Third-Party on early-feedback grounds alone.

## Fastest typical end-to-end completion

- Paper baseline recommendation: `Community`
- Latest snapshot recommendation: `Community`
- Why this recommendation: Latest recommendation comes from Obs. 1.1, which captures the fastest overall operational profile on the repo’s normalized overall-speed metric.
- First optimization target: Stabilize entry/setup variability and reduce execution-path cost; then inspect the remaining residual tail.
- Fallback / feasibility note: If predictability matters almost as much as median speed and GMD is feasible, prefer GMD as the safer trade-off.

## Usable and successful run outcomes

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Why this recommendation: Latest recommendation is shared by Obs. 4.1 and Obs. 4.2, so GMD remains the strongest actionability-oriented choice.
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: Community remains the general-purpose fallback with broader practical coverage; treat Third-Party and Custom as trigger-sensitive.

## Overhead-placement-led optimization

- Paper baseline recommendation: `GMD`
- Latest snapshot recommendation: `GMD`
- Why this recommendation: Latest recommendation is anchored by Obs. 3.1 (GMD) for the execution-centric case, with structural support from Obs. 3.2 (Third-Party), Obs. 3.3 (Custom), and Obs. 3.4 (Custom).
- First optimization target: Optimize the execution path itself, including test efficiency, parallelization, flake reduction, and execution simplification.
- Fallback / feasibility note: If the local bottleneck is not execution-centric, consult the structural notes for Third-Party (entry + execution), Community (distributed), and Custom (tail-heavy) before applying the recommendation.
