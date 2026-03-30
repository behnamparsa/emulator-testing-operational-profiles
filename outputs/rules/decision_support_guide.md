# Decision-support guide (profile-derived)

This guide preserves the paper baseline recommendation, adds the latest refreshed recommendation, and pairs it with the latest refreshed bottleneck and first optimization target.

## Predictable feedback

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck behind the recommendation: execution path
- Why this recommendation: Latest recommendation follows the current active answer(s) behind this objective. Structural basis: Obs. 2.1 → GMD; Obs. 2.2 → Community. Current bottleneck family for GMD: execution path.
- First optimization target: Optimize the execution path itself: improve test efficiency, reduce flakes, simplify execution, and tune parallelization inside the managed-device workflow.
- Fallback / feasibility note: If GMD is not feasible, use Community as the practical fallback, but treat it as the higher-variability alternative and focus first on its distributed overhead bottleneck.

## Fast first signal

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck behind the recommendation: entry setup
- Why this recommendation: Latest recommendation follows the current active answer(s) behind this objective. Structural basis: Obs. 1.2 → GMD; Obs. 1.1 → Community. Current bottleneck family for GMD: entry setup.
- First optimization target: Keep the fast-entry advantage by minimizing pre-test provisioning churn and avoiding unnecessary environment work before the managed-device path starts.
- Fallback / feasibility note: If GMD is not feasible, fall back to Community as the fastest-overall style, but accept that its first-signal profile differs and focus on its entry setup bottleneck.

## Fastest typical end-to-end completion

- Paper baseline recommendation: **Community**
- Latest snapshot recommendation: **Community**
- Current bottleneck behind the recommendation: post execution tail
- Why this recommendation: Latest recommendation follows the current active answer(s) behind this objective. Structural basis: Obs. 1.1 → Community; Obs. 2.1 → GMD. Current bottleneck family for Community: post execution tail.
- First optimization target: Inspect residual tail work after execution, especially reporting, artifact handling, and late cleanup steps.
- Fallback / feasibility note: If Community is not feasible or predictability matters nearly as much as speed, use GMD as the safer fallback and tune its execution path bottleneck first.

## Usable and successful run outcomes

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck behind the recommendation: reliability outcome
- Why this recommendation: Latest recommendation follows the current active answer(s) behind this objective. Structural basis: Obs. 4.2 → GMD; Obs. 4.1 → GMD; Obs. 4.4 → Third-Party. Current bottleneck family for GMD: reliability outcome.
- First optimization target: Preserve the strong outcome profile by focusing on reliable execution and stable environment setup in the trigger regimes where GMD is currently used.
- Fallback / feasibility note: If GMD is not feasible, keep the next closest style-level profile in mind and focus on the currently detected reliability outcome bottleneck.

## Overhead-placement-led optimization

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck behind the recommendation: execution path
- Why this recommendation: Latest recommendation follows the refreshed overhead profile, with GMD selected from the current active overhead observation set. Structural basis: Obs. 3.1 → GMD; Obs. 3.2 → Third-Party; Obs. 3.3 → Custom; Obs. 3.4 → Custom. Current bottleneck family: execution path.
- First optimization target: Optimize the execution path itself: improve test efficiency, reduce flakes, simplify execution, and tune parallelization inside the managed-device workflow.
- Fallback / feasibility note: If GMD is not feasible, choose the style that shows the same bottleneck family in the latest overhead observations and focus on that execution path path first.
