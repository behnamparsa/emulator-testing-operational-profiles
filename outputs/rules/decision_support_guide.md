# Decision-support guide (profile-derived)

The guidance below preserves the paper baseline recommendation and appends a refreshed recommendation from the latest active answers, together with the currently detected bottleneck and the first improvement focus for that rule–style combination.

## If the primary objective is predictable feedback

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Why this recommendation: Latest recommendation comes from Obs. 2.1, Obs. 2.2 using the current active answers in the refreshed catalog.
- Current bottleneck emphasis: **execution_path**
- First optimization target: **Execution-path stabilization**
- Improvement suggestion: Focus first on execution efficiency inside the main execution path, including test efficiency, parallelization, flake reduction, and execution simplification.

## If the primary objective is fast first signal

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Why this recommendation: Latest recommendation comes from Obs. 1.2 using the current active answers in the refreshed catalog.
- Current bottleneck emphasis: **entry_setup**
- First optimization target: **Fast-entry tuning**
- Improvement suggestion: Minimize pre-invocation setup and keep the entry path lightweight and reproducible.

## If the primary objective is fastest typical end-to-end completion

- Paper baseline recommendation: **Community**
- Latest snapshot recommendation: **Community**
- Why this recommendation: Latest recommendation comes from Obs. 1.1 using the current active answers in the refreshed catalog.
- Current bottleneck emphasis: **post_execution_tail**
- First optimization target: **Tail reduction**
- Improvement suggestion: Reduce the completion-tail work first because the end-to-end profile is slowed most by the post-execution segment.

## If the primary objective is usable and successful run outcomes

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Why this recommendation: Latest recommendation comes from Obs. 4.1, Obs. 4.2, Obs. 4.4 using the current active answers in the refreshed catalog.
- Current bottleneck emphasis: **reliability_outcome**
- First optimization target: **Reliability preservation**
- Improvement suggestion: Preserve the strong usable/success profile by focusing on failure prevention in the execution path and maintaining stable configuration.

## If the primary objective is overhead-placement-led optimization

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Why this recommendation: Latest recommendation comes from Obs. 3.1, Obs. 3.2, Obs. 3.3, Obs. 3.4 using the current active answers in the refreshed catalog.
- Current bottleneck emphasis: **execution_path**
- First optimization target: **Execution-centric optimization**
- Improvement suggestion: Treat the problem as primarily execution-centric and focus on the execution path itself.
