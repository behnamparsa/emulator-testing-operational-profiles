# Decision-support guide (profile-derived)

This guide preserves the paper baseline recommendation and appends a refreshed recommendation from the latest active answers, together with the currently detected bottleneck and the first improvement focus for that rule–style combination.  
For the structural selection logic, priority order, bottleneck detection, and guidance lookup method, see:
- `outputs/reports/decision_support_rule_structure.md`

## If the primary objective is predictable feedback

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck emphasis: **execution_path**
- First optimization target: **Execution-path stabilization**
- Improvement suggestion: Focus first on execution efficiency inside the main execution path, including test efficiency, parallelization, flake reduction, and execution simplification.

## If the primary objective is fast first signal

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck emphasis: **entry_setup**
- First optimization target: **Fast-entry tuning**
- Improvement suggestion: Minimize pre-invocation setup and keep the entry path lightweight and reproducible.

## If the primary objective is fastest typical end-to-end completion

- Paper baseline recommendation: **Community**
- Latest snapshot recommendation: **Community**
- Current bottleneck emphasis: **distributed_overhead**
- First optimization target: **End-to-end speed tuning**
- Improvement suggestion: Stabilize entry/setup variability and reduce execution-path cost; then inspect the remaining residual tail.

## If the primary objective is usable and successful run outcomes

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck emphasis: **reliability_outcome**
- First optimization target: **Reliability preservation**
- Improvement suggestion: Preserve the strong usable/success profile by focusing on failure prevention in the execution path and maintaining stable configuration.

## If the primary objective is overhead-placement-led optimization

- Paper baseline recommendation: **GMD**
- Latest snapshot recommendation: **GMD**
- Current bottleneck emphasis: **execution_path**
- First optimization target: **Execution-centric optimization**
- Improvement suggestion: Treat the problem as primarily execution-centric and focus on the execution path itself.
