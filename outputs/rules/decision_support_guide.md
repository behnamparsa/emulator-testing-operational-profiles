# Decision-support guide (profile-derived)

## If the primary objective is predictable feedback
- Prefer **Conditional** when feasible.
- If Conditional is not feasible, use **Community** as the practical fallback and treat it as the higher-tail-risk option.

## If the primary objective is fast first signal
- Prefer **Conditional** when the earliest entry into the instrumentation path matters most.
- For fastest typical end-to-end completion, prefer **Conditional**.

## If the objective includes usable and successful run outcomes
- Prefer **Conditional** for usable-verdict rate and **Conditional** for success among usable outcomes when those remain aligned.

## If the dominant problem is overhead placement rather than median speed
- Treat **Conditional** as the execution-centric case: optimize the main execution path.
- Treat **Conditional** as the distributed-overhead case: inspect entry, execution, and residual tail together.
- Treat **Conditional** as the heavy-entry plus heavy-execution case: optimize provisioning/orchestration and provider-side execution cost.
- Treat **Conditional** as the tail-heavy mixed case: reduce post-execution cleanup/reporting and bespoke orchestration.

