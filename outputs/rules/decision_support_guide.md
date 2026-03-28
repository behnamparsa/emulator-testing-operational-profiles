# Decision-support guide (profile-derived)

## If the primary objective is predictable feedback
- Prefer **GMD** when feasible.
- If GMD is not feasible, use **Community** as the practical fallback and treat it as the higher-tail-risk option.

## If the primary objective is fast first signal
- Prefer **GMD** when the earliest entry into the instrumentation path matters most.
- For fastest typical end-to-end completion, prefer **Community**.

## If the objective includes usable and successful run outcomes
- Prefer **GMD** for usable-verdict rate and **GMD** for success among usable outcomes when those remain aligned.

## If the dominant problem is overhead placement rather than median speed
- Treat **GMD** as the execution-centric case: optimize the main execution path.
- Treat **Community** as the distributed-overhead case: inspect entry, execution, and residual tail together.
- Treat **Third-Party** as the heavy-entry plus heavy-execution case: optimize provisioning/orchestration and provider-side execution cost.
- Treat **Custom** as the tail-heavy mixed case: reduce post-execution cleanup/reporting and bespoke orchestration.

