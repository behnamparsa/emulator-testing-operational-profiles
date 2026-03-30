# Decision-support rule structure

This file documents the structural logic behind each of the five primary decision-support objectives used by the repo.

## Predictable feedback

- Basis observations: Obs. 2.1, Obs. 2.2
- Paper rationale: The paper’s predictability-first guide prefers the style with the tightest dispersion and lightest relative tails on the main completion-oriented measures.
- Latest recommendation rule: Latest recommendation comes from Obs. 2.1 (most predictable style). Obs. 2.2 still marks Community as the fast-but-less-predictable trade-off.
- First optimization target rule: The repo maps the latest recommendation style to its profile-derived first optimization target.
- Fallback / feasibility rule: The repo carries the paper-style fallback or feasibility condition for this objective as a separate note in the guide and export table.

## Fast first signal

- Basis observations: Obs. 1.2
- Paper rationale: The paper’s guide prefers the clearest fast-entry style when early developer feedback is the main objective.
- Latest recommendation rule: Latest recommendation comes from Obs. 1.2, which captures the clearest fast-entry profile under the normalized entry metric.
- First optimization target rule: The repo maps the latest recommendation style to its profile-derived first optimization target.
- Fallback / feasibility rule: The repo carries the paper-style fallback or feasibility condition for this objective as a separate note in the guide and export table.

## Fastest typical end-to-end completion

- Basis observations: Obs. 1.1
- Paper rationale: The paper’s guide treats the fastest typical end-to-end completion objective as the headline overall-speed recommendation.
- Latest recommendation rule: Latest recommendation comes from Obs. 1.1, which captures the fastest overall operational profile on the repo’s normalized overall-speed metric.
- First optimization target rule: The repo maps the latest recommendation style to its profile-derived first optimization target.
- Fallback / feasibility rule: The repo carries the paper-style fallback or feasibility condition for this objective as a separate note in the guide and export table.

## Usable and successful run outcomes

- Basis observations: Obs. 4.1, Obs. 4.2
- Paper rationale: The paper’s guide combines usable-verdict rate and success rate among usable outcomes when actionability of CI results is the main objective.
- Latest recommendation rule: Latest recommendation is shared by Obs. 4.1 and Obs. 4.2, so GMD remains the strongest actionability-oriented choice.
- First optimization target rule: The repo maps the latest recommendation style to its profile-derived first optimization target.
- Fallback / feasibility rule: The repo carries the paper-style fallback or feasibility condition for this objective as a separate note in the guide and export table.

## Overhead-placement-led optimization

- Basis observations: Obs. 3.1, Obs. 3.2, Obs. 3.3, Obs. 3.4
- Paper rationale: The paper’s guide uses the overhead profile to map an optimization objective to the style whose dominant bottleneck best matches the intended intervention.
- Latest recommendation rule: Latest recommendation is anchored by Obs. 3.1 (GMD) for the execution-centric case, with structural support from Obs. 3.2 (Third-Party), Obs. 3.3 (Custom), and Obs. 3.4 (Custom).
- First optimization target rule: The repo maps the latest recommendation style to its profile-derived first optimization target.
- Fallback / feasibility rule: The repo carries the paper-style fallback or feasibility condition for this objective as a separate note in the guide and export table.
