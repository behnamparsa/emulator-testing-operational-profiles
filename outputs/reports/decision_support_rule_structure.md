# Decision-support rule structure

This file documents the structural schema used to regenerate the five decision-support rules from the latest refreshed profile.

## Predictable feedback

- Basis observations: Obs. 2.1, Obs. 2.2
- Paper rationale: The paper pairs predictability-first guidance with GMD's stability profile and uses Community as the fast-but-variable counterpoint.
- Latest recommendation rule: Use the current active answer from Obs. 2.1 as the primary recommendation; use Obs. 2.2 to explain the trade-off against the fast-but-variable alternative.
- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `execution_path` for latest recommendation `GMD`.
- First optimization target rule: Map (`GMD`, `execution_path`) to the style-and-bottleneck suggestion dictionary.
- Fallback / feasibility rule: If the latest recommendation is not feasible, fall back to the fast-but-variable alternative indicated by Obs. 2.2 when it differs from the recommendation; otherwise keep the current recommended style.

## Fast first signal

- Basis observations: Obs. 1.2, Obs. 1.1
- Paper rationale: The paper associates fast first signal with GMD's entry advantage, while still distinguishing it from overall completion speed.
- Latest recommendation rule: Use the current active answer from Obs. 1.2 as the primary recommendation; use Obs. 1.1 to explain whether that style also wins or loses on overall completion.
- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `entry_setup` for latest recommendation `GMD`.
- First optimization target rule: Map (`GMD`, `entry_setup`) to the style-and-bottleneck suggestion dictionary.
- Fallback / feasibility rule: If the latest recommendation is not feasible, fall back to the fastest-overall style from Obs. 1.1 when it differs; otherwise keep the current recommended style.

## Fastest typical end-to-end completion

- Basis observations: Obs. 1.1, Obs. 2.1
- Paper rationale: The paper ties fastest typical completion to Community, while using GMD as the safer trade-off when predictability matters almost as much as speed.
- Latest recommendation rule: Use the current active answer from Obs. 1.1 as the primary recommendation; use Obs. 2.1 to describe the predictability trade-off.
- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `post_execution_tail` for latest recommendation `Community`.
- First optimization target rule: Map (`Community`, `post_execution_tail`) to the style-and-bottleneck suggestion dictionary.
- Fallback / feasibility rule: If the latest recommendation is not feasible, fall back to the predictability-first style from Obs. 2.1 when it differs; otherwise keep the current recommended style.

## Usable and successful run outcomes

- Basis observations: Obs. 4.2, Obs. 4.1, Obs. 4.4
- Paper rationale: The paper prefers GMD for usable and successful outcomes, while also using verdict- and trigger-conditioned observations to qualify that recommendation.
- Latest recommendation rule: Use the current active answer from Obs. 4.2 as the primary recommendation; use Obs. 4.1 and Obs. 4.4 to explain usable-verdict and trigger-conditioned context.
- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `reliability_outcome` for latest recommendation `GMD`.
- First optimization target rule: Map (`GMD`, `reliability_outcome`) to the style-and-bottleneck suggestion dictionary.
- Fallback / feasibility rule: If the latest recommendation is not feasible, fall back to the strongest usable-verdict style from Obs. 4.1 when it differs; otherwise keep the current recommended style and inspect Obs. 4.4 for trigger-conditioned caveats.

## Overhead-placement-led optimization

- Basis observations: Obs. 3.1, Obs. 3.2, Obs. 3.3, Obs. 3.4
- Paper rationale: The paper's overhead guide is style-by-bottleneck rather than a single global winner; the repo flattens that into a primary recommended style plus a refreshed bottleneck-based lever.
- Latest recommendation rule: Use the current active answer from Obs. 3.1 as the primary recommendation, but derive the current bottleneck from the latest active overhead observations across Obs. 3.1–3.4.
- Current bottleneck rule: Detect the refreshed bottleneck label for the latest recommended style; current label = `execution_path` for latest recommendation `GMD`.
- First optimization target rule: Map (`GMD`, `execution_path`) to the style-and-bottleneck suggestion dictionary.
- Fallback / feasibility rule: If the latest recommendation is not feasible, fall back to the style whose current overhead observation best matches the same bottleneck family; otherwise keep the current recommended style.
