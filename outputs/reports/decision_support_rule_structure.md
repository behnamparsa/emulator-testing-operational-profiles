# Decision-support rule structure

This file documents the structural logic used by the repo to refresh the five decision-support rules.

## Predictable feedback

- Basis observations: Obs. 2.1, Obs. 2.2
- Paper rationale: The paper positions GMD as the predictability-first profile and Community as the faster but more variable alternative.
- Latest recommendation rule: Prefer the current active answer from Obs. 2.1; use Obs. 2.2 as supporting context for the speed-versus-variability trade-off.
- Bottleneck detection rule: detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: use the first matching guidance row in the order (rule, style, bottleneck) → (*, style, bottleneck) → (rule, style, *) → (*, style, *).

## Fast first signal

- Basis observations: Obs. 1.2
- Paper rationale: The paper treats GMD as the clearest fast-entry style under the speed profile.
- Latest recommendation rule: Use the current active answer from Obs. 1.2.
- Bottleneck detection rule: detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: use the first matching guidance row in the order (rule, style, bottleneck) → (*, style, bottleneck) → (rule, style, *) → (*, style, *).

## Fastest typical end-to-end completion

- Basis observations: Obs. 1.1
- Paper rationale: The paper treats Community as the fastest overall completion profile.
- Latest recommendation rule: Use the current active answer from Obs. 1.1.
- Bottleneck detection rule: detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: use the first matching guidance row in the order (rule, style, bottleneck) → (*, style, bottleneck) → (rule, style, *) → (*, style, *).

## Usable and successful run outcomes

- Basis observations: Obs. 4.1, Obs. 4.2, Obs. 4.4
- Paper rationale: The paper combines usable-verdict rate, success rate among usable outcomes, and trigger-conditioned outcome context.
- Latest recommendation rule: Prefer the current active answer from Obs. 4.2, use Obs. 4.1 as supporting context, and use Obs. 4.4 to identify trigger-conditioned reliability issues.
- Bottleneck detection rule: detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: use the first matching guidance row in the order (rule, style, bottleneck) → (*, style, bottleneck) → (rule, style, *) → (*, style, *).

## Overhead-placement-led optimization

- Basis observations: Obs. 3.1, Obs. 3.2, Obs. 3.3, Obs. 3.4
- Paper rationale: The paper maps styles to dominant overhead placements and then translates those placements into first optimization targets.
- Latest recommendation rule: Use the current active answer across the RQ3 observations to identify the dominant style-level bottleneck driving the optimization recommendation.
- Bottleneck detection rule: detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: use the first matching guidance row in the order (rule, style, bottleneck) → (*, style, bottleneck) → (rule, style, *) → (*, style, *).
