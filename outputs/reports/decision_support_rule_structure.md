# Decision-support rule structure

This file documents the structural logic used by the repo to refresh the five decision-support rules.

Priority notation used below:
- `A` = primary recommendation source
- `A > B` = use `A` first; use `B` only if `A` is unavailable
- `A + B (support)` = `A` determines the recommendation; `B` is supporting context for interpretation
- `A > B + C (support)` = use `A` first, then `B` as fallback; use `C` only as supporting context

## Predictable feedback

- Priority structure: **Obs. 2.1 + Obs. 2.2 (support)**
- Primary recommendation source: **Obs. 2.1**
- Supporting context: **Obs. 2.2**
- Paper rationale: The paper positions GMD as the predictability-first profile and Community as the faster but more variable alternative.
- Latest recommendation rule: Use the current active answer from **Obs. 2.1** as the latest recommendation. Use **Obs. 2.2** only to explain the speed-versus-variability trade-off; it does not override the recommendation on its own.
- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.

## Fast first signal

- Priority structure: **Obs. 1.2**
- Primary recommendation source: **Obs. 1.2**
- Paper rationale: The paper treats GMD as the clearest fast-entry style under the speed profile.
- Latest recommendation rule: Use the current active answer from **Obs. 1.2**.
- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.

## Fastest typical end-to-end completion

- Priority structure: **Obs. 1.1**
- Primary recommendation source: **Obs. 1.1**
- Paper rationale: The paper treats Community as the fastest overall completion profile.
- Latest recommendation rule: Use the current active answer from **Obs. 1.1**.
- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.

## Usable and successful run outcomes

- Priority structure: **Obs. 4.2 > Obs. 4.1 + Obs. 4.4 (support)**
- Primary recommendation source: **Obs. 4.2**
- Fallback recommendation source: **Obs. 4.1**
- Supporting context: **Obs. 4.4**
- Paper rationale: The paper combines usable-verdict rate, success rate among usable outcomes, and trigger-conditioned outcome context.
- Latest recommendation rule: Prefer the current active answer from **Obs. 4.2**. If **Obs. 4.2** is unavailable, use **Obs. 4.1** as the fallback recommendation source. Use **Obs. 4.4** only as supporting context to describe trigger-conditioned reliability issues; it does not override the recommendation on its own.
- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.

## Overhead-placement-led optimization

- Priority structure: **Obs. 3.1 > Obs. 3.2 > Obs. 3.3 > Obs. 3.4**
- Primary recommendation source: **Obs. 3.1**
- Fallback recommendation sources: **Obs. 3.2**, then **Obs. 3.3**, then **Obs. 3.4**
- Paper rationale: The paper maps styles to dominant overhead placements and then translates those placements into first optimization targets.
- Latest recommendation rule: Use the first non-empty current active answer in this order: **Obs. 3.1**, then **Obs. 3.2**, then **Obs. 3.3**, then **Obs. 3.4**. Earlier observations in the sequence have higher priority than later ones.
- Bottleneck detection rule: Detect the current bottleneck label for the latest recommended style using the rule-aware bottleneck mapper.
- Guidance lookup rule: Use the first matching guidance row in the order `(rule, style, bottleneck)` → `(*, style, bottleneck)` → `(rule, style, *)` → `(*, style, *)`.
