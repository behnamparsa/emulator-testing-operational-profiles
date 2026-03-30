from pathlib import Path
import csv
import pandas as pd


STYLES = ["Community", "Custom", "GMD", "Third-Party"]


def _is_truthy(value: str) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _support_for_style(sig_df: pd.DataFrame, fam_df: pd.DataFrame, style: str) -> tuple[bool, bool]:
    tier1 = bool(((sig_df["qualifies_tier1"]) & (sig_df[style] >= 5)).any()) if style in sig_df.columns else False
    tier2 = bool(((fam_df["qualifies_tier2"]) & (fam_df[style] >= 5)).any()) if style in fam_df.columns else False
    return tier1, tier2


def run_robustness_check(
    main_dataset_csv: Path = Path("data/processed/MainDataset.csv"),
    refreshed_catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"),
    robustness_dir: Path = Path("outputs/robustness_check"),
) -> None:
    robustness_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(main_dataset_csv)
    df = df[df["style"].isin(STYLES)].copy()
    df["Base_bool"] = df["Base"].map(_is_truthy)
    base = df[df["Base_bool"]].copy()
    base["coarsened_family"] = (
        base["study_runner_os_bucket"].astype(str)
        + "|"
        + base["study_job_count_total_bucket"].astype(str)
        + "|"
        + base["study_step_count_total_bucket"].astype(str)
    )

    sig = base.groupby(["study_signature_hash", "coarsened_family", "style"]).size().unstack(fill_value=0).reset_index()
    for style in STYLES:
        if style not in sig.columns:
            sig[style] = 0
    sig["styles_present"] = (sig[STYLES] > 0).sum(axis=1)
    sig["total_records"] = sig[STYLES].sum(axis=1)
    sig["qualifies_tier1"] = (
        (sig["styles_present"] >= 2)
        & (sig["total_records"] >= 20)
        & ((sig[STYLES] >= 5).sum(axis=1) >= 2)
    )
    sig = sig.sort_values(["qualifies_tier1", "total_records"], ascending=[False, False])
    sig.to_csv(robustness_dir / "signature_inventory.csv", index=False)

    fam = base.groupby(["coarsened_family", "style"]).size().unstack(fill_value=0).reset_index()
    for style in STYLES:
        if style not in fam.columns:
            fam[style] = 0
    fam["styles_present"] = (fam[STYLES] > 0).sum(axis=1)
    fam["total_records"] = fam[STYLES].sum(axis=1)
    fam["qualifies_tier2"] = (
        (fam["styles_present"] >= 2)
        & (fam["total_records"] >= 20)
        & ((fam[STYLES] >= 5).sum(axis=1) >= 2)
    )

    catalog = pd.read_csv(refreshed_catalog_csv)
    latest_active_cols = sorted([c for c in catalog.columns if c.startswith("ACTIVE_")])
    answer_col = latest_active_cols[-1] if latest_active_cols else "released_answer"

    key_obs = ["Obs. 1.1", "Obs. 1.2", "Obs. 1.3", "Obs. 2.1", "Obs. 3.1", "Obs. 3.2", "Obs. 4.2", "Obs. 4.3"]
    rows = []
    for obs in key_obs:
        row = catalog[catalog["obs_id"] == obs].iloc[0]
        answer = str(row.get(answer_col, "")) or str(row.get("released_answer", ""))
        if obs == "Obs. 4.3":
            tier1 = bool(((sig["qualifies_tier1"]) & (sig["styles_present"] >= 2)).any())
            tier2 = bool(((fam["qualifies_tier2"]) & (fam["styles_present"] >= 2)).any())
            note = "Cross-style trigger-context distinctness is structurally checkable only where multiple styles overlap within qualifying exact signatures and coarsened families."
        else:
            answer_styles = [part.strip() for part in answer.split("and")]
            support = [_support_for_style(sig, fam, style) for style in answer_styles if style in STYLES]
            tier1 = all(t1 for t1, _ in support) if support else False
            tier2 = all(t2 for _, t2 in support) if support else False
            if tier1 and tier2:
                note = "Current answer style(s) are present in qualifying Tier 1 exact-signature overlap and Tier 2 coarsened-family overlap."
            elif tier1 or tier2:
                note = "Current answer style(s) are visible only in one tier of robustness overlap."
            else:
                note = "Current answer style(s) do not yet have sufficient qualifying overlap for both robustness tiers."

        status = "supported" if tier1 and tier2 else ("partially_supported" if (tier1 or tier2) else "inconclusive")
        rows.append(
            {
                "obs_id": obs,
                "current_reference_answer": answer,
                "tier1_exact_signature_support": "Yes" if tier1 else "No",
                "tier2_coarsened_family_support": "Yes" if tier2 else "No",
                "robustness_status": status,
                "note": note,
            }
        )

    pd.DataFrame(rows).to_csv(robustness_dir / "observation_robustness_check.csv", index=False)

    summary = f"""# Robustness check summary

This folder provides the lightweight refreshable robustness companion for the automated operational profile and decision-support guide.

The repo keeps the **operational profile** and the **decision-support rule set** as the two main outputs, and uses this robustness layer as a methodological companion that re-checks whether key findings remain visible under the paper's two-tier signature logic.

## Current snapshot summary

- Base records evaluated for robustness: **{len(base)}**
- Tier 1 exact-signature candidates with usable overlap: **{int(sig["qualifies_tier1"].sum())}**
- Tier 2 coarsened-family candidates with usable overlap: **{int(fam["qualifies_tier2"].sum())}**

## Two-tier interpretation

- **Tier 1** uses the exact workflow-shape signature (`study_signature_hash`) to locate strict within-signature overlap.
- **Tier 2** uses a coarsened family built from runner OS bucket, job-count bucket, and total step-count bucket.
- The goal is not to replace the main refreshed answers, but to show whether those answers remain visible under narrower workflow-shape conditions.

## Main files

- `signature_inventory.csv` lists the exact signatures, their coarsened family, style counts, and Tier 1 qualification flag.
- `observation_robustness_check.csv` summarizes selected key observations, their current reference answer, and whether the answer remains visible under Tier 1 and Tier 2 overlap.

## Snapshot note

This robustness companion is intentionally lighter than the paper's full robustness section. It is designed to be rerun automatically on each refreshed snapshot so the repo can retain a current robustness view without turning the robustness layer into a third major headline output.
"""
    (robustness_dir / "robustness_summary.md").write_text(summary, encoding="utf-8")


if __name__ == "__main__":
    run_robustness_check()
