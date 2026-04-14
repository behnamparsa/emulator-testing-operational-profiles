from __future__ import annotations

from pathlib import Path
import sys
from typing import Tuple
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from profile_qa.item_logic import evaluate_item, validate_stored_answer, normalize_style_answer

STYLES = ["Community", "GMD", "Third-Party", "Custom"]
KEY_OBS = ["Obs. 1.1", "Obs. 1.2", "Obs. 2.2", "Obs. 1.3", "Obs. 2.1", "Obs. 3.1", "Obs. 3.2", "Obs. 4.2", "Obs. 4.3"]
MIN_TOTAL = 20
MIN_STYLE_COUNT = 5
MIN_STYLES_PRESENT = 2
MIN_STATS_ROWS = 25
MIN_STATS_PER_STYLE = 5

def _is_truthy(value) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes", "y"}

def _coarsened_family(df: pd.DataFrame) -> pd.Series:
    if "Coarsened-family" in df.columns:
        return df["Coarsened-family"].astype(str)
    return df["study_runner_os_bucket"].astype(str) + "|" + df["study_job_count_exec_bucket"].astype(str) + "|" + df["study_step_count_exec_bucket"].astype(str)

def _prepare_regime_frames(df: pd.DataFrame):
    full = df[df["style"].isin(STYLES)].copy()
    full["Base_bool"] = full["Base_timing_regime"].map(_is_truthy) if "Base_timing_regime" in full.columns else False
    full["FirstAttempt_bool"] = full["controller_attempt_eq_1"].map(_is_truthy) if "controller_attempt_eq_1" in full.columns else pd.to_numeric(full.get("run_attempt", 1), errors="coerce").fillna(0).eq(1)
    full["coarsened_family"] = _coarsened_family(full)
    return {"base": full[full["Base_bool"]].copy(), "first_attempt": full[full["FirstAttempt_bool"]].copy()}

def _build_inventory(regime_name: str, subset: pd.DataFrame):
    if subset.empty:
        return pd.DataFrame(), pd.DataFrame()
    sig = subset.groupby(["study_signature_hash", "coarsened_family", "style"]).size().unstack(fill_value=0).reset_index()
    for style in STYLES:
        if style not in sig.columns: sig[style] = 0
    sig["regime"] = regime_name
    sig["styles_present"] = (sig[STYLES] > 0).sum(axis=1)
    sig["total_records"] = sig[STYLES].sum(axis=1)
    if "eligible_signature" in subset.columns:
        eligible = subset.groupby("study_signature_hash")["eligible_signature"].apply(lambda s: any(map(_is_truthy, s))).to_dict()
        sig["qualifies_tier1"] = sig["study_signature_hash"].map(lambda x: bool(eligible.get(x, False)))
    else:
        sig["qualifies_tier1"] = ((sig["styles_present"] >= MIN_STYLES_PRESENT) & (sig["total_records"] >= MIN_TOTAL) & (((sig[STYLES] >= MIN_STYLE_COUNT).sum(axis=1)) >= MIN_STYLES_PRESENT))
    fam = subset.groupby(["coarsened_family", "style"]).size().unstack(fill_value=0).reset_index()
    for style in STYLES:
        if style not in fam.columns: fam[style] = 0
    fam["regime"] = regime_name
    fam["styles_present"] = (fam[STYLES] > 0).sum(axis=1)
    fam["total_records"] = fam[STYLES].sum(axis=1)
    fam["qualifies_tier2"] = ((fam["styles_present"] >= MIN_STYLES_PRESENT) & (fam["total_records"] >= MIN_TOTAL) & (((fam[STYLES] >= MIN_STYLE_COUNT).sum(axis=1)) >= MIN_STYLES_PRESENT))
    return sig, fam

def _obs_regime(obs_id: str) -> str:
    return "first_attempt" if obs_id.startswith("Obs. 4.") else "base"

def _reference_match(answer: str, winner: str) -> bool:
    styles = normalize_style_answer(answer)
    return winner in styles if styles else answer == winner

def _tier_presence(sig: pd.DataFrame, fam: pd.DataFrame, answer: str) -> Tuple[bool, bool]:
    answer_styles = normalize_style_answer(answer)
    if not answer_styles:
        return False, False
    tier1 = all(bool(((sig["qualifies_tier1"]) & (sig[s] >= MIN_STYLE_COUNT)).any()) if s in sig.columns and not sig.empty else False for s in answer_styles)
    tier2 = all(bool(((fam["qualifies_tier2"]) & (fam[s] >= MIN_STYLE_COUNT)).any()) if s in fam.columns and not fam.empty else False for s in answer_styles)
    return tier1, tier2

def _qualifying_subsets(subset: pd.DataFrame, sig: pd.DataFrame, fam: pd.DataFrame):
    qsig = sig.loc[sig["qualifies_tier1"], "study_signature_hash"].astype(str).tolist() if not sig.empty else []
    qfam = fam.loc[fam["qualifies_tier2"], "coarsened_family"].astype(str).tolist() if not fam.empty else []
    return (subset[subset["study_signature_hash"].astype(str).isin(qsig)].copy() if qsig else subset.iloc[0:0].copy(), subset[subset["coarsened_family"].astype(str).isin(qfam)].copy() if qfam else subset.iloc[0:0].copy())

def _enough_for_stats(subset: pd.DataFrame) -> bool:
    return (not subset.empty) and (len(subset) >= MIN_STATS_ROWS) and int((subset.groupby("style").size() >= MIN_STATS_PER_STYLE).sum()) >= 2

def _directional_status(obs_id: str, subset: pd.DataFrame, answer: str):
    if subset.empty: return "inconclusive", "No qualifying overlap subset available."
    result = evaluate_item(obs_id, "", subset)
    if result.winner == "Insufficient evidence": return "inconclusive", f"No usable directional winner inside the robustness subset. {result.note}"
    return ("supported", f"Directional winner inside the robustness subset remains {result.winner}.") if _reference_match(answer, result.winner) else ("not_supported", f"Directional winner inside the robustness subset is {result.winner}, which differs from the current reference answer {answer}.")

def _statistical_status(obs_id: str, row, subset: pd.DataFrame, answer: str):
    if not _enough_for_stats(subset): return "not_feasible", "Subset support is too small for a stable automated statistical robustness check."
    status, note, _ = validate_stored_answer(row, subset, answer)
    return ("supported" if status == "Passed" else "not_supported" if status == "Failed" else "inconclusive"), note

def _overall_status(struct_t1, struct_t2, dir1, dir2, stat1, stat2):
    if stat1 == "supported" or stat2 == "supported": return "supported_statistically"
    if dir1 == "supported" and dir2 == "supported" and struct_t1 and struct_t2: return "supported_directionally"
    if struct_t1 or struct_t2: return "supported_structurally"
    if dir1 == "not_supported" and dir2 == "not_supported": return "not_supported"
    return "inconclusive"

def run_robustness_check(main_dataset_csv: Path = Path("data/processed/MainDataset.csv"), refreshed_catalog_csv: Path = Path("outputs/catalog/observation_qa_catalog_refreshed.csv"), robustness_dir: Path = Path("outputs/robustness_check")) -> None:
    robustness_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(main_dataset_csv)
    catalog = pd.read_csv(refreshed_catalog_csv)
    latest_active_cols = sorted([c for c in catalog.columns if c.startswith("ACTIVE_")])
    answer_col = latest_active_cols[-1] if latest_active_cols else "released_answer"
    regimes = _prepare_regime_frames(df)
    sig_all=[]; fam_all=[]; regime_stats={}
    for regime_name, subset in regimes.items():
        sig, fam = _build_inventory(regime_name, subset)
        sig_all.append(sig); fam_all.append(fam)
        tier1_df, tier2_df = _qualifying_subsets(subset, sig, fam)
        regime_stats[regime_name] = {"subset": subset, "sig": sig, "fam": fam, "tier1_df": tier1_df, "tier2_df": tier2_df}
    pd.concat(sig_all, ignore_index=True).sort_values(["regime", "qualifies_tier1", "total_records"], ascending=[True, False, False]).to_csv(robustness_dir / "signature_inventory.csv", index=False)
    pd.concat(fam_all, ignore_index=True).sort_values(["regime", "qualifies_tier2", "total_records"], ascending=[True, False, False]).to_csv(robustness_dir / "coarsened_family_inventory.csv", index=False)
    out_rows=[]
    for obs in KEY_OBS:
        row = catalog[catalog["obs_id"] == obs].iloc[0].to_dict()
        answer = str(row.get(answer_col, "")) or str(row.get("released_answer", ""))
        rg = regime_stats[_obs_regime(obs)]
        struct_t1, struct_t2 = _tier_presence(rg["sig"], rg["fam"], answer)
        dir1, note1 = _directional_status(obs, rg["tier1_df"], answer)
        dir2, note2 = _directional_status(obs, rg["tier2_df"], answer)
        stat1, stat_note1 = _statistical_status(obs, row, rg["tier1_df"], answer)
        stat2, stat_note2 = _statistical_status(obs, row, rg["tier2_df"], answer)
        out_rows.append({"obs_id": obs, "regime": _obs_regime(obs), "current_reference_answer": answer, "tier1_exact_signature_structural_support": "Yes" if struct_t1 else "No", "tier2_coarsened_family_structural_support": "Yes" if struct_t2 else "No", "tier1_directional_support": dir1, "tier1_directional_note": note1, "tier2_directional_support": dir2, "tier2_directional_note": note2, "tier1_statistical_support": stat1, "tier1_statistical_note": stat_note1, "tier2_statistical_support": stat2, "tier2_statistical_note": stat_note2, "robustness_status": _overall_status(struct_t1, struct_t2, dir1, dir2, stat1, stat2)})
    pd.DataFrame(out_rows).to_csv(robustness_dir / "observation_robustness_check.csv", index=False)
    base_info=regime_stats["base"]; fa_info=regime_stats["first_attempt"]
    summary = f"""# Robustness check summary

- Base records evaluated for robustness: **{len(base_info['subset'])}**
- First-attempt records evaluated for robustness: **{len(fa_info['subset'])}**
- Tier 1 exact-signature candidates with usable overlap (Base): **{int(base_info['sig']['qualifies_tier1'].sum()) if not base_info['sig'].empty else 0}**
- Tier 2 coarsened-family candidates with usable overlap (Base): **{int(base_info['fam']['qualifies_tier2'].sum()) if not base_info['fam'].empty else 0}**
- Tier 1 exact-signature candidates with usable overlap (first-attempt): **{int(fa_info['sig']['qualifies_tier1'].sum()) if not fa_info['sig'].empty else 0}**
- Tier 2 coarsened-family candidates with usable overlap (first-attempt): **{int(fa_info['fam']['qualifies_tier2'].sum()) if not fa_info['fam'].empty else 0}**
"""
    (robustness_dir / "robustness_summary.md").write_text(summary, encoding="utf-8")

if __name__ == "__main__":
    run_robustness_check()
