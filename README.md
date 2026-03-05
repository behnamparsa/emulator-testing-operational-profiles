# Emulator Testing Operational Profiles

This repository automates:
1. Stage 1-4 extraction
2. MainDataset.csv build
3. Deterministic analysis summaries
4. Section V operational profile and decision-support rule generation

## Key output locations
- `data/processed/MainDataset.csv`
- `outputs/profiles/operational_profile.json`
- `outputs/profiles/operational_profile.md`
- `outputs/rules/decision_support_rules.json`
- `outputs/rules/decision_support_rules.md`
- `outputs/reports/latest_refresh_report.md`


## Split-stage GitHub Actions workflow (recommended)

Use `.github/workflows/refresh_split.yml` to run the pipeline in multiple jobs (Stage 1→4, then analysis) using GitHub Actions artifacts as checkpoints. This avoids hosted runner time limits.

Run it from GitHub Actions using **workflow_dispatch** first. After the first successful run, optionally enable the cron schedule in the YAML.
