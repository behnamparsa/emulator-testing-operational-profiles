from __future__ import annotations

from pathlib import Path
from typing import List, Dict
import json

from .io_utils import read_csv_rows


def _latest_column(prefix: str, columns: List[str]) -> str | None:
    matches = sorted([c for c in columns if c.startswith(prefix)])
    return matches[-1] if matches else None


def regenerate_from_catalog(refreshed_catalog_csv: Path, profile_md: Path, profile_json: Path, rules_md: Path, rules_json: Path, report_md: Path) -> None:
    rows: List[Dict[str, str]] = read_csv_rows(refreshed_catalog_csv)
    cols = list(rows[0].keys()) if rows else []
    l1_col = _latest_column('L1_validate_', cols)
    l2_col = _latest_column('L2_answer_', cols)

    profile = []
    rules = []
    report_lines = ['# Latest refresh report', '']
    for row in rows:
        profile.append({
            'rq_id': row['rq_id'],
            'obs_id': row['obs_id'],
            'question': row['question_text'],
            'released_answer': row['released_answer'],
            'validation_status': row.get(l1_col, ''),
            'current_answer': row.get(l2_col, ''),
        })
        rules.append({
            'question': row['question_text'],
            'current_answer': row.get(l2_col, ''),
        })
        report_lines.append(f"- {row['rq_id']} Obs. {row['obs_id']}: released=`{row['released_answer']}`, validation=`{row.get(l1_col, '')}`, current=`{row.get(l2_col, '')}`")

    profile_md.parent.mkdir(parents=True, exist_ok=True)
    rules_md.parent.mkdir(parents=True, exist_ok=True)
    report_md.parent.mkdir(parents=True, exist_ok=True)
    profile_json.write_text(json.dumps(profile, indent=2), encoding='utf-8')
    rules_json.write_text(json.dumps(rules, indent=2), encoding='utf-8')
    profile_md.write_text('# Operational profile (regenerated)

' + '
'.join([f"- {p['rq_id']} Obs. {p['obs_id']}: {p['question']} -> {p['current_answer']}" for p in profile]) + '
', encoding='utf-8')
    rules_md.write_text('# Decision-support rules (regenerated)

' + '
'.join([f"- {r['question']} -> {r['current_answer']}" for r in rules]) + '
', encoding='utf-8')
    report_md.write_text('
'.join(report_lines) + '
', encoding='utf-8')
