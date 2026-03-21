from __future__ import annotations

from typing import Any


def build_source_candidates(
    *,
    tables: list[dict[str, Any]] | None = None,
    kv_pairs: list[dict[str, Any]] | None = None,
    text_facts: list[dict[str, Any]] | None = None,
    sections: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    tables = tables or []
    kv_pairs = kv_pairs or []
    text_facts = text_facts or []
    sections = sections or []
    candidates: list[dict[str, Any]] = []

    for table in tables:
        rows = [dict(row) for row in table.get('rows', []) if isinstance(row, dict)]
        for column in [str(column) for column in table.get('columns', [])]:
            sample_values = [row.get(column) for row in rows if row.get(column) not in (None, '')][:3]
            candidates.append(
                {
                    'candidate_type': 'table_column',
                    'label': column,
                    'sample_values': sample_values,
                    'source_text': str(table.get('name') or ''),
                }
            )

    for pair in kv_pairs:
        label = str(pair.get('label') or '').strip()
        value = pair.get('value')
        if not label or value in (None, ''):
            continue
        candidates.append(
            {
                'candidate_type': 'kv_pair',
                'label': label,
                'value': value,
                'sample_values': [value],
                'source_text': str(pair.get('source_text') or ''),
            }
        )

    for fact in text_facts:
        label = str(fact.get('label') or '').strip()
        value = fact.get('value')
        if not label or value in (None, ''):
            continue
        candidates.append(
            {
                'candidate_type': 'text_fact',
                'label': label,
                'value': value,
                'sample_values': [value],
                'source_text': str(fact.get('source_text') or ''),
            }
        )

    for section in sections[:8]:
        title = str(section.get('title') or '').strip()
        text = str(section.get('text') or '').strip()
        if not title or not text:
            continue
        candidates.append(
            {
                'candidate_type': 'text_section',
                'label': title,
                'value': text[:220],
                'sample_values': [],
                'source_text': text[:220],
                'section_title': title,
            }
        )

    return _dedupe_candidates(candidates)


def build_candidate_source(candidates: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    row: dict[str, Any] = {}
    columns: list[str] = []
    used_columns: set[str] = set()

    for candidate in candidates:
        candidate_type = str(candidate.get('candidate_type') or '')
        if candidate_type not in {'kv_pair', 'text_fact'}:
            continue

        label = str(candidate.get('label') or '').strip()
        value = candidate.get('value')
        if not label or value in (None, ''):
            continue

        unique_label = _make_unique_label(label, used_columns)
        row[unique_label] = value
        columns.append(unique_label)

    if not columns:
        return [], []
    return columns, [row]


def _dedupe_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for candidate in candidates:
        key = (
            str(candidate.get('candidate_type') or ''),
            str(candidate.get('label') or '').casefold(),
            str(candidate.get('value') or '').casefold(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def _make_unique_label(label: str, used_columns: set[str]) -> str:
    if label not in used_columns:
        used_columns.add(label)
        return label

    suffix = 2
    while f'{label} {suffix}' in used_columns:
        suffix += 1
    unique_label = f'{label} {suffix}'
    used_columns.add(unique_label)
    return unique_label
