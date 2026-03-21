from __future__ import annotations

from collections import defaultdict
import logging
from typing import Any

from draft_json_generator import build_draft_field_suggestions
from matcher import normalize
from model_client import suggest_draft_json_fields
from storage import get_global_field_naming_candidates, get_personal_field_naming_candidates

PERSONAL_DRAFT_NAME_MIN_SCORE = 0.6
GLOBAL_DRAFT_NAME_MIN_SCORE = 0.7

logger = logging.getLogger(__name__)


def generate_draft_json_for_source(
    *,
    source_columns: list[str],
    source_rows: list[dict[str, Any]],
    user_id: str | None = None,
    schema_fingerprint_id: int | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str]]:
    logger.info(
        'draft-json pipeline start: source_columns=%d sample_rows=%d user_id=%s schema_fingerprint_id=%s',
        len(source_columns),
        len(source_rows),
        user_id or 'guest',
        schema_fingerprint_id,
    )
    heuristic_fields = build_draft_field_suggestions(source_columns, source_rows)
    heuristic_by_source = {str(item['source_column']): item for item in heuristic_fields}

    personal_candidates = _group_by_source(
        get_personal_field_naming_candidates(
            user_id=user_id,
            source_columns=source_columns,
            schema_fingerprint_id=schema_fingerprint_id,
        )
    )
    global_candidates = _group_by_source(
        get_global_field_naming_candidates(
            source_columns=source_columns,
            schema_fingerprint_id=schema_fingerprint_id,
        )
    )
    model_fields, model_warnings = suggest_draft_json_fields(
        source_columns=source_columns,
        sample_rows=source_rows,
        personal_hints=_flatten_hint_context(personal_candidates),
        global_hints=_flatten_hint_context(global_candidates),
    )
    model_by_source = {str(item['source']): item for item in model_fields}
    logger.info(
        'draft-json pipeline context: heuristic=%d personal_sources=%d global_sources=%d model_fields=%d',
        len(heuristic_fields),
        len(personal_candidates),
        len(global_candidates),
        len(model_fields),
    )

    used_keys: set[str] = set()
    draft_json: dict[str, Any] = {}
    field_suggestions: list[dict[str, Any]] = []
    warnings = list(model_warnings)
    source_stats = {
        'heuristic_fallback': 0,
        'personal_memory': 0,
        'model_suggestion': 0,
        'global_pattern': 0,
    }

    for source_column in source_columns:
        heuristic = heuristic_by_source.get(source_column)
        if heuristic is None:
            continue

        source_key = normalize(source_column)
        source_of_truth = 'heuristic_fallback'
        status = 'suggested'
        reason = 'heuristic_transliteration'
        confidence = 'low'
        target_key = str(heuristic['suggested_key'])
        default_value = heuristic['default_value']

        personal_match = _pick_candidate(personal_candidates.get(source_key, []), min_score=PERSONAL_DRAFT_NAME_MIN_SCORE)
        if personal_match is not None and personal_match.get('target_field'):
            target_key = str(personal_match['target_field'])
            source_of_truth = 'personal_memory'
            status = 'accepted'
            confidence = _score_to_confidence(float(personal_match['score']))
            reason = str(personal_match.get('reason') or 'personal_memory')
        else:
            model_match = model_by_source.get(source_column)
            if model_match is not None and model_match.get('target'):
                target_key = str(model_match['target'])
                default_value = model_match.get('default_value', default_value)
                source_of_truth = 'model_suggestion'
                status = 'suggested'
                confidence = str(model_match.get('confidence') or 'medium')
                reason = str(model_match.get('reason') or 'draft_json_model')
            else:
                global_match = _pick_candidate(global_candidates.get(source_key, []), min_score=GLOBAL_DRAFT_NAME_MIN_SCORE)
                if global_match is not None and global_match.get('target_field'):
                    target_key = str(global_match['target_field'])
                    source_of_truth = 'global_pattern'
                    status = 'suggested'
                    confidence = _score_to_confidence(float(global_match['score']))
                    reason = str(global_match.get('reason') or 'global_pattern_candidate')

        unique_key = _make_unique_key(target_key, used_keys)
        draft_json[unique_key] = default_value
        field_suggestions.append(
            {
                'source_column': source_column,
                'target_field': unique_key,
                'default_value': default_value,
                'field_type': heuristic['field_type'],
                'status': status,
                'source_of_truth': source_of_truth,
                'confidence': confidence,
                'reason': reason,
            }
        )
        source_stats[source_of_truth] = source_stats.get(source_of_truth, 0) + 1

        if source_of_truth == 'model_suggestion':
            warnings.append(f'Проверьте draft JSON поле "{unique_key}" для колонки "{source_column}".')

    logger.info(
        'draft-json pipeline done: fields=%d warnings=%d stats=%s',
        len(field_suggestions),
        len(_dedupe(warnings)),
        source_stats,
    )
    return draft_json, field_suggestions, _dedupe(warnings)


def _group_by_source(candidates: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        source_field = candidate.get('source_field_normalized')
        if not source_field:
            continue
        grouped[str(source_field)].append(candidate)
    return grouped


def _pick_candidate(candidates: list[dict[str, Any]], *, min_score: float) -> dict[str, Any] | None:
    for candidate in sorted(candidates, key=lambda item: float(item.get('score', 0.0)), reverse=True):
        if float(candidate.get('score', 0.0)) >= min_score:
            return candidate
    return None


def _flatten_hint_context(grouped: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    flattened: list[dict[str, Any]] = []
    for items in grouped.values():
        flattened.extend(items[:2])
    return flattened[:12]


def _make_unique_key(target_key: str, used_keys: set[str]) -> str:
    candidate = (target_key or 'field').strip()
    if candidate not in used_keys:
        used_keys.add(candidate)
        return candidate

    suffix = 2
    while f'{candidate}{suffix}' in used_keys:
        suffix += 1
    unique_candidate = f'{candidate}{suffix}'
    used_keys.add(unique_candidate)
    return unique_candidate


def _score_to_confidence(score: float) -> str:
    if score >= 0.9:
        return 'high'
    if score >= 0.65:
        return 'medium'
    return 'low'


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result
