from __future__ import annotations

from collections import defaultdict
from difflib import SequenceMatcher
import logging
from typing import Any

from matcher import (
    build_position_fallback_mappings,
    infer_value_type,
    map_fields,
    normalize,
    prepare_field_name,
)
from model_client import rank_mapping_candidate
from models import FieldMapping, TargetField
from storage import get_global_mapping_pattern_candidates, get_personal_mapping_memory_candidates

PERSONAL_MEMORY_MIN_SCORE = 0.5
GLOBAL_PATTERN_MIN_SCORE = 0.72
MODEL_RANK_ACCEPTANCE_THRESHOLD = 0.68
CANDIDATE_SCORE_MIN_THRESHOLD = 0.16
CANDIDATE_TOP_K = 5
MODEL_CONFIDENCE_WEIGHT = 0.72
CANDIDATE_PRIOR_WEIGHT = 0.28
SEMANTIC_CONFLICT_SCORE_WEIGHT = 0.3

SEMANTIC_GROUP_KEYWORDS = {
    'created': {'created'},
    'updated': {'updated'},
    'id': {'id'},
    'date': {'date'},
    'amount': {'amount'},
    'revenue': {'revenue'},
    'name': {'name'},
    'description': {'description'},
    'quantity': {'quantity'},
    'customer': {'customer'},
    'organization': {'organization'},
    'product': {'product'},
    'creator': {'creator'},
    'responsible': {'responsible'},
    'source': {'source'},
    'partner': {'partner'},
    'license': {'license'},
    'gross': {'gross'},
    'net': {'net'},
}

SEMANTIC_CONFLICT_PAIRS = {
    frozenset({'created', 'updated'}): 1.0,
    frozenset({'id', 'date'}): 0.95,
    frozenset({'amount', 'revenue'}): 0.72,
    frozenset({'name', 'description'}): 0.65,
    frozenset({'customer', 'organization'}): 0.7,
    frozenset({'organization', 'partner'}): 0.62,
    frozenset({'customer', 'product'}): 0.8,
    frozenset({'organization', 'product'}): 0.75,
    frozenset({'creator', 'responsible'}): 0.55,
    frozenset({'creator', 'source'}): 0.78,
    frozenset({'deal', 'creator'}): 0.58,
    frozenset({'license', 'product'}): 0.66,
    frozenset({'gross', 'net'}): 0.92,
    frozenset({'quantity', 'amount'}): 0.4,
}

logger = logging.getLogger(__name__)


def resolve_generation_mappings(
    *,
    source_columns: list[str],
    target_fields: list[TargetField],
    source_rows: list[dict[str, Any]] | None = None,
    user_id: str | None = None,
    schema_fingerprint_id: int | None = None,
) -> tuple[list[FieldMapping], list[str]]:
    result = resolve_generation_mappings_detailed(
        source_columns=source_columns,
        target_fields=target_fields,
        source_rows=source_rows,
        user_id=user_id,
        schema_fingerprint_id=schema_fingerprint_id,
    )
    return result['mappings'], result['warnings']


def resolve_generation_mappings_detailed(
    *,
    source_columns: list[str],
    target_fields: list[TargetField],
    source_rows: list[dict[str, Any]] | None = None,
    user_id: str | None = None,
    schema_fingerprint_id: int | None = None,
) -> dict[str, Any]:
    logger.info(
        'mapping pipeline start: source_columns=%d target_fields=%d user_id=%s schema_fingerprint_id=%s sample_rows=%d',
        len(source_columns),
        len(target_fields),
        user_id or 'guest',
        schema_fingerprint_id,
        len(source_rows or []),
    )

    deterministic_mappings, _deterministic_warnings = map_fields(
        source_columns,
        target_fields,
        allow_position_fallback=False,
    )
    mapping_by_target = {mapping.target: mapping for mapping in deterministic_mappings}
    target_names = [target.name for target in target_fields]
    prepared_sources = {column: prepare_field_name(column) for column in source_columns}
    sample_value_by_source = _build_sample_value_map(source_rows or [])

    personal_candidates = _group_candidates_by_target(
        get_personal_mapping_memory_candidates(
            user_id=user_id,
            source_columns=source_columns,
            target_fields=target_names,
            schema_fingerprint_id=schema_fingerprint_id,
        )
    )
    global_candidates = _group_candidates_by_target(
        get_global_mapping_pattern_candidates(
            source_columns=source_columns,
            target_fields=target_names,
            schema_fingerprint_id=schema_fingerprint_id,
        )
    )

    logger.info(
        'mapping pipeline context: deterministic=%d personal_targets=%d global_targets=%d',
        len(deterministic_mappings),
        len(personal_candidates),
        len(global_candidates),
    )

    used_sources = {
        str(mapping.source)
        for mapping in deterministic_mappings
        if mapping.source and mapping.confidence in {'high', 'medium'}
    }
    warnings: list[str] = []
    resolved_by_target: dict[str, FieldMapping] = {}
    explain_rows: list[dict[str, Any]] = []
    stats = {
        'deterministic_rule': 0,
        'personal_memory': 0,
        'global_pattern': 0,
        'model_suggestion': 0,
        'position_fallback': 0,
        'unresolved': 0,
        'candidate_ranked': 0,
        'candidate_skipped': 0,
    }
    pending_rank_requests: list[dict[str, Any]] = []

    for target in target_fields:
        base_mapping = mapping_by_target.get(target.name) or FieldMapping(
            source=None,
            target=target.name,
            confidence='none',
            reason='not_found',
            status='suggested',
            source_of_truth='unresolved',
            schema_fingerprint_id=schema_fingerprint_id,
        )
        prepared_target = prepare_field_name(target.name, field_type=target.type)
        target_key = normalize(target.name)

        if _is_strong_deterministic_mapping(base_mapping):
            resolved_mapping = _clone_mapping(
                base_mapping,
                status='accepted',
                source_of_truth='deterministic_rule',
                schema_fingerprint_id=schema_fingerprint_id,
            )
            resolved_by_target[target.name] = resolved_mapping
            used_sources.add(str(resolved_mapping.source))
            stats['deterministic_rule'] += 1
            explain_rows.append(_build_explain_row(resolved_mapping))
            continue

        personal_match = _pick_candidate(
            personal_candidates.get(target_key, []),
            used_sources=used_sources,
            min_score=PERSONAL_MEMORY_MIN_SCORE,
        )
        if personal_match is not None:
            resolved_mapping = FieldMapping(
                source=str(personal_match['source_field']),
                target=target.name,
                confidence=_score_to_confidence(personal_match['score']),
                reason=str(personal_match.get('reason') or 'personal_memory'),
                status='accepted',
                source_of_truth='personal_memory',
                schema_fingerprint_id=schema_fingerprint_id,
                candidate_metadata={
                    'memory_score': personal_match['score'],
                    'schema_match': bool(personal_match.get('schema_match')),
                },
            )
            resolved_by_target[target.name] = resolved_mapping
            used_sources.add(str(resolved_mapping.source))
            stats['personal_memory'] += 1
            explain_rows.append(_build_explain_row(resolved_mapping))
            continue

        global_match = _pick_candidate(
            global_candidates.get(target_key, []),
            used_sources=used_sources,
            min_score=GLOBAL_PATTERN_MIN_SCORE,
        )
        if global_match is not None and global_match.get('source_field'):
            resolved_mapping = FieldMapping(
                source=str(global_match['source_field']),
                target=target.name,
                confidence=_score_to_confidence(global_match['score']),
                reason=str(global_match.get('reason') or 'global_pattern_candidate'),
                status='suggested',
                source_of_truth='global_pattern',
                schema_fingerprint_id=schema_fingerprint_id,
                candidate_metadata={
                    'pattern_score': global_match['score'],
                    'support_count': global_match.get('support_count'),
                    'distinct_users_count': global_match.get('distinct_users_count'),
                    'stability_score': global_match.get('stability_score'),
                },
            )
            resolved_by_target[target.name] = resolved_mapping
            used_sources.add(str(resolved_mapping.source))
            stats['global_pattern'] += 1
            warnings.append(
                f'Проверьте сопоставление для "{target.name}": использован устойчивый глобальный паттерн "{resolved_mapping.source}".'
            )
            explain_rows.append(_build_explain_row(resolved_mapping))
            continue

        candidate_shortlist = build_candidates_for_unresolved_field(
            target_field=target,
            source_columns=source_columns,
            source_rows=source_rows or [],
            prepared_sources=prepared_sources,
            used_sources=used_sources,
            personal_hints=personal_candidates.get(target_key, []),
            global_hints=global_candidates.get(target_key, []),
            sample_value_by_source=sample_value_by_source,
        )
        if not candidate_shortlist:
            unresolved_mapping = _clone_mapping(
                base_mapping,
                status='suggested',
                source_of_truth='unresolved',
                schema_fingerprint_id=schema_fingerprint_id,
                candidate_metadata={'candidate_count': 0},
            )
            resolved_by_target[target.name] = unresolved_mapping
            stats['candidate_skipped'] += 1
            stats['unresolved'] += 1
            warnings.append(f'Не найдено исходное поле для "{target.name}".')
            explain_rows.append(_build_explain_row(unresolved_mapping))
            continue

        pending_rank_requests.append(
            {
                'target': target,
                'prepared_target': prepared_target,
                'base_mapping': base_mapping,
                'candidates': candidate_shortlist,
            }
        )

    for pending_request in pending_rank_requests:
        target = pending_request['target']
        candidate_shortlist = pending_request['candidates']
        ranking_result, model_warnings = rank_mapping_candidate(
            target_field=target.name,
            target_type=target.type,
            candidates=[item['source_field'] for item in candidate_shortlist],
            sample_value=_compact_sample_value_for_target(candidate_shortlist, sample_value_by_source),
            hints=[item['reason'] for item in candidate_shortlist[:2]],
        )
        warnings.extend(model_warnings)
        stats['candidate_ranked'] += 1

        ranking_metadata = {
            'candidate_count': len(candidate_shortlist),
            'candidates': candidate_shortlist,
        }
        if ranking_result is None:
            unresolved_mapping = _clone_mapping(
                pending_request['base_mapping'],
                status='suggested',
                source_of_truth='unresolved',
                schema_fingerprint_id=schema_fingerprint_id,
                candidate_metadata=ranking_metadata,
            )
            resolved_by_target[target.name] = unresolved_mapping
            stats['unresolved'] += 1
            warnings.append(f'Модель не помогла выбрать источник для "{target.name}".')
            explain_rows.append(_build_explain_row(unresolved_mapping))
            continue

        best_candidate = ranking_result.get('best_candidate')
        raw_model_confidence = float(ranking_result.get('confidence') or 0.0)
        selected_candidate = _find_candidate_by_source(candidate_shortlist, best_candidate)
        final_confidence_score = _compute_final_model_confidence(
            raw_model_confidence=raw_model_confidence,
            selected_candidate=selected_candidate,
        )
        confidence_band = _score_to_confidence(final_confidence_score)
        if confidence_band == 'none':
            confidence_band = 'low'
        candidate_prior = float(selected_candidate.get('score', 0.0)) if selected_candidate is not None else None
        ranking_metadata.update(
            {
                'model_best_candidate': best_candidate,
                'model_confidence_raw': raw_model_confidence,
                'model_confidence_score_raw': raw_model_confidence,
                'final_confidence': final_confidence_score,
                'model_confidence_score_final': final_confidence_score,
                'candidate_prior': candidate_prior,
                'confidence_band': confidence_band,
                'model_reason': ranking_result.get('reason'),
            }
        )
        if (
            isinstance(best_candidate, str)
            and best_candidate not in used_sources
            and final_confidence_score >= MODEL_RANK_ACCEPTANCE_THRESHOLD
        ):
            resolved_mapping = FieldMapping(
                source=best_candidate,
                target=target.name,
                confidence=confidence_band,
                reason=str(ranking_result.get('reason') or 'candidate_ranker'),
                status='suggested',
                source_of_truth='model_suggestion',
                schema_fingerprint_id=schema_fingerprint_id,
                model_confidence_score=final_confidence_score,
                candidate_metadata=ranking_metadata,
            )
            resolved_by_target[target.name] = resolved_mapping
            used_sources.add(best_candidate)
            stats['model_suggestion'] += 1
            warnings.append(f'Проверьте сопоставление для "{target.name}": модель выбрала "{best_candidate}" из shortlist.')
            explain_rows.append(_build_explain_row(resolved_mapping))
            continue

        rejection_reason = _describe_gate_rejection(
            best_candidate=best_candidate,
            confidence_band=confidence_band,
            final_confidence_score=final_confidence_score,
            selected_candidate=selected_candidate,
            used_sources=used_sources,
        )
        ranking_metadata.update(
            {
                'rejected_by_gate': True,
                'rejection_reason': rejection_reason,
            }
        )
        logger.info(
            'mapping candidate rejected by gate: target=%s source=%s raw_confidence=%.3f candidate_prior=%.3f final_confidence=%.3f reason=%s',
            target.name,
            best_candidate,
            raw_model_confidence,
            candidate_prior or 0.0,
            final_confidence_score,
            rejection_reason,
        )
        unresolved_mapping = _clone_mapping(
            pending_request['base_mapping'],
            confidence=confidence_band if confidence_band != 'none' else pending_request['base_mapping'].confidence,
            status='suggested',
            source_of_truth='unresolved',
            schema_fingerprint_id=schema_fingerprint_id,
            candidate_metadata=ranking_metadata,
            model_confidence_score=final_confidence_score,
        )
        resolved_by_target[target.name] = unresolved_mapping
        stats['unresolved'] += 1
        warnings.append(f'Не удалось уверенно выбрать источник для "{target.name}".')
        explain_rows.append(_build_explain_row(unresolved_mapping))

    resolved = [resolved_by_target[target.name] for target in target_fields]
    deduped_warnings = _dedupe(warnings)
    unresolved_fields = [mapping.target for mapping in resolved if mapping.source is None]

    if all(mapping.source is None for mapping in resolved) and len(source_columns) == len(target_fields):
        logger.warning(
            'mapping pipeline fallback-by-position: source_columns=%d target_fields=%d warnings=%d',
            len(source_columns),
            len(target_fields),
            len(deduped_warnings),
        )
        fallback_mappings = [
            _clone_mapping(mapping, schema_fingerprint_id=schema_fingerprint_id)
            for mapping in build_position_fallback_mappings(source_columns, target_fields)
        ]
        explain_rows = [_build_explain_row(mapping) for mapping in fallback_mappings]
        stats['position_fallback'] = len(fallback_mappings)
        stats['unresolved'] = 0
        stats['deterministic_rule'] = 0
        stats['personal_memory'] = 0
        stats['global_pattern'] = 0
        stats['model_suggestion'] = 0
        return {
            'mappings': fallback_mappings,
            'warnings': [
                'Не найдено уверенных сопоставлений. Применён временный fallback по порядку колонок, его нужно проверить вручную.'
            ],
            'explainability': {
                'mapping_stats': stats,
                'mapping_sources': explain_rows,
                'unresolved_fields': [],
                'suggestions': [
                    row for row in explain_rows if row['status'] == 'suggested' and row['source'] is not None
                ],
            },
        }

    logger.info(
        'mapping pipeline done: resolved=%d warnings=%d stats=%s unresolved=%d',
        len(resolved),
        len(deduped_warnings),
        stats,
        len(unresolved_fields),
    )
    return {
        'mappings': resolved,
        'warnings': deduped_warnings,
        'explainability': {
            'mapping_stats': stats,
            'mapping_sources': explain_rows,
            'unresolved_fields': unresolved_fields,
            'suggestions': [
                row for row in explain_rows if row['status'] == 'suggested' and row['source'] is not None
            ],
        },
    }


def build_candidates_for_unresolved_field(
    *,
    target_field: TargetField,
    source_columns: list[str],
    source_rows: list[dict[str, Any]],
    prepared_sources: dict[str, dict[str, Any]],
    used_sources: set[str],
    personal_hints: list[dict[str, Any]],
    global_hints: list[dict[str, Any]],
    sample_value_by_source: dict[str, Any],
) -> list[dict[str, Any]]:
    prepared_target = prepare_field_name(target_field.name, field_type=target_field.type)
    hint_scores = _build_hint_score_map(personal_hints, global_hints)
    scored_candidates: list[dict[str, Any]] = []

    for source_column in source_columns:
        if source_column in used_sources:
            continue
        prepared_source = prepared_sources[source_column]
        candidate_score, score_details, semantic_conflict_label = _score_candidate(
            prepared_target=prepared_target,
            prepared_source=prepared_source,
            target_type=target_field.type,
            sample_value=sample_value_by_source.get(source_column),
            hint_score=hint_scores.get(source_column, 0.0),
        )
        if candidate_score < CANDIDATE_SCORE_MIN_THRESHOLD:
            continue

        scored_candidates.append(
            {
                'source_field': source_column,
                'score': round(candidate_score, 4),
                'reason': _build_candidate_reason(score_details),
                'details': score_details,
                'semantic_conflict_label': semantic_conflict_label,
                'sample_value': _compact_sample_value(sample_value_by_source.get(source_column)),
            }
        )

    scored_candidates.sort(key=lambda item: item['score'], reverse=True)
    return scored_candidates[:CANDIDATE_TOP_K]


def _score_candidate(
    *,
    prepared_target: dict[str, Any],
    prepared_source: dict[str, Any],
    target_type: str,
    sample_value: Any,
    hint_score: float,
) -> tuple[float, dict[str, float], str | None]:
    canonical_overlap = _jaccard_similarity(prepared_target['canonical_set'], prepared_source['canonical_set'])
    token_overlap = _jaccard_similarity(prepared_target['token_set'], prepared_source['token_set'])
    fuzzy_similarity = SequenceMatcher(None, prepared_target['canonical_name'], prepared_source['canonical_name']).ratio()
    domain_overlap = _domain_overlap_score(prepared_target, prepared_source)
    type_similarity = _type_similarity_score(
        target_type=target_type,
        prepared_target=prepared_target,
        prepared_source=prepared_source,
        sample_value=sample_value,
    )
    pattern_alignment = _pattern_alignment_score(prepared_target, prepared_source)
    semantic_conflict_penalty, semantic_conflict_label = _semantic_conflict_assessment(prepared_target, prepared_source)

    total = (
        canonical_overlap * 0.34
        + token_overlap * 0.1
        + fuzzy_similarity * 0.22
        + domain_overlap * 0.12
        + type_similarity * 0.12
        + pattern_alignment * 0.1
        + min(max(hint_score, 0.0), 1.0) * 0.12
        - semantic_conflict_penalty * SEMANTIC_CONFLICT_SCORE_WEIGHT
    )
    details = {
        'canonical_overlap': round(canonical_overlap, 4),
        'token_overlap': round(token_overlap, 4),
        'fuzzy_similarity': round(fuzzy_similarity, 4),
        'domain_overlap': round(domain_overlap, 4),
        'type_similarity': round(type_similarity, 4),
        'pattern_alignment': round(pattern_alignment, 4),
        'hint_score': round(min(max(hint_score, 0.0), 1.0), 4),
        'semantic_conflict_penalty': round(semantic_conflict_penalty, 4),
    }
    return min(max(total, 0.0), 1.0), details, semantic_conflict_label


def _domain_overlap_score(prepared_target: dict[str, Any], prepared_source: dict[str, Any]) -> float:
    target_domain = prepared_target['canonical_set'] & {
        'deal',
        'customer',
        'organization',
        'product',
        'revenue',
        'amount',
        'quantity',
        'name',
        'description',
        'creator',
        'responsible',
        'id',
        'date',
        'created',
        'updated',
    }
    source_domain = prepared_source['canonical_set'] & {
        'deal',
        'customer',
        'organization',
        'product',
        'revenue',
        'amount',
        'quantity',
        'name',
        'description',
        'creator',
        'responsible',
        'id',
        'date',
        'created',
        'updated',
    }
    return _jaccard_similarity(target_domain, source_domain)


def _type_similarity_score(
    *,
    target_type: str,
    prepared_target: dict[str, Any],
    prepared_source: dict[str, Any],
    sample_value: Any,
) -> float:
    sample_type = infer_value_type(sample_value)
    source_hints = prepared_source['type_hints']
    target_hints = prepared_target['type_hints']

    if target_type == 'number':
        if sample_type == 'number':
            return 1.0
        if {'number'} & source_hints:
            return 0.8
        return 0.0
    if target_type == 'boolean':
        if sample_type == 'boolean':
            return 1.0
        if 'boolean' in source_hints:
            return 0.8
        return 0.0
    if 'date' in target_hints:
        if sample_type == 'date':
            return 1.0
        if 'date' in source_hints:
            return 0.85
        return 0.0
    if 'id' in target_hints and 'id' in source_hints:
        return 0.9
    if target_type == 'string':
        if 'string' in source_hints:
            return 0.5
        if sample_type == 'string':
            return 0.35
    return 0.0


def _pattern_alignment_score(prepared_target: dict[str, Any], prepared_source: dict[str, Any]) -> float:
    target_tokens = prepared_target['canonical_set']
    source_tokens = prepared_source['canonical_set']
    if not target_tokens or not source_tokens:
        return 0.0

    score = 0.0
    for keyword in ('deal', 'customer', 'date', 'id', 'amount', 'revenue', 'name', 'description', 'product', 'quantity'):
        if keyword in target_tokens and keyword in source_tokens:
            score += 0.35
    if 'created' in target_tokens and 'created' in source_tokens:
        score += 0.25
    if 'updated' in target_tokens and 'updated' in source_tokens:
        score += 0.25
    return min(score, 1.0)


def _build_hint_score_map(personal_hints: list[dict[str, Any]], global_hints: list[dict[str, Any]]) -> dict[str, float]:
    score_map: dict[str, float] = {}
    for hint in personal_hints:
        source_field = hint.get('source_field')
        if not source_field:
            continue
        score_map[str(source_field)] = max(score_map.get(str(source_field), 0.0), float(hint.get('score', 0.0)))
    for hint in global_hints:
        source_field = hint.get('source_field')
        if not source_field:
            continue
        score_map[str(source_field)] = max(score_map.get(str(source_field), 0.0), float(hint.get('score', 0.0)) * 0.9)
    return score_map


def _semantic_conflict_assessment(prepared_target: dict[str, Any], prepared_source: dict[str, Any]) -> tuple[float, str | None]:
    target_groups = _extract_semantic_groups(prepared_target)
    source_groups = _extract_semantic_groups(prepared_source)
    if not target_groups or not source_groups:
        return 0.0, None

    penalty = 0.0
    label: str | None = None
    for target_group in target_groups:
        for source_group in source_groups:
            if target_group == source_group:
                continue
            pair_penalty = SEMANTIC_CONFLICT_PAIRS.get(frozenset({target_group, source_group}), 0.0)
            if pair_penalty > penalty:
                penalty = pair_penalty
                left, right = sorted((target_group, source_group))
                label = f'semantic_conflict_{left}_vs_{right}'
    return penalty, label


def _extract_semantic_groups(prepared_field: dict[str, Any]) -> set[str]:
    canonical_tokens = set(prepared_field.get('canonical_set') or set())
    matched_groups: set[str] = set()
    for group_name, keywords in SEMANTIC_GROUP_KEYWORDS.items():
        if canonical_tokens & keywords:
            matched_groups.add(group_name)
    return matched_groups


def _build_sample_value_map(source_rows: list[dict[str, Any]]) -> dict[str, Any]:
    samples: dict[str, Any] = {}
    for row in source_rows:
        if not isinstance(row, dict):
            continue
        for key, value in row.items():
            if key in samples:
                continue
            if value in (None, ''):
                continue
            samples[str(key)] = value
    return samples


def _compact_sample_value_for_target(candidate_shortlist: list[dict[str, Any]], sample_value_by_source: dict[str, Any]) -> Any:
    for candidate in candidate_shortlist:
        source_field = candidate.get('source_field')
        if source_field in sample_value_by_source:
            return sample_value_by_source[source_field]
    return None


def _find_candidate_by_source(candidate_shortlist: list[dict[str, Any]], source_field: Any) -> dict[str, Any] | None:
    if not isinstance(source_field, str):
        return None
    for candidate in candidate_shortlist:
        if candidate.get('source_field') == source_field:
            return candidate
    return None


def _compute_final_model_confidence(
    *,
    raw_model_confidence: float,
    selected_candidate: dict[str, Any] | None,
) -> float:
    bounded_model_confidence = min(max(raw_model_confidence, 0.0), 1.0)
    if selected_candidate is None:
        return bounded_model_confidence

    candidate_prior = min(max(float(selected_candidate.get('score', 0.0)), 0.0), 1.0)
    score_details = selected_candidate.get('details') or {}
    semantic_conflict_penalty = min(max(float(score_details.get('semantic_conflict_penalty', 0.0)), 0.0), 1.0)

    final_score = (
        bounded_model_confidence * MODEL_CONFIDENCE_WEIGHT
        + candidate_prior * CANDIDATE_PRIOR_WEIGHT
        - semantic_conflict_penalty * 0.25
    )
    return min(max(final_score, 0.0), 1.0)


def _describe_gate_rejection(
    *,
    best_candidate: Any,
    confidence_band: str,
    final_confidence_score: float,
    selected_candidate: dict[str, Any] | None,
    used_sources: set[str],
) -> str:
    if not isinstance(best_candidate, str) or not best_candidate.strip():
        return 'model_returned_no_candidate'
    if best_candidate in used_sources:
        return 'source_already_used'
    if selected_candidate is not None:
        semantic_conflict_label = selected_candidate.get('semantic_conflict_label')
        if isinstance(semantic_conflict_label, str) and semantic_conflict_label:
            return semantic_conflict_label
    if final_confidence_score < MODEL_RANK_ACCEPTANCE_THRESHOLD:
        if confidence_band == 'low':
            return 'low_final_confidence'
        return 'below_acceptance_threshold'
    return 'rejected_by_gate'


def _pick_candidate(
    candidates: list[dict[str, Any]],
    *,
    used_sources: set[str],
    min_score: float,
) -> dict[str, Any] | None:
    for candidate in sorted(
        candidates,
        key=lambda item: (float(item.get('score', 0.0)), float(item.get('confidence', 0.0))),
        reverse=True,
    ):
        source_field = candidate.get('source_field')
        if not source_field or str(source_field) in used_sources:
            continue
        if float(candidate.get('score', 0.0)) < min_score:
            continue
        return candidate
    return None


def _group_candidates_by_target(candidates: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        target_value = candidate.get('target_field_normalized')
        if not target_value:
            continue
        grouped[str(target_value)].append(candidate)
    return grouped


def _is_strong_deterministic_mapping(mapping: FieldMapping) -> bool:
    return mapping.source is not None and mapping.confidence in {'high', 'medium'}


def _clone_mapping(mapping: FieldMapping, **changes: object) -> FieldMapping:
    if hasattr(mapping, 'model_copy'):
        return mapping.model_copy(update=changes)
    return mapping.copy(update=changes)


def _score_to_confidence(score: float) -> str:
    if score >= 0.9:
        return 'high'
    if score >= 0.65:
        return 'medium'
    if score > 0:
        return 'low'
    return 'none'


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))


def _build_candidate_reason(score_details: dict[str, float]) -> str:
    ordered = sorted(score_details.items(), key=lambda item: item[1], reverse=True)
    active_parts = [name for name, value in ordered if value >= 0.18][:2]
    if not active_parts:
        return 'candidate_shortlist'
    return ','.join(active_parts)


def _build_explain_row(mapping: FieldMapping) -> dict[str, Any]:
    return {
        'target': mapping.target,
        'source': mapping.source,
        'source_of_truth': mapping.source_of_truth,
        'status': mapping.status,
        'confidence': mapping.confidence,
        'reason': mapping.reason,
        'schema_fingerprint_id': mapping.schema_fingerprint_id,
        'model_confidence_score': mapping.model_confidence_score,
        'candidate_metadata': mapping.candidate_metadata,
    }


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _compact_sample_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:80]
    return value
