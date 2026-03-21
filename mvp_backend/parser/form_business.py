from __future__ import annotations

import re
from typing import Any

from model_client import suggest_form_field_repair


TOKEN_RE = re.compile(r'[a-zA-Zа-яА-Я0-9]+')

ORGANIZATION_LABEL_ALIASES = (
    'наименование организации',
    'полное наименование организации',
    'organization name',
    'company name',
    'наименование',
)
INN_LABEL_ALIASES = (
    'инн',
    'кио',
    'инн кио',
    'инн/кио',
    'inn',
    'kio',
    'inn or kio',
)
TAX_GROUP_KEYWORDS = (
    'налогов резидент',
    'налогов резидент рф',
    'tax residenc',
    'resident rf',
)
FATCA_GROUP_KEYWORDS = ('fatca', 'foreign account tax compliance act')

TAX_RESIDENCY_ENUM_MAP = {
    'да': 'YES',
    'yes': 'YES',
    'не являюсь налоговым резидентом ни в одном государстве': 'NOWHERE',
    'не являюсь налоговым резидентом': 'NOWHERE',
    'не являюсь резидентом ни в одном государстве': 'NOWHERE',
    'нет является резидентом иностранных государств': 'NO',
    'нет является налоговым резидентом в иностранном государстве': 'NO',
    'нет': 'NO',
    'no': 'NO',
}

FATCA_OPTION_MAP = {
    'лицом неотделимым от собственника': 'IS_DISREGARDED_ENTITY',
    'иностранным финансовым институтом': 'IS_FATCA_FOREIGN_INSTITUTE',
    'более 10 акций': 'TEN_OR_MORE_PERCENT_IN_USA',
    'более 10 процентов акций': 'TEN_OR_MORE_PERCENT_IN_USA',
    'не применимы': 'STATEMENTS_NOT_APPILCABLE',
}


def resolve_business_form_fields(
    *,
    form_model: dict[str, Any],
    target_fields: list[Any],
    repair_fn=None,
) -> list[dict[str, Any]]:
    target_names = [str(getattr(field, 'name', field) or '').strip() for field in target_fields]
    scalars = [dict(item) for item in form_model.get('scalars', []) if isinstance(item, dict)]
    groups = [dict(item) for item in form_model.get('groups', []) if isinstance(item, dict)]
    layout_lines = [dict(item) for item in form_model.get('layout_lines', []) if isinstance(item, dict)]

    tax_resolution_cache: dict[str, Any] | None = None
    results: list[dict[str, Any]] = []
    for target_name in target_names:
        lower_name = target_name.casefold()
        if lower_name == 'organizationname':
            results.append(_resolve_scalar_with_aliases(target_name, scalars, ORGANIZATION_LABEL_ALIASES))
            continue
        if lower_name == 'innorkio':
            results.append(_resolve_inn_or_kio(target_name, scalars))
            continue
        if lower_name == 'isresidentrf':
            tax_resolution_cache = tax_resolution_cache or _resolve_tax_residency_group(groups, layout_lines=layout_lines, repair_fn=repair_fn)
            results.append(
                {
                    'field': target_name,
                    'status': tax_resolution_cache['status'],
                    'resolved_by': tax_resolution_cache.get('resolved_by', 'form_resolver'),
                    'value': tax_resolution_cache.get('enum_value'),
                    'candidates': list(tax_resolution_cache.get('candidates', [])),
                    'source_ref': dict(tax_resolution_cache.get('source_ref', {})),
                    'confidence': tax_resolution_cache.get('confidence'),
                }
            )
            continue
        if lower_name == 'istaxresidencyonlyrf':
            tax_resolution_cache = tax_resolution_cache or _resolve_tax_residency_group(groups, layout_lines=layout_lines, repair_fn=repair_fn)
            derived_value = None
            if tax_resolution_cache.get('enum_value') == 'YES':
                derived_value = 'YES'
            elif tax_resolution_cache.get('enum_value') in {'NO', 'NOWHERE'}:
                derived_value = 'NO'
            results.append(
                {
                    'field': target_name,
                    'status': tax_resolution_cache['status'],
                    'resolved_by': tax_resolution_cache.get('resolved_by', 'form_resolver'),
                    'value': derived_value,
                    'candidates': list(tax_resolution_cache.get('candidates', [])),
                    'source_ref': dict(tax_resolution_cache.get('source_ref', {})),
                    'confidence': tax_resolution_cache.get('confidence'),
                }
            )
            continue
        if lower_name == 'fatcabeneficiaryoptionlist':
            results.append(_resolve_fatca_group(target_name, groups, layout_lines=layout_lines, repair_fn=repair_fn))
            continue

        results.append(_resolve_generic_scalar(target_name, scalars))

    return results


def resolve_form_fields(
    *,
    form_model: dict[str, Any],
    target_fields: list[Any],
    repair_fn=None,
) -> list[dict[str, Any]]:
    return resolve_business_form_fields(form_model=form_model, target_fields=target_fields, repair_fn=repair_fn)


def _resolve_scalar_with_aliases(field_name: str, scalars: list[dict[str, Any]], aliases: tuple[str, ...]) -> dict[str, Any]:
    best_match: dict[str, Any] | None = None
    best_score = 0.0
    near_matches: list[dict[str, Any]] = []
    alias_tokens = [_tokenize(alias) for alias in aliases]

    for scalar in scalars:
        label_tokens = _tokenize(scalar.get('label'))
        score = max((_token_overlap_score(label_tokens, tokens) for tokens in alias_tokens), default=0.0)
        if score >= 0.8 and score > best_score:
            best_match = scalar
            best_score = score
        elif score >= 0.6:
            near_matches.append(scalar)

    if best_match is not None:
        return {
            'field': field_name,
            'status': 'resolved' if best_score >= 0.85 else 'weak_match',
            'resolved_by': 'form_resolver',
            'value': best_match.get('value'),
            'candidates': [],
            'source_ref': dict(best_match.get('source_ref', {})),
            'confidence': round(best_score, 4),
        }
    if near_matches:
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [item.get('value') for item in near_matches if item.get('value') not in (None, '')],
            'source_ref': {},
            'confidence': round(
                max((_token_overlap_score(_tokenize(item.get('label')), alias_tokens[0]) for item in near_matches), default=0.0),
                4,
            ),
        }
    return {
        'field': field_name,
        'status': 'not_found',
        'resolved_by': 'form_resolver',
        'value': None,
        'candidates': [],
        'source_ref': {},
        'confidence': None,
    }


def _resolve_inn_or_kio(field_name: str, scalars: list[dict[str, Any]]) -> dict[str, Any]:
    by_label = _resolve_scalar_with_aliases(field_name, scalars, INN_LABEL_ALIASES)
    if by_label['status'] in {'resolved', 'weak_match'} and by_label.get('value') not in (None, ''):
        return by_label

    pattern = re.compile(r'\b[0-9]{8,14}\b')
    candidates: list[str] = []
    source_ref: dict[str, Any] = {}
    for scalar in scalars:
        value = str(scalar.get('value') or '').strip()
        match = pattern.search(value)
        if not match:
            continue
        candidates.append(match.group(0))
        source_ref = dict(scalar.get('source_ref', {}))

    if len(candidates) == 1:
        return {
            'field': field_name,
            'status': 'weak_match',
            'resolved_by': 'form_resolver',
            'value': candidates[0],
            'candidates': [],
            'source_ref': source_ref,
            'confidence': 0.65,
        }
    if len(candidates) > 1:
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': candidates,
            'source_ref': source_ref,
            'confidence': 0.45,
        }
    return by_label


def _resolve_tax_residency_group(
    groups: list[dict[str, Any]],
    *,
    layout_lines: list[dict[str, Any]],
    repair_fn=None,
) -> dict[str, Any]:
    group = _find_best_group(groups, TAX_GROUP_KEYWORDS)
    if group is None:
        return {
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    selected_options = [option for option in group.get('options', []) if option.get('selected')]
    if not selected_options:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='no_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
                repair_fn=repair_fn,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [_map_option_enum(option.get('label'), TAX_RESIDENCY_ENUM_MAP) for option in group.get('options', [])],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.0,
        }
    if len(selected_options) > 1:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='multiple_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
                repair_fn=repair_fn,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [option.get('label') for option in selected_options],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.35,
        }

    enum_value = _map_option_enum(selected_options[0].get('label'), TAX_RESIDENCY_ENUM_MAP)
    if enum_value is None:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='unmapped_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
                repair_fn=repair_fn,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [selected_options[0].get('label')],
            'source_ref': dict(selected_options[0].get('source_ref', {})),
            'confidence': 0.35,
        }
    return {
        'status': 'resolved',
        'resolved_by': 'form_resolver',
        'enum_value': enum_value,
        'candidates': [],
        'source_ref': dict(selected_options[0].get('source_ref', {})),
        'confidence': 0.92,
    }


def _resolve_fatca_group(
    field_name: str,
    groups: list[dict[str, Any]],
    *,
    layout_lines: list[dict[str, Any]],
    repair_fn=None,
) -> dict[str, Any]:
    group = _find_best_group(groups, FATCA_GROUP_KEYWORDS)
    if group is None:
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    selected_options = [option for option in group.get('options', []) if option.get('selected')]
    if not selected_options:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='no_selected', allow_multiple=True):
            repaired = _repair_group_resolution(
                target_field=field_name,
                group=group,
                enum_map=FATCA_OPTION_MAP,
                layout_lines=layout_lines,
                allow_multiple=True,
                repair_fn=repair_fn,
            )
            if repaired is not None:
                return {
                    'field': field_name,
                    'status': repaired['status'],
                    'resolved_by': repaired['resolved_by'],
                    'value': repaired['enum_values'],
                    'candidates': [],
                    'source_ref': repaired['source_ref'],
                    'confidence': repaired['confidence'],
                }
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': [],
            'candidates': [],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.0,
        }

    mapped_values: list[str] = []
    unresolved_labels: list[str] = []
    for option in selected_options:
        mapped = _map_option_enum(option.get('label'), FATCA_OPTION_MAP)
        if mapped is None:
            unresolved_labels.append(str(option.get('label') or ''))
            continue
        if mapped not in mapped_values:
            mapped_values.append(mapped)

    if unresolved_labels:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='unmapped_selected', allow_multiple=True):
            repaired = _repair_group_resolution(
                target_field=field_name,
                group=group,
                enum_map=FATCA_OPTION_MAP,
                layout_lines=layout_lines,
                allow_multiple=True,
                repair_fn=repair_fn,
            )
            if repaired is not None:
                return {
                    'field': field_name,
                    'status': repaired['status'],
                    'resolved_by': repaired['resolved_by'],
                    'value': repaired['enum_values'],
                    'candidates': [],
                    'source_ref': repaired['source_ref'],
                    'confidence': repaired['confidence'],
                }
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': mapped_values or None,
            'candidates': unresolved_labels,
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.45,
        }

    return {
        'field': field_name,
        'status': 'resolved',
        'resolved_by': 'form_resolver',
        'value': mapped_values,
        'candidates': [],
        'source_ref': dict(group.get('source_ref', {})),
        'confidence': 0.9,
    }


def _repair_group_resolution(
    *,
    target_field: str,
    group: dict[str, Any],
    enum_map: dict[str, str],
    layout_lines: list[dict[str, Any]],
    allow_multiple: bool,
    repair_fn=None,
) -> dict[str, Any] | None:
    context_lines = _collect_group_context_lines(group, layout_lines)
    repair_callable = repair_fn or suggest_form_field_repair
    repaired, warnings = repair_callable(
        target_field=target_field,
        question=str(group.get('question') or ''),
        options=[dict(option) for option in group.get('options', []) if isinstance(option, dict)],
        enum_map=enum_map,
        context_lines=context_lines,
        allow_multiple=allow_multiple,
    )
    if warnings:
        group.setdefault('repair_warnings', []).extend(warnings)
    if not isinstance(repaired, dict):
        return None

    if allow_multiple:
        enum_values = [str(value) for value in repaired.get('enum_values', []) if str(value).strip()]
        if not enum_values:
            return None
        return {
            'status': 'resolved',
            'resolved_by': 'repair_model',
            'enum_values': enum_values,
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': float(repaired.get('confidence') or 0.6),
        }

    enum_value = repaired.get('enum_value')
    if enum_value in (None, ''):
        return None
    return {
        'status': 'resolved',
        'resolved_by': 'repair_model',
        'enum_value': str(enum_value),
        'candidates': [],
        'source_ref': dict(group.get('source_ref', {})),
        'confidence': float(repaired.get('confidence') or 0.6),
    }


def _resolve_generic_scalar(field_name: str, scalars: list[dict[str, Any]]) -> dict[str, Any]:
    target_tokens = _tokenize(field_name)
    best_match: dict[str, Any] | None = None
    best_score = 0.0

    for scalar in scalars:
        label_tokens = _tokenize(scalar.get('label'))
        score = _token_overlap_score(target_tokens, label_tokens)
        if score > best_score:
            best_score = score
            best_match = scalar

    if best_match is None or best_score < 0.55:
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    return {
        'field': field_name,
        'status': 'weak_match' if best_score < 0.82 else 'resolved',
        'resolved_by': 'form_resolver',
        'value': best_match.get('value'),
        'candidates': [],
        'source_ref': dict(best_match.get('source_ref', {})),
        'confidence': round(best_score, 4),
    }


def _find_best_group(groups: list[dict[str, Any]], keywords: tuple[str, ...]) -> dict[str, Any] | None:
    best_group: dict[str, Any] | None = None
    best_score = 0.0
    for group in groups:
        group_id = str(group.get('group_id') or '').strip().lower()
        if group_id == 'tax_residency' and keywords == TAX_GROUP_KEYWORDS:
            return group
        if 'fatca' in group_id and keywords == FATCA_GROUP_KEYWORDS:
            return group
        if keywords == FATCA_GROUP_KEYWORDS:
            fatca_option_hits = sum(
                1
                for option in group.get('options', [])
                if isinstance(option, dict) and _map_option_enum(option.get('label'), FATCA_OPTION_MAP) is not None
            )
            if fatca_option_hits >= 2:
                return group
        question_tokens = _tokenize(group.get('question'))
        option_tokens: list[str] = []
        for option in group.get('options', []):
            if not isinstance(option, dict):
                continue
            option_tokens.extend(_tokenize(option.get('label')))
        combined_tokens = question_tokens + option_tokens
        score = max(
            (
                max(
                    _phrase_similarity(question_tokens, _tokenize(keyword)),
                    _phrase_similarity(combined_tokens, _tokenize(keyword)),
                )
                for keyword in keywords
            ),
            default=0.0,
        )
        if score > best_score:
            best_score = score
            best_group = group
    if best_group is None or best_score < 0.4:
        return None
    return best_group


def _collect_group_context_lines(group: dict[str, Any], layout_lines: list[dict[str, Any]]) -> list[str]:
    question = _clean_text(group.get('question'))
    option_labels = [_clean_text(option.get('label')) for option in group.get('options', []) if option.get('label')]
    source_ref = dict(group.get('source_ref', {}))
    source_page = source_ref.get('page')
    source_column = source_ref.get('column_id')
    source_y = _safe_float(source_ref.get('y'))
    context: list[str] = []
    for line in layout_lines:
        text = _clean_text(line.get('text'))
        if not text:
            continue
        if source_page is not None and line.get('page') != source_page:
            continue
        if source_column is not None and line.get('column_id') not in {None, source_column}:
            continue
        line_y = _safe_float(line.get('y'))
        if source_y is not None and line_y is not None and abs(line_y - source_y) > 120:
            continue
        if question and question in text:
            context.append(text)
            continue
        if any(label and label in text for label in option_labels):
            context.append(text)
            continue
    if not context:
        if question:
            context.append(question)
        context.extend(option_labels[:6])
    return context[:12]


def _should_attempt_group_repair(
    *,
    group: dict[str, Any],
    layout_lines: list[dict[str, Any]],
    issue: str,
    allow_multiple: bool,
) -> bool:
    del allow_multiple

    options = [option for option in group.get('options', []) if isinstance(option, dict)]
    if len(options) < 2 or len(options) > 8:
        return False

    context_lines = _collect_group_context_lines(group, layout_lines)
    has_wrapped_lines = len(context_lines) > max(len(options) + 2, 5)
    has_long_option = any(len(_tokenize(option.get('label'))) >= 6 for option in options)
    has_unknown_group_type = str(group.get('group_type') or 'unknown') == 'unknown'
    has_marker_variance = any(
        str(option.get('marker_text') or '').strip() not in {'', 'X', 'x', 'V', 'v', '✓', '✔', '☒', '☑'}
        for option in options
    )
    complex_group = has_wrapped_lines or has_long_option or has_unknown_group_type or has_marker_variance

    if issue in {'multiple_selected', 'unmapped_selected'}:
        return complex_group
    if issue == 'no_selected':
        return complex_group and any(_contains_marker(line) for line in context_lines)
    return False


def _map_option_enum(label: Any, enum_map: dict[str, str]) -> str | None:
    label_tokens = _tokenize(label)
    best_value = None
    best_score = 0.0
    for human_label, enum_value in enum_map.items():
        score = _phrase_similarity(label_tokens, _tokenize(human_label))
        if score > best_score:
            best_score = score
            best_value = enum_value
    if best_score < 0.45:
        return None
    return best_value


def _clean_text(value: Any) -> str:
    return ' '.join(str(value or '').replace('\xa0', ' ').split()).strip()


def _tokenize(value: Any) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(str(value or '')) if token]


def _token_overlap_score(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    return len(left_set & right_set) / max(len(left_set), len(right_set), 1)


def _phrase_similarity(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_overlap = _token_overlap_score(left, right)
    left_stems = {token[:5] for token in left if token}
    right_stems = {token[:5] for token in right if token}
    if not left_stems or not right_stems:
        return left_overlap
    stem_overlap = len(left_stems & right_stems) / max(len(left_stems), len(right_stems), 1)
    return max(left_overlap, stem_overlap)


def _contains_marker(value: Any) -> bool:
    text = str(value or '')
    return any(marker in text for marker in ('[x]', '[X]', '☒', '☑', '✓', '✔')) or bool(
        re.search(r'(^|\s)[XxVv](\s|$)', text)
    )


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == '':
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
