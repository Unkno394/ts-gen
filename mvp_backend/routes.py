from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile

from auth_session import create_access_token, get_current_user, get_optional_current_user, invalidate_user_cache
from draft_json_pipeline import generate_draft_json_for_source
from email_service import (
    EmailDeliveryError,
    EmailServiceError,
    consume_email_change_code,
    consume_registration_code,
    consume_password_reset_code,
    consume_password_reset_token,
    request_email_change_code,
    request_registration_code,
    request_password_reset_code,
    verify_password_reset_code,
)
from generator import build_preview, generate_typescript
from learning_pipeline import resolve_generation_mappings_detailed
from model_client import begin_model_usage_capture, end_model_usage_capture, get_captured_model_usage
from models import (
    AuthPayload,
    ChangeEmailPayload,
    ChangePasswordPayload,
    CorrectionSessionPayload,
    DraftJsonFeedbackPayload,
    EmailChangeCodePayload,
    EmailCodePayload,
    FormFieldResolution,
    GenerationConfirmationPayload,
    MappingFeedbackPayload,
    PatternPromotionPayload,
    RepairApplyPayload,
    RepairPreviewPayload,
    ParsedFile,
    RegisterPayload,
    SourcePreviewRefreshLogPayload,
    ResetPasswordPayload,
    TargetField,
    TrainingRunActivationPayload,
    TrainingRunCompletionPayload,
    TrainingRunPayload,
    TrainingRunStartPayload,
    TrainingSnapshotExportPayload,
    TrainingSnapshotPayload,
    UserTemplatePayload,
    VerifyResetCodePayload,
    UpdateProfilePayload,
)
from parsers import (
    ParseError,
    coerce_parsed_file,
    parse_file,
    parse_target_schema,
    preview_business_form_resolutions,
    resolve_draft_json_source,
    resolve_generation_source,
)
from storage import (
    EmailChangeError,
    InvalidCredentialsError,
    ProfileUpdateError,
    UserConflictError,
    UserNotFoundError,
    apply_mapping_feedback,
    apply_generation_repair_patch,
    apply_draft_json_feedback,
    change_user_password,
    change_user_email,
    cleanup_expired_guest_files,
    confirm_generation_learning,
    create_model_training_run,
    create_training_snapshot,
    delete_generation_history_entry,
    activate_model_training_run,
    ensure_schema_fingerprint,
    export_training_snapshot,
    finalize_guest_upload,
    get_user_profile,
    get_history,
    get_learning_memory_layers,
    list_learning_events,
    get_learning_summary,
    get_model_runtime_status,
    is_email_registered,
    list_user_templates,
    login_user,
    record_uploaded_file,
    prepare_email_change,
    promote_stable_pattern_candidates,
    register_user,
    save_correction_session,
    save_draft_json_suggestions,
    save_generation,
    save_mapping_suggestions,
    save_upload,
    save_user_template,
    start_model_training_run,
    update_user_profile_name,
    update_user_password,
    verify_user_password,
    complete_model_training_run,
)
from validation import assess_mapping_operational_status, compile_typescript_code, validate_preview_against_target_schema

router = APIRouter()
logger = logging.getLogger(__name__)
LOW_GROUP_CONFIDENCE_THRESHOLD = 0.65
LOW_SELECTION_CONFIDENCE_THRESHOLD = 0.6


def _model_to_dict(value: object) -> dict:
    if hasattr(value, 'model_dump'):
        return _to_jsonish(value.model_dump())
    if hasattr(value, 'dict'):
        return _to_jsonish(value.dict())
    raise TypeError(f'Unsupported model value: {type(value)!r}')


def _to_jsonish(value: object) -> object:
    if hasattr(value, 'model_dump'):
        return _to_jsonish(value.model_dump())
    if hasattr(value, 'dict'):
        return _to_jsonish(value.dict())
    if isinstance(value, dict):
        return {str(key): _to_jsonish(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_jsonish(item) for item in value]
    return value


def _build_form_explainability(parsed_file: object) -> dict | None:
    if not hasattr(parsed_file, 'form_model'):
        return None
    form_model = getattr(parsed_file, 'form_model')
    if form_model is None:
        return None
    layout_meta = dict(getattr(form_model, 'layout_meta', {}) or {})
    quality_summary = layout_meta.get('quality_summary', {})
    if not isinstance(quality_summary, dict):
        quality_summary = {}
    resolved_fields = [
        _model_to_dict(field)
        for field in list(getattr(form_model, 'resolved_fields', []) or [])
    ]
    groups = [_to_jsonish(group) for group in list(getattr(form_model, 'groups', []) or [])]
    scalars = [_to_jsonish(item) for item in list(getattr(form_model, 'scalars', []) or [])]
    layout_lines = [_to_jsonish(item) for item in list(getattr(form_model, 'layout_lines', []) or [])]
    generic_quality = _build_generic_form_quality(
        parsed_file=parsed_file,
        groups=groups,
        scalars=scalars,
        layout_lines=layout_lines,
    )
    quality_summary = _merge_form_quality_summaries(quality_summary, generic_quality)

    repair_plan = _build_form_repair_plan(
        parsed_file=parsed_file,
        form_model=form_model,
        layout_meta=layout_meta,
        quality_summary=quality_summary,
        resolved_fields=resolved_fields,
    )
    return {
        'document_mode': getattr(parsed_file, 'document_mode', 'data_table_mode'),
        'final_source_mode': layout_meta.get('final_source_mode'),
        'layout_meta': layout_meta,
        'pdf_zone_summary': _build_pdf_zone_explainability(parsed_file),
        'structure': _to_jsonish(getattr(form_model, 'structure', {}) or {}),
        'quality_summary': quality_summary,
        'repair_plan': repair_plan,
        'resolved_fields': resolved_fields,
        'scalar_count': len(scalars),
        'group_count': len(groups),
        'section_count': len(list(getattr(form_model, 'section_hierarchy', []) or [])),
        'layout_line_count': len(layout_lines),
        'repair_fields': [
            field.get('field')
            for field in resolved_fields
            if field.get('resolved_by') == 'repair_model'
        ],
    }


def _build_form_repair_plan(
    *,
    parsed_file: object,
    form_model: object,
    layout_meta: dict,
    quality_summary: dict,
    resolved_fields: list[dict],
) -> dict:
    groups = [_to_jsonish(group) for group in list(getattr(form_model, 'groups', []) or [])]
    scalars = [_to_jsonish(item) for item in list(getattr(form_model, 'scalars', []) or [])]
    layout_lines = [_to_jsonish(item) for item in list(getattr(form_model, 'layout_lines', []) or [])]
    target_fields = list(layout_meta.get('requested_target_fields') or [])
    red_flags = list(quality_summary.get('red_flags') or [])

    if not quality_summary:
        quality_summary = _build_generic_form_quality(
            parsed_file=parsed_file,
            groups=groups,
            scalars=scalars,
            layout_lines=layout_lines,
        )
        red_flags = list(quality_summary.get('red_flags') or [])

    actions: list[dict] = []
    seen_keys: set[tuple[str, str]] = set()

    for field_name in list(quality_summary.get('unresolved_critical_fields') or []):
        action = _build_field_repair_action(
            action_kind='repair_group_resolution',
            field_name=str(field_name),
            groups=groups,
            scalars=scalars,
            layout_lines=layout_lines,
            priority='high',
            reason='Critical field was not reliably extracted.',
        )
        key = (action['kind'], action['target_field'])
        if key not in seen_keys:
            seen_keys.add(key)
            actions.append(action)

    for field_name in list(quality_summary.get('ambiguous_fields') or []):
        action = _build_field_repair_action(
            action_kind='repair_ambiguous_resolution',
            field_name=str(field_name),
            groups=groups,
            scalars=scalars,
            layout_lines=layout_lines,
            priority='high' if str(field_name).casefold() in {'isresidentrf', 'istaxresidencyonlyrf', 'fatcabeneficiaryoptionlist'} else 'medium',
            reason='Field resolution is ambiguous and should be reconstructed from a local fragment.',
        )
        key = (action['kind'], action['target_field'])
        if key not in seen_keys:
            seen_keys.add(key)
            actions.append(action)

    for group_id in list(quality_summary.get('ambiguous_groups') or []):
        action = _build_group_repair_action(
            group_id=str(group_id),
            groups=groups,
            layout_lines=layout_lines,
            priority='high',
            reason='Generic form group remains ambiguous and should be re-evaluated from local layout chunks.',
        )
        key = (action['kind'], action['group_id'])
        if key not in seen_keys:
            seen_keys.add(key)
            actions.append(action)

    for group_id in list(quality_summary.get('low_confidence_groups') or []):
        action = _build_group_repair_action(
            group_id=str(group_id),
            groups=groups,
            layout_lines=layout_lines,
            priority='medium',
            reason='Generic form group was extracted with low confidence and should be reviewed before mapping.',
        )
        key = (action['kind'], action['group_id'])
        if key not in seen_keys:
            seen_keys.add(key)
            actions.append(action)

    for group_id in list(quality_summary.get('multiple_selected_single_choice_groups') or []):
        action = _build_group_repair_action(
            group_id=str(group_id),
            groups=groups,
            layout_lines=layout_lines,
            priority='high',
            reason='Single-choice group contains multiple selected options.',
        )
        key = (action['kind'], action['group_id'])
        if key not in seen_keys:
            seen_keys.add(key)
            actions.append(action)

    for flag in red_flags:
        if not isinstance(flag, dict):
            continue
        code = str(flag.get('code') or '')
        if code == 'legacy_fallback_used':
            action = {
                'kind': 'review_legacy_fallback',
                'priority': 'medium',
                'reason': str(flag.get('message') or 'Legacy fallback was used.'),
                'fields': [str(item) for item in list(flag.get('fields') or [])],
                'llm_scope': 'disabled_until_local_chunks_selected',
                'chunk_refs': {
                    'group_ids': [],
                    'scalar_labels': [str(item.get('label') or '') for item in scalars[:8]],
                    'line_ids': [str(item.get('line_id') or item.get('block_id') or '') for item in layout_lines[:12] if str(item.get('line_id') or item.get('block_id') or '')],
                },
            }
            key = (action['kind'], ','.join(action['fields']))
            if key not in seen_keys:
                seen_keys.add(key)
                actions.append(action)
        elif code in {'low_field_coverage', 'empty_normalized_row'}:
            action = {
                'kind': 'rebuild_generic_form_understanding',
                'priority': 'high',
                'reason': str(flag.get('message') or 'Generic form understanding did not produce enough structured output.'),
                'llm_scope': 'structured_chunks_only',
                'chunk_refs': {
                    'group_ids': [str(item.get('group_id') or '') for item in groups[:6]],
                    'scalar_labels': [str(item.get('label') or '') for item in scalars[:8]],
                    'line_ids': [str(item.get('line_id') or item.get('block_id') or '') for item in layout_lines[:16] if str(item.get('line_id') or item.get('block_id') or '')],
                },
            }
            key = (action['kind'], 'generic')
            if key not in seen_keys:
                seen_keys.add(key)
                actions.append(action)
        elif code in {'low_confidence_form_zones', 'pdf_zone_prefers_table'}:
            action = {
                'kind': 'review_pdf_zone_routing',
                'priority': 'high' if code == 'pdf_zone_prefers_table' else 'medium',
                'reason': str(flag.get('message') or 'PDF zone routing should be reviewed.'),
                'llm_scope': 'disabled_until_zone_selection_reviewed',
                'chunk_refs': {
                    'group_ids': [str(item.get('group_id') or '') for item in groups[:6]],
                    'scalar_labels': [str(item.get('label') or '') for item in scalars[:8]],
                    'line_ids': [str(item.get('line_id') or item.get('block_id') or '') for item in layout_lines[:16] if str(item.get('line_id') or item.get('block_id') or '')],
                },
                'pdf_zone_routing': quality_summary.get('pdf_zone_routing') or {},
            }
            key = (action['kind'], code)
            if key not in seen_keys:
                seen_keys.add(key)
                actions.append(action)

    needs_attention = bool(quality_summary.get('needs_attention'))
    return {
        'recommended': bool(quality_summary.get('repair_recommended')) or needs_attention,
        'trigger_stage': 'generic_form_understanding' if not target_fields else 'business_mapping',
        'strategy': 'layout_chunks_then_targeted_repair',
        'llm_policy': 'targeted_local_chunks_only',
        'requested_target_fields': target_fields,
        'red_flag_codes': [str(flag.get('code') or '') for flag in red_flags if isinstance(flag, dict)],
        'actions': actions,
        'targeted_chunk_count': sum(len(list(action.get('chunk_refs', {}).get('line_ids', []))) for action in actions),
    }


def _build_generic_form_quality(
    *,
    parsed_file: object,
    groups: list[object],
    scalars: list[object],
    layout_lines: list[object],
) -> dict:
    document_mode = getattr(parsed_file, 'document_mode', 'data_table_mode')
    pdf_zone_summary = _extract_pdf_zone_summary(parsed_file)
    red_flags: list[dict] = []
    group_quality = _derive_group_quality_signals(groups)
    multiple_selected_single_choice_groups = [
        str(group.get('group_id') or '')
        for group in groups
        if isinstance(group, dict)
        and str(group.get('group_type') or 'unknown') == 'single_choice'
        and sum(1 for option in list(group.get('options') or []) if isinstance(option, dict) and option.get('selected')) > 1
    ]
    ambiguous_groups = group_quality['ambiguous_groups']
    low_confidence_groups = group_quality['low_confidence_groups']
    if document_mode == 'form_layout_mode' and not groups and not scalars and layout_lines:
        red_flags.append(
            {
                'code': 'no_structured_fields_extracted',
                'message': 'Layout was detected, but generic form understanding did not extract groups or scalars.',
            }
        )
    if multiple_selected_single_choice_groups:
        red_flags.append(
            {
                'code': 'single_choice_multi_select',
                'message': 'Single-choice groups contain multiple selected options.',
                'groups': multiple_selected_single_choice_groups,
            }
        )
    if ambiguous_groups:
        red_flags.append(
            {
                'code': 'ambiguous_form_groups',
                'message': 'Some extracted form groups remain ambiguous.',
                'groups': ambiguous_groups,
            }
        )
    if low_confidence_groups:
        red_flags.append(
            {
                'code': 'low_confidence_form_groups',
                'message': 'Some extracted form groups have low confidence and should be reviewed.',
                'groups': low_confidence_groups,
            }
        )
    if pdf_zone_summary.get('prefer_table_source'):
        red_flags.append(
            {
                'code': 'pdf_zone_prefers_table',
                'message': 'PDF routing indicates stronger table zones than form zones.',
                'best_form_confidence': pdf_zone_summary.get('best_form_confidence'),
                'best_table_confidence': pdf_zone_summary.get('best_table_confidence'),
            }
        )
    elif pdf_zone_summary.get('low_confidence_form_zones'):
        red_flags.append(
            {
                'code': 'low_confidence_form_zones',
                'message': 'PDF form zones were detected with low confidence.',
                'best_form_confidence': pdf_zone_summary.get('best_form_confidence'),
            }
        )
    return {
        'needs_attention': bool(red_flags),
        'repair_recommended': bool(red_flags),
        'unresolved_critical_fields': [],
        'ambiguous_fields': [],
        'red_flags': red_flags,
        'multiple_selected_single_choice_groups': multiple_selected_single_choice_groups,
        'ambiguous_groups': ambiguous_groups,
        'low_confidence_groups': low_confidence_groups,
        'pdf_zone_routing': pdf_zone_summary,
    }


def _extract_pdf_zone_summary(parsed_file: object) -> dict:
    return _to_jsonish(dict(getattr(parsed_file, 'pdf_zone_summary', {}) or {}))


def _build_pdf_zone_explainability(parsed_file: object) -> dict | None:
    pdf_zone_summary = _extract_pdf_zone_summary(parsed_file)
    if not pdf_zone_summary:
        return None
    parser_outputs = dict(pdf_zone_summary.get('parser_outputs') or {})
    return {
        'dominant_zone': pdf_zone_summary.get('dominant_zone'),
        'counts': dict(pdf_zone_summary.get('counts') or {}),
        'routing': {
            key: pdf_zone_summary.get(key)
            for key in (
                'available',
                'has_table_zones',
                'has_form_zones',
                'has_text_zones',
                'has_noise_zones',
                'best_table_confidence',
                'best_form_confidence',
                'best_text_confidence',
                'best_noise_confidence',
                'has_confident_table_zone',
                'has_confident_form_zone',
                'low_confidence_form_zones',
                'prefer_table_source',
            )
            if key in pdf_zone_summary
        },
        'parser_outputs': {
            'table_zone_count': len(list(dict(parser_outputs.get('table') or {}).get('zones') or [])),
            'form_zone_count': len(list(dict(parser_outputs.get('form') or {}).get('zones') or [])),
            'text_zone_count': len(list(dict(parser_outputs.get('text') or {}).get('zones') or [])),
            'noise_zone_count': len(list(dict(parser_outputs.get('noise') or {}).get('zones') or [])),
        },
    }


def _build_field_repair_action(
    *,
    action_kind: str,
    field_name: str,
    groups: list[object],
    scalars: list[object],
    layout_lines: list[object],
    priority: str,
    reason: str,
) -> dict:
    related_groups = _find_related_groups_for_field(field_name, groups)
    group_ids = [str(group.get('group_id') or '') for group in related_groups]
    line_ids = _collect_related_line_ids(related_groups=related_groups, scalars=scalars, layout_lines=layout_lines, field_name=field_name)
    scalar_labels = _collect_related_scalar_labels(field_name=field_name, scalars=scalars)
    return {
        'kind': action_kind,
        'priority': priority,
        'target_field': field_name,
        'reason': reason,
        'llm_scope': 'targeted_group_fragment' if group_ids else 'targeted_scalar_fragment',
        'chunk_refs': {
            'group_ids': group_ids,
            'scalar_labels': scalar_labels,
            'line_ids': line_ids,
        },
    }


def _build_group_repair_action(
    *,
    group_id: str,
    groups: list[object],
    layout_lines: list[object],
    priority: str,
    reason: str,
) -> dict:
    related_groups = [
        group for group in groups
        if isinstance(group, dict) and str(group.get('group_id') or '') == group_id
    ]
    line_ids = _collect_related_line_ids(related_groups=related_groups, scalars=[], layout_lines=layout_lines, field_name=group_id)
    action = {
        'kind': 'review_group_selection',
        'priority': priority,
        'group_id': group_id,
        'reason': reason,
        'llm_scope': 'targeted_group_fragment',
        'chunk_refs': {
            'group_ids': [group_id],
            'scalar_labels': [],
            'line_ids': line_ids,
        },
    }
    if related_groups:
        group = related_groups[0]
        action['group_confidence'] = _coerce_float(group.get('group_confidence'))
        action['selection_confidence'] = _coerce_float(group.get('selection_confidence'))
        action['ambiguity_reason'] = str(group.get('ambiguity_reason') or '') or None
    return action


def _derive_group_quality_signals(groups: list[object]) -> dict[str, list[str]]:
    ambiguous_groups: list[str] = []
    low_confidence_groups: list[str] = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_id = str(group.get('group_id') or '').strip()
        if not group_id:
            continue
        group_confidence = _coerce_float(group.get('group_confidence'))
        selection_confidence = _coerce_float(group.get('selection_confidence'))
        if bool(group.get('is_ambiguous')):
            ambiguous_groups.append(group_id)
            continue
        if group_confidence is not None and group_confidence < LOW_GROUP_CONFIDENCE_THRESHOLD:
            low_confidence_groups.append(group_id)
            continue
        if selection_confidence is not None and selection_confidence < LOW_SELECTION_CONFIDENCE_THRESHOLD:
            low_confidence_groups.append(group_id)
    return {
        'ambiguous_groups': _merge_unique_str_lists([], ambiguous_groups),
        'low_confidence_groups': _merge_unique_str_lists([], low_confidence_groups),
    }


def _merge_form_quality_summaries(primary: dict, generic: dict) -> dict:
    if not primary:
        return dict(generic)
    if not generic:
        return dict(primary)

    merged = dict(primary)
    for key in (
        'unresolved_critical_fields',
        'ambiguous_fields',
        'multiple_selected_single_choice_groups',
        'ambiguous_groups',
        'low_confidence_groups',
    ):
        merged[key] = _merge_unique_str_lists(primary.get(key), generic.get(key))
    merged['red_flags'] = _merge_red_flags(
        list(primary.get('red_flags') or []),
        list(generic.get('red_flags') or []),
    )
    merged['needs_attention'] = bool(merged['red_flags']) or bool(primary.get('needs_attention')) or bool(generic.get('needs_attention'))
    merged['repair_recommended'] = bool(merged['red_flags']) or bool(primary.get('repair_recommended')) or bool(generic.get('repair_recommended'))
    return merged


def _merge_unique_str_lists(left: object, right: object) -> list[str]:
    result: list[str] = []
    for values in (left, right):
        for item in list(values or []):
            normalized = str(item).strip()
            if normalized and normalized not in result:
                result.append(normalized)
    return result


def _merge_red_flags(left: list[dict], right: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for flag in list(left) + list(right):
        if not isinstance(flag, dict):
            continue
        code = str(flag.get('code') or '')
        message = str(flag.get('message') or '')
        key = (code, message)
        if key in seen:
            continue
        seen.add(key)
        merged.append(flag)
    return merged


def _coerce_float(value: object) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _find_related_groups_for_field(field_name: str, groups: list[object]) -> list[dict]:
    normalized_field = str(field_name or '').casefold()
    hints: list[str] = []
    if 'resident' in normalized_field or 'residency' in normalized_field:
        hints.extend(['tax', 'resident', 'резидент'])
    if 'fatca' in normalized_field:
        hints.extend(['fatca'])
    related = []
    for group in groups:
        if not isinstance(group, dict):
            continue
        group_id = str(group.get('group_id') or '').casefold()
        question = str(group.get('question') or '').casefold()
        if any(hint in group_id or hint in question for hint in hints):
            related.append(group)
    return related


def _collect_related_line_ids(
    *,
    related_groups: list[dict],
    scalars: list[object],
    layout_lines: list[object],
    field_name: str,
) -> list[str]:
    candidate_line_ids: list[str] = []
    related_texts = [str(group.get('question') or '') for group in related_groups]
    for group in related_groups:
        for option in list(group.get('options') or []):
            if isinstance(option, dict):
                related_texts.append(str(option.get('label') or ''))
    for scalar in scalars:
        if not isinstance(scalar, dict):
            continue
        label = str(scalar.get('label') or '')
        if _field_matches_scalar(field_name, label):
            related_texts.append(label)
    for line in layout_lines:
        if not isinstance(line, dict):
            continue
        text = str(line.get('text') or '')
        if not text:
            continue
        if any(item and item.casefold() in text.casefold() for item in related_texts):
            line_id = str(line.get('line_id') or line.get('block_id') or '').strip()
            if line_id and line_id not in candidate_line_ids:
                candidate_line_ids.append(line_id)
    return candidate_line_ids[:16]


def _collect_related_scalar_labels(*, field_name: str, scalars: list[object]) -> list[str]:
    labels: list[str] = []
    for scalar in scalars:
        if not isinstance(scalar, dict):
            continue
        label = str(scalar.get('label') or '')
        if label and _field_matches_scalar(field_name, label):
            labels.append(label)
    return labels[:8]


def _field_matches_scalar(field_name: str, label: str) -> bool:
    normalized_field = str(field_name or '').casefold()
    normalized_label = str(label or '').casefold()
    if not normalized_field or not normalized_label:
        return False
    if 'organization' in normalized_field and any(token in normalized_label for token in ['organization', 'организац', 'company']):
        return True
    if 'inn' in normalized_field or 'kio' in normalized_field:
        return any(token in normalized_label for token in ['инн', 'кио', 'inn', 'kio'])
    return normalized_field in normalized_label or normalized_label in normalized_field


def _coerce_repair_target_fields(
    *,
    target_json: dict | list | str | None,
    parsed_file: ParsedFile,
    action: dict,
) -> list[TargetField]:
    if isinstance(target_json, str):
        raw = target_json.strip()
        if raw:
            target_fields, _, _, _ = parse_target_schema(raw)
            return target_fields
    elif isinstance(target_json, (dict, list)):
        target_fields, _, _, _ = parse_target_schema(json.dumps(target_json, ensure_ascii=False))
        return target_fields

    requested = (
        list(getattr(parsed_file.form_model, 'layout_meta', {}).get('requested_target_fields', []) or [])
        if parsed_file.form_model
        else []
    )
    action_target = str(action.get('target_field') or '').strip()
    field_names: list[str] = []
    for name in requested:
        normalized = str(name).strip()
        if normalized and normalized not in field_names:
            field_names.append(normalized)
    if action_target and action_target not in field_names:
        field_names.append(action_target)
    if action.get('group_id') == 'tax_residency':
        for derived in ('isResidentRF', 'isTaxResidencyOnlyRF'):
            if derived not in field_names:
                field_names.append(derived)
    if action.get('group_id') == 'fatca_beneficiary' and 'fatcaBeneficiaryOptionList' not in field_names:
        field_names.append('fatcaBeneficiaryOptionList')
    return [TargetField(name=name, type='any') for name in field_names]


def _select_local_repair_chunks(parsed_file: ParsedFile, action: dict) -> dict:
    form_model = parsed_file.form_model
    if form_model is None:
        return {'groups': [], 'scalars': [], 'lines': []}

    chunk_refs = dict(action.get('chunk_refs') or {})
    group_ids = {str(item).strip() for item in list(chunk_refs.get('group_ids') or []) if str(item).strip()}
    scalar_labels = {str(item).strip() for item in list(chunk_refs.get('scalar_labels') or []) if str(item).strip()}
    line_ids = {str(item).strip() for item in list(chunk_refs.get('line_ids') or []) if str(item).strip()}

    groups = [
        _model_to_dict(group)
        for group in list(form_model.groups or [])
        if not group_ids or str(group.group_id).strip() in group_ids
    ]
    scalars = [
        _model_to_dict(item)
        for item in list(form_model.scalars or [])
        if not scalar_labels or str(item.label).strip() in scalar_labels
    ]
    lines = [
        _model_to_dict(line)
        for line in list(form_model.layout_lines or [])
        if not line_ids or str(line.line_id or line.block_id or '').strip() in line_ids
    ]
    return {'groups': groups, 'scalars': scalars, 'lines': lines}


def _filter_preview_resolutions(
    *,
    resolutions: list[dict],
    action: dict,
) -> list[dict]:
    target_field = str(action.get('target_field') or '').strip()
    group_id = str(action.get('group_id') or '').strip()
    if target_field:
        filtered = [item for item in resolutions if str(item.get('field') or '') == target_field]
        if target_field == 'isResidentRF':
            filtered.extend(
                item
                for item in resolutions
                if str(item.get('field') or '') == 'isTaxResidencyOnlyRF' and item not in filtered
            )
        return filtered
    if group_id == 'tax_residency':
        return [
            item for item in resolutions if str(item.get('field') or '') in {'isResidentRF', 'isTaxResidencyOnlyRF'}
        ]
    if group_id == 'fatca_beneficiary':
        return [item for item in resolutions if str(item.get('field') or '') == 'fatcaBeneficiaryOptionList']
    return resolutions


def _build_proposed_patch(resolutions: list[dict]) -> dict:
    patch: dict[str, object] = {}
    for item in resolutions:
        status = str(item.get('status') or 'not_found')
        field = str(item.get('field') or '').strip()
        if not field or status not in {'resolved', 'weak_match'}:
            continue
        if item.get('value') is None:
            continue
        patch[field] = item.get('value')
    return patch


def _repair_preview_status(action: dict, proposed_patch: dict, filtered_resolutions: list[dict]) -> str:
    if proposed_patch:
        return 'patch_available'
    if str(action.get('kind') or '') in {'rebuild_generic_form_understanding', 'review_legacy_fallback'}:
        return 'inspection_only'
    if any(str(item.get('status') or '') == 'ambiguous' for item in filtered_resolutions):
        return 'ambiguous'
    return 'no_patch'


def _apply_repair_patch_to_parsed_file(
    *,
    parsed_file: ParsedFile,
    approved_patch: dict[str, object],
    action: dict,
    target_fields: list[TargetField],
) -> tuple[ParsedFile, list[dict]]:
    base_columns, base_rows, _warnings = resolve_generation_source(parsed_file, target_fields=target_fields)
    base_row = dict(base_rows[0]) if base_rows else {}
    for field_name, value in approved_patch.items():
        base_row[str(field_name)] = value

    updated_columns = list(dict.fromkeys([*base_columns, *[str(key) for key in approved_patch.keys()]]))
    parsed_file.columns = updated_columns
    parsed_file.rows = [base_row] if base_row else []

    if parsed_file.form_model is not None:
        existing = {item.field: item for item in list(parsed_file.form_model.resolved_fields or [])}
        for field_name, value in approved_patch.items():
            existing[str(field_name)] = FormFieldResolution(
                field=str(field_name),
                status='resolved',
                resolved_by='repair_apply',
                value=value,
                candidates=[],
                source_ref={'source': 'repair_apply', 'action_kind': str(action.get('kind') or '')},
                confidence=1.0,
            )
        parsed_file.form_model.resolved_fields = list(existing.values())
        parsed_file.form_model.layout_meta['final_source_mode'] = 'repair_apply'
        parsed_file.form_model.layout_meta['applied_repair_patch'] = dict(approved_patch)

    updated_explainability = _build_form_explainability(parsed_file)
    resolved_fields = list(updated_explainability.get('resolved_fields', [])) if isinstance(updated_explainability, dict) else []
    return parsed_file, resolved_fields


def _preview_text(value: str, limit: int = 200) -> str:
    compact = ' '.join(value.split())
    if len(compact) <= limit:
        return compact
    return f'{compact[:limit]}...'


def _build_source_quality_adjustment(mapping_explainability: dict | None) -> dict | None:
    if not isinstance(mapping_explainability, dict):
        return None
    mapping_sources = list(mapping_explainability.get('mapping_sources') or [])
    adjusted_rows = [
        row for row in mapping_sources
        if isinstance(row, dict)
        and isinstance(row.get('candidate_metadata'), dict)
        and row['candidate_metadata'].get('source_routing_penalty_reason')
    ]
    if not adjusted_rows:
        return None

    reasons: dict[str, int] = {}
    affected_targets: list[str] = []
    strongest_penalty = 0.0
    for row in adjusted_rows:
        metadata = dict(row.get('candidate_metadata') or {})
        reason = str(metadata.get('source_routing_penalty_reason') or 'unknown')
        reasons[reason] = reasons.get(reason, 0) + 1
        target = str(row.get('target') or '').strip()
        if target and target not in affected_targets:
            affected_targets.append(target)
        try:
            strongest_penalty = max(strongest_penalty, float(metadata.get('source_routing_penalty') or 0.0))
        except (TypeError, ValueError):
            continue

    return {
        'applied': True,
        'adjusted_count': len(adjusted_rows),
        'reasons': reasons,
        'affected_targets': affected_targets,
        'strongest_penalty': strongest_penalty,
    }


@router.post('/source-preview')
async def source_preview(
    file: UploadFile = File(...),
    target_json: str | None = Form(default=None),
    selected_sheet: str | None = Form(default=None),
) -> dict:
    cleanup_expired_guest_files()

    filename = file.filename or 'uploaded_file'
    file_bytes = await file.read()
    saved_path = save_upload(file_bytes, filename, mode='guest')

    try:
        parsed = parse_file(saved_path, filename)
        source_warnings: list[str] = []
        resolved_target_fields: list[TargetField] | None = None

        if target_json and target_json.strip():
            try:
                resolved_target_fields, _target_payload, _target_schema, _target_schema_summary = parse_target_schema(target_json)
            except ParseError as exc:
                parsed.warnings.append(f'Target JSON was skipped during structure refresh: {exc}')

        try:
            source_columns, source_rows, source_warnings = resolve_generation_source(
                parsed,
                selected_sheet,
                target_fields=resolved_target_fields,
            )
            if source_columns or source_rows:
                parsed.columns = source_columns
                parsed.rows = source_rows
        except ParseError as exc:
            parsed.warnings.append(f'Structure refresh fallback: {exc}')

        if source_warnings:
            deduped_warnings = list(dict.fromkeys([*parsed.warnings, *source_warnings]))
            parsed.warnings = deduped_warnings

        try:
            saved_path.unlink(missing_ok=True)
        except PermissionError:
            parsed.warnings.append('Temporary preview file cleanup was deferred because the file is still locked by the OS.')
        return {
            'parsed_file': _model_to_dict(parsed),
            'form_explainability': _build_form_explainability(parsed),
        }
    except ParseError as exc:
        logger.warning('source preview parse failed: file=%s error=%s', filename, exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception('source preview failed: file=%s error=%s', filename, exc)
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.post('/source-preview-log')
def source_preview_log(
    payload: SourcePreviewRefreshLogPayload,
    current_user: dict[str, str] | None = Depends(get_optional_current_user),
) -> dict:
    logger.info(
        'source-preview-log: user_id=%s file=%s selected_sheet=%s result=%s active_sheet_changed=%s structure_changed=%s previous_sheet=%s next_sheet=%s previous_sheet_count=%s next_sheet_count=%s previous_columns=%s next_columns=%s previous_rows=%s next_rows=%s details=%s message=%s',
        current_user['id'] if current_user else 'guest',
        payload.file_name,
        payload.selected_sheet,
        payload.result,
        payload.active_sheet_changed,
        payload.structure_changed,
        payload.previous_sheet_name,
        payload.next_sheet_name,
        payload.previous_sheet_count,
        payload.next_sheet_count,
        payload.previous_column_count,
        payload.next_column_count,
        payload.previous_row_count,
        payload.next_row_count,
        ', '.join(payload.details),
        payload.message,
    )
    return {
        'logged': True,
        'result': payload.result,
        'user_id': current_user['id'] if current_user else None,
    }


@router.post('/repair-preview')
def repair_preview(payload: RepairPreviewPayload) -> dict:
    try:
        parsed_file = coerce_parsed_file(payload.parsed_file)
        if parsed_file.form_model is None or parsed_file.document_mode != 'form_layout_mode':
            raise HTTPException(status_code=400, detail='Repair preview is available only for form-like parsed documents.')

        action = _to_jsonish(payload.action)
        if not isinstance(action, dict):
            raise HTTPException(status_code=400, detail='Repair action payload must be an object.')

        target_fields = _coerce_repair_target_fields(
            target_json=payload.target_json,
            parsed_file=parsed_file,
            action=action,
        )
        local_chunks = _select_local_repair_chunks(parsed_file, action)
        preview_resolutions = [
            _model_to_dict(item)
            for item in preview_business_form_resolutions(
                parsed_file,
                target_fields=target_fields,
            )
        ]
        filtered_resolutions = _filter_preview_resolutions(resolutions=preview_resolutions, action=action)
        proposed_patch = _build_proposed_patch(filtered_resolutions)

        return {
            'supported': True,
            'preview_status': _repair_preview_status(action, proposed_patch, filtered_resolutions),
            'action': action,
            'target_fields': [_model_to_dict(field) for field in target_fields],
            'local_chunks': local_chunks,
            'proposed_resolutions': filtered_resolutions,
            'proposed_patch': proposed_patch,
            'form_explainability': _build_form_explainability(parsed_file),
            'warnings': [] if proposed_patch else ['Repair preview did not produce a concrete patch from the selected local chunks.'],
        }
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception('repair-preview failed: error=%s', exc)
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.post('/repair-apply')
def repair_apply(
    payload: RepairApplyPayload,
    current_user: dict[str, str] | None = Depends(get_optional_current_user),
) -> dict:
    try:
        parsed_file = coerce_parsed_file(payload.parsed_file)
        if parsed_file.form_model is None or parsed_file.document_mode != 'form_layout_mode':
            raise HTTPException(status_code=400, detail='Repair apply is available only for form-like parsed documents.')

        action = _to_jsonish(payload.action)
        if not isinstance(action, dict):
            raise HTTPException(status_code=400, detail='Repair action payload must be an object.')
        approved_patch = {
            str(key).strip(): value
            for key, value in dict(payload.approved_patch or {}).items()
            if str(key).strip()
        }
        if not approved_patch:
            raise HTTPException(status_code=400, detail='approved_patch must contain at least one field.')

        target_fields = _coerce_repair_target_fields(
            target_json=payload.target_json,
            parsed_file=parsed_file,
            action=action,
        )
        parsed_file, updated_resolved_fields = _apply_repair_patch_to_parsed_file(
            parsed_file=parsed_file,
            approved_patch=approved_patch,
            action=action,
            target_fields=target_fields,
        )
        form_explainability = _build_form_explainability(parsed_file)

        persistence = {
            'persisted': False,
            'generation_id': payload.generation_id,
            'version_id': None,
            'version_number': None,
            'session_id': None,
        }
        if payload.generation_id is not None:
            if current_user is None:
                raise HTTPException(status_code=401, detail='Authentication is required to persist repair apply.')
            persisted = apply_generation_repair_patch(
                user_id=current_user['id'],
                generation_id=int(payload.generation_id),
                parsed_file_json=_model_to_dict(parsed_file),
                approved_patch=approved_patch,
                notes=payload.notes,
                metadata=payload.metadata,
            )
            persistence = {
                'persisted': True,
                'generation_id': persisted.get('generation_id'),
                'version_id': persisted.get('version_id'),
                'version_number': persisted.get('version_number'),
                'session_id': persisted.get('session_id'),
            }

        return {
            'applied': True,
            'action': action,
            'approved_patch': approved_patch,
            'parsed_file': _model_to_dict(parsed_file),
            'form_explainability': form_explainability,
            'updated_resolved_fields': updated_resolved_fields,
            'persistence': persistence,
        }
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception('repair-apply failed: error=%s', exc)
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.post('/auth/send-code')
def send_code(payload: EmailCodePayload) -> dict:
    if is_email_registered(payload.email):
        raise HTTPException(status_code=409, detail='Пользователь с таким email уже зарегистрирован.')

    try:
        return request_registration_code(payload.email)
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/register')
def register(payload: RegisterPayload) -> dict:
    if is_email_registered(payload.email):
        raise HTTPException(status_code=409, detail='Пользователь с таким email уже зарегистрирован.')

    try:
        consume_registration_code(payload.email, payload.verification_code)
        profile = register_user(name=payload.name or '', email=payload.email, password=payload.password)
        return {
            **profile,
            'access_token': create_access_token(profile['id']),
            'token_type': 'bearer',
        }
    except UserConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EmailDeliveryError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/send-reset-code')
def send_reset_code(payload: EmailCodePayload) -> dict:
    try:
        return request_password_reset_code(payload.email, email_exists=is_email_registered(payload.email))
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/reset-password')
def reset_password(payload: ResetPasswordPayload) -> dict:
    try:
        if payload.reset_token and payload.reset_token.strip():
            consume_password_reset_token(payload.email, payload.reset_token)
        else:
            consume_password_reset_code(payload.email, payload.verification_code or '')
        update_user_password(email=payload.email, password=payload.password)
        return {'message': 'Пароль обновлён. Теперь можно войти с новым паролем.'}
    except UserConflictError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except UserNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/verify-reset-code')
def verify_reset_code(payload: VerifyResetCodePayload) -> dict:
    try:
        reset_token = verify_password_reset_code(payload.email, payload.verification_code)
        return {'message': 'Код подтверждён.', 'reset_token': reset_token}
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/send-email-change-code')
def send_email_change_code(
    payload: EmailChangeCodePayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        current_email, normalized_new_email = prepare_email_change(current_user['id'], payload.new_email)
        return request_email_change_code(
            user_id=current_user['id'],
            current_email=current_email,
            new_email=normalized_new_email,
        )
    except (UserNotFoundError, EmailChangeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/change-email')
def change_email(
    payload: ChangeEmailPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        if payload.current_password and payload.current_password.strip():
            verify_user_password(current_user['id'], payload.current_password)
        elif payload.verification_code and payload.verification_code.strip():
            consume_email_change_code(current_user['id'], payload.new_email, payload.verification_code)
        else:
            raise EmailChangeError('Подтвердите смену почты паролем или кодом из письма.')

        updated_profile = change_user_email(current_user['id'], payload.new_email)
        invalidate_user_cache(current_user['id'])
        return updated_profile
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except (UserNotFoundError, EmailChangeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/update-profile')
def update_profile(
    payload: UpdateProfilePayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        updated_profile = update_user_profile_name(current_user['id'], payload.name)
        invalidate_user_cache(current_user['id'])
        return updated_profile
    except (UserNotFoundError, ProfileUpdateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/auth/change-password')
def change_password(
    payload: ChangePasswordPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        change_user_password(current_user['id'], payload.current_password, payload.new_password)
        invalidate_user_cache(current_user['id'])
        return {'message': 'Пароль обновлён.'}
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except (UserNotFoundError, UserConflictError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/auth/login')
def login(payload: AuthPayload) -> dict:
    try:
        profile = login_user(email=payload.email, password=payload.password)
        return {
            **profile,
            'access_token': create_access_token(profile['id']),
            'token_type': 'bearer',
        }
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@router.post('/generate')
async def generate(
    file: UploadFile = File(...),
    target_json: str = Form(...),
    selected_sheet: str | None = Form(default=None),
    keep_guest_file: bool = Form(default=False),
    current_user: dict[str, str] | None = Depends(get_optional_current_user),
) -> dict:
    cleanup_expired_guest_files()
    usage_capture = begin_model_usage_capture()

    filename = file.filename or 'uploaded_file'
    file_bytes = await file.read()
    resolved_user_id = current_user['id'] if current_user else None
    mode = 'authorized' if resolved_user_id else 'guest'

    try:
        saved_path = save_upload(file_bytes, filename, mode=mode, user_id=resolved_user_id)
        upload_record_id = record_uploaded_file(
            file_path=saved_path,
            original_file_name=filename,
            file_bytes=file_bytes,
            mode=mode,
            user_id=resolved_user_id,
        )
        parsed = parse_file(saved_path, filename)
        target_fields, target_payload, target_schema, target_schema_summary = parse_target_schema(target_json)
        source_columns, source_rows, source_warnings = resolve_generation_source(
            parsed,
            selected_sheet,
            target_fields=target_fields,
        )
        parsed_file_json = json.dumps(_model_to_dict(parsed), ensure_ascii=False)
        resolved_schema_fingerprint_id = ensure_schema_fingerprint(
            parsed_file_json=parsed_file_json,
            target_json=json.dumps(target_payload, ensure_ascii=False),
            selected_sheet=selected_sheet,
            source_columns=source_columns,
            user_id=resolved_user_id,
        )
        mapping_result = resolve_generation_mappings_detailed(
            source_columns=source_columns,
            target_fields=target_fields,
            source_rows=source_rows,
            user_id=resolved_user_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            source_routing_context={
                'file_type': parsed.file_type,
                'document_mode': parsed.document_mode,
                'final_source_mode': (
                    dict(getattr(parsed.form_model, 'layout_meta', {}) or {}).get('final_source_mode')
                    if parsed.form_model is not None
                    else None
                ),
                'pdf_zone_routing': (
                    dict(getattr(parsed.form_model, 'layout_meta', {}) or {}).get('pdf_zone_routing', {})
                    if parsed.form_model is not None
                    else {}
                ),
            },
        )
        mappings = mapping_result['mappings']
        mapping_warnings = mapping_result['warnings']
        mapping_explainability = mapping_result['explainability']
        source_quality_adjustment = _build_source_quality_adjustment(mapping_explainability)
        ts_code = generate_typescript(target_fields, mappings)
        preview = build_preview(source_rows, target_fields, mappings)
        ts_validation = compile_typescript_code(ts_code)
        preview_validation = validate_preview_against_target_schema(preview, target_schema)
        quality_summary = {
            'operational_mapping_status': assess_mapping_operational_status(
                mapping_explainability['mapping_stats'],
                target_field_count=len(target_fields),
            ),
            'true_quality_metrics': None,
            'ts_syntax_valid': bool(ts_validation['valid']),
            'ts_runtime_preview_valid': bool(preview_validation['runtime_valid']),
            'output_schema_valid': bool(preview_validation['schema_valid']),
            'source_quality_adjustment': source_quality_adjustment,
        }
        validation_payload = {
            'target_schema': target_schema,
            'target_schema_summary': target_schema_summary,
            'ts_validation': ts_validation,
            'preview_validation': preview_validation,
            'mapping_explainability': mapping_explainability,
            'quality_summary': quality_summary,
        }
        all_warnings = parsed.warnings + source_warnings + mapping_warnings
        serialized_mappings = [_model_to_dict(m) for m in mappings]
        captured_model_usage = get_captured_model_usage()

        generation_id = None
        response_mappings = serialized_mappings
        if resolved_user_id:
            generation_id = save_generation(
                user_id=resolved_user_id,
                file_name=parsed.file_name,
                file_path=str(saved_path),
                file_type=parsed.file_type,
                target_json=json.dumps(target_payload, ensure_ascii=False),
                mappings_json=json.dumps(serialized_mappings, ensure_ascii=False),
                generated_typescript=ts_code,
                preview_json=json.dumps(preview, ensure_ascii=False),
                warnings_json=json.dumps(all_warnings, ensure_ascii=False),
                parsed_file_json=parsed_file_json,
                validation_json=json.dumps(validation_payload, ensure_ascii=False),
                selected_sheet=selected_sheet,
                source_columns=source_columns,
                upload_record_id=upload_record_id,
                schema_fingerprint_id=resolved_schema_fingerprint_id,
                promotion_mode='confirmed_only',
                generation_metrics={
                    'provider': captured_model_usage.get('provider') or 'gigachat',
                    'model_name': captured_model_usage.get('model_name') or 'unknown',
                    'input_tokens': captured_model_usage.get('input_tokens') or 0,
                    'output_tokens': captured_model_usage.get('output_tokens') or 0,
                    'total_tokens': captured_model_usage.get('total_tokens') or 0,
                    'estimated_tokens_saved': captured_model_usage.get('estimated_tokens_saved') or 0,
                    'cache_hits': 0,
                    'cache_misses': int(captured_model_usage.get('call_count') or 0),
                    'success': True,
                },
            )
            response_mappings = save_mapping_suggestions(
                generation_id=generation_id,
                mappings=serialized_mappings,
                user_id=resolved_user_id,
                schema_fingerprint_id=resolved_schema_fingerprint_id,
            )
        else:
            if not keep_guest_file:
                try:
                    saved_path.unlink(missing_ok=True)
                except PermissionError:
                    # Excel readers on Windows can briefly keep the file handle open.
                    all_warnings.append('Temporary upload cleanup was deferred because the file is still locked by the OS.')
            finalize_guest_upload(
                upload_id=upload_record_id,
                schema_fingerprint_id=resolved_schema_fingerprint_id,
                file_path=saved_path,
                keep_file=keep_guest_file,
            )

        return {
            'generation_id': generation_id,
            'schema_fingerprint_id': resolved_schema_fingerprint_id,
            'mode': mode,
            'parsed_file': _model_to_dict(parsed),
            'form_explainability': _build_form_explainability(parsed),
            'target_fields': [_model_to_dict(field) for field in target_fields],
            'mappings': response_mappings,
            'generated_typescript': ts_code,
            'preview': preview,
            'warnings': all_warnings,
            'token_usage': {
                'provider': captured_model_usage.get('provider'),
                'model_name': captured_model_usage.get('model_name'),
                'input_tokens': int(captured_model_usage.get('input_tokens') or 0),
                'output_tokens': int(captured_model_usage.get('output_tokens') or 0),
                'total_tokens': int(captured_model_usage.get('total_tokens') or 0),
            },
            'target_schema': target_schema,
            'required_fields': target_schema_summary['required_fields'],
            'ts_valid': bool(ts_validation['valid']),
            'ts_diagnostics': ts_validation['diagnostics'],
            'preview_diagnostics': preview_validation['diagnostics'],
            'mapping_operational_status': quality_summary['operational_mapping_status'],
            'mapping_quality': quality_summary['operational_mapping_status'],
            'mapping_eval_metrics': quality_summary['true_quality_metrics'],
            'source_quality_adjustment': quality_summary['source_quality_adjustment'],
            'ts_syntax_valid': quality_summary['ts_syntax_valid'],
            'ts_runtime_preview_valid': quality_summary['ts_runtime_preview_valid'],
            'output_schema_valid': quality_summary['output_schema_valid'],
            'mapping_stats': mapping_explainability['mapping_stats'],
            'mapping_sources': mapping_explainability['mapping_sources'],
            'unresolved_fields': mapping_explainability['unresolved_fields'],
            'suggestions': mapping_explainability['suggestions'],
        }
    except ParseError as exc:
        logger.warning(
            'generate parse failed: file=%s user_id=%s selected_sheet=%s target_json_preview=%s error=%s',
            filename,
            resolved_user_id or 'guest',
            selected_sheet,
            _preview_text(target_json),
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            'generate failed: file=%s user_id=%s selected_sheet=%s keep_guest_file=%s error=%s',
            filename,
            resolved_user_id or 'guest',
            selected_sheet,
            keep_guest_file,
            exc,
        )
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


    finally:
        end_model_usage_capture(usage_capture)


@router.post('/draft-json')
async def draft_json(
    file: UploadFile = File(...),
    selected_sheet: str | None = Form(default=None),
    keep_guest_file: bool = Form(default=False),
    current_user: dict[str, str] | None = Depends(get_optional_current_user),
) -> dict:
    cleanup_expired_guest_files()

    filename = file.filename or 'uploaded_file'
    file_bytes = await file.read()
    resolved_user_id = current_user['id'] if current_user else None
    mode = 'authorized' if resolved_user_id else 'guest'

    try:
        saved_path = save_upload(file_bytes, filename, mode=mode, user_id=resolved_user_id)
        upload_record_id = record_uploaded_file(
            file_path=saved_path,
            original_file_name=filename,
            file_bytes=file_bytes,
            mode=mode,
            user_id=resolved_user_id,
        )
        parsed = parse_file(saved_path, filename)
        effective_selected_sheet = selected_sheet.strip() if selected_sheet and selected_sheet.strip() else None
        source_columns, source_rows, source_warnings = resolve_draft_json_source(parsed, selected_sheet=effective_selected_sheet)
        parsed_file_json = json.dumps(_model_to_dict(parsed), ensure_ascii=False)
        schema_fingerprint_id = ensure_schema_fingerprint(
            parsed_file_json=parsed_file_json,
            target_json=json.dumps({}, ensure_ascii=False),
            selected_sheet=effective_selected_sheet,
            source_columns=source_columns,
            user_id=resolved_user_id,
        )
        draft_payload, field_suggestions, draft_warnings = generate_draft_json_for_source(
            source_columns=source_columns,
            source_rows=source_rows,
            user_id=resolved_user_id,
            schema_fingerprint_id=schema_fingerprint_id,
        )
        all_warnings = parsed.warnings + source_warnings + draft_warnings
        response_field_suggestions = field_suggestions

        if resolved_user_id:
            response_field_suggestions = save_draft_json_suggestions(
                suggestions=field_suggestions,
                user_id=resolved_user_id,
                schema_fingerprint_id=schema_fingerprint_id,
            )

        if not keep_guest_file:
            try:
                saved_path.unlink(missing_ok=True)
            except PermissionError:
                all_warnings.append('Temporary upload cleanup was deferred because the file is still locked by the OS.')

        finalize_guest_upload(
            upload_id=upload_record_id,
            schema_fingerprint_id=schema_fingerprint_id,
            file_path=saved_path,
            keep_file=bool(resolved_user_id) or keep_guest_file,
        )

        return {
            'mode': mode,
            'schema_fingerprint_id': schema_fingerprint_id,
            'parsed_file': _model_to_dict(parsed),
            'form_explainability': _build_form_explainability(parsed),
            'draft_json': draft_payload,
            'field_suggestions': response_field_suggestions,
            'warnings': all_warnings,
        }
    except ParseError as exc:
        logger.warning(
            'draft-json parse failed: file=%s user_id=%s selected_sheet=%s error=%s',
            filename,
            resolved_user_id or 'guest',
            selected_sheet,
            exc,
        )
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            'draft-json failed: file=%s user_id=%s selected_sheet=%s keep_guest_file=%s error=%s',
            filename,
            resolved_user_id or 'guest',
            selected_sheet,
            keep_guest_file,
            exc,
        )
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.get('/history')
def history(current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    items = get_history(current_user['id'])
    normalized = []
    for item in items:
        normalized.append(
            {
                'id': str(item['id']),
                'user_id': item['user_id'],
                'file_name': item['file_name'],
                'file_type': item['file_type'],
                'selected_sheet': item['selected_sheet'],
                'parsed_file': json.loads(item['parsed_file_json']) if item.get('parsed_file_json') else None,
                'target_json': json.loads(item['target_json']),
                'mappings': json.loads(item['mappings_json']),
                'generated_typescript': item['generated_typescript'],
                'preview': json.loads(item['preview_json']),
                'warnings': json.loads(item['warnings_json']),
                'validation': json.loads(item['validation_json']) if item.get('validation_json') else {},
                'token_usage': {
                    'provider': item.get('token_usage_provider'),
                    'model_name': item.get('token_usage_model_name'),
                    'input_tokens': int(item.get('token_usage_input_tokens') or 0),
                    'output_tokens': int(item.get('token_usage_output_tokens') or 0),
                    'total_tokens': int(item.get('token_usage_total_tokens') or 0),
                },
                'created_at': item['created_at'],
            }
        )
    return {'items': normalized}


@router.delete('/history/{generation_id}')
def delete_history_entry(generation_id: int, current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    try:
        return delete_generation_history_entry(user_id=current_user['id'], generation_id=generation_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/learning/summary')
def learning_summary(current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    return get_learning_summary(current_user['id'])


@router.get('/learning/events')
def learning_events(limit: int = 20, current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    return {'items': list_learning_events(current_user['id'], limit=limit)}


@router.get('/learning/memory')
def learning_memory(limit: int = 20, current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    return get_learning_memory_layers(current_user['id'], limit=limit)


@router.get('/learning/templates')
def learning_templates(current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    return {'items': list_user_templates(current_user['id'])}


@router.post('/learning/templates')
def learning_save_template(
    payload: UserTemplatePayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    return save_user_template(
        user_id=current_user['id'],
        name=payload.name,
        template_kind=payload.template_kind,
        template_json=payload.template_json,
        description=payload.description,
        target_json=payload.target_json,
        generated_typescript=payload.generated_typescript,
        prompt_suffix=payload.prompt_suffix,
        schema_fingerprint_id=payload.schema_fingerprint_id,
        is_shared=payload.is_shared,
        metadata=payload.metadata,
    )


@router.post('/learning/corrections')
def learning_save_corrections(
    payload: CorrectionSessionPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    return save_correction_session(
        user_id=current_user['id'],
        generation_id=payload.generation_id,
        session_type=payload.session_type,
        schema_fingerprint_id=payload.schema_fingerprint_id,
        notes=payload.notes,
        metadata=payload.metadata,
        corrections=[_model_to_dict(correction) for correction in payload.corrections],
    )


@router.post('/learning/mapping-feedback')
def learning_mapping_feedback(
    payload: MappingFeedbackPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    return apply_mapping_feedback(
        user_id=current_user['id'],
        generation_id=payload.generation_id,
        schema_fingerprint_id=payload.schema_fingerprint_id,
        notes=payload.notes,
        metadata=payload.metadata,
        feedback=[_model_to_dict(item) for item in payload.feedback],
    )


@router.post('/learning/confirm-generation')
def learning_confirm_generation(
    payload: GenerationConfirmationPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return confirm_generation_learning(
            user_id=current_user['id'],
            generation_id=payload.generation_id,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/draft-json-feedback')
def learning_draft_json_feedback(
    payload: DraftJsonFeedbackPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return apply_draft_json_feedback(
            user_id=current_user['id'],
            schema_fingerprint_id=payload.schema_fingerprint_id,
            draft_json=payload.draft_json,
            feedback=[_model_to_dict(item) for item in payload.feedback],
            template_name=payload.template_name,
            save_as_template=payload.save_as_template,
            notes=payload.notes,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/promote-patterns')
def learning_promote_patterns(
    payload: PatternPromotionPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    return promote_stable_pattern_candidates(
        min_support_count=payload.min_support_count,
        min_distinct_users=payload.min_distinct_users,
        min_stability_score=payload.min_stability_score,
        max_drift_score=payload.max_drift_score,
        min_acceptance_rate=payload.min_acceptance_rate,
        max_semantic_conflict_rate=payload.max_semantic_conflict_rate,
        max_sensitivity_score=payload.max_sensitivity_score,
        min_generalizability_score=payload.min_generalizability_score,
    )


@router.post('/learning/training-snapshots')
def learning_create_training_snapshot(
    payload: TrainingSnapshotPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return create_training_snapshot(
            name=payload.name,
            min_quality_score=payload.min_quality_score,
            include_statuses=list(payload.include_statuses),
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-snapshots/{snapshot_id}/export')
def learning_export_training_snapshot(
    snapshot_id: int,
    payload: TrainingSnapshotExportPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return export_training_snapshot(
            snapshot_id=snapshot_id,
            overwrite=payload.overwrite,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-runs')
def learning_create_training_run(
    payload: TrainingRunPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return create_model_training_run(
            snapshot_id=payload.snapshot_id,
            model_family=payload.model_family,
            base_model=payload.base_model,
            train_params=payload.train_params,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-runs/{training_run_id}/start')
def learning_start_training_run(
    training_run_id: int,
    payload: TrainingRunStartPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return start_model_training_run(
            training_run_id=training_run_id,
            trainer_mode=payload.trainer_mode,
            auto_activate=payload.auto_activate,
            serving_provider=payload.serving_provider,
            serving_base_url=payload.serving_base_url,
            serving_model_name=payload.serving_model_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-runs/{training_run_id}/complete')
def learning_complete_training_run(
    training_run_id: int,
    payload: TrainingRunCompletionPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return complete_model_training_run(
            training_run_id=training_run_id,
            artifact_uri=payload.artifact_uri,
            metrics=payload.metrics,
            notes=payload.notes,
            serving_provider=payload.serving_provider,
            serving_base_url=payload.serving_base_url,
            serving_model_name=payload.serving_model_name,
            auto_activate=payload.auto_activate,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-runs/{training_run_id}/activate')
def learning_activate_training_run(
    training_run_id: int,
    payload: TrainingRunActivationPayload,
    current_user: dict[str, str] = Depends(get_current_user),
) -> dict:
    try:
        return activate_model_training_run(
            training_run_id=training_run_id,
            provider=payload.provider,
            base_url=payload.base_url,
            model_name=payload.model_name,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/learning/model-runtime')
def learning_model_runtime(current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    return get_model_runtime_status()


@router.get('/auth/profile')
def auth_profile(current_user: dict[str, str] = Depends(get_current_user)) -> dict:
    try:
        return get_user_profile(current_user['id'])
    except UserNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
