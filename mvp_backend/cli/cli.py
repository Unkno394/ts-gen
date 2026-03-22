from __future__ import annotations

import argparse
import json
import time
import sys
from pathlib import Path
from typing import Any

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from generator import build_preview, generate_typescript
from learning_pipeline import resolve_generation_mappings_detailed
from model_client import begin_model_usage_capture, end_model_usage_capture, get_captured_model_usage
from parsers import ParseError, parse_file, parse_target_schema, resolve_draft_json_source, resolve_generation_source, preview_business_form_resolutions
from routes import (
    _apply_repair_patch_to_parsed_file,
    _build_form_explainability,
    _build_proposed_patch,
    _coerce_repair_target_fields,
    _filter_preview_resolutions,
    _repair_preview_status,
    _select_local_repair_chunks,
)
from storage import cleanup_guest_files, get_generation_by_id, get_history, init_db, save_generation, apply_generation_repair_patch
from validation import assess_mapping_operational_status, compile_typescript_code, validate_preview_against_target_schema
from draft_json_pipeline import generate_draft_json_for_source
from auth_session import create_access_token, invalidate_user_cache
from email_service import (
    EmailDeliveryError,
    EmailServiceError,
    consume_email_change_code,
    consume_password_reset_code,
    consume_password_reset_token,
    consume_registration_code,
    request_email_change_code,
    request_password_reset_code,
    request_registration_code,
    verify_password_reset_code,
)
from storage import (
    EmailChangeError,
    InvalidCredentialsError,
    ProfileUpdateError,
    UserConflictError,
    UserNotFoundError,
    change_user_email,
    change_user_password,
    get_user_profile,
    is_email_registered,
    login_user,
    prepare_email_change,
    register_user,
    update_user_password,
    update_user_profile_name,
    verify_user_password,
)

CLI_SESSION_PATH = BACKEND_DIR / '.runtime' / 'cli_session.json'


def _ensure_file(path: Path, label: str) -> Path:
    if not path.exists() or not path.is_file():
        raise SystemExit(f'{label} does not exist: {path}')
    return path


def _read_schema(schema_path: Path) -> tuple[list[Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        return parse_target_schema(schema_path.read_text(encoding='utf-8'))
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc


def _print_warnings(warnings: list[str]) -> None:
    if not warnings:
        return
    print('\nWarnings:')
    for warning in warnings:
        print(f'  - {warning}')


def _print_json(title: str, value: Any) -> None:
    print(f'\n{title}:')
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _mapping_to_json(mappings: list[Any]) -> list[dict[str, Any]]:
    return [mapping.model_dump() if hasattr(mapping, 'model_dump') else dict(mapping) for mapping in mappings]


def _model_to_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, 'model_dump'):
        return value.model_dump()
    if hasattr(value, 'dict'):
        return value.dict()
    if isinstance(value, dict):
        return value
    raise SystemExit(f'Unsupported model value: {type(value)!r}')


def _quality_status_label(status: str | None) -> str:
    if status == 'high':
        return 'high'
    if status == 'medium':
        return 'medium'
    if status == 'low':
        return 'low'
    return 'unknown'


def _save_cli_session(*, profile: dict[str, str], access_token: str, token_type: str = 'bearer') -> None:
    CLI_SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)
    CLI_SESSION_PATH.write_text(
        json.dumps(
            {
                'profile': profile,
                'access_token': access_token,
                'token_type': token_type,
                'saved_at': int(time.time()),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )


def _load_cli_session() -> dict[str, Any] | None:
    if not CLI_SESSION_PATH.exists():
        return None
    try:
        payload = json.loads(CLI_SESSION_PATH.read_text(encoding='utf-8'))
    except Exception:  # noqa: BLE001
        return None
    return payload if isinstance(payload, dict) else None


def _clear_cli_session() -> None:
    CLI_SESSION_PATH.unlink(missing_ok=True)


def _require_cli_session() -> dict[str, Any]:
    session = _load_cli_session()
    if not session:
        raise SystemExit('CLI session not found. Use auth-login or auth-register first.')
    profile = session.get('profile')
    if not isinstance(profile, dict) or not str(profile.get('id') or '').strip():
        raise SystemExit('CLI session is corrupted. Remove it with auth-logout and sign in again.')
    return session


def _resolve_repair_context(
    *,
    input_path: Path,
    schema_path: Path,
    selected_sheet: str | None = None,
) -> dict[str, Any]:
    parsed_file = parse_file(input_path, input_path.name)
    target_fields, _target_payload, _target_schema, _target_schema_summary = _read_schema(schema_path)
    resolve_generation_source(
        parsed_file,
        selected_sheet=selected_sheet,
        target_fields=target_fields,
    )
    form_explainability = _build_form_explainability(parsed_file)
    if not form_explainability:
        raise SystemExit('Repair commands are available only for form-like parsed documents.')
    repair_plan = form_explainability.get('repair_plan') or {}
    actions = list(repair_plan.get('actions') or [])
    return {
        'parsed_file': parsed_file,
        'target_fields': target_fields,
        'form_explainability': form_explainability,
        'repair_plan': repair_plan,
        'actions': actions,
    }


def _pick_repair_action(actions: list[dict[str, Any]], action_index: int | None) -> dict[str, Any]:
    if not actions:
        raise SystemExit('Repair plan does not contain any actions.')
    if action_index is None:
        raise SystemExit('Specify --action-index. Use repair-plan to inspect available actions.')
    if action_index < 0 or action_index >= len(actions):
        raise SystemExit(f'Action index is out of range: {action_index}. Available: 0..{len(actions) - 1}')
    return actions[action_index]


def _run_generation_core(
    *,
    input_path: Path,
    schema_path: Path,
    selected_sheet: str | None = None,
    user_id: str | None = None,
) -> dict[str, Any]:
    parsed_file = parse_file(input_path, input_path.name)
    target_fields, target_payload, target_schema, target_schema_summary = _read_schema(schema_path)
    source_columns, source_rows, source_warnings = resolve_generation_source(
        parsed_file,
        selected_sheet=selected_sheet,
        target_fields=target_fields,
    )

    usage_capture = begin_model_usage_capture()
    try:
        mapping_result = resolve_generation_mappings_detailed(
            source_columns=source_columns,
            target_fields=target_fields,
            source_rows=source_rows,
            user_id=user_id,
            schema_fingerprint_id=None,
            source_routing_context={
                'file_type': parsed_file.file_type,
                'document_mode': parsed_file.document_mode,
                'final_source_mode': (
                    dict(getattr(parsed_file.form_model, 'layout_meta', {}) or {}).get('final_source_mode')
                    if parsed_file.form_model is not None
                    else None
                ),
                'pdf_zone_routing': (
                    dict(getattr(parsed_file.form_model, 'layout_meta', {}) or {}).get('pdf_zone_routing', {})
                    if parsed_file.form_model is not None
                    else {}
                ),
            },
        )
        mappings = mapping_result['mappings']
        mapping_warnings = mapping_result['warnings']
        mapping_explainability = mapping_result['explainability']
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
        }
        warnings = parsed_file.warnings + source_warnings + mapping_warnings
        return {
            'parsed_file': parsed_file,
            'target_fields': target_fields,
            'target_payload': target_payload,
            'target_schema': target_schema,
            'target_schema_summary': target_schema_summary,
            'source_columns': source_columns,
            'source_rows': source_rows,
            'mappings': mappings,
            'mapping_explainability': mapping_explainability,
            'generated_typescript': ts_code,
            'preview': preview,
            'warnings': warnings,
            'ts_validation': ts_validation,
            'preview_validation': preview_validation,
            'quality_summary': quality_summary,
            'token_usage': get_captured_model_usage(),
        }
    finally:
        end_model_usage_capture(usage_capture)


def cmd_generate(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')
    out_path = Path(args.out)
    init_db()

    try:
        result = _run_generation_core(
            input_path=input_path,
            schema_path=schema_path,
            selected_sheet=args.sheet,
            user_id=None if args.guest else args.user_id,
        )
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    parsed_file = result['parsed_file']
    target_fields = result['target_fields']
    target_payload = result['target_payload']
    mappings = result['mappings']
    code = result['generated_typescript']
    preview = result['preview']
    warnings = result['warnings']

    out_path.write_text(code, encoding='utf-8')

    generation_id: int | None = None
    if not args.guest:
        generation_id = save_generation(
            user_id=args.user_id,
            file_name=parsed_file.file_name,
            file_path=str(input_path),
            file_type=parsed_file.file_type,
            target_json=json.dumps(target_payload, ensure_ascii=False),
            mappings_json=json.dumps(_mapping_to_json(mappings), ensure_ascii=False),
            generated_typescript=code,
            preview_json=json.dumps(preview, ensure_ascii=False),
            warnings_json=json.dumps(warnings, ensure_ascii=False),
            validation_json=json.dumps(
                {
                    'target_schema': result['target_schema'],
                    'target_schema_summary': result['target_schema_summary'],
                    'ts_validation': result['ts_validation'],
                    'preview_validation': result['preview_validation'],
                    'mapping_explainability': result['mapping_explainability'],
                    'quality_summary': result['quality_summary'],
                },
                ensure_ascii=False,
            ),
            parsed_file_json=json.dumps(_model_to_dict(parsed_file), ensure_ascii=False),
            selected_sheet=args.sheet,
            source_columns=list(result['source_columns']),
        )

    print('TSGen generate')
    print(f'Input file:    {input_path.name}')
    print(f'Columns:       {len(result["source_columns"])}')
    print(f'Schema fields: {len(target_fields)}')
    print(f'Output:        {out_path}')
    print(f'Mode:          {"guest" if args.guest else "authorized"}')
    if args.sheet:
        print(f'Sheet:         {args.sheet}')
    if generation_id is not None:
        print(f'History id:    {generation_id}')
    print(f'Mapping ready: {_quality_status_label(result["quality_summary"]["operational_mapping_status"]["status"])}')
    print(f'TS syntax:     {"valid" if result["ts_validation"]["valid"] else "invalid"}')
    print(f'Preview/schema: {"valid" if result["preview_validation"]["schema_valid"] else "mismatch"}')
    token_usage = result['token_usage']
    if int(token_usage.get('total_tokens') or 0) > 0:
        print(f'Tokens:        {token_usage["total_tokens"]}')

    _print_warnings(warnings)

    if args.show_mapping:
        _print_json('Mapping', _mapping_to_json(mappings))
    if args.show_preview:
        _print_json('Preview JSON', preview)
    if args.show_validation:
        _print_json('TS validation', result['ts_validation'])
        _print_json('Preview validation', result['preview_validation'])
    if args.show_quality:
        _print_json('Quality summary', result['quality_summary'])
        _print_json('Mapping stats', result['mapping_explainability']['mapping_stats'])


def cmd_source_preview(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')

    try:
        parsed = parse_file(input_path, input_path.name)
        target_fields = None
        if args.schema:
            schema_path = _ensure_file(Path(args.schema), 'Schema file')
            target_fields, _target_payload, _target_schema, _target_schema_summary = _read_schema(schema_path)
        source_columns, source_rows, source_warnings = resolve_generation_source(
            parsed,
            selected_sheet=args.sheet,
            target_fields=target_fields,
        )
        if source_columns or source_rows:
            parsed.columns = source_columns
            parsed.rows = source_rows
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    print('TSGen source-preview')
    print(f'File:            {parsed.file_name}')
    print(f'Format:          {parsed.file_type}')
    print(f'Content type:    {parsed.content_type}')
    print(f'Document mode:   {parsed.document_mode}')
    print(f'Extraction:      {parsed.extraction_status}')
    if args.sheet:
        print(f'Sheet:           {args.sheet}')
    _print_json('Parsed file', _model_to_dict(parsed))
    _print_warnings(parsed.warnings + source_warnings)


def cmd_draft_json(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    init_db()

    try:
        parsed = parse_file(input_path, input_path.name)
        source_columns, source_rows, source_warnings = resolve_draft_json_source(parsed, selected_sheet=args.sheet)
        draft_payload, field_suggestions, draft_warnings = generate_draft_json_for_source(
            source_columns=source_columns,
            source_rows=source_rows,
            user_id=args.user_id,
            schema_fingerprint_id=None,
        )
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    print('TSGen draft-json')
    print(f'Input file: {input_path.name}')
    if args.sheet:
        print(f'Sheet:      {args.sheet}')
    _print_json('Draft JSON', draft_payload)
    if args.show_suggestions:
        _print_json('Field suggestions', field_suggestions)
    _print_warnings(parsed.warnings + source_warnings + draft_warnings)


def cmd_preview(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')

    try:
        parsed = parse_file(input_path, input_path.name)
        target_fields = None
        if args.schema:
            schema_path = _ensure_file(Path(args.schema), 'Schema file')
            target_fields, _target_payload, _target_schema, _target_schema_summary = _read_schema(schema_path)
        source_columns, source_rows, source_warnings = resolve_generation_source(
            parsed,
            selected_sheet=args.sheet,
            target_fields=target_fields,
        )
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    print('TSGen preview')
    print(f'File:     {parsed.file_name}')
    print(f'Format:   {parsed.file_type}')
    print(f'Columns:  {len(source_columns)}')
    if args.sheet:
        print(f'Sheet:    {args.sheet}')
    if source_columns:
        print(f'Detected: {", ".join(source_columns)}')

    _print_json(f'Sample rows (first {min(len(source_rows), args.rows)})', source_rows[: args.rows])
    _print_warnings(parsed.warnings + source_warnings)


def cmd_explain(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')

    try:
        result = _run_generation_core(
            input_path=input_path,
            schema_path=schema_path,
            selected_sheet=args.sheet,
            user_id=args.user_id,
        )
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    print('TSGen explain')
    print(f'Input file: {input_path.name}')
    if args.sheet:
        print(f'Sheet:      {args.sheet}')
    print(f'Mapping ready: {_quality_status_label(result["quality_summary"]["operational_mapping_status"]["status"])}')
    _print_json('Mapping', _mapping_to_json(result['mappings']))
    _print_json('Mapping stats', result['mapping_explainability']['mapping_stats'])
    _print_json('Unresolved fields', result['mapping_explainability']['unresolved_fields'])
    _print_json('Mapping sources', result['mapping_explainability']['mapping_sources'])
    _print_json('Suggestions', result['mapping_explainability']['suggestions'])
    _print_warnings(result['warnings'])


def cmd_repair_plan(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')

    context = _resolve_repair_context(
        input_path=input_path,
        schema_path=schema_path,
        selected_sheet=args.sheet,
    )

    print('TSGen repair-plan')
    print(f'Input file: {input_path.name}')
    if args.sheet:
        print(f'Sheet:      {args.sheet}')
    _print_json('Form explainability', context['form_explainability'])
    _print_json('Repair actions', [{'index': index, **action} for index, action in enumerate(context['actions'])])


def cmd_repair_preview(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')

    context = _resolve_repair_context(
        input_path=input_path,
        schema_path=schema_path,
        selected_sheet=args.sheet,
    )
    action = _pick_repair_action(context['actions'], args.action_index)
    target_fields = _coerce_repair_target_fields(
        target_json=Path(args.schema).read_text(encoding='utf-8'),
        parsed_file=context['parsed_file'],
        action=action,
    )
    local_chunks = _select_local_repair_chunks(context['parsed_file'], action)
    preview_resolutions = [
        _model_to_dict(item)
        for item in preview_business_form_resolutions(
            context['parsed_file'],
            target_fields=target_fields,
        )
    ]
    filtered_resolutions = _filter_preview_resolutions(resolutions=preview_resolutions, action=action)
    proposed_patch = _build_proposed_patch(filtered_resolutions)

    payload = {
        'supported': True,
        'preview_status': _repair_preview_status(action, proposed_patch, filtered_resolutions),
        'action': action,
        'target_fields': [_model_to_dict(field) for field in target_fields],
        'local_chunks': local_chunks,
        'proposed_resolutions': filtered_resolutions,
        'proposed_patch': proposed_patch,
        'form_explainability': context['form_explainability'],
        'warnings': [] if proposed_patch else ['Repair preview did not produce a concrete patch from the selected local chunks.'],
    }
    print('TSGen repair-preview')
    print(f'Input file: {input_path.name}')
    print(f'Action idx:  {args.action_index}')
    _print_json('Repair preview', payload)


def cmd_repair_apply(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')

    context = _resolve_repair_context(
        input_path=input_path,
        schema_path=schema_path,
        selected_sheet=args.sheet,
    )
    action = _pick_repair_action(context['actions'], args.action_index)
    target_fields = _coerce_repair_target_fields(
        target_json=Path(args.schema).read_text(encoding='utf-8'),
        parsed_file=context['parsed_file'],
        action=action,
    )
    preview_resolutions = [
        _model_to_dict(item)
        for item in preview_business_form_resolutions(
            context['parsed_file'],
            target_fields=target_fields,
        )
    ]
    filtered_resolutions = _filter_preview_resolutions(resolutions=preview_resolutions, action=action)
    auto_patch = _build_proposed_patch(filtered_resolutions)

    approved_patch: dict[str, Any] = {}
    if args.patch_file:
        approved_patch = json.loads(_ensure_file(Path(args.patch_file), 'Patch file').read_text(encoding='utf-8'))
    elif auto_patch:
        approved_patch = auto_patch
    else:
      raise SystemExit('No patch provided and auto preview did not produce a patch.')

    parsed_file, updated_resolved_fields = _apply_repair_patch_to_parsed_file(
        parsed_file=context['parsed_file'],
        approved_patch=approved_patch,
        action=action,
        target_fields=target_fields,
    )
    persistence = {
        'persisted': False,
        'generation_id': args.generation_id,
        'version_id': None,
        'version_number': None,
        'session_id': None,
    }
    if args.persist and args.generation_id is not None and args.user_id:
        persisted = apply_generation_repair_patch(
            user_id=args.user_id,
            generation_id=int(args.generation_id),
            parsed_file_json=_model_to_dict(parsed_file),
            approved_patch=approved_patch,
            notes=args.notes,
            metadata={},
        )
        persistence = {
            'persisted': True,
            'generation_id': persisted.get('generation_id'),
            'version_id': persisted.get('version_id'),
            'version_number': persisted.get('version_number'),
            'session_id': persisted.get('session_id'),
        }

    payload = {
        'applied': True,
        'action': action,
        'approved_patch': approved_patch,
        'parsed_file': _model_to_dict(parsed_file),
        'form_explainability': _build_form_explainability(parsed_file),
        'updated_resolved_fields': updated_resolved_fields,
        'persistence': persistence,
    }
    print('TSGen repair-apply')
    print(f'Input file: {input_path.name}')
    print(f'Action idx:  {args.action_index}')
    _print_json('Repair apply', payload)


def cmd_history(args: argparse.Namespace) -> None:
    init_db()
    records = get_history(user_id=args.user_id, limit=args.limit)
    if not records:
        print('History is empty.')
        return

    print(f'TSGen history ({args.user_id})')
    for index, record in enumerate(records, start=1):
        if args.full:
            _print_json(f'Entry #{index}', record)
            continue

        print(f'\n[{index}] {record["id"]}')
        print(f'  created_at: {record["created_at"]}')
        print(f'  file_name:  {record["file_name"]}')
        print(f'  file_type:  {record["file_type"]}')


def cmd_show(args: argparse.Namespace) -> None:
    init_db()
    entry = get_generation_by_id(args.id)
    if not entry:
        raise SystemExit(f'History entry not found: {args.id}')

    print(f'TSGen show — {args.id}')
    print(f'created_at: {entry["created_at"]}')
    print(f'file_name:  {entry["file_name"]}')
    _print_json('Target JSON', json.loads(entry['target_json']))
    _print_json('Warnings', json.loads(entry['warnings_json']))
    print('\nGenerated TS:')
    print(entry['generated_typescript'])


def cmd_cleanup(args: argparse.Namespace) -> None:
    result = cleanup_guest_files(ttl_hours=args.ttl_hours, dry_run=args.dry_run)
    print('TSGen cleanup')
    _print_json('Cleanup result', result)


def cmd_auth_send_code(args: argparse.Namespace) -> None:
    init_db()
    if is_email_registered(args.email):
        raise SystemExit('Пользователь с таким email уже зарегистрирован.')
    try:
        result = request_registration_code(args.email)
    except EmailServiceError as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-send-code')
    _print_json('Result', result)


def cmd_auth_register(args: argparse.Namespace) -> None:
    init_db()
    if is_email_registered(args.email):
        raise SystemExit('Пользователь с таким email уже зарегистрирован.')
    try:
        consume_registration_code(args.email, args.verification_code)
        profile = register_user(name=args.name or '', email=args.email, password=args.password)
    except (UserConflictError, EmailDeliveryError, EmailServiceError) as exc:
        raise SystemExit(str(exc)) from exc
    access_token = create_access_token(profile['id'])
    _save_cli_session(profile=profile, access_token=access_token)
    print('TSGen auth-register')
    _print_json('Profile', {**profile, 'access_token': access_token, 'token_type': 'bearer'})


def cmd_auth_login(args: argparse.Namespace) -> None:
    init_db()
    try:
        profile = login_user(email=args.email, password=args.password)
    except InvalidCredentialsError as exc:
        raise SystemExit(str(exc)) from exc
    access_token = create_access_token(profile['id'])
    _save_cli_session(profile=profile, access_token=access_token)
    print('TSGen auth-login')
    _print_json('Profile', {**profile, 'access_token': access_token, 'token_type': 'bearer'})


def cmd_auth_logout(_args: argparse.Namespace) -> None:
    _clear_cli_session()
    print('TSGen auth-logout')
    print('CLI session cleared.')


def cmd_auth_profile(_args: argparse.Namespace) -> None:
    init_db()
    session = _require_cli_session()
    user_id = str(session['profile']['id'])
    try:
        profile = get_user_profile(user_id)
    except UserNotFoundError as exc:
        raise SystemExit(str(exc)) from exc
    _save_cli_session(profile=profile, access_token=str(session.get('access_token') or ''), token_type=str(session.get('token_type') or 'bearer'))
    print('TSGen auth-profile')
    _print_json('Profile', profile)


def cmd_auth_update_profile(args: argparse.Namespace) -> None:
    init_db()
    session = _require_cli_session()
    user_id = str(session['profile']['id'])
    try:
        profile = update_user_profile_name(user_id, args.name)
        invalidate_user_cache(user_id)
    except (UserNotFoundError, ProfileUpdateError) as exc:
        raise SystemExit(str(exc)) from exc
    _save_cli_session(profile=profile, access_token=str(session.get('access_token') or ''), token_type=str(session.get('token_type') or 'bearer'))
    print('TSGen auth-update-profile')
    _print_json('Profile', profile)


def cmd_auth_change_password(args: argparse.Namespace) -> None:
    init_db()
    session = _require_cli_session()
    user_id = str(session['profile']['id'])
    try:
        change_user_password(user_id, args.current_password, args.new_password)
        invalidate_user_cache(user_id)
    except (InvalidCredentialsError, UserNotFoundError, UserConflictError) as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-change-password')
    print('Пароль обновлён.')


def cmd_auth_send_email_change_code(args: argparse.Namespace) -> None:
    init_db()
    session = _require_cli_session()
    user_id = str(session['profile']['id'])
    try:
        current_email, normalized_new_email = prepare_email_change(user_id, args.new_email)
        result = request_email_change_code(
            user_id=user_id,
            current_email=current_email,
            new_email=normalized_new_email,
        )
    except (UserNotFoundError, EmailChangeError, EmailServiceError) as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-send-email-change-code')
    _print_json('Result', result)


def cmd_auth_change_email(args: argparse.Namespace) -> None:
    init_db()
    session = _require_cli_session()
    user_id = str(session['profile']['id'])
    try:
        if args.current_password:
            verify_user_password(user_id, args.current_password)
        elif args.verification_code:
            consume_email_change_code(user_id, args.new_email, args.verification_code)
        else:
            raise SystemExit('Укажите --current-password или --verification-code.')
        profile = change_user_email(user_id, args.new_email)
        invalidate_user_cache(user_id)
    except (InvalidCredentialsError, UserNotFoundError, EmailChangeError, EmailServiceError) as exc:
        raise SystemExit(str(exc)) from exc
    _save_cli_session(profile=profile, access_token=str(session.get('access_token') or ''), token_type=str(session.get('token_type') or 'bearer'))
    print('TSGen auth-change-email')
    _print_json('Profile', profile)


def cmd_auth_send_reset_code(args: argparse.Namespace) -> None:
    init_db()
    try:
        result = request_password_reset_code(args.email, email_exists=is_email_registered(args.email))
    except EmailServiceError as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-send-reset-code')
    _print_json('Result', result)


def cmd_auth_verify_reset_code(args: argparse.Namespace) -> None:
    try:
        reset_token = verify_password_reset_code(args.email, args.verification_code)
    except EmailServiceError as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-verify-reset-code')
    _print_json('Result', {'message': 'Код подтверждён.', 'reset_token': reset_token})


def cmd_auth_reset_password(args: argparse.Namespace) -> None:
    init_db()
    try:
        if args.reset_token and args.reset_token.strip():
            consume_password_reset_token(args.email, args.reset_token)
        else:
            consume_password_reset_code(args.email, args.verification_code or '')
        update_user_password(email=args.email, password=args.password)
    except (UserConflictError, UserNotFoundError, EmailServiceError) as exc:
        raise SystemExit(str(exc)) from exc
    print('TSGen auth-reset-password')
    print('Пароль обновлён. Теперь можно войти с новым паролем.')


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='TSGen CLI')
    subparsers = parser.add_subparsers(dest='command', required=True)

    auth_send_code_parser = subparsers.add_parser('auth-send-code', help='Send registration code to email')
    auth_send_code_parser.add_argument('--email', required=True)
    auth_send_code_parser.set_defaults(func=cmd_auth_send_code)

    auth_register_parser = subparsers.add_parser('auth-register', help='Register a new user and store CLI session')
    auth_register_parser.add_argument('--email', required=True)
    auth_register_parser.add_argument('--password', required=True)
    auth_register_parser.add_argument('--verification-code', required=True)
    auth_register_parser.add_argument('--name', default='')
    auth_register_parser.set_defaults(func=cmd_auth_register)

    auth_login_parser = subparsers.add_parser('auth-login', help='Login and store CLI session')
    auth_login_parser.add_argument('--email', required=True)
    auth_login_parser.add_argument('--password', required=True)
    auth_login_parser.set_defaults(func=cmd_auth_login)

    auth_logout_parser = subparsers.add_parser('auth-logout', help='Clear CLI session')
    auth_logout_parser.set_defaults(func=cmd_auth_logout)

    auth_profile_parser = subparsers.add_parser('auth-profile', help='Show current profile from CLI session')
    auth_profile_parser.set_defaults(func=cmd_auth_profile)

    auth_update_profile_parser = subparsers.add_parser('auth-update-profile', help='Update profile display name')
    auth_update_profile_parser.add_argument('--name', required=True)
    auth_update_profile_parser.set_defaults(func=cmd_auth_update_profile)

    auth_change_password_parser = subparsers.add_parser('auth-change-password', help='Change current user password')
    auth_change_password_parser.add_argument('--current-password', required=True)
    auth_change_password_parser.add_argument('--new-password', required=True)
    auth_change_password_parser.set_defaults(func=cmd_auth_change_password)

    auth_send_email_change_code_parser = subparsers.add_parser('auth-send-email-change-code', help='Send email change verification code')
    auth_send_email_change_code_parser.add_argument('--new-email', required=True)
    auth_send_email_change_code_parser.set_defaults(func=cmd_auth_send_email_change_code)

    auth_change_email_parser = subparsers.add_parser('auth-change-email', help='Change current user email')
    auth_change_email_parser.add_argument('--new-email', required=True)
    auth_change_email_parser.add_argument('--current-password')
    auth_change_email_parser.add_argument('--verification-code')
    auth_change_email_parser.set_defaults(func=cmd_auth_change_email)

    auth_send_reset_code_parser = subparsers.add_parser('auth-send-reset-code', help='Send password reset code')
    auth_send_reset_code_parser.add_argument('--email', required=True)
    auth_send_reset_code_parser.set_defaults(func=cmd_auth_send_reset_code)

    auth_verify_reset_code_parser = subparsers.add_parser('auth-verify-reset-code', help='Verify password reset code and return reset token')
    auth_verify_reset_code_parser.add_argument('--email', required=True)
    auth_verify_reset_code_parser.add_argument('--verification-code', required=True)
    auth_verify_reset_code_parser.set_defaults(func=cmd_auth_verify_reset_code)

    auth_reset_password_parser = subparsers.add_parser('auth-reset-password', help='Reset password by code or reset token')
    auth_reset_password_parser.add_argument('--email', required=True)
    auth_reset_password_parser.add_argument('--password', required=True)
    auth_reset_password_parser.add_argument('--verification-code')
    auth_reset_password_parser.add_argument('--reset-token')
    auth_reset_password_parser.set_defaults(func=cmd_auth_reset_password)

    generate_parser = subparsers.add_parser('generate', help='Generate TypeScript from input file and schema')
    generate_parser.add_argument('--input', '-i', required=True)
    generate_parser.add_argument('--schema', '-s', required=True)
    generate_parser.add_argument('--out', '-o', default='parser.ts')
    generate_parser.add_argument('--sheet')
    generate_parser.add_argument('--user-id', default='cli-user')
    generate_parser.add_argument('--guest', action='store_true')
    generate_parser.add_argument('--show-preview', action='store_true')
    generate_parser.add_argument('--show-mapping', action='store_true')
    generate_parser.add_argument('--show-validation', action='store_true')
    generate_parser.add_argument('--show-quality', action='store_true')
    generate_parser.set_defaults(func=cmd_generate)

    source_preview_parser = subparsers.add_parser('source-preview', help='Show resolved source preview similar to backend source preview')
    source_preview_parser.add_argument('--input', '-i', required=True)
    source_preview_parser.add_argument('--schema', '-s')
    source_preview_parser.add_argument('--sheet')
    source_preview_parser.set_defaults(func=cmd_source_preview)

    draft_json_parser = subparsers.add_parser('draft-json', help='Generate draft JSON and field suggestions')
    draft_json_parser.add_argument('--input', '-i', required=True)
    draft_json_parser.add_argument('--sheet')
    draft_json_parser.add_argument('--user-id', default='cli-user')
    draft_json_parser.add_argument('--show-suggestions', action='store_true')
    draft_json_parser.set_defaults(func=cmd_draft_json)

    preview_parser = subparsers.add_parser('preview', help='Show parsed preview for an input file')
    preview_parser.add_argument('--input', '-i', required=True)
    preview_parser.add_argument('--schema', '-s')
    preview_parser.add_argument('--sheet')
    preview_parser.add_argument('--rows', '-r', type=int, default=5)
    preview_parser.set_defaults(func=cmd_preview)

    explain_parser = subparsers.add_parser('explain', help='Show field mapping to target schema')
    explain_parser.add_argument('--input', '-i', required=True)
    explain_parser.add_argument('--schema', '-s', required=True)
    explain_parser.add_argument('--sheet')
    explain_parser.add_argument('--user-id', default='cli-user')
    explain_parser.set_defaults(func=cmd_explain)

    repair_plan_parser = subparsers.add_parser('repair-plan', help='Show repair plan for form-like documents')
    repair_plan_parser.add_argument('--input', '-i', required=True)
    repair_plan_parser.add_argument('--schema', '-s', required=True)
    repair_plan_parser.add_argument('--sheet')
    repair_plan_parser.set_defaults(func=cmd_repair_plan)

    repair_preview_parser = subparsers.add_parser('repair-preview', help='Preview one repair action from repair plan')
    repair_preview_parser.add_argument('--input', '-i', required=True)
    repair_preview_parser.add_argument('--schema', '-s', required=True)
    repair_preview_parser.add_argument('--sheet')
    repair_preview_parser.add_argument('--action-index', type=int, required=True)
    repair_preview_parser.set_defaults(func=cmd_repair_preview)

    repair_apply_parser = subparsers.add_parser('repair-apply', help='Apply one repair action from repair plan')
    repair_apply_parser.add_argument('--input', '-i', required=True)
    repair_apply_parser.add_argument('--schema', '-s', required=True)
    repair_apply_parser.add_argument('--sheet')
    repair_apply_parser.add_argument('--action-index', type=int, required=True)
    repair_apply_parser.add_argument('--patch-file')
    repair_apply_parser.add_argument('--persist', action='store_true')
    repair_apply_parser.add_argument('--generation-id', type=int)
    repair_apply_parser.add_argument('--user-id')
    repair_apply_parser.add_argument('--notes', default='')
    repair_apply_parser.set_defaults(func=cmd_repair_apply)

    history_parser = subparsers.add_parser('history', help='Show saved history for a user')
    history_parser.add_argument('--user-id', default='cli-user')
    history_parser.add_argument('--limit', '-n', type=int, default=20)
    history_parser.add_argument('--full', action='store_true')
    history_parser.set_defaults(func=cmd_history)

    show_parser = subparsers.add_parser('show', help='Show one history entry')
    show_parser.add_argument('--id', type=int, required=True)
    show_parser.set_defaults(func=cmd_show)

    cleanup_parser = subparsers.add_parser('cleanup', help='Cleanup expired guest files')
    cleanup_parser.add_argument('--ttl-hours', type=int, default=24)
    cleanup_parser.add_argument('--dry-run', action='store_true')
    cleanup_parser.set_defaults(func=cmd_cleanup)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
