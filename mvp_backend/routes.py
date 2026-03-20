from __future__ import annotations

import json

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from email_service import (
    EmailDeliveryError,
    EmailServiceError,
    consume_registration_code,
    request_registration_code,
)
from generator import build_preview, generate_typescript
from matcher import map_fields
from models import AuthPayload, CorrectionSessionPayload, EmailCodePayload, RegisterPayload, UserTemplatePayload
from parsers import ParseError, infer_target_fields, parse_file, resolve_generation_source
from storage import (
    InvalidCredentialsError,
    UserConflictError,
    cleanup_expired_guest_files,
    ensure_schema_fingerprint,
    finalize_guest_upload,
    get_history,
    get_learning_summary,
    is_email_registered,
    list_user_templates,
    login_user,
    record_uploaded_file,
    register_user,
    save_correction_session,
    save_generation,
    save_upload,
    save_user_template,
)

router = APIRouter()


def _model_to_dict(value: object) -> dict:
    if hasattr(value, 'model_dump'):
        return value.model_dump()
    if hasattr(value, 'dict'):
        return value.dict()
    raise TypeError(f'Unsupported model value: {type(value)!r}')


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
        return register_user(name=payload.name or '', email=payload.email, password=payload.password)
    except UserConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except EmailDeliveryError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/login')
def login(payload: AuthPayload) -> dict:
    try:
        return login_user(email=payload.email, password=payload.password)
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc


@router.post('/generate')
async def generate(
    file: UploadFile = File(...),
    target_json: str = Form(...),
    user_id: str | None = Form(default=None),
    selected_sheet: str | None = Form(default=None),
    keep_guest_file: bool = Form(default=False),
) -> dict:
    cleanup_expired_guest_files()

    filename = file.filename or 'uploaded_file'
    file_bytes = await file.read()
    mode = 'authorized' if user_id else 'guest'

    try:
        saved_path = save_upload(file_bytes, filename, mode=mode, user_id=user_id)
        upload_record_id = record_uploaded_file(
            file_path=saved_path,
            original_file_name=filename,
            file_bytes=file_bytes,
            mode=mode,
            user_id=user_id,
        )
        parsed = parse_file(saved_path, filename)
        target_fields, target_payload = infer_target_fields(target_json)
        source_columns, source_rows, source_warnings = resolve_generation_source(parsed, selected_sheet)
        mappings, mapping_warnings = map_fields(source_columns, target_fields)
        ts_code = generate_typescript(target_fields, mappings)
        preview = build_preview(source_rows, target_fields, mappings)
        all_warnings = parsed.warnings + source_warnings + mapping_warnings

        generation_id = None
        if user_id:
            generation_id = save_generation(
                user_id=user_id,
                file_name=parsed.file_name,
                file_path=str(saved_path),
                file_type=parsed.file_type,
                target_json=json.dumps(target_payload, ensure_ascii=False),
                mappings_json=json.dumps([_model_to_dict(m) for m in mappings], ensure_ascii=False),
                generated_typescript=ts_code,
                preview_json=json.dumps(preview, ensure_ascii=False),
                warnings_json=json.dumps(all_warnings, ensure_ascii=False),
                parsed_file_json=json.dumps(_model_to_dict(parsed), ensure_ascii=False),
                selected_sheet=selected_sheet,
                source_columns=source_columns,
                upload_record_id=upload_record_id,
            )
        else:
            schema_fingerprint_id = ensure_schema_fingerprint(
                parsed_file_json=json.dumps(_model_to_dict(parsed), ensure_ascii=False),
                target_json=json.dumps(target_payload, ensure_ascii=False),
                selected_sheet=selected_sheet,
                source_columns=source_columns,
            )
            if not keep_guest_file:
                try:
                    saved_path.unlink(missing_ok=True)
                except PermissionError:
                    # Excel readers on Windows can briefly keep the file handle open.
                    all_warnings.append('Temporary upload cleanup was deferred because the file is still locked by the OS.')
            finalize_guest_upload(
                upload_id=upload_record_id,
                schema_fingerprint_id=schema_fingerprint_id,
                file_path=saved_path,
                keep_file=keep_guest_file,
            )

        return {
            'generation_id': generation_id,
            'mode': mode,
            'parsed_file': _model_to_dict(parsed),
            'target_fields': [_model_to_dict(field) for field in target_fields],
            'mappings': [_model_to_dict(m) for m in mappings],
            'generated_typescript': ts_code,
            'preview': preview,
            'warnings': all_warnings,
        }
    except ParseError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.get('/history/{user_id}')
def history(user_id: str) -> dict:
    items = get_history(user_id)
    normalized = []
    for item in items:
        normalized.append(
            {
                'id': str(item['id']),
                'user_id': item['user_id'],
                'file_name': item['file_name'],
                'file_type': item['file_type'],
                'target_json': json.loads(item['target_json']),
                'mappings': json.loads(item['mappings_json']),
                'generated_typescript': item['generated_typescript'],
                'preview': json.loads(item['preview_json']),
                'warnings': json.loads(item['warnings_json']),
                'created_at': item['created_at'],
            }
        )
    return {'items': normalized}


@router.get('/learning/summary/{user_id}')
def learning_summary(user_id: str) -> dict:
    return get_learning_summary(user_id)


@router.get('/learning/templates/{user_id}')
def learning_templates(user_id: str) -> dict:
    return {'items': list_user_templates(user_id)}


@router.post('/learning/templates')
def learning_save_template(payload: UserTemplatePayload) -> dict:
    return save_user_template(
        user_id=payload.user_id,
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
def learning_save_corrections(payload: CorrectionSessionPayload) -> dict:
    return save_correction_session(
        user_id=payload.user_id,
        generation_id=payload.generation_id,
        session_type=payload.session_type,
        schema_fingerprint_id=payload.schema_fingerprint_id,
        notes=payload.notes,
        metadata=payload.metadata,
        corrections=[_model_to_dict(correction) for correction in payload.corrections],
    )
