from __future__ import annotations

import json
import logging

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

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
from models import (
    AuthPayload,
    ChangeEmailPayload,
    ChangePasswordPayload,
    CorrectionSessionPayload,
    DraftJsonFeedbackPayload,
    EmailChangeCodePayload,
    EmailCodePayload,
    GenerationConfirmationPayload,
    MappingFeedbackPayload,
    PatternPromotionPayload,
    RegisterPayload,
    ResetPasswordPayload,
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
from parsers import ParseError, infer_target_fields, parse_file, resolve_generation_source
from storage import (
    EmailChangeError,
    InvalidCredentialsError,
    ProfileUpdateError,
    UserConflictError,
    UserNotFoundError,
    apply_mapping_feedback,
    apply_draft_json_feedback,
    change_user_password,
    change_user_email,
    cleanup_expired_guest_files,
    confirm_generation_learning,
    create_model_training_run,
    create_training_snapshot,
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

router = APIRouter()
logger = logging.getLogger(__name__)


def _model_to_dict(value: object) -> dict:
    if hasattr(value, 'model_dump'):
        return value.model_dump()
    if hasattr(value, 'dict'):
        return value.dict()
    raise TypeError(f'Unsupported model value: {type(value)!r}')


def _preview_text(value: str, limit: int = 200) -> str:
    compact = ' '.join(value.split())
    if len(compact) <= limit:
        return compact
    return f'{compact[:limit]}...'


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
def send_email_change_code(payload: EmailChangeCodePayload) -> dict:
    try:
        current_email, normalized_new_email = prepare_email_change(payload.user_id, payload.new_email)
        return request_email_change_code(
            user_id=payload.user_id,
            current_email=current_email,
            new_email=normalized_new_email,
        )
    except (UserNotFoundError, EmailChangeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/change-email')
def change_email(payload: ChangeEmailPayload) -> dict:
    try:
        if payload.current_password and payload.current_password.strip():
            verify_user_password(payload.user_id, payload.current_password)
        elif payload.verification_code and payload.verification_code.strip():
            consume_email_change_code(payload.user_id, payload.new_email, payload.verification_code)
        else:
            raise EmailChangeError('Подтвердите смену почты паролем или кодом из письма.')

        return change_user_email(payload.user_id, payload.new_email)
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except (UserNotFoundError, EmailChangeError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except EmailServiceError as exc:
        raise HTTPException(status_code=exc.status_code, detail=str(exc)) from exc


@router.post('/auth/update-profile')
def update_profile(payload: UpdateProfilePayload) -> dict:
    try:
        return update_user_profile_name(payload.user_id, payload.name)
    except (UserNotFoundError, ProfileUpdateError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/auth/change-password')
def change_password(payload: ChangePasswordPayload) -> dict:
    try:
        change_user_password(payload.user_id, payload.current_password, payload.new_password)
        return {'message': 'Пароль обновлён.'}
    except InvalidCredentialsError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except (UserNotFoundError, UserConflictError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
        parsed_file_json = json.dumps(_model_to_dict(parsed), ensure_ascii=False)
        resolved_schema_fingerprint_id = ensure_schema_fingerprint(
            parsed_file_json=parsed_file_json,
            target_json=json.dumps(target_payload, ensure_ascii=False),
            selected_sheet=selected_sheet,
            source_columns=source_columns,
            user_id=user_id,
        )
        mapping_result = resolve_generation_mappings_detailed(
            source_columns=source_columns,
            target_fields=target_fields,
            source_rows=source_rows,
            user_id=user_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
        )
        mappings = mapping_result['mappings']
        mapping_warnings = mapping_result['warnings']
        mapping_explainability = mapping_result['explainability']
        ts_code = generate_typescript(target_fields, mappings)
        preview = build_preview(source_rows, target_fields, mappings)
        all_warnings = parsed.warnings + source_warnings + mapping_warnings
        serialized_mappings = [_model_to_dict(m) for m in mappings]

        generation_id = None
        response_mappings = serialized_mappings
        if user_id:
            generation_id = save_generation(
                user_id=user_id,
                file_name=parsed.file_name,
                file_path=str(saved_path),
                file_type=parsed.file_type,
                target_json=json.dumps(target_payload, ensure_ascii=False),
                mappings_json=json.dumps(serialized_mappings, ensure_ascii=False),
                generated_typescript=ts_code,
                preview_json=json.dumps(preview, ensure_ascii=False),
                warnings_json=json.dumps(all_warnings, ensure_ascii=False),
                parsed_file_json=parsed_file_json,
                selected_sheet=selected_sheet,
                source_columns=source_columns,
                upload_record_id=upload_record_id,
                schema_fingerprint_id=resolved_schema_fingerprint_id,
                promotion_mode='confirmed_only',
            )
            response_mappings = save_mapping_suggestions(
                generation_id=generation_id,
                mappings=serialized_mappings,
                user_id=user_id,
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
            'target_fields': [_model_to_dict(field) for field in target_fields],
            'mappings': response_mappings,
            'generated_typescript': ts_code,
            'preview': preview,
            'warnings': all_warnings,
            'mapping_stats': mapping_explainability['mapping_stats'],
            'mapping_sources': mapping_explainability['mapping_sources'],
            'unresolved_fields': mapping_explainability['unresolved_fields'],
            'suggestions': mapping_explainability['suggestions'],
        }
    except ParseError as exc:
        logger.warning(
            'generate parse failed: file=%s user_id=%s selected_sheet=%s target_json_preview=%s error=%s',
            filename,
            user_id or 'guest',
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
            user_id or 'guest',
            selected_sheet,
            keep_guest_file,
            exc,
        )
        raise HTTPException(status_code=500, detail='Произошла внутренняя ошибка сервера. Попробуйте ещё раз.') from exc


@router.post('/draft-json')
async def draft_json(
    file: UploadFile = File(...),
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
        source_columns, source_rows, source_warnings = resolve_generation_source(parsed, selected_sheet)
        parsed_file_json = json.dumps(_model_to_dict(parsed), ensure_ascii=False)
        schema_fingerprint_id = ensure_schema_fingerprint(
            parsed_file_json=parsed_file_json,
            target_json=json.dumps({}, ensure_ascii=False),
            selected_sheet=selected_sheet,
            source_columns=source_columns,
            user_id=user_id,
        )
        draft_payload, field_suggestions, draft_warnings = generate_draft_json_for_source(
            source_columns=source_columns,
            source_rows=source_rows,
            user_id=user_id,
            schema_fingerprint_id=schema_fingerprint_id,
        )
        all_warnings = parsed.warnings + source_warnings + draft_warnings
        response_field_suggestions = field_suggestions

        if user_id:
            response_field_suggestions = save_draft_json_suggestions(
                suggestions=field_suggestions,
                user_id=user_id,
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
            keep_file=bool(user_id) or keep_guest_file,
        )

        return {
            'mode': mode,
            'schema_fingerprint_id': schema_fingerprint_id,
            'parsed_file': _model_to_dict(parsed),
            'draft_json': draft_payload,
            'field_suggestions': response_field_suggestions,
            'warnings': all_warnings,
        }
    except ParseError as exc:
        logger.warning(
            'draft-json parse failed: file=%s user_id=%s selected_sheet=%s error=%s',
            filename,
            user_id or 'guest',
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
            user_id or 'guest',
            selected_sheet,
            keep_guest_file,
            exc,
        )
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
                'selected_sheet': item['selected_sheet'],
                'parsed_file': json.loads(item['parsed_file_json']) if item.get('parsed_file_json') else None,
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


@router.get('/learning/events/{user_id}')
def learning_events(user_id: str, limit: int = 20) -> dict:
    return {'items': list_learning_events(user_id, limit=limit)}


@router.get('/learning/memory/{user_id}')
def learning_memory(user_id: str, limit: int = 20) -> dict:
    return get_learning_memory_layers(user_id, limit=limit)


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


@router.post('/learning/mapping-feedback')
def learning_mapping_feedback(payload: MappingFeedbackPayload) -> dict:
    return apply_mapping_feedback(
        user_id=payload.user_id,
        generation_id=payload.generation_id,
        schema_fingerprint_id=payload.schema_fingerprint_id,
        notes=payload.notes,
        metadata=payload.metadata,
        feedback=[_model_to_dict(item) for item in payload.feedback],
    )


@router.post('/learning/confirm-generation')
def learning_confirm_generation(payload: GenerationConfirmationPayload) -> dict:
    try:
        return confirm_generation_learning(
            user_id=payload.user_id,
            generation_id=payload.generation_id,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/draft-json-feedback')
def learning_draft_json_feedback(payload: DraftJsonFeedbackPayload) -> dict:
    try:
        return apply_draft_json_feedback(
            user_id=payload.user_id,
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
def learning_promote_patterns(payload: PatternPromotionPayload) -> dict:
    return promote_stable_pattern_candidates(
        min_support_count=payload.min_support_count,
        min_distinct_users=payload.min_distinct_users,
        min_stability_score=payload.min_stability_score,
        max_drift_score=payload.max_drift_score,
    )


@router.post('/learning/training-snapshots')
def learning_create_training_snapshot(payload: TrainingSnapshotPayload) -> dict:
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
def learning_export_training_snapshot(snapshot_id: int, payload: TrainingSnapshotExportPayload) -> dict:
    try:
        return export_training_snapshot(
            snapshot_id=snapshot_id,
            overwrite=payload.overwrite,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post('/learning/training-runs')
def learning_create_training_run(payload: TrainingRunPayload) -> dict:
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
def learning_start_training_run(training_run_id: int, payload: TrainingRunStartPayload) -> dict:
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
def learning_complete_training_run(training_run_id: int, payload: TrainingRunCompletionPayload) -> dict:
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
def learning_activate_training_run(training_run_id: int, payload: TrainingRunActivationPayload) -> dict:
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
def learning_model_runtime() -> dict:
    return get_model_runtime_status()


@router.get('/auth/profile/{user_id}')
def auth_profile(user_id: str) -> dict:
    try:
        return get_user_profile(user_id)
    except UserNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
