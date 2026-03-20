from __future__ import annotations

import hashlib
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from infra.database import DatabaseClient, create_database
from infra.security import hash_password, verify_password

PROJECT_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = PROJECT_DIR / '.runtime'
DB_PATH = RUNTIME_DIR / 'app.sqlite'
LEGACY_DB_PATH = RUNTIME_DIR / 'history.db'
BASE_DIR = RUNTIME_DIR / 'storage'
UPLOAD_DIR = BASE_DIR / 'uploads'
GUEST_DIR = UPLOAD_DIR / 'guest'
AUTH_DIR = UPLOAD_DIR / 'authorized'
GUEST_TTL_SECONDS = 60 * 60 * 24
AUTO_DATASET_CANDIDATE_THRESHOLD = 0.72
AUTO_DATASET_APPROVE_THRESHOLD = 0.92
LEARNING_VECTOR_PROVIDER = 'local'
LEARNING_VECTOR_MODEL = 'heuristic-v1'
FEATURE_TOKEN_LIMIT = 48

FIELD_NORMALIZE_RE = re.compile(r'[^a-zA-Zа-яА-Я0-9]+')

_db_client: DatabaseClient | None = None


class UserConflictError(ValueError):
    pass


class InvalidCredentialsError(ValueError):
    pass


def is_email_registered(email: str) -> bool:
    normalized_email = email.strip().lower()
    if not normalized_email:
        return False

    db = get_db()
    row = db.get(
        '''
        SELECT 1
        FROM users
        WHERE email = :email
        ''',
        {'email': normalized_email},
    )
    return row is not None


def ensure_dirs() -> None:
    for path in [RUNTIME_DIR, BASE_DIR, UPLOAD_DIR, GUEST_DIR, AUTH_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def get_db() -> DatabaseClient:
    global _db_client
    ensure_dirs()
    if _db_client is None:
        _db_client = create_database(str(DB_PATH))
    return _db_client


def init_db() -> None:
    ensure_dirs()
    get_db()
    migrate_legacy_history()


def register_user(name: str, email: str, password: str) -> dict[str, str]:
    email = email.strip().lower()
    name = name.strip()
    password = password.strip()

    if not email:
        raise UserConflictError('Введите email.')
    if not password:
        raise UserConflictError('Введите пароль.')
    if len(password) < 8:
        raise UserConflictError('Пароль должен содержать минимум 8 символов.')

    db = get_db()
    external_id = str(uuid.uuid4())
    display_name = name or email.split('@', 1)[0]
    password_hash = hash_password(password)

    try:
        db.run(
            '''
            INSERT INTO users (email, external_id, display_name, password_hash)
            VALUES (:email, :external_id, :display_name, :password_hash)
            ''',
            {
                'email': email,
                'external_id': external_id,
                'display_name': display_name,
                'password_hash': password_hash,
            },
        )
    except sqlite3.IntegrityError as error:
        raise UserConflictError('Пользователь с таким email уже зарегистрирован.') from error

    return {'id': external_id, 'name': display_name, 'email': email}


def login_user(email: str, password: str) -> dict[str, str]:
    email = email.strip().lower()
    password = password.strip()
    if not email or not password:
        raise InvalidCredentialsError('Введите email и пароль.')

    db = get_db()
    row = db.get(
        '''
        SELECT external_id, email, display_name, password_hash
        FROM users
        WHERE email = :email
        ''',
        {'email': email},
    )
    if row is None or not verify_password(password, row['password_hash']):
        raise InvalidCredentialsError('Неверный email или пароль.')

    return {
        'id': str(row['external_id'] or ''),
        'name': str(row['display_name'] or row['email'] or 'Desktop User'),
        'email': str(row['email'] or ''),
    }


def save_upload(content: bytes, filename: str, mode: str, user_id: str | None = None) -> Path:
    ensure_dirs()
    safe_name = filename.replace('/', '_').replace('\\', '_')
    owner_folder = f'user_{user_id}' if mode == 'authorized' and user_id else f'guest_{int(time.time())}'
    base = AUTH_DIR if mode == 'authorized' else GUEST_DIR
    target_dir = base / owner_folder
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_path = Path(safe_name)
    unique_suffix = uuid.uuid4().hex[:12]
    stored_name = f'{safe_path.stem}_{unique_suffix}{safe_path.suffix}'
    path = target_dir / stored_name
    path.write_bytes(content)
    return path


def record_uploaded_file(
    *,
    file_path: Path,
    original_file_name: str,
    file_bytes: bytes,
    mode: str,
    user_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    db = get_db()
    internal_user_id = _resolve_internal_user_id(user_id=user_id)
    file_type = file_path.suffix.lower().lstrip('.') or 'unknown'
    mime_type = mimetypes.guess_type(original_file_name)[0]
    content_hash = hashlib.sha256(file_bytes).hexdigest()
    now = _timestamp()
    payload = _merge_json_objects(
        {
            'original_file_name': original_file_name,
            'stored_file_name': file_path.name,
            'size_bytes': len(file_bytes),
        },
        metadata,
    )

    cursor = db.run(
        '''
        INSERT INTO uploaded_files (
            user_id,
            upload_mode,
            original_file_name,
            stored_file_name,
            storage_path,
            file_type,
            mime_type,
            size_bytes,
            content_hash,
            status,
            metadata_json,
            uploaded_at,
            last_accessed_at
        )
        VALUES (
            :user_id,
            :upload_mode,
            :original_file_name,
            :stored_file_name,
            :storage_path,
            :file_type,
            :mime_type,
            :size_bytes,
            :content_hash,
            'active',
            :metadata_json,
            :uploaded_at,
            :last_accessed_at
        )
        ''',
        {
            'user_id': internal_user_id,
            'upload_mode': mode,
            'original_file_name': original_file_name,
            'stored_file_name': file_path.name,
            'storage_path': str(file_path),
            'file_type': file_type,
            'mime_type': mime_type,
            'size_bytes': len(file_bytes),
            'content_hash': content_hash,
            'metadata_json': _json_or_none(payload),
            'uploaded_at': now,
            'last_accessed_at': now,
        },
    )
    return int(cursor.lastrowid)


def finalize_guest_upload(
    upload_id: int | None,
    schema_fingerprint_id: int | None,
    file_path: Path,
    keep_file: bool,
) -> None:
    if upload_id is None:
        return

    now = _timestamp()
    status = 'processed' if keep_file else 'deleted'
    get_db().run(
        '''
        UPDATE uploaded_files
        SET
            schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
            status = :status,
            expires_at = :expires_at,
            last_accessed_at = :last_accessed_at
        WHERE id = :id
        ''',
        {
            'id': upload_id,
            'schema_fingerprint_id': schema_fingerprint_id,
            'status': status,
            'expires_at': None if keep_file else now,
            'last_accessed_at': now,
        },
    )

    if not keep_file:
        _mark_uploaded_files_by_storage_prefix(file_path, status='deleted', expires_at=now)


def save_generation(
    user_id: str,
    file_name: str,
    file_path: str,
    file_type: str,
    target_json: str,
    mappings_json: str,
    generated_typescript: str,
    preview_json: str,
    warnings_json: str,
    parsed_file_json: str | Any | None = None,
    selected_sheet: str | None = None,
    source_columns: list[str] | None = None,
    upload_record_id: int | None = None,
    schema_fingerprint_id: int | None = None,
) -> int:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    safe_target_json = _ensure_json_text(target_json, {})
    safe_mappings_json = _ensure_json_text(mappings_json, [])
    safe_preview_json = _ensure_json_text(preview_json, [])
    safe_warnings_json = _ensure_json_text(warnings_json, [])
    safe_parsed_file_json = _ensure_json_text(
        parsed_file_json if parsed_file_json is not None else _build_fallback_parsed_file(file_name, file_type),
        _build_fallback_parsed_file(file_name, file_type),
    )
    title = file_name if not selected_sheet else f'{file_name} [{selected_sheet}]'
    source_payload = _build_source_payload(safe_parsed_file_json, file_name, file_type, selected_sheet)

    with db.transaction():
        generation_cursor = db.run(
            '''
            INSERT INTO generations (
                user_id,
                schema_id,
                title,
                source_payload,
                source_payload_format,
                status
            )
            VALUES (
                :user_id,
                NULL,
                :title,
                :source_payload,
                'json',
                'completed'
            )
            ''',
            {
                'user_id': internal_user_id,
                'title': title,
                'source_payload': source_payload,
            },
        )
        generation_id = int(generation_cursor.lastrowid)

        version_cursor = db.run(
            '''
            INSERT INTO generation_versions (
                generation_id,
                parent_version_id,
                version_number,
                change_type,
                note,
                target_json,
                generated_typescript
            )
            VALUES (
                :generation_id,
                NULL,
                1,
                'initial',
                :note,
                :target_json,
                :generated_typescript
            )
            ''',
            {
                'generation_id': generation_id,
                'note': 'Initial generation',
                'target_json': safe_target_json,
                'generated_typescript': generated_typescript,
            },
        )
        version_id = int(version_cursor.lastrowid)

        db.run(
            '''
            UPDATE generations
            SET current_version_id = :version_id, status = 'completed'
            WHERE id = :generation_id
            ''',
            {
                'version_id': version_id,
                'generation_id': generation_id,
            },
        )

        artifact_cursor = db.run(
            '''
            INSERT INTO generation_artifacts (
                generation_id,
                version_id,
                file_name,
                file_path,
                file_type,
                selected_sheet,
                parsed_file_json,
                mappings_json,
                preview_json,
                warnings_json,
                legacy_history_id
            )
            VALUES (
                :generation_id,
                :version_id,
                :file_name,
                :file_path,
                :file_type,
                :selected_sheet,
                :parsed_file_json,
                :mappings_json,
                :preview_json,
                :warnings_json,
                NULL
            )
            ''',
            {
                'generation_id': generation_id,
                'version_id': version_id,
                'file_name': file_name,
                'file_path': file_path,
                'file_type': file_type,
                'selected_sheet': selected_sheet,
                'parsed_file_json': safe_parsed_file_json,
                'mappings_json': safe_mappings_json,
                'preview_json': safe_preview_json,
                'warnings_json': safe_warnings_json,
            },
        )
        artifact_id = int(artifact_cursor.lastrowid)

        resolved_schema_fingerprint_id = schema_fingerprint_id or ensure_schema_fingerprint(
            user_id=user_id,
            internal_user_id=internal_user_id,
            parsed_file_json=safe_parsed_file_json,
            target_json=safe_target_json,
            selected_sheet=selected_sheet,
            source_columns=source_columns,
            generation_id=generation_id,
            artifact_id=artifact_id,
        )

        _upsert_mapping_cache_entries(
            db=db,
            user_id=internal_user_id,
            generation_id=generation_id,
            mappings_json=safe_mappings_json,
        )

        _link_uploaded_file_to_generation(
            db=db,
            upload_record_id=upload_record_id,
            internal_user_id=internal_user_id,
            generation_id=generation_id,
            artifact_id=artifact_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            file_type=file_type,
        )

        _upsert_mapping_memory_entries(
            db=db,
            user_id=internal_user_id,
            generation_id=generation_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            mappings_json=safe_mappings_json,
            source_of_truth='accepted_generation',
        )

        quality_score = _estimate_generation_quality(safe_mappings_json, safe_warnings_json)
        few_shot_example_id = _save_few_shot_example(
            db=db,
            user_id=internal_user_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            generation_id=generation_id,
            version_id=version_id,
            file_name=file_name,
            file_type=file_type,
            parsed_file_json=safe_parsed_file_json,
            target_json=safe_target_json,
            mappings_json=safe_mappings_json,
            warnings_json=safe_warnings_json,
            generated_typescript=generated_typescript,
            selected_sheet=selected_sheet,
            source_columns=source_columns,
            quality_score=quality_score,
        )

        _upsert_frequent_djson(
            db=db,
            user_id=internal_user_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            target_json=safe_target_json,
            default_name=title,
        )

        if few_shot_example_id is not None:
            _create_curated_dataset_candidate(
                db=db,
                source_entity_type='few_shot_example',
                source_entity_id=few_shot_example_id,
                item_kind='example',
                input_payload={
                    'parsed_file': _ensure_json_value(safe_parsed_file_json, _build_fallback_parsed_file(file_name, file_type)),
                    'mappings': _ensure_json_value(safe_mappings_json, []),
                    'warnings': _ensure_json_value(safe_warnings_json, []),
                    'selected_sheet': selected_sheet,
                },
                target_payload={
                    'target_json': _ensure_json_value(safe_target_json, {}),
                    'generated_typescript': generated_typescript,
                },
                quality_score=quality_score,
            )

    return generation_id


def get_history(user_id: str, limit: int | None = None) -> list[dict[str, Any]]:
    db = get_db()
    sql = '''
        SELECT
            g.id,
            u.external_id AS user_id,
            a.file_name,
            a.file_type,
            v.target_json,
            a.mappings_json,
            v.generated_typescript,
            a.preview_json,
            a.warnings_json,
            g.created_at
        FROM generations g
        INNER JOIN users u
            ON u.id = g.user_id
        LEFT JOIN generation_versions v
            ON v.id = g.current_version_id
        LEFT JOIN generation_artifacts a
            ON a.version_id = v.id
        WHERE u.external_id = :external_id
        ORDER BY g.updated_at DESC, g.id DESC
    '''
    params: dict[str, Any] = {'external_id': user_id}
    if limit is not None:
        sql += '\nLIMIT :limit'
        params['limit'] = limit
    return [dict(row) for row in db.all(sql, params)]


def get_generation_by_id(entry_id: int) -> dict[str, Any] | None:
    db = get_db()
    row = db.get(
        '''
        SELECT
            g.id,
            u.external_id AS user_id,
            a.file_name,
            a.file_type,
            v.target_json,
            a.mappings_json,
            v.generated_typescript,
            a.preview_json,
            a.warnings_json,
            g.created_at
        FROM generations g
        INNER JOIN users u
            ON u.id = g.user_id
        LEFT JOIN generation_versions v
            ON v.id = g.current_version_id
        LEFT JOIN generation_artifacts a
            ON a.version_id = v.id
        WHERE g.id = :entry_id
        ''',
        {'entry_id': entry_id},
    )
    return dict(row) if row else None


def get_learning_summary(user_id: str) -> dict[str, Any]:
    internal_user_id = _lookup_internal_user_id(user_id)
    if internal_user_id is None:
        return {
            'user_id': user_id,
            'uploads': 0,
            'schema_fingerprints': 0,
            'mapping_memory': 0,
            'few_shot_examples': 0,
            'user_templates': 0,
            'correction_sessions': 0,
            'user_corrections': 0,
            'frequent_djson': 0,
            'global_pattern_candidates': 0,
            'global_curated_dataset_items': 0,
        }

    db = get_db()
    counts = {
        'uploads': _count_rows(db, 'SELECT COUNT(*) AS value FROM uploaded_files WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'schema_fingerprints': _count_rows(db, 'SELECT COUNT(*) AS value FROM schema_fingerprints WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'mapping_memory': _count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_memory WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'few_shot_examples': _count_rows(db, 'SELECT COUNT(*) AS value FROM few_shot_examples WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'user_templates': _count_rows(db, 'SELECT COUNT(*) AS value FROM user_templates WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'correction_sessions': _count_rows(db, 'SELECT COUNT(*) AS value FROM correction_sessions WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'user_corrections': _count_rows(db, 'SELECT COUNT(*) AS value FROM user_corrections WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'frequent_djson': _count_rows(db, 'SELECT COUNT(*) AS value FROM frequent_djson WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'global_pattern_candidates': _count_rows(db, 'SELECT COUNT(*) AS value FROM pattern_candidates'),
        'global_curated_dataset_items': _count_rows(db, 'SELECT COUNT(*) AS value FROM curated_dataset_items'),
    }
    counts['user_id'] = user_id
    return counts


def list_user_templates(user_id: str) -> list[dict[str, Any]]:
    internal_user_id = _lookup_internal_user_id(user_id)
    if internal_user_id is None:
        return []

    db = get_db()
    rows = db.all(
        '''
        SELECT
            id,
            name,
            template_kind,
            description,
            template_json,
            target_json,
            generated_typescript,
            prompt_suffix,
            usage_count,
            success_count,
            last_used_at,
            is_active,
            is_shared,
            metadata_json,
            created_at,
            updated_at
        FROM user_templates
        WHERE user_id = :user_id
        ORDER BY updated_at DESC, id DESC
        ''',
        {'user_id': internal_user_id},
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(
            {
                'id': int(row['id']),
                'name': str(row['name']),
                'template_kind': str(row['template_kind']),
                'description': row['description'],
                'template_json': json.loads(row['template_json']),
                'target_json': json.loads(row['target_json']) if row['target_json'] else None,
                'generated_typescript': row['generated_typescript'],
                'prompt_suffix': row['prompt_suffix'],
                'usage_count': int(row['usage_count']),
                'success_count': int(row['success_count']),
                'last_used_at': row['last_used_at'],
                'is_active': bool(row['is_active']),
                'is_shared': bool(row['is_shared']),
                'metadata': json.loads(row['metadata_json']) if row['metadata_json'] else {},
                'created_at': row['created_at'],
                'updated_at': row['updated_at'],
            }
        )
    return items


def save_user_template(
    *,
    user_id: str,
    name: str,
    template_kind: str,
    template_json: Any,
    description: str | None = None,
    target_json: Any | None = None,
    generated_typescript: str | None = None,
    prompt_suffix: str | None = None,
    schema_fingerprint_id: int | None = None,
    is_shared: bool = False,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    normalized_name = _normalize_field_name(name) or f'template{int(time.time())}'
    template_payload = _ensure_json_text(template_json, {})
    target_payload = _ensure_json_text(target_json, {}) if target_json is not None else None
    metadata_payload = _json_or_none(metadata)
    existing = db.get(
        '''
        SELECT id
        FROM user_templates
        WHERE user_id = :user_id AND name_normalized = :name_normalized
        ''',
        {'user_id': internal_user_id, 'name_normalized': normalized_name},
    )

    now = _timestamp()
    if existing is None:
        cursor = db.run(
            '''
            INSERT INTO user_templates (
                user_id,
                schema_fingerprint_id,
                name,
                name_normalized,
                template_kind,
                description,
                template_json,
                target_json,
                generated_typescript,
                prompt_suffix,
                is_shared,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES (
                :user_id,
                :schema_fingerprint_id,
                :name,
                :name_normalized,
                :template_kind,
                :description,
                :template_json,
                :target_json,
                :generated_typescript,
                :prompt_suffix,
                :is_shared,
                :metadata_json,
                :created_at,
                :updated_at
            )
            ''',
            {
                'user_id': internal_user_id,
                'schema_fingerprint_id': schema_fingerprint_id,
                'name': name.strip(),
                'name_normalized': normalized_name,
                'template_kind': template_kind,
                'description': description,
                'template_json': template_payload,
                'target_json': target_payload,
                'generated_typescript': generated_typescript,
                'prompt_suffix': prompt_suffix,
                'is_shared': 1 if is_shared else 0,
                'metadata_json': metadata_payload,
                'created_at': now,
                'updated_at': now,
            },
        )
        template_id = int(cursor.lastrowid)
    else:
        template_id = int(existing['id'])
        db.run(
            '''
            UPDATE user_templates
            SET
                schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
                name = :name,
                template_kind = :template_kind,
                description = :description,
                template_json = :template_json,
                target_json = :target_json,
                generated_typescript = :generated_typescript,
                prompt_suffix = :prompt_suffix,
                is_shared = :is_shared,
                metadata_json = :metadata_json,
                updated_at = :updated_at
            WHERE id = :id
            ''',
            {
                'id': template_id,
                'schema_fingerprint_id': schema_fingerprint_id,
                'name': name.strip(),
                'template_kind': template_kind,
                'description': description,
                'template_json': template_payload,
                'target_json': target_payload,
                'generated_typescript': generated_typescript,
                'prompt_suffix': prompt_suffix,
                'is_shared': 1 if is_shared else 0,
                'metadata_json': metadata_payload,
                'updated_at': now,
            },
        )

    return next(item for item in list_user_templates(user_id) if int(item['id']) == template_id)


def save_correction_session(
    *,
    user_id: str,
    corrections: list[dict[str, Any]],
    generation_id: int | None = None,
    session_type: str = 'manual_review',
    schema_fingerprint_id: int | None = None,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    resolved_schema_fingerprint_id = schema_fingerprint_id or _find_schema_fingerprint_for_generation(db, generation_id)
    correction_ids: list[int] = []
    accepted_count = 0

    with db.transaction():
        session_cursor = db.run(
            '''
            INSERT INTO correction_sessions (
                user_id,
                generation_id,
                schema_fingerprint_id,
                session_type,
                status,
                notes,
                metadata_json,
                started_at
            )
            VALUES (
                :user_id,
                :generation_id,
                :schema_fingerprint_id,
                :session_type,
                'open',
                :notes,
                :metadata_json,
                :started_at
            )
            ''',
            {
                'user_id': internal_user_id,
                'generation_id': generation_id,
                'schema_fingerprint_id': resolved_schema_fingerprint_id,
                'session_type': session_type,
                'notes': notes,
                'metadata_json': _json_or_none(metadata),
                'started_at': _timestamp(),
            },
        )
        session_id = int(session_cursor.lastrowid)

        for correction in corrections:
            source_field = correction.get('source_field')
            target_field = correction.get('target_field')
            accepted = bool(correction.get('accepted', True))
            if accepted:
                accepted_count += 1

            correction_cursor = db.run(
                '''
                INSERT INTO user_corrections (
                    session_id,
                    user_id,
                    generation_id,
                    schema_fingerprint_id,
                    correction_type,
                    row_index,
                    field_path,
                    source_field,
                    source_field_normalized,
                    target_field,
                    target_field_normalized,
                    original_value_json,
                    corrected_value_json,
                    correction_payload_json,
                    rationale,
                    confidence_before,
                    confidence_after,
                    accepted,
                    created_at
                )
                VALUES (
                    :session_id,
                    :user_id,
                    :generation_id,
                    :schema_fingerprint_id,
                    :correction_type,
                    :row_index,
                    :field_path,
                    :source_field,
                    :source_field_normalized,
                    :target_field,
                    :target_field_normalized,
                    :original_value_json,
                    :corrected_value_json,
                    :correction_payload_json,
                    :rationale,
                    :confidence_before,
                    :confidence_after,
                    :accepted,
                    :created_at
                )
                ''',
                {
                    'session_id': session_id,
                    'user_id': internal_user_id,
                    'generation_id': generation_id,
                    'schema_fingerprint_id': resolved_schema_fingerprint_id,
                    'correction_type': correction.get('correction_type', 'feedback_note'),
                    'row_index': correction.get('row_index'),
                    'field_path': correction.get('field_path'),
                    'source_field': source_field,
                    'source_field_normalized': _normalize_field_name(str(source_field)) if source_field else None,
                    'target_field': target_field,
                    'target_field_normalized': _normalize_field_name(str(target_field)) if target_field else None,
                    'original_value_json': _json_or_none(correction.get('original_value')),
                    'corrected_value_json': _json_or_none(correction.get('corrected_value')),
                    'correction_payload_json': _json_or_none(correction.get('correction_payload')),
                    'rationale': correction.get('rationale'),
                    'confidence_before': correction.get('confidence_before'),
                    'confidence_after': correction.get('confidence_after'),
                    'accepted': 1 if accepted else 0,
                    'created_at': _timestamp(),
                },
            )
            correction_id = int(correction_cursor.lastrowid)
            correction_ids.append(correction_id)

            if accepted and source_field and target_field:
                _upsert_mapping_memory_entries(
                    db=db,
                    user_id=internal_user_id,
                    generation_id=generation_id,
                    schema_fingerprint_id=resolved_schema_fingerprint_id,
                    mappings_json=json.dumps(
                        [
                            {
                                'source': source_field,
                                'target': target_field,
                                'confidence': _score_to_confidence(correction.get('confidence_after')),
                                'reason': correction.get('correction_type', 'user_correction'),
                            }
                        ],
                        ensure_ascii=False,
                    ),
                    source_of_truth='user_correction',
                    session_id=session_id,
                    correction_id=correction_id,
                )

            if accepted:
                _create_curated_dataset_candidate(
                    db=db,
                    source_entity_type='user_correction',
                    source_entity_id=correction_id,
                    item_kind='correction',
                    input_payload={
                        'generation_id': generation_id,
                        'source_field': source_field,
                        'target_field': target_field,
                        'original_value': correction.get('original_value'),
                        'correction_payload': correction.get('correction_payload'),
                    },
                    target_payload={
                        'corrected_value': correction.get('corrected_value'),
                        'accepted': True,
                    },
                    quality_score=correction.get('confidence_after') or 1.0,
                    review_kind='manual',
                    reviewer_user_id=internal_user_id,
                    review_decision='approved',
                )

        db.run(
            '''
            UPDATE correction_sessions
            SET
                schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
                status = 'applied',
                acceptance_rate = :acceptance_rate,
                closed_at = :closed_at
            WHERE id = :id
            ''',
            {
                'id': session_id,
                'schema_fingerprint_id': resolved_schema_fingerprint_id,
                'acceptance_rate': accepted_count / max(len(corrections), 1),
                'closed_at': _timestamp(),
            },
        )

    return {
        'session_id': session_id,
        'generation_id': generation_id,
        'schema_fingerprint_id': resolved_schema_fingerprint_id,
        'correction_ids': correction_ids,
        'accepted_count': accepted_count,
        'count': len(correction_ids),
    }


def cleanup_expired_guest_files() -> None:
    ensure_dirs()
    now = time.time()
    for item in GUEST_DIR.glob('*'):
        try:
            if now - item.stat().st_mtime > GUEST_TTL_SECONDS:
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    item.unlink(missing_ok=True)
                _mark_uploaded_files_by_storage_prefix(item, status='expired', expires_at=_timestamp())
        except FileNotFoundError:
            continue


def cleanup_guest_files(ttl_hours: int = 24, dry_run: bool = False) -> dict[str, Any]:
    ensure_dirs()
    now = time.time()
    ttl_seconds = ttl_hours * 60 * 60
    removed: list[str] = []

    for item in GUEST_DIR.glob('*'):
        try:
            if now - item.stat().st_mtime <= ttl_seconds:
                continue
            removed.append(str(item))
            if dry_run:
                continue
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
            else:
                item.unlink(missing_ok=True)
            _mark_uploaded_files_by_storage_prefix(item, status='expired', expires_at=_timestamp())
        except FileNotFoundError:
            continue

    return {'dry_run': dry_run, 'ttl_hours': ttl_hours, 'removed': removed, 'count': len(removed)}


def delete_file(path: str) -> None:
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def ensure_user_record(
    external_id: str,
    email: str | None = None,
    display_name: str | None = None,
) -> int:
    db = get_db()
    normalized_external_id = external_id.strip()
    if not normalized_external_id:
        raise UserConflictError('external_id is required.')

    row = db.get(
        '''
        SELECT id, email, display_name
        FROM users
        WHERE external_id = :external_id
        ''',
        {'external_id': normalized_external_id},
    )
    if row is not None:
        needs_update = False
        update_payload = {'id': row['id'], 'email': row['email'], 'display_name': row['display_name']}
        if email and not row['email']:
            update_payload['email'] = email.strip().lower()
            needs_update = True
        if display_name and not row['display_name']:
            update_payload['display_name'] = display_name.strip()
            needs_update = True
        if needs_update:
            db.run(
                '''
                UPDATE users
                SET email = :email, display_name = :display_name
                WHERE id = :id
                ''',
                update_payload,
            )
        return int(row['id'])

    cursor = db.run(
        '''
        INSERT INTO users (email, external_id, display_name, password_hash)
        VALUES (:email, :external_id, :display_name, NULL)
        ''',
        {
            'email': email.strip().lower() if email else None,
            'external_id': normalized_external_id,
            'display_name': display_name.strip() if display_name else None,
        },
    )
    return int(cursor.lastrowid)


def migrate_legacy_history() -> None:
    if not LEGACY_DB_PATH.exists():
        return

    with sqlite3.connect(LEGACY_DB_PATH) as legacy_connection:
        legacy_connection.row_factory = sqlite3.Row
        table_row = legacy_connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'generations'"
        ).fetchone()
        if table_row is None:
            return

        columns = {
            row['name']
            for row in legacy_connection.execute("PRAGMA table_info('generations')").fetchall()
        }
        expected_columns = {
            'id',
            'user_id',
            'file_name',
            'file_path',
            'file_type',
            'target_json',
            'mappings_json',
            'generated_typescript',
            'preview_json',
            'warnings_json',
            'created_at',
        }
        if not expected_columns.issubset(columns):
            return

        rows = legacy_connection.execute(
            '''
            SELECT
                id,
                user_id,
                file_name,
                file_path,
                file_type,
                target_json,
                mappings_json,
                generated_typescript,
                preview_json,
                warnings_json,
                created_at
            FROM generations
            ORDER BY id ASC
            '''
        ).fetchall()

    db = get_db()
    for row in rows:
        if db.get(
            'SELECT 1 FROM generation_artifacts WHERE legacy_history_id = :legacy_history_id',
            {'legacy_history_id': row['id']},
        ):
            continue

        parsed_file_fallback = _build_fallback_parsed_file(str(row['file_name']), str(row['file_type']))
        internal_user_id = ensure_user_record(external_id=str(row['user_id']))
        created_at = str(row['created_at'] or _timestamp())

        with db.transaction():
            generation_cursor = db.run(
                '''
                INSERT INTO generations (
                    user_id,
                    schema_id,
                    title,
                    source_payload,
                    source_payload_format,
                    current_version_id,
                    status,
                    created_at,
                    updated_at
                )
                VALUES (
                    :user_id,
                    NULL,
                    :title,
                    :source_payload,
                    'json',
                    NULL,
                    'completed',
                    :created_at,
                    :updated_at
                )
                ''',
                {
                    'user_id': internal_user_id,
                    'title': str(row['file_name']),
                    'source_payload': _build_source_payload(
                        parsed_file_fallback,
                        str(row['file_name']),
                        str(row['file_type']),
                        None,
                    ),
                    'created_at': created_at,
                    'updated_at': created_at,
                },
            )
            generation_id = int(generation_cursor.lastrowid)

            version_cursor = db.run(
                '''
                INSERT INTO generation_versions (
                    generation_id,
                    parent_version_id,
                    version_number,
                    change_type,
                    note,
                    target_json,
                    generated_typescript,
                    created_at
                )
                VALUES (
                    :generation_id,
                    NULL,
                    1,
                    'initial',
                    'Migrated from legacy history',
                    :target_json,
                    :generated_typescript,
                    :created_at
                )
                ''',
                {
                    'generation_id': generation_id,
                    'target_json': _ensure_json_text(str(row['target_json']), {}),
                    'generated_typescript': str(row['generated_typescript']),
                    'created_at': created_at,
                },
            )
            version_id = int(version_cursor.lastrowid)

            db.run(
                '''
                UPDATE generations
                SET current_version_id = :version_id
                WHERE id = :generation_id
                ''',
                {
                    'version_id': version_id,
                    'generation_id': generation_id,
                },
            )

            db.run(
                '''
                INSERT INTO generation_artifacts (
                    generation_id,
                    version_id,
                    file_name,
                    file_path,
                    file_type,
                    selected_sheet,
                    parsed_file_json,
                    mappings_json,
                    preview_json,
                    warnings_json,
                    legacy_history_id,
                    created_at
                )
                VALUES (
                    :generation_id,
                    :version_id,
                    :file_name,
                    :file_path,
                    :file_type,
                    NULL,
                    :parsed_file_json,
                    :mappings_json,
                    :preview_json,
                    :warnings_json,
                    :legacy_history_id,
                    :created_at
                )
                ''',
                {
                    'generation_id': generation_id,
                    'version_id': version_id,
                    'file_name': str(row['file_name']),
                    'file_path': str(row['file_path']),
                    'file_type': str(row['file_type']),
                    'parsed_file_json': _ensure_json_text(parsed_file_fallback, parsed_file_fallback),
                    'mappings_json': _ensure_json_text(str(row['mappings_json']), []),
                    'preview_json': _ensure_json_text(str(row['preview_json']), []),
                    'warnings_json': _ensure_json_text(str(row['warnings_json']), []),
                    'legacy_history_id': int(row['id']),
                    'created_at': created_at,
                },
            )

            _upsert_mapping_cache_entries(
                db=db,
                user_id=internal_user_id,
                generation_id=generation_id,
                mappings_json=str(row['mappings_json']),
            )


def _upsert_mapping_cache_entries(
    db: DatabaseClient,
    user_id: int,
    generation_id: int,
    mappings_json: str,
) -> None:
    try:
        mappings = json.loads(mappings_json)
    except json.JSONDecodeError:
        return

    if not isinstance(mappings, list):
        return

    timestamp = _timestamp()
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        source = mapping.get('source')
        target = mapping.get('target')
        if not source or not target:
            continue

        normalized_source = _normalize_field_name(str(source))
        normalized_target = _normalize_field_name(str(target))
        confidence = _confidence_to_score(mapping.get('confidence'))
        if not normalized_source or not normalized_target:
            continue

        existing = db.get(
            '''
            SELECT id, usage_count
            FROM mapping_cache
            WHERE user_id = :user_id
              AND schema_scope_key = 0
              AND source_field_normalized = :source_field_normalized
            ''',
            {
                'user_id': user_id,
                'source_field_normalized': normalized_source,
            },
        )

        if existing is None:
            db.run(
                '''
                INSERT INTO mapping_cache (
                    user_id,
                    schema_id,
                    source_field,
                    source_field_normalized,
                    target_field,
                    target_field_normalized,
                    confidence,
                    source_of_truth,
                    usage_count,
                    last_generation_id,
                    updated_at,
                    last_used_at
                )
                VALUES (
                    :user_id,
                    NULL,
                    :source_field,
                    :source_field_normalized,
                    :target_field,
                    :target_field_normalized,
                    :confidence,
                    'system_rule',
                    1,
                    :last_generation_id,
                    :updated_at,
                    :last_used_at
                )
                ''',
                {
                    'user_id': user_id,
                    'source_field': str(source),
                    'source_field_normalized': normalized_source,
                    'target_field': str(target),
                    'target_field_normalized': normalized_target,
                    'confidence': confidence,
                    'last_generation_id': generation_id,
                    'updated_at': timestamp,
                    'last_used_at': timestamp,
                },
            )
            continue

        db.run(
            '''
            UPDATE mapping_cache
            SET
                target_field = :target_field,
                target_field_normalized = :target_field_normalized,
                confidence = :confidence,
                source_of_truth = 'system_rule',
                usage_count = usage_count + 1,
                last_generation_id = :last_generation_id,
                updated_at = :updated_at,
                last_used_at = :last_used_at
            WHERE id = :id
            ''',
            {
                'id': int(existing['id']),
                'target_field': str(target),
                'target_field_normalized': normalized_target,
                'confidence': confidence,
                'last_generation_id': generation_id,
                'updated_at': timestamp,
                'last_used_at': timestamp,
            },
        )


def ensure_schema_fingerprint(
    *,
    parsed_file_json: str | Any,
    target_json: str | Any,
    selected_sheet: str | None = None,
    source_columns: list[str] | None = None,
    user_id: str | None = None,
    internal_user_id: int | None = None,
    generation_id: int | None = None,
    artifact_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> int:
    db = get_db()
    resolved_user_id = _resolve_internal_user_id(user_id=user_id, internal_user_id=internal_user_id)
    parsed_payload = _ensure_json_value(parsed_file_json, _build_fallback_parsed_file('uploaded_file', 'unknown'))
    if not isinstance(parsed_payload, dict):
        parsed_payload = _build_fallback_parsed_file('uploaded_file', 'unknown')

    target_payload = _ensure_json_value(target_json, {})
    if not isinstance(target_payload, dict):
        target_payload = {}

    resolved_source_columns = source_columns or _extract_source_columns(parsed_payload, selected_sheet)
    normalized_schema = _build_normalized_schema(parsed_payload, target_payload, resolved_source_columns, selected_sheet)
    normalized_schema_json = _canonical_json_text(normalized_schema)
    fingerprint_hash = _make_hash(normalized_schema_json)
    vector, sparse_features, normalized_source_text = _build_feature_payload(
        parsed_payload=parsed_payload,
        target_payload=target_payload,
        source_columns=resolved_source_columns,
    )
    metadata_json = _json_or_none(
        _merge_json_objects(
            {
                'file_name': parsed_payload.get('file_name'),
                'file_type': parsed_payload.get('file_type'),
                'warning_count': len(parsed_payload.get('warnings', []) or []),
            },
            metadata,
        )
    )
    existing = db.get(
        '''
        SELECT id
        FROM schema_fingerprints
        WHERE user_scope_key = :user_scope_key
          AND fingerprint_version = 'v1'
          AND fingerprint_hash = :fingerprint_hash
        ''',
        {
            'user_scope_key': resolved_user_id or 0,
            'fingerprint_hash': fingerprint_hash,
        },
    )
    now = _timestamp()
    column_signature = '|'.join(_normalize_field_name(column) for column in resolved_source_columns if column)

    if existing is None:
        cursor = db.run(
            '''
            INSERT INTO schema_fingerprints (
                user_id,
                source_generation_id,
                source_artifact_id,
                source_kind,
                fingerprint_version,
                fingerprint_hash,
                column_signature,
                normalized_schema_json,
                normalized_source_text,
                feature_vector_json,
                embedding_provider,
                embedding_model,
                metadata_json,
                created_at,
                updated_at
            )
            VALUES (
                :user_id,
                :source_generation_id,
                :source_artifact_id,
                :source_kind,
                'v1',
                :fingerprint_hash,
                :column_signature,
                :normalized_schema_json,
                :normalized_source_text,
                :feature_vector_json,
                :embedding_provider,
                :embedding_model,
                :metadata_json,
                :created_at,
                :updated_at
            )
            ''',
            {
                'user_id': resolved_user_id,
                'source_generation_id': generation_id,
                'source_artifact_id': artifact_id,
                'source_kind': _source_kind_for_file_type(str(parsed_payload.get('file_type', 'unknown'))),
                'fingerprint_hash': fingerprint_hash,
                'column_signature': column_signature or None,
                'normalized_schema_json': normalized_schema_json,
                'normalized_source_text': normalized_source_text,
                'feature_vector_json': _ensure_json_text(vector, []),
                'embedding_provider': LEARNING_VECTOR_PROVIDER,
                'embedding_model': LEARNING_VECTOR_MODEL,
                'metadata_json': metadata_json,
                'created_at': now,
                'updated_at': now,
            },
        )
        fingerprint_id = int(cursor.lastrowid)
    else:
        fingerprint_id = int(existing['id'])
        db.run(
            '''
            UPDATE schema_fingerprints
            SET
                source_generation_id = coalesce(:source_generation_id, source_generation_id),
                source_artifact_id = coalesce(:source_artifact_id, source_artifact_id),
                source_kind = :source_kind,
                column_signature = :column_signature,
                normalized_schema_json = :normalized_schema_json,
                normalized_source_text = :normalized_source_text,
                feature_vector_json = :feature_vector_json,
                embedding_provider = :embedding_provider,
                embedding_model = :embedding_model,
                metadata_json = :metadata_json,
                updated_at = :updated_at
            WHERE id = :id
            ''',
            {
                'id': fingerprint_id,
                'source_generation_id': generation_id,
                'source_artifact_id': artifact_id,
                'source_kind': _source_kind_for_file_type(str(parsed_payload.get('file_type', 'unknown'))),
                'column_signature': column_signature or None,
                'normalized_schema_json': normalized_schema_json,
                'normalized_source_text': normalized_source_text,
                'feature_vector_json': _ensure_json_text(vector, []),
                'embedding_provider': LEARNING_VECTOR_PROVIDER,
                'embedding_model': LEARNING_VECTOR_MODEL,
                'metadata_json': metadata_json,
                'updated_at': now,
            },
        )

    _upsert_feature_vector(
        db=db,
        entity_type='schema_fingerprint',
        entity_id=fingerprint_id,
        schema_fingerprint_id=fingerprint_id,
        vector=vector,
        sparse_features=sparse_features,
        text_payload=normalized_source_text,
        metadata={'kind': 'schema_fingerprint'},
    )
    return fingerprint_id


def _link_uploaded_file_to_generation(
    *,
    db: DatabaseClient,
    upload_record_id: int | None,
    internal_user_id: int,
    generation_id: int,
    artifact_id: int,
    schema_fingerprint_id: int | None,
    file_type: str,
) -> None:
    if upload_record_id is None:
        return

    now = _timestamp()
    db.run(
        '''
        UPDATE uploaded_files
        SET
            user_id = coalesce(:user_id, user_id),
            generation_id = :generation_id,
            artifact_id = :artifact_id,
            schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
            file_type = :file_type,
            status = 'processed',
            last_accessed_at = :last_accessed_at
        WHERE id = :id
        ''',
        {
            'id': upload_record_id,
            'user_id': internal_user_id,
            'generation_id': generation_id,
            'artifact_id': artifact_id,
            'schema_fingerprint_id': schema_fingerprint_id,
            'file_type': file_type,
            'last_accessed_at': now,
        },
    )

    if schema_fingerprint_id is not None:
        db.run(
            '''
            UPDATE schema_fingerprints
            SET
                source_generation_id = coalesce(source_generation_id, :generation_id),
                source_artifact_id = coalesce(source_artifact_id, :artifact_id),
                updated_at = :updated_at
            WHERE id = :id
            ''',
            {
                'id': schema_fingerprint_id,
                'generation_id': generation_id,
                'artifact_id': artifact_id,
                'updated_at': now,
            },
        )


def _mark_uploaded_files_by_storage_prefix(path: Path, status: str, expires_at: str | None = None) -> None:
    db = get_db()
    prefix = str(path)
    db.run(
        '''
        UPDATE uploaded_files
        SET
            status = :status,
            expires_at = :expires_at,
            last_accessed_at = :last_accessed_at
        WHERE storage_path = :exact_path
           OR storage_path LIKE :prefix_like
        ''',
        {
            'status': status,
            'expires_at': expires_at,
            'last_accessed_at': _timestamp(),
            'exact_path': prefix,
            'prefix_like': f'{prefix}%',
        },
    )


def _find_schema_fingerprint_for_generation(db: DatabaseClient, generation_id: int | None) -> int | None:
    if generation_id is None:
        return None

    row = db.get(
        '''
        SELECT id
        FROM schema_fingerprints
        WHERE source_generation_id = :generation_id
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        ''',
        {'generation_id': generation_id},
    )
    if row is not None:
        return int(row['id'])

    fallback = db.get(
        '''
        SELECT schema_fingerprint_id AS id
        FROM uploaded_files
        WHERE generation_id = :generation_id
          AND schema_fingerprint_id IS NOT NULL
        ORDER BY id DESC
        LIMIT 1
        ''',
        {'generation_id': generation_id},
    )
    return int(fallback['id']) if fallback is not None else None


def _upsert_mapping_memory_entries(
    *,
    db: DatabaseClient,
    user_id: int,
    generation_id: int | None,
    schema_fingerprint_id: int | None,
    mappings_json: str,
    source_of_truth: str,
    session_id: int | None = None,
    correction_id: int | None = None,
) -> None:
    mappings = _ensure_json_value(mappings_json, [])
    if not isinstance(mappings, list):
        return

    schema_hash = _get_schema_fingerprint_hash(db, schema_fingerprint_id)
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue

        source = mapping.get('source')
        target = mapping.get('target')
        if not source or not target:
            continue

        normalized_source = _normalize_field_name(str(source))
        normalized_target = _normalize_field_name(str(target))
        if not normalized_source or not normalized_target:
            continue

        confidence = _confidence_to_score(mapping.get('confidence'))
        weight_increment = confidence if confidence is not None else 0.2
        now = _timestamp()
        existing = db.get(
            '''
            SELECT id
            FROM mapping_memory
            WHERE user_scope_key = :user_scope_key
              AND fingerprint_scope_key = :fingerprint_scope_key
              AND source_field_normalized = :source_field_normalized
              AND target_field_normalized = :target_field_normalized
            ''',
            {
                'user_scope_key': user_id,
                'fingerprint_scope_key': schema_fingerprint_id or 0,
                'source_field_normalized': normalized_source,
                'target_field_normalized': normalized_target,
            },
        )

        if existing is None:
            db.run(
                '''
                INSERT INTO mapping_memory (
                    user_id,
                    schema_fingerprint_id,
                    session_id,
                    correction_id,
                    last_generation_id,
                    source_field,
                    source_field_normalized,
                    target_field,
                    target_field_normalized,
                    transform_hint,
                    weight,
                    confidence,
                    usage_count,
                    success_count,
                    failure_count,
                    source_of_truth,
                    metadata_json,
                    last_confirmed_at,
                    last_used_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    :user_id,
                    :schema_fingerprint_id,
                    :session_id,
                    :correction_id,
                    :last_generation_id,
                    :source_field,
                    :source_field_normalized,
                    :target_field,
                    :target_field_normalized,
                    :transform_hint,
                    :weight,
                    :confidence,
                    1,
                    1,
                    0,
                    :source_of_truth,
                    :metadata_json,
                    :last_confirmed_at,
                    :last_used_at,
                    :created_at,
                    :updated_at
                )
                ''',
                {
                    'user_id': user_id,
                    'schema_fingerprint_id': schema_fingerprint_id,
                    'session_id': session_id,
                    'correction_id': correction_id,
                    'last_generation_id': generation_id,
                    'source_field': str(source),
                    'source_field_normalized': normalized_source,
                    'target_field': str(target),
                    'target_field_normalized': normalized_target,
                    'transform_hint': mapping.get('reason'),
                    'weight': max(weight_increment, 0.2),
                    'confidence': confidence,
                    'source_of_truth': source_of_truth,
                    'metadata_json': _json_or_none({'reason': mapping.get('reason')}),
                    'last_confirmed_at': now,
                    'last_used_at': now,
                    'created_at': now,
                    'updated_at': now,
                },
            )
        else:
            db.run(
                '''
                UPDATE mapping_memory
                SET
                    session_id = coalesce(:session_id, session_id),
                    correction_id = coalesce(:correction_id, correction_id),
                    last_generation_id = coalesce(:last_generation_id, last_generation_id),
                    source_field = :source_field,
                    target_field = :target_field,
                    transform_hint = coalesce(:transform_hint, transform_hint),
                    weight = weight + :weight_increment,
                    confidence = coalesce(:confidence, confidence),
                    usage_count = usage_count + 1,
                    success_count = success_count + 1,
                    source_of_truth = :source_of_truth,
                    metadata_json = :metadata_json,
                    last_confirmed_at = :last_confirmed_at,
                    last_used_at = :last_used_at,
                    updated_at = :updated_at
                WHERE id = :id
                ''',
                {
                    'id': int(existing['id']),
                    'session_id': session_id,
                    'correction_id': correction_id,
                    'last_generation_id': generation_id,
                    'source_field': str(source),
                    'target_field': str(target),
                    'transform_hint': mapping.get('reason'),
                    'weight_increment': max(weight_increment, 0.2),
                    'confidence': confidence,
                    'source_of_truth': source_of_truth,
                    'metadata_json': _json_or_none({'reason': mapping.get('reason')}),
                    'last_confirmed_at': now,
                    'last_used_at': now,
                    'updated_at': now,
                },
            )

        _upsert_pattern_candidate(
            db=db,
            schema_fingerprint_id=schema_fingerprint_id,
            schema_hash=schema_hash,
            source_field=str(source),
            target_field=str(target),
            source_field_normalized=normalized_source,
            target_field_normalized=normalized_target,
            confidence=confidence,
        )


def _upsert_pattern_candidate(
    *,
    db: DatabaseClient,
    schema_fingerprint_id: int | None,
    schema_hash: str | None,
    source_field: str,
    target_field: str,
    source_field_normalized: str,
    target_field_normalized: str,
    confidence: float | None,
) -> None:
    candidate_key = f'mapping_rule:{schema_hash or "global"}:{source_field_normalized}:{target_field_normalized}'
    existing = db.get(
        '''
        SELECT id
        FROM pattern_candidates
        WHERE candidate_key = :candidate_key
        ''',
        {'candidate_key': candidate_key},
    )
    now = _timestamp()

    if existing is None:
        cursor = db.run(
            '''
            INSERT INTO pattern_candidates (
                candidate_key,
                pattern_type,
                source_field_normalized,
                target_field_normalized,
                schema_hint_hash,
                proposed_rule_json,
                evidence_json,
                status,
                support_count,
                distinct_users_count,
                mean_confidence,
                created_at,
                updated_at
            )
            VALUES (
                :candidate_key,
                'mapping_rule',
                :source_field_normalized,
                :target_field_normalized,
                :schema_hint_hash,
                :proposed_rule_json,
                :evidence_json,
                'new',
                0,
                0,
                :mean_confidence,
                :created_at,
                :updated_at
            )
            ''',
            {
                'candidate_key': candidate_key,
                'source_field_normalized': source_field_normalized,
                'target_field_normalized': target_field_normalized,
                'schema_hint_hash': schema_hash,
                'proposed_rule_json': _ensure_json_text(
                    {
                        'source_field': source_field,
                        'target_field': target_field,
                        'schema_fingerprint_id': schema_fingerprint_id,
                    },
                    {},
                ),
                'evidence_json': _ensure_json_text({'source': source_field, 'target': target_field}, {}),
                'mean_confidence': confidence,
                'created_at': now,
                'updated_at': now,
            },
        )
        candidate_id = int(cursor.lastrowid)
    else:
        candidate_id = int(existing['id'])

    _refresh_pattern_candidate_stats(
        db=db,
        candidate_id=candidate_id,
        schema_fingerprint_id=schema_fingerprint_id,
        source_field_normalized=source_field_normalized,
        target_field_normalized=target_field_normalized,
    )


def _refresh_pattern_candidate_stats(
    *,
    db: DatabaseClient,
    candidate_id: int,
    schema_fingerprint_id: int | None,
    source_field_normalized: str,
    target_field_normalized: str,
) -> None:
    stats = db.get(
        '''
        SELECT
            COUNT(*) AS rows_count,
            COUNT(DISTINCT user_id) AS distinct_users,
            COALESCE(SUM(usage_count), 0) AS usage_total,
            COALESCE(SUM(success_count), 0) AS success_total,
            COALESCE(SUM(failure_count), 0) AS failure_total,
            AVG(confidence) AS average_confidence
        FROM mapping_memory
        WHERE source_field_normalized = :source_field_normalized
          AND target_field_normalized = :target_field_normalized
          AND (:schema_fingerprint_id IS NULL OR schema_fingerprint_id = :schema_fingerprint_id)
        ''',
        {
            'source_field_normalized': source_field_normalized,
            'target_field_normalized': target_field_normalized,
            'schema_fingerprint_id': schema_fingerprint_id,
        },
    )
    if stats is None:
        return

    usage_total = int(stats['usage_total'] or 0)
    success_total = int(stats['success_total'] or 0)
    failure_total = int(stats['failure_total'] or 0)
    distinct_users = int(stats['distinct_users'] or 0)
    total_feedback = success_total + failure_total
    stability_score = (success_total / total_feedback) if total_feedback else None
    drift_score = (failure_total / total_feedback) if total_feedback else None
    now = _timestamp()

    db.run(
        '''
        UPDATE pattern_candidates
        SET
            support_count = :support_count,
            distinct_users_count = :distinct_users_count,
            mean_confidence = :mean_confidence,
            updated_at = :updated_at
        WHERE id = :id
        ''',
        {
            'id': candidate_id,
            'support_count': usage_total,
            'distinct_users_count': distinct_users,
            'mean_confidence': stats['average_confidence'],
            'updated_at': now,
        },
    )

    existing = db.get(
        '''
        SELECT id
        FROM pattern_stats
        WHERE candidate_id = :candidate_id
        ''',
        {'candidate_id': candidate_id},
    )
    payload = {
        'candidate_id': candidate_id,
        'recurrence_count': usage_total,
        'unique_users': distinct_users,
        'accept_count': success_total,
        'reject_count': failure_total,
        'stability_score': stability_score,
        'drift_score': drift_score,
        'last_seen_at': now,
        'stats_json': _ensure_json_text({'rows_count': int(stats['rows_count'] or 0)}, {}),
        'updated_at': now,
    }

    if existing is None:
        db.run(
            '''
            INSERT INTO pattern_stats (
                candidate_id,
                recurrence_count,
                unique_users,
                accept_count,
                reject_count,
                stability_score,
                drift_score,
                first_seen_at,
                last_seen_at,
                stats_json,
                updated_at
            )
            VALUES (
                :candidate_id,
                :recurrence_count,
                :unique_users,
                :accept_count,
                :reject_count,
                :stability_score,
                :drift_score,
                :first_seen_at,
                :last_seen_at,
                :stats_json,
                :updated_at
            )
            ''',
            {
                **payload,
                'first_seen_at': now,
            },
        )
        return

    db.run(
        '''
        UPDATE pattern_stats
        SET
            recurrence_count = :recurrence_count,
            unique_users = :unique_users,
            accept_count = :accept_count,
            reject_count = :reject_count,
            stability_score = :stability_score,
            drift_score = :drift_score,
            last_seen_at = :last_seen_at,
            stats_json = :stats_json,
            updated_at = :updated_at
        WHERE candidate_id = :candidate_id
        ''',
        payload,
    )


def _save_few_shot_example(
    *,
    db: DatabaseClient,
    user_id: int,
    schema_fingerprint_id: int | None,
    generation_id: int,
    version_id: int,
    file_name: str,
    file_type: str,
    parsed_file_json: str,
    target_json: str,
    mappings_json: str,
    warnings_json: str,
    generated_typescript: str,
    selected_sheet: str | None,
    source_columns: list[str] | None,
    quality_score: float,
) -> int:
    parsed_payload = _ensure_json_value(parsed_file_json, _build_fallback_parsed_file(file_name, file_type))
    if not isinstance(parsed_payload, dict):
        parsed_payload = _build_fallback_parsed_file(file_name, file_type)
    target_payload = _ensure_json_value(target_json, {})
    if not isinstance(target_payload, dict):
        target_payload = {}
    mappings_payload = _ensure_json_value(mappings_json, [])
    warnings_payload = _ensure_json_value(warnings_json, [])
    resolved_source_columns = source_columns or _extract_source_columns(parsed_payload, selected_sheet)

    input_summary = {
        'file_name': file_name,
        'file_type': file_type,
        'selected_sheet': selected_sheet,
        'source_columns': resolved_source_columns,
        'target_fields': list(target_payload.keys()),
        'warning_count': len(warnings_payload) if isinstance(warnings_payload, list) else 0,
        'quality_score': quality_score,
    }
    now = _timestamp()
    cursor = db.run(
        '''
        INSERT INTO few_shot_examples (
            user_id,
            schema_fingerprint_id,
            source_generation_id,
            source_version_id,
            title,
            example_kind,
            input_summary_json,
            target_json,
            mapping_json,
            output_typescript,
            quality_score,
            success_count,
            is_active,
            metadata_json,
            created_at,
            updated_at
        )
        VALUES (
            :user_id,
            :schema_fingerprint_id,
            :source_generation_id,
            :source_version_id,
            :title,
            'generation',
            :input_summary_json,
            :target_json,
            :mapping_json,
            :output_typescript,
            :quality_score,
            1,
            1,
            :metadata_json,
            :created_at,
            :updated_at
        )
        ''',
        {
            'user_id': user_id,
            'schema_fingerprint_id': schema_fingerprint_id,
            'source_generation_id': generation_id,
            'source_version_id': version_id,
            'title': file_name,
            'input_summary_json': _ensure_json_text(input_summary, {}),
            'target_json': _ensure_json_text(target_payload, {}),
            'mapping_json': _ensure_json_text(mappings_payload, []),
            'output_typescript': generated_typescript,
            'quality_score': quality_score,
            'metadata_json': _ensure_json_text({'selected_sheet': selected_sheet}, {}),
            'created_at': now,
            'updated_at': now,
        },
    )
    example_id = int(cursor.lastrowid)

    vector, sparse_features, text_payload = _build_feature_payload(
        parsed_payload=parsed_payload,
        target_payload=target_payload,
        source_columns=resolved_source_columns,
        mappings=mappings_payload if isinstance(mappings_payload, list) else [],
        warnings=warnings_payload if isinstance(warnings_payload, list) else [],
    )
    _upsert_feature_vector(
        db=db,
        entity_type='few_shot_example',
        entity_id=example_id,
        schema_fingerprint_id=schema_fingerprint_id,
        vector=vector,
        sparse_features=sparse_features,
        text_payload=text_payload,
        metadata={'kind': 'few_shot_example', 'quality_score': quality_score},
    )
    return example_id


def _upsert_frequent_djson(
    *,
    db: DatabaseClient,
    user_id: int,
    schema_fingerprint_id: int | None,
    target_json: str,
    default_name: str,
) -> None:
    target_payload = _ensure_json_value(target_json, {})
    if not isinstance(target_payload, dict):
        target_payload = {}

    normalized_payload = _canonical_json_text(target_payload)
    djson_hash = _make_hash(normalized_payload)
    existing = db.get(
        '''
        SELECT id
        FROM frequent_djson
        WHERE user_scope_key = :user_scope_key
          AND djson_hash = :djson_hash
        ''',
        {'user_scope_key': user_id, 'djson_hash': djson_hash},
    )
    name = default_name.strip() or 'target_schema'
    name_normalized = _normalize_field_name(name)
    now = _timestamp()

    if existing is None:
        cursor = db.run(
            '''
            INSERT INTO frequent_djson (
                user_id,
                schema_fingerprint_id,
                name,
                name_normalized,
                djson_hash,
                djson_payload,
                usage_count,
                success_count,
                is_shared,
                metadata_json,
                last_used_at,
                created_at,
                updated_at
            )
            VALUES (
                :user_id,
                :schema_fingerprint_id,
                :name,
                :name_normalized,
                :djson_hash,
                :djson_payload,
                1,
                1,
                0,
                :metadata_json,
                :last_used_at,
                :created_at,
                :updated_at
            )
            ''',
            {
                'user_id': user_id,
                'schema_fingerprint_id': schema_fingerprint_id,
                'name': name,
                'name_normalized': name_normalized or None,
                'djson_hash': djson_hash,
                'djson_payload': normalized_payload,
                'metadata_json': _ensure_json_text({'field_count': len(target_payload)}, {}),
                'last_used_at': now,
                'created_at': now,
                'updated_at': now,
            },
        )
        djson_id = int(cursor.lastrowid)
    else:
        djson_id = int(existing['id'])
        db.run(
            '''
            UPDATE frequent_djson
            SET
                schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
                name = :name,
                name_normalized = :name_normalized,
                djson_payload = :djson_payload,
                usage_count = usage_count + 1,
                success_count = success_count + 1,
                metadata_json = :metadata_json,
                last_used_at = :last_used_at,
                updated_at = :updated_at
            WHERE id = :id
            ''',
            {
                'id': djson_id,
                'schema_fingerprint_id': schema_fingerprint_id,
                'name': name,
                'name_normalized': name_normalized or None,
                'djson_payload': normalized_payload,
                'metadata_json': _ensure_json_text({'field_count': len(target_payload)}, {}),
                'last_used_at': now,
                'updated_at': now,
            },
        )

    vector, sparse_features, text_payload = _build_feature_payload(
        parsed_payload={'file_type': 'schema', 'rows': [], 'sheets': [], 'warnings': []},
        target_payload=target_payload,
        source_columns=list(target_payload.keys()),
    )
    _upsert_feature_vector(
        db=db,
        entity_type='frequent_djson',
        entity_id=djson_id,
        schema_fingerprint_id=schema_fingerprint_id,
        vector=vector,
        sparse_features=sparse_features,
        text_payload=text_payload,
        metadata={'kind': 'frequent_djson'},
    )


def _create_curated_dataset_candidate(
    *,
    db: DatabaseClient,
    source_entity_type: str,
    source_entity_id: int,
    item_kind: str,
    input_payload: Any,
    target_payload: Any,
    quality_score: float | None,
    review_kind: str = 'automatic',
    reviewer_user_id: int | None = None,
    review_decision: str | None = None,
) -> int | None:
    if quality_score is None or quality_score < AUTO_DATASET_CANDIDATE_THRESHOLD:
        return None

    status = 'approved' if quality_score >= AUTO_DATASET_APPROVE_THRESHOLD else 'candidate'
    split = 'train' if quality_score >= 0.85 else 'validation'
    now = _timestamp()
    cursor = db.run(
        '''
        INSERT INTO curated_dataset_items (
            source_entity_type,
            source_entity_id,
            item_kind,
            input_payload_json,
            target_payload_json,
            context_json,
            quality_score,
            selection_reason,
            status,
            split,
            created_at,
            updated_at
        )
        VALUES (
            :source_entity_type,
            :source_entity_id,
            :item_kind,
            :input_payload_json,
            :target_payload_json,
            :context_json,
            :quality_score,
            :selection_reason,
            :status,
            :split,
            :created_at,
            :updated_at
        )
        ''',
        {
            'source_entity_type': source_entity_type,
            'source_entity_id': source_entity_id,
            'item_kind': item_kind,
            'input_payload_json': _ensure_json_text(input_payload, {}),
            'target_payload_json': _ensure_json_text(target_payload, {}),
            'context_json': _ensure_json_text({'quality_score': quality_score}, {}),
            'quality_score': quality_score,
            'selection_reason': 'auto_promoted_from_runtime',
            'status': status,
            'split': split,
            'created_at': now,
            'updated_at': now,
        },
    )
    item_id = int(cursor.lastrowid)

    db.run(
        '''
        INSERT INTO dataset_reviews (
            dataset_item_id,
            reviewer_user_id,
            review_kind,
            decision,
            score,
            metrics_json,
            created_at
        )
        VALUES (
            :dataset_item_id,
            :reviewer_user_id,
            :review_kind,
            :decision,
            :score,
            :metrics_json,
            :created_at
        )
        ''',
        {
            'dataset_item_id': item_id,
            'reviewer_user_id': reviewer_user_id,
            'review_kind': review_kind,
            'decision': review_decision or ('approved' if status == 'approved' else 'needs_work'),
            'score': quality_score,
            'metrics_json': _ensure_json_text({'quality_score': quality_score}, {}),
            'created_at': now,
        },
    )
    return item_id


def _upsert_feature_vector(
    *,
    db: DatabaseClient,
    entity_type: str,
    entity_id: int,
    vector: list[float],
    sparse_features: dict[str, float] | None,
    text_payload: str,
    metadata: dict[str, Any] | None = None,
    schema_fingerprint_id: int | None = None,
) -> None:
    existing = db.get(
        '''
        SELECT id
        FROM feature_vectors
        WHERE entity_type = :entity_type
          AND entity_id = :entity_id
          AND provider = :provider
          AND model_name_key = :model_name_key
          AND vector_kind = 'hybrid'
        ''',
        {
            'entity_type': entity_type,
            'entity_id': entity_id,
            'provider': LEARNING_VECTOR_PROVIDER,
            'model_name_key': LEARNING_VECTOR_MODEL,
        },
    )
    now = _timestamp()
    payload = {
        'schema_fingerprint_id': schema_fingerprint_id,
        'dimensions': len(vector),
        'vector_json': _ensure_json_text(vector, []),
        'sparse_features_json': _json_or_none(sparse_features),
        'feature_norm': sum(value * value for value in vector) ** 0.5,
        'text_payload': text_payload,
        'metadata_json': _json_or_none(metadata),
    }

    if existing is None:
        db.run(
            '''
            INSERT INTO feature_vectors (
                entity_type,
                entity_id,
                schema_fingerprint_id,
                vector_kind,
                provider,
                model_name,
                dimensions,
                vector_json,
                sparse_features_json,
                feature_norm,
                text_payload,
                metadata_json,
                created_at
            )
            VALUES (
                :entity_type,
                :entity_id,
                :schema_fingerprint_id,
                'hybrid',
                :provider,
                :model_name,
                :dimensions,
                :vector_json,
                :sparse_features_json,
                :feature_norm,
                :text_payload,
                :metadata_json,
                :created_at
            )
            ''',
            {
                'entity_type': entity_type,
                'entity_id': entity_id,
                'schema_fingerprint_id': payload['schema_fingerprint_id'],
                'provider': LEARNING_VECTOR_PROVIDER,
                'model_name': LEARNING_VECTOR_MODEL,
                'dimensions': payload['dimensions'],
                'vector_json': payload['vector_json'],
                'sparse_features_json': payload['sparse_features_json'],
                'feature_norm': payload['feature_norm'],
                'text_payload': payload['text_payload'],
                'metadata_json': payload['metadata_json'],
                'created_at': now,
            },
        )
        return

    db.run(
        '''
        UPDATE feature_vectors
        SET
            schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
            dimensions = :dimensions,
            vector_json = :vector_json,
            sparse_features_json = :sparse_features_json,
            feature_norm = :feature_norm,
            text_payload = :text_payload,
            metadata_json = :metadata_json
        WHERE id = :id
        ''',
        {
            'id': int(existing['id']),
            **payload,
        },
    )


def _build_feature_payload(
    *,
    parsed_payload: dict[str, Any],
    target_payload: dict[str, Any],
    source_columns: list[str],
    mappings: list[dict[str, Any]] | None = None,
    warnings: list[str] | None = None,
) -> tuple[list[float], dict[str, float], str]:
    mappings = mappings or []
    warning_list = warnings if warnings is not None else list(parsed_payload.get('warnings', []) or [])
    file_type = str(parsed_payload.get('file_type', 'unknown'))

    normalized_sources = [_normalize_field_name(column) for column in source_columns if _normalize_field_name(column)]
    normalized_targets = [_normalize_field_name(str(key)) for key in target_payload.keys() if _normalize_field_name(str(key))]
    overlap_ratio = len(set(normalized_sources) & set(normalized_targets)) / max(len(set(normalized_sources) | set(normalized_targets)), 1)
    mapping_count = max(len(mappings), 1)

    high = sum(1 for mapping in mappings if mapping.get('confidence') == 'high')
    medium = sum(1 for mapping in mappings if mapping.get('confidence') == 'medium')
    low = sum(1 for mapping in mappings if mapping.get('confidence') == 'low')
    none_count = sum(1 for mapping in mappings if mapping.get('confidence') == 'none')

    vector = [
        round(min(len(source_columns), 64) / 64, 6),
        round(min(len(target_payload), 64) / 64, 6),
        round(min(len(parsed_payload.get('rows', []) or []), 10) / 10, 6),
        round(min(len(parsed_payload.get('sheets', []) or []), 10) / 10, 6),
        round(overlap_ratio, 6),
        round(high / mapping_count, 6),
        round(medium / mapping_count, 6),
        round(low / mapping_count, 6),
        round(none_count / mapping_count, 6),
        round(min(len(warning_list), 10) / 10, 6),
        1.0 if file_type == 'csv' else 0.0,
        1.0 if file_type in {'xlsx', 'xls'} else 0.0,
        1.0 if file_type in {'pdf', 'docx'} else 0.0,
    ]

    sparse: dict[str, float] = {}
    for token in normalized_sources[:FEATURE_TOKEN_LIMIT]:
        sparse[f'source:{token}'] = sparse.get(f'source:{token}', 0.0) + 1.0
    for token in normalized_targets[:FEATURE_TOKEN_LIMIT]:
        sparse[f'target:{token}'] = sparse.get(f'target:{token}', 0.0) + 1.0
    sparse[f'file_type:{file_type}'] = 1.0

    text_payload = (
        f'file_type={file_type}; '
        f'source={" ".join(normalized_sources[:FEATURE_TOKEN_LIMIT])}; '
        f'target={" ".join(normalized_targets[:FEATURE_TOKEN_LIMIT])}'
    )
    return vector, sparse, text_payload


def _build_normalized_schema(
    parsed_payload: dict[str, Any],
    target_payload: dict[str, Any],
    source_columns: list[str],
    selected_sheet: str | None,
) -> dict[str, Any]:
    return {
        'file_type': parsed_payload.get('file_type'),
        'selected_sheet': selected_sheet,
        'source_columns': [str(column) for column in source_columns],
        'source_columns_normalized': [_normalize_field_name(str(column)) for column in source_columns],
        'target_fields': [
            {
                'name': str(key),
                'normalized': _normalize_field_name(str(key)),
                'type': _json_type(value),
            }
            for key, value in sorted(target_payload.items())
        ],
        'sheet_count': len(parsed_payload.get('sheets', []) or []),
        'warning_count': len(parsed_payload.get('warnings', []) or []),
    }


def _extract_source_columns(parsed_payload: dict[str, Any], selected_sheet: str | None) -> list[str]:
    sheets = parsed_payload.get('sheets', []) or []
    if selected_sheet:
        for sheet in sheets:
            if isinstance(sheet, dict) and sheet.get('name') == selected_sheet:
                return [str(column) for column in sheet.get('columns', []) or []]
    return [str(column) for column in parsed_payload.get('columns', []) or []]


def _source_kind_for_file_type(file_type: str) -> str:
    if file_type in {'csv', 'xlsx', 'xls'}:
        return 'tabular'
    if file_type in {'pdf', 'docx'}:
        return 'document'
    return 'mixed'


def _estimate_generation_quality(mappings_json: str, warnings_json: str) -> float:
    mappings = _ensure_json_value(mappings_json, [])
    warnings = _ensure_json_value(warnings_json, [])
    if not isinstance(mappings, list):
        mappings = []
    if not isinstance(warnings, list):
        warnings = []

    confidence_map = {'high': 1.0, 'medium': 0.7, 'low': 0.4, 'none': 0.0}
    confidence_scores = [
        confidence_map.get(str(mapping.get('confidence')), 0.0)
        for mapping in mappings
        if isinstance(mapping, dict)
    ]
    base_score = sum(confidence_scores) / max(len(confidence_scores), 1)
    warning_penalty = min(len(warnings) * 0.08, 0.35)
    return max(0.0, min(1.0, base_score - warning_penalty))


def _lookup_internal_user_id(external_id: str | None) -> int | None:
    if not external_id:
        return None

    row = get_db().get(
        '''
        SELECT id
        FROM users
        WHERE external_id = :external_id
        ''',
        {'external_id': external_id.strip()},
    )
    return int(row['id']) if row is not None else None


def _resolve_internal_user_id(user_id: str | None = None, internal_user_id: int | None = None) -> int | None:
    if internal_user_id is not None:
        return internal_user_id
    if user_id:
        return ensure_user_record(external_id=user_id)
    return None


def _count_rows(db: DatabaseClient, sql: str, params: dict[str, Any] | None = None) -> int:
    row = db.get(sql, params or {})
    return int(row['value']) if row is not None else 0


def _merge_json_objects(*values: dict[str, Any] | None) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for value in values:
        if not value:
            continue
        merged.update(value)
    return merged


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _canonical_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def _make_hash(value: str) -> str:
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


def _get_schema_fingerprint_hash(db: DatabaseClient, schema_fingerprint_id: int | None) -> str | None:
    if schema_fingerprint_id is None:
        return None
    row = db.get(
        '''
        SELECT fingerprint_hash
        FROM schema_fingerprints
        WHERE id = :id
        ''',
        {'id': schema_fingerprint_id},
    )
    return str(row['fingerprint_hash']) if row is not None and row['fingerprint_hash'] else None


def _json_type(value: Any) -> str:
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return 'number'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, list):
        return 'array'
    if isinstance(value, dict):
        return 'object'
    if value is None:
        return 'null'
    return 'any'


def _score_to_confidence(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return 'medium'
    if numeric >= 0.9:
        return 'high'
    if numeric >= 0.6:
        return 'medium'
    if numeric > 0:
        return 'low'
    return 'none'


def _build_source_payload(
    parsed_file_json: str | Any,
    file_name: str,
    file_type: str,
    selected_sheet: str | None,
) -> str:
    parsed_payload = _ensure_json_value(parsed_file_json, _build_fallback_parsed_file(file_name, file_type))
    if isinstance(parsed_payload, str):
        return parsed_payload

    wrapped_payload = {
        'file_name': file_name,
        'file_type': file_type,
        'selected_sheet': selected_sheet,
        'parsed_file': parsed_payload,
    }
    return json.dumps(wrapped_payload, ensure_ascii=False)


def _build_fallback_parsed_file(file_name: str, file_type: str) -> dict[str, Any]:
    return {
        'file_name': file_name,
        'file_type': file_type,
        'columns': [],
        'rows': [],
        'sheets': [],
        'warnings': [],
    }


def _ensure_json_text(value: str | Any, fallback: Any) -> str:
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except json.JSONDecodeError:
            return json.dumps(fallback, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _ensure_json_value(value: str | Any, fallback: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return value


def _normalize_field_name(value: str) -> str:
    return ''.join(part for part in FIELD_NORMALIZE_RE.split(value.lower()) if part)


def _confidence_to_score(confidence: Any) -> float | None:
    if confidence == 'high':
        return 0.95
    if confidence == 'medium':
        return 0.7
    if confidence == 'low':
        return 0.4
    return None


def _timestamp() -> str:
    return time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
