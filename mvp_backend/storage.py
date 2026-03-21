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
DEFAULT_PATTERN_PROMOTION_MIN_SUPPORT = 3
DEFAULT_PATTERN_PROMOTION_MIN_USERS = 2
DEFAULT_PATTERN_PROMOTION_MIN_STABILITY = 0.75
DEFAULT_PATTERN_PROMOTION_MAX_DRIFT = 0.25
LEARNING_VECTOR_PROVIDER = 'local'
LEARNING_VECTOR_MODEL = 'heuristic-v1'
FEATURE_TOKEN_LIMIT = 48

FIELD_NORMALIZE_RE = re.compile(r'[^a-zA-Zа-яА-Я0-9]+')

_db_client: DatabaseClient | None = None


class UserConflictError(ValueError):
    pass


class InvalidCredentialsError(ValueError):
    pass


class UserNotFoundError(ValueError):
    pass


class EmailChangeError(ValueError):
    pass


class ProfileUpdateError(ValueError):
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


def _training_root_dir() -> Path:
    return RUNTIME_DIR / 'training'


def _training_snapshots_dir() -> Path:
    return _training_root_dir() / 'snapshots'


def _training_runs_dir() -> Path:
    return _training_root_dir() / 'runs'


def ensure_dirs() -> None:
    for path in [
        RUNTIME_DIR,
        BASE_DIR,
        UPLOAD_DIR,
        GUEST_DIR,
        AUTH_DIR,
        _training_root_dir(),
        _training_snapshots_dir(),
        _training_runs_dir(),
    ]:
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
    _migrate_mapping_suggestions_source_of_truth_constraint()
    _migrate_model_deployments_provider_constraint()
    migrate_legacy_history()


def _migrate_mapping_suggestions_source_of_truth_constraint() -> None:
    db = get_db()
    row = db.get(
        '''
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'mapping_suggestions'
        '''
    )
    if row is None:
        return

    table_sql = str(row['sql'] or '')
    if 'global_pattern' in table_sql:
        return

    db.executescript(
        '''
        BEGIN;
        CREATE TABLE mapping_suggestions__new (
          id INTEGER PRIMARY KEY,
          user_id INTEGER,
          generation_id INTEGER,
          schema_fingerprint_id INTEGER,
          source_field TEXT,
          source_field_normalized TEXT,
          target_field TEXT NOT NULL,
          target_field_normalized TEXT NOT NULL,
          confidence REAL,
          reason TEXT,
          status TEXT NOT NULL DEFAULT 'suggested',
          source_of_truth TEXT NOT NULL DEFAULT 'model_suggestion',
          model_provider TEXT,
          model_name TEXT,
          feedback_payload_json TEXT,
          metadata_json TEXT,
          reviewed_at TEXT,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
          CHECK (status IN ('suggested', 'accepted', 'rejected')),
          CHECK (source_of_truth IN ('deterministic_rule', 'personal_memory', 'model_suggestion', 'global_pattern', 'position_fallback', 'unresolved')),
          CHECK (feedback_payload_json IS NULL OR json_valid(feedback_payload_json)),
          CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
          FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
          FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
          FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
        );
        INSERT INTO mapping_suggestions__new (
          id,
          user_id,
          generation_id,
          schema_fingerprint_id,
          source_field,
          source_field_normalized,
          target_field,
          target_field_normalized,
          confidence,
          reason,
          status,
          source_of_truth,
          model_provider,
          model_name,
          feedback_payload_json,
          metadata_json,
          reviewed_at,
          created_at,
          updated_at
        )
        SELECT
          id,
          user_id,
          generation_id,
          schema_fingerprint_id,
          source_field,
          source_field_normalized,
          target_field,
          target_field_normalized,
          confidence,
          reason,
          status,
          source_of_truth,
          model_provider,
          model_name,
          feedback_payload_json,
          metadata_json,
          reviewed_at,
          created_at,
          updated_at
        FROM mapping_suggestions;
        DROP TABLE mapping_suggestions;
        ALTER TABLE mapping_suggestions__new RENAME TO mapping_suggestions;
        COMMIT;
        '''
    )


def _migrate_model_deployments_provider_constraint() -> None:
    db = get_db()
    row = db.get(
        '''
        SELECT sql
        FROM sqlite_master
        WHERE type = 'table' AND name = 'model_deployments'
        '''
    )
    if row is None:
        return

    table_sql = str(row['sql'] or '')
    if 'gigachat' in table_sql:
        return

    db.executescript(
        '''
        BEGIN;
        CREATE TABLE model_deployments__new (
          id INTEGER PRIMARY KEY,
          training_run_id INTEGER NOT NULL,
          snapshot_id INTEGER NOT NULL,
          model_family TEXT NOT NULL,
          provider TEXT NOT NULL,
          base_url TEXT,
          model_name TEXT NOT NULL,
          artifact_uri TEXT,
          config_json TEXT,
          status TEXT NOT NULL DEFAULT 'inactive',
          notes TEXT,
          created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
          activated_at TEXT,
          deactivated_at TEXT,
          CHECK (provider IN ('ollama', 'openai_compatible', 'gigachat')),
          CHECK (config_json IS NULL OR json_valid(config_json)),
          CHECK (status IN ('inactive', 'active', 'retired')),
          FOREIGN KEY (training_run_id) REFERENCES model_training_runs(id) ON DELETE CASCADE,
          FOREIGN KEY (snapshot_id) REFERENCES training_snapshots(id) ON DELETE CASCADE
        );
        INSERT INTO model_deployments__new (
          id,
          training_run_id,
          snapshot_id,
          model_family,
          provider,
          base_url,
          model_name,
          artifact_uri,
          config_json,
          status,
          notes,
          created_at,
          activated_at,
          deactivated_at
        )
        SELECT
          id,
          training_run_id,
          snapshot_id,
          model_family,
          provider,
          base_url,
          model_name,
          artifact_uri,
          config_json,
          status,
          notes,
          created_at,
          activated_at,
          deactivated_at
        FROM model_deployments;
        DROP TABLE model_deployments;
        ALTER TABLE model_deployments__new RENAME TO model_deployments;
        COMMIT;
        '''
    )


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


def update_user_password(email: str, password: str) -> None:
    normalized_email = email.strip().lower()
    normalized_password = password.strip()

    if not normalized_email:
        raise UserNotFoundError('Введите email.')
    if not normalized_password:
        raise UserConflictError('Введите новый пароль.')
    if len(normalized_password) < 8:
        raise UserConflictError('Пароль должен содержать минимум 8 символов.')

    db = get_db()
    row = db.get(
        '''
        SELECT id
        FROM users
        WHERE email = :email
        ''',
        {'email': normalized_email},
    )
    if row is None:
        raise UserNotFoundError('Пользователь с таким email не найден.')

    db.run(
        '''
        UPDATE users
        SET password_hash = :password_hash
        WHERE email = :email
        ''',
        {
            'email': normalized_email,
            'password_hash': hash_password(normalized_password),
        },
    )


def get_user_profile(external_id: str) -> dict[str, str]:
    normalized_external_id = external_id.strip()
    if not normalized_external_id:
        raise UserNotFoundError('Пользователь не найден.')

    db = get_db()
    row = db.get(
        '''
        SELECT external_id, email, display_name
        FROM users
        WHERE external_id = :external_id
        ''',
        {'external_id': normalized_external_id},
    )
    if row is None:
        raise UserNotFoundError('Пользователь не найден.')

    return {
        'id': str(row['external_id'] or ''),
        'name': str(row['display_name'] or row['email'] or 'Desktop User'),
        'email': str(row['email'] or ''),
    }


def _get_user_row_by_external_id(external_id: str):
    normalized_external_id = external_id.strip()
    if not normalized_external_id:
        raise UserNotFoundError('Пользователь не найден.')

    db = get_db()
    row = db.get(
        '''
        SELECT id, external_id, email, display_name, password_hash
        FROM users
        WHERE external_id = :external_id
        ''',
        {'external_id': normalized_external_id},
    )
    if row is None:
        raise UserNotFoundError('Пользователь не найден.')
    return row


def _validate_new_email(new_email: str, current_email: str) -> str:
    normalized_email = new_email.strip().lower()
    if not normalized_email:
        raise EmailChangeError('Введите новый email.')
    if normalized_email == current_email.strip().lower():
        raise EmailChangeError('Новая почта совпадает с текущей.')
    if is_email_registered(normalized_email):
        raise EmailChangeError('Этот email уже занят другим аккаунтом.')
    return normalized_email


def verify_user_password(external_id: str, password: str) -> None:
    row = _get_user_row_by_external_id(external_id)
    normalized_password = password.strip()
    if not normalized_password:
        raise InvalidCredentialsError('Введите текущий пароль.')
    if not verify_password(normalized_password, row['password_hash']):
        raise InvalidCredentialsError('Неверный текущий пароль.')


def prepare_email_change(external_id: str, new_email: str) -> tuple[str, str]:
    row = _get_user_row_by_external_id(external_id)
    current_email = str(row['email'] or '')
    normalized_new_email = _validate_new_email(new_email, current_email)
    return current_email, normalized_new_email


def change_user_email(external_id: str, new_email: str) -> dict[str, str]:
    row = _get_user_row_by_external_id(external_id)
    normalized_new_email = _validate_new_email(new_email, str(row['email'] or ''))

    db = get_db()
    db.run(
        '''
        UPDATE users
        SET email = :email
        WHERE external_id = :external_id
        ''',
        {
            'email': normalized_new_email,
            'external_id': external_id.strip(),
        },
    )

    return {
        'id': str(row['external_id'] or ''),
        'name': str(row['display_name'] or normalized_new_email.split('@', 1)[0] or 'Desktop User'),
        'email': normalized_new_email,
    }


def update_user_profile_name(external_id: str, name: str) -> dict[str, str]:
    row = _get_user_row_by_external_id(external_id)
    normalized_name = name.strip()
    if not normalized_name:
        raise ProfileUpdateError('Введите имя.')

    db = get_db()
    db.run(
        '''
        UPDATE users
        SET display_name = :display_name
        WHERE external_id = :external_id
        ''',
        {
            'display_name': normalized_name,
            'external_id': external_id.strip(),
        },
    )

    return {
        'id': str(row['external_id'] or ''),
        'name': normalized_name,
        'email': str(row['email'] or ''),
    }


def change_user_password(external_id: str, current_password: str, new_password: str) -> dict[str, str]:
    row = _get_user_row_by_external_id(external_id)
    verify_user_password(external_id, current_password)

    normalized_password = new_password.strip()
    if not normalized_password:
        raise UserConflictError('Введите новый пароль.')
    if len(normalized_password) < 8:
        raise UserConflictError('Пароль должен содержать минимум 8 символов.')

    db = get_db()
    db.run(
        '''
        UPDATE users
        SET password_hash = :password_hash
        WHERE external_id = :external_id
        ''',
        {
            'password_hash': hash_password(normalized_password),
            'external_id': external_id.strip(),
        },
    )

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
    promotion_mode: str = 'automatic',
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

        if promotion_mode == 'automatic':
            _upsert_mapping_memory_entries(
                db=db,
                user_id=internal_user_id,
                generation_id=generation_id,
                schema_fingerprint_id=resolved_schema_fingerprint_id,
                mappings_json=safe_mappings_json,
                source_of_truth='accepted_generation',
            )

        quality_score = _estimate_generation_quality(safe_mappings_json, safe_warnings_json)
        mappings_ready_for_learning = _mappings_are_confirmed(safe_mappings_json)
        few_shot_example_id = None
        if promotion_mode == 'automatic' or mappings_ready_for_learning:
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

        if few_shot_example_id is not None and promotion_mode == 'automatic':
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
            a.selected_sheet,
            a.parsed_file_json,
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
            'mapping_suggestions': 0,
            'draft_json_suggestions': 0,
            'mapping_memory': 0,
            'few_shot_examples': 0,
            'user_templates': 0,
            'correction_sessions': 0,
            'user_corrections': 0,
            'frequent_djson': 0,
            'global_pattern_candidates': 0,
            'global_curated_dataset_items': 0,
            'model_based_suggestions': 0,
            'model_based_accepted': 0,
            'model_based_rejected': 0,
            'model_based_acceptance_rate': None,
        }

    db = get_db()
    counts = {
        'uploads': _count_rows(db, 'SELECT COUNT(*) AS value FROM uploaded_files WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'schema_fingerprints': _count_rows(db, 'SELECT COUNT(*) AS value FROM schema_fingerprints WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'mapping_suggestions': _count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_suggestions WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'draft_json_suggestions': _count_rows(db, 'SELECT COUNT(*) AS value FROM draft_json_suggestions WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'mapping_memory': _count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_memory WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'few_shot_examples': _count_rows(db, 'SELECT COUNT(*) AS value FROM few_shot_examples WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'user_templates': _count_rows(db, 'SELECT COUNT(*) AS value FROM user_templates WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'correction_sessions': _count_rows(db, 'SELECT COUNT(*) AS value FROM correction_sessions WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'user_corrections': _count_rows(db, 'SELECT COUNT(*) AS value FROM user_corrections WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'frequent_djson': _count_rows(db, 'SELECT COUNT(*) AS value FROM frequent_djson WHERE user_id = :user_id', {'user_id': internal_user_id}),
        'global_pattern_candidates': _count_rows(db, 'SELECT COUNT(*) AS value FROM pattern_candidates'),
        'global_curated_dataset_items': _count_rows(db, 'SELECT COUNT(*) AS value FROM curated_dataset_items'),
        'model_based_suggestions': _count_rows(
            db,
            'SELECT COUNT(*) AS value FROM mapping_suggestions WHERE user_id = :user_id AND source_of_truth = "model_suggestion"',
            {'user_id': internal_user_id},
        ),
        'model_based_accepted': _count_rows(
            db,
            'SELECT COUNT(*) AS value FROM mapping_suggestions WHERE user_id = :user_id AND source_of_truth = "model_suggestion" AND status = "accepted"',
            {'user_id': internal_user_id},
        ),
        'model_based_rejected': _count_rows(
            db,
            'SELECT COUNT(*) AS value FROM mapping_suggestions WHERE user_id = :user_id AND source_of_truth = "model_suggestion" AND status = "rejected"',
            {'user_id': internal_user_id},
        ),
    }
    reviewed_model_suggestions = counts['model_based_accepted'] + counts['model_based_rejected']
    counts['model_based_acceptance_rate'] = (
        counts['model_based_accepted'] / reviewed_model_suggestions if reviewed_model_suggestions else None
    )
    counts['user_id'] = user_id
    return counts


def get_learning_memory_layers(user_id: str, limit: int = 20) -> dict[str, Any]:
    internal_user_id = _lookup_internal_user_id(user_id)
    safe_limit = max(limit, 1)
    empty_layers = {
        'user_id': user_id,
        'layers': {
            'staging': {'counts': {'pending': 0, 'rejected': 0, 'total': 0}, 'items': []},
            'personal_memory': {'counts': {'entries': 0, 'accepted': 0, 'rejected': 0}, 'items': []},
            'global_knowledge': {'counts': {'patterns': 0, 'promoted': 0, 'accepted': 0, 'reviewing': 0}, 'items': []},
        },
    }
    if internal_user_id is None:
        return empty_layers

    db = get_db()

    staging_counts_row = db.get(
        '''
        SELECT
            COALESCE(SUM(CASE WHEN status = 'suggested' THEN 1 ELSE 0 END), 0) AS pending_count,
            COALESCE(SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END), 0) AS rejected_count,
            COUNT(*) AS total_count
        FROM mapping_suggestions
        WHERE user_id = :user_id
          AND status IN ('suggested', 'rejected')
        ''',
        {'user_id': internal_user_id},
    )
    staging_rows = db.all(
        '''
        SELECT
            source_field,
            source_field_normalized,
            target_field,
            target_field_normalized,
            source_of_truth,
            status,
            COUNT(*) AS seen_count,
            AVG(confidence) AS average_confidence,
            MAX(confidence) AS max_confidence,
            MAX(updated_at) AS last_seen_at
        FROM mapping_suggestions
        WHERE user_id = :user_id
          AND status IN ('suggested', 'rejected')
        GROUP BY
            source_field_normalized,
            target_field_normalized,
            source_of_truth,
            status
        ORDER BY
            CASE status WHEN 'suggested' THEN 0 ELSE 1 END,
            last_seen_at DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': safe_limit},
    )

    personal_counts_row = db.get(
        '''
        SELECT
            COUNT(*) AS entry_count,
            COALESCE(SUM(success_count), 0) AS accepted_count,
            COALESCE(SUM(failure_count), 0) AS rejected_count
        FROM mapping_memory
        WHERE user_id = :user_id
        ''',
        {'user_id': internal_user_id},
    )
    personal_rows = db.all(
        '''
        SELECT
            source_field,
            source_field_normalized,
            target_field,
            target_field_normalized,
            COUNT(*) AS row_count,
            COUNT(DISTINCT COALESCE(schema_fingerprint_id, 0)) AS schema_fingerprint_count,
            COALESCE(SUM(usage_count), 0) AS usage_total,
            COALESCE(SUM(success_count), 0) AS accepted_count,
            COALESCE(SUM(failure_count), 0) AS rejected_count,
            AVG(confidence) AS average_confidence,
            MAX(last_used_at) AS last_seen_at,
            group_concat(DISTINCT source_of_truth) AS source_of_truths
        FROM mapping_memory
        WHERE user_id = :user_id
        GROUP BY source_field_normalized, target_field_normalized
        ORDER BY accepted_count DESC, usage_total DESC, last_seen_at DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': safe_limit},
    )

    global_counts_row = db.get(
        '''
        SELECT
            COUNT(*) AS pattern_count,
            COALESCE(SUM(CASE WHEN status = 'promoted' THEN 1 ELSE 0 END), 0) AS promoted_count,
            COALESCE(SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END), 0) AS accepted_count,
            COALESCE(SUM(CASE WHEN status = 'reviewing' THEN 1 ELSE 0 END), 0) AS reviewing_count
        FROM pattern_candidates
        ''',
        {},
    )
    global_rows = db.all(
        '''
        SELECT
            pc.id,
            pc.source_field_normalized,
            pc.target_field_normalized,
            pc.status,
            pc.support_count,
            pc.distinct_users_count,
            pc.mean_confidence,
            pc.proposed_rule_json,
            ps.accept_count,
            ps.reject_count,
            ps.unique_users,
            ps.recurrence_count,
            ps.stability_score,
            ps.drift_score,
            ps.last_seen_at
        FROM pattern_candidates pc
        LEFT JOIN pattern_stats ps
            ON ps.candidate_id = pc.id
        ORDER BY
            CASE pc.status
                WHEN 'promoted' THEN 3
                WHEN 'accepted' THEN 2
                WHEN 'reviewing' THEN 1
                ELSE 0
            END DESC,
            COALESCE(ps.stability_score, 0) DESC,
            COALESCE(pc.support_count, 0) DESC,
            COALESCE(pc.distinct_users_count, 0) DESC,
            COALESCE(ps.last_seen_at, pc.updated_at) DESC
        LIMIT :limit
        ''',
        {'limit': safe_limit},
    )

    staging_items = [
        {
            'source_field': row['source_field'],
            'source_field_norm': row['source_field_normalized'],
            'target_field': row['target_field'],
            'target_field_norm': row['target_field_normalized'],
            'status': str(row['status']),
            'source_of_truth': str(row['source_of_truth']),
            'seen_count': int(row['seen_count'] or 0),
            'average_confidence': float(row['average_confidence']) if row['average_confidence'] is not None else None,
            'confidence_band': _score_to_confidence(float(row['average_confidence'])) if row['average_confidence'] is not None else 'none',
            'last_seen_at': row['last_seen_at'],
        }
        for row in staging_rows
    ]

    personal_items = [
        {
            'source_field': row['source_field'],
            'source_field_norm': row['source_field_normalized'],
            'target_field': row['target_field'],
            'target_field_norm': row['target_field_normalized'],
            'accepted_count': int(row['accepted_count'] or 0),
            'rejected_count': int(row['rejected_count'] or 0),
            'usage_count': int(row['usage_total'] or 0),
            'schema_fingerprint_count': int(row['schema_fingerprint_count'] or 0),
            'row_count': int(row['row_count'] or 0),
            'average_confidence': float(row['average_confidence']) if row['average_confidence'] is not None else None,
            'confidence_band': _score_to_confidence(float(row['average_confidence'])) if row['average_confidence'] is not None else 'none',
            'source_of_truths': [part for part in str(row['source_of_truths'] or '').split(',') if part],
            'last_seen_at': row['last_seen_at'],
        }
        for row in personal_rows
    ]

    global_items: list[dict[str, Any]] = []
    for row in global_rows:
        proposed_rule = _ensure_json_value(row['proposed_rule_json'], {})
        if not isinstance(proposed_rule, dict):
            proposed_rule = {}
        average_confidence = float(row['mean_confidence']) if row['mean_confidence'] is not None else None
        global_items.append(
            {
                'candidate_id': int(row['id']),
                'source_field': proposed_rule.get('source_field'),
                'source_field_norm': row['source_field_normalized'],
                'target_field': proposed_rule.get('target_field'),
                'target_field_norm': row['target_field_normalized'],
                'status': str(row['status']),
                'support_count': int(row['support_count'] or row['recurrence_count'] or 0),
                'unique_users': int(row['distinct_users_count'] or row['unique_users'] or 0),
                'accepted_count': int(row['accept_count'] or 0),
                'rejected_count': int(row['reject_count'] or 0),
                'stability_score': float(row['stability_score']) if row['stability_score'] is not None else None,
                'drift_score': float(row['drift_score']) if row['drift_score'] is not None else None,
                'average_confidence': average_confidence,
                'confidence_band': _score_to_confidence(average_confidence) if average_confidence is not None else 'none',
                'last_seen_at': row['last_seen_at'],
            }
        )

    return {
        'user_id': user_id,
        'layers': {
            'staging': {
                'counts': {
                    'pending': int(staging_counts_row['pending_count'] or 0) if staging_counts_row else 0,
                    'rejected': int(staging_counts_row['rejected_count'] or 0) if staging_counts_row else 0,
                    'total': int(staging_counts_row['total_count'] or 0) if staging_counts_row else 0,
                },
                'items': staging_items,
            },
            'personal_memory': {
                'counts': {
                    'entries': int(personal_counts_row['entry_count'] or 0) if personal_counts_row else 0,
                    'accepted': int(personal_counts_row['accepted_count'] or 0) if personal_counts_row else 0,
                    'rejected': int(personal_counts_row['rejected_count'] or 0) if personal_counts_row else 0,
                },
                'items': personal_items,
            },
            'global_knowledge': {
                'counts': {
                    'patterns': int(global_counts_row['pattern_count'] or 0) if global_counts_row else 0,
                    'promoted': int(global_counts_row['promoted_count'] or 0) if global_counts_row else 0,
                    'accepted': int(global_counts_row['accepted_count'] or 0) if global_counts_row else 0,
                    'reviewing': int(global_counts_row['reviewing_count'] or 0) if global_counts_row else 0,
                },
                'items': global_items,
            },
        },
    }


def list_learning_events(user_id: str, limit: int = 20) -> list[dict[str, Any]]:
    internal_user_id = _lookup_internal_user_id(user_id)
    if internal_user_id is None:
        return []

    db = get_db()
    fetch_limit = max(limit, 1)
    events: list[dict[str, Any]] = []

    session_rows = db.all(
        '''
        SELECT
            id,
            generation_id,
            session_type,
            status,
            correction_count,
            acceptance_rate,
            notes,
            started_at,
            closed_at
        FROM correction_sessions
        WHERE user_id = :user_id
        ORDER BY coalesce(closed_at, started_at) DESC, id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    session_titles = {
        'feedback_loop': 'Пользователь подтвердил сопоставления',
        'post_generation_fix': 'Правки после генерации сохранены',
        'manual_review': 'Ручная проверка сохранена',
        'template_authoring': 'Шаблон доработан',
    }
    for row in session_rows:
        accepted_rate = float(row['acceptance_rate']) if row['acceptance_rate'] is not None else 0.0
        accepted_count = round(int(row['correction_count'] or 0) * accepted_rate)
        created_at = str(row['closed_at'] or row['started_at'])
        title = session_titles.get(str(row['session_type']), 'Сохранён learning feedback')
        generation_suffix = f' · generation #{int(row["generation_id"])}' if row['generation_id'] is not None else ''
        events.append(
            {
                'id': f'correction-session-{int(row["id"])}',
                'kind': 'feedback_session',
                'stage': 'staging',
                'title': title,
                'description': f'Принято {accepted_count} из {int(row["correction_count"] or 0)} правок{generation_suffix}.',
                'created_at': created_at,
                'metadata': {
                    'session_type': row['session_type'],
                    'status': row['status'],
                    'generation_id': row['generation_id'],
                    'accepted_count': accepted_count,
                    'count': int(row['correction_count'] or 0),
                    'notes': row['notes'],
                },
            }
        )

    few_shot_rows = db.all(
        '''
        SELECT
            id,
            title,
            quality_score,
            source_generation_id,
            created_at
        FROM few_shot_examples
        WHERE user_id = :user_id
        ORDER BY created_at DESC, id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    for row in few_shot_rows:
        quality = float(row['quality_score']) if row['quality_score'] is not None else 0.0
        title = str(row['title'] or 'generation')
        generation_suffix = f' · generation #{int(row["source_generation_id"])}' if row['source_generation_id'] is not None else ''
        events.append(
            {
                'id': f'few-shot-{int(row["id"])}',
                'kind': 'few_shot_example',
                'stage': 'memory',
                'title': 'Генерация сохранена в few-shot',
                'description': f'{title}{generation_suffix} · quality {quality:.2f}',
                'created_at': str(row['created_at']),
                'metadata': {
                    'quality_score': quality,
                    'source_generation_id': row['source_generation_id'],
                },
            }
        )

    template_rows = db.all(
        '''
        SELECT
            id,
            name,
            template_kind,
            target_json,
            updated_at,
            usage_count
        FROM user_templates
        WHERE user_id = :user_id
        ORDER BY updated_at DESC, id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    for row in template_rows:
        target_json = _ensure_json_value(row['target_json'], {})
        if not isinstance(target_json, dict):
            target_json = {}
        events.append(
            {
                'id': f'user-template-{int(row["id"])}',
                'kind': 'user_template',
                'stage': 'memory',
                'title': 'Шаблон сохранён в память',
                'description': f'{str(row["name"] or "Шаблон")} · тип {str(row["template_kind"] or "schema")} · использован {int(row["usage_count"] or 0)} раз(а)',
                'created_at': str(row['updated_at']),
                'metadata': {
                    'template_id': int(row['id']),
                    'template_kind': row['template_kind'],
                    'usage_count': int(row['usage_count'] or 0),
                    'target_json': target_json,
                },
            }
        )

    djson_rows = db.all(
        '''
        SELECT
            id,
            name,
            djson_payload,
            metadata_json,
            updated_at,
            usage_count
        FROM frequent_djson
        WHERE user_id = :user_id
        ORDER BY updated_at DESC, id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    for row in djson_rows:
        metadata = _ensure_json_value(row['metadata_json'], {})
        if not isinstance(metadata, dict):
            metadata = {}
        field_count = int(metadata.get('field_count') or 0)
        target_json = _ensure_json_value(row['djson_payload'], {})
        if not isinstance(target_json, dict):
            target_json = {}
        events.append(
            {
                'id': f'frequent-djson-{int(row["id"])}',
                'kind': 'draft_memory',
                'stage': 'memory',
                'title': 'Draft JSON сохранён в память',
                'description': f'{str(row["name"] or "Draft JSON")} · {field_count} полей · использован {int(row["usage_count"] or 0)} раз(а)',
                'created_at': str(row['updated_at']),
                'metadata': {
                    'djson_id': int(row['id']),
                    'field_count': field_count,
                    'usage_count': int(row['usage_count'] or 0),
                    'target_json': target_json,
                },
            }
        )

    dataset_rows = db.all(
        '''
        SELECT
            cdi.id,
            cdi.source_entity_type,
            cdi.item_kind,
            cdi.target_payload_json,
            cdi.status,
            cdi.quality_score,
            fse.source_generation_id,
            cdi.updated_at
        FROM curated_dataset_items cdi
        LEFT JOIN user_corrections uc
            ON cdi.source_entity_type = 'user_correction'
           AND uc.id = cdi.source_entity_id
        LEFT JOIN few_shot_examples fse
            ON cdi.source_entity_type = 'few_shot_example'
           AND fse.id = cdi.source_entity_id
        LEFT JOIN user_templates ut
            ON cdi.source_entity_type = 'user_template'
           AND ut.id = cdi.source_entity_id
        LEFT JOIN mapping_memory mm
            ON cdi.source_entity_type = 'mapping_memory'
           AND mm.id = cdi.source_entity_id
        WHERE uc.user_id = :user_id
           OR fse.user_id = :user_id
           OR ut.user_id = :user_id
           OR mm.user_id = :user_id
        ORDER BY cdi.updated_at DESC, cdi.id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    for row in dataset_rows:
        quality = float(row['quality_score']) if row['quality_score'] is not None else 0.0
        target_payload = _ensure_json_value(row['target_payload_json'], {})
        if not isinstance(target_payload, dict):
            target_payload = {}
        events.append(
            {
                'id': f'dataset-item-{int(row["id"])}',
                'kind': 'dataset_item',
                'stage': 'dataset',
                'title': 'Кейс добавлен в curated dataset',
                'description': f'{str(row["item_kind"] or "item")} · статус {str(row["status"] or "candidate")} · quality {quality:.2f}',
                'created_at': str(row['updated_at']),
                'metadata': {
                    'source_entity_type': row['source_entity_type'],
                    'item_kind': row['item_kind'],
                    'status': row['status'],
                    'quality_score': quality,
                    'source_generation_id': row['source_generation_id'],
                    'target_payload': target_payload,
                },
            }
        )

    pattern_rows = db.all(
        '''
        SELECT
            pc.id,
            pc.status,
            pc.source_field_normalized,
            pc.target_field_normalized,
            pc.support_count,
            pc.distinct_users_count,
            pc.updated_at,
            pc.proposed_rule_json,
            ps.stability_score
        FROM pattern_candidates pc
        LEFT JOIN pattern_stats ps
            ON ps.candidate_id = pc.id
        WHERE pc.status IN ('reviewing', 'accepted', 'promoted')
          AND (
              EXISTS (
                  SELECT 1
                  FROM user_corrections uc
                  WHERE uc.user_id = :user_id
                    AND uc.source_field_normalized = pc.source_field_normalized
                    AND uc.target_field_normalized = pc.target_field_normalized
              )
              OR EXISTS (
                  SELECT 1
                  FROM mapping_memory mm
                  WHERE mm.user_id = :user_id
                    AND mm.source_field_normalized = pc.source_field_normalized
                    AND mm.target_field_normalized = pc.target_field_normalized
              )
          )
        ORDER BY pc.updated_at DESC, pc.id DESC
        LIMIT :limit
        ''',
        {'user_id': internal_user_id, 'limit': fetch_limit},
    )
    for row in pattern_rows:
        proposed_rule = _ensure_json_value(row['proposed_rule_json'], {})
        if not isinstance(proposed_rule, dict):
            proposed_rule = {}
        source_field = str(proposed_rule.get('source_field') or row['source_field_normalized'] or 'source')
        target_field = str(proposed_rule.get('target_field') or row['target_field_normalized'] or 'target')
        stability = float(row['stability_score']) if row['stability_score'] is not None else 0.0
        events.append(
            {
                'id': f'pattern-candidate-{int(row["id"])}',
                'kind': 'global_pattern',
                'stage': 'global_pattern',
                'title': 'Паттерн поднялся на глобальный уровень',
                'description': f'{source_field} -> {target_field} · статус {str(row["status"])} · support {int(row["support_count"] or 0)} · users {int(row["distinct_users_count"] or 0)} · stability {stability:.2f}',
                'created_at': str(row['updated_at']),
                'metadata': {
                    'candidate_id': int(row['id']),
                    'source_field': source_field,
                    'target_field': target_field,
                    'status': row['status'],
                    'support_count': int(row['support_count'] or 0),
                    'distinct_users_count': int(row['distinct_users_count'] or 0),
                    'stability_score': stability,
                },
            }
        )

    events.sort(key=lambda item: str(item['created_at']), reverse=True)
    return events[:fetch_limit]


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


def get_personal_mapping_memory_candidates(
    *,
    user_id: str | None,
    source_columns: list[str],
    target_fields: list[str],
    schema_fingerprint_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    internal_user_id = _lookup_internal_user_id(user_id)
    if internal_user_id is None:
        return []

    normalized_sources = list(dict.fromkeys(_normalize_field_name(column) for column in source_columns if _normalize_field_name(column)))
    normalized_targets = list(dict.fromkeys(_normalize_field_name(field) for field in target_fields if _normalize_field_name(field)))
    if not normalized_sources or not normalized_targets:
        return []

    params: dict[str, Any] = {
        'user_id': internal_user_id,
        'schema_fingerprint_id': schema_fingerprint_id,
        'limit': max(limit, 1),
    }
    sources_clause = _build_in_clause_params('source_norm', normalized_sources, params)
    targets_clause = _build_in_clause_params('target_norm', normalized_targets, params)
    rows = get_db().all(
        f'''
        SELECT
            id,
            source_field,
            source_field_normalized,
            target_field,
            target_field_normalized,
            confidence,
            weight,
            usage_count,
            success_count,
            failure_count,
            source_of_truth,
            transform_hint,
            CASE
                WHEN :schema_fingerprint_id IS NOT NULL AND schema_fingerprint_id = :schema_fingerprint_id THEN 1
                ELSE 0
            END AS schema_match
        FROM mapping_memory
        WHERE user_id = :user_id
          AND source_field_normalized IN ({sources_clause})
          AND target_field_normalized IN ({targets_clause})
        ORDER BY schema_match DESC, success_count DESC, weight DESC, updated_at DESC
        LIMIT :limit
        ''',
        params,
    )

    candidates: list[dict[str, Any]] = []
    for row in rows:
        confidence = float(row['confidence']) if row['confidence'] is not None else 0.55
        success_count = int(row['success_count'] or 0)
        failure_count = int(row['failure_count'] or 0)
        usage_count = int(row['usage_count'] or 0)
        success_rate = success_count / max(success_count + failure_count, 1)
        score = min(
            1.0,
            confidence * 0.55
            + success_rate * 0.25
            + min(usage_count / 4, 1.0) * 0.1
            + (0.1 if bool(row['schema_match']) else 0.0),
        )
        candidates.append(
            {
                'memory_id': int(row['id']),
                'source_field': str(row['source_field']),
                'source_field_normalized': str(row['source_field_normalized']),
                'target_field': str(row['target_field']),
                'target_field_normalized': str(row['target_field_normalized']),
                'confidence': confidence,
                'score': score,
                'weight': float(row['weight'] or 0.0),
                'usage_count': usage_count,
                'success_count': success_count,
                'failure_count': failure_count,
                'source_of_truth': str(row['source_of_truth']),
                'reason': row['transform_hint'] or 'personal_memory',
                'schema_match': bool(row['schema_match']),
            }
        )
    return candidates


def get_global_mapping_pattern_candidates(
    *,
    source_columns: list[str],
    target_fields: list[str],
    schema_fingerprint_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_sources = list(dict.fromkeys(_normalize_field_name(column) for column in source_columns if _normalize_field_name(column)))
    normalized_targets = list(dict.fromkeys(_normalize_field_name(field) for field in target_fields if _normalize_field_name(field)))
    if not normalized_sources or not normalized_targets:
        return []

    db = get_db()
    schema_hash = _get_schema_fingerprint_hash(db, schema_fingerprint_id)
    params: dict[str, Any] = {
        'schema_hash': schema_hash,
        'limit': max(limit, 1),
    }
    sources_clause = _build_in_clause_params('pattern_source_norm', normalized_sources, params)
    targets_clause = _build_in_clause_params('pattern_target_norm', normalized_targets, params)
    rows = db.all(
        f'''
        SELECT
            pc.id,
            pc.source_field_normalized,
            pc.target_field_normalized,
            pc.schema_hint_hash,
            pc.proposed_rule_json,
            pc.status,
            pc.support_count,
            pc.distinct_users_count,
            pc.mean_confidence,
            ps.recurrence_count,
            ps.unique_users,
            ps.accept_count,
            ps.reject_count,
            ps.stability_score,
            ps.drift_score,
            CASE
                WHEN :schema_hash IS NOT NULL AND pc.schema_hint_hash = :schema_hash THEN 1
                ELSE 0
            END AS schema_match
        FROM pattern_candidates pc
        LEFT JOIN pattern_stats ps
            ON ps.candidate_id = pc.id
        WHERE pc.source_field_normalized IN ({sources_clause})
          AND pc.target_field_normalized IN ({targets_clause})
          AND pc.status <> 'rejected'
          AND (
              coalesce(ps.recurrence_count, 0) > 0
              OR pc.status IN ('accepted', 'promoted')
          )
        ORDER BY
            schema_match DESC,
            CASE pc.status
                WHEN 'promoted' THEN 3
                WHEN 'accepted' THEN 2
                WHEN 'reviewing' THEN 1
                ELSE 0
            END DESC,
            coalesce(ps.stability_score, 0) DESC,
            coalesce(pc.support_count, 0) DESC,
            coalesce(pc.distinct_users_count, 0) DESC,
            pc.updated_at DESC
        LIMIT :limit
        ''',
        params,
    )

    candidates: list[dict[str, Any]] = []
    for row in rows:
        rule_payload = _ensure_json_value(row['proposed_rule_json'], {})
        confidence = float(row['mean_confidence']) if row['mean_confidence'] is not None else 0.55
        stability = float(row['stability_score']) if row['stability_score'] is not None else 0.0
        support_count = int(row['support_count'] or row['recurrence_count'] or 0)
        distinct_users = int(row['distinct_users_count'] or row['unique_users'] or 0)
        score = min(
            1.0,
            confidence * 0.35
            + stability * 0.35
            + min(support_count / 5, 1.0) * 0.2
            + min(distinct_users / 3, 1.0) * 0.1
            + (0.1 if bool(row['schema_match']) else 0.0),
        )
        if not isinstance(rule_payload, dict):
            rule_payload = {}
        candidates.append(
            {
                'candidate_id': int(row['id']),
                'source_field': rule_payload.get('source_field'),
                'source_field_normalized': str(row['source_field_normalized']),
                'target_field': rule_payload.get('target_field'),
                'target_field_normalized': str(row['target_field_normalized']),
                'confidence': confidence,
                'score': score,
                'support_count': support_count,
                'distinct_users_count': distinct_users,
                'stability_score': stability,
                'drift_score': float(row['drift_score']) if row['drift_score'] is not None else None,
                'status': str(row['status']),
                'schema_match': bool(row['schema_match']),
                'reason': 'global_pattern_candidate',
            }
        )
    return candidates


def get_personal_field_naming_candidates(
    *,
    user_id: str | None,
    source_columns: list[str],
    schema_fingerprint_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    internal_user_id = _lookup_internal_user_id(user_id)
    if internal_user_id is None:
        return []

    normalized_sources = list(dict.fromkeys(_normalize_field_name(column) for column in source_columns if _normalize_field_name(column)))
    if not normalized_sources:
        return []

    params: dict[str, Any] = {
        'user_id': internal_user_id,
        'schema_fingerprint_id': schema_fingerprint_id,
        'limit': max(limit, 1),
    }
    sources_clause = _build_in_clause_params('source_name_norm', normalized_sources, params)
    rows = get_db().all(
        f'''
        SELECT
            id,
            source_field,
            source_field_normalized,
            target_field,
            target_field_normalized,
            confidence,
            weight,
            usage_count,
            success_count,
            failure_count,
            source_of_truth,
            transform_hint,
            CASE
                WHEN :schema_fingerprint_id IS NOT NULL AND schema_fingerprint_id = :schema_fingerprint_id THEN 1
                ELSE 0
            END AS schema_match
        FROM mapping_memory
        WHERE user_id = :user_id
          AND source_field_normalized IN ({sources_clause})
        ORDER BY schema_match DESC, success_count DESC, weight DESC, updated_at DESC
        LIMIT :limit
        ''',
        params,
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        confidence = float(row['confidence']) if row['confidence'] is not None else 0.55
        success_count = int(row['success_count'] or 0)
        failure_count = int(row['failure_count'] or 0)
        usage_count = int(row['usage_count'] or 0)
        success_rate = success_count / max(success_count + failure_count, 1)
        score = min(
            1.0,
            confidence * 0.55
            + success_rate * 0.25
            + min(usage_count / 4, 1.0) * 0.1
            + (0.1 if bool(row['schema_match']) else 0.0),
        )
        items.append(
            {
                'memory_id': int(row['id']),
                'source_field': str(row['source_field']),
                'source_field_normalized': str(row['source_field_normalized']),
                'target_field': str(row['target_field']),
                'target_field_normalized': str(row['target_field_normalized']),
                'confidence': confidence,
                'score': score,
                'success_count': success_count,
                'failure_count': failure_count,
                'usage_count': usage_count,
                'schema_match': bool(row['schema_match']),
                'source_of_truth': str(row['source_of_truth']),
                'reason': row['transform_hint'] or 'personal_memory',
            }
        )
    return items


def get_global_field_naming_candidates(
    *,
    source_columns: list[str],
    schema_fingerprint_id: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    normalized_sources = list(dict.fromkeys(_normalize_field_name(column) for column in source_columns if _normalize_field_name(column)))
    if not normalized_sources:
        return []

    db = get_db()
    schema_hash = _get_schema_fingerprint_hash(db, schema_fingerprint_id)
    params: dict[str, Any] = {
        'schema_hash': schema_hash,
        'limit': max(limit, 1),
    }
    sources_clause = _build_in_clause_params('global_source_norm', normalized_sources, params)
    rows = db.all(
        f'''
        SELECT
            pc.id,
            pc.source_field_normalized,
            pc.target_field_normalized,
            pc.schema_hint_hash,
            pc.proposed_rule_json,
            pc.status,
            pc.support_count,
            pc.distinct_users_count,
            pc.mean_confidence,
            ps.recurrence_count,
            ps.unique_users,
            ps.accept_count,
            ps.reject_count,
            ps.stability_score,
            ps.drift_score,
            CASE
                WHEN :schema_hash IS NOT NULL AND pc.schema_hint_hash = :schema_hash THEN 1
                ELSE 0
            END AS schema_match
        FROM pattern_candidates pc
        LEFT JOIN pattern_stats ps
            ON ps.candidate_id = pc.id
        WHERE pc.source_field_normalized IN ({sources_clause})
          AND pc.status IN ('reviewing', 'accepted', 'promoted')
        ORDER BY
            schema_match DESC,
            coalesce(ps.stability_score, 0) DESC,
            coalesce(pc.support_count, 0) DESC,
            coalesce(pc.distinct_users_count, 0) DESC,
            pc.updated_at DESC
        LIMIT :limit
        ''',
        params,
    )

    items: list[dict[str, Any]] = []
    for row in rows:
        proposed_rule = _ensure_json_value(row['proposed_rule_json'], {})
        if not isinstance(proposed_rule, dict):
            proposed_rule = {}
        support_count = int(row['support_count'] or row['recurrence_count'] or 0)
        distinct_users = int(row['distinct_users_count'] or row['unique_users'] or 0)
        stability_score = float(row['stability_score']) if row['stability_score'] is not None else 0.0
        mean_confidence = float(row['mean_confidence']) if row['mean_confidence'] is not None else 0.55
        score = min(
            1.0,
            stability_score * 0.45
            + mean_confidence * 0.25
            + min(support_count / 5, 1.0) * 0.2
            + min(distinct_users / 3, 1.0) * 0.1,
        )
        items.append(
            {
                'candidate_id': int(row['id']),
                'source_field': proposed_rule.get('source_field'),
                'source_field_normalized': str(row['source_field_normalized']),
                'target_field': proposed_rule.get('target_field'),
                'target_field_normalized': str(row['target_field_normalized']),
                'score': score,
                'confidence': mean_confidence,
                'support_count': support_count,
                'distinct_users_count': distinct_users,
                'stability_score': stability_score,
                'drift_score': float(row['drift_score']) if row['drift_score'] is not None else None,
                'schema_match': bool(row['schema_match']),
                'status': str(row['status']),
                'reason': 'global_pattern_candidate',
            }
        )
    return items


def save_mapping_suggestions(
    *,
    generation_id: int | None,
    mappings: list[dict[str, Any]],
    user_id: str | None = None,
    schema_fingerprint_id: int | None = None,
) -> list[dict[str, Any]]:
    if generation_id is None:
        return mappings

    db = get_db()
    internal_user_id = _resolve_internal_user_id(user_id=user_id)
    now = _timestamp()
    saved: list[dict[str, Any]] = []

    with db.transaction():
        for mapping in mappings:
            target_field = str(mapping.get('target') or '').strip()
            if not target_field:
                saved.append(mapping)
                continue

            source_field = mapping.get('source')
            normalized_source = _normalize_field_name(str(source_field)) if source_field else None
            status = str(mapping.get('status') or ('accepted' if mapping.get('confidence') in {'high', 'medium'} else 'suggested'))
            cursor = db.run(
                '''
                INSERT INTO mapping_suggestions (
                    user_id,
                    generation_id,
                    schema_fingerprint_id,
                    source_field,
                    source_field_normalized,
                    target_field,
                    target_field_normalized,
                    confidence,
                    reason,
                    status,
                    source_of_truth,
                    model_provider,
                    model_name,
                    feedback_payload_json,
                    metadata_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    :user_id,
                    :generation_id,
                    :schema_fingerprint_id,
                    :source_field,
                    :source_field_normalized,
                    :target_field,
                    :target_field_normalized,
                    :confidence,
                    :reason,
                    :status,
                    :source_of_truth,
                    :model_provider,
                    :model_name,
                    NULL,
                    :metadata_json,
                    :created_at,
                    :updated_at
                )
                ''',
                {
                    'user_id': internal_user_id,
                    'generation_id': generation_id,
                    'schema_fingerprint_id': schema_fingerprint_id,
                    'source_field': str(source_field) if source_field else None,
                    'source_field_normalized': normalized_source,
                    'target_field': target_field,
                    'target_field_normalized': _normalize_field_name(target_field),
                    'confidence': _confidence_to_score(mapping.get('confidence')),
                    'reason': mapping.get('reason'),
                    'status': status,
                    'source_of_truth': mapping.get('source_of_truth') or 'model_suggestion',
                    'model_provider': LEARNING_VECTOR_PROVIDER if mapping.get('source_of_truth') == 'model_suggestion' else None,
                    'model_name': LEARNING_VECTOR_MODEL if mapping.get('source_of_truth') == 'model_suggestion' else None,
                    'metadata_json': _json_or_none(
                        {
                            'raw_confidence': mapping.get('confidence'),
                            'reason': mapping.get('reason'),
                            'candidate_metadata': mapping.get('candidate_metadata', {}),
                            'model_confidence_score': mapping.get('model_confidence_score'),
                        }
                    ),
                    'created_at': now,
                    'updated_at': now,
                },
            )
            saved_mapping = dict(mapping)
            saved_mapping['suggestion_id'] = int(cursor.lastrowid)
            saved_mapping['schema_fingerprint_id'] = schema_fingerprint_id
            saved.append(saved_mapping)

    return saved


def save_draft_json_suggestions(
    *,
    suggestions: list[dict[str, Any]],
    user_id: str | None = None,
    schema_fingerprint_id: int | None = None,
) -> list[dict[str, Any]]:
    if not suggestions or user_id is None:
        return suggestions

    db = get_db()
    internal_user_id = _resolve_internal_user_id(user_id=user_id)
    now = _timestamp()
    saved: list[dict[str, Any]] = []

    with db.transaction():
        for suggestion in suggestions:
            source_column = str(suggestion.get('source_column') or '').strip()
            suggested_field = str(suggestion.get('target_field') or '').strip()
            if not source_column or not suggested_field:
                saved.append(suggestion)
                continue

            cursor = db.run(
                '''
                INSERT INTO draft_json_suggestions (
                    user_id,
                    schema_fingerprint_id,
                    source_column,
                    source_column_normalized,
                    suggested_field,
                    suggested_field_normalized,
                    field_type,
                    default_value_json,
                    confidence,
                    reason,
                    status,
                    source_of_truth,
                    model_provider,
                    model_name,
                    feedback_payload_json,
                    metadata_json,
                    reviewed_at,
                    created_at,
                    updated_at
                )
                VALUES (
                    :user_id,
                    :schema_fingerprint_id,
                    :source_column,
                    :source_column_normalized,
                    :suggested_field,
                    :suggested_field_normalized,
                    :field_type,
                    :default_value_json,
                    :confidence,
                    :reason,
                    :status,
                    :source_of_truth,
                    :model_provider,
                    :model_name,
                    NULL,
                    :metadata_json,
                    NULL,
                    :created_at,
                    :updated_at
                )
                ''',
                {
                    'user_id': internal_user_id,
                    'schema_fingerprint_id': schema_fingerprint_id,
                    'source_column': source_column,
                    'source_column_normalized': _normalize_field_name(source_column),
                    'suggested_field': suggested_field,
                    'suggested_field_normalized': _normalize_field_name(suggested_field),
                    'field_type': str(suggestion.get('field_type') or 'string'),
                    'default_value_json': _json_or_none(suggestion.get('default_value')),
                    'confidence': _confidence_to_score(suggestion.get('confidence')),
                    'reason': suggestion.get('reason'),
                    'status': suggestion.get('status') or 'suggested',
                    'source_of_truth': suggestion.get('source_of_truth') or 'model_suggestion',
                    'model_provider': LEARNING_VECTOR_PROVIDER if suggestion.get('source_of_truth') == 'model_suggestion' else None,
                    'model_name': LEARNING_VECTOR_MODEL if suggestion.get('source_of_truth') == 'model_suggestion' else None,
                    'metadata_json': _json_or_none(
                        {
                            'raw_confidence': suggestion.get('confidence'),
                            'reason': suggestion.get('reason'),
                        }
                    ),
                    'created_at': now,
                    'updated_at': now,
                },
            )
            saved_item = dict(suggestion)
            saved_item['suggestion_id'] = int(cursor.lastrowid)
            saved_item['schema_fingerprint_id'] = schema_fingerprint_id
            saved.append(saved_item)

    return saved


def confirm_generation_learning(
    *,
    user_id: str,
    generation_id: int,
    notes: str | None = None,
) -> dict[str, Any]:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    context = _load_generation_learning_context(db, generation_id)
    if context is None:
        raise ValueError(f'Generation не найдена: {generation_id}')
    if int(context['internal_user_id']) != internal_user_id:
        raise ValueError('Нельзя подтверждать чужую генерацию.')

    db.run(
        '''
        UPDATE mapping_suggestions
        SET
            status = CASE
                WHEN status = 'suggested' AND source_field IS NOT NULL THEN 'accepted'
                ELSE status
            END,
            reviewed_at = :reviewed_at,
            updated_at = :updated_at
        WHERE generation_id = :generation_id
          AND user_id = :user_id
        ''',
        {
            'generation_id': generation_id,
            'user_id': internal_user_id,
            'reviewed_at': _timestamp(),
            'updated_at': _timestamp(),
        },
    )
    promoted = _promote_confirmed_generation_learning(
        db=db,
        generation_id=generation_id,
        schema_fingerprint_id=int(context['schema_fingerprint_id']) if context['schema_fingerprint_id'] is not None else None,
        notes=notes,
    )
    promoted['generation_id'] = generation_id
    return promoted


def apply_draft_json_feedback(
    *,
    user_id: str,
    schema_fingerprint_id: int,
    draft_json: dict[str, Any],
    feedback: list[dict[str, Any]],
    template_name: str | None = None,
    save_as_template: bool = True,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    accepted_fields: dict[str, Any] = {}
    accepted_feedback_count = 0
    rejected_count = 0

    for item in feedback:
        suggestion_row = None
        suggestion_id = item.get('suggestion_id')
        if suggestion_id is not None:
            suggestion_row = db.get(
                '''
                SELECT *
                FROM draft_json_suggestions
                WHERE id = :id
                  AND user_id = :user_id
                ''',
                {'id': suggestion_id, 'user_id': internal_user_id},
            )

        source_column = str(item.get('source_column') or (suggestion_row['source_column'] if suggestion_row else '')).strip()
        suggested_field = str(item.get('suggested_field') or (suggestion_row['suggested_field'] if suggestion_row else '')).strip()
        corrected_field = str(item.get('corrected_field') or suggested_field).strip()
        status = str(item.get('status') or 'suggested')
        confidence_after = item.get('confidence_after')
        if confidence_after is None and status == 'accepted':
            confidence_after = 1.0
        elif confidence_after is None and status == 'rejected':
            confidence_after = 0.0

        if suggestion_row is not None:
            db.run(
                '''
                UPDATE draft_json_suggestions
                SET
                    suggested_field = :suggested_field,
                    suggested_field_normalized = :suggested_field_normalized,
                    confidence = coalesce(:confidence, confidence),
                    status = :status,
                    feedback_payload_json = :feedback_payload_json,
                    metadata_json = :metadata_json,
                    reviewed_at = :reviewed_at,
                    updated_at = :updated_at
                WHERE id = :id
                ''',
                {
                    'id': int(suggestion_row['id']),
                    'suggested_field': corrected_field,
                    'suggested_field_normalized': _normalize_field_name(corrected_field),
                    'confidence': confidence_after,
                    'status': status,
                    'feedback_payload_json': _json_or_none(item),
                    'metadata_json': _json_or_none(_merge_json_objects(metadata, {'notes': notes, 'source_column': source_column})),
                    'reviewed_at': _timestamp(),
                    'updated_at': _timestamp(),
                },
            )

        if status == 'accepted' and corrected_field:
            if corrected_field in draft_json:
                accepted_fields[corrected_field] = draft_json[corrected_field]
            elif suggested_field in draft_json:
                accepted_fields[corrected_field] = draft_json[suggested_field]
            accepted_feedback_count += 1
        elif status == 'rejected':
            rejected_count += 1

    if not feedback:
        accepted_fields = dict(draft_json)

    if not accepted_fields:
        raise ValueError('Нет подтверждённых полей для сохранения draft JSON.')

    _upsert_frequent_djson(
        db=db,
        user_id=internal_user_id,
        schema_fingerprint_id=schema_fingerprint_id,
        target_json=_ensure_json_text(accepted_fields, {}),
        default_name=(template_name or 'Draft JSON').strip(),
    )

    saved_template = None
    if save_as_template:
        saved_template = save_user_template(
            user_id=user_id,
            name=(template_name or 'Draft JSON').strip(),
            template_kind='schema',
            template_json=accepted_fields,
            description=notes,
            target_json=accepted_fields,
            schema_fingerprint_id=schema_fingerprint_id,
            metadata=_merge_json_objects(metadata, {'source': 'draft_json_feedback'}),
        )

    _create_curated_dataset_candidate(
        db=db,
        source_entity_type='user_template' if saved_template is not None else 'mapping_memory',
        source_entity_id=int(saved_template['id']) if saved_template is not None else int(schema_fingerprint_id),
        item_kind='template',
        input_payload={
            'schema_fingerprint_id': schema_fingerprint_id,
            'field_feedback_count': len(feedback),
            'accepted_feedback_count': accepted_feedback_count,
            'rejected_count': rejected_count,
        },
        target_payload={
            'draft_json': accepted_fields,
        },
        quality_score=min(1.0, 0.75 + 0.05 * accepted_feedback_count),
        review_kind='manual',
        reviewer_user_id=internal_user_id,
        review_decision='approved',
    )

    return {
        'schema_fingerprint_id': schema_fingerprint_id,
        'draft_json': accepted_fields,
        'accepted_count': accepted_feedback_count if feedback else len(accepted_fields),
        'rejected_count': rejected_count,
        'saved_as_template': save_as_template,
        'template_name': (template_name or 'Draft JSON').strip(),
    }


def apply_mapping_feedback(
    *,
    user_id: str,
    generation_id: int,
    feedback: list[dict[str, Any]],
    schema_fingerprint_id: int | None = None,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not feedback:
        return {
            'session_id': None,
            'generation_id': generation_id,
            'schema_fingerprint_id': schema_fingerprint_id,
            'correction_ids': [],
            'accepted_count': 0,
            'count': 0,
            'reviewed_count': 0,
            'rejected_count': 0,
        }

    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    resolved_schema_fingerprint_id = schema_fingerprint_id or _find_schema_fingerprint_for_generation(db, generation_id)
    corrections: list[dict[str, Any]] = []
    rejected_pairs: list[dict[str, Any]] = []

    for item in feedback:
        suggestion_row = None
        suggestion_id = item.get('suggestion_id')
        if suggestion_id is not None:
            suggestion_row = db.get(
                '''
                SELECT *
                FROM mapping_suggestions
                WHERE id = :id
                  AND generation_id = :generation_id
                  AND (:user_id IS NULL OR user_id = :user_id)
                ''',
                {
                    'id': suggestion_id,
                    'generation_id': generation_id,
                    'user_id': internal_user_id,
                },
            )

        original_source = item.get('source_field') or (str(suggestion_row['source_field']) if suggestion_row and suggestion_row['source_field'] else None)
        original_target = str(item.get('target_field') or (suggestion_row['target_field'] if suggestion_row else '')).strip()
        final_source = item.get('corrected_source_field')
        if final_source is None:
            final_source = original_source
        final_target = str(item.get('corrected_target_field') or original_target).strip()
        status = str(item.get('status') or 'suggested')
        if not final_target:
            continue

        confidence_before = float(suggestion_row['confidence']) if suggestion_row and suggestion_row['confidence'] is not None else None
        confidence_after = item.get('confidence_after')
        if confidence_after is None and status == 'accepted':
            confidence_after = confidence_before if confidence_before is not None else 1.0
        elif confidence_after is None and status == 'rejected':
            confidence_after = 0.0

        if suggestion_row is not None:
            db.run(
                '''
                UPDATE mapping_suggestions
                SET
                    schema_fingerprint_id = coalesce(:schema_fingerprint_id, schema_fingerprint_id),
                    source_field = :source_field,
                    source_field_normalized = :source_field_normalized,
                    target_field = :target_field,
                    target_field_normalized = :target_field_normalized,
                    confidence = coalesce(:confidence, confidence),
                    status = :status,
                    feedback_payload_json = :feedback_payload_json,
                    metadata_json = :metadata_json,
                    reviewed_at = :reviewed_at,
                    updated_at = :updated_at
                WHERE id = :id
                ''',
                {
                    'id': int(suggestion_row['id']),
                    'schema_fingerprint_id': resolved_schema_fingerprint_id,
                    'source_field': final_source,
                    'source_field_normalized': _normalize_field_name(str(final_source)) if final_source else None,
                    'target_field': final_target,
                    'target_field_normalized': _normalize_field_name(final_target),
                    'confidence': confidence_after,
                    'status': status,
                    'feedback_payload_json': _json_or_none(item),
                    'metadata_json': _json_or_none(
                        {
                            'source_of_truth': suggestion_row['source_of_truth'],
                            'reason': suggestion_row['reason'],
                            'feedback_metadata': item.get('metadata', {}),
                        }
                    ),
                    'reviewed_at': _timestamp(),
                    'updated_at': _timestamp(),
                },
            )

        if status in {'accepted', 'rejected'}:
            corrections.append(
                {
                    'correction_type': 'mapping_override'
                    if final_source != original_source or final_target != original_target
                    else 'feedback_note',
                    'source_field': final_source,
                    'target_field': final_target,
                    'original_value': {
                        'source': original_source,
                        'target': original_target,
                        'status': suggestion_row['status'] if suggestion_row is not None else 'suggested',
                    },
                    'corrected_value': {
                        'source': final_source,
                        'target': final_target,
                        'status': status,
                    },
                    'correction_payload': {
                        'suggestion_id': suggestion_id,
                        'source_of_truth': suggestion_row['source_of_truth'] if suggestion_row is not None else None,
                        'reason': suggestion_row['reason'] if suggestion_row is not None else None,
                        'metadata': item.get('metadata', {}),
                    },
                    'rationale': item.get('rationale'),
                    'confidence_before': confidence_before,
                    'confidence_after': confidence_after,
                    'accepted': status == 'accepted',
                }
            )

            if status == 'rejected' and final_source:
                rejected_pairs.append(
                    {
                        'source_field': final_source,
                        'target_field': final_target,
                    }
                )

    if corrections:
        result = save_correction_session(
            user_id=user_id,
            generation_id=generation_id,
            session_type='feedback_loop',
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            notes=notes,
            metadata=_merge_json_objects(metadata, {'feedback_count': len(corrections)}),
            corrections=corrections,
        )
    else:
        result = {
            'session_id': None,
            'generation_id': generation_id,
            'schema_fingerprint_id': resolved_schema_fingerprint_id,
            'correction_ids': [],
            'accepted_count': 0,
            'count': 0,
        }

    for rejected in rejected_pairs:
        _register_mapping_feedback_outcome(
            db=db,
            user_id=internal_user_id,
            schema_fingerprint_id=resolved_schema_fingerprint_id,
            source_field=rejected['source_field'],
            target_field=rejected['target_field'],
            accepted=False,
        )

    promotion = _promote_confirmed_generation_learning(
        db=db,
        generation_id=generation_id,
        schema_fingerprint_id=resolved_schema_fingerprint_id,
        notes=notes,
    )

    return {
        **result,
        'reviewed_count': len(corrections),
        'rejected_count': len(rejected_pairs),
        'promotion': promotion,
    }


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


def promote_stable_pattern_candidates(
    *,
    min_support_count: int = DEFAULT_PATTERN_PROMOTION_MIN_SUPPORT,
    min_distinct_users: int = DEFAULT_PATTERN_PROMOTION_MIN_USERS,
    min_stability_score: float = DEFAULT_PATTERN_PROMOTION_MIN_STABILITY,
    max_drift_score: float = DEFAULT_PATTERN_PROMOTION_MAX_DRIFT,
) -> dict[str, Any]:
    db = get_db()
    rows = db.all(
        '''
        SELECT
            pc.id,
            pc.source_field_normalized,
            pc.target_field_normalized,
            pc.schema_hint_hash,
            pc.proposed_rule_json,
            pc.evidence_json,
            pc.status,
            pc.support_count,
            pc.distinct_users_count,
            pc.mean_confidence,
            ps.recurrence_count,
            ps.unique_users,
            ps.accept_count,
            ps.reject_count,
            ps.stability_score,
            ps.drift_score
        FROM pattern_candidates pc
        INNER JOIN pattern_stats ps
            ON ps.candidate_id = pc.id
        WHERE pc.status <> 'rejected'
          AND coalesce(ps.recurrence_count, pc.support_count) >= :min_support_count
          AND coalesce(ps.unique_users, pc.distinct_users_count) >= :min_distinct_users
          AND coalesce(ps.stability_score, 0) >= :min_stability_score
          AND coalesce(ps.drift_score, 0) <= :max_drift_score
        ORDER BY coalesce(ps.stability_score, 0) DESC, coalesce(ps.recurrence_count, pc.support_count) DESC, pc.id ASC
        ''',
        {
            'min_support_count': max(min_support_count, 1),
            'min_distinct_users': max(min_distinct_users, 1),
            'min_stability_score': max(0.0, min(min_stability_score, 1.0)),
            'max_drift_score': max(0.0, min(max_drift_score, 1.0)),
        },
    )

    promoted_items: list[dict[str, Any]] = []
    with db.transaction():
        for row in rows:
            recurrence_count = int(row['recurrence_count'] or row['support_count'] or 0)
            unique_users = int(row['unique_users'] or row['distinct_users_count'] or 0)
            stability_score = float(row['stability_score']) if row['stability_score'] is not None else 0.0
            mean_confidence = float(row['mean_confidence']) if row['mean_confidence'] is not None else 0.55
            quality_score = min(
                1.0,
                stability_score * 0.55
                + mean_confidence * 0.2
                + min(recurrence_count / 5, 1.0) * 0.15
                + min(unique_users / 3, 1.0) * 0.1,
            )
            existing_item = db.get(
                '''
                SELECT id
                FROM curated_dataset_items
                WHERE source_entity_type = 'pattern_candidate'
                  AND source_entity_id = :source_entity_id
                ''',
                {'source_entity_id': int(row['id'])},
            )

            dataset_item_id = int(existing_item['id']) if existing_item is not None else None
            if dataset_item_id is None:
                dataset_item_id = _create_curated_dataset_candidate(
                    db=db,
                    source_entity_type='pattern_candidate',
                    source_entity_id=int(row['id']),
                    item_kind='pattern',
                    input_payload={
                        'source_field_normalized': row['source_field_normalized'],
                        'target_field_normalized': row['target_field_normalized'],
                        'schema_hint_hash': row['schema_hint_hash'],
                        'evidence': _ensure_json_value(row['evidence_json'], {}),
                        'recurrence_count': recurrence_count,
                        'unique_users': unique_users,
                    },
                    target_payload=_ensure_json_value(row['proposed_rule_json'], {}),
                    quality_score=quality_score,
                )

            db.run(
                '''
                UPDATE pattern_candidates
                SET
                    status = :status,
                    updated_at = :updated_at
                WHERE id = :id
                ''',
                {
                    'id': int(row['id']),
                    'status': 'promoted' if dataset_item_id is not None else 'accepted',
                    'updated_at': _timestamp(),
                },
            )
            promoted_items.append(
                {
                    'candidate_id': int(row['id']),
                    'dataset_item_id': dataset_item_id,
                    'support_count': recurrence_count,
                    'distinct_users': unique_users,
                    'stability_score': stability_score,
                    'quality_score': quality_score,
                }
            )

    return {
        'count': len(promoted_items),
        'items': promoted_items,
        'thresholds': {
            'min_support_count': min_support_count,
            'min_distinct_users': min_distinct_users,
            'min_stability_score': min_stability_score,
            'max_drift_score': max_drift_score,
        },
    }


def create_training_snapshot(
    *,
    name: str,
    min_quality_score: float = 0.8,
    include_statuses: list[str] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    statuses = [status for status in (include_statuses or ['approved']) if status in {'candidate', 'approved'}]
    if not statuses:
        statuses = ['approved']

    params: dict[str, Any] = {
        'min_quality_score': max(0.0, min(min_quality_score, 1.0)),
    }
    statuses_clause = _build_in_clause_params('dataset_status', statuses, params)
    rows = get_db().all(
        f'''
        SELECT
            id,
            source_entity_type,
            source_entity_id,
            item_kind,
            status,
            split,
            quality_score
        FROM curated_dataset_items
        WHERE status IN ({statuses_clause})
          AND coalesce(quality_score, 0) >= :min_quality_score
        ORDER BY quality_score DESC, id ASC
        ''',
        params,
    )
    if not rows:
        raise ValueError('Нет dataset-элементов, подходящих под выбранные фильтры.')

    manifest = {
        'name': name.strip(),
        'filters': {
            'min_quality_score': params['min_quality_score'],
            'include_statuses': statuses,
        },
        'items': [
            {
                'id': int(row['id']),
                'source_entity_type': str(row['source_entity_type']),
                'source_entity_id': int(row['source_entity_id']),
                'item_kind': str(row['item_kind']),
                'status': str(row['status']),
                'split': str(row['split']),
                'quality_score': float(row['quality_score']) if row['quality_score'] is not None else None,
            }
            for row in rows
        ],
    }
    snapshot_hash = _make_hash(_canonical_json_text(manifest))
    db = get_db()
    existing = db.get(
        '''
        SELECT id, status, item_count, train_count, validation_count, test_count, created_at, finalized_at
        FROM training_snapshots
        WHERE snapshot_hash = :snapshot_hash
        ''',
        {'snapshot_hash': snapshot_hash},
    )
    if existing is not None:
        return {
            'snapshot_id': int(existing['id']),
            'name': name.strip(),
            'snapshot_hash': snapshot_hash,
            'status': str(existing['status']),
            'item_count': int(existing['item_count']),
            'train_count': int(existing['train_count']),
            'validation_count': int(existing['validation_count']),
            'test_count': int(existing['test_count']),
            'created_at': existing['created_at'],
            'finalized_at': existing['finalized_at'],
            'created': False,
        }

    split_counts = {'train': 0, 'validation': 0, 'test': 0}
    for row in rows:
        split = str(row['split'])
        if split in split_counts:
            split_counts[split] += 1

    now = _timestamp()
    cursor = db.run(
        '''
        INSERT INTO training_snapshots (
            name,
            snapshot_hash,
            status,
            manifest_json,
            item_count,
            train_count,
            validation_count,
            test_count,
            notes,
            created_at,
            updated_at
        )
        VALUES (
            :name,
            :snapshot_hash,
            'draft',
            :manifest_json,
            :item_count,
            :train_count,
            :validation_count,
            :test_count,
            :notes,
            :created_at,
            :updated_at
        )
        ''',
        {
            'name': name.strip(),
            'snapshot_hash': snapshot_hash,
            'manifest_json': _ensure_json_text(manifest, {}),
            'item_count': len(rows),
            'train_count': split_counts['train'],
            'validation_count': split_counts['validation'],
            'test_count': split_counts['test'],
            'notes': notes,
            'created_at': now,
            'updated_at': now,
        },
    )
    snapshot_id = int(cursor.lastrowid)
    db.run(
        f'''
        UPDATE curated_dataset_items
        SET snapshot_key = :snapshot_key, updated_at = :updated_at
        WHERE id IN ({', '.join(str(int(row['id'])) for row in rows)})
        ''',
        {
            'snapshot_key': snapshot_hash,
            'updated_at': now,
        },
    )
    return {
        'snapshot_id': snapshot_id,
        'name': name.strip(),
        'snapshot_hash': snapshot_hash,
        'status': 'draft',
        'item_count': len(rows),
        'train_count': split_counts['train'],
        'validation_count': split_counts['validation'],
        'test_count': split_counts['test'],
        'created_at': now,
        'finalized_at': None,
        'created': True,
    }


def create_model_training_run(
    *,
    snapshot_id: int,
    model_family: str,
    base_model: str,
    train_params: dict[str, Any] | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    db = get_db()
    snapshot = db.get(
        '''
        SELECT id, name, snapshot_hash, status, item_count
        FROM training_snapshots
        WHERE id = :id
        ''',
        {'id': snapshot_id},
    )
    if snapshot is None:
        raise ValueError(f'Training snapshot не найден: {snapshot_id}')

    now = _timestamp()
    cursor = db.run(
        '''
        INSERT INTO model_training_runs (
            snapshot_id,
            model_family,
            base_model,
            training_job_ref,
            status,
            train_params_json,
            metrics_json,
            artifact_uri,
            started_at,
            finished_at,
            notes,
            created_at
        )
        VALUES (
            :snapshot_id,
            :model_family,
            :base_model,
            NULL,
            'queued',
            :train_params_json,
            NULL,
            NULL,
            NULL,
            NULL,
            :notes,
            :created_at
        )
        ''',
        {
            'snapshot_id': snapshot_id,
            'model_family': model_family.strip() or 'gigachat',
            'base_model': base_model.strip() or 'GigaChat-2-Pro',
            'train_params_json': _ensure_json_text(train_params or {}, {}),
            'notes': notes,
            'created_at': now,
        },
    )
    return {
        'training_run_id': int(cursor.lastrowid),
        'snapshot_id': snapshot_id,
        'snapshot_name': str(snapshot['name']),
        'snapshot_hash': str(snapshot['snapshot_hash']),
        'item_count': int(snapshot['item_count']),
        'model_family': model_family.strip() or 'gigachat',
        'base_model': base_model.strip() or 'GigaChat-2-Pro',
        'status': 'queued',
        'created_at': now,
    }


def export_training_snapshot(
    *,
    snapshot_id: int,
    overwrite: bool = False,
) -> dict[str, Any]:
    db = get_db()
    snapshot = db.get(
        '''
        SELECT id, name, snapshot_hash, status, manifest_json, item_count, train_count, validation_count, test_count
        FROM training_snapshots
        WHERE id = :id
        ''',
        {'id': snapshot_id},
    )
    if snapshot is None:
        raise ValueError(f'Training snapshot не найден: {snapshot_id}')

    manifest = _ensure_json_value(snapshot['manifest_json'], {})
    if not isinstance(manifest, dict):
        raise ValueError('Snapshot manifest повреждён.')
    items = manifest.get('items')
    if not isinstance(items, list) or not items:
        raise ValueError('В training snapshot нет элементов для экспорта.')

    item_ids = [int(item['id']) for item in items if isinstance(item, dict) and item.get('id') is not None]
    if not item_ids:
        raise ValueError('В training snapshot нет корректных dataset item id.')

    params: dict[str, Any] = {}
    ids_clause = _build_in_clause_params('snapshot_item', [str(item_id) for item_id in item_ids], params)
    rows = db.all(
        f'''
        SELECT
            id,
            source_entity_type,
            source_entity_id,
            item_kind,
            status,
            split,
            input_payload_json,
            target_payload_json,
            context_json,
            quality_score,
            created_at,
            updated_at
        FROM curated_dataset_items
        WHERE CAST(id AS TEXT) IN ({ids_clause})
        ORDER BY id ASC
        ''',
        params,
    )
    if not rows:
        raise ValueError('Не удалось загрузить dataset items для snapshot export.')

    export_dir = _training_snapshots_dir() / f'snapshot-{int(snapshot["id"])}-{str(snapshot["snapshot_hash"])[:12]}'
    if overwrite and export_dir.exists():
        shutil.rmtree(export_dir)
    export_dir.mkdir(parents=True, exist_ok=True)

    export_payloads = {'train': [], 'validation': [], 'test': []}
    task_counts = {'mapping': 0, 'draft_json': 0}
    for row in rows:
        example = _build_training_example_from_dataset_row(dict(row))
        split = str(row['split'] or 'train')
        if split not in export_payloads:
            split = 'train'
        export_payloads[split].append(example)
        task_type = str(example['task_type'])
        task_counts[task_type] = task_counts.get(task_type, 0) + 1

    files: dict[str, str] = {}
    split_counts: dict[str, int] = {}
    for split, payloads in export_payloads.items():
        file_path = export_dir / f'{split}.jsonl'
        with file_path.open('w', encoding='utf-8') as handle:
            for payload in payloads:
                handle.write(json.dumps(payload, ensure_ascii=False) + '\n')
        files[split] = str(file_path)
        split_counts[split] = len(payloads)

    export_manifest = {
        'snapshot_id': int(snapshot['id']),
        'name': str(snapshot['name']),
        'snapshot_hash': str(snapshot['snapshot_hash']),
        'exported_at': _timestamp(),
        'item_count': len(rows),
        'splits': split_counts,
        'task_counts': task_counts,
        'files': files,
    }
    manifest_path = export_dir / 'manifest.json'
    manifest_path.write_text(json.dumps(export_manifest, ensure_ascii=False, indent=2), encoding='utf-8')

    now = _timestamp()
    db.run(
        '''
        UPDATE training_snapshots
        SET
            status = 'exported',
            finalized_at = coalesce(finalized_at, :finalized_at),
            updated_at = :updated_at
        WHERE id = :id
        ''',
        {
            'id': snapshot_id,
            'finalized_at': now,
            'updated_at': now,
        },
    )

    return {
        'snapshot_id': int(snapshot['id']),
        'name': str(snapshot['name']),
        'snapshot_hash': str(snapshot['snapshot_hash']),
        'status': 'exported',
        'item_count': len(rows),
        'split_counts': split_counts,
        'task_counts': task_counts,
        'export_dir': str(export_dir),
        'manifest_path': str(manifest_path),
        'files': files,
    }


def start_model_training_run(
    *,
    training_run_id: int,
    trainer_mode: str | None = None,
    auto_activate: bool = False,
    serving_provider: str | None = None,
    serving_base_url: str | None = None,
    serving_model_name: str | None = None,
) -> dict[str, Any]:
    db = get_db()
    run_row = _get_training_run_row(db, training_run_id)
    if run_row is None:
        raise ValueError(f'Training run не найден: {training_run_id}')
    if str(run_row['status']) == 'completed':
        return _build_training_run_response(dict(run_row), already_started=True)
    if str(run_row['status']) == 'running':
        raise ValueError('Training run уже выполняется.')

    snapshot_export = export_training_snapshot(snapshot_id=int(run_row['snapshot_id']))
    train_params = _ensure_json_value(run_row['train_params_json'], {})
    if not isinstance(train_params, dict):
        train_params = {}

    resolved_mode = (
        (trainer_mode or '').strip().lower()
        or str(train_params.get('trainer_mode') or '').strip().lower()
        or 'manifest_only'
        or 'manifest_only'
    )
    if resolved_mode != 'manifest_only':
        raise ValueError(f'Неподдерживаемый trainer_mode: {resolved_mode}')

    resolved_serving = _resolve_serving_config(
        base_model=str(run_row['base_model']),
        provider=serving_provider or train_params.get('serving_provider'),
        base_url=serving_base_url or train_params.get('serving_base_url'),
        model_name=serving_model_name or train_params.get('serving_model_name'),
    )
    run_dir = _training_runs_dir() / f'run-{training_run_id}'
    artifact_dir = run_dir / 'artifacts'
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    now = _timestamp()
    job_ref = f'{resolved_mode}:{training_run_id}'
    db.run(
        '''
        UPDATE model_training_runs
        SET
            status = 'running',
            training_job_ref = :training_job_ref,
            started_at = coalesce(started_at, :started_at),
            notes = coalesce(notes, :notes)
        WHERE id = :id
        ''',
        {
            'id': training_run_id,
            'training_job_ref': job_ref,
            'started_at': now,
            'notes': f'trainer_mode={resolved_mode}',
        },
    )

    run_manifest = {
        'training_run_id': training_run_id,
        'snapshot_id': int(run_row['snapshot_id']),
        'snapshot_hash': str(run_row['snapshot_hash']),
        'model_family': str(run_row['model_family']),
        'base_model': str(run_row['base_model']),
        'trainer_mode': resolved_mode,
        'dataset_export': snapshot_export,
        'serving': resolved_serving,
        'created_at': now,
    }
    run_manifest_path = run_dir / 'run-manifest.json'
    run_manifest_path.write_text(json.dumps(run_manifest, ensure_ascii=False, indent=2), encoding='utf-8')

    artifact_manifest_path = artifact_dir / 'artifact-manifest.json'
    artifact_manifest_path.write_text(
        json.dumps(
            {
                'training_run_id': training_run_id,
                'mode': 'manifest_only',
                'snapshot_export': snapshot_export,
                'serving': resolved_serving,
                'artifact_dir': str(artifact_dir),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding='utf-8',
    )
    result = complete_model_training_run(
        training_run_id=training_run_id,
        artifact_uri=str(artifact_manifest_path),
        metrics={
            'trainer_mode': 'manifest_only',
            'item_count': snapshot_export['item_count'],
            'task_counts': snapshot_export['task_counts'],
            'split_counts': snapshot_export['split_counts'],
        },
        notes='Training run materialized for GigaChat workflow.',
        serving_provider=resolved_serving['provider'],
        serving_base_url=resolved_serving['base_url'],
        serving_model_name=resolved_serving['model_name'],
        auto_activate=auto_activate,
    )
    result['trainer_mode'] = 'manifest_only'
    result['run_manifest_path'] = str(run_manifest_path)
    return result


def complete_model_training_run(
    *,
    training_run_id: int,
    artifact_uri: str,
    metrics: dict[str, Any] | None = None,
    notes: str | None = None,
    serving_provider: str | None = None,
    serving_base_url: str | None = None,
    serving_model_name: str | None = None,
    auto_activate: bool = False,
) -> dict[str, Any]:
    db = get_db()
    run_row = _get_training_run_row(db, training_run_id)
    if run_row is None:
        raise ValueError(f'Training run не найден: {training_run_id}')

    train_params = _ensure_json_value(run_row['train_params_json'], {})
    if not isinstance(train_params, dict):
        train_params = {}
    resolved_serving = _resolve_serving_config(
        base_model=str(run_row['base_model']),
        provider=serving_provider or train_params.get('serving_provider'),
        base_url=serving_base_url or train_params.get('serving_base_url'),
        model_name=serving_model_name or train_params.get('serving_model_name'),
    )

    finished_at = _timestamp()
    db.run(
        '''
        UPDATE model_training_runs
        SET
            status = 'completed',
            artifact_uri = :artifact_uri,
            metrics_json = :metrics_json,
            finished_at = :finished_at,
            notes = coalesce(:notes, notes)
        WHERE id = :id
        ''',
        {
            'id': training_run_id,
            'artifact_uri': artifact_uri.strip(),
            'metrics_json': _ensure_json_text(metrics or {}, {}),
            'finished_at': finished_at,
            'notes': notes,
        },
    )

    activation = None
    if auto_activate:
        activation = activate_model_training_run(
            training_run_id=training_run_id,
            provider=resolved_serving['provider'],
            base_url=resolved_serving['base_url'],
            model_name=resolved_serving['model_name'],
            notes='Activated automatically after training completion.',
        )

    return {
        'training_run_id': training_run_id,
        'snapshot_id': int(run_row['snapshot_id']),
        'status': 'completed',
        'artifact_uri': artifact_uri.strip(),
        'metrics': metrics or {},
        'finished_at': finished_at,
        'serving': resolved_serving,
        'activated': activation is not None,
        'activation': activation,
    }


def activate_model_training_run(
    *,
    training_run_id: int,
    provider: str | None = None,
    base_url: str | None = None,
    model_name: str | None = None,
    notes: str | None = None,
) -> dict[str, Any]:
    db = get_db()
    run_row = _get_training_run_row(db, training_run_id)
    if run_row is None:
        raise ValueError(f'Training run не найден: {training_run_id}')
    if str(run_row['status']) != 'completed':
        raise ValueError('Активировать можно только завершённый training run.')

    artifact_uri = str(run_row['artifact_uri'] or '').strip()
    serving_from_artifact = _load_serving_config_from_artifact(artifact_uri)
    train_params = _ensure_json_value(run_row['train_params_json'], {})
    if not isinstance(train_params, dict):
        train_params = {}
    resolved_serving = _resolve_serving_config(
        base_model=str(run_row['base_model']),
        provider=provider or serving_from_artifact.get('provider') or train_params.get('serving_provider'),
        base_url=base_url or serving_from_artifact.get('base_url') or train_params.get('serving_base_url'),
        model_name=model_name or serving_from_artifact.get('model_name') or train_params.get('serving_model_name'),
    )

    now = _timestamp()
    with db.transaction():
        db.run(
            '''
            UPDATE model_deployments
            SET
                status = CASE WHEN status = 'active' THEN 'retired' ELSE status END,
                deactivated_at = CASE WHEN status = 'active' THEN :deactivated_at ELSE deactivated_at END
            WHERE status = 'active'
            ''',
            {'deactivated_at': now},
        )
        cursor = db.run(
            '''
            INSERT INTO model_deployments (
                training_run_id,
                snapshot_id,
                model_family,
                provider,
                base_url,
                model_name,
                artifact_uri,
                config_json,
                status,
                notes,
                created_at,
                activated_at,
                deactivated_at
            )
            VALUES (
                :training_run_id,
                :snapshot_id,
                :model_family,
                :provider,
                :base_url,
                :model_name,
                :artifact_uri,
                :config_json,
                'active',
                :notes,
                :created_at,
                :activated_at,
                NULL
            )
            ''',
            {
                'training_run_id': training_run_id,
                'snapshot_id': int(run_row['snapshot_id']),
                'model_family': str(run_row['model_family']),
                'provider': resolved_serving['provider'],
                'base_url': resolved_serving['base_url'],
                'model_name': resolved_serving['model_name'],
                'artifact_uri': artifact_uri or None,
                'config_json': _ensure_json_text(
                    {
                        'provider': resolved_serving['provider'],
                        'base_url': resolved_serving['base_url'],
                        'model_name': resolved_serving['model_name'],
                        'artifact_uri': artifact_uri or None,
                    },
                    {},
                ),
                'notes': notes,
                'created_at': now,
                'activated_at': now,
            },
        )

    return {
        'deployment_id': int(cursor.lastrowid),
        'training_run_id': training_run_id,
        'snapshot_id': int(run_row['snapshot_id']),
        'provider': resolved_serving['provider'],
        'base_url': resolved_serving['base_url'],
        'model_name': resolved_serving['model_name'],
        'artifact_uri': artifact_uri or None,
        'status': 'active',
        'activated_at': now,
    }


def get_active_model_runtime() -> dict[str, Any] | None:
    row = get_db().get(
        '''
        SELECT
            id,
            training_run_id,
            snapshot_id,
            model_family,
            provider,
            base_url,
            model_name,
            artifact_uri,
            config_json,
            status,
            notes,
            created_at,
            activated_at,
            deactivated_at
        FROM model_deployments
        WHERE status = 'active'
        ORDER BY activated_at DESC, created_at DESC, id DESC
        LIMIT 1
        '''
    )
    if row is None:
        return None

    payload = dict(row)
    payload['config'] = _ensure_json_value(payload.pop('config_json', None), {})
    return payload


def get_model_runtime_status() -> dict[str, Any]:
    active_deployment = get_active_model_runtime()
    latest_run = get_db().get(
        '''
        SELECT
            id,
            snapshot_id,
            model_family,
            base_model,
            status,
            training_job_ref,
            artifact_uri,
            created_at,
            started_at,
            finished_at
        FROM model_training_runs
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        '''
    )
    return {
        'active_deployment': active_deployment,
        'latest_training_run': dict(latest_run) if latest_run is not None else None,
        'default_runtime': {
            'provider': os.getenv('TSGEN_MODEL_PROVIDER', 'gigachat').strip().lower(),
            'base_url': os.getenv('TSGEN_MODEL_BASE_URL', 'https://gigachat.devices.sberbank.ru/api/v1').strip(),
            'model_name': os.getenv('TSGEN_MODEL_NAME', 'GigaChat-2-Pro').strip(),
        },
        'training_root': str(_training_root_dir()),
    }


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


def _load_generation_learning_context(db: DatabaseClient, generation_id: int) -> dict[str, Any] | None:
    row = db.get(
        '''
        SELECT
            g.id AS generation_id,
            g.user_id AS internal_user_id,
            v.id AS version_id,
            a.file_name,
            a.file_type,
            a.selected_sheet,
            a.parsed_file_json,
            a.mappings_json,
            a.preview_json,
            a.warnings_json,
            v.target_json,
            v.generated_typescript
        FROM generations g
        LEFT JOIN generation_versions v
            ON v.id = g.current_version_id
        LEFT JOIN generation_artifacts a
            ON a.version_id = v.id
        WHERE g.id = :generation_id
        ''',
        {'generation_id': generation_id},
    )
    if row is None:
        return None
    payload = dict(row)
    payload['schema_fingerprint_id'] = _find_schema_fingerprint_for_generation(db, generation_id)
    return payload


def _promote_confirmed_generation_learning(
    *,
    db: DatabaseClient,
    generation_id: int | None,
    schema_fingerprint_id: int | None,
    notes: str | None = None,
) -> dict[str, Any]:
    if generation_id is None:
        return {'promoted': False, 'reason': 'missing_generation_id'}

    context = _load_generation_learning_context(db, generation_id)
    if context is None:
        return {'promoted': False, 'reason': 'generation_not_found'}

    existing_example = db.get(
        '''
        SELECT id
        FROM few_shot_examples
        WHERE source_generation_id = :generation_id
          AND example_kind = 'generation'
        LIMIT 1
        ''',
        {'generation_id': generation_id},
    )
    if existing_example is not None:
        return {
            'promoted': True,
            'already_promoted': True,
            'few_shot_example_id': int(existing_example['id']),
        }

    suggestion_stats = db.get(
        '''
        SELECT
            COUNT(*) AS total_count,
            COALESCE(SUM(CASE WHEN status = 'accepted' THEN 1 ELSE 0 END), 0) AS accepted_count,
            COALESCE(SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END), 0) AS rejected_count,
            COALESCE(SUM(CASE WHEN status = 'suggested' AND source_field IS NOT NULL THEN 1 ELSE 0 END), 0) AS pending_count
        FROM mapping_suggestions
        WHERE generation_id = :generation_id
        ''',
        {'generation_id': generation_id},
    )
    has_suggestions = int(suggestion_stats['total_count'] or 0) > 0 if suggestion_stats is not None else False
    if has_suggestions:
        if int(suggestion_stats['rejected_count'] or 0) > 0:
            return {'promoted': False, 'reason': 'has_rejected_suggestions'}
        if int(suggestion_stats['pending_count'] or 0) > 0:
            return {'promoted': False, 'reason': 'has_pending_suggestions'}
    elif not _mappings_are_confirmed(str(context['mappings_json'] or '[]')):
        return {'promoted': False, 'reason': 'generation_not_confirmed'}

    resolved_schema_fingerprint_id = schema_fingerprint_id or _find_schema_fingerprint_for_generation(db, generation_id)
    _upsert_mapping_memory_entries(
        db=db,
        user_id=int(context['internal_user_id']),
        generation_id=generation_id,
        schema_fingerprint_id=resolved_schema_fingerprint_id,
        mappings_json=str(context['mappings_json'] or '[]'),
        source_of_truth='accepted_generation',
    )

    quality_score = _estimate_generation_quality(str(context['mappings_json'] or '[]'), str(context['warnings_json'] or '[]'))
    example_id = _save_few_shot_example(
        db=db,
        user_id=int(context['internal_user_id']),
        schema_fingerprint_id=resolved_schema_fingerprint_id,
        generation_id=generation_id,
        version_id=int(context['version_id']),
        file_name=str(context['file_name'] or 'generation'),
        file_type=str(context['file_type'] or 'unknown'),
        parsed_file_json=str(context['parsed_file_json'] or _ensure_json_text(_build_fallback_parsed_file('generation', 'unknown'), {})),
        target_json=str(context['target_json'] or '{}'),
        mappings_json=str(context['mappings_json'] or '[]'),
        warnings_json=str(context['warnings_json'] or '[]'),
        generated_typescript=str(context['generated_typescript'] or ''),
        selected_sheet=context['selected_sheet'],
        source_columns=_extract_source_columns(
            _ensure_json_value(
                str(context['parsed_file_json'] or _ensure_json_text(_build_fallback_parsed_file('generation', 'unknown'), {})),
                _build_fallback_parsed_file('generation', 'unknown'),
            ),
            context['selected_sheet'],
        ),
        quality_score=quality_score,
    )
    dataset_item_id = _create_curated_dataset_candidate(
        db=db,
        source_entity_type='few_shot_example',
        source_entity_id=example_id,
        item_kind='example',
        input_payload={
            'generation_id': generation_id,
            'notes': notes,
        },
        target_payload={
            'target_json': _ensure_json_value(str(context['target_json'] or '{}'), {}),
            'generated_typescript': str(context['generated_typescript'] or ''),
        },
        quality_score=quality_score,
    )
    return {
        'promoted': True,
        'already_promoted': False,
        'few_shot_example_id': example_id,
        'dataset_item_id': dataset_item_id,
        'quality_score': quality_score,
    }


def _register_mapping_feedback_outcome(
    *,
    db: DatabaseClient,
    user_id: int,
    schema_fingerprint_id: int | None,
    source_field: str | None,
    target_field: str | None,
    accepted: bool,
) -> None:
    if not source_field or not target_field:
        return

    normalized_source = _normalize_field_name(source_field)
    normalized_target = _normalize_field_name(target_field)
    if not normalized_source or not normalized_target:
        return

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

    if existing is not None:
        db.run(
            '''
            UPDATE mapping_memory
            SET
                usage_count = usage_count + 1,
                success_count = success_count + :success_increment,
                failure_count = failure_count + :failure_increment,
                last_used_at = :last_used_at,
                updated_at = :updated_at
            WHERE id = :id
            ''',
            {
                'id': int(existing['id']),
                'success_increment': 1 if accepted else 0,
                'failure_increment': 0 if accepted else 1,
                'last_used_at': _timestamp(),
                'updated_at': _timestamp(),
            },
        )

    _upsert_pattern_candidate(
        db=db,
        schema_fingerprint_id=schema_fingerprint_id,
        schema_hash=_get_schema_fingerprint_hash(db, schema_fingerprint_id),
        source_field=source_field,
        target_field=target_field,
        source_field_normalized=normalized_source,
        target_field_normalized=normalized_target,
        confidence=1.0 if accepted else 0.0,
    )


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
    correction_stats = db.get(
        '''
        SELECT
            COUNT(*) AS rows_count,
            COUNT(DISTINCT user_id) AS distinct_users,
            COALESCE(SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END), 0) AS success_total,
            COALESCE(SUM(CASE WHEN accepted = 0 THEN 1 ELSE 0 END), 0) AS failure_total,
            AVG(CASE WHEN accepted = 1 THEN confidence_after END) AS average_confidence
        FROM user_corrections
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
    memory_stats = db.get(
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
    if memory_stats is None:
        return

    correction_count = int(correction_stats['rows_count'] or 0) if correction_stats is not None else 0
    if correction_count > 0:
        usage_total = correction_count
        success_total = int(correction_stats['success_total'] or 0)
        failure_total = int(correction_stats['failure_total'] or 0)
        distinct_users = int(correction_stats['distinct_users'] or 0)
        average_confidence = correction_stats['average_confidence']
        stats_source = 'user_corrections'
        rows_count = correction_count
    else:
        usage_total = int(memory_stats['usage_total'] or 0)
        success_total = int(memory_stats['success_total'] or 0)
        failure_total = int(memory_stats['failure_total'] or 0)
        distinct_users = int(memory_stats['distinct_users'] or 0)
        average_confidence = memory_stats['average_confidence']
        stats_source = 'mapping_memory'
        rows_count = int(memory_stats['rows_count'] or 0)

    total_feedback = success_total + failure_total
    stability_score = (success_total / total_feedback) if total_feedback else None
    drift_score = (failure_total / total_feedback) if total_feedback else None
    now = _timestamp()
    candidate_row = db.get('SELECT status FROM pattern_candidates WHERE id = :id', {'id': candidate_id})
    current_status = str(candidate_row['status']) if candidate_row is not None else 'new'
    next_status = current_status
    if current_status not in {'accepted', 'promoted', 'rejected'} and usage_total > 0:
        next_status = 'reviewing'

    db.run(
        '''
        UPDATE pattern_candidates
        SET
            support_count = :support_count,
            distinct_users_count = :distinct_users_count,
            mean_confidence = :mean_confidence,
            status = :status,
            updated_at = :updated_at
        WHERE id = :id
        ''',
        {
            'id': candidate_id,
            'support_count': usage_total,
            'distinct_users_count': distinct_users,
            'mean_confidence': average_confidence,
            'status': next_status,
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
        'stats_json': _ensure_json_text({'rows_count': rows_count, 'source': stats_source}, {}),
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


def _mappings_are_confirmed(mappings_json: str) -> bool:
    mappings = _ensure_json_value(mappings_json, [])
    if not isinstance(mappings, list) or not mappings:
        return False

    for mapping in mappings:
        if not isinstance(mapping, dict):
            return False
        if not mapping.get('source') or not mapping.get('target'):
            return False
        if str(mapping.get('status') or 'accepted') != 'accepted':
            return False
        if str(mapping.get('source_of_truth') or 'deterministic_rule') in {'model_suggestion', 'position_fallback', 'unresolved'}:
            return False
    return True


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


def _build_in_clause_params(prefix: str, values: list[str], params: dict[str, Any]) -> str:
    placeholders: list[str] = []
    for index, value in enumerate(values):
        key = f'{prefix}_{index}'
        params[key] = value
        placeholders.append(f':{key}')
    return ', '.join(placeholders) if placeholders else "''"


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


def _get_training_run_row(db: DatabaseClient, training_run_id: int) -> dict[str, Any] | None:
    row = db.get(
        '''
        SELECT
            r.id,
            r.snapshot_id,
            r.model_family,
            r.base_model,
            r.training_job_ref,
            r.status,
            r.train_params_json,
            r.metrics_json,
            r.artifact_uri,
            r.started_at,
            r.finished_at,
            r.notes,
            r.created_at,
            s.name AS snapshot_name,
            s.snapshot_hash,
            s.item_count,
            s.train_count,
            s.validation_count,
            s.test_count
        FROM model_training_runs r
        INNER JOIN training_snapshots s
            ON s.id = r.snapshot_id
        WHERE r.id = :id
        ''',
        {'id': training_run_id},
    )
    return dict(row) if row is not None else None


def _build_training_run_response(run_row: dict[str, Any], *, already_started: bool = False) -> dict[str, Any]:
    return {
        'training_run_id': int(run_row['id']),
        'snapshot_id': int(run_row['snapshot_id']),
        'snapshot_name': str(run_row['snapshot_name']),
        'snapshot_hash': str(run_row['snapshot_hash']),
        'item_count': int(run_row['item_count']),
        'train_count': int(run_row['train_count']),
        'validation_count': int(run_row['validation_count']),
        'test_count': int(run_row['test_count']),
        'model_family': str(run_row['model_family']),
        'base_model': str(run_row['base_model']),
        'status': str(run_row['status']),
        'training_job_ref': run_row['training_job_ref'],
        'artifact_uri': run_row['artifact_uri'],
        'started_at': run_row['started_at'],
        'finished_at': run_row['finished_at'],
        'created_at': run_row['created_at'],
        'already_started': already_started,
    }


def _build_training_example_from_dataset_row(row: dict[str, Any]) -> dict[str, Any]:
    input_payload = _ensure_json_value(row.get('input_payload_json'), {})
    target_payload = _ensure_json_value(row.get('target_payload_json'), {})
    context_payload = _ensure_json_value(row.get('context_json'), {})
    if not isinstance(input_payload, dict):
        input_payload = {'raw_input': input_payload}
    if not isinstance(target_payload, dict):
        target_payload = {'raw_target': target_payload}
    if not isinstance(context_payload, dict):
        context_payload = {'raw_context': context_payload}

    task_type = _infer_training_task_type(str(row.get('item_kind') or ''), input_payload, target_payload)
    return {
        'dataset_item_id': int(row['id']),
        'task_type': task_type,
        'source_entity_type': str(row['source_entity_type']),
        'source_entity_id': int(row['source_entity_id']),
        'item_kind': str(row['item_kind']),
        'split': str(row['split']),
        'quality_score': float(row['quality_score']) if row['quality_score'] is not None else None,
        'input_payload': input_payload,
        'target_payload': target_payload,
        'context': context_payload,
        'messages': _build_training_messages(task_type=task_type, input_payload=input_payload, target_payload=target_payload),
        'created_at': row['created_at'],
    }


def _infer_training_task_type(item_kind: str, input_payload: dict[str, Any], target_payload: dict[str, Any]) -> str:
    if 'draft_json' in target_payload or item_kind == 'template':
        return 'draft_json'
    if 'generated_typescript' in target_payload or item_kind in {'pattern', 'correction', 'example', 'mapping'}:
        return 'mapping'
    return 'mapping'


def _build_training_messages(*, task_type: str, input_payload: dict[str, Any], target_payload: dict[str, Any]) -> list[dict[str, str]]:
    if task_type == 'draft_json':
        system_prompt = (
            'Generate a canonical draft JSON schema from table columns and user-confirmed naming patterns. '
            'Return only JSON.'
        )
        assistant_payload: Any = target_payload.get('draft_json', target_payload)
    else:
        system_prompt = (
            'Resolve semantic mappings between source table fields and target JSON fields using domain-consistent synonyms. '
            'Return only JSON.'
        )
        assistant_payload = target_payload

    return [
        {'role': 'system', 'content': system_prompt},
        {'role': 'user', 'content': _canonical_json_text(input_payload)},
        {'role': 'assistant', 'content': _canonical_json_text(assistant_payload)},
    ]


def _resolve_serving_config(
    *,
    base_model: str,
    provider: Any = None,
    base_url: Any = None,
    model_name: Any = None,
) -> dict[str, str]:
    resolved_provider = str(provider or os.getenv('TSGEN_MODEL_PROVIDER', 'gigachat')).strip().lower() or 'gigachat'
    if resolved_provider != 'gigachat':
        resolved_provider = 'gigachat'
    default_base_url = 'https://gigachat.devices.sberbank.ru/api/v1'
    resolved_base_url = str(base_url or os.getenv('TSGEN_MODEL_BASE_URL', default_base_url)).strip()
    resolved_model_name = str(model_name or base_model or os.getenv('TSGEN_MODEL_NAME', 'GigaChat-2-Pro')).strip()
    return {
        'provider': resolved_provider,
        'base_url': resolved_base_url,
        'model_name': resolved_model_name,
    }


def _load_serving_config_from_artifact(artifact_uri: str | None) -> dict[str, Any]:
    if not artifact_uri:
        return {}
    try:
        artifact_path = Path(artifact_uri)
        if not artifact_path.exists() or not artifact_path.is_file():
            return {}
        payload = json.loads(artifact_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    serving = payload.get('serving')
    return serving if isinstance(serving, dict) else {}


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
    try:
        from matcher import normalize as matcher_normalize

        normalized = matcher_normalize(value)
        if normalized:
            return normalized
    except Exception:
        pass
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
