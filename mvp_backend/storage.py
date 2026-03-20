from __future__ import annotations

import json
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
    path = target_dir / safe_name
    path.write_bytes(content)
    return path


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
    parsed_file_json: str,
    selected_sheet: str | None = None,
) -> int:
    db = get_db()
    internal_user_id = ensure_user_record(external_id=user_id)
    title = file_name if not selected_sheet else f'{file_name} [{selected_sheet}]'
    source_payload = _build_source_payload(parsed_file_json, file_name, file_type, selected_sheet)

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
                'target_json': target_json,
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
                'parsed_file_json': _ensure_json_text(parsed_file_json, _build_fallback_parsed_file(file_name, file_type)),
                'mappings_json': _ensure_json_text(mappings_json, []),
                'preview_json': _ensure_json_text(preview_json, []),
                'warnings_json': _ensure_json_text(warnings_json, []),
            },
        )

        _upsert_mapping_cache_entries(
            db=db,
            user_id=internal_user_id,
            generation_id=generation_id,
            mappings_json=mappings_json,
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
