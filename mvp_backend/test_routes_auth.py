from __future__ import annotations

import json
import shutil
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    import pydantic  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    pydantic_stub = types.ModuleType('pydantic')

    class _FieldInfo:
        def __init__(self, default=None, default_factory=None):
            self.default = default
            self.default_factory = default_factory

    def Field(default=None, default_factory=None):
        return _FieldInfo(default=default, default_factory=default_factory)

    class BaseModel:
        def __init__(self, **data):
            annotations = {}
            for cls in reversed(self.__class__.mro()):
                annotations.update(getattr(cls, '__annotations__', {}))
            for key in annotations:
                if key in data:
                    value = data[key]
                else:
                    class_value = getattr(self.__class__, key, None)
                    if isinstance(class_value, _FieldInfo):
                        if class_value.default_factory is not None:
                            value = class_value.default_factory()
                        else:
                            value = class_value.default
                    else:
                        value = class_value
                setattr(self, key, value)

        def dict(self):
            return dict(self.__dict__)

        def model_dump(self):
            return dict(self.__dict__)

    pydantic_stub.BaseModel = BaseModel
    pydantic_stub.Field = Field
    sys.modules['pydantic'] = pydantic_stub

from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

import auth_session
import routes
import storage
from models import CorrectionSessionPayload, UpdateProfilePayload


class AuthRouteSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = BACKEND_DIR / '.test_runtime' / str(uuid.uuid4())
        root = self.test_root
        runtime_dir = root / '.runtime'
        base_dir = runtime_dir / 'storage'
        upload_dir = base_dir / 'uploads'
        guest_dir = upload_dir / 'guest'
        auth_dir = upload_dir / 'authorized'

        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None

        self.storage_patcher = patch.multiple(
            storage,
            PROJECT_DIR=root,
            RUNTIME_DIR=runtime_dir,
            DB_PATH=runtime_dir / 'app.sqlite',
            LEGACY_DB_PATH=runtime_dir / 'history.db',
            BASE_DIR=base_dir,
            UPLOAD_DIR=upload_dir,
            GUEST_DIR=guest_dir,
            AUTH_DIR=auth_dir,
        )
        self.storage_patcher.start()
        self.addCleanup(self.storage_patcher.stop)
        self.addCleanup(lambda: shutil.rmtree(self.test_root, ignore_errors=True))

        self.auth_secret_patcher = patch.object(auth_session, 'AUTH_SECRET', 'test-auth-secret')
        self.auth_secret_patcher.start()
        self.addCleanup(self.auth_secret_patcher.stop)
        auth_session._user_cache.clear()
        self.addCleanup(auth_session._user_cache.clear)

        storage._db_client = None
        storage.init_db()

    def tearDown(self) -> None:
        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None

    def _current_user(self, user_id: str) -> dict[str, str]:
        return storage.get_user_profile(user_id)

    def _create_generation_for_user(self, user_id: str, file_name: str) -> int:
        file_bytes = b'customerName,amount\nAlice,10\n'
        saved_path = storage.save_upload(file_bytes, file_name, mode='authorized', user_id=user_id)
        upload_id = storage.record_uploaded_file(
            file_path=saved_path,
            original_file_name=file_name,
            file_bytes=file_bytes,
            mode='authorized',
            user_id=user_id,
        )
        return storage.save_generation(
            user_id=user_id,
            file_name=file_name,
            file_path=str(saved_path),
            file_type='csv',
            target_json=json.dumps({'customerName': '', 'amount': 0}, ensure_ascii=False),
            mappings_json=json.dumps(
                [
                    {'source': 'customerName', 'target': 'customerName', 'confidence': 'high', 'reason': 'exact'},
                    {'source': 'amount', 'target': 'amount', 'confidence': 'high', 'reason': 'exact'},
                ],
                ensure_ascii=False,
            ),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([{'customerName': 'Alice', 'amount': 10}], ensure_ascii=False),
            warnings_json=json.dumps([], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': file_name,
                    'file_type': 'csv',
                    'columns': ['customerName', 'amount'],
                    'rows': [{'customerName': 'Alice', 'amount': 10}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['customerName', 'amount'],
            upload_record_id=upload_id,
        )

    def test_get_current_user_requires_valid_bearer_token(self) -> None:
        with self.assertRaises(HTTPException) as missing_token:
            auth_session.get_current_user(None)
        self.assertEqual(missing_token.exception.status_code, 401)

        user = storage.register_user(name='Alice', email='alice@example.com', password='password123')
        credentials = HTTPAuthorizationCredentials(scheme='Bearer', credentials=auth_session.create_access_token(user['id']))
        current_user = auth_session.get_current_user(credentials)
        self.assertEqual(current_user['id'], user['id'])
        self.assertEqual(current_user['email'], user['email'])

    def test_profile_routes_ignore_spoofed_user_id(self) -> None:
        user_one = storage.register_user(name='Alice', email='alice.profile@example.com', password='password123')
        user_two = storage.register_user(name='Bob', email='bob.profile@example.com', password='password123')

        payload = UpdateProfilePayload(name='Alice Renamed', user_id=user_two['id'])
        updated = routes.update_profile(payload, current_user=self._current_user(user_one['id']))

        self.assertEqual(updated['id'], user_one['id'])
        self.assertEqual(updated['name'], 'Alice Renamed')
        self.assertEqual(storage.get_user_profile(user_one['id'])['name'], 'Alice Renamed')
        self.assertEqual(storage.get_user_profile(user_two['id'])['name'], 'Bob')

    def test_history_routes_are_scoped_to_authenticated_user(self) -> None:
        user_one = storage.register_user(name='Alice', email='alice.history@example.com', password='password123')
        user_two = storage.register_user(name='Bob', email='bob.history@example.com', password='password123')
        generation_one = self._create_generation_for_user(user_one['id'], 'alice.csv')
        generation_two = self._create_generation_for_user(user_two['id'], 'bob.csv')

        user_one_history = routes.history(current_user=self._current_user(user_one['id']))
        self.assertEqual(len(user_one_history['items']), 1)
        self.assertEqual(user_one_history['items'][0]['id'], str(generation_one))
        self.assertEqual(user_one_history['items'][0]['file_name'], 'alice.csv')

        with self.assertRaises(HTTPException) as foreign_delete:
            routes.delete_history_entry(generation_two, current_user=self._current_user(user_one['id']))
        self.assertEqual(foreign_delete.exception.status_code, 404)

        user_two_history = routes.history(current_user=self._current_user(user_two['id']))
        self.assertEqual(len(user_two_history['items']), 1)
        self.assertEqual(user_two_history['items'][0]['id'], str(generation_two))
        self.assertEqual(user_two_history['items'][0]['file_name'], 'bob.csv')

    def test_learning_routes_ignore_spoofed_user_id_and_stay_user_scoped(self) -> None:
        user_one = storage.register_user(name='Alice', email='alice.learning@example.com', password='password123')
        user_two = storage.register_user(name='Bob', email='bob.learning@example.com', password='password123')

        payload = CorrectionSessionPayload(
            user_id=user_two['id'],
            session_type='manual_review',
            notes='spoofed learning write',
            corrections=[],
        )
        result = routes.learning_save_corrections(payload, current_user=self._current_user(user_one['id']))

        self.assertIsNotNone(result['session_id'])

        summary_one = routes.learning_summary(current_user=self._current_user(user_one['id']))
        summary_two = routes.learning_summary(current_user=self._current_user(user_two['id']))
        memory_one = routes.learning_memory(current_user=self._current_user(user_one['id']))
        memory_two = routes.learning_memory(current_user=self._current_user(user_two['id']))

        self.assertEqual(summary_one['correction_sessions'], 1)
        self.assertEqual(summary_two['correction_sessions'], 0)
        self.assertEqual(memory_one['user_id'], user_one['id'])
        self.assertEqual(memory_two['user_id'], user_two['id'])

    def test_auth_profile_and_learning_events_are_user_scoped(self) -> None:
        user_one = storage.register_user(name='Alice', email='alice.events@example.com', password='password123')
        user_two = storage.register_user(name='Bob', email='bob.events@example.com', password='password123')

        routes.learning_save_corrections(
            CorrectionSessionPayload(session_type='manual_review', notes='alice event', corrections=[]),
            current_user=self._current_user(user_one['id']),
        )
        routes.learning_save_corrections(
            CorrectionSessionPayload(session_type='manual_review', notes='bob event', corrections=[]),
            current_user=self._current_user(user_two['id']),
        )

        profile_one = routes.auth_profile(current_user=self._current_user(user_one['id']))
        profile_two = routes.auth_profile(current_user=self._current_user(user_two['id']))
        events_one = routes.learning_events(current_user=self._current_user(user_one['id']))
        events_two = routes.learning_events(current_user=self._current_user(user_two['id']))

        self.assertEqual(profile_one['id'], user_one['id'])
        self.assertEqual(profile_one['email'], user_one['email'])
        self.assertEqual(profile_two['id'], user_two['id'])
        self.assertEqual(profile_two['email'], user_two['email'])
        self.assertEqual(len(events_one['items']), 1)
        self.assertEqual(len(events_two['items']), 1)
        self.assertEqual(events_one['items'][0]['metadata']['notes'], 'alice event')
        self.assertEqual(events_two['items'][0]['metadata']['notes'], 'bob event')


if __name__ == '__main__':
    unittest.main()
