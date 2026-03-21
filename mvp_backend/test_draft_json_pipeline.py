from __future__ import annotations

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

        def copy(self, update=None):
            payload = dict(self.__dict__)
            payload.update(update or {})
            return self.__class__(**payload)

        def model_copy(self, update=None):
            return self.copy(update=update)

    pydantic_stub.BaseModel = BaseModel
    pydantic_stub.Field = Field
    sys.modules['pydantic'] = pydantic_stub

import storage
from draft_json_pipeline import generate_draft_json_for_source


class DraftJsonPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = BACKEND_DIR / '.test_runtime_draft' / str(uuid.uuid4())
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

        storage._db_client = None
        storage.init_db()

    def tearDown(self) -> None:
        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None

    def test_personal_memory_is_used_for_draft_json_field_names(self) -> None:
        storage.save_correction_session(
            user_id='draft-user',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'ФИО клиента',
                    'target_field': 'customerName',
                    'corrected_value': {'source': 'ФИО клиента', 'target': 'customerName'},
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
        )

        with patch('draft_json_pipeline.suggest_draft_json_fields', return_value=([], [])):
            draft_json, field_suggestions, warnings = generate_draft_json_for_source(
                source_columns=['ФИО клиента', 'Сумма руб'],
                source_rows=[{'ФИО клиента': 'Иванов Иван', 'Сумма руб': '120000'}],
                user_id='draft-user',
            )

        self.assertIn('customerName', draft_json)
        customer_name_field = next(item for item in field_suggestions if item['source_column'] == 'ФИО клиента')
        self.assertEqual(customer_name_field['source_of_truth'], 'personal_memory')
        self.assertEqual(customer_name_field['status'], 'accepted')
        self.assertEqual(warnings, [])


if __name__ == '__main__':
    unittest.main()
