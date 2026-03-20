from __future__ import annotations

import json
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import storage


class StorageLearningTests(unittest.TestCase):
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

        storage._db_client = None
        storage.init_db()

    def tearDown(self) -> None:
        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None

    def test_save_generation_populates_learning_tables(self) -> None:
        file_bytes = b'customerName,amount\nAlice,10\n'
        saved_path = storage.save_upload(file_bytes, 'example.csv', mode='authorized', user_id='user-1')
        upload_id = storage.record_uploaded_file(
            file_path=saved_path,
            original_file_name='example.csv',
            file_bytes=file_bytes,
            mode='authorized',
            user_id='user-1',
        )

        generation_id = storage.save_generation(
            user_id='user-1',
            file_name='example.csv',
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
                    'file_name': 'example.csv',
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

        db = storage.get_db()
        upload_row = db.get(
            'SELECT generation_id, status, schema_fingerprint_id FROM uploaded_files WHERE id = :id',
            {'id': upload_id},
        )

        self.assertEqual(int(upload_row['generation_id']), generation_id)
        self.assertEqual(str(upload_row['status']), 'processed')
        self.assertIsNotNone(upload_row['schema_fingerprint_id'])
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM schema_fingerprints'), 1)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_memory'), 2)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM few_shot_examples'), 1)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM frequent_djson'), 1)
        self.assertGreaterEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM feature_vectors'), 2)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM curated_dataset_items'), 1)

    def test_templates_corrections_and_summary_are_persisted(self) -> None:
        template = storage.save_user_template(
            user_id='user-2',
            name='Invoice Mapping',
            template_kind='mapping',
            template_json={'invoiceId': '', 'amount': 0},
            target_json={'invoiceId': '', 'amount': 0},
            generated_typescript='export const template = true;',
            metadata={'source': 'unit-test'},
        )
        self.assertEqual(template['name'], 'Invoice Mapping')

        correction_result = storage.save_correction_session(
            user_id='user-2',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'Invoice Number',
                    'target_field': 'invoiceId',
                    'original_value': None,
                    'corrected_value': {'source': 'Invoice Number', 'target': 'invoiceId'},
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
            notes='manual override',
        )

        summary = storage.get_learning_summary('user-2')
        self.assertEqual(summary['user_templates'], 1)
        self.assertEqual(summary['correction_sessions'], 1)
        self.assertEqual(summary['user_corrections'], 1)
        self.assertEqual(summary['mapping_memory'], 1)
        self.assertEqual(correction_result['accepted_count'], 1)

    def test_repeated_uploads_use_distinct_storage_paths(self) -> None:
        file_bytes = b'col_a,col_b\n1,2\n'

        first_path = storage.save_upload(file_bytes, 'same-name.xlsx', mode='guest')
        second_path = storage.save_upload(file_bytes, 'same-name.xlsx', mode='guest')

        self.assertNotEqual(first_path, second_path)
        self.assertEqual(first_path.name.endswith('.xlsx'), True)
        self.assertEqual(second_path.name.endswith('.xlsx'), True)

        first_id = storage.record_uploaded_file(
            file_path=first_path,
            original_file_name='same-name.xlsx',
            file_bytes=file_bytes,
            mode='guest',
        )
        second_id = storage.record_uploaded_file(
            file_path=second_path,
            original_file_name='same-name.xlsx',
            file_bytes=file_bytes,
            mode='guest',
        )

        self.assertNotEqual(first_id, second_id)


if __name__ == '__main__':
    unittest.main()
