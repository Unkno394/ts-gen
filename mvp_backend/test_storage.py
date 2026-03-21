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

    def test_learning_memory_layers_return_aggregated_stats(self) -> None:
        generation_id = storage.save_generation(
            user_id='memory-user',
            file_name='crm.csv',
            file_path='/tmp/crm.csv',
            file_type='csv',
            target_json=json.dumps({'customerName': '', 'dealRevenueAmount': 0, 'creator': ''}, ensure_ascii=False),
            mappings_json=json.dumps([], ensure_ascii=False),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([], ensure_ascii=False),
            warnings_json=json.dumps([], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': 'crm.csv',
                    'file_type': 'csv',
                    'columns': ['ФИО клиента', 'Сделка - Сумма выручки', 'Создатель'],
                    'rows': [{'ФИО клиента': 'Alice', 'Сделка - Сумма выручки': 1200, 'Создатель': 'Bob'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['ФИО клиента', 'Сделка - Сумма выручки', 'Создатель'],
            promotion_mode='confirmed_only',
        )

        suggestions = storage.save_mapping_suggestions(
            generation_id=generation_id,
            user_id='memory-user',
            mappings=[
                {
                    'source': 'ФИО клиента',
                    'target': 'customerName',
                    'confidence': 'medium',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                },
                {
                    'source': 'Сделка - Сумма выручки',
                    'target': 'dealRevenueAmount',
                    'confidence': 'medium',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                },
                {
                    'source': 'Создатель',
                    'target': 'creator',
                    'confidence': 'low',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                },
            ],
        )

        storage.apply_mapping_feedback(
            user_id='memory-user',
            generation_id=generation_id,
            feedback=[
                {
                    'suggestion_id': suggestions[0]['suggestion_id'],
                    'target_field': 'customerName',
                    'status': 'accepted',
                    'confidence_after': 1.0,
                },
                {
                    'suggestion_id': suggestions[1]['suggestion_id'],
                    'target_field': 'dealRevenueAmount',
                    'status': 'rejected',
                    'confidence_after': 0.0,
                },
            ],
        )

        layers = storage.get_learning_memory_layers('memory-user', limit=10)
        staging = layers['layers']['staging']
        personal = layers['layers']['personal_memory']
        global_knowledge = layers['layers']['global_knowledge']

        self.assertEqual(staging['counts']['pending'], 1)
        self.assertEqual(staging['counts']['rejected'], 1)
        self.assertEqual(staging['counts']['total'], 2)
        self.assertTrue(any(item['status'] == 'suggested' for item in staging['items']))
        self.assertTrue(any(item['status'] == 'rejected' for item in staging['items']))

        self.assertGreaterEqual(personal['counts']['entries'], 1)
        self.assertGreaterEqual(personal['counts']['accepted'], 1)
        self.assertTrue(any(item['target_field'] == 'customerName' for item in personal['items']))

        self.assertGreaterEqual(global_knowledge['counts']['patterns'], 1)
        self.assertTrue(any(item['target_field_norm'] == 'customername' for item in global_knowledge['items']))

    def test_confirmed_only_generation_requires_feedback_before_memory_promotion(self) -> None:
        generation_id = storage.save_generation(
            user_id='user-confirmed',
            file_name='customers.csv',
            file_path='/tmp/customers.csv',
            file_type='csv',
            target_json=json.dumps({'customerName': ''}, ensure_ascii=False),
            mappings_json=json.dumps(
                [
                    {
                        'source': 'ФИО клиента',
                        'target': 'customerName',
                        'confidence': 'low',
                        'reason': 'model_suggestion',
                        'status': 'suggested',
                        'source_of_truth': 'model_suggestion',
                    }
                ],
                ensure_ascii=False,
            ),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([{'customerName': 'Alice'}], ensure_ascii=False),
            warnings_json=json.dumps(['check model mapping'], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': 'customers.csv',
                    'file_type': 'csv',
                    'columns': ['ФИО клиента'],
                    'rows': [{'ФИО клиента': 'Alice'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['ФИО клиента'],
            promotion_mode='confirmed_only',
        )

        db = storage.get_db()
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_memory'), 0)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM curated_dataset_items'), 0)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM few_shot_examples'), 0)

        suggestions = storage.save_mapping_suggestions(
            generation_id=generation_id,
            mappings=[
                {
                    'source': 'ФИО клиента',
                    'target': 'customerName',
                    'confidence': 'low',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                }
            ],
            user_id='user-confirmed',
        )

        feedback_result = storage.apply_mapping_feedback(
            user_id='user-confirmed',
            generation_id=generation_id,
            feedback=[
                {
                    'suggestion_id': suggestions[0]['suggestion_id'],
                    'target_field': 'customerName',
                    'status': 'accepted',
                    'confidence_after': 1.0,
                }
            ],
        )

        self.assertEqual(feedback_result['accepted_count'], 1)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_memory'), 1)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM mapping_suggestions WHERE status = "accepted"'), 1)
        self.assertTrue(feedback_result['promotion']['promoted'])
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM few_shot_examples'), 1)

    def test_confirm_generation_endpoint_promotes_generation_once(self) -> None:
        generation_id = storage.save_generation(
            user_id='user-confirm-generation',
            file_name='customers.csv',
            file_path='/tmp/customers.csv',
            file_type='csv',
            target_json=json.dumps({'customerName': ''}, ensure_ascii=False),
            mappings_json=json.dumps(
                [
                    {
                        'source': 'ФИО клиента',
                        'target': 'customerName',
                        'confidence': 'low',
                        'reason': 'model_suggestion',
                        'status': 'suggested',
                        'source_of_truth': 'model_suggestion',
                    }
                ],
                ensure_ascii=False,
            ),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([{'customerName': 'Alice'}], ensure_ascii=False),
            warnings_json=json.dumps([], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': 'customers.csv',
                    'file_type': 'csv',
                    'columns': ['ФИО клиента'],
                    'rows': [{'ФИО клиента': 'Alice'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['ФИО клиента'],
            promotion_mode='confirmed_only',
        )
        storage.save_mapping_suggestions(
            generation_id=generation_id,
            mappings=[
                {
                    'source': 'ФИО клиента',
                    'target': 'customerName',
                    'confidence': 'low',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                }
            ],
            user_id='user-confirm-generation',
        )

        result = storage.confirm_generation_learning(
            user_id='user-confirm-generation',
            generation_id=generation_id,
        )
        self.assertTrue(result['promoted'])

        second_result = storage.confirm_generation_learning(
            user_id='user-confirm-generation',
            generation_id=generation_id,
        )
        self.assertTrue(second_result['promoted'])
        self.assertTrue(second_result['already_promoted'])

    def test_save_mapping_suggestions_allows_global_pattern_source(self) -> None:
        generation_id = storage.save_generation(
            user_id='user-global-pattern',
            file_name='customers.csv',
            file_path='/tmp/customers.csv',
            file_type='csv',
            target_json=json.dumps({'customerName': ''}, ensure_ascii=False),
            mappings_json=json.dumps([], ensure_ascii=False),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([], ensure_ascii=False),
            warnings_json=json.dumps([], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': 'customers.csv',
                    'file_type': 'csv',
                    'columns': ['ФИО клиента'],
                    'rows': [{'ФИО клиента': 'Alice'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['ФИО клиента'],
            promotion_mode='confirmed_only',
        )

        suggestions = storage.save_mapping_suggestions(
            generation_id=generation_id,
            mappings=[
                {
                    'source': 'ФИО клиента',
                    'target': 'customerName',
                    'confidence': 'medium',
                    'reason': 'global_pattern_backfill',
                    'status': 'suggested',
                    'source_of_truth': 'global_pattern',
                }
            ],
            user_id='user-global-pattern',
        )

        self.assertEqual(len(suggestions), 1)
        row = storage.get_db().get(
            'SELECT source_of_truth FROM mapping_suggestions WHERE id = :id',
            {'id': suggestions[0]['suggestion_id']},
        )
        self.assertEqual(str(row['source_of_truth']), 'global_pattern')

    def test_draft_json_feedback_saves_template_and_frequent_schema(self) -> None:
        schema_fingerprint_id = storage.ensure_schema_fingerprint(
            user_id='draft-feedback-user',
            parsed_file_json=json.dumps(
                {
                    'file_name': 'draft.csv',
                    'file_type': 'csv',
                    'columns': ['ФИО клиента', 'Сумма руб'],
                    'rows': [{'ФИО клиента': 'Иванов Иван', 'Сумма руб': '100'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            target_json=json.dumps({}, ensure_ascii=False),
            source_columns=['ФИО клиента', 'Сумма руб'],
        )
        suggestions = storage.save_draft_json_suggestions(
            user_id='draft-feedback-user',
            schema_fingerprint_id=schema_fingerprint_id,
            suggestions=[
                {
                    'source_column': 'ФИО клиента',
                    'target_field': 'customerName',
                    'default_value': '',
                    'field_type': 'string',
                    'confidence': 'medium',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                    'reason': 'semantic_model',
                },
                {
                    'source_column': 'Сумма руб',
                    'target_field': 'amount',
                    'default_value': 0,
                    'field_type': 'number',
                    'confidence': 'medium',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                    'reason': 'semantic_model',
                },
            ],
        )
        result = storage.apply_draft_json_feedback(
            user_id='draft-feedback-user',
            schema_fingerprint_id=schema_fingerprint_id,
            draft_json={'customerName': '', 'amount': 0},
            template_name='Customer Draft',
            feedback=[
                {
                    'suggestion_id': suggestions[0]['suggestion_id'],
                    'source_column': 'ФИО клиента',
                    'suggested_field': 'customerName',
                    'status': 'accepted',
                },
                {
                    'suggestion_id': suggestions[1]['suggestion_id'],
                    'source_column': 'Сумма руб',
                    'suggested_field': 'amount',
                    'status': 'accepted',
                },
            ],
        )

        db = storage.get_db()
        self.assertEqual(result['accepted_count'], 2)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM draft_json_suggestions WHERE status = "accepted"'), 2)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM user_templates'), 1)
        self.assertEqual(storage._count_rows(db, 'SELECT COUNT(*) AS value FROM frequent_djson'), 1)

    def test_pattern_promotion_and_training_snapshot_flow(self) -> None:
        storage.save_correction_session(
            user_id='pattern-user-1',
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
        storage.save_correction_session(
            user_id='pattern-user-2',
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

        promoted = storage.promote_stable_pattern_candidates(
            min_support_count=2,
            min_distinct_users=2,
            min_stability_score=1.0,
            max_drift_score=0.0,
        )
        self.assertEqual(promoted['count'], 1)
        self.assertIsNotNone(promoted['items'][0]['dataset_item_id'])

        snapshot = storage.create_training_snapshot(
            name='unit-test snapshot',
            min_quality_score=0.7,
            include_statuses=['candidate', 'approved'],
        )
        self.assertTrue(snapshot['created'])
        self.assertGreaterEqual(snapshot['item_count'], 1)

        training_run = storage.create_model_training_run(
            snapshot_id=snapshot['snapshot_id'],
            model_family='gigachat',
            base_model='GigaChat-2-Pro',
            train_params={'epochs': 1},
        )
        self.assertEqual(training_run['status'], 'queued')

    def test_training_export_and_activation_close_runtime_loop(self) -> None:
        storage.save_correction_session(
            user_id='runtime-user-1',
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
        storage.save_correction_session(
            user_id='runtime-user-2',
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
        storage.promote_stable_pattern_candidates(
            min_support_count=2,
            min_distinct_users=2,
            min_stability_score=1.0,
            max_drift_score=0.0,
        )
        snapshot = storage.create_training_snapshot(
            name='runtime snapshot',
            min_quality_score=0.7,
            include_statuses=['candidate', 'approved'],
        )

        export_result = storage.export_training_snapshot(snapshot_id=snapshot['snapshot_id'])
        self.assertTrue(Path(export_result['files']['train']).exists())
        self.assertTrue(Path(export_result['manifest_path']).exists())

        training_run = storage.create_model_training_run(
            snapshot_id=snapshot['snapshot_id'],
            model_family='gigachat',
            base_model='GigaChat-2-Pro',
            train_params={
                'serving_provider': 'gigachat',
                'serving_base_url': 'https://gigachat.devices.sberbank.ru/api/v1',
                'serving_model_name': 'GigaChat-2-Pro',
            },
        )
        started = storage.start_model_training_run(
            training_run_id=training_run['training_run_id'],
            trainer_mode='manifest_only',
            auto_activate=True,
        )
        self.assertEqual(started['status'], 'completed')
        self.assertTrue(started['activated'])

        active_runtime = storage.get_active_model_runtime()
        self.assertIsNotNone(active_runtime)
        self.assertEqual(active_runtime['model_name'], 'GigaChat-2-Pro')
        self.assertEqual(active_runtime['provider'], 'gigachat')

    def test_manifest_training_can_activate_gigachat_runtime(self) -> None:
        storage.save_correction_session(
            user_id='gigachat-user-1',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'Дата создания',
                    'target_field': 'creationDate',
                    'corrected_value': {'source': 'Дата создания', 'target': 'creationDate'},
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
        )
        storage.save_correction_session(
            user_id='gigachat-user-2',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'Дата создания',
                    'target_field': 'creationDate',
                    'corrected_value': {'source': 'Дата создания', 'target': 'creationDate'},
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
        )
        storage.promote_stable_pattern_candidates(
            min_support_count=2,
            min_distinct_users=2,
            min_stability_score=1.0,
            max_drift_score=0.0,
        )
        snapshot = storage.create_training_snapshot(
            name='gigachat runtime snapshot',
            min_quality_score=0.7,
            include_statuses=['candidate', 'approved'],
        )
        training_run = storage.create_model_training_run(
            snapshot_id=snapshot['snapshot_id'],
            model_family='gigachat',
            base_model='GigaChat-2-Pro',
            train_params={
                'serving_provider': 'gigachat',
                'serving_base_url': 'https://gigachat.devices.sberbank.ru/api/v1',
                'serving_model_name': 'GigaChat-2-Pro',
            },
        )

        result = storage.start_model_training_run(
            training_run_id=training_run['training_run_id'],
            trainer_mode='manifest_only',
            auto_activate=True,
        )

        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['activated'])
        active_runtime = storage.get_active_model_runtime()
        self.assertIsNotNone(active_runtime)
        self.assertEqual(active_runtime['provider'], 'gigachat')
        self.assertEqual(active_runtime['model_name'], 'GigaChat-2-Pro')

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
