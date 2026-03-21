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
        semantic_graph = layers['layers']['semantic_graph']

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

        self.assertGreaterEqual(semantic_graph['counts']['nodes'], 2)
        self.assertGreaterEqual(semantic_graph['counts']['edges'], 2)
        self.assertGreaterEqual(semantic_graph['counts']['accepted'], 1)
        self.assertGreaterEqual(semantic_graph['counts']['rejected'], 1)
        self.assertTrue(any(item['right_field_norm'] == 'customername' or item['left_field_norm'] == 'customername' for item in semantic_graph['items']))
        self.assertGreaterEqual(len(semantic_graph['clusters']), 1)

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
        self.assertTrue(result['canonical_state']['updated'])

        db = storage.get_db()
        current_version = db.get(
            '''
            SELECT version_number
            FROM generation_versions
            WHERE id = (
                SELECT current_version_id
                FROM generations
                WHERE id = :generation_id
            )
            ''',
            {'generation_id': generation_id},
        )
        self.assertEqual(int(current_version['version_number']), 2)

        history_item = storage.get_history('user-confirm-generation')[0]
        current_mappings = json.loads(history_item['mappings_json'])
        self.assertEqual(current_mappings[0]['status'], 'accepted')
        self.assertEqual(current_mappings[0]['source_of_truth'], 'model_suggestion')

        second_result = storage.confirm_generation_learning(
            user_id='user-confirm-generation',
            generation_id=generation_id,
        )
        self.assertTrue(second_result['promoted'])
        self.assertTrue(second_result['already_promoted'])
        self.assertFalse(second_result['canonical_state']['updated'])

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

    def test_pattern_candidate_status_pipeline_moves_from_personal_to_shared(self) -> None:
        for index, user_id in enumerate(('pipeline-user-1', 'pipeline-user-2', 'pipeline-user-1'), start=1):
            generation_id = storage.save_generation(
                user_id=user_id,
                file_name=f'crm-{index}.csv',
                file_path=f'/tmp/crm-{index}.csv',
                file_type='csv',
                target_json=json.dumps({'creationDate': ''}, ensure_ascii=False),
                mappings_json=json.dumps([], ensure_ascii=False),
                generated_typescript='export function transform() { return {}; }',
                preview_json=json.dumps([], ensure_ascii=False),
                warnings_json=json.dumps([], ensure_ascii=False),
                parsed_file_json=json.dumps(
                    {
                        'file_name': f'crm-{index}.csv',
                        'file_type': 'csv',
                        'columns': ['Дата создания клиента'],
                        'rows': [{'Дата создания клиента': '2026-03-21'}],
                        'sheets': [],
                        'warnings': [],
                    },
                    ensure_ascii=False,
                ),
                source_columns=['Дата создания клиента'],
                promotion_mode='confirmed_only',
            )
            suggestions = storage.save_mapping_suggestions(
                generation_id=generation_id,
                user_id=user_id,
                mappings=[
                    {
                        'source': 'Дата создания клиента',
                        'target': 'creationDate',
                        'confidence': 'high',
                        'reason': 'model_suggestion',
                        'status': 'suggested',
                        'source_of_truth': 'model_suggestion',
                    }
                ],
            )
            storage.apply_mapping_feedback(
                user_id=user_id,
                generation_id=generation_id,
                feedback=[
                    {
                        'suggestion_id': suggestions[0]['suggestion_id'],
                        'target_field': 'creationDate',
                        'status': 'accepted',
                        'confidence_after': 1.0,
                    }
                ],
            )

        row = storage.get_db().get(
            '''
            SELECT status, acceptance_rate, distinct_users_count, support_count
            FROM pattern_candidates
            WHERE target_field_normalized = :target
            ORDER BY id DESC
            LIMIT 1
            ''',
            {'target': 'creationdate'},
        )
        self.assertIsNotNone(row)
        self.assertEqual(str(row['status']), 'shared_promoted')
        self.assertGreaterEqual(int(row['distinct_users_count'] or 0), 2)
        self.assertGreaterEqual(int(row['support_count'] or 0), 3)
        self.assertGreaterEqual(float(row['acceptance_rate'] or 0.0), 0.8)

    def test_sensitive_pattern_stays_blocked_from_shared_layer(self) -> None:
        for index, user_id in enumerate(('sensitive-user-1', 'sensitive-user-2', 'sensitive-user-1'), start=1):
            generation_id = storage.save_generation(
                user_id=user_id,
                file_name=f'sensitive-{index}.csv',
                file_path=f'/tmp/sensitive-{index}.csv',
                file_type='csv',
                target_json=json.dumps({'customerEmail': ''}, ensure_ascii=False),
                mappings_json=json.dumps([], ensure_ascii=False),
                generated_typescript='export function transform() { return {}; }',
                preview_json=json.dumps([], ensure_ascii=False),
                warnings_json=json.dumps([], ensure_ascii=False),
                parsed_file_json=json.dumps(
                    {
                        'file_name': f'sensitive-{index}.csv',
                        'file_type': 'csv',
                        'columns': ['Email клиента'],
                        'rows': [{'Email клиента': 'a@example.com'}],
                        'sheets': [],
                        'warnings': [],
                    },
                    ensure_ascii=False,
                ),
                source_columns=['Email клиента'],
                promotion_mode='confirmed_only',
            )
            suggestions = storage.save_mapping_suggestions(
                generation_id=generation_id,
                user_id=user_id,
                mappings=[
                    {
                        'source': 'Email клиента',
                        'target': 'customerEmail',
                        'confidence': 'high',
                        'reason': 'model_suggestion',
                        'status': 'suggested',
                        'source_of_truth': 'model_suggestion',
                    }
                ],
            )
            storage.apply_mapping_feedback(
                user_id=user_id,
                generation_id=generation_id,
                feedback=[
                    {
                        'suggestion_id': suggestions[0]['suggestion_id'],
                        'target_field': 'customerEmail',
                        'status': 'accepted',
                        'confidence_after': 1.0,
                    }
                ],
            )

        row = storage.get_db().get(
            '''
            SELECT status, sensitivity_score
            FROM pattern_candidates
            WHERE target_field_normalized = :target
            ORDER BY id DESC
            LIMIT 1
            ''',
            {'target': 'customeremail'},
        )
        self.assertIsNotNone(row)
        self.assertEqual(str(row['status']), 'blocked_sensitive')
        self.assertGreaterEqual(float(row['sensitivity_score'] or 0.0), 0.6)

    def test_guest_runtime_uses_only_shared_promoted_patterns(self) -> None:
        generation_id = storage.save_generation(
            user_id='guest-seed-user',
            file_name='seed.csv',
            file_path='/tmp/seed.csv',
            file_type='csv',
            target_json=json.dumps({'creationDate': ''}, ensure_ascii=False),
            mappings_json=json.dumps([], ensure_ascii=False),
            generated_typescript='export function transform() { return {}; }',
            preview_json=json.dumps([], ensure_ascii=False),
            warnings_json=json.dumps([], ensure_ascii=False),
            parsed_file_json=json.dumps(
                {
                    'file_name': 'seed.csv',
                    'file_type': 'csv',
                    'columns': ['Дата создания клиента'],
                    'rows': [{'Дата создания клиента': '2026-03-21'}],
                    'sheets': [],
                    'warnings': [],
                },
                ensure_ascii=False,
            ),
            source_columns=['Дата создания клиента'],
            promotion_mode='confirmed_only',
        )
        suggestions = storage.save_mapping_suggestions(
            generation_id=generation_id,
            user_id='guest-seed-user',
            mappings=[
                {
                    'source': 'Дата создания клиента',
                    'target': 'creationDate',
                    'confidence': 'high',
                    'reason': 'model_suggestion',
                    'status': 'suggested',
                    'source_of_truth': 'model_suggestion',
                }
            ],
        )
        storage.apply_mapping_feedback(
            user_id='guest-seed-user',
            generation_id=generation_id,
            feedback=[
                {
                    'suggestion_id': suggestions[0]['suggestion_id'],
                    'target_field': 'creationDate',
                    'status': 'accepted',
                    'confidence_after': 1.0,
                }
            ],
        )

        guest_before = storage.get_global_mapping_pattern_candidates(
            user_id=None,
            source_columns=['Дата создания клиента'],
            target_fields=['creationDate'],
        )
        auth_before = storage.get_global_mapping_pattern_candidates(
            user_id='guest-seed-user',
            source_columns=['Дата создания клиента'],
            target_fields=['creationDate'],
        )

        self.assertEqual(guest_before, [])
        self.assertEqual(auth_before, [])

        for index, user_id in enumerate(('guest-seed-user-2', 'guest-seed-user-1'), start=2):
            generation_id = storage.save_generation(
                user_id=user_id,
                file_name=f'seed-{index}.csv',
                file_path=f'/tmp/seed-{index}.csv',
                file_type='csv',
                target_json=json.dumps({'creationDate': ''}, ensure_ascii=False),
                mappings_json=json.dumps([], ensure_ascii=False),
                generated_typescript='export function transform() { return {}; }',
                preview_json=json.dumps([], ensure_ascii=False),
                warnings_json=json.dumps([], ensure_ascii=False),
                parsed_file_json=json.dumps(
                    {
                        'file_name': f'seed-{index}.csv',
                        'file_type': 'csv',
                        'columns': ['Дата создания клиента'],
                        'rows': [{'Дата создания клиента': '2026-03-21'}],
                        'sheets': [],
                        'warnings': [],
                    },
                    ensure_ascii=False,
                ),
                source_columns=['Дата создания клиента'],
                promotion_mode='confirmed_only',
            )
            suggestions = storage.save_mapping_suggestions(
                generation_id=generation_id,
                user_id=user_id,
                mappings=[
                    {
                        'source': 'Дата создания клиента',
                        'target': 'creationDate',
                        'confidence': 'high',
                        'reason': 'model_suggestion',
                        'status': 'suggested',
                        'source_of_truth': 'model_suggestion',
                    }
                ],
            )
            storage.apply_mapping_feedback(
                user_id=user_id,
                generation_id=generation_id,
                feedback=[
                    {
                        'suggestion_id': suggestions[0]['suggestion_id'],
                        'target_field': 'creationDate',
                        'status': 'accepted',
                        'confidence_after': 1.0,
                    }
                ],
            )

        guest_after = storage.get_global_mapping_pattern_candidates(
            user_id=None,
            source_columns=['Дата создания клиента'],
            target_fields=['creationDate'],
        )
        self.assertTrue(guest_after)

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

    def test_delete_generation_history_entry_removes_generation_and_marks_upload_deleted(self) -> None:
        file_bytes = b'customerName,amount\nAlice,10\n'
        saved_path = storage.save_upload(file_bytes, 'history-delete.csv', mode='authorized', user_id='user-delete')
        upload_id = storage.record_uploaded_file(
            file_path=saved_path,
            original_file_name='history-delete.csv',
            file_bytes=file_bytes,
            mode='authorized',
            user_id='user-delete',
        )

        generation_id = storage.save_generation(
            user_id='user-delete',
            file_name='history-delete.csv',
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
                    'file_name': 'history-delete.csv',
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

        self.assertTrue(saved_path.exists())
        self.assertEqual(len(storage.get_history('user-delete')), 1)

        deletion = storage.delete_generation_history_entry(user_id='user-delete', generation_id=generation_id)
        self.assertEqual(deletion['deleted'], True)
        self.assertEqual(deletion['generation_id'], generation_id)
        self.assertEqual(deletion['deleted_files'], 1)
        self.assertEqual(storage.get_history('user-delete'), [])
        self.assertFalse(saved_path.exists())

        db = storage.get_db()
        generation_row = db.get('SELECT id FROM generations WHERE id = :id', {'id': generation_id})
        upload_row = db.get(
            'SELECT generation_id, status FROM uploaded_files WHERE id = :id',
            {'id': upload_id},
        )

        self.assertIsNone(generation_row)
        self.assertIsNone(upload_row['generation_id'])
        self.assertEqual(str(upload_row['status']), 'deleted')


if __name__ == '__main__':
    unittest.main()
