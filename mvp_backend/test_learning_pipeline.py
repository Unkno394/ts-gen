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
from learning_pipeline import (
    _semantic_conflict_assessment,
    resolve_generation_mappings,
    resolve_generation_mappings_detailed,
)
from matcher import prepare_field_name
from models import TargetField


class LearningPipelineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = BACKEND_DIR / '.test_runtime_pipeline' / str(uuid.uuid4())
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

    def test_model_suggestion_is_used_when_memory_and_rules_are_insufficient(self) -> None:
        with patch(
            'learning_pipeline.rank_mapping_candidate',
            return_value=(
                {
                    'target': 'mainComment',
                    'best_candidate': 'Комментарий клиента',
                    'confidence': 0.91,
                    'reason': 'semantic_synonym',
                },
                [],
            ),
        ):
            mappings, warnings = resolve_generation_mappings(
                source_columns=['Комментарий клиента'],
                source_rows=[{'Комментарий клиента': 'Иванов Иван'}],
                target_fields=[TargetField(name='mainComment', type='string')],
                user_id='pipeline-user',
                schema_fingerprint_id=None,
            )

        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0].source, 'Комментарий клиента')
        self.assertEqual(mappings[0].source_of_truth, 'model_suggestion')
        self.assertEqual(mappings[0].status, 'suggested')
        self.assertTrue(any('модель выбрала' in warning for warning in warnings))

    def test_detailed_result_contains_explainability_block(self) -> None:
        with patch(
            'learning_pipeline.rank_mapping_candidate',
            return_value=(
                {
                    'target': 'mainComment',
                    'best_candidate': 'Комментарий клиента',
                    'confidence': 0.88,
                    'reason': 'semantic_comment_match',
                },
                [],
            ),
        ):
            result = resolve_generation_mappings_detailed(
                source_columns=['Комментарий клиента', 'Статус'],
                source_rows=[{'Комментарий клиента': 'test', 'Статус': 'open'}],
                target_fields=[TargetField(name='mainComment', type='string')],
                user_id='pipeline-user',
                schema_fingerprint_id=1,
            )

        self.assertEqual(len(result['mappings']), 1)
        self.assertIn('mapping_stats', result['explainability'])
        self.assertIn('mapping_sources', result['explainability'])
        self.assertIn('suggestions', result['explainability'])
        self.assertEqual(result['explainability']['mapping_sources'][0]['source_of_truth'], 'model_suggestion')

    def test_conflicting_semantics_stay_unresolved_below_final_threshold(self) -> None:
        with patch(
            'learning_pipeline.rank_mapping_candidate',
            return_value=(
                {
                    'target': 'dealCreationDate',
                    'best_candidate': 'dealUpdateDate',
                    'confidence': 0.74,
                    'reason': 'semantic_date_match',
                },
                [],
            ),
        ):
            result = resolve_generation_mappings_detailed(
                source_columns=['dealUpdateDate', 'Статус'],
                source_rows=[{'dealUpdateDate': '21.03.2026', 'Статус': 'open'}],
                target_fields=[TargetField(name='dealCreationDate', type='string')],
                user_id='pipeline-user',
                schema_fingerprint_id=2,
            )

        self.assertEqual(len(result['mappings']), 1)
        mapping = result['mappings'][0]
        self.assertIsNone(mapping.source)
        self.assertEqual(mapping.source_of_truth, 'unresolved')
        self.assertLess(mapping.model_confidence_score or 0.0, 0.68)
        metadata = mapping.candidate_metadata or {}
        self.assertEqual(mapping.confidence, 'low')
        self.assertIn('model_confidence_score_raw', metadata)
        self.assertIn('model_confidence_score_final', metadata)
        self.assertEqual(metadata.get('rejected_by_gate'), True)
        self.assertEqual(metadata.get('rejection_reason'), 'semantic_conflict_created_vs_updated')
        self.assertIn('candidate_prior', metadata)
        self.assertIn('confidence_band', metadata)
        self.assertEqual(result['explainability']['unresolved_fields'], ['dealCreationDate'])

    def test_semantic_graph_can_resolve_transitive_identifier_match_before_llm(self) -> None:
        storage.save_correction_session(
            user_id='graph-user',
            session_type='feedback_loop',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'user_id',
                    'target_field': 'accountId',
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
        )
        storage.save_correction_session(
            user_id='graph-user',
            session_type='feedback_loop',
            corrections=[
                {
                    'correction_type': 'mapping_override',
                    'source_field': 'id',
                    'target_field': 'user_id',
                    'confidence_after': 1.0,
                    'accepted': True,
                }
            ],
        )

        with patch('learning_pipeline.rank_mapping_candidate') as rank_mock:
            result = resolve_generation_mappings_detailed(
                source_columns=['id', 'comment'],
                source_rows=[{'id': '42', 'comment': 'hello'}],
                target_fields=[TargetField(name='accountId', type='string')],
                user_id='graph-user',
                schema_fingerprint_id=None,
            )

        rank_mock.assert_not_called()
        mapping = result['mappings'][0]
        self.assertEqual(mapping.source, 'id')
        self.assertEqual(mapping.source_of_truth, 'semantic_graph')
        self.assertEqual(result['explainability']['mapping_stats']['semantic_graph'], 1)
        self.assertIn(mapping.reason, {'semantic_graph_direct', 'semantic_graph_transitive'})

    def test_semantic_conflict_assessment_flags_typical_mistakes(self) -> None:
        cases = [
            ('creator_vs_responsible', 'creator', 'responsiblePerson', 'semantic_conflict_creator_vs_responsible'),
            ('deal_source_vs_creator', 'dealSource', 'creator', 'semantic_conflict_creator_vs_source'),
            ('organization_vs_partner', 'organization', 'partnerName', 'semantic_conflict_organization_vs_partner'),
            ('invoice_amount_vs_revenue', 'invoiceAmount', 'revenue', 'semantic_conflict_amount_vs_revenue'),
            ('license_amount_vs_product_amount', 'licenseAmount', 'productAmount', 'semantic_conflict_license_vs_product'),
            ('description_vs_deal_name', 'description', 'dealName', 'semantic_conflict_description_vs_name'),
            ('creation_vs_last_update', 'creationDate', 'lastUpdateDate', 'semantic_conflict_created_vs_updated'),
            ('id_vs_date', 'dealId', 'dealDate', 'semantic_conflict_date_vs_id'),
            ('customer_vs_organization', 'customerName', 'organizationName', 'semantic_conflict_customer_vs_organization'),
            ('amount_with_vat_vs_without_vat', 'amountWithVAT', 'amountWithoutVAT', 'semantic_conflict_gross_vs_net'),
        ]

        for label, target_name, source_name, expected_label in cases:
            with self.subTest(case=label):
                penalty, conflict_label = _semantic_conflict_assessment(
                    prepare_field_name(target_name, field_type='string'),
                    prepare_field_name(source_name, field_type='string'),
                )
                self.assertGreater(penalty, 0.0)
                self.assertEqual(conflict_label, expected_label)


if __name__ == '__main__':
    unittest.main()
