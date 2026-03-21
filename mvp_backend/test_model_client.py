from __future__ import annotations

import shutil
import sys
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

import model_client
import storage


class ModelClientRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = BACKEND_DIR / '.test_runtime_model_client' / str(uuid.uuid4())
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
        model_client._gigachat_token_cache.clear()

    def tearDown(self) -> None:
        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None
        model_client._gigachat_token_cache.clear()

    def test_model_client_uses_active_runtime_deployment(self) -> None:
        storage.save_correction_session(
            user_id='model-runtime-user-1',
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
            user_id='model-runtime-user-2',
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
            name='model runtime snapshot',
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
        storage.start_model_training_run(
            training_run_id=training_run['training_run_id'],
            trainer_mode='manifest_only',
            auto_activate=True,
        )

        captured: dict[str, object] = {}

        def fake_post_json(
            url: str,
            payload: dict,
            headers: dict,
            timeout_seconds: float,
            ssl_context=None,
        ) -> dict:
            captured['url'] = url
            captured['payload'] = payload
            captured['headers'] = headers
            captured['timeout_seconds'] = timeout_seconds
            return {
                'choices': [
                    {
                        'message': {
                            'content': (
                                '{"mappings":[{"target":"customerName","source":"ФИО клиента",'
                                '"confidence":"high","reason":"active_runtime"}]}'
                            )
                        }
                    }
                ]
            }

        with patch.dict('os.environ', {'TSGEN_MODEL_API_KEY': 'gigachat-access-token'}, clear=False), patch(
            'model_client._post_json', side_effect=fake_post_json
        ):
            mappings, warnings = model_client.suggest_field_mappings(
                source_columns=['ФИО клиента'],
                target_fields=['customerName'],
                sample_rows=[{'ФИО клиента': 'Иванов Иван'}],
            )

        self.assertEqual(warnings, [])
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]['source'], 'ФИО клиента')
        self.assertEqual(captured['url'], 'https://gigachat.devices.sberbank.ru/api/v1/chat/completions')
        self.assertEqual(captured['payload']['model'], 'GigaChat-2-Pro')
        self.assertEqual(captured['headers'], {'Authorization': 'Bearer gigachat-access-token'})

    def test_gigachat_content_list_is_flattened(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_MODEL_API_KEY': 'gigachat-access-token',
            },
            clear=False,
        ):
            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': [
                                    {
                                        'type': 'text',
                                        'text': (
                                            '{"mappings":[{"target":"customerName","source":"ФИО клиента",'
                                            '"confidence":"high","reason":"chunked"}]}'
                                        ),
                                    }
                                ]
                            }
                        }
                    ]
                }

            with patch('model_client._post_json', side_effect=fake_post_json):
                mappings, warnings = model_client.suggest_field_mappings(
                    source_columns=['ФИО клиента'],
                    target_fields=['customerName'],
                    sample_rows=[{'ФИО клиента': 'Иванов Иван'}],
                )

        self.assertEqual(warnings, [])
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]['target'], 'customerName')

    def test_timeout_returns_warning_instead_of_exception(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_MODEL_API_KEY': 'gigachat-access-token',
                'TSGEN_MODEL_TIMEOUT_SECONDS': '5',
            },
            clear=False,
        ):
            with patch('model_client._post_json', side_effect=RuntimeError('GigaChat не ответил вовремя за 5 сек.')):
                mappings, warnings = model_client.suggest_field_mappings(
                    source_columns=['ФИО клиента'],
                    target_fields=['customerName'],
                    sample_rows=[{'ФИО клиента': 'Иванов Иван'}],
                )

        self.assertEqual(mappings, [])
        self.assertEqual(warnings, ['GigaChat не ответил вовремя за 5 сек.'])

    def test_rank_mapping_candidate_returns_best_candidate(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_MODEL_API_KEY': 'gigachat-access-token',
            },
            clear=False,
        ):
            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    '{"target":"creationDate","best_candidate":"Дата создания",'
                                    '"confidence":0.93,"reason":"semantic date match"}'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_json', side_effect=fake_post_json):
                ranking, warnings = model_client.rank_mapping_candidate(
                    target_field='creationDate',
                    target_type='string',
                    candidates=['Дата создания', 'Дата обновления'],
                    sample_value='21.03.2026',
                )

        self.assertEqual(warnings, [])
        self.assertIsNotNone(ranking)
        self.assertEqual(ranking['best_candidate'], 'Дата создания')
        self.assertGreater(ranking['confidence'], 0.9)

    def test_gigachat_provider_fetches_oauth_token_and_calls_chat_endpoint(self) -> None:
        captured: dict[str, object] = {}

        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_GIGACHAT_AUTH_KEY': 'Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ=',
                'TSGEN_GIGACHAT_SCOPE': 'GIGACHAT_API_PERS',
                'TSGEN_MODEL_TIMEOUT_SECONDS': '15',
            },
            clear=False,
        ):
            def fake_post_form_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                captured['oauth_url'] = url
                captured['oauth_payload'] = payload
                captured['oauth_headers'] = headers
                captured['oauth_timeout_seconds'] = timeout_seconds
                return {'access_token': 'gigachat-access-token', 'expires_at': int(time.time()) + 1800}

            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                captured['chat_url'] = url
                captured['chat_payload'] = payload
                captured['chat_headers'] = headers
                captured['chat_timeout_seconds'] = timeout_seconds
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    '{"target":"creationDate","best_candidate":"Дата создания",'
                                    '"confidence":0.91,"reason":"semantic date match"}'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_form_json', side_effect=fake_post_form_json), patch(
                'model_client._post_json', side_effect=fake_post_json
            ):
                ranking, warnings = model_client.rank_mapping_candidate(
                    target_field='creationDate',
                    target_type='string',
                    candidates=['Дата создания', 'Дата обновления'],
                    sample_value='21.03.2026',
                )

        self.assertEqual(warnings, [])
        self.assertIsNotNone(ranking)
        self.assertEqual(ranking['best_candidate'], 'Дата создания')
        self.assertEqual(captured['oauth_url'], 'https://ngw.devices.sberbank.ru:9443/api/v2/oauth')
        self.assertEqual(captured['oauth_payload'], {'scope': 'GIGACHAT_API_PERS'})
        self.assertEqual(captured['chat_url'], 'https://gigachat.devices.sberbank.ru/api/v1/chat/completions')
        self.assertEqual(captured['chat_headers'], {'Authorization': 'Bearer gigachat-access-token'})

    def test_gigachat_provider_reuses_cached_oauth_token(self) -> None:
        oauth_calls = 0

        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_GIGACHAT_AUTH_KEY': 'Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ=',
            },
            clear=False,
        ):
            def fake_post_form_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                nonlocal oauth_calls
                oauth_calls += 1
                return {'access_token': 'gigachat-access-token', 'expires_at': int(time.time()) + 1800}

            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    '{"target":"amount","best_candidate":"Сумма",'
                                    '"confidence":0.83,"reason":"semantic amount match"}'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_form_json', side_effect=fake_post_form_json), patch(
                'model_client._post_json', side_effect=fake_post_json
            ):
                first_ranking, first_warnings = model_client.rank_mapping_candidate(
                    target_field='amount',
                    target_type='number',
                    candidates=['Сумма', 'Дата'],
                )
                second_ranking, second_warnings = model_client.rank_mapping_candidate(
                    target_field='amount',
                    target_type='number',
                    candidates=['Сумма', 'Дата'],
                )

        self.assertEqual(first_warnings, [])
        self.assertEqual(second_warnings, [])
        self.assertEqual(first_ranking['best_candidate'], 'Сумма')
        self.assertEqual(second_ranking['best_candidate'], 'Сумма')
        self.assertEqual(oauth_calls, 1)

    def test_gigachat_provider_falls_back_to_corp_scope(self) -> None:
        seen_scopes: list[str] = []

        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Max',
                'TSGEN_GIGACHAT_AUTH_KEY': 'Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ=',
                'TSGEN_GIGACHAT_SCOPE': 'GIGACHAT_API_PERS',
            },
            clear=False,
        ):
            def fake_post_form_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                scope = payload['scope']
                seen_scopes.append(scope)
                if scope != 'GIGACHAT_API_CORP':
                    raise RuntimeError('GigaChat OAuth вернул HTTP 400: {"code":7,"message":"scope from db not fully includes consumed scope"}')
                return {'access_token': 'gigachat-access-token', 'expires_at': int(time.time()) + 1800}

            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    '{"target":"amount","best_candidate":"Сумма",'
                                    '"confidence":0.83,"reason":"semantic amount match"}'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_form_json', side_effect=fake_post_form_json), patch(
                'model_client._post_json', side_effect=fake_post_json
            ):
                ranking, warnings = model_client.rank_mapping_candidate(
                    target_field='amount',
                    target_type='number',
                    candidates=['Сумма', 'Дата'],
                )

        self.assertEqual(warnings, [])
        self.assertEqual(ranking['best_candidate'], 'Сумма')
        self.assertEqual(seen_scopes, ['GIGACHAT_API_PERS', 'GIGACHAT_API_CORP'])

    def test_gigachat_provider_accepts_raw_client_pair_and_encodes_it(self) -> None:
        captured: dict[str, object] = {}

        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Max',
                'TSGEN_GIGACHAT_AUTH_KEY': 'client-id:client-secret',
            },
            clear=False,
        ):
            def fake_post_form_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                captured['oauth_headers'] = headers
                return {'access_token': 'gigachat-access-token', 'expires_at': int(time.time()) + 1800}

            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    '{"target":"amount","best_candidate":"Сумма",'
                                    '"confidence":0.82,"reason":"semantic amount match"}'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_form_json', side_effect=fake_post_form_json), patch(
                'model_client._post_json', side_effect=fake_post_json
            ):
                ranking, warnings = model_client.rank_mapping_candidate(
                    target_field='amount',
                    target_type='number',
                    candidates=['Сумма', 'Дата'],
                )

        self.assertEqual(warnings, [])
        self.assertEqual(ranking['best_candidate'], 'Сумма')
        self.assertEqual(captured['oauth_headers']['Authorization'], 'Basic Y2xpZW50LWlkOmNsaWVudC1zZWNyZXQ=')

    def test_gigachat_markdown_fence_json_is_parsed(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_MODEL_API_KEY': 'gigachat-access-token',
            },
            clear=False,
        ):
            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    'Here is the result.\n'
                                    '```json\n'
                                    '{"mappings":[{"target":"customerName","source":"ФИО клиента","confidence":"high","reason":"fenced"}]}\n'
                                    '```\n'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_json', side_effect=fake_post_json):
                mappings, warnings = model_client.suggest_field_mappings(
                    source_columns=['ФИО клиента'],
                    target_fields=['customerName'],
                    sample_rows=[{'ФИО клиента': 'Иванов Иван'}],
                )

        self.assertEqual(warnings, [])
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]['reason'], 'fenced')

    def test_gigachat_json_with_noise_is_parsed(self) -> None:
        with patch.dict(
            'os.environ',
            {
                'TSGEN_MODEL_PROVIDER': 'gigachat',
                'TSGEN_MODEL_BASE_URL': 'https://gigachat.devices.sberbank.ru/api/v1',
                'TSGEN_MODEL_NAME': 'GigaChat-2-Pro',
                'TSGEN_MODEL_API_KEY': 'gigachat-access-token',
            },
            clear=False,
        ):
            def fake_post_json(
                url: str,
                payload: dict,
                headers: dict,
                timeout_seconds: float,
                ssl_context=None,
            ) -> dict:
                return {
                    'choices': [
                        {
                            'message': {
                                'content': (
                                    'Semantic match found.\n'
                                    '{"mappings":[{"target":"customerName","source":"ФИО клиента","confidence":"high","reason":"noisy"}]}\n'
                                    'Done.'
                                )
                            }
                        }
                    ]
                }

            with patch('model_client._post_json', side_effect=fake_post_json):
                mappings, warnings = model_client.suggest_field_mappings(
                    source_columns=['ФИО клиента'],
                    target_fields=['customerName'],
                    sample_rows=[{'ФИО клиента': 'Иванов Иван'}],
                )

        self.assertEqual(warnings, [])
        self.assertEqual(len(mappings), 1)
        self.assertEqual(mappings[0]['reason'], 'noisy')


if __name__ == '__main__':
    unittest.main()
