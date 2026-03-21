from __future__ import annotations

import json
import logging
import os
import re
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import base64
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

MARKDOWN_JSON_FENCE_RE = re.compile(r'```(?:json)?\s*([\s\S]*?)```', re.IGNORECASE)
REGEX_JSON_BLOCK_RE = re.compile(r'(\{[\s\S]*\}|\[[\s\S]*\])')


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding='utf-8').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_dotenv(Path(__file__).parent / '.env')

DEFAULT_SAMPLE_ROWS = 2
DEFAULT_HINT_LIMIT = 4
DEFAULT_MAPPING_MAX_TOKENS = 256
DEFAULT_DRAFT_MAX_TOKENS = 384
GIGACHAT_DEFAULT_AUTH_URL = 'https://ngw.devices.sberbank.ru:9443/api/v2/oauth'
GIGACHAT_DEFAULT_BASE_URL = 'https://gigachat.devices.sberbank.ru/api/v1'
GIGACHAT_TOKEN_LEEWAY_SECONDS = 60
DEFAULT_GIGACHAT_CA_BUNDLE = '/app/certs/russian_trusted_root_ca_pem.crt'

_gigachat_token_cache: dict[str, dict[str, Any]] = {}


def rank_mapping_candidate(
    *,
    target_field: str,
    target_type: str | None,
    candidates: list[str],
    sample_value: Any | None = None,
    hints: list[str] | None = None,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not candidates:
        return None, []

    if not _model_enabled():
        logger.info(
            'llm candidate ranker skipped: model runtime disabled provider=%s base_url=%s model=%s',
            _get_model_runtime_config()['provider'],
            _get_model_runtime_config()['base_url'],
            _get_model_runtime_config()['model_name'],
        )
        return None, []

    runtime = _get_model_runtime_config()
    compact_candidates = [str(candidate).strip() for candidate in candidates if str(candidate).strip()][:5]
    compact_hints = [str(hint).strip()[:80] for hint in (hints or []) if str(hint).strip()][:3]
    logger.info(
        'llm candidate ranker request: provider=%s model=%s target=%s candidates=%d max_tokens=%d',
        runtime['provider'],
        runtime['model_name'],
        target_field,
        len(compact_candidates),
        runtime['mapping_max_tokens'],
    )

    payload = {
        'task': 'choose_best_source_column_for_target_field',
        'target_field': target_field,
        'target_type': target_type or 'string',
        'candidates': compact_candidates,
        'sample_value': _compact_sample_value(sample_value),
        'hints': compact_hints,
        'rules': [
            'choose one best candidate from candidates or null',
            'return strict JSON only',
        ],
    }
    instructions = (
        'You are a data mapping ranker. '
        'Return only valid JSON. '
        'Choose exactly one best candidate from the provided candidates or null. '
        'Never invent source columns. '
        'Confidence must be a number between 0 and 1.'
    )

    raw_response, warnings = _call_model_as_json(
        instructions=instructions,
        payload=payload,
        max_tokens=min(runtime['mapping_max_tokens'], 128),
    )
    if raw_response is None:
        return None, warnings
    if not isinstance(raw_response, dict):
        return None, warnings + ['Модель вернула некорректный формат ранжирования кандидатов.']

    best_candidate = raw_response.get('best_candidate')
    normalized_candidate = None
    if best_candidate is not None:
        candidate_text = str(best_candidate).strip()
        if candidate_text in compact_candidates:
            normalized_candidate = candidate_text

    result = {
        'target': str(raw_response.get('target') or raw_response.get('target_field') or target_field).strip() or target_field,
        'best_candidate': normalized_candidate,
        'confidence': _normalize_confidence_score(raw_response.get('confidence')),
        'reason': str(raw_response.get('reason') or 'candidate_ranker'),
    }
    logger.info(
        'llm candidate ranker response: target=%s best_candidate=%s confidence=%.3f warnings=%d',
        result['target'],
        result['best_candidate'],
        result['confidence'],
        len(warnings),
    )
    return result, warnings


def suggest_field_mappings(
    *,
    source_columns: list[str],
    target_fields: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
    personal_hints: list[dict[str, Any]] | None = None,
    global_hints: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not _model_enabled():
        logger.info(
            'llm mapping skipped: model runtime disabled provider=%s base_url=%s model=%s',
            _get_model_runtime_config()['provider'],
            _get_model_runtime_config()['base_url'],
            _get_model_runtime_config()['model_name'],
        )
        return [], []

    runtime = _get_model_runtime_config()
    sample_preview = _truncate_sample_rows(sample_rows or [], runtime['sample_rows'])
    personal_hint_preview = _compact_hints(personal_hints or [], runtime['hint_limit'])
    global_hint_preview = _compact_hints(global_hints or [], runtime['hint_limit'])
    logger.info(
        'llm mapping request: provider=%s base_url=%s model=%s source_columns=%d target_fields=%d sample_rows=%d personal_hints=%d global_hints=%d max_tokens=%d',
        runtime['provider'],
        runtime['base_url'],
        runtime['model_name'],
        len(source_columns),
        len(target_fields),
        len(sample_preview),
        len(personal_hint_preview),
        len(global_hint_preview),
        runtime['mapping_max_tokens'],
    )

    prompt_payload = {
        'task': 'mapping',
        'source_columns': source_columns,
        'target_fields': target_fields,
        'sample_rows': sample_preview,
        'personal_hints': personal_hint_preview,
        'global_hints': global_hint_preview,
        'output_schema': {
            'mappings': [
                {
                    'target': 'string',
                    'source': 'string|null',
                    'confidence': 'high|medium|low|none',
                    'reason': 'short_reason',
                }
            ]
        },
    }
    instructions = (
        'Resolve semantic field mappings between source columns and target JSON fields. '
        'Return only JSON. Do not invent source columns. If uncertain, set source to null. '
        'Use personal_hints first, then semantic similarity from sample rows and global_hints. '
        'Be concise. Return only fields that you are reasonably confident about.'
    )
    raw_response, warnings = _call_model_as_json(
        instructions=instructions,
        payload=prompt_payload,
        max_tokens=runtime['mapping_max_tokens'],
    )
    if raw_response is None:
        logger.warning(
            'llm mapping request failed: provider=%s model=%s warnings=%s',
            runtime['provider'],
            runtime['model_name'],
            '; '.join(warnings) if warnings else 'none',
        )
        return [], warnings

    mappings = raw_response.get('mappings') if isinstance(raw_response, dict) else None
    if not isinstance(mappings, list):
        logger.warning(
            'llm mapping request returned invalid payload: provider=%s model=%s payload_type=%s',
            runtime['provider'],
            runtime['model_name'],
            type(raw_response).__name__,
        )
        return [], warnings + ['Модель вернула некорректный формат сопоставлений.']

    valid_targets = set(target_fields)
    valid_sources = set(source_columns)
    normalized: list[dict[str, Any]] = []
    used_targets: set[str] = set()
    for item in mappings:
        if not isinstance(item, dict):
            continue
        target = str(item.get('target') or '').strip()
        source = item.get('source')
        if target not in valid_targets or target in used_targets:
            continue
        if source is not None:
            source = str(source).strip()
            if source not in valid_sources:
                source = None
        normalized.append(
            {
                'target': target,
                'source': source,
                'confidence': _normalize_confidence(item.get('confidence')),
                'reason': str(item.get('reason') or 'semantic_model'),
            }
        )
        used_targets.add(target)
    logger.info(
        'llm mapping response: provider=%s model=%s returned=%d normalized=%d warnings=%d',
        runtime['provider'],
        runtime['model_name'],
        len(mappings),
        len(normalized),
        len(warnings),
    )
    return normalized, warnings


def suggest_draft_json_fields(
    *,
    source_columns: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
    personal_hints: list[dict[str, Any]] | None = None,
    global_hints: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    if not _model_enabled():
        logger.info(
            'llm draft-json skipped: model runtime disabled provider=%s base_url=%s model=%s',
            _get_model_runtime_config()['provider'],
            _get_model_runtime_config()['base_url'],
            _get_model_runtime_config()['model_name'],
        )
        return [], []

    runtime = _get_model_runtime_config()
    sample_preview = _truncate_sample_rows(sample_rows or [], runtime['sample_rows'])
    personal_hint_preview = _compact_hints(personal_hints or [], runtime['hint_limit'])
    global_hint_preview = _compact_hints(global_hints or [], runtime['hint_limit'])
    logger.info(
        'llm draft-json request: provider=%s base_url=%s model=%s source_columns=%d sample_rows=%d personal_hints=%d global_hints=%d max_tokens=%d',
        runtime['provider'],
        runtime['base_url'],
        runtime['model_name'],
        len(source_columns),
        len(sample_preview),
        len(personal_hint_preview),
        len(global_hint_preview),
        runtime['draft_max_tokens'],
    )

    prompt_payload = {
        'task': 'draft_json',
        'source_columns': source_columns,
        'sample_rows': sample_preview,
        'personal_hints': personal_hint_preview,
        'global_hints': global_hint_preview,
        'output_schema': {
            'fields': [
                {
                    'source': 'string',
                    'target': 'string',
                    'default_value': 'string|number|boolean|null',
                    'confidence': 'high|medium|low',
                    'reason': 'short_reason',
                }
            ]
        },
    }
    instructions = (
        'Generate a draft JSON schema from the table columns. '
        'Return only JSON. Produce one field suggestion per source column. '
        'Prefer canonical camelCase target names. Reuse personal_hints first, then global_hints. '
        'Keep reasons very short.'
    )
    raw_response, warnings = _call_model_as_json(
        instructions=instructions,
        payload=prompt_payload,
        max_tokens=runtime['draft_max_tokens'],
    )
    if raw_response is None:
        logger.warning(
            'llm draft-json request failed: provider=%s model=%s warnings=%s',
            runtime['provider'],
            runtime['model_name'],
            '; '.join(warnings) if warnings else 'none',
        )
        return [], warnings

    fields = raw_response.get('fields') if isinstance(raw_response, dict) else None
    if not isinstance(fields, list):
        logger.warning(
            'llm draft-json request returned invalid payload: provider=%s model=%s payload_type=%s',
            runtime['provider'],
            runtime['model_name'],
            type(raw_response).__name__,
        )
        return [], warnings + ['Модель вернула некорректный формат draft JSON.']

    valid_sources = set(source_columns)
    normalized: list[dict[str, Any]] = []
    used_sources: set[str] = set()
    for item in fields:
        if not isinstance(item, dict):
            continue
        source = str(item.get('source') or '').strip()
        target = str(item.get('target') or '').strip()
        if source not in valid_sources or source in used_sources or not target:
            continue
        normalized.append(
            {
                'source': source,
                'target': target,
                'default_value': item.get('default_value', ''),
                'confidence': _normalize_confidence(item.get('confidence')),
                'reason': str(item.get('reason') or 'draft_json_model'),
            }
        )
        used_sources.add(source)
    logger.info(
        'llm draft-json response: provider=%s model=%s returned=%d normalized=%d warnings=%d',
        runtime['provider'],
        runtime['model_name'],
        len(fields),
        len(normalized),
        len(warnings),
    )
    return normalized, warnings


def suggest_form_field_repair(
    *,
    target_field: str,
    question: str,
    options: list[dict[str, Any]],
    enum_map: dict[str, str],
    context_lines: list[str] | None = None,
    allow_multiple: bool = False,
) -> tuple[dict[str, Any] | None, list[str]]:
    if not _form_repair_enabled():
        return None, []

    runtime = _get_model_runtime_config()
    compact_options = []
    for option in options[:8]:
        if not isinstance(option, dict):
            continue
        compact_options.append(
            {
                'label': str(option.get('label') or '').strip(),
                'selected': bool(option.get('selected')),
                'marker_text': str(option.get('marker_text') or '').strip() or None,
            }
        )
    payload = {
        'task': 'repair_form_field_resolution',
        'target_field': target_field,
        'question': question,
        'options': compact_options,
        'enum_map': enum_map,
        'context_lines': [str(line).strip() for line in (context_lines or []) if str(line).strip()][:12],
        'allow_multiple': allow_multiple,
        'output_schema': {
            'status': 'resolved|ambiguous|not_found',
            'enum_value': 'string|null',
            'enum_values': ['string'],
            'reason': 'short_reason',
            'confidence': '0..1',
        },
    }
    instructions = (
        'Repair one ambiguous form-field extraction from a document fragment. '
        'Return only strict JSON. '
        'Use only the provided question, options, markers and local context. '
        'Do not invent enums outside enum_map. '
        'If uncertain, return status ambiguous or not_found.'
    )
    raw_response, warnings = _call_model_as_json(
        instructions=instructions,
        payload=payload,
        max_tokens=min(runtime['mapping_max_tokens'], 192),
    )
    if not isinstance(raw_response, dict):
        return None, warnings

    if allow_multiple:
        enum_values = [
            str(value).strip()
            for value in raw_response.get('enum_values', [])
            if str(value).strip() in set(enum_map.values())
        ] if isinstance(raw_response.get('enum_values'), list) else []
        if not enum_values:
            single_value = str(raw_response.get('enum_value') or '').strip()
            if single_value in set(enum_map.values()):
                enum_values = [single_value]
        if not enum_values:
            return None, warnings
        return {
            'status': str(raw_response.get('status') or 'resolved'),
            'enum_values': list(dict.fromkeys(enum_values)),
            'confidence': _normalize_confidence_score(raw_response.get('confidence')),
            'reason': str(raw_response.get('reason') or 'form_repair_model'),
        }, warnings

    enum_value = str(raw_response.get('enum_value') or '').strip()
    if enum_value not in set(enum_map.values()):
        return None, warnings
    return {
        'status': str(raw_response.get('status') or 'resolved'),
        'enum_value': enum_value,
        'confidence': _normalize_confidence_score(raw_response.get('confidence')),
        'reason': str(raw_response.get('reason') or 'form_repair_model'),
    }, warnings


def _model_enabled() -> bool:
    runtime = _get_model_runtime_config()
    return runtime['provider'] == 'gigachat'


def _form_repair_enabled() -> bool:
    runtime = _get_model_runtime_config()
    if runtime['provider'] != 'gigachat':
        return False
    return bool(runtime.get('api_key') or runtime.get('gigachat_auth_key'))


def _call_model_as_json(
    *,
    instructions: str,
    payload: dict[str, Any],
    max_tokens: int,
) -> tuple[Any | None, list[str]]:
    runtime = _get_model_runtime_config()
    try:
        content = _call_model(instructions=instructions, payload=payload, runtime=runtime, max_tokens=max_tokens)
    except RuntimeError as exc:
        logger.warning('model call failed: provider=%s model=%s error=%s', runtime['provider'], runtime['model_name'], exc)
        return None, [str(exc)]

    logger.info(
        'llm content received: provider=%s model=%s raw_length=%d preview=%s',
        runtime['provider'],
        runtime['model_name'],
        len(content),
        _preview_text(content),
    )
    parsed = _extract_json_payload(content)
    if parsed is None:
        logger.warning(
            'llm response parse failed: provider=%s model=%s raw_length=%d preview=%s',
            runtime['provider'],
            runtime['model_name'],
            len(content),
            _preview_text(content),
        )
        return None, ['Модель вернула ответ, который не удалось разобрать как JSON.']
    return parsed, []


def _call_model(*, instructions: str, payload: dict[str, Any], runtime: dict[str, Any], max_tokens: int) -> str:
    provider = str(runtime['provider'])
    base_url = str(runtime['base_url'])
    model_name = str(runtime['model_name'])
    api_key = str(runtime['api_key'])
    timeout_seconds = float(runtime['timeout_seconds'])
    user_content = _compact_json_text(payload)
    ssl_context = _build_ssl_context(runtime)

    if provider == 'gigachat':
        access_token = api_key or _get_gigachat_access_token(runtime)
        logger.info(
            'llm transport gigachat: url=%s model=%s timeout_seconds=%.1f max_tokens=%d prompt_bytes=%d token_source=%s',
            f'{base_url.rstrip("/")}/chat/completions',
            model_name,
            timeout_seconds,
            max_tokens,
            len(user_content),
            'env_token' if api_key else 'oauth',
        )
        request_payload = {
            'model': model_name,
            'temperature': 0,
            'max_tokens': max_tokens,
            'messages': [
                {'role': 'system', 'content': instructions},
                {'role': 'user', 'content': user_content},
            ],
        }
        request_kwargs: dict[str, Any] = {
            'headers': {'Authorization': _as_bearer_header(access_token)},
            'timeout_seconds': timeout_seconds,
        }
        if ssl_context is not None:
            request_kwargs['ssl_context'] = ssl_context
        response = _post_json(
            f'{base_url.rstrip("/")}/chat/completions',
            request_payload,
            **request_kwargs,
        )
        choices = response.get('choices') if isinstance(response, dict) else None
        if not isinstance(choices, list) or not choices:
            raise RuntimeError('GigaChat не вернул choices.')
        message = choices[0].get('message') if isinstance(choices[0], dict) else None
        content = _extract_openai_message_content(message)
        if not isinstance(content, str) or not content.strip():
            logger.warning(
                'gigachat content missing: model=%s message_type=%s content_type=%s choice_keys=%s',
                model_name,
                type(message).__name__,
                type(message.get('content')).__name__ if isinstance(message, dict) else 'missing',
                ','.join(sorted(choices[0].keys())) if isinstance(choices[0], dict) else 'n/a',
            )
            raise RuntimeError('GigaChat вернул пустой ответ.')
        return content

    raise RuntimeError('Провайдер модели не настроен. Поддерживается только GigaChat.')


def _get_gigachat_access_token(runtime: dict[str, Any]) -> str:
    auth_key = str(runtime.get('gigachat_auth_key') or '').strip()
    auth_url = str(runtime.get('gigachat_auth_url') or GIGACHAT_DEFAULT_AUTH_URL).strip()
    scope = str(runtime.get('gigachat_scope') or 'GIGACHAT_API_PERS').strip() or 'GIGACHAT_API_PERS'
    auth_scheme = str(runtime.get('gigachat_auth_scheme') or 'Basic').strip() or 'Basic'
    timeout_seconds = float(runtime['timeout_seconds'])

    if not auth_key:
        raise RuntimeError('Для GigaChat не настроен ключ авторизации. Заполните TSGEN_GIGACHAT_AUTH_KEY.')

    auth_header = _build_gigachat_auth_header(auth_key=auth_key, auth_scheme=auth_scheme)
    logger.info(
        'gigachat oauth auth header prepared: scheme=%s key_mode=%s payload_preview=%s',
        auth_scheme,
        _describe_gigachat_auth_key(auth_key),
        _preview_text(auth_header, limit=48),
    )

    now = time.time()
    scope_candidates = _get_gigachat_scope_candidates(scope)

    for candidate_scope in scope_candidates:
        cache_key = _gigachat_cache_key(
            auth_url=auth_url,
            scope=candidate_scope,
            auth_scheme=auth_scheme,
            auth_key=auth_key,
        )
        cached = _gigachat_token_cache.get(cache_key)
        if cached is not None:
            cached_token = str(cached.get('access_token') or '').strip()
            cached_expires_at = float(cached.get('expires_at') or 0)
            if cached_token and cached_expires_at - GIGACHAT_TOKEN_LEEWAY_SECONDS > now:
                logger.info(
                    'gigachat oauth token cache hit: auth_url=%s scope=%s expires_in=%ds',
                    auth_url,
                    candidate_scope,
                    int(max(cached_expires_at - now, 0)),
                )
                return cached_token

    last_error: RuntimeError | None = None
    for candidate_scope in scope_candidates:
        logger.info(
            'gigachat oauth token request: auth_url=%s scope=%s timeout_seconds=%.1f',
            auth_url,
            candidate_scope,
            timeout_seconds,
        )
        request_kwargs: dict[str, Any] = {
            'payload': {'scope': candidate_scope},
            'headers': {
                'Accept': 'application/json',
                'RqUID': str(uuid.uuid4()),
                'Authorization': auth_header,
            },
            'timeout_seconds': timeout_seconds,
        }
        ssl_context = _build_ssl_context(runtime)
        if ssl_context is not None:
            request_kwargs['ssl_context'] = ssl_context

        try:
            response = _post_form_json(
                auth_url,
                **request_kwargs,
            )
        except RuntimeError as exc:
            last_error = exc
            if _is_gigachat_scope_mismatch_error(str(exc)) and candidate_scope != scope_candidates[-1]:
                logger.warning(
                    'gigachat oauth scope rejected: requested_scope=%s error=%s next_scope=%s',
                    candidate_scope,
                    exc,
                    scope_candidates[scope_candidates.index(candidate_scope) + 1],
                )
                continue
            raise

        access_token = str(response.get('access_token') or '').strip()
        if not access_token:
            raise RuntimeError('GigaChat OAuth не вернул access_token.')

        expires_at = _normalize_gigachat_expires_at(response.get('expires_at'), now=now)
        cache_key = _gigachat_cache_key(
            auth_url=auth_url,
            scope=candidate_scope,
            auth_scheme=auth_scheme,
            auth_key=auth_key,
        )
        _gigachat_token_cache[cache_key] = {'access_token': access_token, 'expires_at': expires_at}
        logger.info(
            'gigachat oauth token refreshed: auth_url=%s scope=%s expires_in=%ds',
            auth_url,
            candidate_scope,
            int(max(expires_at - now, 0)),
        )
        if candidate_scope != scope:
            logger.warning(
                'gigachat oauth scope fallback succeeded: configured_scope=%s actual_scope=%s',
                scope,
                candidate_scope,
            )
        return access_token

    if last_error is not None:
        raise last_error
    raise RuntimeError('GigaChat OAuth не смог подобрать рабочий scope.')


def _post_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: float,
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, Any]:
    request_headers = {'Content-Type': 'application/json', **headers}
    logger.info(
        'llm http request: url=%s timeout_seconds=%.1f has_auth=%s payload_keys=%s',
        url,
        timeout_seconds,
        'Authorization' in request_headers,
        ','.join(sorted(payload.keys())),
    )
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode('utf-8'),
        headers=request_headers,
        method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=ssl_context) as response:
            body = response.read().decode('utf-8')
            logger.info(
                'llm http response: url=%s status=%s body_length=%d',
                url,
                getattr(response, 'status', 'unknown'),
                len(body),
            )
    except (TimeoutError, socket.timeout) as exc:
        logger.warning('llm timeout: url=%s timeout_seconds=%.1f error=%s', url, timeout_seconds, exc)
        raise RuntimeError(f'Локальная модель не ответила вовремя за {timeout_seconds:.0f} сек.') from exc
    except urllib.error.HTTPError as exc:
        body = exc.read().decode('utf-8', errors='replace')
        logger.warning('llm http error: url=%s status=%s body=%s', url, exc.code, body[:300])
        raise RuntimeError(f'Локальная модель вернула HTTP {exc.code}: {body[:300]}') from exc
    except urllib.error.URLError as exc:
        logger.warning('llm connection error: url=%s reason=%s', url, exc.reason)
        raise RuntimeError(f'Не удалось подключиться к локальной модели: {exc.reason}') from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError('Локальная модель вернула не-JSON ответ.') from exc

    if not isinstance(parsed, dict):
        raise RuntimeError('Локальная модель вернула неожиданный формат ответа.')
    return parsed


def _post_form_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    timeout_seconds: float,
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, Any]:
    request_headers = {'Content-Type': 'application/x-www-form-urlencoded', **headers}
    logger.info(
        'llm http form request: url=%s timeout_seconds=%.1f has_auth=%s payload_keys=%s',
        url,
        timeout_seconds,
        'Authorization' in request_headers,
        ','.join(sorted(payload.keys())),
    )
    request = urllib.request.Request(
        url,
        data=urllib.parse.urlencode(payload).encode('utf-8'),
        headers=request_headers,
        method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=ssl_context) as response:
            body = response.read().decode('utf-8')
            logger.info(
                'llm http form response: url=%s status=%s body_length=%d',
                url,
                getattr(response, 'status', 'unknown'),
                len(body),
            )
    except (TimeoutError, socket.timeout) as exc:
        logger.warning('llm form timeout: url=%s timeout_seconds=%.1f error=%s', url, timeout_seconds, exc)
        raise RuntimeError(f'GigaChat OAuth не ответил вовремя за {timeout_seconds:.0f} сек.') from exc
    except urllib.error.HTTPError as exc:
        body = exc.read().decode('utf-8', errors='replace')
        logger.warning('llm form http error: url=%s status=%s body=%s', url, exc.code, body[:300])
        raise RuntimeError(f'GigaChat OAuth вернул HTTP {exc.code}: {body[:300]}') from exc
    except urllib.error.URLError as exc:
        logger.warning('llm form connection error: url=%s reason=%s', url, exc.reason)
        raise RuntimeError(f'Не удалось подключиться к GigaChat OAuth: {exc.reason}') from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError('GigaChat OAuth вернул не-JSON ответ.') from exc

    if not isinstance(parsed, dict):
        raise RuntimeError('GigaChat OAuth вернул неожиданный формат ответа.')
    return parsed


def _get_model_runtime_config() -> dict[str, Any]:
    default_timeout_raw = os.getenv('TSGEN_MODEL_TIMEOUT_SECONDS', '20').strip()
    try:
        default_timeout = float(default_timeout_raw)
    except ValueError:
        default_timeout = 20.0
    runtime = {
        'provider': os.getenv('TSGEN_MODEL_PROVIDER', 'gigachat').strip().lower(),
        'base_url': os.getenv('TSGEN_MODEL_BASE_URL', GIGACHAT_DEFAULT_BASE_URL).strip(),
        'model_name': os.getenv('TSGEN_MODEL_NAME', 'GigaChat-2-Pro').strip(),
        'api_key': os.getenv('TSGEN_MODEL_API_KEY', '').strip(),
        'timeout_seconds': default_timeout,
        'sample_rows': _safe_env_int('TSGEN_MODEL_SAMPLE_ROWS', DEFAULT_SAMPLE_ROWS),
        'hint_limit': _safe_env_int('TSGEN_MODEL_HINT_LIMIT', DEFAULT_HINT_LIMIT),
        'mapping_max_tokens': _safe_env_int('TSGEN_MODEL_MAPPING_MAX_TOKENS', DEFAULT_MAPPING_MAX_TOKENS),
        'draft_max_tokens': _safe_env_int('TSGEN_MODEL_DRAFT_MAX_TOKENS', DEFAULT_DRAFT_MAX_TOKENS),
        'gigachat_auth_url': os.getenv('TSGEN_GIGACHAT_AUTH_URL', GIGACHAT_DEFAULT_AUTH_URL).strip(),
        'gigachat_auth_key': os.getenv('TSGEN_GIGACHAT_AUTH_KEY', '').strip(),
        'gigachat_scope': os.getenv('TSGEN_GIGACHAT_SCOPE', 'GIGACHAT_API_PERS').strip(),
        'gigachat_auth_scheme': os.getenv('TSGEN_GIGACHAT_AUTH_SCHEME', 'Basic').strip(),
        'gigachat_ca_bundle': os.getenv('TSGEN_GIGACHAT_CA_BUNDLE', DEFAULT_GIGACHAT_CA_BUNDLE).strip(),
        'gigachat_ssl_verify': _safe_env_bool('TSGEN_GIGACHAT_SSL_VERIFY', True),
    }
    if not runtime['base_url']:
        runtime['base_url'] = GIGACHAT_DEFAULT_BASE_URL
    if not runtime['model_name']:
        runtime['model_name'] = 'GigaChat-2-Pro'
    try:
        from storage import get_active_model_runtime

        active = get_active_model_runtime()
    except Exception as exc:  # noqa: BLE001
        logger.debug('failed to load active model runtime: %s', exc)
        active = None

    if isinstance(active, dict):
        provider = str(active.get('provider') or runtime['provider']).strip().lower()
        base_url = str(active.get('base_url') or runtime['base_url']).strip()
        model_name = str(active.get('model_name') or runtime['model_name']).strip()
        if provider == 'gigachat' and base_url and model_name:
            runtime.update(
                {
                    'provider': provider,
                    'base_url': base_url,
                    'model_name': model_name,
                }
            )
    return runtime


def _safe_env_int(name: str, default: int) -> int:
    raw_value = os.getenv(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return value if value > 0 else default


def _safe_env_bool(name: str, default: bool) -> bool:
    raw_value = os.getenv(name, 'true' if default else 'false').strip().lower()
    if raw_value in {'1', 'true', 'yes', 'on'}:
        return True
    if raw_value in {'0', 'false', 'no', 'off'}:
        return False
    return default


def _as_bearer_header(value: str) -> str:
    normalized = value.strip()
    if normalized.lower().startswith('bearer '):
        return normalized
    return f'Bearer {normalized}'


def _as_auth_scheme_header(value: str, scheme: str) -> str:
    normalized = value.strip()
    scheme_prefix = f'{scheme.strip()} '.strip()
    if normalized.lower().startswith(scheme_prefix.lower()):
        return normalized
    return f'{scheme_prefix}{normalized}'


def _build_gigachat_auth_header(*, auth_key: str, auth_scheme: str) -> str:
    scheme = (auth_scheme or 'Basic').strip() or 'Basic'
    raw_value = auth_key.strip()
    scheme_prefix = f'{scheme} '

    if raw_value.lower().startswith(scheme_prefix.lower()):
        raw_value = raw_value[len(scheme_prefix) :].strip()

    if not raw_value:
        raise RuntimeError('Ключ авторизации GigaChat пустой.')

    normalized_key = _normalize_gigachat_auth_key(raw_value)
    return f'{scheme} {normalized_key}'


def _normalize_gigachat_auth_key(value: str) -> str:
    raw_value = value.strip()
    if not raw_value:
        raise RuntimeError('Ключ авторизации GigaChat пустой.')

    if ':' in raw_value:
        return base64.b64encode(raw_value.encode('utf-8')).decode('ascii')

    try:
        decoded = base64.b64decode(raw_value, validate=True)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            'Ключ авторизации GigaChat имеет неверный формат. Ожидается Base64-ключ из кабинета или строка client_id:client_secret.'
        ) from exc

    try:
        decoded_text = decoded.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise RuntimeError('Ключ авторизации GigaChat не декодируется как UTF-8.') from exc

    if ':' not in decoded_text:
        raise RuntimeError(
            'Ключ авторизации GigaChat декодируется, но не содержит пару client_id:client_secret. Проверь ключ из кабинета.'
        )

    return raw_value


def _describe_gigachat_auth_key(value: str) -> str:
    raw_value = value.strip()
    if not raw_value:
        return 'empty'
    if ':' in raw_value:
        return 'raw_pair'
    try:
        decoded = base64.b64decode(raw_value, validate=True).decode('utf-8')
    except Exception:  # noqa: BLE001
        return 'invalid'
    if ':' in decoded:
        return 'base64_pair'
    return 'base64_other'


def _gigachat_cache_key(*, auth_url: str, scope: str, auth_scheme: str, auth_key: str) -> str:
    return f'{auth_scheme}|{auth_url}|{scope}|{hash(auth_key)}'


def _get_gigachat_scope_candidates(primary_scope: str) -> list[str]:
    normalized = primary_scope.strip() or 'GIGACHAT_API_PERS'
    candidates = [normalized]
    for fallback_scope in ('GIGACHAT_API_CORP', 'GIGACHAT_API_B2B', 'GIGACHAT_API_PERS'):
        if fallback_scope not in candidates:
            candidates.append(fallback_scope)
    return candidates


def _is_gigachat_scope_mismatch_error(message: str) -> bool:
    normalized = message.lower()
    return 'scope from db not fully includes consumed scope' in normalized


def _normalize_gigachat_expires_at(value: Any, *, now: float) -> float:
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 1_000_000_000_000:
            return numeric / 1000.0
        if numeric > now + 60:
            return numeric
    if isinstance(value, str):
        try:
            numeric = float(value.strip())
        except ValueError:
            numeric = 0.0
        if numeric > 1_000_000_000_000:
            return numeric / 1000.0
        if numeric > now + 60:
            return numeric
    return now + (30 * 60)


def _build_ssl_context(runtime: dict[str, Any]) -> ssl.SSLContext | None:
    verify_ssl = bool(runtime.get('gigachat_ssl_verify', True))
    ca_bundle = str(runtime.get('gigachat_ca_bundle') or '').strip()

    if not verify_ssl:
        logger.warning('gigachat ssl verification disabled: this should only be used for debugging')
        insecure_context = ssl.create_default_context()
        insecure_context.check_hostname = False
        insecure_context.verify_mode = ssl.CERT_NONE
        return insecure_context

    resolved_ca_bundle = _resolve_ca_bundle_path(ca_bundle)
    if resolved_ca_bundle is not None:
        logger.info('gigachat ssl context: using custom ca bundle path=%s', resolved_ca_bundle)
        return ssl.create_default_context(cafile=str(resolved_ca_bundle))

    logger.info('gigachat ssl context: using system trust store')
    return None


def _resolve_ca_bundle_path(raw_path: str) -> Path | None:
    normalized = raw_path.strip()
    if not normalized:
        return None

    candidate = Path(normalized)
    if candidate.exists():
        return candidate

    local_fallback = Path(__file__).parent / 'certs' / candidate.name
    if local_fallback.exists():
        return local_fallback

    return None


def _compact_json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def _truncate_sample_rows(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    truncated: list[dict[str, Any]] = []
    for row in rows[:limit]:
        if not isinstance(row, dict):
            continue
        normalized_row: dict[str, Any] = {}
        for key, value in list(row.items())[:12]:
            if isinstance(value, str):
                normalized_row[str(key)] = value[:80]
            else:
                normalized_row[str(key)] = value
        truncated.append(normalized_row)
    return truncated


def _compact_hints(hints: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    compacted: list[dict[str, Any]] = []
    for hint in hints[:limit]:
        if not isinstance(hint, dict):
            continue
        compacted.append(
            {
                'source_field': hint.get('source_field'),
                'target_field': hint.get('target_field'),
                'score': hint.get('score'),
                'confidence': hint.get('confidence'),
                'reason': hint.get('reason'),
                'source_of_truth': hint.get('source_of_truth'),
            }
        )
    return compacted


def _compact_sample_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:80]
    return value


def _extract_openai_message_content(message: Any) -> str | None:
    if not isinstance(message, dict):
        return None

    content = message.get('content')
    if isinstance(content, str):
        stripped = content.strip()
        return stripped or None

    if isinstance(content, dict):
        text_value = content.get('text')
        if isinstance(text_value, str):
            stripped = text_value.strip()
            return stripped or None
        return _compact_json_text(content)

    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if isinstance(item, str):
                chunk = item.strip()
                if chunk:
                    chunks.append(chunk)
                continue
            if not isinstance(item, dict):
                continue
            text_value = item.get('text')
            if isinstance(text_value, str) and text_value.strip():
                chunks.append(text_value.strip())
                continue
            if item.get('type') == 'text' and isinstance(item.get('content'), str) and item['content'].strip():
                chunks.append(item['content'].strip())
        if chunks:
            return '\n'.join(chunks)
        return None

    return None


def _extract_json_payload(content: str) -> Any:
    content = content.strip()
    if not content:
        return None

    for candidate in _iter_json_candidates(content):
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue
    return None


def _iter_json_candidates(content: str) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    def add_candidate(value: str | None) -> None:
        if not value:
            return
        normalized = value.strip()
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        candidates.append(normalized)

    add_candidate(content)

    for match in MARKDOWN_JSON_FENCE_RE.finditer(content):
        add_candidate(match.group(1))

    for candidate in _extract_balanced_json_blocks(content):
        add_candidate(candidate)

    regex_match = REGEX_JSON_BLOCK_RE.search(content)
    if regex_match is not None:
        add_candidate(regex_match.group(1))

    return candidates


def _extract_balanced_json_blocks(content: str) -> list[str]:
    blocks: list[str] = []
    length = len(content)

    for start_index, start_char in enumerate(content):
        if start_char not in '{[':
            continue
        end_char = '}' if start_char == '{' else ']'
        depth = 0
        in_string = False
        escape_next = False

        for current_index in range(start_index, length):
            char = content[current_index]
            if in_string:
                if escape_next:
                    escape_next = False
                elif char == '\\':
                    escape_next = True
                elif char == '"':
                    in_string = False
                continue

            if char == '"':
                in_string = True
                continue
            if char == start_char:
                depth += 1
                continue
            if char == end_char:
                depth -= 1
                if depth == 0:
                    blocks.append(content[start_index : current_index + 1])
                    break

    return blocks


def _preview_text(value: str, limit: int = 800) -> str:
    compact = ' '.join(value.split())
    if len(compact) <= limit:
        return compact
    return f'{compact[:limit]}...'


def _normalize_confidence_score(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return max(0.0, min(1.0, float(value)))

    normalized = str(value).strip().lower()
    if normalized == 'high':
        return 0.92
    if normalized == 'medium':
        return 0.72
    if normalized == 'low':
        return 0.4
    if normalized == 'none':
        return 0.0
    try:
        return max(0.0, min(1.0, float(normalized)))
    except ValueError:
        return 0.0


def _normalize_confidence(value: Any) -> str:
    normalized = str(value or '').strip().lower()
    if normalized in {'high', 'medium', 'low', 'none'}:
        return normalized
    return 'medium'
