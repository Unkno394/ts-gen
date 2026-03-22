from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib import error, request

logger = logging.getLogger(__name__)

LLAMAPARSE_ENABLED = os.getenv('LLAMAPARSE_ENABLED', '').strip().casefold() in {'1', 'true', 'yes', 'on'}
LLAMAPARSE_SERVICE_URL = os.getenv('LLAMAPARSE_SERVICE_URL', 'http://llamaparse:8030')
LLAMAPARSE_TIMEOUT_SECONDS = float(os.getenv('LLAMAPARSE_TIMEOUT_SECONDS', '120'))


def extract_text_from_llamaparse(file_path: str | Path) -> dict[str, Any] | None:
    if not LLAMAPARSE_ENABLED:
        return None

    path = Path(file_path)
    if not path.exists():
        logger.info('llamaparse client skipped missing file: path=%s', path)
        return None

    try:
        content = path.read_bytes()
    except OSError as exc:
        logger.warning('llamaparse client failed to read file: path=%s error=%s', path, exc)
        return None

    logger.info(
        'llamaparse request started: service_url=%s path=%s size_bytes=%d timeout_seconds=%s',
        LLAMAPARSE_SERVICE_URL,
        path,
        len(content),
        LLAMAPARSE_TIMEOUT_SECONDS,
    )
    payload = json.dumps(
        {
            'filename': path.name,
            'content_base64': base64.b64encode(content).decode('ascii'),
        }
    ).encode('utf-8')

    try:
        response = _post_json('/extract', payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning('llamaparse request failed: path=%s error=%s', path, exc)
        return None

    text = str(response.get('text') or '').strip()
    if not text:
        logger.warning('llamaparse response empty text: path=%s response_keys=%s', path, sorted(response.keys()))
        return None

    lines = []
    for index, item in enumerate(list(response.get('lines') or []), start=1):
        if not isinstance(item, dict):
            continue
        line_text = str(item.get('text') or '').strip()
        if not line_text:
            continue
        lines.append(
            {
                'id': str(item.get('line_id') or f'llamaparse-line-{index}'),
                'kind': 'line',
                'text': line_text,
                'label': 'llamaparse',
                'page': int(item.get('page') or 1),
                'source_type': 'line',
            }
        )

    logger.info(
        'llamaparse response received: path=%s text_chars=%d line_items=%d',
        path,
        len(text),
        len(lines),
    )
    return {
        'text': text,
        'blocks': lines,
        'warnings': ['Primary extraction used LlamaParse.'],
        'llamaparse_metadata': {
            'provider': str(response.get('provider') or 'llamaparse'),
            'line_count': len(lines),
            **dict(response.get('metadata') or {}),
        },
    }


def get_llamaparse_service_health() -> dict[str, Any]:
    try:
        return _get_json('/health')
    except Exception as exc:  # noqa: BLE001
        logger.warning('llamaparse service health check failed: error=%s', exc)
        return {'status': 'unavailable', 'llamaparse_available': False}


def _post_json(path: str, payload: bytes) -> dict[str, Any]:
    url = f'{LLAMAPARSE_SERVICE_URL.rstrip("/")}{path}'
    req = request.Request(
        url,
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with request.urlopen(req, timeout=LLAMAPARSE_TIMEOUT_SECONDS) as response:
            response_payload = response.read().decode('utf-8')
    except error.HTTPError as exc:
        error_payload = exc.read().decode('utf-8', errors='replace')
        logger.warning('llamaparse http error: url=%s status=%s body=%s', url, exc.code, error_payload)
        raise
    except Exception:
        logger.exception('llamaparse transport error: url=%s', url)
        raise
    return json.loads(response_payload)


def _get_json(path: str) -> dict[str, Any]:
    url = f'{LLAMAPARSE_SERVICE_URL.rstrip("/")}{path}'
    req = request.Request(url, method='GET')
    with request.urlopen(req, timeout=LLAMAPARSE_TIMEOUT_SECONDS) as response:
        response_payload = response.read().decode('utf-8')
    return json.loads(response_payload)
