from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import Any
from urllib import error, request

logger = logging.getLogger(__name__)

OCR_SERVICE_URL = os.getenv('OCR_SERVICE_URL', 'http://ocr:8010')
OCR_SERVICE_TIMEOUT_SECONDS = float(os.getenv('OCR_SERVICE_TIMEOUT_SECONDS', '20'))


def extract_text_from_ocr_service(file_path: str | Path) -> dict[str, Any] | None:
    path = Path(file_path)
    if not path.exists():
        logger.warning('ocr client skipped missing file: path=%s', path)
        return None

    try:
        content = path.read_bytes()
    except OSError as exc:
        logger.warning('ocr client failed to read file: path=%s error=%s', path, exc)
        return None

    logger.info(
        'ocr client request started: service_url=%s path=%s size_bytes=%d timeout_seconds=%s',
        OCR_SERVICE_URL,
        path,
        len(content),
        OCR_SERVICE_TIMEOUT_SECONDS,
    )

    payload = json.dumps(
        {
            'filename': path.name,
            'content_base64': base64.b64encode(content).decode('ascii'),
        }
    ).encode('utf-8')

    try:
        response = _post_json('/ocr/extract', payload)
    except Exception as exc:  # noqa: BLE001
        logger.warning('ocr service request failed: path=%s error=%s', path, exc)
        return None

    text = str(response.get('text') or '').strip()
    logger.info(
        'ocr client response received: path=%s text_chars=%d line_items=%d provider=%s',
        path,
        len(text),
        len(list(response.get('lines') or [])),
        str(response.get('provider') or 'unknown'),
    )
    if not text:
        logger.warning('ocr client response empty text: path=%s response_keys=%s', path, sorted(response.keys()))
        return None

    lines = []
    for index, item in enumerate(list(response.get('lines') or []), start=1):
        if not isinstance(item, dict):
            continue
        line_text = str(item.get('text') or '').strip()
        if not line_text:
            continue
        bbox = list(item.get('bbox') or [])
        x_values = [point[0] for point in bbox if isinstance(point, (list, tuple)) and len(point) >= 2]
        y_values = [point[1] for point in bbox if isinstance(point, (list, tuple)) and len(point) >= 2]
        confidence = _safe_float(item.get('confidence'))
        lines.append(
            {
                'id': str(item.get('line_id') or f'ocr-line-{index}'),
                'kind': 'line',
                'text': line_text,
                'label': 'ocr',
                'confidence': confidence,
                'page': int(item.get('page') or 1),
                'x': min(x_values) if x_values else None,
                'y': min(y_values) if y_values else None,
                'width': (max(x_values) - min(x_values)) if len(x_values) >= 2 else None,
                'height': (max(y_values) - min(y_values)) if len(y_values) >= 2 else None,
                'bbox': bbox,
                'source_type': 'line',
            }
        )

    return {
        'text': text,
        'blocks': lines,
        'warnings': [
            'Text was extracted via the external OCR service.',
        ],
        'ocr_metadata': {
            'provider': str(response.get('provider') or 'paddleocr'),
            'line_count': len(lines),
            'ocr_used': True,
        },
    }


def get_ocr_service_health() -> dict[str, Any]:
    try:
        health = _get_json('/health')
        logger.info('ocr service health response: service_url=%s payload=%s', OCR_SERVICE_URL, health)
        return health
    except Exception as exc:  # noqa: BLE001
        logger.warning('ocr service health check failed: error=%s', exc)
        return {'status': 'unavailable', 'paddleocr_available': False}


def _post_json(path: str, payload: bytes) -> dict[str, Any]:
    url = f'{OCR_SERVICE_URL.rstrip("/")}{path}'
    req = request.Request(
        url,
        data=payload,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with request.urlopen(req, timeout=OCR_SERVICE_TIMEOUT_SECONDS) as response:
            response_payload = response.read().decode('utf-8')
    except error.HTTPError as exc:
        error_payload = exc.read().decode('utf-8', errors='replace')
        logger.warning('ocr service http error: url=%s status=%s body=%s', url, exc.code, error_payload)
        raise
    except Exception:
        logger.exception('ocr service transport error: url=%s', url)
        raise
    return json.loads(response_payload)


def _get_json(path: str) -> dict[str, Any]:
    url = f'{OCR_SERVICE_URL.rstrip("/")}{path}'
    req = request.Request(url, method='GET')
    with request.urlopen(req, timeout=OCR_SERVICE_TIMEOUT_SECONDS) as response:
        response_payload = response.read().decode('utf-8')
    return json.loads(response_payload)


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
