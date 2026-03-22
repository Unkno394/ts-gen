from __future__ import annotations

import base64
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s')

try:
    from llama_parse import LlamaParse  # type: ignore
except ImportError as exc:  # pragma: no cover - optional dependency
    LlamaParse = None
    LLAMAPARSE_IMPORT_ERROR = str(exc)
else:
    LLAMAPARSE_IMPORT_ERROR = None


app = FastAPI(title='LlamaParse Service')


class ExtractRequest(BaseModel):
    filename: str
    content_base64: str


@app.get('/health')
def health() -> dict[str, Any]:
    return {
        'status': 'ok',
        'llamaparse_available': LlamaParse is not None,
        'import_error': LLAMAPARSE_IMPORT_ERROR,
        'api_key_configured': bool(_get_api_key()),
    }


@app.post('/extract')
def extract(payload: ExtractRequest) -> dict[str, Any]:
    if LlamaParse is None:
        raise HTTPException(status_code=503, detail='llama_parse is not installed in this container.')
    api_key = _get_api_key()
    if not api_key:
        raise HTTPException(status_code=503, detail='LLAMAPARSE_API_KEY or LLAMA_CLOUD_API_KEY is not configured.')

    try:
        content = base64.b64decode(payload.content_base64)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f'Invalid base64 payload: {exc}') from exc

    suffix = Path(payload.filename).suffix or ''
    with tempfile.TemporaryDirectory(prefix='llamaparse-service-') as temp_dir:
        input_path = Path(temp_dir) / f'input{suffix}'
        input_path.write_bytes(content)
        logger.info('llamaparse extract started: filename=%s input=%s', payload.filename, input_path)

        try:
            parser = LlamaParse(
                api_key=api_key,
                result_type='markdown',
                language='ru',
                verbose=False,
            )
            documents = parser.load_data(str(input_path))
        except Exception as exc:  # noqa: BLE001
            logger.exception('llamaparse extract failed: filename=%s', payload.filename)
            raise HTTPException(status_code=502, detail=f'LlamaParse extraction failed: {exc}') from exc

        text_fragments: list[str] = []
        lines: list[dict[str, Any]] = []
        for index, document in enumerate(documents or [], start=1):
            page_text = _extract_document_text(document)
            if not page_text:
                continue
            page_number = _extract_page_number(document, fallback=index)
            text_fragments.append(page_text)
            for line_index, line in enumerate(page_text.splitlines(), start=1):
                normalized = str(line or '').strip()
                if not normalized:
                    continue
                lines.append(
                    {
                        'line_id': f'llamaparse-line-{page_number}-{line_index}',
                        'text': normalized,
                        'page': page_number,
                    }
                )

        text = '\n'.join(fragment for fragment in text_fragments if fragment).strip()
        if not text:
            raise HTTPException(status_code=422, detail='LlamaParse returned empty text.')

        logger.info(
            'llamaparse extract completed: filename=%s text_chars=%d line_items=%d document_count=%d',
            payload.filename,
            len(text),
            len(lines),
            len(documents or []),
        )
        return {
            'provider': 'llamaparse',
            'text': text,
            'lines': lines,
            'metadata': {
                'document_count': len(documents or []),
            },
        }


def _get_api_key() -> str:
    return str(
        os.getenv('LLAMAPARSE_API_KEY')
        or os.getenv('LLAMA_CLOUD_API_KEY')
        or ''
    ).strip()


def _extract_document_text(document: Any) -> str:
    if document is None:
        return ''
    text = getattr(document, 'text', None)
    if text:
        return str(text).strip()
    get_content = getattr(document, 'get_content', None)
    if callable(get_content):
        try:
            content = get_content()
        except TypeError:
            content = get_content(None)
        except Exception:  # noqa: BLE001
            content = None
        if content:
            return str(content).strip()
    return ''


def _extract_page_number(document: Any, *, fallback: int) -> int:
    metadata = getattr(document, 'metadata', None)
    if isinstance(metadata, dict):
        for key in ('page', 'page_number', 'page_num'):
            value = metadata.get(key)
            try:
                if value is not None:
                    return int(value)
            except (TypeError, ValueError):
                continue
    return fallback
