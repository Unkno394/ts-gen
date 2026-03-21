from __future__ import annotations

from typing import Any

IMAGE_TYPES = {'png', 'jpg', 'jpeg', 'bmp', 'gif', 'tif', 'tiff', 'webp'}
TEXT_MIN_LENGTH = 16


def classify_document(
    *,
    file_type: str,
    tables: list[dict[str, Any]] | None = None,
    raw_text: str = '',
    kv_pairs: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    tables = tables or []
    kv_pairs = kv_pairs or []
    text_length = len(raw_text.strip())

    if tables and (kv_pairs or text_length >= TEXT_MIN_LENGTH):
        return {'content_type': 'mixed', 'extraction_status': 'structured_extracted'}
    if tables:
        return {'content_type': 'table', 'extraction_status': 'structured_extracted'}
    if len(kv_pairs) >= 2:
        return {'content_type': 'form', 'extraction_status': 'text_extracted'}
    if text_length >= TEXT_MIN_LENGTH:
        return {'content_type': 'text', 'extraction_status': 'text_extracted'}
    if file_type in IMAGE_TYPES:
        return {'content_type': 'image_like', 'extraction_status': 'image_parse_not_supported_yet'}
    if file_type == 'pdf':
        return {'content_type': 'image_like', 'extraction_status': 'requires_ocr_or_manual_input'}
    return {'content_type': 'image_like', 'extraction_status': 'text_not_extracted'}
