from __future__ import annotations

from pathlib import Path
from typing import Any

from candidate_normalizer import build_source_candidates
from document_classifier import classify_document
from docx_parser import parse_docx
from form_layout import extract_layout_layer, understand_generic_form
from form_parser import extract_kv_pairs
from pdf_parser import parse_pdf
from text_parser import extract_sections, extract_text_facts, normalize_text, split_text_blocks


class UnsupportedFileTypeError(ValueError):
    pass


def empty_result(file_path: str | Path, file_type: str) -> dict[str, Any]:
    return {
        "file_name": Path(file_path).name,
        "file_type": file_type,
        "content_type": "unknown",
        "columns": [],
        "rows": [],
        "text": "",
        "blocks": [],
        "warnings": [],
    }


SUPPORTED_TYPES = {"pdf", "docx"}
IMAGE_TYPES = {'png', 'jpg', 'jpeg', 'bmp', 'gif', 'tif', 'tiff', 'webp'}
TEXT_TYPES = {'txt'}
SUPPORTED_TYPES = SUPPORTED_TYPES | IMAGE_TYPES | TEXT_TYPES


def parse_document(file_path: str | Path) -> dict[str, Any]:
    path = Path(file_path)
    ext = path.suffix.lower().lstrip(".")

    if ext not in SUPPORTED_TYPES:
        raise UnsupportedFileTypeError(
            f"Unsupported file type: {ext}. Supported: {sorted(SUPPORTED_TYPES)}"
        )

    if ext == "docx":
        base_result = parse_docx(path)
    elif ext == "pdf":
        base_result = parse_pdf(path)
    elif ext == 'txt':
        base_result = _parse_text_file(path)
    elif ext in IMAGE_TYPES:
        base_result = _parse_image_like_file(path)
    else:
        raise UnsupportedFileTypeError(f"Unsupported file type: {ext}")

    return _enrich_document_result(path=path, ext=ext, base_result=base_result)


def _parse_text_file(path: Path) -> dict[str, Any]:
    encodings_to_try = ['utf-8-sig', 'utf-8', 'cp1251', 'latin-1']
    last_error: Exception | None = None
    for encoding in encodings_to_try:
        try:
            text = path.read_text(encoding=encoding)
            return {
                'file_name': path.name,
                'file_type': 'txt',
                'columns': [],
                'rows': [],
                'tables': [],
                'text': text,
                'blocks': [],
                'warnings': [],
            }
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    raise UnsupportedFileTypeError(f'Failed to read TXT file: {last_error}')


def _parse_image_like_file(path: Path) -> dict[str, Any]:
    return {
        'file_name': path.name,
        'file_type': path.suffix.lower().lstrip('.'),
        'columns': [],
        'rows': [],
        'tables': [],
        'text': '',
        'blocks': [],
        'warnings': [
            'Document looks like an image or scan. Text extraction without OCR is not supported yet.',
        ],
    }


def _enrich_document_result(*, path: Path, ext: str, base_result: dict[str, Any]) -> dict[str, Any]:
    tables = [table for table in base_result.get('tables', []) if isinstance(table, dict)]
    raw_text = normalize_text(base_result.get('text', ''))
    layout_blocks = [dict(block) for block in base_result.get('blocks', []) if isinstance(block, dict)]
    text_blocks = split_text_blocks(raw_text)
    sections = extract_sections(raw_text)
    kv_pairs = extract_kv_pairs(raw_text)
    text_facts = extract_text_facts(raw_text)
    classification = classify_document(file_type=ext, tables=tables, raw_text=raw_text, kv_pairs=kv_pairs)
    source_candidates = build_source_candidates(
        tables=tables,
        kv_pairs=kv_pairs,
        text_facts=text_facts,
        sections=sections,
    )
    layout_layer = extract_layout_layer(
        file_path=path,
        file_type=ext,
        raw_text=raw_text,
        tables=tables,
        kv_pairs=kv_pairs,
        text_blocks=text_blocks,
        sections=sections,
        layout_blocks=layout_blocks,
    )
    form_model = understand_generic_form(
        layout_layer=layout_layer,
        tables=tables,
        kv_pairs=kv_pairs,
    )
    document_mode = 'data_table_mode'
    if isinstance(form_model, dict):
        layout_meta = form_model.get('layout_meta')
        if isinstance(layout_meta, dict) and layout_meta.get('document_mode') == 'form_layout_mode':
            document_mode = 'form_layout_mode'

    content_type = classification['content_type']
    extraction_status = classification['extraction_status']
    if document_mode == 'form_layout_mode' and isinstance(form_model, dict):
        scalar_count = len([item for item in form_model.get('scalars', []) if isinstance(item, dict)])
        group_count = len([item for item in form_model.get('groups', []) if isinstance(item, dict)])
        if scalar_count > 0 or group_count > 0:
            content_type = 'form'

    warnings = [str(warning) for warning in base_result.get('warnings', [])]
    if extraction_status == 'requires_ocr_or_manual_input':
        warnings.append('Text layer was not extracted. OCR or manual input is required.')
    elif extraction_status == 'text_not_extracted':
        warnings.append('Text could not be extracted from the document.')
    elif extraction_status == 'image_parse_not_supported_yet':
        warnings.append('Image-like files are detected, but OCR-free extraction is not supported yet.')

    if content_type == 'form' and kv_pairs:
        warnings.append(f'Detected {len(kv_pairs)} extracted field(s) from a semi-structured document.')
    elif content_type == 'text' and raw_text:
        warnings.append('Detected text document with extracted text blocks.')
    if document_mode == 'form_layout_mode':
        warnings.append('Detected form-like layout document. Form-aware extraction is enabled.')
        if tables and classification['content_type'] in {'table', 'mixed'}:
            warnings.append('PDF table extraction looked form-like, so preview was switched to form-aware extraction.')

    deduped_warnings: list[str] = []
    seen_warnings: set[str] = set()
    for warning in warnings:
        if warning in seen_warnings:
            continue
        seen_warnings.add(warning)
        deduped_warnings.append(warning)

    return {
        'file_name': base_result.get('file_name', path.name),
        'file_type': base_result.get('file_type', ext),
        'content_type': content_type,
        'extraction_status': extraction_status,
        'columns': [str(column) for column in base_result.get('columns', [])],
        'rows': [dict(row) for row in base_result.get('rows', []) if isinstance(row, dict)],
        'tables': tables,
        'text': raw_text,
        'text_blocks': text_blocks,
        'blocks': text_blocks,
        'sections': sections,
        'kv_pairs': kv_pairs,
        'text_facts': text_facts,
        'source_candidates': source_candidates,
        'document_mode': document_mode,
        'form_model': form_model,
        'warnings': deduped_warnings,
    }


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python document_parser.py /path/to/file.pdf")
        raise SystemExit(1)

    parsed = parse_document(sys.argv[1])
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
