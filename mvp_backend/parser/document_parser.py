from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

from candidate_normalizer import build_source_candidates
from document_classifier import classify_document
from docx_parser import parse_docx
from form_layout import extract_layout_layer, understand_generic_form
from form_parser import extract_kv_pairs
from llamaparse_client import extract_text_from_llamaparse
from ocr_client import extract_text_from_ocr_service
from pdf_parser import parse_pdf
from text_parser import extract_sections, extract_tables, extract_text_facts, normalize_text, split_text_blocks
from pdf_zoning import classify_pdf_document_zones


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
            tables = extract_tables(text)
            columns = [str(column) for column in tables[0].get('columns', [])] if tables else []
            rows = [dict(row) for row in tables[0].get('rows', []) if isinstance(row, dict)] if tables else []
            warnings: list[str] = []
            if len(tables) > 1:
                warnings.append(f'Found {len(tables)} tables in TXT.')
            return {
                'file_name': path.name,
                'file_type': 'txt',
                'columns': columns,
                'rows': rows,
                'tables': tables,
                'text': text,
                'blocks': [],
                'warnings': warnings,
            }
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    raise UnsupportedFileTypeError(f'Failed to read TXT file: {last_error}')


def _parse_image_like_file(path: Path) -> dict[str, Any]:
    primary_result = extract_text_from_llamaparse(path)
    if _has_usable_primary_text(primary_result):
        markdown_table = _extract_markdown_table_from_text(str(primary_result.get('text') or ''))
        return {
            'file_name': path.name,
            'file_type': path.suffix.lower().lstrip('.'),
            'columns': list(markdown_table.get('columns') or []),
            'rows': list(markdown_table.get('rows') or []),
            'tables': [markdown_table] if markdown_table else [],
            'text': str(primary_result.get('text') or ''),
            'blocks': [dict(block) for block in primary_result.get('blocks', []) if isinstance(block, dict)],
            'warnings': [str(warning) for warning in primary_result.get('warnings', [])],
            'ocr_used': False,
            'ocr_metadata': {},
            'has_primary_markdown_table': bool(markdown_table),
        }

    ocr_result = extract_text_from_ocr_service(path)
    if isinstance(ocr_result, dict) and str(ocr_result.get('text') or '').strip():
        warnings = [str(warning) for warning in ocr_result.get('warnings', [])]
        if primary_result is not None:
            warnings.insert(0, 'LlamaParse result was empty or low-quality for image input, so OCR fallback was used.')
        return {
            'file_name': path.name,
            'file_type': path.suffix.lower().lstrip('.'),
            'columns': [],
            'rows': [],
            'tables': [],
            'text': str(ocr_result.get('text') or ''),
            'blocks': [dict(block) for block in ocr_result.get('blocks', []) if isinstance(block, dict)],
            'warnings': warnings,
            'ocr_used': True,
            'ocr_metadata': dict(ocr_result.get('ocr_metadata') or {}),
        }
    return {
        'file_name': path.name,
        'file_type': path.suffix.lower().lstrip('.'),
        'columns': [],
        'rows': [],
        'tables': [],
        'text': '',
        'blocks': [],
        'warnings': [
            'Document looks like an image or scan. External OCR service did not return extractable text.',
        ],
        'ocr_used': False,
        'ocr_metadata': {},
    }


def _is_good_primary_document_result(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return False
    text = str(result.get('text') or '').strip()
    if not text:
        return False
    if len(text) < 80:
        non_empty_lines = [line for line in text.splitlines() if str(line).strip()]
        if len(non_empty_lines) < 3:
            return False
    bad_char_ratio = sum(1 for char in text if char in {'�', '\x00'}) / max(len(text), 1)
    if bad_char_ratio > 0.01:
        return False
    useful_chars = sum(1 for char in text if char.isalpha() or char.isdigit())
    if useful_chars < 40:
        return False
    return True


def _has_usable_primary_text(result: dict[str, Any] | None) -> bool:
    if not isinstance(result, dict):
        return False
    text = str(result.get('text') or '').strip()
    if not text:
        return False
    bad_char_ratio = sum(1 for char in text if char in {'�', '\x00'}) / max(len(text), 1)
    if bad_char_ratio > 0.05:
        return False
    useful_chars = sum(1 for char in text if char.isalpha() or char.isdigit())
    return useful_chars >= 8


def _extract_markdown_table_from_text(text: str) -> dict[str, Any] | None:
    lines = [str(line or '').strip() for line in str(text or '').splitlines()]
    table_lines: list[str] = []
    for line in lines:
        if _looks_like_markdown_table_row(line):
            table_lines.append(line)
        elif table_lines:
            break

    if len(table_lines) < 3:
        return None

    separator_index = next(
        (index for index, line in enumerate(table_lines) if _looks_like_markdown_separator_row(line)),
        None,
    )
    if separator_index != 1:
        return None

    header_cells = _split_markdown_table_row(table_lines[0])
    if len(header_cells) < 2:
        return None

    rows: list[dict[str, Any]] = []
    for line in table_lines[separator_index + 1:]:
        cells = _split_markdown_table_row(line)
        if len(cells) != len(header_cells):
            continue
        rows.append({header_cells[index]: cells[index] for index in range(len(header_cells))})

    if not rows:
        return None

    return {
        'name': 'LlamaParse markdown table',
        'columns': header_cells,
        'rows': rows,
    }


def _looks_like_markdown_table_row(line: str) -> bool:
    normalized = str(line or '').strip()
    return normalized.startswith('|') and normalized.endswith('|') and normalized.count('|') >= 3


def _looks_like_markdown_separator_row(line: str) -> bool:
    cells = _split_markdown_table_row(line)
    if len(cells) < 2:
        return False
    return all(re.fullmatch(r':?-{2,}:?', cell) for cell in cells)


def _split_markdown_table_row(line: str) -> list[str]:
    normalized = str(line or '').strip()
    if normalized.startswith('|'):
        normalized = normalized[1:]
    if normalized.endswith('|'):
        normalized = normalized[:-1]
    return [' '.join(cell.split()) for cell in normalized.split('|')]


def _enrich_document_result(*, path: Path, ext: str, base_result: dict[str, Any]) -> dict[str, Any]:
    tables = [table for table in base_result.get('tables', []) if isinstance(table, dict)]
    has_primary_markdown_table = bool(base_result.get('has_primary_markdown_table'))
    zone_summary = dict(base_result.get('zone_summary') or {})
    ocr_used = bool(base_result.get('ocr_used'))
    ocr_metadata = dict(base_result.get('ocr_metadata') or {})
    raw_text = normalize_text(base_result.get('text', ''))
    layout_blocks = [dict(block) for block in base_result.get('blocks', []) if isinstance(block, dict)]
    preview_columns = [str(column) for column in base_result.get('columns', [])]
    preview_rows = [dict(row) for row in base_result.get('rows', []) if isinstance(row, dict)]
    ocr_zone_summary: dict[str, Any] = {}
    if ocr_used and layout_blocks and (
        ext in IMAGE_TYPES or (ext == 'pdf' and bool(zone_summary.get('ocr_fallback_used')))
    ):
        raw_text, layout_blocks, ocr_zone_summary = _merge_ocr_image_results(raw_text=raw_text, layout_blocks=layout_blocks)
        ocr_metadata['zone_summary'] = ocr_zone_summary
        ocr_metadata['zone_counts'] = dict(ocr_zone_summary.get('counts') or {})
        if ext == 'pdf' and ocr_zone_summary:
            zone_summary['ocr_zone_summary'] = dict(ocr_zone_summary)
    ocr_table = None
    if ocr_used and layout_blocks:
        ocr_table = _extract_table_like_rows_from_ocr_blocks(layout_blocks)
        if ocr_table:
            tables = [ocr_table, *tables]
            preview_columns = [str(column) for column in list(ocr_table.get('columns') or [])]
            preview_rows = [dict(row) for row in list(ocr_table.get('rows') or []) if isinstance(row, dict)]
            ocr_metadata['table_reconstruction'] = {
                'column_count': len(preview_columns),
                'row_count': len(preview_rows),
                'column_types': dict(ocr_table.get('column_types') or {}),
                'header_detected': bool(ocr_table.get('header_detected')),
                'recovered_table_confidence': str(ocr_table.get('recovered_table_confidence') or 'low'),
                'row_stats': dict(ocr_table.get('row_stats') or {}),
            }
    text_blocks = split_text_blocks(raw_text)
    sections = extract_sections(raw_text)
    kv_pairs = extract_kv_pairs(raw_text)
    text_facts = extract_text_facts(raw_text)
    classification = classify_document(file_type=ext, tables=tables, raw_text=raw_text, kv_pairs=kv_pairs)
    if ext == 'pdf' and zone_summary:
        classification = {
            'content_type': str(zone_summary.get('content_type') or classification['content_type']),
            'extraction_status': str(zone_summary.get('extraction_status') or classification['extraction_status']),
        }
        raw_text, text_blocks, sections, kv_pairs, text_facts = _merge_pdf_zone_results(
            raw_text=raw_text,
            text_blocks=text_blocks,
            sections=sections,
            kv_pairs=kv_pairs,
            text_facts=text_facts,
            zone_summary=zone_summary,
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
    if has_primary_markdown_table:
        form_model = None
    document_mode = 'data_table_mode'
    if isinstance(form_model, dict) and not has_primary_markdown_table:
        layout_meta = form_model.get('layout_meta')
        if isinstance(layout_meta, dict) and layout_meta.get('document_mode') == 'form_layout_mode':
            document_mode = 'form_layout_mode'
            kv_pairs, text_facts = _suppress_consumed_group_fragments(
                kv_pairs=kv_pairs,
                text_facts=text_facts,
                form_model=form_model,
            )

    source_candidates = build_source_candidates(
        tables=tables,
        kv_pairs=kv_pairs,
        text_facts=text_facts,
        sections=sections,
    )

    content_type = classification['content_type']
    extraction_status = classification['extraction_status']
    if has_primary_markdown_table:
        content_type = 'table'
        extraction_status = 'structured_extracted'
    elif document_mode == 'form_layout_mode' and isinstance(form_model, dict):
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
        warnings.append('Image-like file was detected, but the external OCR service is unavailable or did not return text.')

    if content_type == 'form' and kv_pairs:
        warnings.append(f'Detected {len(kv_pairs)} extracted field(s) from a semi-structured document.')
    elif content_type == 'text' and raw_text:
        warnings.append('Detected text document with extracted text blocks.')
    if document_mode == 'form_layout_mode':
        warnings.append('Detected form-like layout document. Form-aware extraction is enabled.')
        if tables and classification['content_type'] in {'table', 'mixed'}:
            warnings.append('Extracted table looked form-like, so preview was switched to form-aware extraction.')
    if ocr_used:
        warnings.append('External OCR service was used for source extraction.')
        if ocr_table:
            warnings.append(
                'OCR table reconstruction detected '
                f'{len(preview_columns)} column(s) and {len(preview_rows)} row(s) '
                f'with {str(ocr_table.get("recovered_table_confidence") or "low")} confidence.'
            )
    if ext == 'pdf' and zone_summary:
        zone_counts = dict(zone_summary.get('counts') or {})
        warnings.append(
            'PDF zoning summary: '
            f"table={int(zone_counts.get('table') or 0)}, "
            f"form={int(zone_counts.get('form') or 0)}, "
            f"text={int(zone_counts.get('text') or 0)}, "
            f"noise={int(zone_counts.get('noise') or 0)}."
        )
        region_count = len([item for item in zone_summary.get('region_zones', []) if isinstance(item, dict)])
        warnings.append(f'PDF zoning regions: {region_count}.')

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
        'columns': preview_columns,
        'rows': preview_rows,
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
        'pdf_zone_summary': zone_summary if ext == 'pdf' else {},
        'ocr_used': ocr_used,
        'ocr_metadata': ocr_metadata,
        'warnings': deduped_warnings,
    }


def _merge_ocr_image_results(
    *,
    raw_text: str,
    layout_blocks: list[dict[str, Any]],
) -> tuple[str, list[dict[str, Any]], dict[str, Any]]:
    normalized_blocks = [_normalize_ocr_block(block, index=index) for index, block in enumerate(layout_blocks, start=1)]
    normalized_blocks = [block for block in normalized_blocks if str(block.get('text') or '').strip()]
    zone_summary = classify_pdf_document_zones(
        tables=[],
        layout_lines=[
            {
                'line_id': str(block.get('id') or ''),
                'text': str(block.get('text') or ''),
                'page': int(block.get('page') or 1),
                'column_id': block.get('column_id'),
                'y': _safe_float(block.get('y')),
                'x': _safe_float(block.get('x')),
            }
            for block in normalized_blocks
        ],
        raw_text=raw_text,
    )

    line_zone_map = {
        str(zone.get('line_id') or ''): dict(zone)
        for zone in list(zone_summary.get('line_zones') or [])
        if isinstance(zone, dict) and str(zone.get('line_id') or '')
    }
    region_zone_map: dict[str, dict[str, Any]] = {}
    preferred_regions = [
        zone
        for zone in list(zone_summary.get('region_zones') or [])
        if isinstance(zone, dict)
        and str(zone.get('zone_type') or '') in {'form', 'text'}
        and _safe_float(zone.get('zone_confidence')) is not None
        and float(zone.get('zone_confidence') or 0.0) >= 0.58
    ]
    if not preferred_regions:
        preferred_regions = [
            zone
            for zone in list(zone_summary.get('region_zones') or [])
            if isinstance(zone, dict) and str(zone.get('zone_type') or '') in {'form', 'text'}
        ]
    preferred_line_ids = {
        str(line_id).strip()
        for region in preferred_regions
        for line_id in list(region.get('line_ids') or [])
        if str(line_id).strip()
    }
    for region in [item for item in list(zone_summary.get('region_zones') or []) if isinstance(item, dict)]:
        for line_id in list(region.get('line_ids') or []):
            normalized_line_id = str(line_id).strip()
            if normalized_line_id:
                region_zone_map[normalized_line_id] = region

    selected_region_ids = {
        str(region.get('zone_id') or '').strip()
        for region in preferred_regions
        if str(region.get('zone_id') or '').strip()
    }
    dropped_low_confidence_lines = 0
    dropped_noise_lines = 0

    scored_blocks: list[dict[str, Any]] = []
    filtered_lines: list[str] = []
    for block in normalized_blocks:
        line_id = str(block.get('id') or '').strip()
        line_zone = dict(line_zone_map.get(line_id) or {})
        region_zone = dict(region_zone_map.get(line_id) or {})
        zone_type = str(line_zone.get('zone_type') or _classify_ocr_block(text=str(block.get('text') or ''), confidence=_safe_float(block.get('confidence'))))
        zone_confidence = _safe_float(line_zone.get('zone_confidence'))
        region_type = str(region_zone.get('zone_type') or '')
        region_confidence = _safe_float(region_zone.get('zone_confidence'))
        confidence = _safe_float(block.get('confidence'))
        selected_for_merge = True
        next_block = {
            **block,
            'label': str(block.get('label') or region_type or zone_type),
            'ocr_zone_type': zone_type,
            'ocr_zone_confidence': zone_confidence,
            'ocr_region_id': str(region_zone.get('zone_id') or ''),
            'ocr_region_type': region_type,
            'ocr_region_confidence': region_confidence,
        }
        if line_id and preferred_line_ids and line_id not in preferred_line_ids:
            selected_for_merge = False
        if zone_type == 'noise':
            dropped_noise_lines += 1
            selected_for_merge = False
        if (
            selected_for_merge
            and confidence is not None
            and confidence < 0.35
            and not _looks_like_ocr_checkbox_marker(str(block.get('text') or ''))
        ):
            dropped_low_confidence_lines += 1
            selected_for_merge = False
        next_block['ocr_selected_for_merge'] = selected_for_merge
        scored_blocks.append(next_block)
        if not selected_for_merge:
            continue
        filtered_lines.append(str(block.get('text') or '').strip())
    merged_text = normalize_text('\n'.join(filtered_lines)) or raw_text
    return merged_text, scored_blocks, {
        **dict(zone_summary),
        'ocr_zone_classification': True,
        'selected_region_ids': sorted(selected_region_ids),
        'merge_stats': {
            'input_line_count': len(normalized_blocks),
            'selected_line_count': len(filtered_lines),
            'dropped_low_confidence_lines': dropped_low_confidence_lines,
            'dropped_noise_lines': dropped_noise_lines,
        },
    }


def _normalize_ocr_block(block: dict[str, Any], *, index: int) -> dict[str, Any]:
    text = _normalize_ocr_text(block.get('text'))
    return {
        **block,
        'id': str(block.get('id') or f'ocr-layout-{index}'),
        'text': text,
        'page': int(block.get('page') or 1),
        'source_type': str(block.get('source_type') or 'line'),
    }


def _normalize_ocr_text(value: Any) -> str:
    text = str(value or '').strip()
    if not text:
        return ''
    text = ' '.join(text.replace('\n', ' ').replace('\r', ' ').split())
    if _looks_like_ocr_checkbox_marker(text):
        return 'X'
    prefixed_checked = _strip_ocr_checkbox_prefix(text, checked=True)
    if prefixed_checked is not None:
        return prefixed_checked
    prefixed_unchecked = _strip_ocr_checkbox_prefix(text, checked=False)
    if prefixed_unchecked is not None:
        return prefixed_unchecked
    return text


def _looks_like_ocr_checkbox_marker(text: str) -> bool:
    cleaned = str(text or '').strip()
    return cleaned in {'1', 'I', 'l', '|', '/', '\\', '■', '☑', '☒', '✓', '✔', 'V', 'v', 'X', 'x'}


def _strip_ocr_checkbox_prefix(text: str, *, checked: bool) -> str | None:
    cleaned = str(text or '').strip()
    marker_group = r'(?:1|I|l|\||/|\\|■|☑|☒|✓|✔|V|v|X|x)' if checked else r'(?:0|O|o|□|☐|○|◯)'
    match = __import__('re').match(rf'^\s*(?:\[\s*)?{marker_group}(?:\s*\])?\s+(.+)$', cleaned)
    if not match:
        return None
    prefix = 'X' if checked else '[ ]'
    return f"{prefix} {match.group(1).strip()}"


def _classify_ocr_block(*, text: str, confidence: float | None) -> str:
    normalized = ' '.join(str(text or '').split())
    lower = normalized.casefold()
    token_count = len(lower.split())
    uppercase_ratio = _uppercase_ratio(normalized)
    alnum_count = sum(1 for char in normalized if char.isalnum())
    punctuation_count = sum(1 for char in normalized if not char.isalnum() and not char.isspace())
    punctuation_ratio = punctuation_count / max(len(normalized), 1)
    if confidence is not None and confidence < 0.42:
        return 'noise'
    if alnum_count == 0:
        return 'noise'
    if len(normalized) <= 2 and not _looks_like_ocr_checkbox_marker(normalized):
        return 'noise'
    if punctuation_ratio >= 0.35 and token_count <= 4:
        return 'noise'
    if uppercase_ratio >= 0.82 and token_count <= 10:
        return 'noise'
    if any(marker in lower for marker in ('настоящее согласие', 'персональн', 'подпись', 'подписи сторон', 'банк вправе')):
        return 'noise'
    if ':' in normalized or token_count <= 8:
        return 'form'
    return 'text'


def _uppercase_ratio(value: str) -> float:
    letters = [char for char in str(value or '') if char.isalpha()]
    if not letters:
        return 0.0
    uppercase_letters = [char for char in letters if char.upper() == char]
    return len(uppercase_letters) / len(letters)


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _merge_pdf_zone_results(
    *,
    raw_text: str,
    text_blocks: list[dict[str, Any]],
    sections: list[dict[str, Any]],
    kv_pairs: list[dict[str, Any]],
    text_facts: list[dict[str, Any]],
    zone_summary: dict[str, Any],
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    parser_outputs = dict(zone_summary.get('parser_outputs') or {})
    text_output = dict(parser_outputs.get('text') or {})
    form_output = dict(parser_outputs.get('form') or {})
    noise_output = dict(parser_outputs.get('noise') or {})
    text_regions = [dict(item) for item in text_output.get('regions', []) if isinstance(item, dict)]
    form_regions = [dict(item) for item in form_output.get('regions', []) if isinstance(item, dict)]
    noise_regions = [dict(item) for item in noise_output.get('regions', []) if isinstance(item, dict)]
    if not text_regions and not form_regions and not noise_regions:
        region_zones = [dict(item) for item in zone_summary.get('region_zones', []) if isinstance(item, dict)]
    else:
        region_zones = text_regions + form_regions + noise_regions
    if not region_zones:
        return raw_text, text_blocks, sections, kv_pairs, text_facts

    non_noise_regions = [zone for zone in region_zones if str(zone.get('zone_type') or '') in {'text', 'form'}]
    if not text_regions:
        text_regions = [zone for zone in region_zones if str(zone.get('zone_type') or '') == 'text']
    if not noise_regions:
        noise_regions = [zone for zone in region_zones if str(zone.get('zone_type') or '') == 'noise']

    preferred_regions = text_regions or non_noise_regions
    preferred_text = '\n\n'.join(str(zone.get('text') or '').strip() for zone in preferred_regions if str(zone.get('text') or '').strip())
    merged_text = normalize_text(preferred_text) or raw_text

    merged_text_blocks = []
    for index, zone in enumerate(non_noise_regions, start=1):
        text = str(zone.get('text') or '').strip()
        if not text:
            continue
        merged_text_blocks.append(
            {
                'id': str(zone.get('zone_id') or f'pdf-zone-{index}'),
                'kind': 'paragraph',
                'text': text,
                'label': str(zone.get('zone_type') or ''),
            }
        )
    if not merged_text_blocks:
        merged_text_blocks = text_blocks

    merged_sections = extract_sections(merged_text) if merged_text else sections
    filtered_kv_pairs = _filter_pdf_structured_items_by_zones(
        items=kv_pairs,
        allowed_regions=non_noise_regions,
        excluded_regions=noise_regions,
    )
    filtered_text_facts = _filter_pdf_structured_items_by_zones(
        items=text_facts,
        allowed_regions=text_regions or non_noise_regions,
        excluded_regions=noise_regions,
    )
    return merged_text, merged_text_blocks, merged_sections, filtered_kv_pairs, filtered_text_facts


def _filter_pdf_structured_items_by_zones(
    *,
    items: list[dict[str, Any]],
    allowed_regions: list[dict[str, Any]],
    excluded_regions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not items:
        return items
    allowed_texts = [_normalize_form_text(zone.get('text')) for zone in allowed_regions if _normalize_form_text(zone.get('text'))]
    excluded_texts = [_normalize_form_text(zone.get('text')) for zone in excluded_regions if _normalize_form_text(zone.get('text'))]
    if not allowed_texts and not excluded_texts:
        return items

    filtered: list[dict[str, Any]] = []
    for item in items:
        label = _normalize_form_text(item.get('label'))
        value = _normalize_form_text(item.get('value'))
        combined = ' '.join(part for part in (label, value) if part).strip()
        if not combined:
            continue
        if any(excluded and combined in excluded for excluded in excluded_texts):
            continue
        if allowed_texts and not any(combined in allowed or label and label in allowed or value and value in allowed for allowed in allowed_texts):
            continue
        filtered.append(item)
    return filtered


def _suppress_consumed_group_fragments(
    *,
    kv_pairs: list[dict[str, Any]],
    text_facts: list[dict[str, Any]],
    form_model: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    consumed_option_texts: set[str] = set()
    consumed_question_texts: set[str] = set()

    for group in [item for item in form_model.get('groups', []) if isinstance(item, dict)]:
        question_text = _normalize_form_text(group.get('question'))
        if question_text:
            consumed_question_texts.add(question_text)
        for option in [item for item in group.get('options', []) if isinstance(item, dict)]:
            option_text = _normalize_form_text(option.get('label'))
            if option_text:
                consumed_option_texts.add(option_text)

    if not consumed_option_texts and not consumed_question_texts:
        return kv_pairs, text_facts

    filtered_pairs = [
        pair
        for pair in kv_pairs
        if not _is_consumed_group_fragment(
            label=pair.get('label'),
            value=pair.get('value'),
            consumed_option_texts=consumed_option_texts,
            consumed_question_texts=consumed_question_texts,
        )
    ]
    filtered_facts = [
        fact
        for fact in text_facts
        if not _is_consumed_group_fragment(
            label=fact.get('label'),
            value=fact.get('value'),
            consumed_option_texts=consumed_option_texts,
            consumed_question_texts=consumed_question_texts,
        )
    ]
    return filtered_pairs, filtered_facts


def _is_consumed_group_fragment(
    *,
    label: Any,
    value: Any,
    consumed_option_texts: set[str],
    consumed_question_texts: set[str],
) -> bool:
    normalized_label = _normalize_form_text(label)
    normalized_value = _normalize_form_text(value)
    combined = ' '.join(part for part in [normalized_label, normalized_value] if part).strip()
    if not combined:
        return False

    if any(option_text and option_text in combined for option_text in consumed_option_texts):
        return True

    marker_like = normalized_label in {'x', '[x]', 'v', '[v]', '✓', '✔', '☒', '☑', 'да', 'нет', 'yes', 'no'}
    if marker_like and any(question_text and question_text in combined for question_text in consumed_question_texts):
        return True

    return False


def _normalize_form_text(value: Any) -> str:
    return ' '.join(str(value or '').strip().casefold().split())


def _extract_table_like_rows_from_ocr_blocks(layout_blocks: list[dict[str, Any]]) -> dict[str, Any] | None:
    row_candidates = _collect_ocr_table_row_candidates(layout_blocks)

    if len(row_candidates) < 2:
        return None

    cell_count_frequency: dict[int, int] = {}
    for row in row_candidates:
        cell_count = len(row['cells'])
        cell_count_frequency[cell_count] = cell_count_frequency.get(cell_count, 0) + 1
    dominant_cell_count = max(
        cell_count_frequency,
        key=lambda value: (cell_count_frequency.get(value, 0), value),
    )
    if dominant_cell_count < 2 or cell_count_frequency.get(dominant_cell_count, 0) < 2:
        return None

    normalized_rows = [
        row for row in row_candidates if len(row['cells']) == dominant_cell_count and _is_valid_ocr_table_row(row['cells'])
    ]
    normalized_rows.sort(key=lambda item: (item['page'], item['y']))
    if len(normalized_rows) < 2:
        return None

    header_detected = _looks_like_ocr_header_row(normalized_rows[0]['cells'])
    anchor_positions = _extract_header_anchor_positions(normalized_rows[0]) if header_detected else []
    data_rows = normalized_rows[1:] if header_detected else normalized_rows
    if header_detected and anchor_positions:
        data_rows = [_apply_header_anchors_to_row(row, anchor_positions) for row in data_rows]
    if len(data_rows) < 1:
        return None

    columns = _build_ocr_table_columns(
        normalized_rows[0]['cells'] if header_detected else [f'column_{index}' for index in range(1, dominant_cell_count + 1)]
    )
    data_rows = [row for row in data_rows if len(row['cells']) == len(columns) and _is_table_eligible_ocr_row(row['cells'], header_detected=False)]
    if not data_rows:
        return None
    column_types = _infer_ocr_column_types(data_rows, columns)

    rows: list[dict[str, Any]] = []
    for row in data_rows:
        values: dict[str, Any] = {}
        non_empty_values = 0
        for column_index, column in enumerate(columns):
            raw_cell = row['cells'][column_index] if column_index < len(row['cells']) else ''
            repaired_cell = _repair_ocr_cell_by_type(raw_cell, column_types.get(column, 'text'))
            if repaired_cell:
                non_empty_values += 1
            values[column] = repaired_cell
        if non_empty_values == 0:
            continue
        rows.append(values)

    if not rows:
        return None

    dropped_rows = max(len(row_candidates) - (len(rows) + (1 if header_detected else 0)), 0)
    shape_stability = len(normalized_rows) / max(len(row_candidates), 1)
    if shape_stability >= 0.8 and dropped_rows <= 1:
        recovered_table_confidence = 'high'
    elif shape_stability >= 0.55:
        recovered_table_confidence = 'medium'
    else:
        recovered_table_confidence = 'low'

    return {
        'name': 'OCR reconstructed table',
        'columns': columns,
        'rows': rows,
        'column_types': column_types,
        'header_detected': header_detected,
        'recovered_table_confidence': recovered_table_confidence,
        'row_stats': {
            'accepted_rows': len(rows),
            'dropped_rows': dropped_rows,
            'candidate_rows': len(row_candidates),
            'shape_stability': round(shape_stability, 3),
        },
    }


def _normalize_ocr_table_cell(value: Any) -> str:
    normalized = ' '.join(str(value or '').replace('\n', ' ').replace('\r', ' ').split())
    if not normalized:
        return ''
    normalized = normalized.strip('|').strip()
    if not normalized:
        return ''
    return normalized


def _is_valid_ocr_table_row(cells: list[str]) -> bool:
    if len(cells) < 2 or len(cells) > 8:
        return False
    meaningful_cells = [cell for cell in cells if cell and not _is_noise_like_ocr_table_cell(cell)]
    if len(meaningful_cells) < max(2, len(cells) // 2):
        return False
    return _is_table_eligible_ocr_row(cells, header_detected=False) or _looks_like_ocr_header_row(cells)


def _is_noise_like_ocr_table_cell(value: str) -> bool:
    normalized = str(value or '').strip()
    if not normalized:
        return True
    if len(normalized) == 1 and normalized not in {'X', '[ ]'} and not normalized.isdigit() and not normalized.isalpha():
        return True
    if not any(char.isalnum() for char in normalized):
        return True
    return False


def _looks_like_ocr_header_row(cells: list[str]) -> bool:
    if len(cells) < 2:
        return False
    alpha_rich_cells = 0
    numeric_like_cells = 0
    unique_cells: set[str] = set()
    for cell in cells:
        normalized = ' '.join(str(cell or '').split())
        if not normalized:
            return False
        unique_cells.add(normalized.casefold())
        if _looks_like_date_value(normalized) or _looks_like_numeric_value(normalized):
            numeric_like_cells += 1
        elif any(char.isalpha() for char in normalized):
            alpha_rich_cells += 1
    return len(unique_cells) == len(cells) and alpha_rich_cells >= max(2, len(cells) - 1) and numeric_like_cells <= 1


def _is_table_eligible_ocr_row(cells: list[str], *, header_detected: bool) -> bool:
    if len(cells) < 2:
        return False
    non_empty_cells = [cell for cell in cells if str(cell or '').strip()]
    if len(non_empty_cells) < 2:
        return False
    alpha_cells = 0
    numeric_cells = 0
    date_cells = 0
    for cell in non_empty_cells:
        normalized = str(cell or '').strip()
        if _looks_like_date_value(normalized):
            date_cells += 1
        elif _looks_like_numeric_value(normalized):
            numeric_cells += 1
        elif any(char.isalpha() for char in normalized):
            alpha_cells += 1
    if header_detected:
        return alpha_cells >= max(2, len(non_empty_cells) - 1)
    return alpha_cells >= 1 and (numeric_cells + date_cells) >= 1


def _collect_ocr_table_row_candidates(layout_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    row_candidates: list[dict[str, Any]] = []
    seen_signatures: set[tuple[int, str]] = set()
    for index, block in enumerate(layout_blocks, start=1):
        if block.get('ocr_selected_for_merge') is False:
            continue
        text = ' '.join(str(block.get('text') or '').split())
        if '|' not in text:
            continue
        cells = [cell for cell in (_normalize_ocr_table_cell(part) for part in text.split('|')) if cell]
        if len(cells) < 2:
            continue
        signature = (int(block.get('page') or 1), '|'.join(cells).casefold())
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        row_candidates.append(
            {
                'row_id': str(block.get('id') or f'ocr-row-{index}'),
                'text': text,
                'cells': cells,
                'cell_positions': _build_fallback_cell_positions(cells, block),
                'page': int(block.get('page') or 1),
                'y': _safe_float(block.get('y')) or float(index),
            }
        )

    for band_index, band in enumerate(_cluster_ocr_row_bands(layout_blocks), start=1):
        cells = [item['text'] for item in band['cells'] if str(item.get('text') or '').strip()]
        if len(cells) < 2:
            continue
        signature = (int(band['page']), '|'.join(cells).casefold())
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        row_candidates.append(
            {
                'row_id': f'ocr-band-{band_index}',
                'text': ' | '.join(cells),
                'cells': cells,
                'cell_positions': [dict(item) for item in band['cells']],
                'page': int(band['page']),
                'y': float(band['y']),
            }
        )

    return row_candidates


def _cluster_ocr_row_bands(layout_blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    positioned_blocks = [
        block
        for block in layout_blocks
        if block.get('ocr_selected_for_merge') is not False
        and _safe_float(block.get('x')) is not None
        and _safe_float(block.get('y')) is not None
        and str(block.get('text') or '').strip()
    ]
    positioned_blocks.sort(key=lambda item: (int(item.get('page') or 1), _safe_float(item.get('y')) or 0.0, _safe_float(item.get('x')) or 0.0))
    bands: list[dict[str, Any]] = []
    for block in positioned_blocks:
        page = int(block.get('page') or 1)
        y = float(_safe_float(block.get('y')) or 0.0)
        height = float(_safe_float(block.get('height')) or 12.0)
        assigned_band = None
        for band in bands:
            threshold = max(8.0, min((band['height'] + height) * 0.55, 18.0))
            if band['page'] == page and abs(y - band['y']) <= threshold:
                assigned_band = band
                break
        if assigned_band is None:
            assigned_band = {'page': page, 'y': y, 'height': height, 'blocks': []}
            bands.append(assigned_band)
        assigned_band['blocks'].append(dict(block))
        assigned_band['y'] = sum(float(_safe_float(item.get('y')) or assigned_band['y']) for item in assigned_band['blocks']) / len(assigned_band['blocks'])
        assigned_band['height'] = max(float(_safe_float(item.get('height')) or height) for item in assigned_band['blocks'])

    normalized_bands: list[dict[str, Any]] = []
    for band in bands:
        ordered_blocks = sorted(band['blocks'], key=lambda item: _safe_float(item.get('x')) or 0.0)
        cells: list[dict[str, Any]] = []
        for block in ordered_blocks:
            text = ' '.join(str(block.get('text') or '').split())
            if not text:
                continue
            if '|' in text:
                fallback_cells = _build_fallback_cell_positions(
                    [cell for cell in (_normalize_ocr_table_cell(part) for part in text.split('|')) if cell],
                    block,
                )
                cells.extend(fallback_cells)
                continue
            x = float(_safe_float(block.get('x')) or 0.0)
            width = float(_safe_float(block.get('width')) or max(len(text) * 7.0, 12.0))
            cell = {'text': text, 'x': x, 'width': width}
            if cells:
                previous = cells[-1]
                previous_right = float(previous['x']) + float(previous['width'])
                gap = x - previous_right
                if gap <= max(18.0, float(previous['width']) * 0.45):
                    previous['text'] = f"{previous['text']} {text}".strip()
                    previous['width'] = max(previous_right, x + width) - float(previous['x'])
                    continue
            cells.append(cell)
        if len(cells) >= 2:
            normalized_bands.append(
                {
                    'page': band['page'],
                    'y': band['y'],
                    'cells': cells,
                }
            )
    return normalized_bands


def _build_fallback_cell_positions(cells: list[str], block: dict[str, Any]) -> list[dict[str, Any]]:
    x = float(_safe_float(block.get('x')) or 0.0)
    width = float(_safe_float(block.get('width')) or max(len(' '.join(cells)) * 7.0, float(len(cells) * 20)))
    if not cells:
        return []
    cell_width = width / max(len(cells), 1)
    return [
        {
            'text': cell,
            'x': x + (index * cell_width),
            'width': cell_width,
        }
        for index, cell in enumerate(cells)
    ]


def _extract_header_anchor_positions(row: dict[str, Any]) -> list[float]:
    anchors: list[float] = []
    for position in list(row.get('cell_positions') or []):
        x = _safe_float(position.get('x'))
        width = _safe_float(position.get('width')) or 0.0
        if x is None:
            continue
        anchors.append(float(x) + (float(width) / 2.0))
    return anchors


def _apply_header_anchors_to_row(row: dict[str, Any], anchor_positions: list[float]) -> dict[str, Any]:
    cell_positions = [dict(item) for item in list(row.get('cell_positions') or []) if isinstance(item, dict)]
    if not cell_positions or not anchor_positions:
        return row
    assigned_cells = ['' for _ in anchor_positions]
    for cell in cell_positions:
        x = _safe_float(cell.get('x'))
        width = _safe_float(cell.get('width')) or 0.0
        text = str(cell.get('text') or '').strip()
        if x is None or not text:
            continue
        center = float(x) + (float(width) / 2.0)
        target_index = min(range(len(anchor_positions)), key=lambda index: abs(anchor_positions[index] - center))
        if assigned_cells[target_index]:
            assigned_cells[target_index] = f"{assigned_cells[target_index]} {text}".strip()
        else:
            assigned_cells[target_index] = text
    return {
        **row,
        'cells': assigned_cells,
    }


def _build_ocr_table_columns(header_cells: list[str]) -> list[str]:
    columns: list[str] = []
    seen: dict[str, int] = {}
    for index, raw_cell in enumerate(header_cells, start=1):
        normalized = ' '.join(str(raw_cell or '').split()).strip()
        if not normalized:
            normalized = f'column_{index}'
        count = seen.get(normalized.casefold(), 0) + 1
        seen[normalized.casefold()] = count
        columns.append(normalized if count == 1 else f'{normalized}_{count}')
    return columns


def _infer_ocr_column_types(rows: list[dict[str, Any]], columns: list[str]) -> dict[str, str]:
    if not rows:
        return {}
    column_count = min(len(rows[0]['cells']), len(columns))
    inferred: dict[str, str] = {}
    for index, column in enumerate(columns[:column_count]):
        numeric_score = 0
        date_score = 0
        text_score = 0
        for row in rows:
            cell = row['cells'][index] if index < len(row['cells']) else ''
            if not cell:
                continue
            if _repair_date_value(cell):
                date_score += 1
            elif _repair_numeric_value(cell):
                numeric_score += 1
            else:
                text_score += 1
        if date_score >= max(numeric_score, text_score) and date_score >= 1:
            inferred[column] = 'date'
        elif numeric_score >= max(date_score, text_score) and numeric_score >= 1:
            inferred[column] = 'number'
        else:
            inferred[column] = 'text'
    return inferred


def _repair_ocr_cell_by_type(value: str, column_type: str) -> str:
    normalized = ' '.join(str(value or '').split()).strip()
    if not normalized:
        return ''
    if column_type == 'number':
        return _repair_numeric_value(normalized) or ''
    if column_type == 'date':
        return _repair_date_value(normalized) or ''
    repaired = _normalize_mixed_script_value(normalized)
    if _is_noise_like_ocr_table_cell(repaired):
        return ''
    return repaired


def _repair_numeric_value(value: str) -> str | None:
    normalized = str(value or '').strip()
    if not normalized:
        return None
    translation = str.maketrans({
        'о': '0',
        'О': '0',
        'o': '0',
        'O': '0',
        'I': '1',
        'l': '1',
        '|': '1',
        'S': '5',
        's': '5',
        'B': '8',
    })
    normalized = normalized.translate(translation).replace(' ', '')
    normalized = normalized.replace('−', '-').replace('—', '-')
    if not any(char.isdigit() for char in normalized):
        return None
    if re.fullmatch(r'[+-]?\d+(?:[.,]\d+)?', normalized):
        return normalized.replace(',', '.')
    return None


def _looks_like_numeric_value(value: str) -> bool:
    return _repair_numeric_value(value) is not None


def _repair_date_value(value: str) -> str | None:
    normalized = str(value or '').strip()
    if not normalized:
        return None
    translation = str.maketrans({
        'о': '0',
        'О': '0',
        'o': '0',
        'O': '0',
        'I': '1',
        'l': '1',
        '|': '1',
    })
    normalized = normalized.translate(translation)
    normalized = re.sub(r'[./\\]', '-', normalized)
    normalized = re.sub(r'\s+', '', normalized)
    match = re.fullmatch(r'(\d{1,4})-(\d{1,2})-(\d{1,4})', normalized)
    if not match:
        return None
    part1, part2, part3 = match.groups()
    try:
        if len(part1) == 4:
            year, month, day = int(part1), int(part2), int(part3)
        elif len(part3) == 4:
            day, month, year = int(part1), int(part2), int(part3)
        else:
            return None
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def _looks_like_date_value(value: str) -> bool:
    return _repair_date_value(value) is not None


def _normalize_mixed_script_value(value: str) -> str:
    normalized = str(value or '').strip()
    if not normalized:
        return ''
    cyrillic_count = sum(1 for char in normalized if 'а' <= char.casefold() <= 'я' or char.casefold() == 'ё')
    latin_count = sum(1 for char in normalized if 'a' <= char.casefold() <= 'z')
    if cyrillic_count == 0 or latin_count == 0:
        return normalized
    if cyrillic_count >= latin_count:
        translation = str.maketrans({
            'A': 'А', 'a': 'а',
            'B': 'В',
            'C': 'С', 'c': 'с',
            'E': 'Е', 'e': 'е',
            'H': 'Н',
            'K': 'К',
            'M': 'М',
            'O': 'О', 'o': 'о',
            'P': 'Р', 'p': 'р',
            'T': 'Т',
            'X': 'Х', 'x': 'х',
            'Y': 'У', 'y': 'у',
        })
    else:
        translation = str.maketrans({
            'А': 'A', 'а': 'a',
            'В': 'B',
            'С': 'C', 'с': 'c',
            'Е': 'E', 'е': 'e',
            'Н': 'H',
            'К': 'K',
            'М': 'M',
            'О': 'O', 'о': 'o',
            'Р': 'P', 'р': 'p',
            'Т': 'T',
            'Х': 'X', 'х': 'x',
            'У': 'Y', 'у': 'y',
        })
    return normalized.translate(translation)


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python document_parser.py /path/to/file.pdf")
        raise SystemExit(1)

    parsed = parse_document(sys.argv[1])
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
