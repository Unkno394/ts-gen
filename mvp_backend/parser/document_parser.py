from __future__ import annotations

from pathlib import Path
from typing import Any

from candidate_normalizer import build_source_candidates
from document_classifier import classify_document
from docx_parser import parse_docx
from form_layout import extract_layout_layer, understand_generic_form
from form_parser import extract_kv_pairs
from pdf_parser import parse_pdf
from text_parser import extract_sections, extract_tables, extract_text_facts, normalize_text, split_text_blocks


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
    zone_summary = dict(base_result.get('zone_summary') or {})
    raw_text = normalize_text(base_result.get('text', ''))
    layout_blocks = [dict(block) for block in base_result.get('blocks', []) if isinstance(block, dict)]
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
    document_mode = 'data_table_mode'
    downgrade_form_mode_for_multitable_docx = False
    if isinstance(form_model, dict):
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
            warnings.append('Extracted table looked form-like, so preview was switched to form-aware extraction.')
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
        'pdf_zone_summary': zone_summary if ext == 'pdf' else {},
        'warnings': deduped_warnings,
    }


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


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python document_parser.py /path/to/file.pdf")
        raise SystemExit(1)

    parsed = parse_document(sys.argv[1])
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
