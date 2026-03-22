from __future__ import annotations

from pathlib import Path
from typing import Any

import pdfplumber
from ocr_client import extract_text_from_ocr_service
from pdf_zoning import classify_pdf_document_zones


TEXT_MIN_LENGTH = 40
LINE_Y_TOLERANCE = 4.0
LINE_SEGMENT_GAP_THRESHOLD = 60.0
LINE_COLUMN_GAP_THRESHOLD = 120.0



def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()



def extract_tables_from_pdf(path: str | Path) -> list[dict[str, Any]]:
    found_tables: list[dict[str, Any]] = []
    with pdfplumber.open(str(path)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            for table_index, table in enumerate(tables, start=1):
                if not table or len(table) < 2:
                    continue

                normalized_rows = []
                for row in table:
                    normalized_row = [clean_text(cell) for cell in row]
                    if any(normalized_row):
                        normalized_rows.append(normalized_row)

                if len(normalized_rows) < 2:
                    continue

                header = normalized_rows[0]
                if not any(header):
                    continue

                columns = [col if col else f"column{idx + 1}" for idx, col in enumerate(header)]
                rows: list[dict[str, str]] = []
                for row in normalized_rows[1:]:
                    padded = row + [""] * max(0, len(columns) - len(row))
                    row_obj = {columns[idx]: padded[idx] for idx in range(len(columns))}
                    if any(v != "" for v in row_obj.values()):
                        rows.append(row_obj)

                if rows:
                    found_tables.append(
                        {
                            "name": f"Page {page_index} · Table {table_index}",
                            "columns": columns,
                            "rows": rows,
                            "raw_rows": normalized_rows,
                        }
                    )

    return found_tables



def extract_text_from_pdf(path: str | Path) -> str:
    parts: list[str] = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            text = text.strip()
            if text:
                parts.append(text)
    return "\n\n".join(parts).strip()


def extract_layout_lines_from_pdf(path: str | Path) -> list[dict[str, Any]]:
    layout_lines: list[dict[str, Any]] = []
    with pdfplumber.open(str(path)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            try:
                words = page.extract_words() or []
            except Exception:
                words = []
            grouped = _group_words_into_lines(words)
            for line_index, line in enumerate(grouped, start=1):
                if not line['text']:
                    continue
                layout_lines.append(
                    {
                        'id': f'page-{page_index}-line-{line_index}',
                        'type': 'line',
                        'text': line['text'],
                        'page': page_index,
                        'column_id': line.get('column_id'),
                        'x': line['x0'],
                        'y': line['top'],
                        'width': max(line['x1'] - line['x0'], 0.0),
                        'height': max(line['bottom'] - line['top'], 0.0),
                        'source_type': 'line',
                        'tokens': line['tokens'],
                    }
                )
    return layout_lines



def parse_pdf(path: str | Path) -> dict[str, Any]:
    warnings: list[str] = []
    layout_lines = extract_layout_lines_from_pdf(path)
    tables = extract_tables_from_pdf(path)
    direct_text = extract_text_from_pdf(path)
    zone_summary = classify_pdf_document_zones(
        tables=tables,
        layout_lines=layout_lines,
        raw_text=direct_text,
    )
    data_tables = [dict(table) for table in list(zone_summary.get('data_tables') or [])]
    form_tables = [dict(table) for table in list(zone_summary.get('form_tables') or [])]
    synthetic_form_blocks = _build_form_blocks_from_tables(form_tables) if form_tables and not layout_lines else []
    blocks = layout_lines or synthetic_form_blocks

    if tables:
        warnings.append(f"Found {len(tables)} tables in PDF.")
    if data_tables:
        warnings.append(
            f"PDF zoning classified {len(data_tables)} table zone(s)."
        )
    if form_tables:
        warnings.append(
            f"PDF zoning classified {len(form_tables)} form-like table zone(s)."
        )
    if data_tables:
        columns = data_tables[0]["columns"]
        rows = data_tables[0]["rows"]
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": str(zone_summary.get('content_type') or 'table'),
            "columns": columns,
            "rows": rows,
            "tables": data_tables,
            "text": direct_text,
            "blocks": blocks,
            "zone_summary": zone_summary,
            "warnings": warnings,
        }

    if len(direct_text) >= TEXT_MIN_LENGTH or blocks:
        if not data_tables:
            warnings.append("No data-table zones found in PDF. Returned zoned text/layout extraction.")
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": str(zone_summary.get('content_type') or 'text'),
            "columns": [],
            "rows": [],
            "tables": data_tables,
            "text": direct_text,
            "blocks": blocks or [{"type": "paragraph", "text": direct_text}],
            "zone_summary": zone_summary,
            "warnings": warnings,
        }

    ocr_result = extract_text_from_ocr_service(path)
    if isinstance(ocr_result, dict) and str(ocr_result.get('text') or '').strip():
        ocr_blocks = [dict(block) for block in ocr_result.get('blocks', []) if isinstance(block, dict)]
        warnings.extend([str(warning) for warning in ocr_result.get('warnings', [])])
        warnings.append('PDF text layer was empty, so OCR fallback was used.')
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": "text",
            "columns": [],
            "rows": [],
            "tables": data_tables,
            "text": str(ocr_result.get('text') or ''),
            "blocks": ocr_blocks,
            "ocr_used": True,
            "ocr_metadata": dict(ocr_result.get('ocr_metadata') or {}),
            "zone_summary": {
                **dict(zone_summary or {}),
                'content_type': 'text',
                'extraction_status': 'text_extracted',
                'ocr_fallback_used': True,
            },
            "warnings": warnings,
        }

    warnings.append("PDF text layer is empty or too small. No extractable text found.")
    return {
        "file_name": Path(path).name,
        "file_type": "pdf",
        "content_type": str(zone_summary.get('content_type') or 'text'),
        "columns": [],
        "rows": [],
        "tables": data_tables,
        "text": "",
        "blocks": blocks,
        "zone_summary": zone_summary,
        "warnings": warnings,
    }


def _build_form_blocks_from_tables(form_tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for table_index, table in enumerate(form_tables):
        raw_rows = [row for row in list(table.get('raw_rows') or []) if isinstance(row, list)]
        if not raw_rows:
            columns = [str(column or '').strip() for column in list(table.get('columns') or [])]
            raw_rows = [[column for column in columns]]
            for row in [item for item in list(table.get('rows') or []) if isinstance(item, dict)]:
                raw_rows.append([str(row.get(column) or '').strip() for column in columns])
        for row_index, row_cells in enumerate(raw_rows):
            row_y = float(len(blocks) * 14)
            for cell_index, raw_value in enumerate(row_cells):
                value = str(raw_value or '').strip()
                if not value:
                    continue
                blocks.append(
                    {
                        'id': f'pdf-form-table-{table_index + 1}-row-{row_index}-cell-{cell_index + 1}',
                        'type': 'line',
                        'text': value,
                        'page': 1,
                        'column_id': cell_index + 1,
                        'x': 20.0 + cell_index * 280.0,
                        'y': row_y,
                        'width': None,
                        'height': None,
                        'source_type': 'line',
                        'tokens': [],
                    }
                )
    return blocks


def _group_words_into_lines(words: list[dict[str, Any]]) -> list[dict[str, Any]]:
    line_buckets: list[dict[str, Any]] = []
    for word in words:
        text = clean_text(word.get('text'))
        if not text:
            continue
        top = _safe_float(word.get('top'))
        bottom = _safe_float(word.get('bottom'))
        x0 = _safe_float(word.get('x0'))
        x1 = _safe_float(word.get('x1'))
        bucket = next(
            (
                item
                for item in line_buckets
                if item['top'] is not None
                and top is not None
                and abs(item['top'] - top) <= LINE_Y_TOLERANCE
            ),
            None,
        )
        token = {
            'text': text,
            'page': None,
            'x': x0,
            'y': top,
            'width': max((x1 or 0.0) - (x0 or 0.0), 0.0) if x1 is not None and x0 is not None else None,
            'height': max((bottom or 0.0) - (top or 0.0), 0.0) if bottom is not None and top is not None else None,
            'source_type': 'line',
        }
        if bucket is None:
            line_buckets.append(
                {
                    'top': top,
                    'bottom': bottom,
                    'x0': x0,
                    'x1': x1,
                    'tokens': [token],
                }
            )
            continue

        bucket['tokens'].append(token)
        if top is not None:
            bucket['top'] = min(bucket['top'], top) if bucket['top'] is not None else top
        if bottom is not None:
            bucket['bottom'] = max(bucket['bottom'], bottom) if bucket['bottom'] is not None else bottom
        if x0 is not None:
            bucket['x0'] = min(bucket['x0'], x0) if bucket['x0'] is not None else x0
        if x1 is not None:
            bucket['x1'] = max(bucket['x1'], x1) if bucket['x1'] is not None else x1

    result: list[dict[str, Any]] = []
    for bucket in sorted(line_buckets, key=lambda item: ((item['top'] or 0.0), (item['x0'] or 0.0))):
        tokens = sorted(bucket['tokens'], key=lambda item: (item['x'] or 0.0))
        for segment in _split_line_segments(tokens, top=bucket['top'], bottom=bucket['bottom']):
            text = ' '.join(token['text'] for token in segment['tokens'] if token.get('text')).strip()
            result.append(
                {
                    'text': text,
                    'top': segment['top'],
                    'bottom': segment['bottom'],
                    'x0': segment['x0'],
                    'x1': segment['x1'],
                    'tokens': segment['tokens'],
                }
            )
    return _assign_line_columns(result)


def _split_line_segments(
    tokens: list[dict[str, Any]],
    *,
    top: float | None,
    bottom: float | None,
) -> list[dict[str, Any]]:
    if not tokens:
        return []

    segments: list[list[dict[str, Any]]] = [[]]
    previous_right: float | None = None
    for token in tokens:
        token_left = _safe_float(token.get('x'))
        token_width = _safe_float(token.get('width'))
        token_right = None if token_left is None or token_width is None else token_left + token_width
        if (
            segments[-1]
            and previous_right is not None
            and token_left is not None
            and token_left - previous_right > LINE_SEGMENT_GAP_THRESHOLD
        ):
            segments.append([])
        segments[-1].append(token)
        if token_right is not None:
            previous_right = token_right

    normalized: list[dict[str, Any]] = []
    for segment_tokens in segments:
        if not segment_tokens:
            continue
        x_values = [value for value in (_safe_float(token.get('x')) for token in segment_tokens) if value is not None]
        right_values = [
            value
            for value in (
                None
                if _safe_float(token.get('x')) is None or _safe_float(token.get('width')) is None
                else _safe_float(token.get('x')) + _safe_float(token.get('width'))
                for token in segment_tokens
            )
            if value is not None
        ]
        normalized.append(
            {
                'tokens': segment_tokens,
                'top': top,
                'bottom': bottom,
                'x0': min(x_values) if x_values else None,
                'x1': max(right_values) if right_values else None,
            }
        )
    return normalized


def _assign_line_columns(lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not lines:
        return lines

    sorted_lines = sorted(lines, key=lambda item: (item.get('x0') or 0.0))
    anchors: list[float] = []
    for line in sorted_lines:
        x0 = _safe_float(line.get('x0'))
        if x0 is None:
            continue
        matched_index = None
        for index, anchor in enumerate(anchors):
            if abs(anchor - x0) <= LINE_COLUMN_GAP_THRESHOLD:
                matched_index = index
                anchors[index] = min(anchor, x0)
                break
        if matched_index is None:
            anchors.append(x0)

    normalized_anchors = sorted(anchors)
    if not normalized_anchors:
        return lines

    annotated: list[dict[str, Any]] = []
    for line in lines:
        x0 = _safe_float(line.get('x0'))
        column_id = None
        if x0 is not None:
            column_id = min(
                range(len(normalized_anchors)),
                key=lambda index: abs(normalized_anchors[index] - x0),
            ) + 1
        tokens = []
        for token in line.get('tokens', []) or []:
            if not isinstance(token, dict):
                continue
            tokens.append({**token, 'column_id': column_id})
        annotated.append({**line, 'column_id': column_id, 'tokens': tokens})
    return annotated


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
