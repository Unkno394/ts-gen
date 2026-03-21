from __future__ import annotations

import csv
import json
import logging
from copy import deepcopy
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from models import ParsedFile, ParsedKvPair, ParsedSection, ParsedSheet, ParsedTextBlock, SourceCandidate, TargetField

try:
    from openpyxl import load_workbook
except ImportError:  # pragma: no cover - optional dependency for Excel support
    load_workbook = None

try:
    import xlrd
except ImportError:  # pragma: no cover - optional dependency for legacy XLS support
    xlrd = None

PARSER_DIR = Path(__file__).resolve().parent / 'parser'
if str(PARSER_DIR) not in sys.path:
    sys.path.insert(0, str(PARSER_DIR))

from candidate_normalizer import build_candidate_source, build_source_candidates  # noqa: E402
from document_parser import parse_document  # noqa: E402

PREVIEW_ROW_LIMIT = 5
SPARSE_ROW_RATIO_THRESHOLD = 0.35
IMAGE_FILE_TYPES = {'png', 'jpg', 'jpeg', 'bmp', 'gif', 'tif', 'tiff', 'webp'}
logger = logging.getLogger(__name__)


class ParseError(ValueError):
    pass


def parse_file(file_path: Path, original_name: str | None = None) -> ParsedFile:
    ext = file_path.suffix.lower().lstrip('.')
    original_name = original_name or file_path.name
    parsed_kwargs: dict[str, Any] = {
        'columns': [],
        'rows': [],
        'content_type': 'unknown',
        'extraction_status': 'unknown',
        'raw_text': '',
        'text_blocks': [],
        'sections': [],
        'kv_pairs': [],
        'source_candidates': [],
        'sheets': [],
        'warnings': [],
    }

    logger.info('parse_file started: name=%s ext=%s path=%s', original_name, ext, file_path)

    if ext == 'csv':
        columns, rows = _parse_csv(file_path)
        parsed_kwargs.update(
            {
                'columns': columns,
                'rows': rows[:PREVIEW_ROW_LIMIT],
                'content_type': 'table',
                'extraction_status': 'structured_extracted',
                'source_candidates': [
                    SourceCandidate(**candidate)
                    for candidate in build_source_candidates(
                        tables=[{'name': original_name, 'columns': columns, 'rows': rows[:PREVIEW_ROW_LIMIT]}]
                    )
                ],
            }
        )
    elif ext in {'xlsx', 'xls'}:
        columns, rows, extra_warnings, sheets = _parse_excel(file_path)
        parsed_kwargs.update(
            {
                'columns': columns,
                'rows': rows[:PREVIEW_ROW_LIMIT],
                'sheets': sheets,
                'warnings': extra_warnings,
                'content_type': 'table',
                'extraction_status': 'structured_extracted',
                'source_candidates': [
                    SourceCandidate(**candidate)
                    for candidate in build_source_candidates(
                        tables=[
                            {
                                'name': sheet.name,
                                'columns': sheet.columns,
                                'rows': sheet.rows,
                            }
                            for sheet in sheets
                        ]
                        or [{'name': original_name, 'columns': columns, 'rows': rows[:PREVIEW_ROW_LIMIT]}]
                    )
                ],
            }
        )
    elif ext in {'pdf', 'docx', 'txt'} | IMAGE_FILE_TYPES:
        parsed_kwargs.update(_parse_document(file_path))
    else:
        raise ParseError(f'Unsupported file type: {ext}')

    columns = list(parsed_kwargs['columns'])
    rows = list(parsed_kwargs['rows'])
    warnings = list(parsed_kwargs['warnings'])
    if not columns and not rows and not warnings:
        warnings.append('No columns detected in the file.')
    parsed_kwargs['warnings'] = warnings

    logger.info(
        'parse_file finished: name=%s ext=%s columns=%d preview_rows=%d warnings=%d content_type=%s extraction_status=%s',
        original_name,
        ext,
        len(columns),
        min(len(rows), PREVIEW_ROW_LIMIT),
        len(warnings),
        parsed_kwargs['content_type'],
        parsed_kwargs['extraction_status'],
    )

    return ParsedFile(
        file_name=original_name,
        file_type=ext,
        columns=columns,
        rows=rows[:PREVIEW_ROW_LIMIT],
        content_type=str(parsed_kwargs['content_type']),
        extraction_status=str(parsed_kwargs['extraction_status']),
        raw_text=str(parsed_kwargs['raw_text']),
        text_blocks=list(parsed_kwargs['text_blocks']),
        sections=list(parsed_kwargs['sections']),
        kv_pairs=list(parsed_kwargs['kv_pairs']),
        source_candidates=list(parsed_kwargs['source_candidates']),
        sheets=list(parsed_kwargs['sheets']),
        warnings=warnings,
    )


def resolve_generation_source(
    parsed_file: ParsedFile,
    selected_sheet: str | None = None,
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    if not parsed_file.sheets:
        if parsed_file.columns or parsed_file.rows:
            return parsed_file.columns, parsed_file.rows, []

        candidate_columns, candidate_rows = build_candidate_source(
            [_model_to_plain_dict(candidate) for candidate in parsed_file.source_candidates]
        )
        if candidate_columns and candidate_rows:
            return candidate_columns, candidate_rows, ['Generated mapping from extracted fields/text candidates.']

        if parsed_file.extraction_status in {'requires_ocr_or_manual_input', 'text_not_extracted', 'image_parse_not_supported_yet'}:
            raise ParseError(_extraction_status_message(parsed_file))

        return parsed_file.columns, parsed_file.rows, []

    if selected_sheet is None or selected_sheet.strip() == '':
        return parsed_file.columns, parsed_file.rows, []

    normalized_name = selected_sheet.strip()
    for sheet in parsed_file.sheets:
        if sheet.name == normalized_name:
            return (
                sheet.columns,
                sheet.rows,
                [f'Generated mapping from selected sheet: {sheet.name}'],
            )

    available_sheets = ', '.join(sheet.name for sheet in parsed_file.sheets)
    label = 'Worksheet' if parsed_file.file_type in {'xlsx', 'xls'} else 'Table'
    raise ParseError(f'{label} "{selected_sheet}" not found. Available: {available_sheets}')



def _parse_csv(file_path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    encodings_to_try = ['utf-8-sig', 'utf-8', 'cp1251', 'latin-1']
    last_error: Exception | None = None

    for encoding in encodings_to_try:
        try:
            with file_path.open('r', encoding=encoding, newline='') as f:
                sample = f.read(4096)
                f.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                except csv.Error:
                    dialect = csv.excel
                reader = csv.DictReader(f, dialect=dialect)
                rows = [dict(row) for row in reader]
                columns = reader.fieldnames or []
                return [str(c) for c in columns], rows
        except Exception as exc:  # noqa: BLE001
            last_error = exc

    raise ParseError(f'Failed to parse CSV: {last_error}')



def _parse_excel(file_path: Path) -> tuple[list[str], list[dict[str, Any]], list[str], list[ParsedSheet]]:
    warnings: list[str] = []
    suffix = file_path.suffix.lower()

    try:
        if suffix == '.xlsx':
            return _parse_xlsx(file_path, warnings)
        if suffix == '.xls':
            return _parse_xls(file_path, warnings)
    except Exception as exc:  # noqa: BLE001
        raise ParseError(f'Failed to parse Excel: {exc}') from exc

    raise ParseError(f'Unsupported Excel file type: {suffix}')


def _parse_xlsx(file_path: Path, warnings: list[str]) -> tuple[list[str], list[dict[str, Any]], list[str], list[ParsedSheet]]:
    if load_workbook is None:
        raise ParseError('XLSX parsing is disabled in this build. Rebuild backend with openpyxl support.')

    workbook = load_workbook(filename=file_path, read_only=True, data_only=True)
    try:
        return _collect_excel_sheets(
            sheet_names=list(workbook.sheetnames),
            iter_sheet_rows=lambda sheet_name: workbook[sheet_name].iter_rows(values_only=True),
            warnings=warnings,
        )
    finally:
        workbook.close()


def _parse_xls(file_path: Path, warnings: list[str]) -> tuple[list[str], list[dict[str, Any]], list[str], list[ParsedSheet]]:
    if xlrd is None:
        raise ParseError('XLS parsing is disabled in this build. Rebuild backend with xlrd support or convert the file to XLSX.')

    workbook = xlrd.open_workbook(file_path)
    return _collect_excel_sheets(
        sheet_names=list(workbook.sheet_names()),
        iter_sheet_rows=lambda sheet_name: _iter_xls_sheet_rows(workbook, sheet_name),
        warnings=warnings,
    )


def _collect_excel_sheets(
    *,
    sheet_names: list[str],
    iter_sheet_rows,
    warnings: list[str],
) -> tuple[list[str], list[dict[str, Any]], list[str], list[ParsedSheet]]:
    combined_columns: list[str] = []
    combined_rows: list[dict[str, Any]] = []
    non_empty_sheets: list[str] = []
    sheets: list[ParsedSheet] = []

    for sheet_name in sheet_names:
        rows_iter = list(iter_sheet_rows(sheet_name))
        columns, rows, sheet_warnings = _sheet_rows_to_records(sheet_name, rows_iter)
        warnings.extend(sheet_warnings)

        if not columns and not rows:
            continue

        non_empty_sheets.append(sheet_name)
        sheets.append(ParsedSheet(name=sheet_name, columns=columns, rows=rows[:PREVIEW_ROW_LIMIT]))
        for column in columns:
            if column not in combined_columns:
                combined_columns.append(column)
        combined_rows.extend(rows)

    if len(non_empty_sheets) > 1:
        warnings.append(f'Merged {len(non_empty_sheets)} sheets: {", ".join(non_empty_sheets)}')
    elif len(sheet_names) > 1 and len(non_empty_sheets) == 1:
        warnings.append(f'Workbook has multiple sheets. Used the only non-empty sheet: {non_empty_sheets[0]}')

    return combined_columns, combined_rows[:PREVIEW_ROW_LIMIT], warnings, sheets


def _sheet_rows_to_records(sheet_name: str, rows_iter: list[tuple[Any, ...] | list[Any]]) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    if not rows_iter:
        return [], [], warnings

    raw_headers = list(rows_iter[0])
    if not raw_headers:
        return [], [], warnings

    if any(
        column is None
        or not isinstance(column, str)
        or (isinstance(column, str) and (column.strip() == '' or column.startswith('Unnamed:')))
        for column in raw_headers
    ):
        warnings.append(
            f'Sheet "{sheet_name}": Excel first row is treated as column headers. Some headers are empty or non-text, so the first row may contain data instead of column names.'
        )

    columns = [_stringify_excel_header(column, index) for index, column in enumerate(raw_headers, start=1)]
    rows: list[dict[str, Any]] = []

    for raw_row in rows_iter[1:]:
        values = list(raw_row)
        if not any(_has_visible_value(value) for value in values):
            continue
        record: dict[str, Any] = {}
        for index, column in enumerate(columns):
            value = values[index] if index < len(values) else None
            record[column] = _coerce_excel_value(value)
        rows.append(record)

    return columns, rows, warnings


def _iter_xls_sheet_rows(workbook: Any, sheet_name: str):
    sheet = workbook.sheet_by_name(sheet_name)
    for row_index in range(sheet.nrows):
        yield tuple(_coerce_xls_cell(workbook, sheet.cell(row_index, col_index)) for col_index in range(sheet.ncols))


def _coerce_xls_cell(workbook: Any, cell: Any) -> Any:
    if xlrd is None:
        return cell.value
    if cell.ctype == xlrd.XL_CELL_DATE:
        return xlrd.xldate.xldate_as_datetime(cell.value, workbook.datemode).isoformat()
    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        return bool(cell.value)
    if cell.ctype == xlrd.XL_CELL_NUMBER:
        if float(cell.value).is_integer():
            return int(cell.value)
        return float(cell.value)
    if cell.ctype == xlrd.XL_CELL_EMPTY:
        return None
    return cell.value


def _stringify_excel_header(value: Any, index: int) -> str:
    if value is None:
        return f'Column {index}'
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or f'Column {index}'
    return str(value)


def _coerce_excel_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _has_visible_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and value.strip() == '':
        return False
    return True


def _parse_document(file_path: Path) -> dict[str, Any]:
    try:
        parsed = parse_document(file_path)
        content_type = str(parsed.get('content_type', 'unknown'))
        extraction_status = str(parsed.get('extraction_status', 'unknown'))
        columns = [str(column) for column in parsed.get('columns', [])]
        raw_rows = parsed.get('rows', [])
        rows = [dict(row) for row in raw_rows if isinstance(row, dict)]
        warnings = [str(warning) for warning in parsed.get('warnings', [])]
        sheets: list[ParsedSheet] = []
        raw_tables = parsed.get('tables', [])
        if isinstance(raw_tables, list):
            for index, table in enumerate(raw_tables, start=1):
                if not isinstance(table, dict):
                    continue
                table_columns = [str(column) for column in table.get('columns', [])]
                table_rows = [dict(row) for row in table.get('rows', []) if isinstance(row, dict)]
                if not table_columns and not table_rows:
                    continue
                sheets.append(
                    ParsedSheet(
                        name=str(table.get('name') or f'Table {index}'),
                        columns=table_columns,
                        rows=table_rows[:PREVIEW_ROW_LIMIT],
                    )
                )
        original_row_count = len(rows)

        rows = _filter_sparse_rows(columns, rows)
        filtered_out = original_row_count - len(rows)
        if columns and not rows:
            warnings.append('Extracted table is too sparse to use as a structured source.')
            columns = []

        text_blocks = [
            ParsedTextBlock(
                id=str(block.get('id') or f'block-{index}'),
                kind='line' if str(block.get('kind') or 'paragraph') == 'line' else 'paragraph',
                text=str(block.get('text') or ''),
                label=str(block.get('label')) if block.get('label') is not None else None,
            )
            for index, block in enumerate(parsed.get('text_blocks', []), start=1)
            if isinstance(block, dict) and str(block.get('text') or '').strip()
        ]
        sections = [
            ParsedSection(title=str(section.get('title') or 'Section'), text=str(section.get('text') or ''))
            for section in parsed.get('sections', [])
            if isinstance(section, dict) and str(section.get('text') or '').strip()
        ]
        kv_pairs = [
            ParsedKvPair(
                label=str(pair.get('label') or ''),
                value=str(pair.get('value') or ''),
                confidence=_safe_kv_confidence(pair.get('confidence')),
                source_text=str(pair.get('source_text')) if pair.get('source_text') is not None else None,
            )
            for pair in parsed.get('kv_pairs', [])
            if isinstance(pair, dict) and str(pair.get('label') or '').strip() and str(pair.get('value') or '').strip()
        ]
        source_candidates = [
            SourceCandidate(
                candidate_type=_safe_candidate_type(candidate.get('candidate_type')),
                label=str(candidate.get('label') or ''),
                value=candidate.get('value'),
                sample_values=list(candidate.get('sample_values') or []),
                source_text=str(candidate.get('source_text')) if candidate.get('source_text') is not None else None,
                section_title=str(candidate.get('section_title')) if candidate.get('section_title') is not None else None,
            )
            for candidate in parsed.get('source_candidates', [])
            if isinstance(candidate, dict) and str(candidate.get('label') or '').strip()
        ]

        if content_type not in {'table', 'mixed'} and not rows and not kv_pairs:
            warnings.append('Document uploaded without a tabular preview. Generation will use extracted fields when available.')

        logger.info(
            'document parser result: file=%s content_type=%s extraction_status=%s columns=%d rows=%d filtered_sparse_rows=%d kv_pairs=%d candidates=%d',
            file_path.name,
            content_type,
            extraction_status,
            len(columns),
            len(rows),
            max(filtered_out, 0),
            len(kv_pairs),
            len(source_candidates),
        )

        if content_type != 'table':
            logger.info(
                'document parser fallback to non-tabular mode: file=%s content_type=%s warnings=%s',
                file_path.name,
                content_type,
                warnings,
            )
        elif filtered_out > 0:
            logger.info(
                'document parser dropped sparse rows: file=%s dropped=%d remaining=%d',
                file_path.name,
                filtered_out,
                len(rows),
            )

        if sheets:
            filtered_sheets: list[ParsedSheet] = []
            for sheet in sheets:
                filtered_rows = _filter_sparse_rows(sheet.columns, [dict(row) for row in sheet.rows])
                if not sheet.columns and not filtered_rows:
                    continue
                filtered_sheets.append(
                    ParsedSheet(
                        name=sheet.name,
                        columns=sheet.columns,
                        rows=filtered_rows[:PREVIEW_ROW_LIMIT],
                    )
                )
            sheets = filtered_sheets

        return {
            'columns': columns,
            'rows': rows[:PREVIEW_ROW_LIMIT],
            'warnings': warnings,
            'sheets': sheets,
            'content_type': content_type,
            'extraction_status': extraction_status,
            'raw_text': str(parsed.get('text') or ''),
            'text_blocks': text_blocks,
            'sections': sections,
            'kv_pairs': kv_pairs,
            'source_candidates': source_candidates,
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception('document parser failed: file=%s error=%s', file_path.name, exc)
        raise ParseError(f'Failed to parse document: {exc}') from exc


def _filter_sparse_rows(columns: list[str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not columns or not rows:
        return rows

    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        filled_cells = 0
        for column in columns:
            value = row.get(column)
            if value is None:
                continue
            if isinstance(value, str) and value.strip() == '':
                continue
            filled_cells += 1

        fill_ratio = filled_cells / max(len(columns), 1)
        if fill_ratio >= SPARSE_ROW_RATIO_THRESHOLD:
            filtered_rows.append(row)

    return filtered_rows


def _model_to_plain_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, 'model_dump'):
        return value.model_dump()
    if hasattr(value, 'dict'):
        return value.dict()
    if isinstance(value, dict):
        return value
    raise TypeError(f'Unsupported parsed model type: {type(value)!r}')


def _safe_candidate_type(value: Any) -> str:
    candidate_type = str(value or 'text_section')
    if candidate_type in {'table_column', 'kv_pair', 'text_fact', 'text_section'}:
        return candidate_type
    return 'text_section'


def _safe_kv_confidence(value: Any) -> str:
    confidence = str(value or 'medium')
    if confidence in {'high', 'medium', 'low'}:
        return confidence
    return 'medium'


def _extraction_status_message(parsed_file: ParsedFile) -> str:
    if parsed_file.extraction_status == 'requires_ocr_or_manual_input':
        return 'Document looks like a scan or image-based PDF. OCR or manual input is required.'
    if parsed_file.extraction_status == 'image_parse_not_supported_yet':
        return 'Image-like files are detected, but OCR-free extraction is not supported yet.'
    if parsed_file.extraction_status == 'text_not_extracted':
        return 'Text could not be extracted from the document.'
    return 'No usable source fields were extracted from the document.'



def infer_target_fields(target_json_raw: str) -> tuple[list[TargetField], dict[str, Any] | list[Any]]:
    target_fields, payload, _, _ = parse_target_schema(target_json_raw)
    return target_fields, payload


def parse_target_schema(
    target_json_raw: str,
) -> tuple[list[TargetField], dict[str, Any] | list[Any], dict[str, Any], dict[str, Any]]:
    payload, duplicate_keys = _load_target_json_with_duplicate_tracking(target_json_raw)

    if duplicate_keys:
        duplicates = ', '.join(sorted(set(duplicate_keys)))
        raise ParseError(f'Target JSON contains duplicate keys: {duplicates}')

    if not isinstance(payload, (dict, list)):
        raise ParseError('Target JSON must be an object or an array of objects.')

    target_schema = _build_target_schema(payload, path='$')
    root_object_schema = _resolve_root_object_schema(target_schema)
    target_fields = _extract_target_fields_from_schema(root_object_schema)
    schema_summary = {
        'root_type': target_schema['type'],
        'required_fields': list(root_object_schema.get('required_fields', [])),
        'field_count': len(target_fields),
        'root_is_array': target_schema['type'] == 'array',
    }
    return target_fields, payload, target_schema, schema_summary


def _load_target_json_with_duplicate_tracking(target_json_raw: str) -> tuple[Any, list[str]]:
    duplicate_keys: list[str] = []

    def _object_pairs_hook(pairs: list[tuple[Any, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            key_text = key if isinstance(key, str) else str(key)
            if key_text in result:
                duplicate_keys.append(key_text)
            result[key_text] = value
        return result

    try:
        payload = json.loads(target_json_raw, object_pairs_hook=_object_pairs_hook)
    except json.JSONDecodeError as exc:
        raise ParseError(f'Invalid target JSON: {exc}') from exc

    return payload, duplicate_keys


def _build_target_schema(value: Any, *, path: str) -> dict[str, Any]:
    if isinstance(value, dict):
        properties: dict[str, Any] = {}
        required_fields: list[str] = []
        for key, nested_value in value.items():
            if not isinstance(key, str):
                raise ParseError(f'Target JSON key at {path} must be a string.')
            normalized_key = key.strip()
            if not normalized_key:
                raise ParseError(f'Target JSON contains an empty key at {path}.')
            if normalized_key in properties:
                raise ParseError(f'Target JSON contains duplicate key "{normalized_key}" at {path}.')
            child_path = f'{path}.{normalized_key}'
            properties[normalized_key] = _build_target_schema(nested_value, path=child_path)
            required_fields.append(normalized_key)
        return {
            'type': 'object',
            'nullable': False,
            'properties': properties,
            'required_fields': required_fields,
        }

    if isinstance(value, list):
        if not value:
            return {
                'type': 'array',
                'nullable': False,
                'items': {'type': 'any', 'nullable': True},
            }

        item_schemas = [_build_target_schema(item, path=f'{path}[]') for item in value]
        merged_item_schema = _merge_array_item_schemas(item_schemas, path=f'{path}[]')
        return {
            'type': 'array',
            'nullable': False,
            'items': merged_item_schema,
        }

    inferred_type = _infer_type(value)
    return {
        'type': inferred_type,
        'nullable': value is None,
    }


def _merge_array_item_schemas(item_schemas: list[dict[str, Any]], *, path: str) -> dict[str, Any]:
    non_null_schemas = [schema for schema in item_schemas if schema.get('type') != 'null']
    nullable = len(non_null_schemas) != len(item_schemas)

    if not non_null_schemas:
        return {'type': 'null', 'nullable': True}

    merged = deepcopy(non_null_schemas[0])
    merged['nullable'] = bool(merged.get('nullable')) or nullable

    for schema in non_null_schemas[1:]:
        if merged.get('type') != schema.get('type'):
            raise ParseError(f'Target JSON contains conflicting array item types at {path}.')
        if merged.get('type') == 'object':
            merged = _merge_object_schemas(merged, schema, path=path)
            merged['nullable'] = bool(merged.get('nullable')) or nullable
            continue
        if merged.get('type') == 'array':
            merged['items'] = _merge_array_item_schemas(
                [merged.get('items', {'type': 'any'}), schema.get('items', {'type': 'any'})],
                path=f'{path}[]',
            )
            merged['nullable'] = bool(merged.get('nullable')) or nullable

    return merged


def _merge_object_schemas(left: dict[str, Any], right: dict[str, Any], *, path: str) -> dict[str, Any]:
    merged = deepcopy(left)
    left_properties = dict(merged.get('properties', {}))
    right_properties = dict(right.get('properties', {}))
    all_keys = sorted(set(left_properties) | set(right_properties))
    required_fields = set(merged.get('required_fields', [])) & set(right.get('required_fields', []))
    merged_properties: dict[str, Any] = {}

    for key in all_keys:
        child_path = f'{path}.{key}'
        left_schema = left_properties.get(key)
        right_schema = right_properties.get(key)
        if left_schema is None:
            merged_properties[key] = deepcopy(right_schema)
            continue
        if right_schema is None:
            merged_properties[key] = deepcopy(left_schema)
            continue
        if left_schema.get('type') != right_schema.get('type'):
            raise ParseError(f'Target JSON contains conflicting types for "{key}" at {child_path}.')
        if left_schema.get('type') == 'object':
            merged_properties[key] = _merge_object_schemas(left_schema, right_schema, path=child_path)
        elif left_schema.get('type') == 'array':
            merged_properties[key] = deepcopy(left_schema)
            merged_properties[key]['items'] = _merge_array_item_schemas(
                [left_schema.get('items', {'type': 'any'}), right_schema.get('items', {'type': 'any'})],
                path=f'{child_path}[]',
            )
        else:
            merged_properties[key] = deepcopy(left_schema)
            merged_properties[key]['nullable'] = bool(left_schema.get('nullable')) or bool(right_schema.get('nullable'))

    merged['properties'] = merged_properties
    merged['required_fields'] = [key for key in all_keys if key in required_fields]
    return merged


def _resolve_root_object_schema(target_schema: dict[str, Any]) -> dict[str, Any]:
    if target_schema.get('type') == 'object':
        return target_schema
    if target_schema.get('type') == 'array':
        item_schema = target_schema.get('items') or {}
        if item_schema.get('type') != 'object':
            raise ParseError('Target JSON array root must contain objects.')
        return item_schema
    raise ParseError('Target JSON must describe an object structure.')


def _extract_target_fields_from_schema(object_schema: dict[str, Any]) -> list[TargetField]:
    fields: list[TargetField] = []
    for key, field_schema in dict(object_schema.get('properties', {})).items():
        field_type = field_schema.get('type', 'any')
        if field_type not in {'string', 'number', 'boolean', 'object', 'array', 'null', 'any'}:
            field_type = 'any'
        fields.append(TargetField(name=key, type=field_type))
    return fields



def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return 'number'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, list):
        return 'array'
    if isinstance(value, dict):
        return 'object'
    if value is None:
        return 'null'
    return 'any'
