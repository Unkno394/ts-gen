from __future__ import annotations

import csv
import json
import logging
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

from models import ParsedFile, ParsedSheet, TargetField

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

from document_parser import parse_document  # noqa: E402

PREVIEW_ROW_LIMIT = 5
SPARSE_ROW_RATIO_THRESHOLD = 0.35
logger = logging.getLogger(__name__)


class ParseError(ValueError):
    pass


def parse_file(file_path: Path, original_name: str | None = None) -> ParsedFile:
    ext = file_path.suffix.lower().lstrip('.')
    original_name = original_name or file_path.name
    warnings: list[str] = []
    sheets: list[ParsedSheet] = []

    logger.info('parse_file started: name=%s ext=%s path=%s', original_name, ext, file_path)

    if ext == 'csv':
        columns, rows = _parse_csv(file_path)
    elif ext in {'xlsx', 'xls'}:
        columns, rows, extra_warnings, sheets = _parse_excel(file_path)
        warnings.extend(extra_warnings)
    elif ext in {'pdf', 'docx'}:
        columns, rows, extra_warnings, sheets = _parse_document(file_path)
        warnings.extend(extra_warnings)
    else:
        raise ParseError(f'Unsupported file type: {ext}')

    if not columns and not (ext in {'pdf', 'docx'} and warnings):
        warnings.append('No columns detected in the file.')

    logger.info(
        'parse_file finished: name=%s ext=%s columns=%d preview_rows=%d warnings=%d',
        original_name,
        ext,
        len(columns),
        min(len(rows), PREVIEW_ROW_LIMIT),
        len(warnings),
    )

    return ParsedFile(
        file_name=original_name,
        file_type=ext,
        columns=columns,
        rows=rows[:PREVIEW_ROW_LIMIT],
        sheets=sheets,
        warnings=warnings,
    )


def resolve_generation_source(
    parsed_file: ParsedFile,
    selected_sheet: str | None = None,
) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    if not parsed_file.sheets:
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


def _parse_document(file_path: Path) -> tuple[list[str], list[dict[str, Any]], list[str], list[ParsedSheet]]:
    try:
        parsed = parse_document(file_path)
        content_type = str(parsed.get('content_type', 'unknown'))
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
            warnings.append('Таблица в документе получилась слишком пустой, поэтому она пока не используется.')
            columns = []

        if parsed.get('content_type') != 'table' and not rows:
            warnings.append('Документ загружен. Таблица не найдена или пока не подходит для обработки.')

        logger.info(
            'document parser result: file=%s content_type=%s columns=%d rows=%d filtered_sparse_rows=%d',
            file_path.name,
            content_type,
            len(columns),
            len(rows),
            max(filtered_out, 0),
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

        return columns, rows, warnings, sheets
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



def infer_target_fields(target_json_raw: str) -> tuple[list[TargetField], dict[str, Any]]:
    try:
        payload = json.loads(target_json_raw)
    except json.JSONDecodeError as exc:
        raise ParseError(f'Invalid target JSON: {exc}') from exc

    if not isinstance(payload, dict):
        raise ParseError('Target JSON must be an object.')

    fields: list[TargetField] = []
    for key, value in payload.items():
        fields.append(TargetField(name=key, type=_infer_type(value)))
    return fields, payload



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
