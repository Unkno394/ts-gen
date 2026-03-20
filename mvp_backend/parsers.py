from __future__ import annotations

import csv
import json
import logging
import sys
from pathlib import Path
from typing import Any

from models import ParsedFile, ParsedSheet, TargetField

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency for Excel support
    pd = None

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
        columns, rows, extra_warnings = _parse_document(file_path)
        warnings.extend(extra_warnings)
    else:
        raise ParseError(f'Unsupported file type: {ext}')

    if not columns:
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
    if parsed_file.file_type not in {'xlsx', 'xls'} or not parsed_file.sheets:
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
    raise ParseError(f'Worksheet "{selected_sheet}" not found. Available sheets: {available_sheets}')



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
    if pd is None:
        raise ParseError('Excel parsing is disabled in this build. Upload CSV, DOCX, or text PDF instead.')

    warnings: list[str] = []
    engine = 'openpyxl' if file_path.suffix.lower() == '.xlsx' else 'xlrd'
    try:
        excel = pd.ExcelFile(file_path, engine=engine)
        combined_columns: list[str] = []
        combined_rows: list[dict[str, Any]] = []
        non_empty_sheets: list[str] = []
        sheets: list[ParsedSheet] = []

        for sheet_name in excel.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
            df = df.where(pd.notnull(df), None)
            raw_columns = df.columns.tolist()
            if any(
                column is None
                or not isinstance(column, str)
                or (isinstance(column, str) and (column.strip() == '' or column.startswith('Unnamed:')))
                for column in raw_columns
            ):
                warnings.append(
                    f'Sheet "{sheet_name}": Excel first row is treated as column headers. Some headers are empty or non-text, so the first row may contain data instead of column names.'
                )

            columns = [str(column) for column in raw_columns]
            df.columns = columns
            rows = [{str(key): value for key, value in row.items()} for row in df.to_dict(orient='records')]

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
        elif len(excel.sheet_names) > 1 and len(non_empty_sheets) == 1:
            warnings.append(f'Workbook has multiple sheets. Used the only non-empty sheet: {non_empty_sheets[0]}')

        return combined_columns, combined_rows[:PREVIEW_ROW_LIMIT], warnings, sheets
    except Exception as exc:  # noqa: BLE001
        raise ParseError(f'Failed to parse Excel: {exc}') from exc


def _parse_document(file_path: Path) -> tuple[list[str], list[dict[str, Any]], list[str]]:
    try:
        parsed = parse_document(file_path)
        content_type = str(parsed.get('content_type', 'unknown'))
        columns = [str(column) for column in parsed.get('columns', [])]
        raw_rows = parsed.get('rows', [])
        rows = [dict(row) for row in raw_rows if isinstance(row, dict)]
        warnings = [str(warning) for warning in parsed.get('warnings', [])]
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
            logger.warning(
                'document parser fallback to non-tabular mode: file=%s content_type=%s warnings=%s',
                file_path.name,
                content_type,
                warnings,
            )
        elif filtered_out > 0:
            logger.warning(
                'document parser dropped sparse rows: file=%s dropped=%d remaining=%d',
                file_path.name,
                filtered_out,
                len(rows),
            )

        return columns, rows, warnings
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
