from __future__ import annotations

from typing import Any

from models import FieldMapping, TargetField


TS_TYPE_MAP = {
    'string': 'string',
    'number': 'number',
    'boolean': 'boolean',
    'object': 'Record<string, any>',
    'array': 'any[]',
    'null': 'null',
    'any': 'any',
}



def generate_typescript(target_fields: list[TargetField], mappings: list[FieldMapping], interface_name: str = 'GeneratedRow') -> str:
    field_tree = _build_field_tree(target_fields)
    interface_lines = [f'export interface {interface_name} {{']
    interface_lines.extend(_render_interface_lines(field_tree, indent='  '))
    interface_lines.append('}')

    mapping_by_target = {m.target: m for m in mappings}
    helper_lines = _render_runtime_helper_lines(_collect_expected_source_columns(mappings))

    transform_lines = [
        f'export function transform(row: Record<string, any>): {interface_name} {{',
        '  return {',
    ]
    transform_lines.extend(_render_transform_lines(field_tree, mapping_by_target, indent='    '))
    transform_lines.extend(['  };', '}'])

    transform_all_lines = [
        f'export function transformAll(rows: Record<string, any>[]): {interface_name}[] {{',
        '  return rows.map(transform);',
        '}',
    ]

    default_export_lines = [
        f'export default async function transformBase64File(base64file: string): Promise<{interface_name}[]> {{',
        '  const parsedRows = await parseBase64FileToRows(base64file);',
        '  return transformAll(parsedRows);',
        '}',
    ]

    return '\n'.join(interface_lines + [''] + helper_lines + [''] + transform_lines + [''] + transform_all_lines + [''] + default_export_lines)



def build_preview(parsed_rows: list[dict[str, Any]], target_fields: list[TargetField], mappings: list[FieldMapping]) -> list[dict[str, Any]]:
    mapping_by_target = {m.target: m for m in mappings}
    result: list[dict[str, Any]] = []
    for row in parsed_rows:
        out: dict[str, Any] = {}
        for field in target_fields:
            mapping = mapping_by_target.get(field.name)
            raw_value = row.get(mapping.source) if mapping and mapping.source else None
            _set_nested_value(out, field.name.split('.'), _py_cast(field.type, raw_value))
        result.append(out)
    return result


def _build_field_tree(target_fields: list[TargetField]) -> dict[str, Any]:
    tree: dict[str, Any] = {}
    for field in target_fields:
        segments = [segment for segment in str(field.name).split('.') if segment]
        if not segments:
            continue
        current = tree
        for segment in segments[:-1]:
            current = current.setdefault(segment, {})
        current[segments[-1]] = field
    return tree


def _collect_expected_source_columns(mappings: list[FieldMapping]) -> list[str]:
    seen: set[str] = set()
    columns: list[str] = []
    for mapping in mappings:
        source = str(mapping.source or '').strip()
        if not source or source in seen:
            continue
        seen.add(source)
        columns.append(source)
    return columns


def _render_interface_lines(field_tree: dict[str, Any], *, indent: str) -> list[str]:
    lines: list[str] = []
    for key, value in field_tree.items():
        if isinstance(value, TargetField):
            lines.append(f'{indent}{key}: {TS_TYPE_MAP.get(value.type, "any")};')
            continue
        lines.append(f'{indent}{key}: {{')
        lines.extend(_render_interface_lines(value, indent=f'{indent}  '))
        lines.append(f'{indent}}};')
    return lines


def _render_transform_lines(
    field_tree: dict[str, Any],
    mapping_by_target: dict[str, FieldMapping],
    *,
    indent: str,
    prefix: str = '',
) -> list[str]:
    lines: list[str] = []
    for key, value in field_tree.items():
        current_path = f'{prefix}.{key}' if prefix else key
        if isinstance(value, TargetField):
            mapping = mapping_by_target.get(current_path)
            expr = 'undefined as any'
            if mapping and mapping.source:
                expr = _ts_cast(value.type, f'row[{mapping.source!r}]')
            lines.append(f'{indent}{key}: {expr},')
            continue
        lines.append(f'{indent}{key}: {{')
        lines.extend(_render_transform_lines(value, mapping_by_target, indent=f'{indent}  ', prefix=current_path))
        lines.append(f'{indent}}},')
    return lines



def _ts_cast(field_type: str, expr: str) -> str:
    if field_type == 'number':
        return f'Number({expr})'
    if field_type == 'boolean':
        return f'Boolean({expr})'
    if field_type == 'string':
        return expr
    return expr



def _py_cast(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == 'number':
        try:
            text = str(value).replace(' ', '').replace(',', '.')
            return float(text) if '.' in text else int(text)
        except Exception:  # noqa: BLE001
            return value
    if field_type == 'boolean':
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'да'}
    if field_type == 'string':
        return str(value)
    return value


def _set_nested_value(target: dict[str, Any], path: list[str], value: Any) -> None:
    if not path:
        return
    current = target
    for segment in path[:-1]:
        next_value = current.get(segment)
        if not isinstance(next_value, dict):
            next_value = {}
            current[segment] = next_value
        current = next_value
    current[path[-1]] = value


def _render_runtime_helper_lines(expected_source_columns: list[str]) -> list[str]:
    expected_columns_literal = ', '.join(repr(column) for column in expected_source_columns)
    helpers = """
declare const require: undefined | ((specifier: string) => any);

type ParsedSourceRow = Record<string, any>;
type DetectedFileKind = 'json' | 'text' | 'xlsx' | 'docx' | 'pdf' | 'unknown';

type BufferLike = {
  from(input: string, encoding: 'base64'): ArrayLike<number> & { toString(encoding: 'utf-8'): string };
};

type XlsxRuntimeLike = {
  read(data: Uint8Array, options: { type: 'array' }): {
    SheetNames?: string[];
    Sheets?: Record<string, any>;
  };
  utils: {
    sheet_to_json(sheet: any, options?: Record<string, any>): unknown[];
  };
};

type MammothRuntimeLike = {
  convertToHtml(options: { arrayBuffer: ArrayBuffer }): Promise<{ value: string }>;
  extractRawText?(options: { arrayBuffer: ArrayBuffer }): Promise<{ value: string }>;
};

type PdfTextItemLike = {
  str?: string;
  transform?: number[];
};

type PdfPageLike = {
  getTextContent(): Promise<{ items?: PdfTextItemLike[] }>;
};

type PdfDocumentLike = {
  numPages: number;
  getPage(pageNumber: number): Promise<PdfPageLike>;
};

type PdfRuntimeLike = {
  getDocument(options: { data: Uint8Array }): { promise: Promise<PdfDocumentLike> };
};

const EXPECTED_SOURCE_COLUMNS = [__EXPECTED_COLUMNS__];

function decodeBase64ToBytes(base64file: string): Uint8Array {
  const globalBuffer = (globalThis as { Buffer?: BufferLike }).Buffer;
  if (globalBuffer) {
    const binaryView = globalBuffer.from(base64file, 'base64');
    return Uint8Array.from(binaryView);
  }
  if (typeof atob === 'function') {
    const binary = atob(base64file);
    return Uint8Array.from(binary, (char) => char.charCodeAt(0));
  }
  throw new Error('Base64 decoding is not supported in the current runtime.');
}

function decodeBase64ToUtf8(base64file: string): string {
  return decodeBytesToUtf8(decodeBase64ToBytes(base64file));
}

function decodeBytesToUtf8(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

function decodeBytesToLatin1(bytes: Uint8Array): string {
  return Array.from(bytes, (value) => String.fromCharCode(value)).join('');
}

function toArrayBuffer(bytes: Uint8Array): ArrayBuffer {
  return Uint8Array.from(bytes).buffer;
}

async function parseBase64FileToRows(base64file: string): Promise<ParsedSourceRow[]> {
  const bytes = decodeBase64ToBytes(base64file);
  const fileKind = detectFileKind(bytes);
  if (fileKind === 'xlsx') {
    return selectRelevantRowGroup(parseXlsxRows(bytes));
  }
  if (fileKind === 'docx') {
    return selectRelevantRowGroup(await parseDocxRows(bytes));
  }
  if (fileKind === 'pdf') {
    return selectRelevantRowGroup(await parsePdfRows(bytes));
  }
  const text = decodeBytesToUtf8(bytes);
  const parsedJsonRows = tryParseJsonRows(text);
  if (parsedJsonRows !== null) {
    return selectRelevantRowGroup(parsedJsonRows);
  }
  return selectRelevantRowGroup(parseDelimitedText(text));
}

function detectFileKind(bytes: Uint8Array): DetectedFileKind {
  const header = decodeBytesToLatin1(bytes.slice(0, Math.min(bytes.length, 4096)));
  if (header.startsWith('%PDF-')) {
    return 'pdf';
  }
  if (header.startsWith('PK')) {
    const zipText = decodeBytesToLatin1(bytes);
    if (zipText.includes('xl/worksheets') || zipText.includes('xl/workbook.xml')) {
      return 'xlsx';
    }
    if (zipText.includes('word/document.xml')) {
      return 'docx';
    }
    return 'unknown';
  }
  const text = decodeBytesToUtf8(bytes).trim();
  if (!text) {
    return 'text';
  }
  if (text.startsWith('[') || text.startsWith('{')) {
    return 'json';
  }
  return 'text';
}

function tryParseJsonRows(text: string): ParsedSourceRow[] | null {
  const trimmed = text.trim();
  if (!trimmed) {
    return [];
  }
  if (!trimmed.startsWith('[') && !trimmed.startsWith('{')) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (Array.isArray(parsed)) {
      return parsed.filter(isPlainObject) as ParsedSourceRow[];
    }
    if (isPlainObject(parsed) && Array.isArray((parsed as { rows?: unknown }).rows)) {
      return ((parsed as { rows: unknown[] }).rows).filter(isPlainObject) as ParsedSourceRow[];
    }
    return null;
  } catch {
    return null;
  }
}

function parseDelimitedText(text: string): ParsedSourceRow[] {
  const normalized = text.replace(/^\\uFEFF/, '').replace(/\\r\\n/g, '\\n').replace(/\\r/g, '\\n').trim();
  if (!normalized) {
    return [];
  }
  const lines = normalized.split('\\n').map((line) => line.trimEnd()).filter((line) => line.trim().length > 0);
  if (lines.length < 2) {
    return [];
  }
  const delimiter = detectDelimiter(lines.slice(0, Math.min(lines.length, 5)));
  const headers = splitDelimitedLine(lines[0], delimiter).map(normalizeCell);
  if (!headers.length) {
    return [];
  }
  return lines
    .slice(1)
    .map((line) => {
      const cells = splitDelimitedLine(line, delimiter).map(normalizeCell);
      const row: ParsedSourceRow = {};
      headers.forEach((header, index) => {
        if (!header) {
          return;
        }
        row[header] = cells[index] ?? '';
      });
      return row;
    })
    .filter((row) => Object.values(row).some((value) => String(value ?? '').trim().length > 0));
}

function detectDelimiter(lines: string[]): string {
  const candidates = [';', '\\t', ',', '|'];
  let bestDelimiter = ';';
  let bestScore = -1;
  for (const candidate of candidates) {
    let score = 0;
    for (const line of lines) {
      score += Math.max(splitDelimitedLine(line, candidate).length - 1, 0);
    }
    if (score > bestScore) {
      bestScore = score;
      bestDelimiter = candidate;
    }
  }
  return bestDelimiter;
}

function splitDelimitedLine(line: string, delimiter: string): string[] {
  const result: string[] = [];
  let current = '';
  let insideQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === '"') {
      const nextChar = line[index + 1];
      if (insideQuotes && nextChar === '"') {
        current += '"';
        index += 1;
        continue;
      }
      insideQuotes = !insideQuotes;
      continue;
    }
    if (char === delimiter && !insideQuotes) {
      result.push(current);
      current = '';
      continue;
    }
    current += char;
  }
  result.push(current);
  return result;
}

function normalizeCell(value: string): string {
  return decodeHtmlEntities(value.trim().replace(/^"|"$/g, '').replace(/""/g, '"').trim());
}

function isPlainObject(value: unknown): value is ParsedSourceRow {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function normalizeHeaderToken(value: string): string {
  return normalizeCell(String(value ?? ''))
    .toLowerCase()
    .replace(/[\\s_\\-\\/\\\\]+/g, '')
    .replace(/[^\\p{L}\\p{N}]+/gu, '');
}

function rowKeys(row: ParsedSourceRow): string[] {
  return Object.keys(row).map((key) => normalizeCell(key)).filter((key) => key.length > 0);
}

function signatureForHeaders(headers: string[]): string {
  return headers.map(normalizeHeaderToken).filter((value) => value.length > 0).sort().join('|');
}

function scoreHeaders(headers: string[]): number {
  if (!headers.length || !EXPECTED_SOURCE_COLUMNS.length) {
    return 0;
  }
  const normalizedHeaders = headers.map(normalizeHeaderToken).filter((value) => value.length > 0);
  const normalizedExpected = EXPECTED_SOURCE_COLUMNS.map(normalizeHeaderToken).filter((value) => value.length > 0);
  let score = 0;
  for (const header of normalizedHeaders) {
    for (const expected of normalizedExpected) {
      if (!expected) {
        continue;
      }
      if (header === expected) {
        score += 5;
      } else if (header.includes(expected) || expected.includes(header)) {
        score += 2;
      }
    }
  }
  return score;
}

function selectRelevantRowGroup(rows: ParsedSourceRow[]): ParsedSourceRow[] {
  if (!rows.length) {
    return rows;
  }
  const groups = new Map<string, { score: number; rows: ParsedSourceRow[] }>();
  for (const row of rows) {
    const headers = rowKeys(row);
    const signature = signatureForHeaders(headers) || `row-${groups.size}`;
    const existing = groups.get(signature);
    if (existing) {
      existing.rows.push(row);
      continue;
    }
    groups.set(signature, {
      score: scoreHeaders(headers),
      rows: [row],
    });
  }

  let bestGroup: { score: number; rows: ParsedSourceRow[] } | null = null;
  for (const group of groups.values()) {
    if (!bestGroup) {
      bestGroup = group;
      continue;
    }
    if (group.score > bestGroup.score) {
      bestGroup = group;
      continue;
    }
    if (group.score === bestGroup.score && group.rows.length > bestGroup.rows.length) {
      bestGroup = group;
    }
  }

  if (!bestGroup || bestGroup.score <= 0) {
    return rows;
  }
  return bestGroup.rows;
}

function getOptionalGlobal<T>(name: 'XLSX' | 'mammoth' | 'pdfjsLib'): T | null {
  const value = (globalThis as Record<string, unknown>)[name];
  return value ? (value as T) : null;
}

function getOptionalCommonJsModule<T>(specifier: string): T | null {
  if (typeof require !== 'function') {
    return null;
  }
  try {
    return require(specifier) as T;
  } catch {
    return null;
  }
}

async function importOptionalModule<T>(specifier: string): Promise<T | null> {
  const commonJsModule = getOptionalCommonJsModule<T>(specifier);
  if (commonJsModule) {
    return commonJsModule;
  }
  try {
    const dynamicImporter = new Function('specifier', 'return import(specifier);') as (path: string) => Promise<any>;
    const imported = await dynamicImporter(specifier);
    return ((imported?.default ?? imported) as T) || null;
  } catch {
    return null;
  }
}

function parseXlsxRows(bytes: Uint8Array): ParsedSourceRow[] {
  const xlsxRuntime =
    getOptionalGlobal<XlsxRuntimeLike>('XLSX') ??
    getOptionalCommonJsModule<XlsxRuntimeLike>('xlsx');
  if (!xlsxRuntime) {
    throw new Error('Raw XLSX parsing requires the xlsx runtime package.');
  }
  const workbook = xlsxRuntime.read(bytes, { type: 'array' });
  const sheetNames = Array.isArray(workbook.SheetNames) ? workbook.SheetNames : [];
  const parsedRows: ParsedSourceRow[] = [];
  for (const sheetName of sheetNames) {
    const sheet = workbook.Sheets?.[sheetName];
    if (!sheet) {
      continue;
    }
    const rows = xlsxRuntime.utils.sheet_to_json(sheet, { defval: '', raw: false });
    for (const row of rows) {
      if (isPlainObject(row)) {
        parsedRows.push(row);
      }
    }
  }
  return parsedRows;
}

async function parseDocxRows(bytes: Uint8Array): Promise<ParsedSourceRow[]> {
  const mammothRuntime =
    getOptionalGlobal<MammothRuntimeLike>('mammoth') ??
    (await importOptionalModule<MammothRuntimeLike>('mammoth'));
  if (!mammothRuntime || typeof mammothRuntime.convertToHtml !== 'function') {
    throw new Error('Raw DOCX parsing requires the mammoth runtime package.');
  }
  const arrayBuffer = toArrayBuffer(bytes);
  const htmlResult = await mammothRuntime.convertToHtml({ arrayBuffer });
  const html = String(htmlResult?.value ?? '');
  const tableRows = extractRowsFromHtmlTables(html);
  if (tableRows.length > 0) {
    return tableRows;
  }
  if (typeof mammothRuntime.extractRawText === 'function') {
    const rawTextResult = await mammothRuntime.extractRawText({ arrayBuffer });
    return parseDelimitedText(String(rawTextResult?.value ?? ''));
  }
  return parseDelimitedText(stripHtml(html));
}

async function parsePdfRows(bytes: Uint8Array): Promise<ParsedSourceRow[]> {
  const pdfRuntime =
    getOptionalGlobal<PdfRuntimeLike>('pdfjsLib') ??
    (await importOptionalModule<PdfRuntimeLike>('pdfjs-dist/legacy/build/pdf.mjs')) ??
    (await importOptionalModule<PdfRuntimeLike>('pdfjs-dist/legacy/build/pdf'));
  if (!pdfRuntime || typeof pdfRuntime.getDocument !== 'function') {
    throw new Error('Raw PDF parsing requires the pdfjs-dist runtime package.');
  }
  const loadingTask = pdfRuntime.getDocument({ data: bytes });
  const document = await loadingTask.promise;
  const allLines: string[][] = [];
  for (let pageNumber = 1; pageNumber <= document.numPages; pageNumber += 1) {
    const page = await document.getPage(pageNumber);
    const textContent = await page.getTextContent();
    allLines.push(...groupPdfItemsIntoLines(textContent.items ?? []));
  }
  const tableRows = extractRowsFromPdfLines(allLines);
  if (tableRows.length > 0) {
    return tableRows;
  }
  const text = allLines.map((cells) => cells.join(' ')).join('\\n');
  return parseDelimitedText(text);
}

function extractRowsFromHtmlTables(html: string): ParsedSourceRow[] {
  const tables = html.match(/<table[\\s\\S]*?<\\/table>/gi) ?? [];
  const rows: ParsedSourceRow[] = [];
  let anchorHeaders: string[] = [];
  for (const table of tables) {
    const rowMatches = table.match(/<tr[\\s\\S]*?<\\/tr>/gi) ?? [];
    if (rowMatches.length < 1) {
      continue;
    }
    const matrix = rowMatches.map((rowMatch) => extractHtmlCells(rowMatch)).filter((cells) => cells.length > 0);
    if (!matrix.length) {
      continue;
    }
    const firstRow = matrix[0] ?? [];
    const headerScore = scoreHeaders(firstRow);
    let headers: string[] = [];
    let dataStartIndex = 0;
    if (headerScore > 0 || (!anchorHeaders.length && matrix.length >= 2)) {
      headers = firstRow.map(normalizeCell);
      dataStartIndex = 1;
      if (headerScore >= scoreHeaders(anchorHeaders)) {
        anchorHeaders = headers;
      }
    } else if (anchorHeaders.length && firstRow.length === anchorHeaders.length) {
      headers = anchorHeaders;
      dataStartIndex = 0;
    } else {
      continue;
    }
    for (const cells of matrix.slice(dataStartIndex)) {
      if (!cells.length || isLikelySectionHeadingRow(cells)) {
        continue;
      }
      const row = buildRowFromCells(headers, cells);
      if (Object.values(row).some((value) => String(value ?? '').trim().length > 0)) {
        rows.push(row);
      }
    }
  }
  return rows;
}

function extractHtmlCells(rowHtml: string): string[] {
  const cells = rowHtml.match(/<t[dh][^>]*>[\\s\\S]*?<\\/t[dh]>/gi) ?? [];
  return cells.map((cell) => decodeHtmlEntities(stripHtml(cell)).trim()).filter((cell) => cell.length > 0);
}

function stripHtml(value: string): string {
  return value
    .replace(/<br\\s*\\/?/gi, '\\n')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\\s+/g, ' ')
    .trim();
}

function decodeHtmlEntities(value: string): string {
  if (typeof document !== 'undefined') {
    const textarea = document.createElement('textarea');
    textarea.innerHTML = value;
    return textarea.value;
  }
  return value
    .replace(/&nbsp;/g, ' ')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function groupPdfItemsIntoLines(items: PdfTextItemLike[]): string[][] {
  const preparedItems = items
    .map((item) => ({
      text: String(item?.str ?? '').trim(),
      x: Array.isArray(item?.transform) ? Number(item.transform[4] ?? 0) : 0,
      y: Array.isArray(item?.transform) ? Number(item.transform[5] ?? 0) : 0,
    }))
    .filter((item) => item.text.length > 0)
    .sort((left, right) => {
      if (Math.abs(right.y - left.y) > 2) {
        return right.y - left.y;
      }
      return left.x - right.x;
    });

  const lines: Array<Array<{ text: string; x: number; y: number }>> = [];
  for (const item of preparedItems) {
    const line = lines.find((candidate) => Math.abs(candidate[0].y - item.y) <= 3);
    if (line) {
      line.push(item);
    } else {
      lines.push([item]);
    }
  }
  return lines
    .map((line) => line.sort((left, right) => left.x - right.x))
    .map((line) => splitPdfLineIntoCells(line))
    .filter((line) => line.length > 0);
}

function splitPdfLineIntoCells(line: Array<{ text: string; x: number; y: number }>): string[] {
  const cells: string[] = [];
  let currentCell = '';
  let previousX = -1;
  for (const item of line) {
    if (previousX >= 0 && item.x - previousX > 28) {
      if (currentCell.trim()) {
        cells.push(currentCell.trim());
      }
      currentCell = item.text;
    } else {
      currentCell = currentCell ? `${currentCell} ${item.text}` : item.text;
    }
    previousX = item.x;
  }
  if (currentCell.trim()) {
    cells.push(currentCell.trim());
  }
  return cells;
}

function extractRowsFromPdfLines(lines: string[][]): ParsedSourceRow[] {
  const rows: ParsedSourceRow[] = [];
  let anchorHeaders: string[] = [];
  let currentHeaders: string[] = [];
  for (const line of lines) {
    if (line.length < 2) {
      continue;
    }
    const normalizedLine = line.map((cell) => normalizeCell(String(cell ?? ''))).filter((cell) => cell.length > 0);
    if (!normalizedLine.length) {
      continue;
    }
    const headerScore = scoreHeaders(normalizedLine);
    if (headerScore > 0) {
      currentHeaders = normalizedLine;
      if (headerScore >= scoreHeaders(anchorHeaders)) {
        anchorHeaders = normalizedLine;
      }
      continue;
    }
    if (!currentHeaders.length && anchorHeaders.length && normalizedLine.length === anchorHeaders.length) {
      currentHeaders = anchorHeaders;
    }
    if (!currentHeaders.length || isLikelySectionHeadingRow(normalizedLine)) {
      continue;
    }
    const row = buildRowFromCells(currentHeaders, normalizedLine);
    if (Object.values(row).some((value) => String(value ?? '').trim().length > 0)) {
      rows.push(row);
    }
  }
  return rows;
}

function buildRowFromCells(headers: string[], cells: string[]): ParsedSourceRow {
  const row: ParsedSourceRow = {};
  headers.forEach((header, index) => {
    if (!header) {
      return;
    }
    const value = index === headers.length - 1 ? cells.slice(index).join(' ') : cells[index] ?? '';
    row[header] = normalizeCell(String(value ?? ''));
  });
  return row;
}

function isLikelySectionHeadingRow(cells: string[]): boolean {
  if (cells.length !== 2) {
    return false;
  }
  const [left, right] = cells.map((value) => normalizeCell(String(value ?? '')));
  return Boolean(left) && left.endsWith(':') && /^\\d{{3,}}$/.test(right);
}
"""
    helpers = helpers.replace('__EXPECTED_COLUMNS__', expected_columns_literal)
    return [line.rstrip() for line in helpers.strip('\n').splitlines()]
