import {
  ChevronLeft,
  ChevronRight,
  Check,
  Copy,
  Download,
  Eye,
  EyeOff,
  FileSpreadsheet,
  History,
  KeyRound,
  Mail,
  LockKeyhole,
  LogOut,
  Search,
  RotateCcw,
  ShieldCheck,
  Sparkles,
  SquarePen,
  TriangleAlert,
  Upload,
  UserRound,
  WandSparkles,
  X,
} from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import type { ChangeEvent, DragEvent } from 'react';
import type { WorkBook } from 'xlsx';
import {
  applyRepairFromBackend,
  changeEmailWithCode,
  changeEmailWithPassword,
  changePasswordWithBackend,
  confirmGenerationLearning,
  deleteHistoryEntry,
  fetchRepairPreviewFromBackend,
  fetchLearningEvents,
  fetchLearningMemory,
  fetchBackendHealth,
  fetchSourcePreviewFromBackend,
  generateDraftJsonFromBackend,
  generateFromBackend,
  logSourcePreviewRefreshToBackend,
  refreshSourceStructureFromBackend,
  requestEmailChangeCode,
  saveLearningCorrections,
  saveDraftJsonFeedback,
  sendMappingFeedback,
  updateProfileName,
} from '../lib/api';
import type {
  DraftFieldSuggestion,
  BackendHealth,
  FormRepairAction,
  GenerationResult,
  HistoryItem,
  LearningEvent,
  LearningMemory,
  OperationalMappingStatus,
  ManualCorrectionInput,
  MappingInfo,
  ParsedFileInfo,
  ParsedSheetInfo,
  RepairPreviewResult,
  UserProfile,
  ValidationDiagnostic,
} from '../types';
import { VibeBackground } from './VibeBackground';

type Props = {
  profile: UserProfile;
  history: HistoryItem[];
  onLogout: () => void;
  onProfileUpdate: (profile: UserProfile) => void;
  onSaveHistory: () => Promise<void>;
};

type LearningEventFilter = 'all' | LearningEvent['stage'];
type SourceStructureTab = 'tables' | 'fields' | 'text' | 'warnings';

const defaultSchema = `{
  "customerName": "",
  "amount": 0,
  "createdAt": ""
}`;

const CLEAR_HISTORY_BUSY_ID = '__clear_history__';

const defaultCode = `// Generated TypeScript will appear here
export function transform(row: any) {
  return {};
}`;

const CORRECTION_AUTOSAVE_MS = 900;

type CorrectionBaseline = {
  generationId: string;
  schema: string;
  code: string;
  mappings: MappingInfo[];
};

type SectionWorkspaceState = {
  schema: string;
  result: GenerationResult;
  draftSuggestions: DraftFieldSuggestion[];
  draftJsonSaved: boolean;
  generationConfirmed: boolean;
  correctionBaseline: CorrectionBaseline | null;
  mappingReviewNotes: Record<string, string>;
  draftReviewNotes: Record<string, string>;
};

type InsightFeedEntry = {
  id: string;
  title: string;
  badge: string;
  badgeClassName: string;
  categoryLabel: string;
  categoryClassName: string;
  description: string;
  tags: string[];
  note: string | null;
  timestamp: string | null;
};

type UploadedTargetJson = {
  fileName: string;
  content: string;
};

function buildPreviewSheet(name: string, columns: string[], rows: Record<string, unknown>[]): ParsedSheetInfo {
  return {
    name,
    columns,
    rows,
  };
}

function formatPreviewCellValue(value: unknown): string {
  if (Array.isArray(value)) {
    return value.map((item) => String(item ?? '')).filter(Boolean).join(' | ');
  }
  if (value === null || value === undefined) {
    return '';
  }
  if (typeof value === 'object') {
    try {
      return JSON.stringify(value);
    } catch {
      return String(value);
    }
  }
  return String(value);
}

function isGenericFormGroupId(groupId: string): boolean {
  const normalized = groupId.trim().toLowerCase();
  return !normalized || normalized === 'group' || normalized === 'unknown' || normalized.startsWith('group_');
}

function uppercaseRatio(text: string): number {
  const letters = Array.from(text).filter((char) => /[A-Za-zА-Яа-яЁё]/u.test(char));
  if (letters.length === 0) {
    return 0;
  }
  const uppercase = letters.filter((char) => char === char.toUpperCase()).length;
  return uppercase / letters.length;
}

function looksLikeSectionHeadingText(text: string): boolean {
  const normalized = text.trim().replace(/\s+/g, ' ');
  if (!normalized) {
    return false;
  }
  const tokens = normalized.toLowerCase().split(' ');
  const headingKeywords = new Set(['сведения', 'информация', 'данные', 'анкета', 'раздел', 'часть', 'приложение']);
  if (tokens.length >= 2 && headingKeywords.has(tokens[0] ?? '')) {
    return true;
  }
  return uppercaseRatio(normalized) >= 0.9 && tokens.length >= 4;
}

function looksLikeHeadingPair(label: string, value: unknown): boolean {
  const normalizedLabel = label.trim().replace(/\s+/g, ' ');
  const normalizedValue = formatPreviewCellValue(value).trim().replace(/\s+/g, ' ');
  if (!normalizedLabel || !normalizedValue) {
    return false;
  }
  if (!looksLikeSectionHeadingText(normalizedLabel)) {
    return false;
  }
  if (/\d/u.test(`${normalizedLabel} ${normalizedValue}`)) {
    return false;
  }
  return looksLikeSectionHeadingText(normalizedValue) || uppercaseRatio(normalizedValue) >= 0.55;
}

function isUsefulFormPreviewPair(label: string, value: unknown): boolean {
  const normalizedLabel = label.trim().replace(/\s+/g, ' ');
  if (!normalizedLabel || value === null || value === undefined) {
    return false;
  }
  if (normalizedLabel.startsWith('•') || normalizedLabel.startsWith('-') || normalizedLabel.startsWith('*')) {
    return false;
  }
  if (isGenericFormGroupId(normalizedLabel)) {
    return false;
  }

  const valueText = formatPreviewCellValue(value).trim().replace(/\s+/g, ' ');
  if (!valueText) {
    return false;
  }
  if (looksLikeHeadingPair(normalizedLabel, valueText)) {
    return false;
  }

  const labelLetters = normalizedLabel.replace(/[^A-Za-zА-Яа-яЁё]/g, '');
  const valueLetters = valueText.replace(/[^A-Za-zА-Яа-яЁё]/g, '');
  if (
    labelLetters.length >= 16 &&
    valueLetters.length >= 8 &&
    labelLetters === labelLetters.toUpperCase() &&
    valueLetters === valueLetters.toUpperCase()
  ) {
    return false;
  }

  return true;
}

function normalizeFormConsumptionText(value: unknown): string {
  const text = formatPreviewCellValue(value).trim().replace(/\s+/g, ' ');
  if (!text) {
    return '';
  }

  return text
    .replace(/^[\[(<{«"']*\s*(?:x|х|v|✓|✔|☒|☑)\s*[\])}>»"']*\s*/iu, '')
    .replace(/^[•*·\-–—]+\s*/u, '')
    .replace(/\s+/g, ' ')
    .toLowerCase();
}

function isStandaloneOptionMarker(value: unknown): boolean {
  return /^(?:x|х|v|✓|✔|☒|☑|\[\s*(?:x|х|v|✓|✔)\s*\])$/iu.test(String(value ?? '').trim());
}

function collectConsumedFormOptionTexts(parsedFile: ParsedFileInfo): Set<string> {
  const consumedTexts = new Set<string>();
  const groups = parsedFile.formModel?.groups ?? [];

  groups.forEach((group) => {
    if (!isObjectRecord(group)) {
      return;
    }

    const options = Array.isArray(group.options) ? group.options : [];
    options.forEach((option) => {
      if (!isObjectRecord(option)) {
        return;
      }

      const label = normalizeFormConsumptionText(option.label);
      if (label) {
        consumedTexts.add(label);
      }

      const markerText = normalizeFormConsumptionText(option.marker_text);
      if (label && markerText) {
        consumedTexts.add(normalizeFormConsumptionText(`${markerText} ${label}`));
      }
    });
  });

  return consumedTexts;
}

function isConsumedFormOptionEntry(label: string, value: unknown, consumedTexts: Set<string>): boolean {
  if (consumedTexts.size === 0) {
    return false;
  }

  const normalizedLabel = normalizeFormConsumptionText(label);
  const normalizedValue = normalizeFormConsumptionText(value);
  const normalizedCombined = normalizeFormConsumptionText(`${label} ${formatPreviewCellValue(value)}`);

  if (normalizedLabel && consumedTexts.has(normalizedLabel)) {
    return true;
  }
  if (normalizedCombined && consumedTexts.has(normalizedCombined)) {
    return true;
  }
  if (isStandaloneOptionMarker(label) && normalizedValue && consumedTexts.has(normalizedValue)) {
    return true;
  }

  return false;
}

function buildResolvedFormPreviewRows(parsedFile: ParsedFileInfo): Record<string, unknown>[] {
  const resolvedFields = parsedFile.formModel?.resolvedFields ?? [];
  return resolvedFields.flatMap((field) => {
    if (!isObjectRecord(field)) {
      return [];
    }
    const label = typeof field.field === 'string' ? field.field.trim() : '';
    const status = typeof field.status === 'string' ? field.status.trim() : '';
    const value = field.value;
    const resolvedBy = typeof field.resolved_by === 'string' ? field.resolved_by.trim() : 'resolved';
    if (!label || !['resolved', 'weak_match'].includes(status) || value === null || value === undefined) {
      return [];
    }
    return [
      {
        field: label,
        value: formatPreviewCellValue(value),
        source: resolvedBy,
      },
    ];
  });
}

function buildFormPreviewSheet(parsedFile: ParsedFileInfo): ParsedSheetInfo | null {
  const formModel = parsedFile.formModel;
  if (!formModel) {
    return null;
  }

  const consumedOptionTexts = collectConsumedFormOptionTexts(parsedFile);
  const rows: Record<string, unknown>[] = buildResolvedFormPreviewRows(parsedFile);
  const seenEntries = new Set<string>();

  rows.forEach((row) => {
    if (typeof row.field === 'string') {
      seenEntries.add(`resolved:${row.field}`);
      seenEntries.add(`pair:${row.field}`);
    }
  });

  if (rows.length === 0) {
    const scalarRows: Record<string, unknown>[] = [];
    formModel.scalars.forEach((scalar) => {
      if (!isObjectRecord(scalar)) {
        return;
      }

      const label = typeof scalar.label === 'string' ? scalar.label.trim() : '';
      const value = scalar.value;
      if (!isUsefulFormPreviewPair(label, value) || isConsumedFormOptionEntry(label, value, consumedOptionTexts)) {
        return;
      }

      const key = `scalar:${label}`;
      if (seenEntries.has(key) || seenEntries.has(`pair:${label}`)) {
        return;
      }
      seenEntries.add(key);
      scalarRows.push({
        field: label,
        value: formatPreviewCellValue(value),
        source: 'scalar',
      });
    });
    rows.push(...scalarRows);
  }

  formModel.groups.forEach((group) => {
    if (!isObjectRecord(group)) {
      return;
    }

    const groupId = typeof group.group_id === 'string' ? group.group_id.trim() : '';
    const question = typeof group.question === 'string' ? group.question.trim() : '';
    const options = Array.isArray(group.options) ? group.options : [];
    const selectedLabels = options
      .filter((option): option is Record<string, unknown> => isObjectRecord(option))
      .flatMap((option) => {
        const label = typeof option.label === 'string' ? option.label.trim() : '';
        return option.selected === true && label ? [label] : [];
      });

    if (selectedLabels.length === 0) {
      return;
    }

    const label = groupId && !isGenericFormGroupId(groupId) ? groupId : question;
    if (!label || !isUsefulFormPreviewPair(label, selectedLabels.join(' | '))) {
      return;
    }

    const key = `group:${label}`;
    if (seenEntries.has(key) || seenEntries.has(`pair:${label}`)) {
      return;
    }
    seenEntries.add(key);
    rows.push({
      field: label,
      value: selectedLabels.join(' | '),
      source: 'group',
    });
  });

  if (rows.length === 0) {
    return null;
  }

  return buildPreviewSheet('Form extraction', ['field', 'value', 'source'], rows);
}

function shouldPreferTableSheetsInFormPreview(parsedFile: ParsedFileInfo | null): boolean {
  return parsedFile?.documentMode === 'form_layout_mode' && parsedFile.sheets.length > 1;
}

function buildPreviewSheetsForParsedFile(parsedFile: ParsedFileInfo | null): ParsedSheetInfo[] {
  if (!parsedFile) {
    return [];
  }

  if (shouldPreferTableSheetsInFormPreview(parsedFile)) {
    return parsedFile.sheets;
  }

  if (parsedFile.documentMode === 'form_layout_mode') {
    const formPreviewSheet = buildFormPreviewSheet(parsedFile);
    if (formPreviewSheet) {
      return [formPreviewSheet];
    }
  }

  if (parsedFile.sheets.length > 0) {
    return parsedFile.sheets;
  }

  if (parsedFile.columns.length === 0 && parsedFile.rows.length === 0) {
    return [];
  }

  return [buildPreviewSheet(parsedFile.fileName, parsedFile.columns, parsedFile.rows)];
}

function buildPreviewSheetSignature(sheet: ParsedSheetInfo | null): string {
  if (!sheet) {
    return '';
  }

  return JSON.stringify({
    name: sheet.name,
    columns: sheet.columns,
    rows: sheet.rows.map((row) => sheet.columns.map((column) => formatPreviewCellValue(row[column]))),
  });
}

function buildPreviewSheetCollectionSignature(sheets: ParsedSheetInfo[]): string {
  return JSON.stringify(
    sheets.map((sheet) => ({
      name: sheet.name,
      columns: sheet.columns,
      rows: sheet.rows.map((row) => sheet.columns.map((column) => formatPreviewCellValue(row[column]))),
    }))
  );
}

function describeSourceStructureRefresh(previousParsedFile: ParsedFileInfo | null, nextParsedFile: ParsedFileInfo, preferredSheetName: string | null) {
  const previousSheets = buildPreviewSheetsForParsedFile(previousParsedFile);
  const nextSheets = buildPreviewSheetsForParsedFile(nextParsedFile);
  const fallbackSheetName = preferredSheetName ?? previousSheets[0]?.name ?? nextSheets[0]?.name ?? null;
  const previousActiveSheet = previousSheets.find((sheet) => sheet.name === fallbackSheetName) ?? previousSheets[0] ?? null;
  const nextActiveSheet = nextSheets.find((sheet) => sheet.name === fallbackSheetName) ?? nextSheets[0] ?? null;
  const activeSheetChanged = buildPreviewSheetSignature(previousActiveSheet) !== buildPreviewSheetSignature(nextActiveSheet);
  const structureChanged = buildPreviewSheetCollectionSignature(previousSheets) !== buildPreviewSheetCollectionSignature(nextSheets);
  const details: string[] = [];

  if (previousSheets.length !== nextSheets.length) {
    details.push(`листов ${previousSheets.length}→${nextSheets.length}`);
  }

  if ((previousActiveSheet?.name ?? null) !== (nextActiveSheet?.name ?? null) && (previousActiveSheet || nextActiveSheet)) {
    details.push(`активная таблица "${previousActiveSheet?.name ?? 'нет'}" → "${nextActiveSheet?.name ?? 'нет'}"`);
  }

  if ((previousActiveSheet?.columns.length ?? 0) !== (nextActiveSheet?.columns.length ?? 0)) {
    details.push(`колонки ${(previousActiveSheet?.columns.length ?? 0)}→${nextActiveSheet?.columns.length ?? 0}`);
  }

  if ((previousActiveSheet?.rows.length ?? 0) !== (nextActiveSheet?.rows.length ?? 0)) {
    details.push(`строки ${(previousActiveSheet?.rows.length ?? 0)}→${nextActiveSheet?.rows.length ?? 0}`);
  }

  if (activeSheetChanged && details.length === 0) {
    details.push('содержимое обновлено');
  }

  const networkResult: 'changed' | 'unchanged' | 'initial' = !previousParsedFile
    ? 'initial'
    : activeSheetChanged || structureChanged
      ? 'changed'
      : 'unchanged';

  let message = 'Перегенерация завершена: структура источника обновлена.';
  if (!previousParsedFile) {
    message = nextActiveSheet
      ? `Перегенерация завершена: таблица "${nextActiveSheet.name}" собрана.`
      : 'Перегенерация завершена: структура источника собрана.';
  } else if (!structureChanged && !activeSheetChanged) {
    message = previousActiveSheet
      ? `Перегенерация завершена: таблица "${previousActiveSheet.name}" не изменилась.`
      : 'Перегенерация завершена: структура источника не изменилась.';
  } else if (activeSheetChanged) {
    const targetSheetLabel = nextActiveSheet?.name ?? previousActiveSheet?.name ?? null;
    const suffix = details.length > 0 ? ` (${details.join(', ')})` : '';
    message = targetSheetLabel
      ? `Перегенерация завершена: таблица "${targetSheetLabel}" изменилась${suffix}.`
      : `Перегенерация завершена: структура источника обновлена${suffix}.`;
  } else {
    const targetSheetLabel = nextActiveSheet?.name ?? previousActiveSheet?.name ?? null;
    const suffix = details.length > 0 ? ` (${details.join(', ')})` : '';
    message = targetSheetLabel
      ? `Перегенерация завершена: таблица "${targetSheetLabel}" не изменилась, но структура источника обновилась${suffix}.`
      : `Перегенерация завершена: структура источника обновилась${suffix}.`;
  }

  return {
    activeSheetChanged,
    structureChanged,
    networkResult,
    message,
    nextActiveSheetName: nextActiveSheet?.name ?? null,
    consolePayload: {
      activeSheetChanged,
      structureChanged,
      previousSheetName: previousActiveSheet?.name ?? null,
      nextSheetName: nextActiveSheet?.name ?? null,
      previousSheetCount: previousSheets.length,
      nextSheetCount: nextSheets.length,
      previousColumnCount: previousActiveSheet?.columns.length ?? 0,
      nextColumnCount: nextActiveSheet?.columns.length ?? 0,
      previousRowCount: previousActiveSheet?.rows.length ?? 0,
      nextRowCount: nextActiveSheet?.rows.length ?? 0,
      details,
    },
  };
}

function needsBackendSourcePreview(extension: string | undefined): boolean {
  return ['pdf', 'docx', 'txt', 'png', 'jpg', 'jpeg', 'bmp', 'gif', 'tif', 'tiff', 'webp'].includes(extension ?? '');
}

function parsedContentTypeLabel(contentType: ParsedFileInfo['contentType']): string {
  if (contentType === 'mixed') return 'Смешанный источник';
  if (contentType === 'form') return 'Анкета / форма';
  if (contentType === 'text') return 'Текстовый документ';
  if (contentType === 'image_like') return 'Скан / изображение';
  if (contentType === 'table') return 'Табличный источник';
  return 'Источник';
}

function parsedExtractionStatusLabel(extractionStatus: string): string {
  if (extractionStatus === 'structured_extracted') return 'Таблица извлечена';
  if (extractionStatus === 'text_extracted') return 'Текст извлечён';
  if (extractionStatus === 'requires_ocr_or_manual_input') return 'Нужен OCR или ручной ввод';
  if (extractionStatus === 'image_parse_not_supported_yet') return 'OCR service недоступен или не дал текст';
  if (extractionStatus === 'text_not_extracted') return 'Текст не извлечён';
  return 'Статус неизвестен';
}

function getExtractedFieldEntries(parsedFile: ParsedFileInfo | null): Array<{ kind: 'kv_pair' | 'text_fact'; label: string; value: string; hint?: string | null }> {
  if (!parsedFile) {
    return [];
  }

  const consumedOptionTexts = collectConsumedFormOptionTexts(parsedFile);
  const kvEntries = parsedFile.kvPairs
    .filter((pair) => !isConsumedFormOptionEntry(pair.label, pair.value, consumedOptionTexts))
    .map((pair) => ({
      kind: 'kv_pair' as const,
      label: pair.label,
      value: pair.value,
      hint: pair.sourceText ?? null,
    }));
  const factEntries = parsedFile.sourceCandidates
    .filter((candidate) => candidate.candidateType === 'text_fact')
    .map((candidate) => ({
      kind: 'text_fact' as const,
      label: candidate.label,
      value: String(candidate.value ?? ''),
      hint: candidate.sourceText ?? null,
    }))
    .filter((entry) => entry.value.trim().length > 0)
    .filter((entry) => !isConsumedFormOptionEntry(entry.label, entry.value, consumedOptionTexts));

  return [...kvEntries, ...factEntries];
}

function collectParsedSourceColumns(parsedFile: ParsedFileInfo | null, currentPreviewSheet: ParsedSheetInfo | null): string[] {
  const columns = currentPreviewSheet?.columns ?? parsedFile?.columns ?? [];
  const extractedFields = getExtractedFieldEntries(parsedFile).map((entry) => entry.label);
  return Array.from(new Set([...columns, ...extractedFields]));
}

function preferredSourceStructureTab(parsedFile: ParsedFileInfo | null): SourceStructureTab {
  if (!parsedFile) {
    return 'warnings';
  }
  if (
    parsedFile.documentMode === 'form_layout_mode' ||
    parsedFile.sheets.length > 0 ||
    parsedFile.columns.length > 0 ||
    parsedFile.rows.length > 0
  ) {
    return 'tables';
  }
  if (parsedFile.kvPairs.length > 0) {
    return 'fields';
  }
  if ((parsedFile.rawText ?? '').trim().length > 0) {
    return 'text';
  }
  return 'warnings';
}

function hasVisibleWorkbookCell(value: unknown): boolean {
  if (value === null || value === undefined) {
    return false;
  }
  if (typeof value === 'string') {
    return value.trim() !== '';
  }
  return true;
}

function splitWorkbookRowBlocks(rows: unknown[][]): unknown[][][] {
  const blocks: unknown[][][] = [];
  let currentBlock: unknown[][] = [];

  rows.forEach((rawRow) => {
    const row = Array.isArray(rawRow) ? rawRow : [];
    if (row.some((value) => hasVisibleWorkbookCell(value))) {
      currentBlock.push(row);
      return;
    }
    if (currentBlock.length > 0) {
      blocks.push(currentBlock);
      currentBlock = [];
    }
  });

  if (currentBlock.length > 0) {
    blocks.push(currentBlock);
  }

  return blocks;
}

function countVisibleWorkbookCells(row: unknown[]): number {
  return row.reduce<number>((count, value) => count + (hasVisibleWorkbookCell(value) ? 1 : 0), 0);
}

function looksLikeWorkbookTableTitle(row: unknown[]): boolean {
  const visibleValues = row.filter((value) => hasVisibleWorkbookCell(value));
  return visibleValues.length === 1 && typeof visibleValues[0] === 'string' && visibleValues[0].trim() !== '';
}

function firstVisibleWorkbookCellText(row: unknown[]): string | null {
  const value = row.find((item) => hasVisibleWorkbookCell(item));
  if (value === undefined) {
    return null;
  }
  return String(value).trim();
}

function stringifyWorkbookHeader(value: unknown, index: number): string {
  if (value === null || value === undefined) {
    return `Column ${index}`;
  }
  if (typeof value === 'string') {
    const stripped = value.trim();
    return stripped || `Column ${index}`;
  }
  return String(value);
}

function buildWorkbookTableName(sheetName: string, tableIndex: number, totalTables: number, tableTitle: string | null): string {
  const normalizedSheetName = sheetName.trim() || `Sheet ${tableIndex}`;
  const normalizedTitle = tableTitle?.trim() ?? '';
  if (totalTables > 1) {
    if (normalizedTitle) {
      return `${normalizedSheetName} · ${normalizedTitle}`;
    }
    return `${normalizedSheetName} · Table ${tableIndex}`;
  }
  return normalizedSheetName;
}

function workbookRowsToRecords(sheetName: string, rowsIter: unknown[][]): {
  columns: string[];
  rows: Record<string, unknown>[];
  warnings: string[];
} {
  const warnings: string[] = [];
  if (rowsIter.length === 0) {
    return { columns: [], rows: [], warnings };
  }

  const rawHeaders = rowsIter[0] ?? [];
  if (rawHeaders.length === 0) {
    return { columns: [], rows: [], warnings };
  }

  if (
    rawHeaders.some(
      (column) =>
        column === null ||
        column === undefined ||
        typeof column !== 'string' ||
        (typeof column === 'string' && (column.trim() === '' || column.startsWith('Unnamed:')))
    )
  ) {
    warnings.push(
      `Sheet "${sheetName}": Excel first row is treated as column headers. Some headers are empty or non-text, so the first row may contain data instead of column names.`
    );
  }

  const columns = rawHeaders.map((column, index) => stringifyWorkbookHeader(column, index + 1));
  const rows = rowsIter.slice(1).flatMap((rawRow) => {
    const values = Array.isArray(rawRow) ? rawRow : [];
    if (!values.some((value) => hasVisibleWorkbookCell(value))) {
      return [];
    }

    const record: Record<string, unknown> = {};
    columns.forEach((column, index) => {
      record[column] = values[index] ?? null;
    });
    return [record];
  });

  return { columns, rows, warnings };
}

function extractWorkbookTablesFromSheet(sheetName: string, rows: unknown[][]): {
  tables: ParsedSheetInfo[];
  warnings: string[];
} {
  const warnings: string[] = [];
  const blocks = splitWorkbookRowBlocks(rows);
  if (blocks.length === 0) {
    return { tables: [], warnings };
  }

  const parsedTables: Array<{ title: string | null; columns: string[]; rows: Record<string, unknown>[] }> = [];
  blocks.forEach((block) => {
    let tableTitle: string | null = null;
    let blockRows = block;
    if (block.length >= 2 && looksLikeWorkbookTableTitle(block[0]) && countVisibleWorkbookCells(block[1]) >= 2) {
      tableTitle = firstVisibleWorkbookCellText(block[0]);
      blockRows = block.slice(1);
    }

    const parsed = workbookRowsToRecords(sheetName, blockRows);
    warnings.push(...parsed.warnings);
    if (parsed.columns.length === 0 && parsed.rows.length === 0) {
      return;
    }
    parsedTables.push({
      title: tableTitle,
      columns: parsed.columns,
      rows: parsed.rows,
    });
  });

  const tables = parsedTables.map((table, index) =>
    buildPreviewSheet(buildWorkbookTableName(sheetName, index + 1, parsedTables.length, table.title), table.columns, table.rows.slice(0, 8))
  );

  if (tables.length > 1) {
    warnings.push(`Sheet "${sheetName}" was split into ${tables.length} tables.`);
  }

  return { tables, warnings };
}

function parseWorkbookSheets(workbook: WorkBook, xlsxModule: typeof import('xlsx')): {
  columns: string[];
  rows: Record<string, unknown>[];
  sheets: ParsedSheetInfo[];
  warnings: string[];
} {
  const warnings: string[] = [];
  const sheets = workbook.SheetNames.flatMap((sheetName) => {
    const sheet = workbook.Sheets[sheetName];
    const rows = xlsxModule.utils.sheet_to_json<unknown[]>(sheet, {
      header: 1,
      defval: null,
      raw: true,
      blankrows: true,
    });
    const extracted = extractWorkbookTablesFromSheet(sheetName, rows);
    warnings.push(...extracted.warnings);
    return extracted.tables;
  }).filter((sheet) => sheet.columns.length > 0 || sheet.rows.length > 0);

  const firstSheet = sheets[0] ?? buildPreviewSheet(workbook.SheetNames[0] ?? 'Sheet 1', [], []);

  if (workbook.SheetNames.length > 1) {
    warnings.push(`Preview is split by sheets. Found ${workbook.SheetNames.length} sheet(s).`);
  }

  if (sheets.length === 0) {
    warnings.push('No previewable rows were found in the workbook.');
  }

  return {
    columns: firstSheet.columns,
    rows: firstSheet.rows,
    sheets,
    warnings,
  };
}

type PasswordInputProps = {
  icon: typeof LockKeyhole;
  placeholder: string;
  value: string;
  onChange: (value: string) => void;
};

function PasswordInput({ icon: Icon, placeholder, value, onChange }: PasswordInputProps) {
  const [visible, setVisible] = useState(false);

  return (
    <div className="auth-input-wrap">
      <Icon size={18} />
      <input
        placeholder={placeholder}
        type={visible ? 'text' : 'password'}
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
      <button
        className="password-toggle"
        onClick={() => setVisible((current) => !current)}
        tabIndex={-1}
        title={visible ? 'Скрыть пароль' : 'Показать пароль'}
        type="button"
      >
        {visible ? <EyeOff size={18} /> : <Eye size={18} />}
      </button>
    </div>
  );
}

async function parseFile(file: File): Promise<ParsedFileInfo> {
  const extension = file.name.split('.').pop()?.toLowerCase() ?? 'unknown';

  if (extension === 'csv') {
    const text = await file.text();
    const lines = text.split(/\r?\n/).filter((line) => line.trim() !== '');
    const [headerLine = '', ...dataLines] = lines;
    const columns = headerLine ? headerLine.split(',').map((item) => item.trim()) : [];
    const rows = dataLines.slice(0, 8).map((line) => {
      const cells = line.split(',');
      return Object.fromEntries(columns.map((column, index) => [column, cells[index] ?? '']));
    });

    return {
      fileName: file.name,
      extension,
      columns,
      rows,
      contentType: 'table',
      extractionStatus: 'structured_extracted',
      rawText: '',
      textBlocks: [],
      sections: [],
      kvPairs: [],
      sourceCandidates: [],
      sheets: [buildPreviewSheet(file.name, columns, rows)],
      warnings: rows.length === 0 ? ['В файле нет строк данных.'] : [],
    };
  }

  if (extension === 'xlsx' || extension === 'xls') {
    const XLSX = await import('xlsx');
    const buffer = await file.arrayBuffer();
    const workbook = XLSX.read(buffer, { type: 'array' });
    const workbookPreview = parseWorkbookSheets(workbook, XLSX);

    return {
      fileName: file.name,
      extension,
      columns: workbookPreview.columns,
      rows: workbookPreview.rows,
      contentType: 'table',
      extractionStatus: 'structured_extracted',
      rawText: '',
      textBlocks: [],
      sections: [],
      kvPairs: [],
      sourceCandidates: [],
      sheets: workbookPreview.sheets,
      warnings: workbookPreview.warnings,
    };
  }


  if (needsBackendSourcePreview(extension)) {
    return {
      fileName: file.name,
      extension,
      columns: [],
      rows: [],
      contentType: 'unknown',
      extractionStatus: 'unknown',
      rawText: '',
      textBlocks: [],
      sections: [],
      kvPairs: [],
      sourceCandidates: [],
      sheets: [],
      warnings: ['Предварительную структуру источника прочитаем на backend.'],
    };
  }

  return {
    fileName: file.name,
    extension,
    columns: [],
    rows: [],
    contentType: 'unknown',
    extractionStatus: 'unknown',
    rawText: '',
    textBlocks: [],
    sections: [],
    kvPairs: [],
    sourceCandidates: [],
    sheets: [],
    warnings: ['Поддерживаются CSV, XLSX, XLS, PDF, DOCX, TXT, фотографии и сканы через backend OCR service.'],
  };
}

function cloneMappings(mappings: MappingInfo[]): MappingInfo[] {
  return mappings.map((mapping) => ({ ...mapping }));
}

function cloneDraftSuggestions(suggestions: DraftFieldSuggestion[]): DraftFieldSuggestion[] {
  return suggestions.map((suggestion) => ({ ...suggestion }));
}

function buildCorrectionBaseline(generationId: string, schema: string, result: GenerationResult): CorrectionBaseline {
  return {
    generationId,
    schema,
    code: result.code,
    mappings: cloneMappings(result.mappings),
  };
}

function buildSectionCacheKey(fileName: string | undefined, sectionName: string | null | undefined): string {
  return `${fileName ?? 'file'}::${sectionName ?? 'default'}`;
}

function buildSectionWorkspaceState(params: {
  schema: string;
  result: GenerationResult;
  draftSuggestions?: DraftFieldSuggestion[];
  draftJsonSaved?: boolean;
  generationConfirmed?: boolean;
  correctionBaseline?: CorrectionBaseline | null;
  mappingReviewNotes?: Record<string, string>;
  draftReviewNotes?: Record<string, string>;
}): SectionWorkspaceState {
  return {
    schema: params.schema,
    result: {
      ...params.result,
      mappings: cloneMappings(params.result.mappings),
      preview: params.result.preview.map((row) => ({ ...row })),
    },
    draftSuggestions: cloneDraftSuggestions(params.draftSuggestions ?? []),
    draftJsonSaved: params.draftJsonSaved ?? false,
    generationConfirmed: params.generationConfirmed ?? false,
    correctionBaseline: params.correctionBaseline
      ? {
          ...params.correctionBaseline,
          mappings: cloneMappings(params.correctionBaseline.mappings),
        }
      : null,
    mappingReviewNotes: { ...(params.mappingReviewNotes ?? {}) },
    draftReviewNotes: { ...(params.draftReviewNotes ?? {}) },
  };
}

function detachGenerationLinkFromSectionState(state: SectionWorkspaceState, generationId: string): SectionWorkspaceState {
  const generationMatches = state.result.generationId === generationId;
  const baselineMatches = state.correctionBaseline?.generationId === generationId;

  if (!generationMatches && !baselineMatches) {
    return state;
  }

  return {
    ...state,
    result: generationMatches ? { ...state.result, generationId: null } : state.result,
    generationConfirmed: generationMatches ? false : state.generationConfirmed,
    correctionBaseline: baselineMatches ? null : state.correctionBaseline,
  };
}

function parseSchemaFields(schemaText: string): string[] {
  try {
    const parsed = JSON.parse(schemaText) as Record<string, unknown> | Array<Record<string, unknown>>;
    if (Array.isArray(parsed)) {
      const firstObject = parsed.find((item) => isObjectRecord(item));
      return firstObject ? Object.keys(firstObject) : [];
    }
    if (!parsed || typeof parsed !== 'object') {
      return [];
    }
    return Object.keys(parsed);
  } catch {
    return [];
  }
}

function parseMaybeJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function compactStrings(values: Array<string | null | undefined>): string[] {
  return values.filter((value): value is string => typeof value === 'string' && value.trim().length > 0);
}

function confidenceToScore(confidence: MappingInfo['confidence']): number {
  switch (confidence) {
    case 'high':
      return 1;
    case 'medium':
      return 0.7;
    case 'low':
      return 0.35;
    default:
      return 0;
  }
}

function buildSchemaFromDraftSuggestions(suggestions: DraftFieldSuggestion[]): string {
  const payload: Record<string, unknown> = {};
  suggestions.forEach((suggestion) => {
    const key = suggestion.targetField.trim();
    if (!key) {
      return;
    }
    payload[key] = suggestion.defaultValue;
  });
  return JSON.stringify(payload, null, 2);
}

function buildRepairActionKey(action: FormRepairAction): string {
  return [action.kind, action.targetField ?? '', action.groupId ?? '', action.chunkRefs.lineIds.join('|')].join('::');
}

function repairActionLabel(action: FormRepairAction): string {
  if (action.targetField) {
    return action.targetField;
  }
  if (action.groupId) {
    return action.groupId;
  }
  return action.kind;
}

function repairActionChunkCount(action: FormRepairAction): number {
  return new Set([...action.chunkRefs.groupIds, ...action.chunkRefs.scalarLabels, ...action.chunkRefs.lineIds]).size;
}

function mappingStatusLabel(mapping: MappingInfo): string {
  if (mapping.status === 'rejected') {
    return 'Отклонено';
  }
  if (mappingNeedsExplicitReview(mapping) && (mapping.status ?? 'suggested') === 'suggested') {
    return 'На проверке';
  }
  return 'Подтверждено';
}

function draftSuggestionStatusLabel(suggestion: DraftFieldSuggestion): string {
  if (suggestion.status === 'rejected') {
    return 'Отклонено';
  }
  if (draftSuggestionNeedsExplicitReview(suggestion) && (suggestion.status ?? 'suggested') === 'suggested') {
    return 'На проверке';
  }
  return 'Подтверждено';
}

function mappingSourceLabel(mapping: MappingInfo): string {
  if (mapping.sourceOfTruth === 'personal_memory') {
    return 'Персональная память';
  }
  if (mapping.sourceOfTruth === 'global_pattern') {
    return 'Глобальный паттерн';
  }
  if (mapping.sourceOfTruth === 'model_suggestion') {
    return 'Модель';
  }
  if (mapping.sourceOfTruth === 'semantic_graph') {
    return 'Семантический граф';
  }
  if (mapping.sourceOfTruth === 'deterministic_rule') {
    return 'Системное правило';
  }
  if (mapping.sourceOfTruth === 'position_fallback') {
    return 'По позиции';
  }
  if (mapping.sourceOfTruth === 'unresolved') {
    return 'Не определено';
  }
  return 'Источник не указан';
}

function qualityBandLabel(status: OperationalMappingStatus['status'] | undefined | null): string {
  if (status === 'high') {
    return 'Высокое';
  }
  if (status === 'medium') {
    return 'Среднее';
  }
  if (status === 'low') {
    return 'Низкое';
  }
  return 'Нет данных';
}

function qualityBandTone(status: OperationalMappingStatus['status'] | undefined | null): string {
  if (status === 'high') {
    return 'success';
  }
  if (status === 'medium') {
    return 'warning';
  }
  if (status === 'low') {
    return 'danger';
  }
  return 'neutral';
}

function validationStateLabel(valid: boolean | undefined, okText = 'OK', failText = 'Ошибка'): string {
  return valid ? okText : failText;
}

function validationStateTone(valid: boolean | undefined): string {
  return valid ? 'success' : 'danger';
}

function formatRatio(value: number | undefined | null): string {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return '0%';
  }
  return `${Math.round(value * 100)}%`;
}

function formatDiagnosticLabel(diagnostic: ValidationDiagnostic): string {
  const parts = [diagnostic.path, diagnostic.code].filter(Boolean);
  return parts.length > 0 ? parts.join(' · ') : 'diagnostic';
}

function semanticGraphRelationLabel(relationKind: LearningMemory['layers']['semanticGraph']['items'][number]['relationKind']): string {
  return relationKind === 'mapping_synonym' ? 'Семантическая связь' : 'Семантический конфликт';
}

function semanticGraphRoleLabel(role: string | null | undefined): string {
  if (role === 'identifier') return 'identifier';
  if (role === 'label') return 'label';
  if (role === 'description') return 'description';
  if (role === 'timestamp') return 'timestamp';
  if (role === 'numeric_value') return 'numeric';
  if (role === 'flag') return 'flag';
  if (role === 'unit') return 'unit';
  return role ?? 'role';
}

function memoryPatternStatusLabel(status: LearningMemory['layers']['globalKnowledge']['items'][number]['status']): string {
  if (status === 'shared_promoted') return 'Shared promoted';
  if (status === 'shared_candidate') return 'Shared candidate';
  if (status === 'blocked_sensitive') return 'Blocked sensitive';
  return 'Personal only';
}

function memoryPatternStatusTone(status: LearningMemory['layers']['globalKnowledge']['items'][number]['status']): string {
  if (status === 'shared_promoted') return 'promoted';
  if (status === 'shared_candidate') return 'candidate';
  if (status === 'blocked_sensitive') return 'blocked';
  return 'personal';
}

function formatMetricScore(value: number | null | undefined): string {
  if (typeof value !== 'number' || Number.isNaN(value)) {
    return '—';
  }
  return value.toFixed(2);
}

function previewSectionLabel(extension: string | undefined, totalSections: number): string {
  if (extension === 'pdf' || extension === 'docx') {
    return totalSections > 1 ? 'Таблицы документа' : 'Таблица документа';
  }
  if (extension === 'xlsx' || extension === 'xls') {
    return totalSections > 1 ? 'Таблицы файла' : 'Таблица файла';
  }
  return 'Секции preview';
}

function previewSectionStatusLabel(status: 'idle' | 'cached' | 'confirmed' | 'loading'): string {
  if (status === 'confirmed') return 'Подтверждена';
  if (status === 'cached') return 'В кэше';
  if (status === 'loading') return 'Генерируем';
  return 'Не считалась';
}

function sourceStructureTabLabel(tab: SourceStructureTab): string {
  if (tab === 'tables') return 'Таблицы';
  if (tab === 'fields') return 'Поля';
  if (tab === 'text') return 'Текст';
  return 'Предупреждения';
}

function previewSectionDisplayLabel(parsedFile: ParsedFileInfo | null, totalSections: number): string {
  if (shouldPreferTableSheetsInFormPreview(parsedFile)) {
    return totalSections > 1 ? 'Таблицы формы' : 'Таблица формы';
  }
  if (parsedFile?.documentMode === 'form_layout_mode') {
    return totalSections > 1 ? 'Извлечённые секции формы' : 'Извлечённая секция формы';
  }
  return previewSectionLabel(parsedFile?.extension, totalSections);
}

function sourceStructureTabDisplayLabel(tab: SourceStructureTab, parsedFile: ParsedFileInfo | null): string {
  if (tab === 'tables' && shouldPreferTableSheetsInFormPreview(parsedFile)) {
    return 'Таблицы';
  }
  if (tab === 'tables' && parsedFile?.documentMode === 'form_layout_mode') {
    return 'Форма';
  }
  return sourceStructureTabLabel(tab);
}

function draftSuggestionSourceLabel(suggestion: DraftFieldSuggestion): string {
  if (suggestion.sourceOfTruth === 'personal_memory') {
    return 'Персональная память';
  }
  if (suggestion.sourceOfTruth === 'global_pattern') {
    return 'Глобальный паттерн';
  }
  if (suggestion.sourceOfTruth === 'model_suggestion') {
    return 'Модель';
  }
  if (suggestion.sourceOfTruth === 'heuristic_fallback') {
    return 'Эвристика';
  }
  return 'Источник не указан';
}

function mappingStageState(mapping: MappingInfo, generationConfirmed: boolean): 'staging' | 'memory' | 'auto' | 'rejected' {
  if (mapping.status === 'rejected') {
    return 'rejected';
  }
  if (mapping.sourceOfTruth === 'personal_memory') {
    return 'memory';
  }
  if (mappingNeedsExplicitReview(mapping)) {
    return generationConfirmed && mapping.status === 'accepted' ? 'memory' : 'staging';
  }
  return 'auto';
}

function draftSuggestionStageState(suggestion: DraftFieldSuggestion, draftJsonSaved: boolean): 'staging' | 'memory' | 'auto' | 'rejected' {
  if (suggestion.status === 'rejected') {
    return 'rejected';
  }
  if (suggestion.sourceOfTruth === 'personal_memory') {
    return 'memory';
  }
  if (draftSuggestionNeedsExplicitReview(suggestion)) {
    return draftJsonSaved && suggestion.status === 'accepted' ? 'memory' : 'staging';
  }
  return 'auto';
}

function stageStateLabel(state: 'staging' | 'memory' | 'auto' | 'rejected'): string {
  if (state === 'memory') {
    return 'В памяти';
  }
  if (state === 'staging') {
    return 'Staging';
  }
  if (state === 'rejected') {
    return 'Отклонено';
  }
  return 'Авто';
}

function learningEventStageLabel(stage: LearningEvent['stage']): string {
  if (stage === 'memory') {
    return 'Память';
  }
  if (stage === 'global_pattern') {
    return 'Глобальный паттерн';
  }
  if (stage === 'dataset') {
    return 'Dataset';
  }
  return 'Staging';
}

function learningEventFilterLabel(filter: LearningEventFilter): string {
  if (filter === 'all') {
    return 'Все';
  }
  return learningEventStageLabel(filter);
}

const reviewItemCollator = new Intl.Collator('ru', {
  sensitivity: 'base',
  numeric: true,
});

function compareReviewLabels(left: string, right: string): number {
  const normalizedLeft = left.trim();
  const normalizedRight = right.trim();

  if (!normalizedLeft && !normalizedRight) {
    return 0;
  }
  if (!normalizedLeft) {
    return 1;
  }
  if (!normalizedRight) {
    return -1;
  }
  return reviewItemCollator.compare(normalizedLeft, normalizedRight);
}

function pluralizeRu(count: number, one: string, few: string, many: string): string {
  const normalized = Math.abs(count) % 100;
  const lastDigit = normalized % 10;
  if (normalized >= 11 && normalized <= 19) {
    return many;
  }
  if (lastDigit === 1) {
    return one;
  }
  if (lastDigit >= 2 && lastDigit <= 4) {
    return few;
  }
  return many;
}

function mappingReviewSortLabel(mapping: MappingInfo): string {
  if (mapping.source && mapping.source !== 'not found') {
    return mapping.source;
  }
  return mapping.target;
}

function draftSuggestionReviewSortLabel(suggestion: DraftFieldSuggestion): string {
  return suggestion.sourceColumn || suggestion.targetField;
}

function mappingDisplayPriority(mapping: MappingInfo): number {
  if (mappingNeedsExplicitReview(mapping) && (mapping.status ?? 'suggested') === 'suggested') {
    return 0;
  }
  if (mapping.status === 'accepted' || (!mappingNeedsExplicitReview(mapping) && mapping.status !== 'rejected')) {
    return 1;
  }
  if (mapping.status === 'rejected') {
    return 2;
  }
  return 3;
}

function draftSuggestionDisplayPriority(suggestion: DraftFieldSuggestion): number {
  if (draftSuggestionNeedsExplicitReview(suggestion) && (suggestion.status ?? 'suggested') === 'suggested') {
    return 0;
  }
  if (suggestion.status === 'accepted' || (!draftSuggestionNeedsExplicitReview(suggestion) && suggestion.status !== 'rejected')) {
    return 1;
  }
  if (suggestion.status === 'rejected') {
    return 2;
  }
  return 3;
}

function isObjectRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function learningEventActionLabel(event: LearningEvent): string | null {
  if (event.kind === 'feedback_session' || event.kind === 'few_shot_example') {
    return 'Открыть генерацию';
  }
  if (event.kind === 'user_template') {
    return 'Загрузить JSON';
  }
  if (event.kind === 'draft_memory') {
    return 'Загрузить draft JSON';
  }
  if (event.kind === 'dataset_item') {
    return 'Открыть связанный кейс';
  }
  if (event.kind === 'global_pattern') {
    return 'Показать паттерн';
  }
  return null;
}

function mappingNeedsExplicitReview(mapping: MappingInfo): boolean {
  if (mapping.status === 'rejected') {
    return true;
  }
  if (mapping.sourceOfTruth === 'model_suggestion' || mapping.sourceOfTruth === 'position_fallback' || mapping.sourceOfTruth === 'unresolved') {
    return true;
  }
  if ((mapping.status ?? 'accepted') !== 'suggested') {
    return false;
  }
  if (mapping.sourceOfTruth === 'deterministic_rule' || mapping.sourceOfTruth === 'personal_memory') {
    return false;
  }
  if (mapping.sourceOfTruth === 'global_pattern' || mapping.sourceOfTruth === 'semantic_graph') {
    return mapping.confidence === 'low' || mapping.confidence === 'none';
  }
  return true;
}

function draftSuggestionNeedsExplicitReview(suggestion: DraftFieldSuggestion): boolean {
  return Boolean(suggestion.suggestionId) || suggestion.status === 'suggested' || suggestion.sourceOfTruth === 'model_suggestion' || suggestion.sourceOfTruth === 'global_pattern';
}

function areMappingsEqual(left: MappingInfo, right: MappingInfo): boolean {
  return (
    left.source === right.source &&
    left.target === right.target &&
    left.confidence === right.confidence &&
    (left.reason ?? '') === (right.reason ?? '')
  );
}

export function Workspace({ profile, history, onLogout, onProfileUpdate, onSaveHistory }: Props) {
  const [schema, setSchema] = useState(defaultSchema);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [parsedFile, setParsedFile] = useState<ParsedFileInfo | null>(null);
  const [result, setResult] = useState<GenerationResult>({ code: defaultCode, mappings: [], preview: [], warnings: [] });
  const [busy, setBusy] = useState(false);
  const [activeHistoryId, setActiveHistoryId] = useState<string | null>(null);
  const [historyDeleteTarget, setHistoryDeleteTarget] = useState<HistoryItem | null>(null);
  const [historyDeleteBusyId, setHistoryDeleteBusyId] = useState<string | null>(null);
  const [historyClearDialogOpen, setHistoryClearDialogOpen] = useState(false);
  const [historyDeleteError, setHistoryDeleteError] = useState('');
  const [saveMessage, setSaveMessage] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [copied, setCopied] = useState(false);
  const [activePreviewSheet, setActivePreviewSheet] = useState<string | null>(null);
  const [sourceStructureTab, setSourceStructureTab] = useState<SourceStructureTab>('tables');
  const [activeView, setActiveView] = useState<'generator' | 'profile'>('generator');
  const [displayName, setDisplayName] = useState(profile.name);
  const [profileSaveBusy, setProfileSaveBusy] = useState(false);
  const [profileSaveNotice, setProfileSaveNotice] = useState('');
  const [profileSaveError, setProfileSaveError] = useState('');
  const [accountModalOpen, setAccountModalOpen] = useState(false);
  const [accountModalMode, setAccountModalMode] = useState<'email' | 'password'>('email');
  const [newEmail, setNewEmail] = useState('');
  const [emailPassword, setEmailPassword] = useState('');
  const [emailChangeCode, setEmailChangeCode] = useState('');
  const [emailChangeBusy, setEmailChangeBusy] = useState(false);
  const [emailCodeBusy, setEmailCodeBusy] = useState(false);
  const [emailChangeNotice, setEmailChangeNotice] = useState('');
  const [emailChangeError, setEmailChangeError] = useState('');
  const [emailCodeRequested, setEmailCodeRequested] = useState(false);
  const [currentPassword, setCurrentPassword] = useState('');
  const [nextPassword, setNextPassword] = useState('');
  const [repeatPassword, setRepeatPassword] = useState('');
  const [passwordChangeBusy, setPasswordChangeBusy] = useState(false);
  const [passwordChangeNotice, setPasswordChangeNotice] = useState('');
  const [passwordChangeError, setPasswordChangeError] = useState('');
  const [learningEvents, setLearningEvents] = useState<LearningEvent[]>([]);
  const [learningMemory, setLearningMemory] = useState<LearningMemory | null>(null);
  const [learningEventsError, setLearningEventsError] = useState('');
  const [learningEventFilter, setLearningEventFilter] = useState<LearningEventFilter>('all');
  const [learningEventSearch, setLearningEventSearch] = useState('');
  const [correctionBaseline, setCorrectionBaseline] = useState<CorrectionBaseline | null>(null);
  const [correctionSaveBusy, setCorrectionSaveBusy] = useState(false);
  const [correctionSaveNotice, setCorrectionSaveNotice] = useState('');
  const [correctionSaveError, setCorrectionSaveError] = useState('');
  const [draftSuggestions, setDraftSuggestions] = useState<DraftFieldSuggestion[]>([]);
  const [draftJsonBusy, setDraftJsonBusy] = useState(false);
  const [draftJsonNotice, setDraftJsonNotice] = useState('');
  const [draftJsonError, setDraftJsonError] = useState('');
  const [draftJsonSaved, setDraftJsonSaved] = useState(false);
  const [uploadedTargetJson, setUploadedTargetJson] = useState<UploadedTargetJson | null>(null);
  const [targetJsonPrepared, setTargetJsonPrepared] = useState(false);
  const [targetJsonUploadNotice, setTargetJsonUploadNotice] = useState('');
  const [targetJsonUploadError, setTargetJsonUploadError] = useState('');
  const [learningReviewBusy, setLearningReviewBusy] = useState(false);
  const [learningReviewNotice, setLearningReviewNotice] = useState('');
  const [learningReviewError, setLearningReviewError] = useState('');
  const [generationConfirmed, setGenerationConfirmed] = useState(false);
  const [mappingReviewNotes, setMappingReviewNotes] = useState<Record<string, string>>({});
  const [draftReviewNotes, setDraftReviewNotes] = useState<Record<string, string>>({});
  const [repairBusy, setRepairBusy] = useState(false);
  const [repairNotice, setRepairNotice] = useState('');
  const [repairError, setRepairError] = useState('');
  const [sourcePreviewRefreshBusy, setSourcePreviewRefreshBusy] = useState(false);
  const [sourcePreviewRefreshNotice, setSourcePreviewRefreshNotice] = useState('');
  const [sourcePreviewRefreshError, setSourcePreviewRefreshError] = useState('');
  const [sourcePreviewRefreshDone, setSourcePreviewRefreshDone] = useState(false);
  const [activeRepairActionKey, setActiveRepairActionKey] = useState<string | null>(null);
  const [repairPreview, setRepairPreview] = useState<RepairPreviewResult | null>(null);
  const [sectionStateCache, setSectionStateCache] = useState<Record<string, SectionWorkspaceState>>({});
  const [autoGenerateSectionKey, setAutoGenerateSectionKey] = useState<string | null>(null);
  const [reviewFocusTarget, setReviewFocusTarget] = useState<string | null>(null);
  const [profileAccountCardHeight, setProfileAccountCardHeight] = useState<number | null>(null);
  const [backendHealth, setBackendHealth] = useState<BackendHealth | null>(null);
  const reviewFocusTimerRef = useRef<number | null>(null);
  const sourcePreviewRefreshTimerRef = useRef<number | null>(null);
  const previewGridWrapRef = useRef<HTMLDivElement | null>(null);
  const profileAccountCardRef = useRef<HTMLElement | null>(null);
  const hasGeneratedResult = result.code !== defaultCode;
  const currentFormExplainability = result.formExplainability ?? null;
  const currentRepairPlan = currentFormExplainability?.repairPlan ?? null;
  const currentRepairActions = currentRepairPlan?.actions ?? [];
  const currentFormQuality = currentFormExplainability?.qualitySummary ?? null;
  const currentFormRedFlags = currentFormQuality?.redFlags ?? [];
  const currentPdfZoneSummary = currentFormExplainability?.pdfZoneSummary ?? null;
  const currentOcrZoneSummary = currentFormExplainability?.ocrZoneSummary ?? null;
  const visibleTsDiagnostics = (result.tsDiagnostics ?? []).filter((diagnostic) => Boolean(diagnostic?.message));
  const visiblePreviewDiagnostics = (result.previewDiagnostics ?? []).filter((diagnostic) => Boolean(diagnostic?.message));

  const refreshLearningData = async () => {
    if (profile.skipped) {
      setLearningEvents([]);
      setLearningMemory(null);
      setLearningEventsError('');
      return;
    }

    try {
      const [nextEvents, nextMemory] = await Promise.all([
        fetchLearningEvents(profile.id, 18),
        fetchLearningMemory(profile.id, 14),
      ]);
      setLearningEvents(nextEvents);
      setLearningMemory(nextMemory);
      setLearningEventsError('');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось загрузить данные обучения.';
      setLearningEventsError(message);
    }
  };

  useEffect(() => {
    if ((!historyDeleteTarget && !historyClearDialogOpen) || historyDeleteBusyId !== null) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setHistoryDeleteError('');
        setHistoryDeleteTarget(null);
        setHistoryClearDialogOpen(false);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [historyClearDialogOpen, historyDeleteBusyId, historyDeleteTarget]);

  useEffect(() => {
    void fetchBackendHealth()
      .then((health) => setBackendHealth(health))
      .catch(() => setBackendHealth(null));
  }, []);

  useEffect(() => {
    return () => {
      if (sourcePreviewRefreshTimerRef.current !== null) {
        window.clearTimeout(sourcePreviewRefreshTimerRef.current);
      }
    };
  }, []);

  const previewSheets = useMemo(() => buildPreviewSheetsForParsedFile(parsedFile), [parsedFile]);

  const currentPreviewSheet = useMemo(() => {
    if (previewSheets.length === 0) {
      return null;
    }

    return previewSheets.find((sheet) => sheet.name === activePreviewSheet) ?? previewSheets[0];
  }, [activePreviewSheet, previewSheets]);

  const currentPreviewIndex = useMemo(() => {
    if (!currentPreviewSheet) {
      return -1;
    }
    return previewSheets.findIndex((sheet) => sheet.name === currentPreviewSheet.name);
  }, [currentPreviewSheet, previewSheets]);

  useEffect(() => {
    if (sourceStructureTab !== 'tables') {
      return;
    }
    previewGridWrapRef.current?.scrollTo({ left: 0, top: 0, behavior: 'auto' });
  }, [currentPreviewSheet?.name, parsedFile?.fileName, sourceStructureTab]);

  const extractedFieldEntries = useMemo(() => getExtractedFieldEntries(parsedFile), [parsedFile]);

  const availableSourceStructureTabs = useMemo(() => {
    if (!parsedFile) {
      return [];
    }
    const tabs: SourceStructureTab[] = [];
    if (previewSheets.length > 0) {
      tabs.push('tables');
    }
    if (extractedFieldEntries.length > 0) {
      tabs.push('fields');
    }
    if ((parsedFile?.rawText ?? '').trim().length > 0 || (parsedFile?.textBlocks.length ?? 0) > 0 || (parsedFile?.sections.length ?? 0) > 0) {
      tabs.push('text');
    }
    if ((parsedFile?.warnings.length ?? 0) > 0 || !!parsedFile?.extractionStatus) {
      tabs.push('warnings');
    }
    return tabs;
  }, [extractedFieldEntries.length, parsedFile?.extractionStatus, parsedFile?.rawText, parsedFile?.sections.length, parsedFile?.textBlocks.length, parsedFile?.warnings.length, previewSheets.length]);

  const normalizedSourceStructureTabs = useMemo<SourceStructureTab[]>(() => {
    if (Array.isArray(availableSourceStructureTabs)) {
      return availableSourceStructureTabs;
    }

    const fallbackTabs: SourceStructureTab[] = [];
    if (previewSheets.length > 0) {
      fallbackTabs.push('tables');
    }
    if (extractedFieldEntries.length > 0) {
      fallbackTabs.push('fields');
    }
    if ((parsedFile?.rawText ?? '').trim().length > 0 || (parsedFile?.textBlocks.length ?? 0) > 0 || (parsedFile?.sections.length ?? 0) > 0) {
      fallbackTabs.push('text');
    }
    if ((parsedFile?.warnings.length ?? 0) > 0 || !!parsedFile?.extractionStatus) {
      fallbackTabs.push('warnings');
    }
    return fallbackTabs;
  }, [
    availableSourceStructureTabs,
    extractedFieldEntries.length,
    parsedFile?.extractionStatus,
    parsedFile?.rawText,
    parsedFile?.sections.length,
    parsedFile?.textBlocks.length,
    parsedFile?.warnings.length,
    previewSheets.length,
  ]);

  const currentSectionKey = useMemo(
    () => buildSectionCacheKey(parsedFile?.fileName, currentPreviewSheet?.name ?? activePreviewSheet ?? 'default'),
    [activePreviewSheet, currentPreviewSheet?.name, parsedFile?.fileName]
  );

  const fileSummary = useMemo(() => {
    if (!parsedFile) {
      return 'Файл еще не загружен';
    }

    if (parsedFile.contentType === 'image_like') {
      return `${parsedFile.fileName} · ${parsedContentTypeLabel(parsedFile.contentType)} · ${parsedExtractionStatusLabel(parsedFile.extractionStatus)}${
        parsedFile.ocrUsed ? ' · OCR used' : ''
      }`;
    }

    if (previewSheets.length > 0) {
      return `${parsedFile.fileName} · ${parsedContentTypeLabel(parsedFile.contentType)} · ${previewSheets.length} preview section(s)`;
    }

    if (extractedFieldEntries.length > 0) {
      return `${parsedFile.fileName} · ${parsedContentTypeLabel(parsedFile.contentType)} · ${extractedFieldEntries.length} field(s) extracted`;
    }

    if ((parsedFile.rawText ?? '').trim().length > 0) {
      return `${parsedFile.fileName} · ${parsedContentTypeLabel(parsedFile.contentType)} · text extracted`;
    }

    return `${parsedFile.fileName} · ${parsedContentTypeLabel(parsedFile.contentType)} · ${parsedExtractionStatusLabel(parsedFile.extractionStatus)}`;
  }, [extractedFieldEntries.length, parsedFile, previewSheets.length]);

  const targetJsonFileSummary = useMemo(() => {
    if (!uploadedTargetJson) {
      return 'JSON-файл еще не загружен';
    }
    return `${uploadedTargetJson.fileName} · загружен в Target JSON`;
  }, [uploadedTargetJson]);

  const schemaTargetFields = useMemo(() => parseSchemaFields(schema), [schema]);

  const mappingTargetOptions = useMemo(() => {
    const options = schemaTargetFields.length > 0 ? schemaTargetFields : result.mappings.map((mapping) => mapping.target);
    return Array.from(new Set(options.filter(Boolean)));
  }, [result.mappings, schemaTargetFields]);

  const mappingSourceOptions = useMemo(() => {
    const sourceColumns = collectParsedSourceColumns(parsedFile, currentPreviewSheet);
    const currentSources = result.mappings.map((mapping) => mapping.source).filter((value) => value && value !== 'not found');
    return Array.from(new Set([...sourceColumns, ...currentSources])) as string[];
  }, [currentPreviewSheet, parsedFile, result.mappings]);

  const hasReviewableMappings = useMemo(() => {
    return result.mappings.some((mapping) => mappingNeedsExplicitReview(mapping));
  }, [result.mappings]);

  const hasPendingMappingReview = useMemo(() => {
    return result.mappings.some((mapping) => mappingNeedsExplicitReview(mapping) && (mapping.status ?? 'suggested') === 'suggested');
  }, [result.mappings]);

  const hasPendingDraftReview = useMemo(() => {
    return draftSuggestions.some((suggestion) => draftSuggestionNeedsExplicitReview(suggestion) && (suggestion.status ?? 'suggested') === 'suggested');
  }, [draftSuggestions]);

  const hasRejectedMappingWithoutReason = useMemo(() => {
    return result.mappings.some((mapping, index) => mappingNeedsExplicitReview(mapping) && mapping.status === 'rejected' && !mappingReviewNotes[`mapping-${index}`]?.trim());
  }, [mappingReviewNotes, result.mappings]);

  const hasRejectedDraftWithoutReason = useMemo(() => {
    return draftSuggestions.some((suggestion, index) => draftSuggestionNeedsExplicitReview(suggestion) && suggestion.status === 'rejected' && !draftReviewNotes[`draft-${index}`]?.trim());
  }, [draftReviewNotes, draftSuggestions]);

  const currentGenerationTokenUsage = result.tokenUsage ?? null;

  const sortedMappingRows = useMemo(() => {
    return result.mappings
      .map((mapping, index) => ({ mapping, index }))
      .sort((left, right) => {
        const priorityDelta = mappingDisplayPriority(left.mapping) - mappingDisplayPriority(right.mapping);
        if (priorityDelta !== 0) {
          return priorityDelta;
        }

        const alphabeticalOrder = compareReviewLabels(mappingReviewSortLabel(left.mapping), mappingReviewSortLabel(right.mapping));
        if (alphabeticalOrder !== 0) {
          return alphabeticalOrder;
        }

        return left.index - right.index;
      });
  }, [result.mappings]);

  const sortedDraftSuggestionRows = useMemo(() => {
    return draftSuggestions
      .map((suggestion, index) => ({ suggestion, index }))
      .sort((left, right) => {
        const priorityDelta = draftSuggestionDisplayPriority(left.suggestion) - draftSuggestionDisplayPriority(right.suggestion);
        if (priorityDelta !== 0) {
          return priorityDelta;
        }

        const alphabeticalOrder = compareReviewLabels(draftSuggestionReviewSortLabel(left.suggestion), draftSuggestionReviewSortLabel(right.suggestion));
        if (alphabeticalOrder !== 0) {
          return alphabeticalOrder;
        }

        return left.index - right.index;
      });
  }, [draftSuggestions]);

  const pendingMappingReviewItems = useMemo(() => {
    return result.mappings.flatMap((mapping, index) =>
      mappingNeedsExplicitReview(mapping) && (mapping.status ?? 'suggested') === 'suggested'
        ? [
            {
              id: `mapping-${index}-${mapping.target}`,
              scrollTargetId: `mapping-review-${index}`,
              title: mapping.target,
              sortLabel: mappingReviewSortLabel(mapping),
              description: mapping.source && mapping.source !== 'not found' ? `source: ${mapping.source}` : 'source пока не определён',
            },
          ]
        : []
    );
  }, [result.mappings]);

  const pendingDraftReviewItems = useMemo(() => {
    return draftSuggestions.flatMap((suggestion, index) =>
      draftSuggestionNeedsExplicitReview(suggestion) && (suggestion.status ?? 'suggested') === 'suggested'
        ? [
            {
              id: `draft-${index}-${suggestion.sourceColumn}`,
              scrollTargetId: `draft-review-${index}`,
              title: suggestion.targetField || suggestion.sourceColumn,
              sortLabel: draftSuggestionReviewSortLabel(suggestion),
              description: `колонка: ${suggestion.sourceColumn}`,
            },
          ]
        : []
    );
  }, [draftSuggestions]);

  const sortedPendingMappingReviewItems = useMemo(() => {
    return [...pendingMappingReviewItems].sort((left, right) => {
      const alphabeticalOrder = compareReviewLabels(left.sortLabel, right.sortLabel);
      if (alphabeticalOrder !== 0) {
        return alphabeticalOrder;
      }
      return compareReviewLabels(left.title, right.title);
    });
  }, [pendingMappingReviewItems]);

  const sortedPendingDraftReviewItems = useMemo(() => {
    return [...pendingDraftReviewItems].sort((left, right) => {
      const alphabeticalOrder = compareReviewLabels(left.sortLabel, right.sortLabel);
      if (alphabeticalOrder !== 0) {
        return alphabeticalOrder;
      }
      return compareReviewLabels(left.title, right.title);
    });
  }, [pendingDraftReviewItems]);

  const pendingReviewTotal = pendingMappingReviewItems.length + pendingDraftReviewItems.length;

  const learningEventCounts = useMemo(() => {
    return learningEvents.reduce(
      (accumulator, event) => {
        accumulator.all += 1;
        accumulator[event.stage] += 1;
        return accumulator;
      },
      {
        all: 0,
        staging: 0,
        memory: 0,
        global_pattern: 0,
        dataset: 0,
      } as Record<LearningEventFilter, number>
    );
  }, [learningEvents]);

  const filteredLearningEvents = useMemo(() => {
    const normalizedQuery = learningEventSearch.trim().toLowerCase();
    return learningEvents.filter((event) => {
      const matchesFilter = learningEventFilter === 'all' || event.stage === learningEventFilter;
      if (!matchesFilter) {
        return false;
      }
      if (!normalizedQuery) {
        return true;
      }
      const haystack = `${event.title} ${event.description}`.toLowerCase();
      return haystack.includes(normalizedQuery);
    });
  }, [learningEventFilter, learningEventSearch, learningEvents]);

  useEffect(() => {
    return () => {
      if (reviewFocusTimerRef.current !== null) {
        window.clearTimeout(reviewFocusTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (activeView !== 'profile') {
      setProfileAccountCardHeight(null);
      return;
    }

    const accountCard = profileAccountCardRef.current;
    if (!accountCard) {
      return;
    }

    const syncHeight = () => {
      setProfileAccountCardHeight(Math.ceil(accountCard.getBoundingClientRect().height));
    };

    syncHeight();

    if (typeof ResizeObserver === 'undefined') {
      return;
    }

    const observer = new ResizeObserver(() => {
      syncHeight();
    });
    observer.observe(accountCard);
    return () => observer.disconnect();
  }, [activeView]);

  const pendingCorrections = useMemo(() => {
    if (profile.skipped || !correctionBaseline) {
      return [] as ManualCorrectionInput[];
    }

    const nextCorrections: ManualCorrectionInput[] = [];
    if (schema !== correctionBaseline.schema) {
      nextCorrections.push({
        correctionType: 'target_schema_edit',
        fieldPath: '$',
        originalValue: parseMaybeJson(correctionBaseline.schema),
        correctedValue: parseMaybeJson(schema),
        correctionPayload: {
          originalText: correctionBaseline.schema,
          correctedText: schema,
        },
        rationale: 'Schema edited in the frontend workspace.',
        accepted: true,
      });
    }

    const maxLength = Math.max(correctionBaseline.mappings.length, result.mappings.length);
    for (let index = 0; index < maxLength; index += 1) {
      const previousMapping = correctionBaseline.mappings[index];
      const currentMapping = result.mappings[index];
      if (!currentMapping || !previousMapping) {
        continue;
      }
      if (areMappingsEqual(previousMapping, currentMapping)) {
        continue;
      }

      nextCorrections.push({
        correctionType: 'mapping_override',
        sourceField: currentMapping.source === 'not found' ? null : currentMapping.source,
        targetField: currentMapping.target,
        originalValue: previousMapping,
        correctedValue: currentMapping,
        correctionPayload: {
          mappingIndex: index,
          previousTarget: previousMapping.target,
          nextTarget: currentMapping.target,
          previousReason: previousMapping.reason ?? null,
          nextReason: currentMapping.reason ?? null,
        },
        rationale: 'Mapping adjusted manually in the frontend workspace.',
        confidenceBefore: confidenceToScore(previousMapping.confidence),
        confidenceAfter: confidenceToScore(currentMapping.confidence),
        accepted: true,
      });
    }

    if (result.code !== correctionBaseline.code) {
      nextCorrections.push({
        correctionType: 'code_edit',
        fieldPath: 'generated_typescript',
        originalValue: correctionBaseline.code,
        correctedValue: result.code,
        rationale: 'Generated TypeScript edited in the frontend workspace.',
        accepted: true,
      });
    }

    return nextCorrections;
  }, [correctionBaseline, profile.skipped, result.code, result.mappings, schema]);

  const visibleWarnings = useMemo(() => {
    return Array.from(
      new Set(
        [
          ...result.warnings,
          ...(parsedFile?.warnings ?? []),
          saveMessage,
          correctionSaveNotice,
          correctionSaveError,
          draftJsonError,
          learningReviewError,
          draftJsonNotice,
          targetJsonUploadError,
          targetJsonUploadNotice,
          learningReviewNotice,
          sourcePreviewRefreshNotice,
          sourcePreviewRefreshError,
          repairNotice,
          repairError,
        ].filter(Boolean)
      )
    );
  }, [correctionSaveError, correctionSaveNotice, draftJsonError, draftJsonNotice, learningReviewError, learningReviewNotice, parsedFile?.warnings, repairError, repairNotice, result.warnings, saveMessage, sourcePreviewRefreshError, sourcePreviewRefreshNotice, targetJsonUploadError, targetJsonUploadNotice]);

  const clearUploadedTargetJson = () => {
    setUploadedTargetJson(null);
    setTargetJsonUploadNotice('');
    setTargetJsonUploadError('');
  };

  const invalidateGeneratedResult = () => {
    setResult({ code: defaultCode, mappings: [], preview: [], warnings: [] });
    setActiveHistoryId(null);
    setCorrectionBaseline(null);
    setCorrectionSaveNotice('');
    setCorrectionSaveError('');
    setMappingReviewNotes({});
    setGenerationConfirmed(false);
    setLearningReviewNotice('');
    setLearningReviewError('');
    setRepairNotice('');
    setRepairError('');
    setActiveRepairActionKey(null);
    setRepairPreview(null);
    setAutoGenerateSectionKey(null);
  };

  const onClearTargetJsonFile = () => {
    if (!uploadedTargetJson) {
      return;
    }
    invalidateGeneratedResult();
    clearUploadedTargetJson();
    setTargetJsonPrepared(false);
    setDraftSuggestions([]);
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setDraftJsonSaved(false);
    setSchema(defaultSchema);
    setSaveMessage('Загруженный JSON удален из Target JSON.');
  };

  const profileStats = useMemo(() => {
    const totalGenerations = history.length;
    const uniqueFiles = new Set(history.map((item) => item.fileName)).size;
    const totalWarnings = history.reduce((sum, item) => sum + item.warnings.length, 0);
    const gigachatApiTokens = history.reduce((sum, item) => sum + (item.tokenUsage?.totalTokens ?? 0), 0);
    const lastGeneratedAt = history[0]?.createdAt ?? null;
    return {
      totalGenerations,
      uniqueFiles,
      totalWarnings,
      gigachatApiTokens,
      lastGeneratedAt,
    };
  }, [history]);

  const semanticGraphHighlights = useMemo(() => learningMemory?.layers.semanticGraph.items ?? [], [learningMemory]);
  const semanticGraphClusters = useMemo(() => learningMemory?.layers.semanticGraph.clusters ?? [], [learningMemory]);
  const personalMemoryHighlights = useMemo(() => learningMemory?.layers.personalMemory.items ?? [], [learningMemory]);
  const globalKnowledgeHighlights = useMemo(() => learningMemory?.layers.globalKnowledge.items ?? [], [learningMemory]);
  const systemMemoryFeed = useMemo<InsightFeedEntry[]>(() => {
    const personalItems = personalMemoryHighlights.slice(0, 6).map((item) => ({
      id: `personal-${item.sourceFieldNorm}-${item.targetFieldNorm}`,
      title: `${item.sourceField ?? item.sourceFieldNorm} → ${item.targetField}`,
      badge: item.confidenceBand,
      badgeClassName: 'memory-band-chip',
      categoryLabel: 'Личная память',
      categoryClassName: 'knowledge-inline-chip knowledge-inline-chip-personal',
      description: `usage ${item.usageCount} · accept ${item.acceptedCount} · reject ${item.rejectedCount} · schemas ${item.schemaFingerprintCount}`,
      tags: [],
      note: null,
      timestamp: null,
    }));

    const globalItems = globalKnowledgeHighlights.slice(0, 6).map((item) => ({
      id: `global-${item.candidateId}-${item.targetFieldNorm}`,
      title: `${item.sourceField ?? item.sourceFieldNorm} → ${item.targetField ?? item.targetFieldNorm}`,
      badge: memoryPatternStatusLabel(item.status),
      badgeClassName: `memory-status-chip memory-status-${memoryPatternStatusTone(item.status)}`,
      categoryLabel: 'Общий паттерн',
      categoryClassName: 'knowledge-inline-chip knowledge-inline-chip-accent',
      description: `support ${item.supportCount} · users ${item.uniqueUsers} · accept ${formatMetricScore(item.acceptanceRate)} · drift ${formatMetricScore(item.driftScore)}`,
      tags: compactStrings([item.semanticRole, item.conceptCluster, ...item.domainTags.slice(0, 2)]),
      note: item.promotionReason ? `promotion: ${item.promotionReason}` : item.rejectionReason ? `reason: ${item.rejectionReason}` : null,
      timestamp: null,
    }));

    return [...personalItems, ...globalItems];
  }, [globalKnowledgeHighlights, personalMemoryHighlights]);
  const semanticGraphFeed = useMemo<InsightFeedEntry[]>(() => {
    const clusterItems = semanticGraphClusters.slice(0, 4).map((cluster) => ({
      id: `cluster-${cluster.clusterId}`,
      title: `Кластер из ${cluster.size} полей`,
      badge: 'Кластер',
      badgeClassName: 'knowledge-inline-chip knowledge-inline-chip-accent',
      categoryLabel: 'Семантическая группа',
      categoryClassName: 'knowledge-inline-chip knowledge-inline-chip-muted',
      description: `support ${cluster.supportCount} · fields ${cluster.fields.length}`,
      tags: compactStrings([
        ...cluster.sharedAttributes.slice(0, 2),
        ...cluster.sharedRoles.slice(0, 2).map((role) => semanticGraphRoleLabel(role)),
        ...cluster.entities.slice(0, 2),
      ]),
      note: cluster.fields
        .slice(0, 3)
        .map((field) => `${field.field} (${field.entityToken ?? 'any'}.${field.attributeToken ?? 'field'})`)
        .join(' · '),
      timestamp: null,
    }));

    const edgeItems = semanticGraphHighlights.slice(0, 8).map((edge) => ({
      id: `edge-${edge.leftFieldNorm}-${edge.rightFieldNorm}-${edge.relationKind}`,
      title: `${edge.leftField} ↔ ${edge.rightField}`,
      badge: semanticGraphRelationLabel(edge.relationKind),
      badgeClassName: `semantic-graph-kind semantic-graph-kind-${edge.relationKind}`,
      categoryLabel: 'Semantic edge',
      categoryClassName: 'knowledge-inline-chip knowledge-inline-chip-personal',
      description: `${edge.leftEntityToken ?? 'any'}.${edge.leftAttributeToken ?? 'field'} · ${semanticGraphRoleLabel(edge.leftRoleLabel)} ↔ ${edge.rightEntityToken ?? 'any'}.${edge.rightAttributeToken ?? 'field'} · ${semanticGraphRoleLabel(edge.rightRoleLabel)}`,
      tags: [`support ${edge.supportCount}`, `accepted ${edge.acceptedCount}`, `rejected ${edge.rejectedCount}`, edge.confidenceBand],
      note: null,
      timestamp: edge.lastSeenAt ? new Date(edge.lastSeenAt).toLocaleString() : null,
    }));

    return [...clusterItems, ...edgeItems];
  }, [semanticGraphClusters, semanticGraphHighlights]);

  const restoreHistoryItem = (item: HistoryItem) => {
    setHistoryDeleteError('');
    setActiveHistoryId(item.id);
    setSelectedFile(null);
    setParsedFile(item.parsedFile ?? null);
    clearUploadedTargetJson();
    setTargetJsonPrepared(true);
    setSchema(item.schema);
    setAutoGenerateSectionKey(null);
    const restoredResult: GenerationResult = {
      generationId: item.id,
      code: item.code,
      mappings: item.mappings,
      preview: item.preview,
      warnings: item.warnings,
      parsedFile: item.parsedFile ?? null,
      formExplainability: item.formExplainability ?? null,
      targetSchema: item.validation?.targetSchema ?? null,
      requiredFields: item.validation?.targetSchemaSummary?.requiredFields ?? [],
      tsValid: item.validation?.tsValidation?.valid ?? false,
      tsDiagnostics: item.validation?.tsValidation?.diagnostics ?? [],
      previewDiagnostics: item.validation?.previewValidation?.diagnostics ?? [],
      mappingOperationalStatus: item.validation?.qualitySummary?.operationalMappingStatus ?? null,
      mappingEvalMetrics: item.validation?.qualitySummary?.trueQualityMetrics ?? null,
      sourceQualityAdjustment: item.validation?.qualitySummary?.sourceQualityAdjustment ?? null,
      tsSyntaxValid: item.validation?.qualitySummary?.tsSyntaxValid ?? item.validation?.tsValidation?.valid ?? false,
      tsRuntimePreviewValid: item.validation?.qualitySummary?.tsRuntimePreviewValid ?? item.validation?.previewValidation?.runtimeValid ?? false,
      outputSchemaValid: item.validation?.qualitySummary?.outputSchemaValid ?? item.validation?.previewValidation?.schemaValid ?? false,
    };
    setResult(restoredResult);
    setActivePreviewSheet(item.selectedSheet ?? item.parsedFile?.sheets[0]?.name ?? null);
    setSourceStructureTab(preferredSourceStructureTab(item.parsedFile ?? null));
    setSectionStateCache({});
    setCorrectionBaseline(profile.skipped ? null : buildCorrectionBaseline(item.id, item.schema, restoredResult));
    setCorrectionSaveNotice('');
    setCorrectionSaveError('');
    setDraftSuggestions([]);
    setMappingReviewNotes({});
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setSourcePreviewRefreshNotice('');
    setSourcePreviewRefreshError('');
    setDraftJsonSaved(false);
    setRepairNotice('');
    setRepairError('');
    setSourcePreviewRefreshNotice('');
    setSourcePreviewRefreshError('');
    setSourcePreviewRefreshDone(false);
    setActiveRepairActionKey(null);
    setRepairPreview(null);
    setLearningReviewNotice('');
    setLearningReviewError('');
    setGenerationConfirmed(true);
    setActiveView('generator');
  };

  const openHistoryDeleteModal = (item: HistoryItem) => {
    if (profile.skipped || historyDeleteBusyId !== null) {
      return;
    }
    setHistoryDeleteError('');
    setHistoryClearDialogOpen(false);
    setHistoryDeleteTarget(item);
  };

  const closeHistoryDeleteModal = () => {
    if (historyDeleteBusyId !== null) {
      return;
    }
    setHistoryDeleteError('');
    setHistoryDeleteTarget(null);
  };

  const openClearHistoryModal = () => {
    if (profile.skipped || historyDeleteBusyId !== null || history.length === 0) {
      return;
    }
    setHistoryDeleteError('');
    setHistoryDeleteTarget(null);
    setHistoryClearDialogOpen(true);
  };

  const closeClearHistoryModal = () => {
    if (historyDeleteBusyId !== null) {
      return;
    }
    setHistoryDeleteError('');
    setHistoryClearDialogOpen(false);
  };

  const onDeleteHistoryItem = async () => {
    if (profile.skipped || historyDeleteBusyId !== null || !historyDeleteTarget) {
      return;
    }

    const item = historyDeleteTarget;

    setHistoryDeleteBusyId(item.id);
    setHistoryDeleteError('');

    try {
      await deleteHistoryEntry(profile.id, item.id);

      setSectionStateCache((current) => {
        let changed = false;
        const nextEntries = Object.entries(current).map(([sectionKey, state]) => {
          const nextState = detachGenerationLinkFromSectionState(state, item.id);
          if (nextState !== state) {
            changed = true;
          }
          return [sectionKey, nextState] as const;
        });
        return changed ? Object.fromEntries(nextEntries) : current;
      });
      setResult((current) => (current.generationId === item.id ? { ...current, generationId: null } : current));
      setCorrectionBaseline((current) => (current?.generationId === item.id ? null : current));
      if (result.generationId === item.id || correctionBaseline?.generationId === item.id) {
        setGenerationConfirmed(false);
      }
      if (activeHistoryId === item.id) {
        setActiveHistoryId(null);
      }

      setHistoryDeleteTarget(null);
      await onSaveHistory();
      setSaveMessage(`Запрос "${item.fileName}" удален из истории.`);
    } catch (error) {
      setHistoryDeleteError(error instanceof Error ? error.message : 'Не удалось удалить запрос из истории.');
    } finally {
      setHistoryDeleteBusyId(null);
    }
  };

  const onClearHistory = async () => {
    if (profile.skipped || historyDeleteBusyId !== null || history.length === 0) {
      return;
    }

    const historyIds = new Set(history.map((item) => item.id));
    setHistoryDeleteBusyId(CLEAR_HISTORY_BUSY_ID);
    setHistoryDeleteError('');
    setHistoryDeleteTarget(null);

    try {
      for (const item of history) {
        await deleteHistoryEntry(profile.id, item.id);
      }

      setSectionStateCache((current) => {
        let changed = false;
        const nextEntries = Object.entries(current).map(([sectionKey, state]) => {
          let nextState = state;
          for (const item of history) {
            const detached = detachGenerationLinkFromSectionState(nextState, item.id);
            if (detached !== nextState) {
              nextState = detached;
              changed = true;
            }
          }
          return [sectionKey, nextState] as const;
        });
        return changed ? Object.fromEntries(nextEntries) : current;
      });
      setResult((current) =>
        current.generationId !== null && current.generationId !== undefined && historyIds.has(String(current.generationId))
          ? { ...current, generationId: null }
          : current
      );
      setCorrectionBaseline((current) => (current && historyIds.has(current.generationId) ? null : current));
      if (
        (result.generationId !== null && result.generationId !== undefined && historyIds.has(String(result.generationId))) ||
        (correctionBaseline && historyIds.has(correctionBaseline.generationId))
      ) {
        setGenerationConfirmed(false);
      }
      if (activeHistoryId && historyIds.has(activeHistoryId)) {
        setActiveHistoryId(null);
      }

      setHistoryClearDialogOpen(false);
      await onSaveHistory();
      setSaveMessage('История генераций очищена.');
    } catch (error) {
      setHistoryDeleteError(error instanceof Error ? error.message : 'Не удалось очистить историю генераций.');
    } finally {
      setHistoryDeleteBusyId(null);
    }
  };

  const findHistoryItemByGenerationId = (generationId: unknown): HistoryItem | null => {
    if (typeof generationId !== 'string' && typeof generationId !== 'number') {
      return null;
    }
    return history.find((item) => item.id === String(generationId)) ?? null;
  };

  const isLearningEventActionable = (event: LearningEvent): boolean => {
    const metadata = event.metadata ?? {};
    if (event.kind === 'feedback_session' || event.kind === 'few_shot_example') {
      return Boolean(findHistoryItemByGenerationId(metadata.generation_id ?? metadata.source_generation_id));
    }
    if (event.kind === 'user_template' || event.kind === 'draft_memory') {
      return isObjectRecord(metadata.target_json);
    }
    if (event.kind === 'dataset_item') {
      return (
        Boolean(findHistoryItemByGenerationId(metadata.source_generation_id)) ||
        (isObjectRecord(metadata.target_payload) &&
          (isObjectRecord(metadata.target_payload.target_json) || Object.keys(metadata.target_payload).length > 0))
      );
    }
    if (event.kind === 'global_pattern') {
      return typeof metadata.target_field === 'string' && metadata.target_field.length > 0;
    }
    return false;
  };

  const loadSchemaFromEvent = (payload: unknown, message: string) => {
    if (!isObjectRecord(payload)) {
      return false;
    }
    invalidateGeneratedResult();
    clearUploadedTargetJson();
    setTargetJsonPrepared(true);
    setSchema(JSON.stringify(payload, null, 2));
    setActiveView('generator');
    setSaveMessage(message);
    return true;
  };

  const onOpenLearningEvent = (event: LearningEvent) => {
    const metadata = event.metadata ?? {};

    const linkedHistoryItem = findHistoryItemByGenerationId(metadata.generation_id ?? metadata.source_generation_id);
    if (linkedHistoryItem) {
      restoreHistoryItem(linkedHistoryItem);
      setSaveMessage(`Открыта связанная генерация: ${linkedHistoryItem.fileName}.`);
      return;
    }

    if (event.kind === 'user_template' || event.kind === 'draft_memory') {
      if (
        loadSchemaFromEvent(
          metadata.target_json,
          event.kind === 'user_template' ? 'Шаблон загружен в Target JSON.' : 'Сохранённый draft JSON загружен в Target JSON.'
        )
      ) {
        return;
      }
    }

    if (event.kind === 'dataset_item' && isObjectRecord(metadata.target_payload)) {
      const payload = isObjectRecord(metadata.target_payload.target_json) ? metadata.target_payload.target_json : metadata.target_payload;
      if (loadSchemaFromEvent(payload, 'Связанный dataset item загружен в Target JSON.')) {
        return;
      }
    }

    if (event.kind === 'global_pattern') {
      const targetField = typeof metadata.target_field === 'string' ? metadata.target_field : '';
      const sourceField = typeof metadata.source_field === 'string' ? metadata.source_field : '';
      setActiveView('generator');
      setSaveMessage(`Глобальный паттерн: ${sourceField || 'source'} -> ${targetField || 'target'}.`);
      const mappingIndex = result.mappings.findIndex((mapping) => mapping.target === targetField);
      if (mappingIndex >= 0) {
        window.setTimeout(() => {
          scrollToReviewTarget(`mapping-review-${mappingIndex}`);
        }, 120);
      }
    }
  };

  useEffect(() => {
    if (previewSheets.length === 0) {
      if (activePreviewSheet !== null) {
        setActivePreviewSheet(null);
      }
      return;
    }

    if (!activePreviewSheet || !previewSheets.some((sheet) => sheet.name === activePreviewSheet)) {
      setActivePreviewSheet(previewSheets[0].name);
    }
  }, [activePreviewSheet, previewSheets]);

  useEffect(() => {
    if (normalizedSourceStructureTabs.length === 0) {
      if (sourceStructureTab !== 'warnings') {
        setSourceStructureTab('warnings');
      }
      return;
    }

    if (!normalizedSourceStructureTabs.includes(sourceStructureTab)) {
      setSourceStructureTab(normalizedSourceStructureTabs[0]);
    }
  }, [normalizedSourceStructureTabs, sourceStructureTab]);

  useEffect(() => {
    setDisplayName(profile.name);
  }, [profile.name]);

  useEffect(() => {
    let cancelled = false;

    async function loadLearningData() {
      if (profile.skipped) {
        if (!cancelled) {
          setLearningMemory(null);
        }
        return;
      }

      try {
        const [nextEvents, nextMemory] = await Promise.all([
          fetchLearningEvents(profile.id, 18),
          fetchLearningMemory(profile.id, 14),
        ]);
        if (!cancelled) {
          setLearningEvents(nextEvents);
          setLearningMemory(nextMemory);
          setLearningEventsError('');
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Не удалось загрузить данные обучения.';
          setLearningEventsError(message);
        }
      }
    }

    void loadLearningData();
    return () => {
      cancelled = true;
    };
  }, [profile.id, profile.skipped]);

  useEffect(() => {
    if (profile.skipped || !correctionBaseline || pendingCorrections.length === 0 || correctionSaveBusy) {
      return;
    }

    const generationId = Number(correctionBaseline.generationId);
    if (!Number.isFinite(generationId)) {
      return;
    }

    const timer = window.setTimeout(async () => {
      setCorrectionSaveBusy(true);
      setCorrectionSaveError('');

      try {
        const saved = await saveLearningCorrections({
          userId: profile.id,
          generationId,
          sessionType: 'post_generation_fix',
          notes: `Autosaved ${pendingCorrections.length} correction(s) from the frontend workspace.`,
          metadata: {
            source: 'workspace_autosave',
            file_name: parsedFile?.fileName ?? null,
            selected_sheet: currentPreviewSheet?.name ?? null,
          },
          corrections: pendingCorrections,
        });

        setCorrectionBaseline(buildCorrectionBaseline(String(saved.generationId ?? generationId), schema, result));
        setCorrectionSaveNotice(`Сервер сохранил ${saved.count} правк(и).`);
        setCorrectionSaveError('');
        await refreshLearningData();
      } catch (error) {
        setCorrectionSaveError(error instanceof Error ? error.message : 'Не удалось сохранить правки на сервере.');
      } finally {
        setCorrectionSaveBusy(false);
      }
    }, CORRECTION_AUTOSAVE_MS);

    return () => {
      window.clearTimeout(timer);
    };
  }, [correctionBaseline, correctionSaveBusy, currentPreviewSheet?.name, parsedFile?.fileName, pendingCorrections, profile.id, profile.skipped, result, schema]);

  const handleSelectedFile = async (file: File) => {
    setSelectedFile(file);
    let parsed = await parseFile(file);
    if (needsBackendSourcePreview(parsed.extension)) {
      try {
        parsed = await fetchSourcePreviewFromBackend(file);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Не удалось прочитать структуру источника на backend.';
        parsed = {
          ...parsed,
          warnings: [...parsed.warnings, message],
        };
      }
    }
    setParsedFile(parsed);
    setActivePreviewSheet(parsed.sheets[0]?.name ?? null);
    setSourceStructureTab(preferredSourceStructureTab(parsed));
    setTargetJsonPrepared(Boolean(uploadedTargetJson));
    setSectionStateCache({});
    setAutoGenerateSectionKey(null);
    setSaveMessage('');
    setCorrectionBaseline(null);
    setCorrectionSaveError('');
    setDraftSuggestions([]);
    setMappingReviewNotes({});
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setTargetJsonUploadNotice('');
    setTargetJsonUploadError('');
    setDraftJsonSaved(false);
    setRepairNotice('');
    setRepairError('');
    setActiveRepairActionKey(null);
    setRepairPreview(null);
    setLearningReviewNotice('');
    setLearningReviewError('');
    setGenerationConfirmed(false);
    setResult({ code: defaultCode, mappings: [], preview: [], warnings: [] });
  };

  const onFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    await handleSelectedFile(file);
  };

  const onTargetJsonFileChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = '';
    if (!file) {
      return;
    }

    setTargetJsonUploadNotice('');
    setTargetJsonUploadError('');

    try {
      const content = await file.text();
      JSON.parse(content);
      invalidateGeneratedResult();
      setUploadedTargetJson({
        fileName: file.name,
        content,
      });
      setTargetJsonPrepared(true);
      setSchema(content);
      setDraftSuggestions([]);
      setDraftReviewNotes({});
      setDraftJsonNotice('');
      setDraftJsonError('');
      setDraftJsonSaved(false);
      setTargetJsonUploadNotice('JSON загружен в Target JSON. Draft JSON по источнику временно отключен.');
    } catch (error) {
      setTargetJsonUploadNotice('');
      setTargetJsonUploadError(error instanceof Error ? error.message : 'Не удалось загрузить JSON-файл.');
    }
  };

  const onSchemaChange = (event: ChangeEvent<HTMLTextAreaElement>) => {
    const nextSchema = event.target.value;
    if (nextSchema !== schema) {
      invalidateGeneratedResult();
    }
    if (uploadedTargetJson && nextSchema !== uploadedTargetJson.content) {
      clearUploadedTargetJson();
    }
    setSchema(nextSchema);
  };

  const onDragEnter = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragActive(true);
  };

  const onDragOver = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    if (!dragActive) {
      setDragActive(true);
    }
  };

  const onDragLeave = (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    if (event.currentTarget.contains(event.relatedTarget as Node | null)) return;
    setDragActive(false);
  };

  const onDrop = async (event: DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    if (!file) return;
    await handleSelectedFile(file);
  };

  const captureCurrentSectionState = (): SectionWorkspaceState =>
    buildSectionWorkspaceState({
      schema,
      result,
      draftSuggestions,
      draftJsonSaved,
      generationConfirmed,
      correctionBaseline,
      mappingReviewNotes,
      draftReviewNotes,
    });

  const getSectionStateSnapshot = (sectionKey: string): SectionWorkspaceState | null => {
    if (sectionKey === currentSectionKey) {
      return captureCurrentSectionState();
    }
    return sectionStateCache[sectionKey] ?? null;
  };

  const getPreviewSectionStatus = (sheetName: string): 'idle' | 'cached' | 'confirmed' | 'loading' => {
    const sectionKey = buildSectionCacheKey(parsedFile?.fileName, sheetName);
    if (autoGenerateSectionKey === sectionKey && busy) {
      return 'loading';
    }

    const snapshot = getSectionStateSnapshot(sectionKey);
    if (!snapshot || snapshot.result.code === defaultCode) {
      return 'idle';
    }
    if (snapshot.generationConfirmed) {
      return 'confirmed';
    }
    return 'cached';
  };

  const applySectionState = (nextState: SectionWorkspaceState | null | undefined) => {
    if (!nextState) {
      clearUploadedTargetJson();
      setTargetJsonPrepared(false);
      setSchema(defaultSchema);
      setResult({ code: defaultCode, mappings: [], preview: [], warnings: [] });
      setDraftSuggestions([]);
      setDraftJsonSaved(false);
      setGenerationConfirmed(false);
      setCorrectionBaseline(null);
      setMappingReviewNotes({});
      setDraftReviewNotes({});
      setDraftJsonNotice('');
      setDraftJsonError('');
      setRepairNotice('');
      setRepairError('');
      setActiveRepairActionKey(null);
      setRepairPreview(null);
      setLearningReviewNotice('');
      setLearningReviewError('');
      return;
    }

    clearUploadedTargetJson();
    setTargetJsonPrepared(true);
    setSchema(nextState.schema);
    setResult({
      ...nextState.result,
      mappings: cloneMappings(nextState.result.mappings),
      preview: nextState.result.preview.map((row) => ({ ...row })),
    });
    setDraftSuggestions(cloneDraftSuggestions(nextState.draftSuggestions));
    setDraftJsonSaved(nextState.draftJsonSaved);
    setGenerationConfirmed(nextState.generationConfirmed);
    setCorrectionBaseline(
      nextState.correctionBaseline
        ? {
            ...nextState.correctionBaseline,
            mappings: cloneMappings(nextState.correctionBaseline.mappings),
          }
        : null
    );
    setMappingReviewNotes({ ...nextState.mappingReviewNotes });
    setDraftReviewNotes({ ...nextState.draftReviewNotes });
    setDraftJsonNotice('');
    setDraftJsonError('');
    setSourcePreviewRefreshNotice('');
    setSourcePreviewRefreshError('');
    setRepairNotice('');
    setRepairError('');
    setActiveRepairActionKey(null);
    setRepairPreview(null);
    setLearningReviewNotice('');
    setLearningReviewError('');
  };

  const switchToPreviewSheet = (nextSheetName: string) => {
    if (currentPreviewSheet?.name === nextSheetName) {
      return;
    }
    const nextSectionKey = buildSectionCacheKey(parsedFile?.fileName, nextSheetName);
    const currentState = captureCurrentSectionState();
    const nextState = sectionStateCache[nextSectionKey];
    setSectionStateCache((current) => ({
      ...current,
      [currentSectionKey]: currentState,
    }));
    setActivePreviewSheet(nextSheetName);
    if (nextState) {
      setAutoGenerateSectionKey(null);
      applySectionState(nextState);
      return;
    }

    applySectionState(null);
    if (selectedFile && hasGeneratedResult) {
      setAutoGenerateSectionKey(nextSectionKey);
      setSaveMessage(`Подгружаем генерацию для ${nextSheetName}...`);
    } else {
      setAutoGenerateSectionKey(null);
    }
  };

  const selectRelativePreviewSheet = (direction: -1 | 1) => {
    if (previewSheets.length <= 1 || currentPreviewIndex < 0) {
      return;
    }
    const nextIndex = (currentPreviewIndex + direction + previewSheets.length) % previewSheets.length;
    switchToPreviewSheet(previewSheets[nextIndex].name);
  };

  const onGenerate = async () => {
    if (!selectedFile) {
      setResult({
        code: defaultCode,
        mappings: [],
        preview: [],
        warnings: ['Сначала загрузите CSV, XLSX, XLS, PDF или DOCX.'],
      });
      return;
    }
    if (!targetJsonPrepared) {
      setAutoGenerateSectionKey(null);
      setResult({
        code: defaultCode,
        mappings: [],
        preview: [],
        warnings: ['Сначала загрузите JSON или постройте Draft JSON по источнику.'],
      });
      return;
    }

    setBusy(true);
    setDraftSuggestions([]);
    setMappingReviewNotes({});
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setDraftJsonSaved(false);
    setRepairNotice('');
    setRepairError('');
    setActiveRepairActionKey(null);
    setRepairPreview(null);
    setLearningReviewNotice('');
    setLearningReviewError('');
    setGenerationConfirmed(false);
    try {
      const generated = await generateFromBackend({
        file: selectedFile,
        targetJson: schema,
        userId: profile.skipped ? undefined : profile.id,
        selectedSheet: parsedFile?.sheets.length ? currentPreviewSheet?.name : undefined,
      });

      setParsedFile(generated.parsedFile ?? parsedFile);
      setResult(generated);
      setSaveMessage('');
      setCorrectionSaveError('');

      const nextCorrectionBaseline =
        !profile.skipped && generated.generationId ? buildCorrectionBaseline(generated.generationId, schema, generated) : null;
      setSectionStateCache((current) => ({
        ...current,
        [currentSectionKey]: buildSectionWorkspaceState({
          schema,
          result: generated,
          draftSuggestions: [],
          draftJsonSaved: false,
          generationConfirmed: false,
          correctionBaseline: nextCorrectionBaseline,
          mappingReviewNotes: {},
          draftReviewNotes: {},
        }),
      }));
      setAutoGenerateSectionKey(null);

      if (!profile.skipped) {
        if (generated.generationId) {
          setActiveHistoryId(generated.generationId);
          setCorrectionBaseline(nextCorrectionBaseline);
        }
        try {
          await onSaveHistory();
        } catch (historyError) {
          setSaveMessage(
            historyError instanceof Error
              ? historyError.message
              : 'Generation finished, but the history list could not be refreshed.'
          );
        }
        await refreshLearningData();
      } else {
        setCorrectionBaseline(null);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось выполнить генерацию.';
      setAutoGenerateSectionKey(null);
      setResult({
        code: defaultCode,
        mappings: [],
        preview: [],
        warnings: [message],
      });
      setCorrectionBaseline(null);
    } finally {
      setBusy(false);
    }
  };

  const onRefreshSourceStructure = async () => {
    if (!selectedFile) {
      setSourcePreviewRefreshError('Сначала загрузите файл.');
      return;
    }

    if (sourcePreviewRefreshTimerRef.current !== null) {
      window.clearTimeout(sourcePreviewRefreshTimerRef.current);
      sourcePreviewRefreshTimerRef.current = null;
    }

    setSourcePreviewRefreshBusy(true);
    setSourcePreviewRefreshDone(false);
    setSourcePreviewRefreshNotice('');
    setSourcePreviewRefreshError('');

    try {
      const previousParsedFile = parsedFile;
      const previousPreferredSheetName = currentPreviewSheet?.name ?? activePreviewSheet;
      const refreshedResponse = await refreshSourceStructureFromBackend({
        file: selectedFile,
        targetJson: schema,
        selectedSheet: previousPreferredSheetName,
      });
      const refreshed = refreshedResponse.parsedFile;
      const refreshSummary = describeSourceStructureRefresh(previousParsedFile, refreshed, previousPreferredSheetName);

      setParsedFile(refreshed);
      setResult((current) => ({
        ...current,
        parsedFile: refreshed,
        formExplainability: refreshedResponse.formExplainability ?? current.formExplainability ?? null,
      }));
      setActivePreviewSheet(refreshSummary.nextActiveSheetName);
      setSourceStructureTab(preferredSourceStructureTab(refreshed));
      setSectionStateCache({});
      setAutoGenerateSectionKey(null);
      setActiveRepairActionKey(null);
      setRepairPreview(null);
      setSourcePreviewRefreshDone(true);
      void logSourcePreviewRefreshToBackend({
        fileName: refreshed.fileName ?? selectedFile.name,
        selectedSheet: refreshSummary.nextActiveSheetName,
        result: refreshSummary.networkResult,
        activeSheetChanged: refreshSummary.activeSheetChanged,
        structureChanged: refreshSummary.structureChanged,
        previousSheetName: refreshSummary.consolePayload.previousSheetName,
        nextSheetName: refreshSummary.consolePayload.nextSheetName,
        previousSheetCount: refreshSummary.consolePayload.previousSheetCount,
        nextSheetCount: refreshSummary.consolePayload.nextSheetCount,
        previousColumnCount: refreshSummary.consolePayload.previousColumnCount,
        nextColumnCount: refreshSummary.consolePayload.nextColumnCount,
        previousRowCount: refreshSummary.consolePayload.previousRowCount,
        nextRowCount: refreshSummary.consolePayload.nextRowCount,
        details: refreshSummary.consolePayload.details,
        message: refreshSummary.message,
      });
      sourcePreviewRefreshTimerRef.current = window.setTimeout(() => {
        setSourcePreviewRefreshDone(false);
        sourcePreviewRefreshTimerRef.current = null;
      }, 1800);
      setSourcePreviewRefreshNotice(refreshSummary.message);
    } catch (error) {
      setSourcePreviewRefreshDone(false);
      setSourcePreviewRefreshError(error instanceof Error ? error.message : 'Не удалось перегенерировать структуру источника.');
    } finally {
      setSourcePreviewRefreshBusy(false);
    }
  };

  const onPreviewRepair = async (action: FormRepairAction) => {
    const currentParsed = result.parsedFile ?? parsedFile;
    if (!currentParsed) {
      setRepairError('Сначала постройте генерацию для формы.');
      return;
    }

    const actionKey = buildRepairActionKey(action);
    setRepairBusy(true);
    setRepairError('');
    setRepairNotice('');
    setActiveRepairActionKey(actionKey);

    try {
      const preview = await fetchRepairPreviewFromBackend({
        parsedFile: currentParsed,
        action,
        targetJson: schema,
      });
      setRepairPreview(preview);
      if (preview.previewStatus === 'patch_available') {
        setRepairNotice(`Готов repair preview для "${repairActionLabel(action)}".`);
      } else if (preview.previewStatus === 'inspection_only') {
        setRepairNotice(`Для "${repairActionLabel(action)}" собраны локальные chunks без готового patch.`);
      } else if (preview.previewStatus === 'ambiguous') {
        setRepairNotice(`Preview для "${repairActionLabel(action)}" остаётся ambiguous.`);
      } else {
        setRepairNotice(`Для "${repairActionLabel(action)}" repair patch не собран.`);
      }
    } catch (error) {
      setRepairPreview(null);
      setRepairError(error instanceof Error ? error.message : 'Не удалось получить repair preview.');
    } finally {
      setRepairBusy(false);
    }
  };

  const onApplyRepair = async () => {
    const currentParsed = result.parsedFile ?? parsedFile;
    if (!currentParsed || !repairPreview) {
      setRepairError('Сначала запросите repair preview.');
      return;
    }
    if (Object.keys(repairPreview.proposedPatch ?? {}).length === 0) {
      setRepairError('В repair preview нет patch для применения.');
      return;
    }

    const numericGenerationId =
      !profile.skipped && typeof result.generationId === 'string' && Number.isFinite(Number(result.generationId))
        ? Number(result.generationId)
        : !profile.skipped && typeof result.generationId === 'number'
          ? result.generationId
          : null;

    setRepairBusy(true);
    setRepairError('');
    setRepairNotice('');

    try {
      const applied = await applyRepairFromBackend({
        parsedFile: currentParsed,
        action: repairPreview.action,
        approvedPatch: repairPreview.proposedPatch,
        targetJson: schema,
        generationId: numericGenerationId,
        notes: 'Applied from frontend repair flow.',
        metadata: {
          source: 'workspace_repair_apply',
          action_kind: repairPreview.action.kind,
          target_field: repairPreview.action.targetField ?? null,
          file_name: currentParsed.fileName,
        },
      });

      const nextResult: GenerationResult = {
        ...result,
        parsedFile: applied.parsedFile,
        formExplainability: applied.formExplainability ?? result.formExplainability ?? null,
      };
      setParsedFile(applied.parsedFile);
      setResult(nextResult);
      setRepairPreview((current) =>
        current
          ? {
              ...current,
              proposedPatch: applied.approvedPatch,
              proposedResolutions: applied.updatedResolvedFields,
              formExplainability: applied.formExplainability ?? current.formExplainability ?? null,
            }
          : current
      );
      setSectionStateCache((current) => ({
        ...current,
        [currentSectionKey]: buildSectionWorkspaceState({
          schema,
          result: nextResult,
          draftSuggestions,
          draftJsonSaved,
          generationConfirmed,
          correctionBaseline,
          mappingReviewNotes,
          draftReviewNotes,
        }),
      }));

      if (applied.persistence.persisted && !profile.skipped) {
        await onSaveHistory();
        await refreshLearningData();
        setRepairNotice(
          applied.persistence.versionNumber
            ? `Repair patch применён и сохранён как версия ${applied.persistence.versionNumber}.`
            : 'Repair patch применён и сохранён в истории.'
        );
      } else {
        setRepairNotice('Repair patch применён локально к extraction state.');
      }
    } catch (error) {
      setRepairError(error instanceof Error ? error.message : 'Не удалось применить repair patch.');
    } finally {
      setRepairBusy(false);
    }
  };

  useEffect(() => {
    if (!autoGenerateSectionKey || busy || !selectedFile) {
      return;
    }
    if (autoGenerateSectionKey !== currentSectionKey) {
      return;
    }
    if (sectionStateCache[autoGenerateSectionKey]) {
      setAutoGenerateSectionKey(null);
      return;
    }
    void onGenerate();
  }, [autoGenerateSectionKey, busy, currentSectionKey, sectionStateCache, selectedFile]);

  const onGenerateDraftJson = async () => {
    if (!selectedFile) {
      setDraftJsonError('Сначала загрузите файл.');
      return;
    }
    if (uploadedTargetJson) {
      setDraftJsonError('Сначала измените или уберите загруженный JSON, чтобы снова строить Draft JSON.');
      return;
    }

    setDraftJsonBusy(true);
    setDraftJsonError('');
    setDraftJsonNotice('');

    try {
      const generated = await generateDraftJsonFromBackend({
        file: selectedFile,
        userId: profile.skipped ? undefined : profile.id,
        selectedSheet: currentPreviewSheet?.name,
      });

      setParsedFile(generated.parsedFile ?? parsedFile);
      setDraftSuggestions(cloneDraftSuggestions(generated.fieldSuggestions));
      setDraftReviewNotes({});
      invalidateGeneratedResult();
      clearUploadedTargetJson();
      setTargetJsonPrepared(true);
      setSchema(JSON.stringify(generated.draftJson, null, 2));
      setDraftJsonNotice('Draft JSON построен. Проверьте названия полей и при необходимости сохраните их в память.');
      setDraftJsonError('');
      setDraftJsonSaved(false);
    } catch (error) {
      setDraftJsonError(error instanceof Error ? error.message : 'Не удалось построить draft JSON.');
    } finally {
      setDraftJsonBusy(false);
    }
  };

  const onDraftSuggestionChange = (index: number, nextTargetField: string) => {
    setDraftSuggestions((current) => {
      const updated: DraftFieldSuggestion[] = current.map((item, itemIndex) =>
        itemIndex === index
          ? {
              ...item,
              targetField: nextTargetField,
              status: 'accepted' as const,
            }
          : item
      );
      invalidateGeneratedResult();
      clearUploadedTargetJson();
      setSchema(buildSchemaFromDraftSuggestions(updated));
      return updated;
    });
  };

  const onDraftSuggestionDecision = (index: number, nextStatus: 'accepted' | 'rejected') => {
    setDraftSuggestions((current) => {
      const updated: DraftFieldSuggestion[] = current.map((item, itemIndex) =>
        itemIndex === index
          ? {
              ...item,
              status: nextStatus,
            }
          : item
      );
      invalidateGeneratedResult();
      clearUploadedTargetJson();
      setSchema(buildSchemaFromDraftSuggestions(updated.filter((item) => item.status !== 'rejected' && item.targetField.trim())));
      return updated;
    });
  };

  const onSaveDraftJsonLearning = async () => {
    if (profile.skipped) {
      setDraftJsonError('Сохранение в память доступно только после входа в аккаунт.');
      return;
    }

    const schemaFingerprintId = draftSuggestions[0]?.schemaFingerprintId;
    if (!schemaFingerprintId) {
      setDraftJsonError('Сначала постройте draft JSON из файла.');
      return;
    }

    if (hasPendingDraftReview) {
      setDraftJsonError('Сначала разберите все предложенные поля: примите или отклоните каждый вариант.');
      return;
    }

    if (hasRejectedDraftWithoutReason) {
      setDraftJsonError('Для каждого отклонённого поля укажите причину, чтобы сохранить корректный feedback.');
      return;
    }

    let draftJson: Record<string, unknown>;
    try {
      const parsed = JSON.parse(schema) as Record<string, unknown>;
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
        throw new Error('Target JSON must be an object.');
      }
      draftJson = parsed;
    } catch {
      setDraftJsonError('Target JSON сейчас невалиден. Исправьте JSON перед сохранением.');
      return;
    }

    setDraftJsonBusy(true);
    setDraftJsonError('');
    setDraftJsonNotice('');

    try {
      const result = await saveDraftJsonFeedback({
        userId: profile.id,
        schemaFingerprintId,
        draftJson,
        templateName: `${selectedFile?.name?.split('.')?.[0] ?? 'draft'} schema`,
        saveAsTemplate: true,
        notes: 'Draft JSON confirmed from frontend workspace.',
        metadata: {
          file_name: parsedFile?.fileName ?? null,
          selected_sheet: currentPreviewSheet?.name ?? null,
        },
        feedback: draftSuggestions.map((suggestion, index) => {
          const nextStatus = suggestion.status ?? 'accepted';
          const correctedField = nextStatus === 'accepted' ? suggestion.targetField.trim() || null : null;
          const rationale = nextStatus === 'rejected' ? draftReviewNotes[`draft-${index}`]?.trim() || 'Rejected in frontend workspace review.' : null;
          return {
            suggestionId: suggestion.suggestionId ?? null,
            sourceColumn: suggestion.sourceColumn,
            suggestedField: suggestion.targetField,
            status: nextStatus,
            correctedField,
            confidenceAfter: nextStatus === 'accepted' ? 1 : 0,
            rationale,
          };
        }),
      });
      clearUploadedTargetJson();
      setTargetJsonPrepared(true);
      setSchema(JSON.stringify(result.draftJson, null, 2));
      setDraftSuggestions((current) =>
        current.map((item) => ({
          ...item,
          status: item.status ?? 'accepted',
        }))
      );
      setDraftJsonSaved(true);
      setDraftJsonNotice(`Draft JSON сохранён. Шаблон "${result.templateName}" добавлен в память.`);
      await refreshLearningData();
    } catch (error) {
      setDraftJsonError(error instanceof Error ? error.message : 'Не удалось сохранить draft JSON.');
    } finally {
      setDraftJsonBusy(false);
    }
  };

  const onConfirmGeneration = async () => {
    if (profile.skipped) {
      setLearningReviewError('Подтверждение генерации доступно только после входа в аккаунт.');
      return;
    }

    const generationId = Number(result.generationId);
    if (!Number.isFinite(generationId)) {
      setLearningReviewError('Нет generation id для подтверждения результата.');
      return;
    }

    if (hasPendingMappingReview) {
      setLearningReviewError('Сначала разберите все спорные маппинги: примите их или отклоните.');
      return;
    }

    if (hasRejectedMappingWithoutReason) {
      setLearningReviewError('Для каждого отклонённого маппинга укажите причину перед подтверждением генерации.');
      return;
    }

    setLearningReviewBusy(true);
    setLearningReviewError('');
    setLearningReviewNotice('');

    try {
      const feedbackItems = result.mappings.flatMap((mapping, index) => {
        if (!mappingNeedsExplicitReview(mapping)) {
          return [];
        }

          const nextStatus = mapping.status ?? 'accepted';
          const hasSource = Boolean(mapping.source && mapping.source !== 'not found');
          const rationale = nextStatus === 'rejected' ? mappingReviewNotes[`mapping-${index}`]?.trim() || 'Rejected in frontend workspace review.' : null;
          return [
            {
              suggestionId: mapping.suggestionId ?? null,
              targetField: mapping.target,
              status: nextStatus,
              sourceField: hasSource ? mapping.source : null,
              correctedSourceField: nextStatus === 'accepted' && hasSource ? mapping.source : null,
              correctedTargetField: nextStatus === 'accepted' ? mapping.target : null,
              confidenceAfter: confidenceToScore(mapping.confidence),
              rationale,
              metadata: {
                source_of_truth: mapping.sourceOfTruth ?? null,
              },
            },
          ];
        });

      if (feedbackItems.length > 0) {
        await sendMappingFeedback({
          userId: profile.id,
          generationId,
          schemaFingerprintId: result.schemaFingerprintId ?? null,
          notes: 'Generation reviewed and confirmed in frontend workspace.',
          metadata: {
            file_name: parsedFile?.fileName ?? null,
            selected_sheet: currentPreviewSheet?.name ?? null,
          },
          feedback: feedbackItems,
        });
      }

      const confirmation = await confirmGenerationLearning({
        userId: profile.id,
        generationId,
        notes: 'Generation confirmed in frontend workspace.',
      });
      setGenerationConfirmed(true);
      setResult((current) => ({
        ...current,
        mappings: current.mappings.map((mapping) => ({
          ...mapping,
          status: mapping.status ?? (mapping.source && mapping.source !== 'not found' ? 'accepted' : mapping.status),
        })),
      }));
      setLearningReviewNotice(
        confirmation.alreadyPromoted
          ? 'Генерация уже была подтверждена раньше.'
          : 'Генерация подтверждена и добавлена в обучающую память.'
      );
      await refreshLearningData();
      await onSaveHistory();
    } catch (error) {
      setLearningReviewError(error instanceof Error ? error.message : 'Не удалось подтвердить генерацию.');
    } finally {
      setLearningReviewBusy(false);
    }
  };

  const onMappingTargetChange = (mappingIndex: number, nextTarget: string) => {
    setResult((current) => ({
      ...current,
      mappings: current.mappings.map((mapping, index) =>
        index === mappingIndex
          ? {
              ...mapping,
              target: nextTarget,
              confidence: 'high',
              reason: 'Manual override from frontend workspace',
              status: 'accepted',
            }
          : mapping
      ),
    }));
  };

  const onMappingSourceChange = (mappingIndex: number, nextSource: string) => {
    setResult((current) => ({
      ...current,
      mappings: current.mappings.map((mapping, index) =>
        index === mappingIndex
          ? {
              ...mapping,
              source: nextSource || 'not found',
              confidence: nextSource ? 'high' : 'none',
              reason: nextSource ? 'Manual source override from frontend workspace' : 'Source removed in frontend workspace',
              status: nextSource ? 'accepted' : 'rejected',
            }
          : mapping
      ),
    }));
  };

  const onMappingDecision = (mappingIndex: number, nextStatus: 'accepted' | 'rejected') => {
    setResult((current) => ({
      ...current,
      mappings: current.mappings.map((mapping, index) =>
        index === mappingIndex
          ? {
              ...mapping,
              status: nextStatus,
              confidence: nextStatus === 'accepted' ? 'high' : mapping.confidence,
              reason:
                nextStatus === 'accepted'
                  ? mapping.reason ?? 'Confirmed in frontend workspace'
                  : 'Rejected in frontend workspace',
            }
          : mapping
      ),
    }));
  };

  const onMappingReviewNoteChange = (mappingIndex: number, note: string) => {
    setMappingReviewNotes((current) => ({
      ...current,
      [`mapping-${mappingIndex}`]: note,
    }));
  };

  const onDraftReviewNoteChange = (draftIndex: number, note: string) => {
    setDraftReviewNotes((current) => ({
      ...current,
      [`draft-${draftIndex}`]: note,
    }));
  };

  const scrollToReviewTarget = (targetId: string) => {
    if (reviewFocusTimerRef.current !== null) {
      window.clearTimeout(reviewFocusTimerRef.current);
    }

    setReviewFocusTarget(targetId);

    window.requestAnimationFrame(() => {
      const target = document.getElementById(targetId);
      if (!target) {
        return;
      }

      target.scrollIntoView({
        behavior: 'smooth',
        block: 'center',
      });

      window.setTimeout(() => {
        const focusable = target.querySelector('input, select, button, textarea') as HTMLElement | null;
        focusable?.focus({ preventScroll: true });
      }, 220);
    });

    reviewFocusTimerRef.current = window.setTimeout(() => {
      setReviewFocusTarget((current) => (current === targetId ? null : current));
      reviewFocusTimerRef.current = null;
    }, 2200);
  };

  const onDownload = async () => {
    if (window.electronAPI) {
      const saved = await window.electronAPI.saveGeneratedFile({
        code: result.code,
        suggestedName: `${parsedFile?.fileName?.split('.')?.[0] ?? 'parser'}.ts`,
      });
      if (!saved.canceled && saved.filePath) {
        setSaveMessage(`Файл сохранен: ${saved.filePath}`);
      }
      return;
    }

    const blob = new Blob([result.code], { type: 'text/typescript;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'parser.ts';
    anchor.click();
    URL.revokeObjectURL(url);
    setSaveMessage('Файл скачан через браузер.');
  };

  const onCopyCode = async () => {
    if (!result.code) return;
    try {
      await navigator.clipboard.writeText(result.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch (error) {
      console.error(error);
    }
  };

  const resetEmailChangeState = () => {
    setNewEmail('');
    setEmailPassword('');
    setEmailChangeCode('');
    setEmailChangeError('');
    setEmailChangeNotice('');
    setEmailCodeRequested(false);
  };

  const resetPasswordChangeState = () => {
    setCurrentPassword('');
    setNextPassword('');
    setRepeatPassword('');
    setPasswordChangeError('');
    setPasswordChangeNotice('');
  };

  const closeAccountModal = () => {
    setAccountModalOpen(false);
    resetEmailChangeState();
    resetPasswordChangeState();
  };

  const sendEmailChangeCode = async () => {
    if (!newEmail.trim()) {
      setEmailChangeError('Введите новый email.');
      return;
    }

    setEmailCodeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const response = await requestEmailChangeCode({ userId: profile.id, newEmail: newEmail.trim().toLowerCase() });
      const ttlMinutes = Math.max(1, Math.ceil(response.expiresIn / 60));
      setEmailCodeRequested(true);
      setEmailChangeNotice(`${response.message} Код действует ${ttlMinutes} мин.`);
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось отправить код.');
    } finally {
      setEmailCodeBusy(false);
    }
  };

  const submitEmailChangeByPassword = async () => {
    setEmailChangeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const nextProfile = await changeEmailWithPassword({
        userId: profile.id,
        newEmail: newEmail.trim().toLowerCase(),
        currentPassword: emailPassword,
      });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      resetEmailChangeState();
      setEmailChangeNotice('Почта успешно обновлена через пароль.');
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось сменить почту.');
    } finally {
      setEmailChangeBusy(false);
    }
  };

  const submitEmailChangeByCode = async () => {
    setEmailChangeBusy(true);
    setEmailChangeError('');
    setEmailChangeNotice('');

    try {
      const nextProfile = await changeEmailWithCode({
        userId: profile.id,
        newEmail: newEmail.trim().toLowerCase(),
        verificationCode: emailChangeCode.trim(),
      });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      resetEmailChangeState();
      setEmailChangeNotice('Почта успешно обновлена по коду из письма.');
    } catch (error) {
      setEmailChangeError(error instanceof Error ? error.message : 'Не удалось сменить почту.');
    } finally {
      setEmailChangeBusy(false);
    }
  };

  const submitDisplayName = async () => {
    if (profile.skipped) {
      return;
    }

    setProfileSaveBusy(true);
    setProfileSaveError('');
    setProfileSaveNotice('');

    try {
      const nextProfile = await updateProfileName({ userId: profile.id, name: displayName.trim() });
      onProfileUpdate({ ...nextProfile, skipped: profile.skipped });
      setProfileSaveNotice('Имя обновлено.');
    } catch (error) {
      setProfileSaveError(error instanceof Error ? error.message : 'Не удалось обновить имя.');
    } finally {
      setProfileSaveBusy(false);
    }
  };

  const submitPasswordChange = async () => {
    if (!currentPassword.trim()) {
      setPasswordChangeError('Введите текущий пароль.');
      return;
    }
    if (!nextPassword.trim()) {
      setPasswordChangeError('Введите новый пароль.');
      return;
    }
    if (nextPassword.trim().length < 8) {
      setPasswordChangeError('Пароль должен содержать минимум 8 символов.');
      return;
    }
    if (nextPassword !== repeatPassword) {
      setPasswordChangeError('Новый пароль и повтор не совпадают.');
      return;
    }

    setPasswordChangeBusy(true);
    setPasswordChangeError('');
    setPasswordChangeNotice('');

    try {
      const message = await changePasswordWithBackend({
        userId: profile.id,
        currentPassword,
        newPassword: nextPassword,
      });
      resetPasswordChangeState();
      setPasswordChangeNotice(message);
    } catch (error) {
      setPasswordChangeError(error instanceof Error ? error.message : 'Не удалось сменить пароль.');
    } finally {
      setPasswordChangeBusy(false);
    }
  };

  void formatDiagnosticLabel;
  void onPreviewRepair;
  void onApplyRepair;
  void repairActionChunkCount;
  void repairBusy;
  void activeRepairActionKey;

  return (
    <div
      className="workspace-stage"
      onDragEnter={onDragEnter}
      onDragLeave={onDragLeave}
      onDragOver={onDragOver}
      onDrop={onDrop}
    >
      <VibeBackground className="workspace-scene" baseScale={1.08} energy={0.26} lite staticFrame />
      <div className="workspace-overlay" />

      <div className="workspace-shell">
        <aside className="sidebar glass-card">
          <div className="sidebar-top">
            <div>
              <div className="eyebrow">Workspace</div>
              <h2>TSGen</h2>
              <p className="subtle-text">{activeView === 'generator' ? 'Генерация и просмотр результата.' : 'Профиль и настройки аккаунта.'}</p>
            </div>
            <button className="icon-btn" onClick={onLogout} title="Выйти" type="button">
              <LogOut size={16} />
            </button>
          </div>

          <div className="mode-switch workspace-mode-switch">
            <button className={activeView === 'generator' ? 'active' : ''} onClick={() => setActiveView('generator')} type="button">
              Генерация
            </button>
            <button className={activeView === 'profile' ? 'active' : ''} onClick={() => setActiveView('profile')} type="button">
              Профиль
            </button>
          </div>

          {activeView === 'generator' ? (
            <section className="generator-panel">
              <div className="panel-title">
                <Sparkles size={16} /> Генерация
              </div>

              <label className={dragActive ? 'upload-zone drag-active' : 'upload-zone'}>
                <input accept=".csv,.xlsx,.xls,.pdf,.docx,.txt,.png,.jpg,.jpeg,.bmp,.gif,.tif,.tiff,.webp" hidden onChange={onFileChange} type="file" />
                <Upload size={18} />
                <strong>Загрузить CSV/XLSX/PDF/DOCX/TXT/IMG</strong>
                <span>{fileSummary}</span>
              </label>
              <div className="subtle-text" style={{ marginTop: 8 }}>
                OCR service:{' '}
                <strong>
                  {backendHealth?.ocrService?.status === 'ok' && backendHealth?.ocrService?.paddleocrAvailable
                    ? 'available'
                    : 'unavailable'}
                </strong>
                {parsedFile?.ocrUsed ? ' · OCR used for current source' : ''}
              </div>

              <label className="upload-zone upload-zone-target-json">
                <input accept=".json,application/json" hidden onChange={onTargetJsonFileChange} type="file" />
                <Upload size={18} />
                <strong>Загрузить JSON</strong>
                <span>{targetJsonFileSummary}</span>
              </label>
              <button className="secondary-btn target-json-clear-btn" disabled={!uploadedTargetJson} onClick={onClearTargetJsonFile} type="button">
                <X size={16} /> Очистить JSON
              </button>

              <div className="field-block">
                <div className="field-caption">Target JSON</div>
                <textarea className="editor-area schema-editor-area" onChange={onSchemaChange} value={schema} />
              </div>

              <div className="generator-action-row">
                <button className="secondary-btn" disabled={draftJsonBusy || busy || !selectedFile || Boolean(uploadedTargetJson)} onClick={onGenerateDraftJson} type="button">
                  <Sparkles size={16} /> {draftJsonBusy ? 'Строим draft JSON...' : 'Draft JSON по источнику'}
                </button>
                {!profile.skipped && draftSuggestions.length > 0 && (
                  <button className="secondary-btn" disabled={draftJsonBusy || hasPendingDraftReview || hasRejectedDraftWithoutReason} onClick={onSaveDraftJsonLearning} type="button">
                    <ShieldCheck size={16} /> {draftJsonBusy ? 'Сохраняем...' : 'Сохранить draft JSON'}
                  </button>
                )}
              </div>

              <button className="primary-btn" disabled={busy || !selectedFile || !targetJsonPrepared} onClick={onGenerate} type="button">
                <WandSparkles size={16} /> {busy ? 'Генерируем...' : 'Сгенерировать'}
              </button>

              <button
                className={hasGeneratedResult ? 'download-btn ready' : 'download-btn'}
                disabled={!hasGeneratedResult}
                onClick={onDownload}
                type="button"
              >
                <Download size={16} /> Скачать .ts
              </button>
              {false && !profile.skipped && (
                <div className="learning-status-stack">
                  <div className="empty-card compact learning-status-card">
                    <strong>{correctionSaveBusy ? 'Сохраняем правки...' : 'Серверное хранение включено'}</strong>
                    <span>Схема, маппинги и код из рабочей области сохраняются в backend автоматически.</span>
                  </div>
                  {correctionSaveNotice && <div className="auth-status auth-status-success">{correctionSaveNotice}</div>}
                  {correctionSaveError && <div className="warning-item auth-status auth-status-error">{correctionSaveError}</div>}
                </div>
              )}
            </section>
          ) : (
            <>
              <section
                className="profile-nav-card sidebar-history-card sidebar-history-card-match-account"
                style={profileAccountCardHeight ? { maxHeight: profileAccountCardHeight } : undefined}
              >
                <div className="panel-title">
                  <History size={16} /> История генераций
                </div>
                <div className="history-list sidebar-history-list">
                  {history.length === 0 && <div className="empty-card compact">Пока пусто.</div>}
                  {history.map((item) => (
                    <div className="history-item-row" key={item.id}>
                      <button
                        className={item.id === activeHistoryId ? 'history-item active' : 'history-item'}
                        onClick={() => restoreHistoryItem(item)}
                        type="button"
                      >
                        <strong>{item.fileName}</strong>
                        <span>{new Date(item.createdAt).toLocaleString()}</span>
                      </button>
                      <button
                        aria-label={`Удалить ${item.fileName} из истории`}
                        className="history-item-delete"
                        disabled={historyDeleteBusyId !== null}
                        onClick={(event) => {
                          event.stopPropagation();
                          openHistoryDeleteModal(item);
                        }}
                        title="Удалить из истории"
                        type="button"
                      >
                        <X size={12} />
                      </button>
                    </div>
                  ))}
                </div>
              </section>
              {!profile.skipped && history.length > 0 && (
                <button className="secondary-btn sidebar-history-reset" disabled={historyDeleteBusyId !== null} onClick={openClearHistoryModal} type="button">
                  <X size={16} /> {historyDeleteBusyId === CLEAR_HISTORY_BUSY_ID ? 'Сбрасываем...' : 'Сбросить историю'}
                </button>
              )}
            </>
          )}
        </aside>

        <main className="viewer-area glass-card">
          {activeView === 'generator' ? (
            <>
              <div className="viewer-toolbar">
                <div>
                  <div className="eyebrow">Generated output</div>
                  <h2>Код и просмотр результата</h2>
                </div>
              </div>

              <div className="viewer-grid">
                <section className="viewer-pane">
                  <div className="pane-header pane-header-with-action">
                    <span className="pane-header-label">
                      <FileSpreadsheet size={16} /> Структура источника
                    </span>
                    <button
                      aria-label="Перегенерировать структуру источника"
                      className={
                        sourcePreviewRefreshBusy
                          ? 'icon-btn copy-code-btn source-refresh-btn is-busy'
                          : sourcePreviewRefreshDone
                            ? 'icon-btn copy-code-btn source-refresh-btn is-success'
                            : 'icon-btn copy-code-btn source-refresh-btn'
                      }
                      disabled={!selectedFile || sourcePreviewRefreshBusy}
                      onClick={onRefreshSourceStructure}
                      title="Перегенерировать структуру источника"
                      type="button"
                    >
                      {sourcePreviewRefreshDone && !sourcePreviewRefreshBusy ? (
                        <Check className="source-refresh-icon source-refresh-icon-success" size={16} />
                      ) : (
                        <RotateCcw className={sourcePreviewRefreshBusy ? 'source-refresh-icon source-refresh-icon-spinning' : 'source-refresh-icon'} size={16} />
                      )}
                    </button>
                  </div>
                  <div className="source-structure-tabs">
                    {normalizedSourceStructureTabs.map((tab) => (
                      <button
                        className={sourceStructureTab === tab ? 'source-structure-tab active' : 'source-structure-tab'}
                        key={tab}
                        onClick={() => setSourceStructureTab(tab)}
                        type="button"
                      >
                        {sourceStructureTabDisplayLabel(tab, parsedFile)}
                      </button>
                    ))}
                  </div>
                  {sourcePreviewRefreshNotice && <div className="auth-status auth-status-success source-refresh-status">{sourcePreviewRefreshNotice}</div>}
                  {sourcePreviewRefreshError && <div className="warning-item auth-status auth-status-error source-refresh-status">{sourcePreviewRefreshError}</div>}
                  {sourceStructureTab === 'tables' && previewSheets.length > 0 && (
                    <>
                      {(parsedFile?.sheets.length ?? 0) > 1 && (
                        <p className="subtle-text mapping-editor-note">
                          Draft JSON строится по текущей выбранной таблице предпросмотра.
                        </p>
                      )}
                      {previewSheets.length > 1 && (
                        <>
                          <div className="preview-switcher">
                            <button className="icon-btn preview-switch-btn" onClick={() => selectRelativePreviewSheet(-1)} title="Предыдущая таблица" type="button">
                              <ChevronLeft size={16} />
                            </button>
                            <div className="preview-switch-info">
                              <span>{previewSectionDisplayLabel(parsedFile, previewSheets.length)}</span>
                              <strong>
                                {currentPreviewIndex + 1} / {previewSheets.length}
                              </strong>
                              <small>{currentPreviewSheet?.name}</small>
                            </div>
                            <button className="icon-btn preview-switch-btn" onClick={() => selectRelativePreviewSheet(1)} title="Следующая таблица" type="button">
                              <ChevronRight size={16} />
                            </button>
                          </div>
                          <div className="preview-status-row">
                            {previewSheets.map((sheet, index) => {
                              const status = getPreviewSectionStatus(sheet.name);
                              return (
                                <button
                                  className={sheet.name === currentPreviewSheet?.name ? 'preview-status-pill active' : 'preview-status-pill'}
                                  key={sheet.name}
                                  onClick={() => switchToPreviewSheet(sheet.name)}
                                  type="button"
                                >
                                  <span className={`preview-status-dot preview-status-dot-${status}`} />
                                  <strong>{index + 1}</strong>
                                  <small>{previewSectionStatusLabel(status)}</small>
                                </button>
                              );
                            })}
                          </div>
                        </>
                      )}
                      <div className="data-grid-wrap" ref={previewGridWrapRef}>
                        {currentPreviewSheet && (currentPreviewSheet.columns.length > 0 || currentPreviewSheet.rows.length > 0) ? (
                          <table className="data-grid">
                            <thead>
                              <tr>
                                {currentPreviewSheet.columns.map((column) => (
                                  <th key={column}>{column}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {currentPreviewSheet.rows.map((row, index) => (
                                <tr key={index}>
                                  {currentPreviewSheet.columns.map((column) => (
                                    <td key={`${index}-${column}`}>{String(row[column] ?? '')}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        ) : (
                          <div className="empty-card">Табличная структура не найдена.</div>
                        )}
                      </div>
                    </>
                  )}
                  {sourceStructureTab === 'fields' && (
                    <div className="source-structure-stack">
                      {extractedFieldEntries.length > 0 ? (
                        <div className="source-field-grid">
                          {extractedFieldEntries.map((entry, index) => (
                            <div className="source-field-card" key={`${entry.kind}-${entry.label}-${index}`}>
                              <span>{entry.kind === 'kv_pair' ? 'label:value' : 'text fact'}</span>
                              <strong>{entry.label}</strong>
                              <p>{entry.value}</p>
                              {entry.hint && <small>{entry.hint}</small>}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="empty-card">Извлечённые поля пока не найдены.</div>
                      )}
                    </div>
                  )}
                  {sourceStructureTab === 'text' && (
                    <div className="source-structure-stack">
                      {parsedFile?.sections.length ? (
                        <div className="text-block-list">
                          {parsedFile.sections.map((section, index) => (
                            <div className="text-block-card" key={`${section.title}-${index}`}>
                              <strong>{section.title}</strong>
                              <p>{section.text}</p>
                            </div>
                          ))}
                        </div>
                      ) : parsedFile?.textBlocks.length ? (
                        <div className="text-block-list">
                          {parsedFile.textBlocks.map((block) => (
                            <div className="text-block-card" key={block.id}>
                              {block.label && <strong>{block.label}</strong>}
                              <p>{block.text}</p>
                            </div>
                          ))}
                        </div>
                      ) : parsedFile?.rawText ? (
                        <pre className="preview-pane source-text-pane">{parsedFile.rawText}</pre>
                      ) : (
                        <div className="empty-card">Текстовый слой не извлечён.</div>
                      )}
                    </div>
                  )}
                  {sourceStructureTab === 'warnings' && (
                    <div className="source-structure-stack">
                      {parsedFile && (
                        <div className="source-field-card source-status-card">
                          <span>{parsedContentTypeLabel(parsedFile.contentType)}</span>
                          <strong>{parsedExtractionStatusLabel(parsedFile.extractionStatus)}</strong>
                          <p>{fileSummary}</p>
                        </div>
                      )}
                      {parsedFile?.warnings.length ? (
                        <div className="warning-list">
                          {parsedFile.warnings.map((warning, index) => (
                            <div className="warning-item" key={`${warning}-${index}`}>
                              {warning}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="empty-card">Предупреждений нет.</div>
                      )}
                    </div>
                  )}
                </section>

                <section className="viewer-pane">
                  <div className="pane-header pane-header-with-action">
                    <span className="pane-header-label">
                      <Sparkles size={16} /> Generated TypeScript
                    </span>
                    <button className="icon-btn copy-code-btn" onClick={onCopyCode} title="Скопировать код" type="button">
                      {copied ? <Check size={16} /> : <Copy size={16} />}
                    </button>
                  </div>
                  <textarea
                    className="code-pane code-editor"
                    onChange={(event) =>
                      setResult((current) => ({
                        ...current,
                        code: event.target.value,
                      }))
                    }
                    spellCheck={false}
                    value={result.code}
                  />
                </section>

              </div>

              <div className="insight-grid">
                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Preview JSON
                  </div>
                  <pre className="preview-pane">{JSON.stringify(result.preview, null, 2)}</pre>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <ShieldCheck size={16} /> Качество генерации
                  </div>
                  <div className="generation-quality-grid">
                    <div className="generation-quality-item">
                      <span>Mapping readiness</span>
                      <strong>{qualityBandLabel(result.mappingOperationalStatus?.status)}</strong>
                      <small>
                        {result.mappingOperationalStatus
                          ? `${result.mappingOperationalStatus.resolvedCount}/${result.mappingOperationalStatus.resolvedCount + result.mappingOperationalStatus.unresolvedCount} полей`
                          : 'ещё не считалось'}
                      </small>
                      <em className={`generation-quality-chip generation-quality-chip-${qualityBandTone(result.mappingOperationalStatus?.status)}`}>
                        {result.mappingOperationalStatus ? formatRatio(result.mappingOperationalStatus.resolvedRatio) : 'нет данных'}
                      </em>
                    </div>
                    <div className="generation-quality-item">
                      <span>TS syntax</span>
                      <strong>{validationStateLabel(result.tsSyntaxValid, 'Компилируется', 'Есть ошибки')}</strong>
                      <small>{result.tsValid ? 'compiler check пройден' : 'compiler diagnostics доступны ниже'}</small>
                      <em className={`generation-quality-chip generation-quality-chip-${validationStateTone(result.tsSyntaxValid)}`}>
                        {result.tsSyntaxValid ? 'valid' : 'invalid'}
                      </em>
                    </div>
                    <div className="generation-quality-item">
                      <span>Preview runtime</span>
                      <strong>{validationStateLabel(result.tsRuntimePreviewValid, 'OK', 'Проблемы')}</strong>
                      <small>Проверка preview на 1-3 строках</small>
                      <em className={`generation-quality-chip generation-quality-chip-${validationStateTone(result.tsRuntimePreviewValid)}`}>
                        {result.tsRuntimePreviewValid ? 'pass' : 'fail'}
                      </em>
                    </div>
                    <div className="generation-quality-item">
                      <span>Output schema</span>
                      <strong>{validationStateLabel(result.outputSchemaValid, 'Совпадает', 'Есть расхождения')}</strong>
                      <small>{result.requiredFields && result.requiredFields.length > 0 ? `required: ${result.requiredFields.join(', ')}` : 'required fields не заданы'}</small>
                      <em className={`generation-quality-chip generation-quality-chip-${validationStateTone(result.outputSchemaValid)}`}>
                        {result.outputSchemaValid ? 'match' : 'mismatch'}
                      </em>
                    </div>
                    <div className="generation-quality-item">
                      <span>Token usage</span>
                      <strong>
                        {currentGenerationTokenUsage ? `${currentGenerationTokenUsage.totalTokens.toLocaleString('ru-RU')} токенов` : 'Нет данных'}
                      </strong>
                      <small>
                        {currentGenerationTokenUsage
                          ? `input ${currentGenerationTokenUsage.inputTokens.toLocaleString('ru-RU')} · output ${currentGenerationTokenUsage.outputTokens.toLocaleString('ru-RU')}`
                          : 'Токены для этой генерации не были сохранены.'}
                      </small>
                      <em className="generation-quality-chip generation-quality-chip-neutral">
                        {currentGenerationTokenUsage?.modelName ?? currentGenerationTokenUsage?.provider ?? 'usage'}
                      </em>
                    </div>
                  </div>
                  <p className="subtle-text mapping-editor-note">
                    Readiness показывает полноту и объём ручной проверки в runtime. Accuracy-метрики считаются отдельно оффлайн на benchmark-наборах.
                  </p>
                  {result.sourceQualityAdjustment?.applied && (
                    <p className="subtle-text mapping-editor-note">
                      Source quality adjustment: confidence был понижен из-за качества source extraction. Причины:{' '}
                      {Object.entries(result.sourceQualityAdjustment.reasons)
                        .map(([reason, count]) => `${reason} (${count})`)
                        .join(', ')}
                      .
                    </p>
                  )}
                  {currentFormExplainability && (
                    <div className="generation-diagnostics-stack">
                      <div className="generation-diagnostics-group">
                        <strong>Form extraction</strong>
                        <div className="generation-diagnostics-list">
                          <div className="generation-diagnostic-item">
                            <span>
                              {currentFormExplainability.documentMode} → {currentFormExplainability.finalSourceMode ?? 'unknown'}
                            </span>
                            <small>
                              scalar: {currentFormExplainability.scalarCount}, groups: {currentFormExplainability.groupCount}, sections:{' '}
                              {currentFormExplainability.sectionCount}, layout lines: {currentFormExplainability.layoutLineCount}
                            </small>
                          </div>
                          <div className="generation-diagnostic-item">
                            <span>
                              resolved {currentFormQuality?.resolvedFieldCount ?? 0}/{currentFormQuality?.targetFieldCount ?? 0}
                            </span>
                            <small>
                              ambiguous: {(currentFormQuality?.ambiguousFields ?? []).join(', ') || 'none'} · critical unresolved:{' '}
                              {(currentFormQuality?.unresolvedCriticalFields ?? []).join(', ') || 'none'}
                            </small>
                          </div>
                        </div>
                      </div>
                      {currentOcrZoneSummary && (
                        <div className="generation-diagnostics-group">
                          <strong>OCR extraction</strong>
                          <div className="generation-diagnostics-list">
                            <div className="generation-diagnostic-item">
                              <span>
                                zones: form {Number(currentOcrZoneSummary.counts?.form ?? 0)}, text {Number(currentOcrZoneSummary.counts?.text ?? 0)},
                                noise {Number(currentOcrZoneSummary.counts?.noise ?? 0)}
                              </span>
                              <small>
                                selected regions: {Array.isArray(currentOcrZoneSummary.routing?.selectedRegionIds)
                                  ? currentOcrZoneSummary.routing?.selectedRegionIds?.join(', ') || 'none'
                                  : 'none'}
                              </small>
                            </div>
                            <div className="generation-diagnostic-item">
                              <span>
                                form confidence:{' '}
                                {typeof currentOcrZoneSummary.routing?.bestFormConfidence === 'number'
                                  ? currentOcrZoneSummary.routing.bestFormConfidence.toFixed(2)
                                  : 'n/a'}{' '}
                                · noise confidence:{' '}
                                {typeof currentOcrZoneSummary.routing?.bestNoiseConfidence === 'number'
                                  ? currentOcrZoneSummary.routing.bestNoiseConfidence.toFixed(2)
                                  : 'n/a'}
                              </span>
                              <small>
                                merge kept {Number(currentOcrZoneSummary.mergeStats?.selectedLineCount ?? 0)}/
                                {Number(currentOcrZoneSummary.mergeStats?.inputLineCount ?? 0)} lines · dropped noise{' '}
                                {Number(currentOcrZoneSummary.mergeStats?.droppedNoiseLines ?? 0)} · dropped low confidence{' '}
                                {Number(currentOcrZoneSummary.mergeStats?.droppedLowConfidenceLines ?? 0)}
                              </small>
                            </div>
                          </div>
                          <p className="subtle-text mapping-editor-note">
                            OCR zones route photo/scan extraction before form parsing. Если noise доминирует или form confidence низкий,
                            repair план рекомендует review OCR routing и checkbox selection.
                          </p>
                        </div>
                      )}
                      {currentPdfZoneSummary && (
                        <div className="generation-diagnostics-group">
                          <strong>PDF zoning</strong>
                          <div className="generation-diagnostics-list">
                            <div className="generation-diagnostic-item">
                              <span>
                                dominant: {currentPdfZoneSummary.dominantZone ?? 'unknown'} · table {Number(currentPdfZoneSummary.counts?.table ?? 0)} ·
                                form {Number(currentPdfZoneSummary.counts?.form ?? 0)} · text {Number(currentPdfZoneSummary.counts?.text ?? 0)} · noise{' '}
                                {Number(currentPdfZoneSummary.counts?.noise ?? 0)}
                              </span>
                              <small>
                                form confidence:{' '}
                                {typeof currentPdfZoneSummary.routing?.bestFormConfidence === 'number'
                                  ? currentPdfZoneSummary.routing.bestFormConfidence.toFixed(2)
                                  : 'n/a'}{' '}
                                · table confidence:{' '}
                                {typeof currentPdfZoneSummary.routing?.bestTableConfidence === 'number'
                                  ? currentPdfZoneSummary.routing.bestTableConfidence.toFixed(2)
                                  : 'n/a'}
                              </small>
                            </div>
                          </div>
                        </div>
                      )}
                      {currentFormRedFlags.length > 0 && (
                        <div className="generation-diagnostics-group">
                          <strong>Red flags</strong>
                          <div className="warning-list">
                            {currentFormRedFlags.map((flag) => (
                              <div className="warning-item" key={flag.code}>
                                <strong>{flag.code}</strong>
                                <div>{flag.message ?? 'Нужно проверить form extraction.'}</div>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                      {currentRepairPlan && currentRepairActions.length > 0 && (
                        <div className="generation-diagnostics-group">
                          <strong>Режим «Переделать»</strong>
                          <div className="review-queue-list">
                            {currentRepairActions.map((action) => {
                              const actionKey = buildRepairActionKey(action);
                              const isActive = activeRepairActionKey === actionKey;
                              const isPreviewForAction =
                                isActive && repairPreview !== null && buildRepairActionKey(repairPreview.action) === actionKey;
                              return (
                                <div className="review-queue-item" key={actionKey}>
                                  <strong>{repairActionLabel(action)}</strong>
                                  <span>
                                    {action.reason} · {action.priority} priority · {repairActionChunkCount(action)} chunk(s)
                                  </span>
                                  <div className="mapping-decision-row">
                                    <button className="secondary-btn" disabled={repairBusy} onClick={() => void onPreviewRepair(action)} type="button">
                                      {repairBusy && isActive ? 'Собираем preview...' : 'Предпросмотр'}
                                    </button>
                                    {isPreviewForAction && repairPreview.previewStatus === 'patch_available' && (
                                      <button className="primary-btn" disabled={repairBusy} onClick={() => void onApplyRepair()} type="button">
                                        {repairBusy ? 'Применяем...' : 'Применить'}
                                      </button>
                                    )}
                                  </div>
                                  {isPreviewForAction && (
                                    <div className="generation-diagnostics-stack">
                                      <div className="generation-diagnostic-item">
                                        <span>{repairPreview.previewStatus}</span>
                                        <small>
                                          local chunks: groups {repairPreview.localChunks.groups.length}, scalars {repairPreview.localChunks.scalars.length},
                                          lines {repairPreview.localChunks.lines.length}
                                        </small>
                                      </div>
                                      {repairPreview.proposedResolutions.length > 0 && (
                                        <div className="generation-diagnostic-item">
                                          <span>Proposed resolutions</span>
                                          <small>
                                            {repairPreview.proposedResolutions
                                              .map((item) => `${item.field}: ${item.status} via ${item.resolvedBy}`)
                                              .join(' · ')}
                                          </small>
                                        </div>
                                      )}
                                      {Object.keys(repairPreview.proposedPatch ?? {}).length > 0 && (
                                        <pre className="preview-pane source-text-pane">{JSON.stringify(repairPreview.proposedPatch, null, 2)}</pre>
                                      )}
                                      {repairPreview.warnings.length > 0 && (
                                        <div className="warning-list">
                                          {repairPreview.warnings.map((warning, index) => (
                                            <div className="warning-item" key={`${actionKey}-warning-${index}`}>
                                              {warning}
                                            </div>
                                          ))}
                                        </div>
                                      )}
                                    </div>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                          <p className="subtle-text mapping-editor-note">
                            Repair работает по локальным chunks из form layout, а не по полному документу. Сначала preview, потом apply.
                          </p>
                        </div>
                      )}
                    </div>
                  )}
                  {(visibleTsDiagnostics.length > 0 || visiblePreviewDiagnostics.length > 0) && (
                    <div className="generation-diagnostics-stack">
                      {visibleTsDiagnostics.length > 0 && (
                        <div className="generation-diagnostics-group">
                          <strong>TypeScript diagnostics</strong>
                          <div className="generation-diagnostics-list">
                            {visibleTsDiagnostics.slice(0, 6).map((diagnostic, index) => (
                              <div className="generation-diagnostic-item" key={`ts-${index}`}>
                                <span>{formatDiagnosticLabel(diagnostic)}</span>
                                <small>{diagnostic.message}</small>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                      {visiblePreviewDiagnostics.length > 0 && (
                        <div className="generation-diagnostics-group">
                          <strong>Preview vs schema</strong>
                          <div className="generation-diagnostics-list">
                            {visiblePreviewDiagnostics.slice(0, 6).map((diagnostic, index) => (
                              <div className="generation-diagnostic-item" key={`preview-${index}`}>
                                <span>{formatDiagnosticLabel(diagnostic)}</span>
                                <small>{diagnostic.message}</small>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Eye size={16} /> Ожидают проверки
                  </div>
                  <div className="review-queue-grid">
                    <div className="review-queue-stat">
                      <strong>{pendingReviewTotal}</strong>
                      <span>всего на проверке</span>
                    </div>
                    <div className="review-queue-stat">
                      <strong>{pendingMappingReviewItems.length}</strong>
                      <span>mapping suggestions</span>
                    </div>
                    <div className="review-queue-stat">
                      <strong>{pendingDraftReviewItems.length}</strong>
                      <span>draft JSON suggestions</span>
                    </div>
                  </div>
                  {pendingReviewTotal > 0 ? (
                    <div className="review-queue-list">
                      {sortedPendingMappingReviewItems.map((item) => (
                        <button className="review-queue-item review-queue-item-button" key={item.id} onClick={() => scrollToReviewTarget(item.scrollTargetId)} type="button">
                          <strong>{item.title}</strong>
                          <span>{item.description}</span>
                        </button>
                      ))}
                      {sortedPendingDraftReviewItems.map((item) => (
                        <button className="review-queue-item review-queue-item-button" key={item.id} onClick={() => scrollToReviewTarget(item.scrollTargetId)} type="button">
                          <strong>{item.title}</strong>
                          <span>{item.description}</span>
                        </button>
                      ))}
                    </div>
                  ) : (
                    <div className="empty-card compact review-queue-empty-card">Сейчас всё разобрано. Можно сохранять draft JSON и подтверждать генерацию.</div>
                  )}
                  <p className="subtle-text mapping-editor-note">
                    Пока здесь есть элементы, сохранение draft JSON и подтверждение генерации будут ждать вашего решения.
                  </p>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <SquarePen size={16} /> Mapping overrides
                  </div>
                  <div className="mapping-editor-list mapping-editor-list-scroll">
                    {result.mappings.length === 0 && <div className="empty-card compact">После генерации здесь появятся найденные соответствия.</div>}
                    {sortedMappingRows.map(({ mapping, index }) => (
                      <div
                        className={reviewFocusTarget === `mapping-review-${index}` ? 'mapping-editor-row review-target-focus' : 'mapping-editor-row'}
                        id={`mapping-review-${index}`}
                        key={`${mapping.source}-${mapping.target}-${index}`}
                      >
                        <div className="mapping-editor-meta">
                          <span className={`mapping-status-chip mapping-status-${mapping.status ?? 'suggested'}`}>{mappingStatusLabel(mapping)}</span>
                          <span className="mapping-source-chip">{mappingSourceLabel(mapping)}</span>
                          <span className={`mapping-stage-chip mapping-stage-${mappingStageState(mapping, generationConfirmed)}`}>
                            {stageStateLabel(mappingStageState(mapping, generationConfirmed))}
                          </span>
                          {mapping.reason && <span className="mapping-reason-chip">{mapping.reason}</span>}
                        </div>
                        <div className="mapping-editor-grid">
                          <div className="mapping-editor-source">
                            <span>Source</span>
                            <select
                              className="mapping-editor-select"
                              onChange={(event) => onMappingSourceChange(index, event.target.value)}
                              value={mapping.source === 'not found' ? '' : mapping.source}
                            >
                              <option value="">Не найдено</option>
                              {mappingSourceOptions.map((sourceField) => (
                                <option key={sourceField} value={sourceField}>
                                  {sourceField}
                                </option>
                              ))}
                            </select>
                          </div>
                          <div className="mapping-editor-source">
                            <span>Target</span>
                            <select
                              className="mapping-editor-select"
                              disabled={mappingTargetOptions.length === 0}
                              onChange={(event) => onMappingTargetChange(index, event.target.value)}
                              value={mapping.target}
                            >
                              {mappingTargetOptions.length === 0 && <option value={mapping.target}>{mapping.target}</option>}
                              {mappingTargetOptions.map((targetField) => (
                                <option key={targetField} value={targetField}>
                                  {targetField}
                                </option>
                              ))}
                            </select>
                          </div>
                        </div>
                        {mappingNeedsExplicitReview(mapping) && (
                          <>
                            <div className="mapping-decision-row">
                              <button
                                className={mapping.status === 'accepted' ? 'decision-chip active accepted' : 'decision-chip'}
                                onClick={() => onMappingDecision(index, 'accepted')}
                                type="button"
                              >
                                Принять
                              </button>
                              <button
                                className={mapping.status === 'rejected' ? 'decision-chip active rejected' : 'decision-chip'}
                                onClick={() => onMappingDecision(index, 'rejected')}
                                type="button"
                              >
                                Отклонить
                              </button>
                            </div>
                            {mapping.status === 'rejected' && (
                              <label className="review-note-field">
                                <span>Причина отклонения</span>
                                <textarea
                                  className="review-note-textarea"
                                  onChange={(event) => onMappingReviewNoteChange(index, event.target.value)}
                                  placeholder="Например: колонка означает другое поле, это не customerName."
                                  value={mappingReviewNotes[`mapping-${index}`] ?? ''}
                                />
                              </label>
                            )}
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                  {!profile.skipped && result.generationId && (
                    <div className="learning-review-actions">
                      <button
                        className="primary-btn"
                        disabled={learningReviewBusy || generationConfirmed || hasPendingMappingReview || hasRejectedMappingWithoutReason || (!hasReviewableMappings && result.mappings.length === 0)}
                        onClick={onConfirmGeneration}
                        type="button"
                      >
                        <ShieldCheck size={16} />
                        {learningReviewBusy ? 'Подтверждаем...' : generationConfirmed ? 'Генерация подтверждена' : 'Подтвердить генерацию'}
                      </button>
                    </div>
                  )}
                  <p className="subtle-text mapping-editor-note">
                    {hasReviewableMappings
                      ? hasRejectedMappingWithoutReason
                        ? 'Для всех отклонённых маппингов укажите причину. После этого генерацию можно будет подтвердить.'
                        : 'Проверьте спорные соответствия и подтвердите генерацию, чтобы добавить результат в обучающую память.'
                      : 'Изменения сохраняются как рабочие правки. Подтверждение закрепит результат в обучающей памяти.'}
                  </p>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <WandSparkles size={16} /> Draft JSON naming
                  </div>
                  <div className="mapping-editor-list">
                    {draftSuggestions.length === 0 && <div className="empty-card compact">После нажатия «Draft JSON по источнику» здесь появятся названия полей для проверки.</div>}
                    {sortedDraftSuggestionRows.map(({ suggestion, index }) => (
                      <div
                        className={reviewFocusTarget === `draft-review-${index}` ? 'mapping-editor-row review-target-focus' : 'mapping-editor-row'}
                        id={`draft-review-${index}`}
                        key={`${suggestion.sourceColumn}-${suggestion.targetField}-${index}`}
                      >
                        <div className="mapping-editor-meta">
                          <span className={`mapping-status-chip mapping-status-${suggestion.status ?? 'suggested'}`}>{draftSuggestionStatusLabel(suggestion)}</span>
                          <span className="mapping-source-chip">{draftSuggestionSourceLabel(suggestion)}</span>
                          <span className={`mapping-stage-chip mapping-stage-${draftSuggestionStageState(suggestion, draftJsonSaved)}`}>
                            {stageStateLabel(draftSuggestionStageState(suggestion, draftJsonSaved))}
                          </span>
                          {suggestion.reason && <span className="mapping-reason-chip">{suggestion.reason}</span>}
                        </div>
                        <div className="mapping-editor-grid">
                          <div className="mapping-editor-source">
                            <span>Source column</span>
                            <strong>{suggestion.sourceColumn}</strong>
                          </div>
                          <div className="mapping-editor-source">
                            <span>JSON field</span>
                            <input
                              className="mapping-editor-select"
                              onChange={(event) => onDraftSuggestionChange(index, event.target.value)}
                              type="text"
                              value={suggestion.targetField}
                            />
                          </div>
                        </div>
                        {draftSuggestionNeedsExplicitReview(suggestion) && (
                          <>
                            <div className="mapping-decision-row">
                              <button
                                className={suggestion.status === 'accepted' ? 'decision-chip active accepted' : 'decision-chip'}
                                onClick={() => onDraftSuggestionDecision(index, 'accepted')}
                                type="button"
                              >
                                Принять
                              </button>
                              <button
                                className={suggestion.status === 'rejected' ? 'decision-chip active rejected' : 'decision-chip'}
                                onClick={() => onDraftSuggestionDecision(index, 'rejected')}
                                type="button"
                              >
                                Отклонить
                              </button>
                            </div>
                            {suggestion.status === 'rejected' && (
                              <label className="review-note-field">
                                <span>Причина отклонения</span>
                                <textarea
                                  className="review-note-textarea"
                                  onChange={(event) => onDraftReviewNoteChange(index, event.target.value)}
                                  placeholder="Например: такое имя поля не подходит по смыслу для этой колонки."
                                  value={draftReviewNotes[`draft-${index}`] ?? ''}
                                />
                              </label>
                            )}
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                  <p className="subtle-text mapping-editor-note">
                    {hasPendingDraftReview
                      ? 'Разберите все предложенные названия. Отклонённые варианты не попадут в память и шаблоны.'
                      : hasRejectedDraftWithoutReason
                        ? 'Для всех отклонённых названий укажите причину. Без неё feedback не сохранится.'
                        : 'Подтверждённые названия попадут в персональную память и шаблоны.'}
                  </p>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <TriangleAlert size={16} /> Warnings
                  </div>
                  <div className="warning-list warning-list-scroll">
                    {visibleWarnings.map((warning, index) => (
                      <div className="warning-item" key={index}>
                        {warning}
                      </div>
                    ))}
                    {visibleWarnings.length === 0 && <div className="empty-card compact">Пока без предупреждений.</div>}
                  </div>
                </section>

              </div>
            </>
          ) : (
            <>
              <div className="viewer-toolbar">
                <div>
                  <div className="eyebrow">Account</div>
                  <h2>Профиль пользователя</h2>
                </div>
              </div>

              <div className="profile-view-grid">
                <section className="insight-card">
                  <div className="pane-header">
                    <UserRound size={16} /> Аккаунт
                  </div>
                  <div className="profile-details">
                    <div className="profile-detail">
                      <span>Имя</span>
                      <div className="profile-inline-form">
                        <div className="auth-input-wrap">
                          <UserRound size={18} />
                          <input
                            placeholder="Введите имя"
                            type="text"
                            value={displayName}
                            onChange={(event) => setDisplayName(event.target.value)}
                          />
                        </div>
                        <button className="secondary-btn profile-inline-save" disabled={profileSaveBusy || profile.skipped} onClick={submitDisplayName} type="button">
                          {profileSaveBusy ? 'Сохраняем...' : 'Сохранить имя'}
                        </button>
                      </div>
                    </div>
                    <div className="profile-detail">
                      <span>Почта</span>
                      <div className="profile-detail-row">
                        <strong>{profile.email}</strong>
                        {!profile.skipped && (
                          <button
                            className="icon-btn profile-edit-btn"
                            onClick={() => {
                              setAccountModalMode('email');
                              setAccountModalOpen(true);
                            }}
                            title="Сменить почту"
                            type="button"
                          >
                            <SquarePen size={16} />
                          </button>
                        )}
                      </div>
                    </div>
                    <div className="profile-detail">
                      <span>Пароль</span>
                      <div className="profile-detail-row">
                        <strong>••••••••••</strong>
                        {!profile.skipped && (
                          <button
                            className="icon-btn profile-edit-btn"
                            onClick={() => {
                              setAccountModalMode('password');
                              setAccountModalOpen(true);
                            }}
                            title="Сменить пароль"
                            type="button"
                          >
                            <SquarePen size={16} />
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                  {profileSaveNotice && <div className="auth-status auth-status-success">{profileSaveNotice}</div>}
                  {profileSaveError && <div className="warning-item auth-status auth-status-error">{profileSaveError}</div>}
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Статистика аккаунта
                  </div>
                  <div className="profile-fun-grid">
                    <div className="empty-card compact">
                      <strong>{profileStats.totalGenerations}</strong>
                      <span>генераций</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.uniqueFiles}</strong>
                      <span>уникальных файлов</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.totalWarnings}</strong>
                      <span>предупреждений всего</span>
                    </div>
                    <div className="empty-card compact">
                      <strong>{profileStats.gigachatApiTokens.toLocaleString('ru-RU')}</strong>
                      <span>токенов GigaChat API</span>
                    </div>
                    <div className="empty-card compact profile-fun-wide">
                      <strong>{profileStats.lastGeneratedAt ? new Date(profileStats.lastGeneratedAt).toLocaleDateString() : '—'}</strong>
                      <span>последняя генерация</span>
                    </div>
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Память системы
                  </div>
                  <div className="insight-stat-row">
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.personalMemory.counts.entries ?? 0}</strong>
                      <span>личная память</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.personalMemory.counts.accepted ?? 0}</strong>
                      <span>accepted</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.globalKnowledge.counts.patterns ?? 0}</strong>
                      <span>общие паттерны</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.globalKnowledge.counts.shared_promoted ?? 0}</strong>
                      <span>promoted</span>
                    </div>
                  </div>
                  <div className="profile-list knowledge-scroll-list system-memory-list">
                    {systemMemoryFeed.length === 0 && (
                      <div className="empty-card compact">Подтверждённые пары и общие паттерны пока не накопились.</div>
                    )}
                    {systemMemoryFeed.map((item) => (
                      <div className="profile-list-item knowledge-event-item" key={item.id}>
                        <div className="knowledge-event-main">
                          <div className="knowledge-event-top">
                            <strong>{item.title}</strong>
                            <span className={item.badgeClassName}>{item.badge}</span>
                          </div>
                          <span>{item.description}</span>
                          <div className="knowledge-chip-row">
                            <span className={item.categoryClassName}>{item.categoryLabel}</span>
                            {item.tags.map((tag, index) => (
                              <span className="knowledge-inline-chip knowledge-inline-chip-muted" key={`${item.id}-${index}`}>
                                {tag}
                              </span>
                            ))}
                          </div>
                          {item.note && <span className="knowledge-item-note">{item.note}</span>}
                        </div>
                        {item.timestamp && <small>{item.timestamp}</small>}
                      </div>
                    ))}
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Семантический граф
                  </div>
                  <div className="insight-stat-row">
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.semanticGraph.counts.nodes ?? 0}</strong>
                      <span>узлов</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.semanticGraph.counts.edges ?? 0}</strong>
                      <span>рёбер</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.semanticGraph.counts.accepted ?? 0}</strong>
                      <span>accepted</span>
                    </div>
                    <div className="insight-stat-pill">
                      <strong>{learningMemory?.layers.semanticGraph.counts.rejected ?? 0}</strong>
                      <span>rejected</span>
                    </div>
                  </div>
                  <div className="profile-list semantic-graph-list knowledge-scroll-list">
                    {semanticGraphFeed.length === 0 && (
                      <div className="empty-card compact">
                        Пока граф пуст. Он начнёт расти после подтверждённых и отклонённых сопоставлений.
                      </div>
                    )}
                    {semanticGraphFeed.map((item) => (
                      <div className="profile-list-item knowledge-event-item semantic-graph-item" key={item.id}>
                        <div className="knowledge-event-main">
                          <div className="knowledge-event-top">
                            <strong>{item.title}</strong>
                            <span className={item.badgeClassName}>{item.badge}</span>
                          </div>
                          <span>{item.description}</span>
                          <div className="knowledge-chip-row">
                            <span className={item.categoryClassName}>{item.categoryLabel}</span>
                            {item.tags.map((tag, index) => (
                              <span className="knowledge-inline-chip knowledge-inline-chip-muted" key={`${item.id}-${index}`}>
                                {tag}
                              </span>
                            ))}
                          </div>
                          {item.note && <span className="knowledge-item-note">{item.note}</span>}
                        </div>
                        {item.timestamp && <small>{item.timestamp}</small>}
                      </div>
                    ))}
                  </div>
                  <p className="subtle-text mapping-editor-note">
                    Граф хранит не целые кейсы, а агрегированные semantic edges между полями с контекстом `entity + attribute + role`.
                  </p>
                </section>

                <section className="insight-card profile-history-card" ref={profileAccountCardRef}>
                  <div className="pane-header">
                    <Sparkles size={16} /> События обучения
                  </div>
                  <div className="event-search-wrap">
                    <Search size={16} />
                    <input
                      onChange={(event) => setLearningEventSearch(event.target.value)}
                      placeholder="Поиск по событиям обучения"
                      type="text"
                      value={learningEventSearch}
                    />
                  </div>
                  <div className="event-filter-row">
                    {(['all', 'staging', 'memory', 'global_pattern', 'dataset'] as LearningEventFilter[]).map((filter) => (
                      <button
                        className={learningEventFilter === filter ? 'event-filter-btn active' : 'event-filter-btn'}
                        key={filter}
                        onClick={() => setLearningEventFilter(filter)}
                        type="button"
                      >
                        <span>{learningEventFilterLabel(filter)}</span>
                        <strong>{learningEventCounts[filter]}</strong>
                      </button>
                    ))}
                  </div>
                  <div className="profile-list knowledge-scroll-list learning-events-list">
                    {learningEvents.length === 0 && <div className="empty-card compact">Пока нет событий обучения. Они появятся после подтверждений, сохранений в память и попадания кейсов в dataset.</div>}
                    {learningEvents.length > 0 && filteredLearningEvents.length === 0 && (
                      <div className="empty-card compact">
                        {learningEventSearch.trim()
                          ? `По запросу «${learningEventSearch.trim()}» ничего не найдено.`
                          : `Для фильтра «${learningEventFilterLabel(learningEventFilter)}» пока нет событий.`}
                      </div>
                    )}
                    {filteredLearningEvents.map((event) =>
                      isLearningEventActionable(event) ? (
                        <button
                          className="profile-list-item profile-list-item-button knowledge-event-item knowledge-event-item-button"
                          key={event.id}
                          onClick={() => onOpenLearningEvent(event)}
                          type="button"
                        >
                          <div className="knowledge-event-main">
                            <div className="knowledge-event-top">
                              <strong>{event.title}</strong>
                              <span className={`knowledge-event-stage knowledge-event-stage-${event.stage}`}>{learningEventStageLabel(event.stage)}</span>
                            </div>
                            <span>{event.description}</span>
                            {learningEventActionLabel(event) && <span className="knowledge-event-action">{learningEventActionLabel(event)}</span>}
                          </div>
                          <small>{new Date(event.createdAt).toLocaleString()}</small>
                        </button>
                      ) : (
                        <div className="profile-list-item knowledge-event-item" key={event.id}>
                          <div className="knowledge-event-main">
                            <div className="knowledge-event-top">
                              <strong>{event.title}</strong>
                              <span className={`knowledge-event-stage knowledge-event-stage-${event.stage}`}>{learningEventStageLabel(event.stage)}</span>
                            </div>
                            <span>{event.description}</span>
                          </div>
                          <small>{new Date(event.createdAt).toLocaleString()}</small>
                        </div>
                      )
                    )}
                  </div>
                  {learningEventsError && <div className="warning-item auth-status auth-status-error">{learningEventsError}</div>}
                </section>
              </div>
            </>
          )}
        </main>
      </div>

      {historyDeleteTarget && !profile.skipped && (
        <div className="profile-modal-backdrop" role="presentation" onClick={closeHistoryDeleteModal}>
          <section className="profile-modal history-delete-modal glass-card" role="dialog" aria-modal="true" aria-labelledby="history-delete-title" onClick={(event) => event.stopPropagation()}>
            <div className="profile-modal-header">
              <div className="history-delete-heading">
                <div className="history-delete-icon" aria-hidden="true">
                  <TriangleAlert size={18} />
                </div>
                <div>
                  <div className="eyebrow">History deletion</div>
                  <h3 id="history-delete-title">Удалить запрос из истории?</h3>
                </div>
              </div>
              <button className="icon-btn" disabled={historyDeleteBusyId !== null} onClick={closeHistoryDeleteModal} title="Закрыть" type="button">
                <X size={16} />
              </button>
            </div>

            <div className="history-delete-copy">
              <p>
                Запрос <strong>{historyDeleteTarget.fileName}</strong> будет удален из истории генераций.
              </p>
              <p className="subtle-text">Связанный сохраненный результат и исходный файл тоже будут удалены из backend-хранилища.</p>
            </div>

            {historyDeleteError && <div className="warning-item auth-status auth-status-error">{historyDeleteError}</div>}

            <div className="history-delete-actions">
              <button className="secondary-btn" disabled={historyDeleteBusyId !== null} onClick={closeHistoryDeleteModal} type="button">
                Отмена
              </button>
              <button className="primary-btn history-delete-confirm" disabled={historyDeleteBusyId !== null} onClick={() => void onDeleteHistoryItem()} type="button">
                <X size={16} /> {historyDeleteBusyId === historyDeleteTarget.id ? 'Удаляем...' : 'Удалить запрос'}
              </button>
            </div>
          </section>
        </div>
      )}

      {historyClearDialogOpen && !profile.skipped && (
        <div className="profile-modal-backdrop" role="presentation" onClick={closeClearHistoryModal}>
          <section className="profile-modal history-delete-modal glass-card" role="dialog" aria-modal="true" aria-labelledby="history-clear-title" onClick={(event) => event.stopPropagation()}>
            <div className="profile-modal-header">
              <div className="history-delete-heading">
                <div className="history-delete-icon" aria-hidden="true">
                  <TriangleAlert size={18} />
                </div>
                <div>
                  <div className="eyebrow">History cleanup</div>
                  <h3 id="history-clear-title">Очистить всю историю генераций?</h3>
                </div>
              </div>
              <button className="icon-btn" disabled={historyDeleteBusyId !== null} onClick={closeClearHistoryModal} title="Закрыть" type="button">
                <X size={16} />
              </button>
            </div>

            <div className="history-delete-copy">
              <p>
                Из истории будут удалены <strong>{history.length}</strong> {pluralizeRu(history.length, 'запись', 'записи', 'записей')}.
              </p>
              <p className="subtle-text">Связанные сохранённые результаты и исходные файлы тоже будут удалены из backend-хранилища.</p>
            </div>

            {historyDeleteError && <div className="warning-item auth-status auth-status-error">{historyDeleteError}</div>}

            <div className="history-delete-actions">
              <button className="secondary-btn" disabled={historyDeleteBusyId !== null} onClick={closeClearHistoryModal} type="button">
                Отмена
              </button>
              <button className="primary-btn history-delete-confirm" disabled={historyDeleteBusyId !== null} onClick={() => void onClearHistory()} type="button">
                <X size={16} /> {historyDeleteBusyId === CLEAR_HISTORY_BUSY_ID ? 'Очищаем...' : 'Очистить историю'}
              </button>
            </div>
          </section>
        </div>
      )}

      {accountModalOpen && !profile.skipped && (
        <div className="profile-modal-backdrop" role="presentation" onClick={closeAccountModal}>
          <section className="profile-modal glass-card" role="dialog" aria-modal="true" onClick={(event) => event.stopPropagation()}>
            <div className="profile-modal-header">
              <div>
                <div className="eyebrow">Account edit</div>
                <h3>{accountModalMode === 'email' ? 'Сменить почту' : 'Сменить пароль'}</h3>
              </div>
              <button className="icon-btn" onClick={closeAccountModal} title="Закрыть" type="button">
                <X size={16} />
              </button>
            </div>

            <div className="mode-switch workspace-mode-switch profile-modal-switch">
              <button className={accountModalMode === 'email' ? 'active' : ''} onClick={() => setAccountModalMode('email')} type="button">
                Почта
              </button>
              <button className={accountModalMode === 'password' ? 'active' : ''} onClick={() => setAccountModalMode('password')} type="button">
                Пароль
              </button>
            </div>

            {accountModalMode === 'email' ? (
              <div className="profile-email-form">
                <label className="auth-field">
                  <span className="auth-field-label">Новый email</span>
                  <div className="auth-input-wrap">
                    <Mail size={18} />
                    <input
                      placeholder="new@email.com"
                      type="email"
                      value={newEmail}
                      onChange={(event) => {
                        setNewEmail(event.target.value);
                        setEmailChangeCode('');
                        setEmailCodeRequested(false);
                      }}
                    />
                  </div>
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Через текущий пароль</span>
                  <PasswordInput icon={LockKeyhole} onChange={setEmailPassword} placeholder="Введите текущий пароль" value={emailPassword} />
                </label>

                <button className="primary-btn" disabled={emailChangeBusy} onClick={submitEmailChangeByPassword} type="button">
                  <ShieldCheck size={16} /> Сменить почту через пароль
                </button>

                <div className="profile-divider" />

                <div className="auth-code-inline">
                  <div className="auth-input-wrap auth-code-input">
                    <ShieldCheck size={18} />
                    <input
                      placeholder="Код с текущей почты"
                      type="text"
                      value={emailChangeCode}
                      onChange={(event) => setEmailChangeCode(event.target.value)}
                    />
                  </div>
                  <button className="secondary-btn auth-code-btn auth-code-btn-inline" disabled={emailCodeBusy} onClick={sendEmailChangeCode} type="button">
                    <Mail size={16} />
                    <span>{emailCodeBusy ? 'Отправляем...' : 'Получить код'}</span>
                  </button>
                </div>

                <button className="primary-btn" disabled={emailChangeBusy || !emailCodeRequested} onClick={submitEmailChangeByCode} type="button">
                  <ShieldCheck size={16} /> Сменить почту по коду
                </button>

                {emailChangeNotice && <div className="auth-status auth-status-success">{emailChangeNotice}</div>}
                {emailChangeError && <div className="warning-item auth-status auth-status-error">{emailChangeError}</div>}
              </div>
            ) : (
              <div className="profile-email-form">
                <label className="auth-field">
                  <span className="auth-field-label">Текущий пароль</span>
                  <PasswordInput icon={LockKeyhole} onChange={setCurrentPassword} placeholder="Введите текущий пароль" value={currentPassword} />
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Новый пароль</span>
                  <PasswordInput icon={KeyRound} onChange={setNextPassword} placeholder="Минимум 8 символов" value={nextPassword} />
                </label>

                <label className="auth-field">
                  <span className="auth-field-label">Повторите пароль</span>
                  <PasswordInput icon={ShieldCheck} onChange={setRepeatPassword} placeholder="Повторите новый пароль" value={repeatPassword} />
                </label>

                <button className="primary-btn" disabled={passwordChangeBusy} onClick={submitPasswordChange} type="button">
                  <ShieldCheck size={16} /> {passwordChangeBusy ? 'Сохраняем...' : 'Сменить пароль'}
                </button>

                {passwordChangeNotice && <div className="auth-status auth-status-success">{passwordChangeNotice}</div>}
                {passwordChangeError && <div className="warning-item auth-status auth-status-error">{passwordChangeError}</div>}
              </div>
            )}
          </section>
        </div>
      )}
    </div>
  );
}
