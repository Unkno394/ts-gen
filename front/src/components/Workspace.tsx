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
  changeEmailWithCode,
  changeEmailWithPassword,
  changePasswordWithBackend,
  confirmGenerationLearning,
  deleteHistoryEntry,
  fetchLearningEvents,
  fetchLearningMemory,
  fetchLearningSummary,
  fetchSourcePreviewFromBackend,
  generateDraftJsonFromBackend,
  generateFromBackend,
  requestEmailChangeCode,
  saveLearningCorrections,
  saveDraftJsonFeedback,
  sendMappingFeedback,
  updateProfileName,
} from '../lib/api';
import type {
  DraftFieldSuggestion,
  GenerationResult,
  HistoryItem,
  LearningEvent,
  LearningMemory,
  LearningSummary,
  ManualCorrectionInput,
  MappingInfo,
  ParsedFileInfo,
  ParsedSheetInfo,
  UserProfile,
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

const defaultSchema = `{
  "customerName": "",
  "amount": 0,
  "createdAt": ""
}`;

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

function buildPreviewSheet(name: string, columns: string[], rows: Record<string, unknown>[]): ParsedSheetInfo {
  return {
    name,
    columns,
    rows,
  };
}

function parseWorkbookSheets(workbook: WorkBook, xlsxModule: typeof import('xlsx')): {
  columns: string[];
  rows: Record<string, unknown>[];
  sheets: ParsedSheetInfo[];
  warnings: string[];
} {
  const sheets = workbook.SheetNames.map((sheetName) => {
    const sheet = workbook.Sheets[sheetName];
    const json = xlsxModule.utils.sheet_to_json<Record<string, unknown>>(sheet, { defval: '' });
    const columns = Object.keys(json[0] ?? {});
    const rows = json
      .slice(0, 8)
      .map((row: Record<string, unknown>) => row as Record<string, string | number | boolean | null>);
    return buildPreviewSheet(sheetName, columns, rows);
  }).filter((sheet) => sheet.columns.length > 0 || sheet.rows.length > 0);

  const firstSheet = sheets[0] ?? buildPreviewSheet(workbook.SheetNames[0] ?? 'Sheet 1', [], []);
  const warnings: string[] = [];

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
      sheets: workbookPreview.sheets,
      warnings: workbookPreview.warnings,
    };
  }


  if (extension === 'pdf' || extension === 'docx') {
    return {
      fileName: file.name,
      extension,
      columns: [],
      rows: [],
      sheets: [],
      warnings: ['Документ загружен. Таблицу из PDF/DOCX прочитаем на backend при генерации.'],
    };
  }

  return {
    fileName: file.name,
    extension,
    columns: [],
    rows: [],
    sheets: [],
    warnings: ['Поддерживаются CSV, XLSX, XLS, PDF и DOCX.'],
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
    const parsed = JSON.parse(schemaText) as Record<string, unknown>;
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
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
    return 'Листы файла';
  }
  return 'Секции preview';
}

function previewSectionStatusLabel(status: 'idle' | 'cached' | 'confirmed' | 'loading'): string {
  if (status === 'confirmed') return 'Подтверждена';
  if (status === 'cached') return 'В кэше';
  if (status === 'loading') return 'Генерируем';
  return 'Не считалась';
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
  return Boolean(mapping.suggestionId) || mapping.status === 'suggested' || mapping.sourceOfTruth === 'model_suggestion' || mapping.sourceOfTruth === 'position_fallback' || mapping.sourceOfTruth === 'unresolved';
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
  const [historyDeleteError, setHistoryDeleteError] = useState('');
  const [saveMessage, setSaveMessage] = useState('');
  const [dragActive, setDragActive] = useState(false);
  const [copied, setCopied] = useState(false);
  const [activePreviewSheet, setActivePreviewSheet] = useState<string | null>(null);
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
  const [learningSummary, setLearningSummary] = useState<LearningSummary | null>(null);
  const [learningSummaryError, setLearningSummaryError] = useState('');
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
  const [learningReviewBusy, setLearningReviewBusy] = useState(false);
  const [learningReviewNotice, setLearningReviewNotice] = useState('');
  const [learningReviewError, setLearningReviewError] = useState('');
  const [generationConfirmed, setGenerationConfirmed] = useState(false);
  const [mappingReviewNotes, setMappingReviewNotes] = useState<Record<string, string>>({});
  const [draftReviewNotes, setDraftReviewNotes] = useState<Record<string, string>>({});
  const [sectionStateCache, setSectionStateCache] = useState<Record<string, SectionWorkspaceState>>({});
  const [autoGenerateSectionKey, setAutoGenerateSectionKey] = useState<string | null>(null);
  const [reviewFocusTarget, setReviewFocusTarget] = useState<string | null>(null);
  const reviewFocusTimerRef = useRef<number | null>(null);
  const hasGeneratedResult = result.code !== defaultCode;

  const refreshLearningSummary = async () => {
    if (profile.skipped) {
      setLearningSummary(null);
      setLearningSummaryError('');
      setLearningEvents([]);
      setLearningMemory(null);
      setLearningEventsError('');
      return;
    }

    try {
      const [nextSummary, nextEvents, nextMemory] = await Promise.all([
        fetchLearningSummary(profile.id),
        fetchLearningEvents(profile.id, 18),
        fetchLearningMemory(profile.id, 14),
      ]);
      setLearningSummary(nextSummary);
      setLearningSummaryError('');
      setLearningEvents(nextEvents);
      setLearningMemory(nextMemory);
      setLearningEventsError('');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось загрузить данные обучения.';
      setLearningSummaryError(message);
      setLearningEventsError(message);
    }
  };

  useEffect(() => {
    if (!historyDeleteTarget || historyDeleteBusyId !== null) {
      return;
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setHistoryDeleteError('');
        setHistoryDeleteTarget(null);
      }
    };

    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [historyDeleteBusyId, historyDeleteTarget]);

  const previewSheets = useMemo(() => {
    if (!parsedFile) {
      return [];
    }

    if (parsedFile.sheets.length > 0) {
      return parsedFile.sheets;
    }

    if (parsedFile.columns.length === 0 && parsedFile.rows.length === 0) {
      return [];
    }

    return [buildPreviewSheet(parsedFile.fileName, parsedFile.columns, parsedFile.rows)];
  }, [parsedFile]);

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

  const currentSectionKey = useMemo(
    () => buildSectionCacheKey(parsedFile?.fileName, currentPreviewSheet?.name ?? activePreviewSheet ?? 'default'),
    [activePreviewSheet, currentPreviewSheet?.name, parsedFile?.fileName]
  );

  const fileSummary = useMemo(() => {
    if (!parsedFile) {
      return 'Файл еще не загружен';
    }

    if ((parsedFile.extension === 'pdf' || parsedFile.extension === 'docx') && parsedFile.sheets.length > 1) {
      return `${parsedFile.fileName} · ${parsedFile.sheets.length} таблиц(ы) найдено`;
    }

    if (parsedFile.extension === 'pdf' || parsedFile.extension === 'docx') {
      return `${parsedFile.fileName} · документ загружен`;
    }

    if (parsedFile.sheets.length > 1) {
      return `${parsedFile.fileName} · ${parsedFile.sheets.length} sheets · ${parsedFile.rows.length} preview rows`;
    }

    return `${parsedFile.fileName} · ${parsedFile.columns.length} колонок · ${parsedFile.rows.length} preview rows`;
  }, [parsedFile]);

  const schemaTargetFields = useMemo(() => parseSchemaFields(schema), [schema]);

  const mappingTargetOptions = useMemo(() => {
    const options = schemaTargetFields.length > 0 ? schemaTargetFields : result.mappings.map((mapping) => mapping.target);
    return Array.from(new Set(options.filter(Boolean)));
  }, [result.mappings, schemaTargetFields]);

  const mappingSourceOptions = useMemo(() => {
    const sourceColumns = currentPreviewSheet?.columns ?? parsedFile?.columns ?? [];
    const currentSources = result.mappings.map((mapping) => mapping.source).filter((value) => value && value !== 'not found');
    return Array.from(new Set([...sourceColumns, ...currentSources])) as string[];
  }, [currentPreviewSheet?.columns, parsedFile?.columns, result.mappings]);

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

  const currentKnowledgeSummary = useMemo(() => {
    const mappings = result.mappings.reduce(
      (accumulator, mapping) => {
        const stage = mappingStageState(mapping, generationConfirmed);
        if (stage === 'staging') {
          accumulator.staging += 1;
        } else if (stage === 'memory') {
          accumulator.memory += 1;
        } else if (mapping.sourceOfTruth === 'global_pattern') {
          accumulator.globalPatterns += 1;
        } else {
          accumulator.system += 1;
        }
        return accumulator;
      },
      { staging: 0, memory: 0, globalPatterns: 0, system: 0 }
    );

    const drafts = draftSuggestions.reduce(
      (accumulator, suggestion) => {
        const stage = draftSuggestionStageState(suggestion, draftJsonSaved);
        if (stage === 'staging') {
          accumulator.staging += 1;
        } else if (stage === 'memory') {
          accumulator.memory += 1;
        } else if (suggestion.sourceOfTruth === 'global_pattern') {
          accumulator.globalPatterns += 1;
        } else {
          accumulator.system += 1;
        }
        return accumulator;
      },
      { staging: 0, memory: 0, globalPatterns: 0, system: 0 }
    );

    return {
      staging: mappings.staging + drafts.staging,
      memory: mappings.memory + drafts.memory,
      globalPatterns: mappings.globalPatterns + drafts.globalPatterns,
      system: mappings.system + drafts.system,
    };
  }, [draftJsonSaved, draftSuggestions, generationConfirmed, result.mappings]);

  const learningPipelineStats = useMemo(() => {
    return {
      staging: (learningSummary?.mappingSuggestions ?? 0) + (learningSummary?.draftJsonSuggestions ?? 0),
      memory:
        (learningSummary?.mappingMemory ?? 0) +
        (learningSummary?.fewShotExamples ?? 0) +
        (learningSummary?.userTemplates ?? 0) +
        (learningSummary?.frequentDjson ?? 0),
      globalPatterns: learningSummary?.globalPatternCandidates ?? 0,
      dataset: learningSummary?.globalCuratedDatasetItems ?? 0,
    };
  }, [learningSummary]);

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
          learningReviewNotice,
        ].filter(Boolean)
      )
    );
  }, [correctionSaveError, correctionSaveNotice, draftJsonError, draftJsonNotice, learningReviewError, learningReviewNotice, parsedFile?.warnings, result.warnings, saveMessage]);

  const profileStats = useMemo(() => {
    const totalGenerations = history.length;
    const uniqueFiles = new Set(history.map((item) => item.fileName)).size;
    const totalWarnings = history.reduce((sum, item) => sum + item.warnings.length, 0);
    const lastGeneratedAt = history[0]?.createdAt ?? null;
    return {
      totalGenerations,
      uniqueFiles,
      totalWarnings,
      lastGeneratedAt,
    };
  }, [history]);

  const recentFiles = useMemo(() => {
    const seen = new Set<string>();
    return history
      .filter((item) => {
        if (seen.has(item.fileName)) {
          return false;
        }
        seen.add(item.fileName);
        return true;
      })
      .slice(0, 6)
      .map((item) => ({
        id: item.id,
        fileName: item.fileName,
        createdAt: item.createdAt,
      }));
  }, [history]);

  const frequentSchemas = useMemo(() => {
    const schemaMap = new Map<
      string,
      {
        schema: string;
        count: number;
        lastUsedAt: string;
        sampleFileName: string;
      }
    >();

    history.forEach((item) => {
      const existing = schemaMap.get(item.schema);
      if (existing) {
        existing.count += 1;
        if (new Date(item.createdAt).getTime() > new Date(existing.lastUsedAt).getTime()) {
          existing.lastUsedAt = item.createdAt;
          existing.sampleFileName = item.fileName;
        }
        return;
      }

      schemaMap.set(item.schema, {
        schema: item.schema,
        count: 1,
        lastUsedAt: item.createdAt,
        sampleFileName: item.fileName,
      });
    });

    return Array.from(schemaMap.values())
      .sort((a, b) => {
        if (b.count !== a.count) {
          return b.count - a.count;
        }
        return new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime();
      })
      .slice(0, 5)
      .map((item) => {
        let label = 'JSON шаблон';
        try {
          const parsed = JSON.parse(item.schema) as Record<string, unknown>;
          const keys = Object.keys(parsed);
          if (keys.length > 0) {
            label = keys.slice(0, 3).join(', ');
            if (keys.length > 3) {
              label += ` +${keys.length - 3}`;
            }
          }
        } catch {
          label = item.sampleFileName;
        }

        return {
          ...item,
          label,
        };
      });
  }, [history]);

  const semanticGraphHighlights = useMemo(() => learningMemory?.layers.semanticGraph.items ?? [], [learningMemory]);
  const semanticGraphClusters = useMemo(() => learningMemory?.layers.semanticGraph.clusters ?? [], [learningMemory]);
  const personalMemoryHighlights = useMemo(() => learningMemory?.layers.personalMemory.items ?? [], [learningMemory]);
  const globalKnowledgeHighlights = useMemo(() => learningMemory?.layers.globalKnowledge.items ?? [], [learningMemory]);

  const restoreHistoryItem = (item: HistoryItem) => {
    setHistoryDeleteError('');
    setActiveHistoryId(item.id);
    setSelectedFile(null);
    setParsedFile(item.parsedFile ?? null);
    setSchema(item.schema);
    setAutoGenerateSectionKey(null);
    const restoredResult: GenerationResult = {
      generationId: item.id,
      code: item.code,
      mappings: item.mappings,
      preview: item.preview,
      warnings: item.warnings,
      parsedFile: item.parsedFile ?? null,
    };
    setResult(restoredResult);
    setActivePreviewSheet(item.selectedSheet ?? item.parsedFile?.sheets[0]?.name ?? null);
    setSectionStateCache({});
    setCorrectionBaseline(profile.skipped ? null : buildCorrectionBaseline(item.id, item.schema, restoredResult));
    setCorrectionSaveNotice('');
    setCorrectionSaveError('');
    setDraftSuggestions([]);
    setMappingReviewNotes({});
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setDraftJsonSaved(false);
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
    setHistoryDeleteTarget(item);
  };

  const closeHistoryDeleteModal = () => {
    if (historyDeleteBusyId !== null) {
      return;
    }
    setHistoryDeleteError('');
    setHistoryDeleteTarget(null);
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
    setDisplayName(profile.name);
  }, [profile.name]);

  useEffect(() => {
    let cancelled = false;

    async function loadLearningData() {
      if (profile.skipped) {
        if (!cancelled) {
          setLearningSummary(null);
          setLearningSummaryError('');
          setLearningMemory(null);
        }
        return;
      }

      try {
        const [nextSummary, nextEvents, nextMemory] = await Promise.all([
          fetchLearningSummary(profile.id),
          fetchLearningEvents(profile.id, 18),
          fetchLearningMemory(profile.id, 14),
        ]);
        if (!cancelled) {
          setLearningSummary(nextSummary);
          setLearningSummaryError('');
          setLearningEvents(nextEvents);
          setLearningMemory(nextMemory);
          setLearningEventsError('');
        }
      } catch (error) {
        if (!cancelled) {
          const message = error instanceof Error ? error.message : 'Не удалось загрузить данные обучения.';
          setLearningSummaryError(message);
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
        await refreshLearningSummary();
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
    if (parsed.extension === 'pdf' || parsed.extension === 'docx') {
      try {
        parsed = await fetchSourcePreviewFromBackend(file);
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Не удалось прочитать таблицы документа на backend.';
        parsed = {
          ...parsed,
          warnings: [...parsed.warnings, message],
        };
      }
    }
    setParsedFile(parsed);
    setActivePreviewSheet(parsed.sheets[0]?.name ?? null);
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
    setDraftJsonSaved(false);
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
      setResult({ code: defaultCode, mappings: [], preview: [], warnings: [] });
      setDraftSuggestions([]);
      setDraftJsonSaved(false);
      setGenerationConfirmed(false);
      setCorrectionBaseline(null);
      setMappingReviewNotes({});
      setDraftReviewNotes({});
      setDraftJsonNotice('');
      setDraftJsonError('');
      setLearningReviewNotice('');
      setLearningReviewError('');
      return;
    }

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

    setBusy(true);
    setDraftSuggestions([]);
    setMappingReviewNotes({});
    setDraftReviewNotes({});
    setDraftJsonNotice('');
    setDraftJsonError('');
    setDraftJsonSaved(false);
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
        await refreshLearningSummary();
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

    setDraftJsonBusy(true);
    setDraftJsonError('');
    setDraftJsonNotice('');

    try {
      const generated = await generateDraftJsonFromBackend({
        file: selectedFile,
        userId: profile.skipped ? undefined : profile.id,
        selectedSheet: parsedFile?.sheets.length ? currentPreviewSheet?.name : undefined,
      });

      setParsedFile(generated.parsedFile ?? parsedFile);
      setDraftSuggestions(cloneDraftSuggestions(generated.fieldSuggestions));
      setDraftReviewNotes({});
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
      setSchema(JSON.stringify(result.draftJson, null, 2));
      setDraftSuggestions((current) =>
        current.map((item) => ({
          ...item,
          status: item.status ?? 'accepted',
        }))
      );
      setDraftJsonSaved(true);
      setDraftJsonNotice(`Draft JSON сохранён. Шаблон "${result.templateName}" добавлен в память.`);
      await refreshLearningSummary();
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
      await refreshLearningSummary();
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
                <input accept=".csv,.xlsx,.xls,.pdf,.docx" hidden onChange={onFileChange} type="file" />
                <Upload size={18} />
                <strong>Загрузить CSV/XLSX/PDF/DOCX</strong>
                <span>{fileSummary}</span>
              </label>

              <div className="field-block">
                <div className="field-caption">Target JSON</div>
                <textarea className="editor-area" onChange={(event) => setSchema(event.target.value)} value={schema} />
              </div>

              <div className="generator-action-row">
                <button className="secondary-btn" disabled={draftJsonBusy || busy || !selectedFile} onClick={onGenerateDraftJson} type="button">
                  <Sparkles size={16} /> {draftJsonBusy ? 'Строим draft JSON...' : 'Draft JSON по таблице'}
                </button>
                {!profile.skipped && draftSuggestions.length > 0 && (
                  <button className="secondary-btn" disabled={draftJsonBusy || hasPendingDraftReview || hasRejectedDraftWithoutReason} onClick={onSaveDraftJsonLearning} type="button">
                    <ShieldCheck size={16} /> {draftJsonBusy ? 'Сохраняем...' : 'Сохранить draft JSON'}
                  </button>
                )}
              </div>

              <button className="primary-btn" disabled={busy} onClick={onGenerate} type="button">
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
            <section className="profile-nav-card sidebar-history-card">
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
                  <div className="pane-header">
                    <FileSpreadsheet size={16} /> Preview файла
                  </div>
                  {previewSheets.length > 1 && (
                    <>
                      <div className="preview-switcher">
                        <button className="icon-btn preview-switch-btn" onClick={() => selectRelativePreviewSheet(-1)} title="Предыдущая таблица" type="button">
                          <ChevronLeft size={16} />
                        </button>
                        <div className="preview-switch-info">
                          <span>{previewSectionLabel(parsedFile?.extension, previewSheets.length)}</span>
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
                  <div className="data-grid-wrap">
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
                      <div className="empty-card">После загрузки тут покажется содержимое файла.</div>
                    )}
                  </div>
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
                    <div className="empty-card compact">Сейчас всё разобрано. Можно сохранять draft JSON и подтверждать генерацию.</div>
                  )}
                  <p className="subtle-text mapping-editor-note">
                    Пока здесь есть элементы, сохранение draft JSON и подтверждение генерации будут ждать вашего решения.
                  </p>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <SquarePen size={16} /> Mapping overrides
                  </div>
                  <div className="mapping-editor-list">
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
                    {draftSuggestions.length === 0 && <div className="empty-card compact">После нажатия «Draft JSON по таблице» здесь появятся названия полей для проверки.</div>}
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
                  <div className="warning-list">
                    {visibleWarnings.map((warning, index) => (
                      <div className="warning-item" key={index}>
                        {warning}
                      </div>
                    ))}
                    {visibleWarnings.length === 0 && <div className="empty-card compact">Пока без предупреждений.</div>}
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Состояние знаний
                  </div>
                  <div className="knowledge-state-grid">
                    <div className="knowledge-state-card">
                      <strong>{currentKnowledgeSummary.staging}</strong>
                      <span>в staging сейчас</span>
                      <small>текущая генерация и draft JSON</small>
                    </div>
                    <div className="knowledge-state-card">
                      <strong>{learningPipelineStats.memory}</strong>
                      <span>в персональной памяти</span>
                      <small>mapping memory, шаблоны и few-shot</small>
                    </div>
                    <div className="knowledge-state-card">
                      <strong>{learningPipelineStats.globalPatterns}</strong>
                      <span>глобальные паттерны</span>
                      <small>устойчивые совпадения между пользователями</small>
                    </div>
                    <div className="knowledge-state-card">
                      <strong>{learningPipelineStats.dataset}</strong>
                      <span>в curated dataset</span>
                      <small>готово для обучения локальной модели</small>
                    </div>
                  </div>
                  <div className="knowledge-breakdown">
                    <div className="knowledge-breakdown-row">
                      <span>Текущий запуск</span>
                      <strong>
                        staging {currentKnowledgeSummary.staging} · память {currentKnowledgeSummary.memory} · паттерны {currentKnowledgeSummary.globalPatterns} · авто {currentKnowledgeSummary.system}
                      </strong>
                    </div>
                    <div className="knowledge-breakdown-row">
                      <span>Серверный staging</span>
                      <strong>
                        {learningSummary?.mappingSuggestions ?? 0} mapping · {learningSummary?.draftJsonSuggestions ?? 0} draft JSON
                      </strong>
                    </div>
                  </div>
                  {learningSummaryError && <div className="warning-item auth-status auth-status-error">{learningSummaryError}</div>}
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
                      <strong>{profileStats.lastGeneratedAt ? new Date(profileStats.lastGeneratedAt).toLocaleDateString() : '—'}</strong>
                      <span>последняя генерация</span>
                    </div>
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <FileSpreadsheet size={16} /> Последние файлы
                  </div>
                  <div className="profile-list">
                    {recentFiles.length === 0 && <div className="empty-card compact">Пока нет загруженных файлов.</div>}
                    {recentFiles.map((item) => (
                      <div className="profile-list-item" key={item.id}>
                        <div>
                          <strong>{item.fileName}</strong>
                          <span>{new Date(item.createdAt).toLocaleString()}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Часто используемые JSON
                  </div>
                  <div className="profile-list">
                    {frequentSchemas.length === 0 && <div className="empty-card compact">Пока нет сохранённых шаблонов.</div>}
                    {frequentSchemas.map((item) => (
                      <button
                        className="profile-list-item profile-list-item-button"
                        key={`${item.label}-${item.lastUsedAt}`}
                        onClick={() => {
                          setSchema(item.schema);
                          setActiveView('generator');
                        }}
                        type="button"
                      >
                        <div>
                          <strong>{item.label}</strong>
                          <span>Использован {item.count} раз(а) · последний файл {item.sampleFileName}</span>
                        </div>
                      </button>
                    ))}
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Память системы
                  </div>
                  <div className="memory-layer-grid">
                    <div className="memory-layer-card">
                      <div className="memory-layer-top">
                        <strong>Личная память</strong>
                        <span>{learningMemory?.layers.personalMemory.counts.entries ?? 0}</span>
                      </div>
                      <div className="memory-layer-stats">
                        <span>accept {learningMemory?.layers.personalMemory.counts.accepted ?? 0}</span>
                        <span>reject {learningMemory?.layers.personalMemory.counts.rejected ?? 0}</span>
                      </div>
                      <div className="memory-layer-list">
                        {personalMemoryHighlights.length === 0 && (
                          <div className="empty-card compact">Подтверждённые пользовательские пары пока не накопились.</div>
                        )}
                        {personalMemoryHighlights.slice(0, 5).map((item) => (
                          <div className="memory-layer-item" key={`${item.sourceFieldNorm}-${item.targetFieldNorm}`}>
                            <div className="memory-layer-item-top">
                              <strong>{item.sourceField ?? item.sourceFieldNorm} → {item.targetField}</strong>
                              <span className="memory-band-chip">{item.confidenceBand}</span>
                            </div>
                            <div className="memory-layer-meta">
                              <span>usage {item.usageCount}</span>
                              <span>accept {item.acceptedCount}</span>
                              <span>reject {item.rejectedCount}</span>
                              <span>schemas {item.schemaFingerprintCount}</span>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div className="memory-layer-card">
                      <div className="memory-layer-top">
                        <strong>Общие паттерны</strong>
                        <span>{learningMemory?.layers.globalKnowledge.counts.patterns ?? 0}</span>
                      </div>
                      <div className="memory-layer-stats">
                        <span>promoted {learningMemory?.layers.globalKnowledge.counts.shared_promoted ?? 0}</span>
                        <span>candidate {learningMemory?.layers.globalKnowledge.counts.shared_candidate ?? 0}</span>
                        <span>blocked {learningMemory?.layers.globalKnowledge.counts.blocked_sensitive ?? 0}</span>
                      </div>
                      <div className="memory-layer-list">
                        {globalKnowledgeHighlights.length === 0 && (
                          <div className="empty-card compact">Общие паттерны ещё не сформированы.</div>
                        )}
                        {globalKnowledgeHighlights.slice(0, 6).map((item) => (
                          <div className="memory-layer-item memory-layer-item-global" key={`${item.candidateId}-${item.targetFieldNorm}`}>
                            <div className="memory-layer-item-top">
                              <strong>{item.sourceField ?? item.sourceFieldNorm} → {item.targetField ?? item.targetFieldNorm}</strong>
                              <span className={`memory-status-chip memory-status-${memoryPatternStatusTone(item.status)}`}>
                                {memoryPatternStatusLabel(item.status)}
                              </span>
                            </div>
                            <div className="memory-layer-tags">
                              {item.semanticRole && <span className="memory-tag">{item.semanticRole}</span>}
                              {item.conceptCluster && <span className="memory-tag memory-tag-accent">{item.conceptCluster}</span>}
                              {item.domainTags.slice(0, 3).map((tag) => (
                                <span className="memory-tag memory-tag-muted" key={`${item.candidateId}-${tag}`}>
                                  {tag}
                                </span>
                              ))}
                            </div>
                            <div className="memory-layer-metrics">
                              <span>support {item.supportCount}</span>
                              <span>users {item.uniqueUsers}</span>
                              <span>accept rate {formatMetricScore(item.acceptanceRate)}</span>
                              <span>drift {formatMetricScore(item.driftScore)}</span>
                            </div>
                            <div className="memory-layer-metrics">
                              <span>sensitivity {formatMetricScore(item.sensitivityScore)}</span>
                              <span>generalizability {formatMetricScore(item.generalizabilityScore)}</span>
                              <span>conflict {formatMetricScore(item.semanticConflictRate)}</span>
                            </div>
                            {(item.promotionReason || item.rejectionReason) && (
                              <small className="memory-layer-reason">
                                {item.promotionReason
                                  ? `promotion: ${item.promotionReason}`
                                  : `reason: ${item.rejectionReason}`}
                              </small>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                </section>

                <section className="insight-card">
                  <div className="pane-header">
                    <Sparkles size={16} /> Семантический граф
                  </div>
                  <div className="semantic-graph-stats">
                    <div className="semantic-graph-stat">
                      <strong>{learningMemory?.layers.semanticGraph.counts.nodes ?? 0}</strong>
                      <span>узлов</span>
                    </div>
                    <div className="semantic-graph-stat">
                      <strong>{learningMemory?.layers.semanticGraph.counts.edges ?? 0}</strong>
                      <span>рёбер</span>
                    </div>
                    <div className="semantic-graph-stat">
                      <strong>{learningMemory?.layers.semanticGraph.counts.accepted ?? 0}</strong>
                      <span>accepted</span>
                    </div>
                    <div className="semantic-graph-stat">
                      <strong>{learningMemory?.layers.semanticGraph.counts.rejected ?? 0}</strong>
                      <span>rejected</span>
                    </div>
                  </div>
                  <div className="profile-list semantic-graph-list">
                    {semanticGraphClusters.length > 0 && (
                      <div className="semantic-cluster-list">
                        {semanticGraphClusters.map((cluster) => (
                          <div className="semantic-cluster-card" key={cluster.clusterId}>
                            <div className="semantic-cluster-top">
                              <strong>Кластер из {cluster.size} полей</strong>
                              <span>support {cluster.supportCount}</span>
                            </div>
                            <div className="semantic-cluster-tags">
                              {cluster.sharedAttributes.map((attribute) => (
                                <span className="semantic-cluster-tag" key={`${cluster.clusterId}-${attribute}`}>
                                  {attribute}
                                </span>
                              ))}
                              {cluster.sharedRoles.map((role) => (
                                <span className="semantic-cluster-tag semantic-cluster-tag-role" key={`${cluster.clusterId}-${role}`}>
                                  {semanticGraphRoleLabel(role)}
                                </span>
                              ))}
                              {cluster.entities.map((entity) => (
                                <span className="semantic-cluster-tag semantic-cluster-tag-entity" key={`${cluster.clusterId}-${entity}`}>
                                  {entity}
                                </span>
                              ))}
                            </div>
                            <div className="semantic-cluster-fields">
                              {cluster.fields.map((field) => (
                                <div className="semantic-cluster-field" key={`${cluster.clusterId}-${field.fieldNorm}`}>
                                  <strong>{field.field}</strong>
                                  <span>
                                    {field.entityToken ?? 'any'}.{field.attributeToken ?? 'field'} · {semanticGraphRoleLabel(field.roleLabel)}
                                  </span>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                    {semanticGraphHighlights.length === 0 && (
                      <div className="empty-card compact">
                        Пока граф пуст. Он начнёт расти после подтверждённых и отклонённых сопоставлений.
                      </div>
                    )}
                    {semanticGraphHighlights.map((edge) => (
                      <div className="profile-list-item semantic-graph-item" key={`${edge.leftFieldNorm}-${edge.rightFieldNorm}-${edge.relationKind}`}>
                        <div className="semantic-graph-main">
                          <div className="semantic-graph-top">
                            <strong>
                              {edge.leftField} ↔ {edge.rightField}
                            </strong>
                            <span className={`semantic-graph-kind semantic-graph-kind-${edge.relationKind}`}>
                              {semanticGraphRelationLabel(edge.relationKind)}
                            </span>
                          </div>
                          <div className="semantic-graph-meta-row">
                            <span>
                              {edge.leftEntityToken ?? 'any'}.{edge.leftAttributeToken ?? 'field'} · {semanticGraphRoleLabel(edge.leftRoleLabel)}
                            </span>
                            <span>
                              {edge.rightEntityToken ?? 'any'}.{edge.rightAttributeToken ?? 'field'} · {semanticGraphRoleLabel(edge.rightRoleLabel)}
                            </span>
                          </div>
                          <div className="semantic-graph-meta-row semantic-graph-metrics">
                            <span>support {edge.supportCount}</span>
                            <span>accepted {edge.acceptedCount}</span>
                            <span>rejected {edge.rejectedCount}</span>
                            <span>{edge.confidenceBand}</span>
                          </div>
                        </div>
                        <small>{edge.lastSeenAt ? new Date(edge.lastSeenAt).toLocaleString() : '—'}</small>
                      </div>
                    ))}
                  </div>
                  <p className="subtle-text mapping-editor-note">
                    Граф хранит не целые кейсы, а агрегированные semantic edges между полями с контекстом `entity + attribute + role`.
                  </p>
                </section>

                <section className="insight-card profile-history-card">
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
                  <div className="profile-list">
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
