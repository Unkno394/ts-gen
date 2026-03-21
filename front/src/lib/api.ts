import type {
  CorrectionSessionResult,
  DraftJsonFeedbackResult,
  DraftJsonResult,
  FormRepairAction,
  FormExplainability,
  GenerationConfirmationResult,
  GenerationResult,
  HistoryItem,
  LearningEvent,
  LearningMemory,
  LearningSummary,
  ManualCorrectionInput,
  MappingFeedbackResult,
  RepairApplyResult,
  RepairPreviewResult,
  UserProfile,
} from '../types';

type GenerateParams = {
  file: File;
  targetJson: string;
  userId?: string;
  selectedSheet?: string;
};

type SourcePreviewRefreshLogParams = {
  fileName?: string | null;
  selectedSheet?: string | null;
  result: 'changed' | 'unchanged' | 'initial' | 'error';
  activeSheetChanged: boolean;
  structureChanged: boolean;
  previousSheetName?: string | null;
  nextSheetName?: string | null;
  previousSheetCount: number;
  nextSheetCount: number;
  previousColumnCount: number;
  nextColumnCount: number;
  previousRowCount: number;
  nextRowCount: number;
  details: string[];
  message?: string | null;
};

type SourceStructureRefreshParams = {
  file: File;
  targetJson?: string;
  selectedSheet?: string | null;
};

type BackendParsedFile = {
  file_name: string;
  file_type: string;
  columns: string[];
  rows: Record<string, unknown>[];
  content_type?: 'table' | 'form' | 'text' | 'image_like' | 'mixed' | 'unknown';
  document_mode?: 'data_table_mode' | 'form_layout_mode';
  extraction_status?: string;
  raw_text?: string;
  text_blocks?: Array<{
    id: string;
    kind: 'paragraph' | 'line';
    text: string;
    label?: string | null;
  }>;
  sections?: Array<{
    title: string;
    text: string;
  }>;
  kv_pairs?: Array<{
    label: string;
    value: string;
    confidence: 'high' | 'medium' | 'low';
    source_text?: string | null;
  }>;
  source_candidates?: Array<{
    candidate_type: 'table_column' | 'kv_pair' | 'text_fact' | 'text_section';
    label: string;
    value?: unknown;
    sample_values?: unknown[];
    source_text?: string | null;
    section_title?: string | null;
  }>;
  sheets: Array<{
    name: string;
    columns: string[];
    rows: Record<string, unknown>[];
  }>;
  form_model?: {
    scalars?: Record<string, unknown>[];
    groups?: Record<string, unknown>[];
    section_hierarchy?: Record<string, unknown>[];
    layout_lines?: Record<string, unknown>[];
    layout_meta?: Record<string, unknown>;
    resolved_fields?: Record<string, unknown>[];
  } | null;
  warnings: string[];
};

type BackendGenerateResponse = {
  generation_id: string | null;
  schema_fingerprint_id?: number | null;
  mode: 'guest' | 'authorized';
  parsed_file: BackendParsedFile;
  form_explainability?: BackendFormExplainability | null;
  mappings: Array<{
    source: string | null;
    target: string;
    confidence: 'high' | 'medium' | 'low' | 'none';
    reason: string;
    status?: 'suggested' | 'accepted' | 'rejected';
    source_of_truth?: 'deterministic_rule' | 'personal_memory' | 'model_suggestion' | 'global_pattern' | 'semantic_graph' | 'position_fallback' | 'unresolved';
    suggestion_id?: number | null;
    schema_fingerprint_id?: number | null;
  }>;
  generated_typescript: string;
  preview: Record<string, unknown>[];
  warnings: string[];
  target_schema?: Record<string, unknown> | unknown[] | null;
  required_fields?: string[];
  ts_valid?: boolean;
  ts_diagnostics?: Array<{
    path?: string;
    code?: string;
    expected?: string;
    actual?: string;
    message: string;
    source?: string;
  }>;
  preview_diagnostics?: Array<{
    path?: string;
    code?: string;
    expected?: string;
    actual?: string;
    message: string;
    source?: string;
  }>;
  mapping_operational_status?: {
    status: 'high' | 'medium' | 'low';
    resolved_count: number;
    unresolved_count: number;
    resolved_ratio: number;
    review_ratio: number;
    stats: Record<string, number>;
  };
  mapping_quality?: {
    status: 'high' | 'medium' | 'low';
    resolved_count: number;
    unresolved_count: number;
    resolved_ratio: number;
    review_ratio: number;
    stats: Record<string, number>;
  };
  mapping_eval_metrics?: {
    available: boolean;
    exact_match_rate?: number | null;
    false_positive_rate?: number | null;
    unresolved_rate?: number | null;
    accepted_after_review_rate?: number | null;
    notes?: string | null;
  } | null;
  ts_syntax_valid?: boolean;
  ts_runtime_preview_valid?: boolean;
  output_schema_valid?: boolean;
};

type BackendSourcePreviewResponse = {
  parsed_file: BackendParsedFile;
  form_explainability?: BackendFormExplainability | null;
};

type BackendSourcePreviewLogResponse = {
  logged: boolean;
  result: 'changed' | 'unchanged' | 'initial' | 'error';
  user_id?: string | null;
};

type BackendHistoryResponse = {
  items: Array<{
    id: string;
    user_id: string;
    file_name: string;
    file_type: string;
    selected_sheet: string | null;
    parsed_file: BackendParsedFile | null;
    target_json: Record<string, unknown>;
    mappings: Array<{
      source: string | null;
      target: string;
      confidence: 'high' | 'medium' | 'low' | 'none';
      reason: string;
      status?: 'suggested' | 'accepted' | 'rejected';
      source_of_truth?: 'deterministic_rule' | 'personal_memory' | 'model_suggestion' | 'global_pattern' | 'semantic_graph' | 'position_fallback' | 'unresolved';
      suggestion_id?: number | null;
      schema_fingerprint_id?: number | null;
    }>;
    generated_typescript: string;
    preview: Record<string, unknown>[];
    warnings: string[];
    validation?: {
      target_schema?: Record<string, unknown> | unknown[] | null;
      target_schema_summary?: {
        root_type?: string;
        required_fields?: string[];
        field_count?: number;
        root_is_array?: boolean;
      } | null;
      ts_validation?: {
        valid?: boolean;
        compiler_available?: boolean;
        diagnostics?: Array<{
          path?: string;
          code?: string;
          expected?: string;
          actual?: string;
          message: string;
          source?: string;
        }>;
      } | null;
      preview_validation?: {
        runtime_valid?: boolean;
        schema_valid?: boolean;
        diagnostics?: Array<{
          path?: string;
          code?: string;
          expected?: string;
          actual?: string;
          message: string;
          source?: string;
        }>;
        validated_rows?: number;
      } | null;
      quality_summary?: {
        operational_mapping_status?: {
          status: 'high' | 'medium' | 'low';
          resolved_count: number;
          unresolved_count: number;
          resolved_ratio: number;
          review_ratio: number;
          stats: Record<string, number>;
        } | null;
        mapping_quality?: {
          status: 'high' | 'medium' | 'low';
          resolved_count: number;
          unresolved_count: number;
          resolved_ratio: number;
          review_ratio: number;
          stats: Record<string, number>;
        } | null;
        true_quality_metrics?: {
          available: boolean;
          exact_match_rate?: number | null;
          false_positive_rate?: number | null;
          unresolved_rate?: number | null;
          accepted_after_review_rate?: number | null;
          notes?: string | null;
        } | null;
        ts_syntax_valid?: boolean;
        ts_runtime_preview_valid?: boolean;
        output_schema_valid?: boolean;
      } | null;
    } | null;
    created_at: string;
  }>;
};

type BackendDeleteHistoryResponse = {
  deleted: boolean;
  generation_id: number;
  deleted_files: number;
};

type BackendLearningSummaryResponse = {
  user_id: string;
  uploads: number;
  schema_fingerprints: number;
  mapping_suggestions: number;
  draft_json_suggestions: number;
  mapping_memory: number;
  few_shot_examples: number;
  user_templates: number;
  correction_sessions: number;
  user_corrections: number;
  frequent_djson: number;
  global_pattern_candidates: number;
  global_curated_dataset_items: number;
};

type BackendLearningEventsResponse = {
  items: Array<{
    id: string;
    kind: 'feedback_session' | 'few_shot_example' | 'user_template' | 'draft_memory' | 'dataset_item' | 'global_pattern';
    stage: 'staging' | 'memory' | 'global_pattern' | 'dataset';
    title: string;
    description: string;
    created_at: string;
    metadata?: Record<string, unknown>;
  }>;
};

type BackendLearningMemoryResponse = {
  user_id: string;
  layers: {
    staging: {
      counts: {
        pending: number;
        rejected: number;
        total: number;
      };
      items: Array<{
        source_field?: string | null;
        source_field_norm?: string | null;
        target_field: string;
        target_field_norm: string;
        status: 'suggested' | 'rejected';
        source_of_truth: string;
        seen_count: number;
        average_confidence?: number | null;
        confidence_band: 'high' | 'medium' | 'low' | 'none';
        last_seen_at?: string | null;
      }>;
    };
    personal_memory: {
      counts: {
        entries: number;
        accepted: number;
        rejected: number;
      };
      items: Array<{
        source_field?: string | null;
        source_field_norm?: string | null;
        target_field: string;
        target_field_norm: string;
        accepted_count: number;
        rejected_count: number;
        usage_count: number;
        schema_fingerprint_count: number;
        row_count: number;
        average_confidence?: number | null;
        confidence_band: 'high' | 'medium' | 'low' | 'none';
        source_of_truths: string[];
        last_seen_at?: string | null;
      }>;
    };
    global_knowledge: {
      counts: {
        patterns: number;
        promoted: number;
        accepted: number;
        reviewing: number;
        personal_only: number;
        shared_candidate: number;
        shared_promoted: number;
        blocked_sensitive: number;
      };
      items: Array<{
        candidate_id: number;
        source_field?: string | null;
        source_field_norm: string;
        target_field?: string | null;
        target_field_norm: string;
        status: 'personal_only' | 'shared_candidate' | 'shared_promoted' | 'blocked_sensitive';
        semantic_role?: string | null;
        concept_cluster?: string | null;
        domain_tags?: string[] | null;
        sensitivity_score?: number | null;
        generalizability_score?: number | null;
        support_count: number;
        unique_users: number;
        accepted_count: number;
        rejected_count: number;
        acceptance_rate?: number | null;
        stability_score?: number | null;
        drift_score?: number | null;
        semantic_conflict_rate?: number | null;
        average_confidence?: number | null;
        confidence_band: 'high' | 'medium' | 'low' | 'none';
        promotion_reason?: string | null;
        rejection_reason?: string | null;
        last_seen_at?: string | null;
      }>;
    };
    semantic_graph: {
      counts: {
        nodes: number;
        edges: number;
        accepted: number;
        rejected: number;
      };
      items: Array<{
        left_field: string;
        left_field_norm: string;
        left_entity_token?: string | null;
        left_attribute_token?: string | null;
        left_role_label?: string | null;
        right_field: string;
        right_field_norm: string;
        right_entity_token?: string | null;
        right_attribute_token?: string | null;
        right_role_label?: string | null;
        relation_kind: 'mapping_synonym' | 'semantic_conflict';
        accepted_count: number;
        rejected_count: number;
        support_count: number;
        average_confidence?: number | null;
        confidence_band: 'high' | 'medium' | 'low' | 'none';
        last_outcome?: 'accepted' | 'rejected' | null;
        source_of_truth?: string | null;
        last_seen_at?: string | null;
      }>;
      clusters: Array<{
        cluster_id: string;
        size: number;
        support_count: number;
        shared_attributes: string[];
        shared_roles: string[];
        entities: string[];
        fields: Array<{
          field: string;
          field_norm: string;
          entity_token?: string | null;
          attribute_token?: string | null;
          role_label?: string | null;
        }>;
        edges: Array<{
          left_field_norm: string;
          right_field_norm: string;
          support_count: number;
        }>;
      }>;
    };
  };
};

function normalizeParsedFile(payload: BackendParsedFile) {
  return {
    fileName: payload.file_name,
    extension: payload.file_type,
    columns: payload.columns,
    rows: payload.rows,
    contentType: payload.content_type ?? 'unknown',
    documentMode: payload.document_mode ?? 'data_table_mode',
    extractionStatus: payload.extraction_status ?? 'unknown',
    rawText: payload.raw_text ?? '',
    textBlocks: (payload.text_blocks ?? []).map((block) => ({
      id: block.id,
      kind: block.kind,
      text: block.text,
      label: block.label ?? null,
    })),
    sections: (payload.sections ?? []).map((section) => ({
      title: section.title,
      text: section.text,
    })),
    kvPairs: (payload.kv_pairs ?? []).map((pair) => ({
      label: pair.label,
      value: pair.value,
      confidence: pair.confidence,
      sourceText: pair.source_text ?? null,
    })),
    sourceCandidates: (payload.source_candidates ?? []).map((candidate) => ({
      candidateType: candidate.candidate_type,
      label: candidate.label,
      value: candidate.value,
      sampleValues: candidate.sample_values ?? [],
      sourceText: candidate.source_text ?? null,
      sectionTitle: candidate.section_title ?? null,
    })),
    sheets: payload.sheets ?? [],
    formModel: payload.form_model
      ? {
          scalars: payload.form_model.scalars ?? [],
          groups: payload.form_model.groups ?? [],
          sectionHierarchy: payload.form_model.section_hierarchy ?? [],
          layoutLines: payload.form_model.layout_lines ?? [],
          layoutMeta: payload.form_model.layout_meta ?? {},
          resolvedFields: payload.form_model.resolved_fields ?? [],
        }
      : null,
    warnings: payload.warnings,
  };
}

function serializeParsedFile(payload: NonNullable<GenerationResult['parsedFile']>): BackendParsedFile {
  return {
    file_name: payload.fileName,
    file_type: payload.extension,
    columns: payload.columns,
    rows: payload.rows,
    content_type: payload.contentType,
    document_mode: payload.documentMode ?? 'data_table_mode',
    extraction_status: payload.extractionStatus,
    raw_text: payload.rawText,
    text_blocks: payload.textBlocks.map((block) => ({
      id: block.id,
      kind: block.kind,
      text: block.text,
      label: block.label ?? null,
    })),
    sections: payload.sections.map((section) => ({
      title: section.title,
      text: section.text,
    })),
    kv_pairs: payload.kvPairs.map((pair) => ({
      label: pair.label,
      value: pair.value,
      confidence: pair.confidence,
      source_text: pair.sourceText ?? null,
    })),
    source_candidates: payload.sourceCandidates.map((candidate) => ({
      candidate_type: candidate.candidateType,
      label: candidate.label,
      value: candidate.value,
      sample_values: candidate.sampleValues,
      source_text: candidate.sourceText ?? null,
      section_title: candidate.sectionTitle ?? null,
    })),
    sheets: payload.sheets.map((sheet) => ({
      name: sheet.name,
      columns: sheet.columns,
      rows: sheet.rows,
    })),
    form_model: payload.formModel
      ? {
          scalars: payload.formModel.scalars,
          groups: payload.formModel.groups,
          section_hierarchy: payload.formModel.sectionHierarchy,
          layout_lines: payload.formModel.layoutLines,
          layout_meta: payload.formModel.layoutMeta,
          resolved_fields: payload.formModel.resolvedFields,
        }
      : null,
    warnings: payload.warnings,
  };
}

type BackendCorrectionSessionResponse = {
  session_id: number;
  generation_id: number | null;
  schema_fingerprint_id: number | null;
  correction_ids: number[];
  accepted_count: number;
  count: number;
  reviewed_count?: number;
  rejected_count?: number;
  promotion?: {
    promoted?: boolean;
    already_promoted?: boolean;
    few_shot_example_id?: number | null;
    dataset_item_id?: number | null;
    quality_score?: number | null;
    reason?: string;
  };
};

type BackendGenerationConfirmationResponse = {
  generation_id: number;
  promoted: boolean;
  already_promoted?: boolean;
  few_shot_example_id?: number | null;
  dataset_item_id?: number | null;
  quality_score?: number | null;
  reason?: string;
};

type BackendDraftJsonResponse = {
  schema_fingerprint_id?: number | null;
  parsed_file: BackendGenerateResponse['parsed_file'];
  form_explainability?: BackendFormExplainability | null;
  draft_json: Record<string, unknown>;
  field_suggestions: Array<{
    source_column: string;
    target_field: string;
    default_value: unknown;
    field_type: 'string' | 'number' | 'boolean' | 'object' | 'array' | 'null' | 'any';
    status?: 'suggested' | 'accepted' | 'rejected';
    source_of_truth?: 'heuristic_fallback' | 'personal_memory' | 'model_suggestion' | 'global_pattern';
    confidence?: 'high' | 'medium' | 'low' | 'none';
    reason?: string;
    suggestion_id?: number | null;
    schema_fingerprint_id?: number | null;
  }>;
  warnings: string[];
};

type BackendFormExplainability = {
  document_mode: string;
  final_source_mode?: string | null;
  layout_meta?: Record<string, unknown>;
  quality_summary?: {
    needs_attention?: boolean;
    repair_recommended?: boolean;
    resolved_field_count?: number;
    target_field_count?: number;
    ambiguous_fields?: string[];
    unresolved_fields?: string[];
    unresolved_critical_fields?: string[];
    repair_fields?: string[];
    blocked_fields?: string[];
    multiple_selected_single_choice_groups?: string[];
    red_flags?: Array<{
      code: string;
      message?: string;
      fields?: string[];
      groups?: string[];
      resolved_field_count?: number;
      target_field_count?: number;
    }>;
  } | null;
  repair_plan?: {
    recommended: boolean;
    trigger_stage: 'generic_form_understanding' | 'business_mapping';
    strategy: string;
    llm_policy: string;
    requested_target_fields?: string[];
    red_flag_codes?: string[];
    targeted_chunk_count?: number;
    actions?: Array<{
      kind: string;
      priority: 'high' | 'medium' | 'low';
      reason: string;
      target_field?: string;
      group_id?: string;
      fields?: string[];
      llm_scope?: string;
      chunk_refs?: {
        group_ids?: string[];
        scalar_labels?: string[];
        line_ids?: string[];
      };
    }>;
  } | null;
  resolved_fields?: Array<{
    field: string;
    status: 'resolved' | 'weak_match' | 'ambiguous' | 'not_found';
    resolved_by: 'form_resolver' | 'repair_model' | 'repair_apply' | 'legacy_fallback' | 'fallback_blocked' | 'unresolved';
    value?: unknown;
    candidates?: unknown[];
    source_ref?: Record<string, unknown>;
    confidence?: number | null;
  }>;
  scalar_count?: number;
  group_count?: number;
  section_count?: number;
  layout_line_count?: number;
  repair_fields?: string[];
};

type BackendRepairAction = NonNullable<NonNullable<BackendFormExplainability['repair_plan']>['actions']>[number];

type BackendRepairPreviewResponse = {
  supported: boolean;
  preview_status: 'patch_available' | 'inspection_only' | 'ambiguous' | 'no_patch';
  action: BackendRepairAction;
  target_fields: Array<{
    name: string;
    type: string;
  }>;
  local_chunks: {
    groups: Record<string, unknown>[];
    scalars: Record<string, unknown>[];
    lines: Record<string, unknown>[];
  };
  proposed_resolutions: NonNullable<BackendFormExplainability['resolved_fields']>;
  proposed_patch: Record<string, unknown>;
  form_explainability?: BackendFormExplainability | null;
  warnings: string[];
};

type BackendRepairApplyResponse = {
  applied: boolean;
  action: BackendRepairAction;
  approved_patch: Record<string, unknown>;
  parsed_file: BackendParsedFile;
  form_explainability?: BackendFormExplainability | null;
  updated_resolved_fields: NonNullable<BackendFormExplainability['resolved_fields']>;
  persistence: {
    persisted: boolean;
    generation_id?: number | string | null;
    version_id?: number | null;
    version_number?: number | null;
    session_id?: number | null;
  };
};

type BackendDraftJsonFeedbackResponse = {
  schema_fingerprint_id: number;
  draft_json: Record<string, unknown>;
  accepted_count: number;
  rejected_count: number;
  saved_as_template: boolean;
  template_name: string;
};

type AuthParams = {
  email: string;
  password: string;
  name?: string;
  verificationCode?: string;
};

type BackendUserProfile = {
  id: string;
  name: string;
  email: string;
};

type BackendAuthenticatedUserProfile = BackendUserProfile & {
  access_token: string;
  token_type: 'bearer';
};

type BackendSendCodeResponse = {
  message: string;
  expires_in: number;
};

type BackendMessageResponse = {
  message: string;
};

type BackendVerifyResetCodeResponse = {
  message: string;
  reset_token: string;
};

const DEFAULT_BACKEND_URL = 'http://127.0.0.1:8000';
const DEFAULT_REQUEST_TIMEOUT_MS = 15_000;
const AUTH_TOKEN_KEY = 'tsgen.authToken';

let authTokenCache: string | null | undefined;

function readStoredAuthToken(): string | null {
  if (typeof window === 'undefined') {
    return null;
  }
  try {
    return localStorage.getItem(AUTH_TOKEN_KEY);
  } catch {
    return null;
  }
}

function getAuthToken(): string | null {
  if (authTokenCache === undefined) {
    authTokenCache = readStoredAuthToken();
  }
  return authTokenCache ?? null;
}

export function setAuthToken(token: string | null): void {
  authTokenCache = token?.trim() || null;

  if (typeof window === 'undefined') {
    return;
  }

  try {
    if (authTokenCache) {
      localStorage.setItem(AUTH_TOKEN_KEY, authTokenCache);
    } else {
      localStorage.removeItem(AUTH_TOKEN_KEY);
    }
  } catch {
    // Ignore storage failures and keep the in-memory token.
  }
}

export function clearAuthToken(): void {
  setAuthToken(null);
}

function resolveTimeoutMs(envKey: 'VITE_REQUEST_TIMEOUT_MS' | 'VITE_GENERATE_TIMEOUT_MS', fallbackMs: number): number {
  const rawValue = (import.meta.env as ImportMetaEnv & { VITE_REQUEST_TIMEOUT_MS?: string; VITE_GENERATE_TIMEOUT_MS?: string })[
    envKey
  ]?.trim();
  if (!rawValue) {
    return fallbackMs;
  }

  const parsed = Number.parseInt(rawValue, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallbackMs;
  }
  return parsed;
}

const GENERATE_REQUEST_TIMEOUT_MS = resolveTimeoutMs('VITE_GENERATE_TIMEOUT_MS', 180_000);

function normalizeBackendError(detail: string, status?: number): string {
  const normalized = detail.trim();

  if (normalized === 'Invalid email or password.' || normalized === 'Неверный email или пароль.') {
    return 'Неверный email или пароль. Проверьте данные и попробуйте ещё раз.';
  }

  if (normalized === 'Email and password are required.' || normalized === 'Введите email и пароль.') {
    return 'Введите email и пароль.';
  }

  if (normalized === 'User with this email already exists.' || normalized === 'Пользователь с таким email уже зарегистрирован.') {
    return 'Этот email уже зарегистрирован. Попробуйте войти в аккаунт.';
  }

  if (normalized.includes('API key is invalid')) {
    return 'Сервис отправки писем настроен неправильно. Обратитесь к администратору или попробуйте позже.';
  }

  if (normalized.includes('Email transport is not configured') || normalized.includes('SMTP is not configured') || normalized.includes('Resend is not configured')) {
    return 'Сервис отправки писем сейчас не настроен. Попробуйте позже.';
  }

  if (normalized.startsWith('Resend API error')) {
    return 'Не удалось отправить письмо с кодом подтверждения. Попробуйте позже.';
  }

  if (normalized.startsWith('Resend connection error')) {
    return 'Нет соединения с сервисом отправки писем. Попробуйте позже.';
  }

  if (normalized.startsWith('Unexpected error:') || status === 500) {
    return 'Произошла внутренняя ошибка сервера. Попробуйте ещё раз.';
  }

  return normalized;
}

function resolveApiBaseUrl(): string {
  const envBaseUrl = (import.meta.env as ImportMetaEnv & { VITE_BACKEND_URL?: string }).VITE_BACKEND_URL?.trim();
  if (envBaseUrl) {
    return envBaseUrl.replace(/\/+$/, '');
  }

  const electronBaseUrl = window.electronAPI?.backendBaseUrl?.trim();
  if (electronBaseUrl) {
    return electronBaseUrl.replace(/\/+$/, '');
  }

  if (typeof window !== 'undefined' && (window.electronAPI || window.location.protocol === 'file:')) {
    return DEFAULT_BACKEND_URL;
  }

  return '';
}

function buildApiUrl(path: string): string {
  return `${resolveApiBaseUrl()}${path}`;
}

function parseConfidence(value: 'high' | 'medium' | 'low' | 'none'): 'high' | 'medium' | 'low' | 'none' {
  return value;
}

function normalizeOperationalMappingStatus(
  payload:
    | {
        status: 'high' | 'medium' | 'low';
        resolved_count: number;
        unresolved_count: number;
        resolved_ratio: number;
        review_ratio: number;
        stats: Record<string, number>;
      }
    | null
    | undefined
) {
  if (!payload) {
    return null;
  }
  return {
    status: payload.status,
    resolvedCount: payload.resolved_count,
    unresolvedCount: payload.unresolved_count,
    resolvedRatio: payload.resolved_ratio,
    reviewRatio: payload.review_ratio,
    stats: payload.stats,
  };
}

function normalizeTrueQualityMetrics(
  payload:
    | {
        available: boolean;
        exact_match_rate?: number | null;
        false_positive_rate?: number | null;
        unresolved_rate?: number | null;
        accepted_after_review_rate?: number | null;
        notes?: string | null;
      }
    | null
    | undefined
) {
  if (!payload) {
    return null;
  }
  return {
    available: payload.available,
    exactMatchRate: payload.exact_match_rate ?? null,
    falsePositiveRate: payload.false_positive_rate ?? null,
    unresolvedRate: payload.unresolved_rate ?? null,
    acceptedAfterReviewRate: payload.accepted_after_review_rate ?? null,
    notes: payload.notes ?? null,
  };
}

function normalizeFormExplainability(payload: BackendFormExplainability | null | undefined): FormExplainability | null {
  if (!payload) {
    return null;
  }
  return {
    documentMode: payload.document_mode,
    finalSourceMode: payload.final_source_mode ?? null,
    layoutMeta: payload.layout_meta ?? {},
    qualitySummary: {
      needsAttention: payload.quality_summary?.needs_attention ?? false,
      repairRecommended: payload.quality_summary?.repair_recommended ?? false,
      resolvedFieldCount: payload.quality_summary?.resolved_field_count ?? 0,
      targetFieldCount: payload.quality_summary?.target_field_count ?? 0,
      ambiguousFields: payload.quality_summary?.ambiguous_fields ?? [],
      unresolvedFields: payload.quality_summary?.unresolved_fields ?? [],
      unresolvedCriticalFields: payload.quality_summary?.unresolved_critical_fields ?? [],
      repairFields: payload.quality_summary?.repair_fields ?? [],
      blockedFields: payload.quality_summary?.blocked_fields ?? [],
      multipleSelectedSingleChoiceGroups: payload.quality_summary?.multiple_selected_single_choice_groups ?? [],
      redFlags: (payload.quality_summary?.red_flags ?? []).map((flag) => ({
        code: flag.code,
        message: flag.message,
        fields: flag.fields ?? [],
        groups: flag.groups ?? [],
        resolvedFieldCount: flag.resolved_field_count,
        targetFieldCount: flag.target_field_count,
      })),
    },
    repairPlan: {
      recommended: payload.repair_plan?.recommended ?? false,
      triggerStage: payload.repair_plan?.trigger_stage ?? 'business_mapping',
      strategy: payload.repair_plan?.strategy ?? '',
      llmPolicy: payload.repair_plan?.llm_policy ?? '',
      requestedTargetFields: payload.repair_plan?.requested_target_fields ?? [],
      redFlagCodes: payload.repair_plan?.red_flag_codes ?? [],
      actions: (payload.repair_plan?.actions ?? []).map((action) => ({
        kind: action.kind,
        priority: action.priority,
        reason: action.reason,
        targetField: action.target_field,
        groupId: action.group_id,
        fields: action.fields ?? [],
        llmScope: action.llm_scope,
        chunkRefs: {
          groupIds: action.chunk_refs?.group_ids ?? [],
          scalarLabels: action.chunk_refs?.scalar_labels ?? [],
          lineIds: action.chunk_refs?.line_ids ?? [],
        },
      })),
      targetedChunkCount: payload.repair_plan?.targeted_chunk_count ?? 0,
    },
    resolvedFields: (payload.resolved_fields ?? []).map((field) => ({
      field: field.field,
      status: field.status,
      resolvedBy: field.resolved_by,
      value: field.value,
      candidates: field.candidates ?? [],
      sourceRef: field.source_ref ?? {},
      confidence: field.confidence ?? null,
    })),
    scalarCount: payload.scalar_count ?? 0,
    groupCount: payload.group_count ?? 0,
    sectionCount: payload.section_count ?? 0,
    layoutLineCount: payload.layout_line_count ?? 0,
    repairFields: payload.repair_fields ?? [],
  };
}

function normalizeRepairAction(action: BackendRepairAction): FormRepairAction {
  return {
    kind: action.kind,
    priority: action.priority,
    reason: action.reason,
    targetField: action.target_field,
    groupId: action.group_id,
    fields: action.fields ?? [],
    llmScope: action.llm_scope,
    chunkRefs: {
      groupIds: action.chunk_refs?.group_ids ?? [],
      scalarLabels: action.chunk_refs?.scalar_labels ?? [],
      lineIds: action.chunk_refs?.line_ids ?? [],
    },
  };
}

function serializeRepairAction(action: FormRepairAction): Record<string, unknown> {
  return {
    kind: action.kind,
    priority: action.priority,
    reason: action.reason,
    target_field: action.targetField,
    group_id: action.groupId,
    fields: action.fields ?? [],
    llm_scope: action.llmScope,
    chunk_refs: {
      group_ids: action.chunkRefs.groupIds,
      scalar_labels: action.chunkRefs.scalarLabels,
      line_ids: action.chunkRefs.lineIds,
    },
  };
}

async function parseJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const payload = await response.json().catch(() => null);
    const detail = payload && typeof payload.detail === 'string' ? payload.detail : `Request failed: ${response.status}`;
    throw new Error(normalizeBackendError(detail, response.status));
  }
  return response.json() as Promise<T>;
}

async function fetchWithTimeout(input: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), timeoutMs);
  const headers = new Headers(init.headers ?? undefined);
  const authToken = getAuthToken();

  if (authToken && !headers.has('Authorization')) {
    headers.set('Authorization', `Bearer ${authToken}`);
  }

  try {
    return await fetch(input, {
      ...init,
      headers,
      signal: controller.signal,
    });
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      throw new Error(`Сервер не ответил за ${Math.round(timeoutMs / 1000)} сек. Попробуйте ещё раз.`);
    }
    throw new Error('Сервер сейчас недоступен. Проверьте, что backend запущен, и попробуйте ещё раз.');
  } finally {
    window.clearTimeout(timeoutId);
  }
}

async function postJson<T>(path: string, payload: Record<string, unknown>, timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS): Promise<T> {
  const response = await fetchWithTimeout(
    buildApiUrl(path),
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    },
    timeoutMs
  );
  return parseJson<T>(response);
}

export async function registerWithBackend({ email, password, name, verificationCode }: AuthParams): Promise<UserProfile> {
  const response = await postJson<BackendAuthenticatedUserProfile>('/api/auth/register', {
    email,
    password,
    name,
    verification_code: verificationCode,
  });
  setAuthToken(response.access_token);
  return {
    id: response.id,
    name: response.name,
    email: response.email,
  };
}

export async function loginWithBackend({ email, password }: AuthParams): Promise<UserProfile> {
  const response = await postJson<BackendAuthenticatedUserProfile>('/api/auth/login', {
    email,
    password,
  });
  setAuthToken(response.access_token);
  return {
    id: response.id,
    name: response.name,
    email: response.email,
  };
}

export async function fetchCurrentProfile(): Promise<UserProfile> {
  const response = await fetchWithTimeout(
    buildApiUrl('/api/auth/profile'),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendUserProfile>(response);
  return {
    id: payload.id,
    name: payload.name,
    email: payload.email,
  };
}

export async function requestRegistrationCode(email: string): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-code', {
    email,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function requestPasswordResetCode(email: string): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-reset-code', {
    email,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function requestEmailChangeCode(params: { userId: string; newEmail: string }): Promise<{ message: string; expiresIn: number }> {
  const response = await postJson<BackendSendCodeResponse>('/api/auth/send-email-change-code', {
    new_email: params.newEmail,
  });

  return {
    message: response.message,
    expiresIn: response.expires_in,
  };
}

export async function resetPasswordWithBackend(params: { email: string; password: string; verificationCode: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/reset-password', {
    email: params.email,
    password: params.password,
    verification_code: params.verificationCode,
  });
  return response.message;
}

export async function verifyPasswordResetCode(email: string, verificationCode: string): Promise<{ message: string; resetToken: string }> {
  const response = await postJson<BackendVerifyResetCodeResponse>('/api/auth/verify-reset-code', {
    email,
    verification_code: verificationCode,
  });

  return {
    message: response.message,
    resetToken: response.reset_token,
  };
}

export async function completePasswordReset(params: { email: string; password: string; resetToken: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/reset-password', {
    email: params.email,
    password: params.password,
    reset_token: params.resetToken,
  });
  return response.message;
}

export async function changeEmailWithPassword(params: { userId: string; newEmail: string; currentPassword: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/change-email', {
    new_email: params.newEmail,
    current_password: params.currentPassword,
  });
}

export async function changeEmailWithCode(params: { userId: string; newEmail: string; verificationCode: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/change-email', {
    new_email: params.newEmail,
    verification_code: params.verificationCode,
  });
}

export async function updateProfileName(params: { userId: string; name: string }): Promise<UserProfile> {
  return postJson<BackendUserProfile>('/api/auth/update-profile', {
    name: params.name,
  });
}

export async function changePasswordWithBackend(params: { userId: string; currentPassword: string; newPassword: string }): Promise<string> {
  const response = await postJson<BackendMessageResponse>('/api/auth/change-password', {
    current_password: params.currentPassword,
    new_password: params.newPassword,
  });
  return response.message;
}

export async function generateFromBackend({ file, targetJson, userId: _userId, selectedSheet }: GenerateParams): Promise<GenerationResult> {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('target_json', targetJson);
  if (selectedSheet) {
    formData.append('selected_sheet', selectedSheet);
  }

  const response = await fetchWithTimeout(
    buildApiUrl('/api/generate'),
    {
      method: 'POST',
      body: formData,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  const payload = await parseJson<BackendGenerateResponse>(response);
  return {
    generationId: payload.generation_id,
    schemaFingerprintId: payload.schema_fingerprint_id ?? null,
    parsedFile: normalizeParsedFile(payload.parsed_file),
    formExplainability: normalizeFormExplainability(payload.form_explainability),
    code: payload.generated_typescript,
    mappings: payload.mappings.map((item) => ({
      source: item.source ?? 'not found',
      target: item.target,
      confidence: parseConfidence(item.confidence),
      reason: item.reason,
      status: item.status,
      sourceOfTruth: item.source_of_truth,
      suggestionId: item.suggestion_id ?? null,
      schemaFingerprintId: item.schema_fingerprint_id ?? null,
    })),
    preview: payload.preview,
    warnings: payload.warnings,
    targetSchema: payload.target_schema ?? null,
    requiredFields: payload.required_fields ?? [],
    tsValid: payload.ts_valid ?? false,
    tsDiagnostics: payload.ts_diagnostics ?? [],
    previewDiagnostics: payload.preview_diagnostics ?? [],
    mappingOperationalStatus: normalizeOperationalMappingStatus(payload.mapping_operational_status ?? payload.mapping_quality),
    mappingEvalMetrics: normalizeTrueQualityMetrics(payload.mapping_eval_metrics),
    tsSyntaxValid: payload.ts_syntax_valid ?? false,
    tsRuntimePreviewValid: payload.ts_runtime_preview_valid ?? false,
    outputSchemaValid: payload.output_schema_valid ?? false,
  };
}

export async function fetchSourcePreviewFromBackend(file: File) {
  const formData = new FormData();
  formData.append('file', file);

  const response = await fetchWithTimeout(
    buildApiUrl('/api/source-preview'),
    {
      method: 'POST',
      body: formData,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  const payload = await parseJson<BackendSourcePreviewResponse>(response);
  return normalizeParsedFile(payload.parsed_file);
}

export async function refreshSourceStructureFromBackend(params: SourceStructureRefreshParams): Promise<{
  parsedFile: ReturnType<typeof normalizeParsedFile>;
  formExplainability: FormExplainability | null;
}> {
  const formData = new FormData();
  formData.append('file', params.file);
  if (params.targetJson?.trim()) {
    formData.append('target_json', params.targetJson);
  }
  if (params.selectedSheet?.trim()) {
    formData.append('selected_sheet', params.selectedSheet);
  }

  const response = await fetchWithTimeout(
    buildApiUrl('/api/source-preview'),
    {
      method: 'POST',
      body: formData,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  const payload = await parseJson<BackendSourcePreviewResponse>(response);
  return {
    parsedFile: normalizeParsedFile(payload.parsed_file),
    formExplainability: normalizeFormExplainability(payload.form_explainability),
  };
}

export async function logSourcePreviewRefreshToBackend(params: SourcePreviewRefreshLogParams): Promise<void> {
  await postJson<BackendSourcePreviewLogResponse>(
    '/api/source-preview-log',
    {
      file_name: params.fileName ?? null,
      selected_sheet: params.selectedSheet ?? null,
      result: params.result,
      active_sheet_changed: params.activeSheetChanged,
      structure_changed: params.structureChanged,
      previous_sheet_name: params.previousSheetName ?? null,
      next_sheet_name: params.nextSheetName ?? null,
      previous_sheet_count: params.previousSheetCount,
      next_sheet_count: params.nextSheetCount,
      previous_column_count: params.previousColumnCount,
      next_column_count: params.nextColumnCount,
      previous_row_count: params.previousRowCount,
      next_row_count: params.nextRowCount,
      details: params.details,
      message: params.message ?? null,
    },
    6000
  );
}

export async function fetchHistory(_userId: string): Promise<HistoryItem[]> {
  const response = await fetchWithTimeout(
    buildApiUrl('/api/history'),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendHistoryResponse>(response);
  return payload.items.map((item) => ({
    id: item.id,
    createdAt: item.created_at,
    fileName: item.file_name,
    selectedSheet: item.selected_sheet,
    parsedFile: item.parsed_file ? normalizeParsedFile(item.parsed_file) : null,
    schema: JSON.stringify(item.target_json, null, 2),
    code: item.generated_typescript,
    mappings: item.mappings.map((mapping) => ({
      source: mapping.source ?? 'not found',
      target: mapping.target,
      confidence: parseConfidence(mapping.confidence),
      reason: mapping.reason,
      status: mapping.status,
      sourceOfTruth: mapping.source_of_truth,
      suggestionId: mapping.suggestion_id ?? null,
      schemaFingerprintId: mapping.schema_fingerprint_id ?? null,
    })),
    preview: item.preview,
    warnings: item.warnings,
    validation: item.validation
      ? {
          targetSchema: item.validation.target_schema ?? null,
          targetSchemaSummary: item.validation.target_schema_summary
            ? {
                rootType: item.validation.target_schema_summary.root_type,
                requiredFields: item.validation.target_schema_summary.required_fields ?? [],
                fieldCount: item.validation.target_schema_summary.field_count,
                rootIsArray: item.validation.target_schema_summary.root_is_array,
              }
            : null,
          tsValidation: item.validation.ts_validation
            ? {
                valid: item.validation.ts_validation.valid,
                compilerAvailable: item.validation.ts_validation.compiler_available,
                diagnostics: item.validation.ts_validation.diagnostics ?? [],
              }
            : null,
          previewValidation: item.validation.preview_validation
            ? {
                runtimeValid: item.validation.preview_validation.runtime_valid,
                schemaValid: item.validation.preview_validation.schema_valid,
                diagnostics: item.validation.preview_validation.diagnostics ?? [],
                validatedRows: item.validation.preview_validation.validated_rows,
              }
            : null,
          qualitySummary: item.validation.quality_summary
            ? {
                operationalMappingStatus: normalizeOperationalMappingStatus(
                  item.validation.quality_summary.operational_mapping_status ?? item.validation.quality_summary.mapping_quality
                ),
                trueQualityMetrics: normalizeTrueQualityMetrics(item.validation.quality_summary.true_quality_metrics),
                tsSyntaxValid: item.validation.quality_summary.ts_syntax_valid,
                tsRuntimePreviewValid: item.validation.quality_summary.ts_runtime_preview_valid,
                outputSchemaValid: item.validation.quality_summary.output_schema_valid,
              }
            : null,
        }
      : null,
  }));
}

export async function deleteHistoryEntry(_userId: string, generationId: string): Promise<BackendDeleteHistoryResponse> {
  const response = await fetchWithTimeout(
    buildApiUrl(`/api/history/${encodeURIComponent(generationId)}`),
    {
      method: 'DELETE',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  return parseJson<BackendDeleteHistoryResponse>(response);
}

export async function fetchLearningSummary(_userId: string): Promise<LearningSummary> {
  const response = await fetchWithTimeout(
    buildApiUrl('/api/learning/summary'),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendLearningSummaryResponse>(response);
  return {
    userId: payload.user_id,
    uploads: payload.uploads,
    schemaFingerprints: payload.schema_fingerprints,
    mappingSuggestions: payload.mapping_suggestions,
    draftJsonSuggestions: payload.draft_json_suggestions,
    mappingMemory: payload.mapping_memory,
    fewShotExamples: payload.few_shot_examples,
    userTemplates: payload.user_templates,
    correctionSessions: payload.correction_sessions,
    userCorrections: payload.user_corrections,
    frequentDjson: payload.frequent_djson,
    globalPatternCandidates: payload.global_pattern_candidates,
    globalCuratedDatasetItems: payload.global_curated_dataset_items,
  };
}

export async function fetchLearningEvents(_userId: string, limit = 20): Promise<LearningEvent[]> {
  const response = await fetchWithTimeout(
    buildApiUrl(`/api/learning/events?limit=${encodeURIComponent(String(limit))}`),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendLearningEventsResponse>(response);
  return payload.items.map((item) => ({
    id: item.id,
    kind: item.kind,
    stage: item.stage,
    title: item.title,
    description: item.description,
    createdAt: item.created_at,
    metadata: item.metadata ?? {},
  }));
}

export async function fetchLearningMemory(_userId: string, limit = 20): Promise<LearningMemory> {
  const response = await fetchWithTimeout(
    buildApiUrl(`/api/learning/memory?limit=${encodeURIComponent(String(limit))}`),
    {
      method: 'GET',
    },
    DEFAULT_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendLearningMemoryResponse>(response);
  return {
    userId: payload.user_id,
    layers: {
      staging: {
        counts: payload.layers.staging.counts,
        items: payload.layers.staging.items.map((item) => ({
          sourceField: item.source_field ?? null,
          sourceFieldNorm: item.source_field_norm ?? null,
          targetField: item.target_field,
          targetFieldNorm: item.target_field_norm,
          status: item.status,
          sourceOfTruth: item.source_of_truth,
          seenCount: item.seen_count,
          averageConfidence: item.average_confidence ?? null,
          confidenceBand: item.confidence_band,
          lastSeenAt: item.last_seen_at ?? null,
        })),
      },
      personalMemory: {
        counts: payload.layers.personal_memory.counts,
        items: payload.layers.personal_memory.items.map((item) => ({
          sourceField: item.source_field ?? null,
          sourceFieldNorm: item.source_field_norm ?? null,
          targetField: item.target_field,
          targetFieldNorm: item.target_field_norm,
          acceptedCount: item.accepted_count,
          rejectedCount: item.rejected_count,
          usageCount: item.usage_count,
          schemaFingerprintCount: item.schema_fingerprint_count,
          rowCount: item.row_count,
          averageConfidence: item.average_confidence ?? null,
          confidenceBand: item.confidence_band,
          sourceOfTruths: item.source_of_truths,
          lastSeenAt: item.last_seen_at ?? null,
        })),
      },
      globalKnowledge: {
        counts: payload.layers.global_knowledge.counts,
        items: payload.layers.global_knowledge.items.map((item) => ({
          candidateId: item.candidate_id,
          sourceField: item.source_field ?? null,
          sourceFieldNorm: item.source_field_norm,
          targetField: item.target_field ?? null,
          targetFieldNorm: item.target_field_norm,
          status: item.status,
          semanticRole: item.semantic_role ?? null,
          conceptCluster: item.concept_cluster ?? null,
          domainTags: Array.isArray(item.domain_tags) ? item.domain_tags : [],
          sensitivityScore: item.sensitivity_score ?? null,
          generalizabilityScore: item.generalizability_score ?? null,
          supportCount: item.support_count,
          uniqueUsers: item.unique_users,
          acceptedCount: item.accepted_count,
          rejectedCount: item.rejected_count,
          acceptanceRate: item.acceptance_rate ?? null,
          stabilityScore: item.stability_score ?? null,
          driftScore: item.drift_score ?? null,
          semanticConflictRate: item.semantic_conflict_rate ?? null,
          averageConfidence: item.average_confidence ?? null,
          confidenceBand: item.confidence_band,
          promotionReason: item.promotion_reason ?? null,
          rejectionReason: item.rejection_reason ?? null,
          lastSeenAt: item.last_seen_at ?? null,
        })),
      },
      semanticGraph: {
        counts: payload.layers.semantic_graph.counts,
        items: payload.layers.semantic_graph.items.map((item) => ({
          leftField: item.left_field,
          leftFieldNorm: item.left_field_norm,
          leftEntityToken: item.left_entity_token ?? null,
          leftAttributeToken: item.left_attribute_token ?? null,
          leftRoleLabel: item.left_role_label ?? null,
          rightField: item.right_field,
          rightFieldNorm: item.right_field_norm,
          rightEntityToken: item.right_entity_token ?? null,
          rightAttributeToken: item.right_attribute_token ?? null,
          rightRoleLabel: item.right_role_label ?? null,
          relationKind: item.relation_kind,
          acceptedCount: item.accepted_count,
          rejectedCount: item.rejected_count,
          supportCount: item.support_count,
          averageConfidence: item.average_confidence ?? null,
          confidenceBand: item.confidence_band,
          lastOutcome: item.last_outcome ?? null,
          sourceOfTruth: item.source_of_truth ?? null,
          lastSeenAt: item.last_seen_at ?? null,
        })),
        clusters: payload.layers.semantic_graph.clusters.map((cluster) => ({
          clusterId: cluster.cluster_id,
          size: cluster.size,
          supportCount: cluster.support_count,
          sharedAttributes: cluster.shared_attributes,
          sharedRoles: cluster.shared_roles,
          entities: cluster.entities,
          fields: cluster.fields.map((field) => ({
            field: field.field,
            fieldNorm: field.field_norm,
            entityToken: field.entity_token ?? null,
            attributeToken: field.attribute_token ?? null,
            roleLabel: field.role_label ?? null,
          })),
          edges: cluster.edges.map((edge) => ({
            leftFieldNorm: edge.left_field_norm,
            rightFieldNorm: edge.right_field_norm,
            supportCount: edge.support_count,
          })),
        })),
      },
    },
  };
}

export async function saveLearningCorrections(params: {
  userId: string;
  generationId?: number | null;
  sessionType?: 'manual_review' | 'post_generation_fix' | 'template_authoring' | 'feedback_loop';
  notes?: string;
  metadata?: Record<string, unknown>;
  corrections: ManualCorrectionInput[];
}): Promise<CorrectionSessionResult> {
  const response = await postJson<BackendCorrectionSessionResponse>('/api/learning/corrections', {
    generation_id: params.generationId ?? null,
    session_type: params.sessionType ?? 'manual_review',
    notes: params.notes ?? null,
    metadata: params.metadata ?? {},
    corrections: params.corrections.map((correction) => ({
      correction_type: correction.correctionType,
      row_index: correction.rowIndex ?? null,
      field_path: correction.fieldPath ?? null,
      source_field: correction.sourceField ?? null,
      target_field: correction.targetField ?? null,
      original_value: correction.originalValue ?? null,
      corrected_value: correction.correctedValue ?? null,
      correction_payload: correction.correctionPayload ?? null,
      rationale: correction.rationale ?? null,
      confidence_before: correction.confidenceBefore ?? null,
      confidence_after: correction.confidenceAfter ?? null,
      accepted: correction.accepted ?? true,
    })),
  });

  return {
    sessionId: response.session_id,
    generationId: response.generation_id,
    schemaFingerprintId: response.schema_fingerprint_id,
    correctionIds: response.correction_ids,
    acceptedCount: response.accepted_count,
    count: response.count,
  };
}

export async function generateDraftJsonFromBackend(params: {
  file: File;
  userId?: string;
  selectedSheet?: string;
}): Promise<DraftJsonResult> {
  const formData = new FormData();
  formData.append('file', params.file);
  if (params.selectedSheet) {
    formData.append('selected_sheet', params.selectedSheet);
  }

  const response = await fetchWithTimeout(
    buildApiUrl('/api/draft-json'),
    {
      method: 'POST',
      body: formData,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );
  const payload = await parseJson<BackendDraftJsonResponse>(response);
  return {
    schemaFingerprintId: payload.schema_fingerprint_id ?? null,
    parsedFile: normalizeParsedFile(payload.parsed_file),
    formExplainability: normalizeFormExplainability(payload.form_explainability),
    draftJson: payload.draft_json,
    fieldSuggestions: payload.field_suggestions.map((item) => ({
      sourceColumn: item.source_column,
      targetField: item.target_field,
      defaultValue: item.default_value,
      fieldType: item.field_type,
      status: item.status,
      sourceOfTruth: item.source_of_truth,
      confidence: item.confidence,
      reason: item.reason,
      suggestionId: item.suggestion_id ?? null,
      schemaFingerprintId: item.schema_fingerprint_id ?? null,
    })),
    warnings: payload.warnings,
  };
}

export async function fetchRepairPreviewFromBackend(params: {
  parsedFile: NonNullable<GenerationResult['parsedFile']>;
  action: FormRepairAction;
  targetJson?: string | Record<string, unknown> | unknown[] | null;
}): Promise<RepairPreviewResult> {
  const response = await postJson<BackendRepairPreviewResponse>(
    '/api/repair-preview',
    {
      parsed_file: serializeParsedFile(params.parsedFile),
      action: serializeRepairAction(params.action),
      target_json: params.targetJson ?? null,
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  return {
    supported: response.supported,
    previewStatus: response.preview_status,
    action: normalizeRepairAction(response.action),
    targetFields: response.target_fields.map((field) => ({
      name: field.name,
      type: field.type,
    })),
    localChunks: response.local_chunks,
    proposedResolutions: (response.proposed_resolutions ?? []).map((field) => ({
      field: field.field,
      status: field.status,
      resolvedBy: field.resolved_by,
      value: field.value,
      candidates: field.candidates ?? [],
      sourceRef: field.source_ref ?? {},
      confidence: field.confidence ?? null,
    })),
    proposedPatch: response.proposed_patch ?? {},
    formExplainability: normalizeFormExplainability(response.form_explainability),
    warnings: response.warnings ?? [],
  };
}

export async function applyRepairFromBackend(params: {
  parsedFile: NonNullable<GenerationResult['parsedFile']>;
  action: FormRepairAction;
  approvedPatch: Record<string, unknown>;
  targetJson?: string | Record<string, unknown> | unknown[] | null;
  generationId?: number | null;
  notes?: string;
  metadata?: Record<string, unknown>;
}): Promise<RepairApplyResult> {
  const response = await postJson<BackendRepairApplyResponse>(
    '/api/repair-apply',
    {
      parsed_file: serializeParsedFile(params.parsedFile),
      action: serializeRepairAction(params.action),
      approved_patch: params.approvedPatch,
      target_json: params.targetJson ?? null,
      generation_id: params.generationId ?? null,
      notes: params.notes ?? null,
      metadata: params.metadata ?? {},
    },
    GENERATE_REQUEST_TIMEOUT_MS
  );

  return {
    applied: response.applied,
    action: normalizeRepairAction(response.action),
    approvedPatch: response.approved_patch ?? {},
    parsedFile: normalizeParsedFile(response.parsed_file),
    formExplainability: normalizeFormExplainability(response.form_explainability),
    updatedResolvedFields: (response.updated_resolved_fields ?? []).map((field) => ({
      field: field.field,
      status: field.status,
      resolvedBy: field.resolved_by,
      value: field.value,
      candidates: field.candidates ?? [],
      sourceRef: field.source_ref ?? {},
      confidence: field.confidence ?? null,
    })),
    persistence: {
      persisted: response.persistence.persisted,
      generationId: response.persistence.generation_id ?? null,
      versionId: response.persistence.version_id ?? null,
      versionNumber: response.persistence.version_number ?? null,
      sessionId: response.persistence.session_id ?? null,
    },
  };
}

export async function sendMappingFeedback(params: {
  userId: string;
  generationId: number;
  schemaFingerprintId?: number | null;
  notes?: string;
  metadata?: Record<string, unknown>;
  feedback: Array<{
    suggestionId?: number | null;
    targetField: string;
    status: 'suggested' | 'accepted' | 'rejected';
    sourceField?: string | null;
    correctedSourceField?: string | null;
    correctedTargetField?: string | null;
    rationale?: string | null;
    confidenceAfter?: number | null;
    metadata?: Record<string, unknown>;
  }>;
}): Promise<MappingFeedbackResult> {
  const response = await postJson<BackendCorrectionSessionResponse>('/api/learning/mapping-feedback', {
    generation_id: params.generationId,
    schema_fingerprint_id: params.schemaFingerprintId ?? null,
    notes: params.notes ?? null,
    metadata: params.metadata ?? {},
    feedback: params.feedback.map((item) => ({
      suggestion_id: item.suggestionId ?? null,
      target_field: item.targetField,
      status: item.status,
      source_field: item.sourceField ?? null,
      corrected_source_field: item.correctedSourceField ?? null,
      corrected_target_field: item.correctedTargetField ?? null,
      rationale: item.rationale ?? null,
      confidence_after: item.confidenceAfter ?? null,
      metadata: item.metadata ?? {},
    })),
  });

  return {
    sessionId: response.session_id,
    generationId: response.generation_id,
    schemaFingerprintId: response.schema_fingerprint_id,
    correctionIds: response.correction_ids,
    acceptedCount: response.accepted_count,
    count: response.count,
    reviewedCount: response.reviewed_count,
    rejectedCount: response.rejected_count,
    promotion: response.promotion
      ? {
          promoted: response.promotion.promoted,
          alreadyPromoted: response.promotion.already_promoted,
          fewShotExampleId: response.promotion.few_shot_example_id ?? null,
          datasetItemId: response.promotion.dataset_item_id ?? null,
          qualityScore: response.promotion.quality_score ?? null,
          reason: response.promotion.reason,
        }
      : undefined,
  };
}

export async function confirmGenerationLearning(params: {
  userId: string;
  generationId: number;
  notes?: string;
}): Promise<GenerationConfirmationResult> {
  const response = await postJson<BackendGenerationConfirmationResponse>('/api/learning/confirm-generation', {
    generation_id: params.generationId,
    notes: params.notes ?? null,
  });
  return {
    generationId: response.generation_id,
    promoted: response.promoted,
    alreadyPromoted: response.already_promoted,
    fewShotExampleId: response.few_shot_example_id ?? null,
    datasetItemId: response.dataset_item_id ?? null,
    qualityScore: response.quality_score ?? null,
    reason: response.reason,
  };
}

export async function saveDraftJsonFeedback(params: {
  userId: string;
  schemaFingerprintId: number;
  draftJson: Record<string, unknown>;
  templateName?: string;
  saveAsTemplate?: boolean;
  notes?: string;
  metadata?: Record<string, unknown>;
  feedback: Array<{
    suggestionId?: number | null;
    sourceColumn: string;
    suggestedField: string;
    status: 'suggested' | 'accepted' | 'rejected';
    correctedField?: string | null;
    rationale?: string | null;
    confidenceAfter?: number | null;
    metadata?: Record<string, unknown>;
  }>;
}): Promise<DraftJsonFeedbackResult> {
  const response = await postJson<BackendDraftJsonFeedbackResponse>('/api/learning/draft-json-feedback', {
    schema_fingerprint_id: params.schemaFingerprintId,
    draft_json: params.draftJson,
    template_name: params.templateName ?? null,
    save_as_template: params.saveAsTemplate ?? true,
    notes: params.notes ?? null,
    metadata: params.metadata ?? {},
    feedback: params.feedback.map((item) => ({
      suggestion_id: item.suggestionId ?? null,
      source_column: item.sourceColumn,
      suggested_field: item.suggestedField,
      status: item.status,
      corrected_field: item.correctedField ?? null,
      rationale: item.rationale ?? null,
      confidence_after: item.confidenceAfter ?? null,
      metadata: item.metadata ?? {},
    })),
  });
  return {
    schemaFingerprintId: response.schema_fingerprint_id,
    draftJson: response.draft_json,
    acceptedCount: response.accepted_count,
    rejectedCount: response.rejected_count,
    savedAsTemplate: response.saved_as_template,
    templateName: response.template_name,
  };
}
