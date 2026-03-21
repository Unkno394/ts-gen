export type AuthMode = 'register' | 'login';

export type UserProfile = {
  id: string;
  name: string;
  email: string;
  skipped?: boolean;
};

export type HistoryItem = {
  id: string;
  createdAt: string;
  fileName: string;
  selectedSheet?: string | null;
  parsedFile?: ParsedFileInfo | null;
  schema: string;
  code: string;
  mappings: MappingInfo[];
  preview: Record<string, unknown>[];
  warnings: string[];
};

export type ParsedFileInfo = {
  fileName: string;
  extension: string;
  columns: string[];
  rows: Record<string, unknown>[];
  sheets: ParsedSheetInfo[];
  warnings: string[];
};

export type ParsedSheetInfo = {
  name: string;
  columns: string[];
  rows: Record<string, unknown>[];
};

export type MappingInfo = {
  source: string;
  target: string;
  confidence: 'high' | 'medium' | 'low' | 'none';
  reason?: string;
  status?: 'suggested' | 'accepted' | 'rejected';
  sourceOfTruth?: 'deterministic_rule' | 'personal_memory' | 'model_suggestion' | 'global_pattern' | 'position_fallback' | 'unresolved';
  suggestionId?: number | null;
  schemaFingerprintId?: number | null;
};

export type GenerationResult = {
  generationId?: string | null;
  schemaFingerprintId?: number | null;
  parsedFile?: ParsedFileInfo | null;
  code: string;
  mappings: MappingInfo[];
  preview: Record<string, unknown>[];
  warnings: string[];
};

export type DraftFieldSuggestion = {
  sourceColumn: string;
  targetField: string;
  defaultValue: unknown;
  fieldType: 'string' | 'number' | 'boolean' | 'object' | 'array' | 'null' | 'any';
  status?: 'suggested' | 'accepted' | 'rejected';
  sourceOfTruth?: 'heuristic_fallback' | 'personal_memory' | 'model_suggestion' | 'global_pattern';
  confidence?: 'high' | 'medium' | 'low' | 'none';
  reason?: string;
  suggestionId?: number | null;
  schemaFingerprintId?: number | null;
};

export type DraftJsonResult = {
  schemaFingerprintId?: number | null;
  parsedFile?: ParsedFileInfo | null;
  draftJson: Record<string, unknown>;
  fieldSuggestions: DraftFieldSuggestion[];
  warnings: string[];
};

export type LearningSummary = {
  userId: string;
  uploads: number;
  schemaFingerprints: number;
  mappingSuggestions: number;
  draftJsonSuggestions: number;
  mappingMemory: number;
  fewShotExamples: number;
  userTemplates: number;
  correctionSessions: number;
  userCorrections: number;
  frequentDjson: number;
  globalPatternCandidates: number;
  globalCuratedDatasetItems: number;
};

export type ManualCorrectionInput = {
  correctionType:
    | 'mapping_override'
    | 'value_fix'
    | 'ignore_field'
    | 'rename_field'
    | 'template_edit'
    | 'target_schema_edit'
    | 'code_edit'
    | 'feedback_note';
  rowIndex?: number | null;
  fieldPath?: string | null;
  sourceField?: string | null;
  targetField?: string | null;
  originalValue?: unknown;
  correctedValue?: unknown;
  correctionPayload?: unknown;
  rationale?: string | null;
  confidenceBefore?: number | null;
  confidenceAfter?: number | null;
  accepted?: boolean;
};

export type CorrectionSessionResult = {
  sessionId: number;
  generationId: number | null;
  schemaFingerprintId: number | null;
  correctionIds: number[];
  acceptedCount: number;
  count: number;
};

export type MappingFeedbackResult = CorrectionSessionResult & {
  reviewedCount?: number;
  rejectedCount?: number;
  promotion?: {
    promoted?: boolean;
    alreadyPromoted?: boolean;
    fewShotExampleId?: number | null;
    datasetItemId?: number | null;
    qualityScore?: number | null;
    reason?: string;
  };
};

export type GenerationConfirmationResult = {
  generationId: number;
  promoted: boolean;
  alreadyPromoted?: boolean;
  fewShotExampleId?: number | null;
  datasetItemId?: number | null;
  qualityScore?: number | null;
  reason?: string;
};

export type DraftJsonFeedbackResult = {
  schemaFingerprintId: number;
  draftJson: Record<string, unknown>;
  acceptedCount: number;
  rejectedCount: number;
  savedAsTemplate: boolean;
  templateName: string;
};

export type LearningEvent = {
  id: string;
  kind: 'feedback_session' | 'few_shot_example' | 'user_template' | 'draft_memory' | 'dataset_item' | 'global_pattern';
  stage: 'staging' | 'memory' | 'global_pattern' | 'dataset';
  title: string;
  description: string;
  createdAt: string;
  metadata?: Record<string, unknown>;
};
