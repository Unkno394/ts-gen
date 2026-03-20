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
};

export type GenerationResult = {
  generationId?: string | null;
  parsedFile?: ParsedFileInfo | null;
  code: string;
  mappings: MappingInfo[];
  preview: Record<string, unknown>[];
  warnings: string[];
};

export type LearningSummary = {
  userId: string;
  uploads: number;
  schemaFingerprints: number;
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
