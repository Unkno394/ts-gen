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
  validation?: GenerationValidation | null;
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
  sourceOfTruth?: 'deterministic_rule' | 'personal_memory' | 'model_suggestion' | 'global_pattern' | 'semantic_graph' | 'position_fallback' | 'unresolved';
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
  targetSchema?: Record<string, unknown> | unknown[] | null;
  requiredFields?: string[];
  tsValid?: boolean;
  tsDiagnostics?: ValidationDiagnostic[];
  previewDiagnostics?: ValidationDiagnostic[];
  mappingOperationalStatus?: OperationalMappingStatus | null;
  mappingEvalMetrics?: TrueQualityMetrics | null;
  tsSyntaxValid?: boolean;
  tsRuntimePreviewValid?: boolean;
  outputSchemaValid?: boolean;
};

export type ValidationDiagnostic = {
  path?: string;
  code?: string;
  expected?: string;
  actual?: string;
  message: string;
  source?: string;
};

export type OperationalMappingStatus = {
  status: 'high' | 'medium' | 'low';
  resolvedCount: number;
  unresolvedCount: number;
  resolvedRatio: number;
  reviewRatio: number;
  stats: Record<string, number>;
};

export type TrueQualityMetrics = {
  available: boolean;
  exactMatchRate?: number | null;
  falsePositiveRate?: number | null;
  unresolvedRate?: number | null;
  acceptedAfterReviewRate?: number | null;
  notes?: string | null;
};

export type GenerationValidation = {
  targetSchema?: Record<string, unknown> | unknown[] | null;
  targetSchemaSummary?: {
    rootType?: string;
    requiredFields?: string[];
    fieldCount?: number;
    rootIsArray?: boolean;
  } | null;
  tsValidation?: {
    valid?: boolean;
    compilerAvailable?: boolean;
    diagnostics?: ValidationDiagnostic[];
  } | null;
  previewValidation?: {
    runtimeValid?: boolean;
    schemaValid?: boolean;
    diagnostics?: ValidationDiagnostic[];
    validatedRows?: number;
  } | null;
  qualitySummary?: {
    operationalMappingStatus?: OperationalMappingStatus | null;
    trueQualityMetrics?: TrueQualityMetrics | null;
    tsSyntaxValid?: boolean;
    tsRuntimePreviewValid?: boolean;
    outputSchemaValid?: boolean;
  } | null;
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

export type LearningMemoryStagingItem = {
  sourceField?: string | null;
  sourceFieldNorm?: string | null;
  targetField: string;
  targetFieldNorm: string;
  status: 'suggested' | 'rejected';
  sourceOfTruth: string;
  seenCount: number;
  averageConfidence?: number | null;
  confidenceBand: 'high' | 'medium' | 'low' | 'none';
  lastSeenAt?: string | null;
};

export type LearningMemoryPersonalItem = {
  sourceField?: string | null;
  sourceFieldNorm?: string | null;
  targetField: string;
  targetFieldNorm: string;
  acceptedCount: number;
  rejectedCount: number;
  usageCount: number;
  schemaFingerprintCount: number;
  rowCount: number;
  averageConfidence?: number | null;
  confidenceBand: 'high' | 'medium' | 'low' | 'none';
  sourceOfTruths: string[];
  lastSeenAt?: string | null;
};

export type LearningMemoryGlobalItem = {
  candidateId: number;
  sourceField?: string | null;
  sourceFieldNorm: string;
  targetField?: string | null;
  targetFieldNorm: string;
  status: 'personal_only' | 'shared_candidate' | 'shared_promoted' | 'blocked_sensitive';
  semanticRole?: string | null;
  conceptCluster?: string | null;
  domainTags: string[];
  sensitivityScore?: number | null;
  generalizabilityScore?: number | null;
  supportCount: number;
  uniqueUsers: number;
  acceptedCount: number;
  rejectedCount: number;
  acceptanceRate?: number | null;
  stabilityScore?: number | null;
  driftScore?: number | null;
  semanticConflictRate?: number | null;
  averageConfidence?: number | null;
  confidenceBand: 'high' | 'medium' | 'low' | 'none';
  promotionReason?: string | null;
  rejectionReason?: string | null;
  lastSeenAt?: string | null;
};

export type LearningSemanticGraphItem = {
  leftField: string;
  leftFieldNorm: string;
  leftEntityToken?: string | null;
  leftAttributeToken?: string | null;
  leftRoleLabel?: string | null;
  rightField: string;
  rightFieldNorm: string;
  rightEntityToken?: string | null;
  rightAttributeToken?: string | null;
  rightRoleLabel?: string | null;
  relationKind: 'mapping_synonym' | 'semantic_conflict';
  acceptedCount: number;
  rejectedCount: number;
  supportCount: number;
  averageConfidence?: number | null;
  confidenceBand: 'high' | 'medium' | 'low' | 'none';
  lastOutcome?: 'accepted' | 'rejected' | null;
  sourceOfTruth?: string | null;
  lastSeenAt?: string | null;
};

export type LearningSemanticGraphCluster = {
  clusterId: string;
  size: number;
  supportCount: number;
  sharedAttributes: string[];
  sharedRoles: string[];
  entities: string[];
  fields: Array<{
    field: string;
    fieldNorm: string;
    entityToken?: string | null;
    attributeToken?: string | null;
    roleLabel?: string | null;
  }>;
  edges: Array<{
    leftFieldNorm: string;
    rightFieldNorm: string;
    supportCount: number;
  }>;
};

export type LearningMemory = {
  userId: string;
  layers: {
    staging: {
      counts: {
        pending: number;
        rejected: number;
        total: number;
      };
      items: LearningMemoryStagingItem[];
    };
    personalMemory: {
      counts: {
        entries: number;
        accepted: number;
        rejected: number;
      };
      items: LearningMemoryPersonalItem[];
    };
    globalKnowledge: {
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
      items: LearningMemoryGlobalItem[];
    };
    semanticGraph: {
      counts: {
        nodes: number;
        edges: number;
        accepted: number;
        rejected: number;
      };
      items: LearningSemanticGraphItem[];
      clusters: LearningSemanticGraphCluster[];
    };
  };
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
