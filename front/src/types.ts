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
  formExplainability?: FormExplainability | null;
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
  contentType: 'table' | 'form' | 'text' | 'image_like' | 'mixed' | 'unknown';
  documentMode?: 'data_table_mode' | 'form_layout_mode';
  extractionStatus: string;
  rawText: string;
  textBlocks: ParsedTextBlockInfo[];
  sections: ParsedSectionInfo[];
  kvPairs: ParsedKvPairInfo[];
  sourceCandidates: SourceCandidateInfo[];
  sheets: ParsedSheetInfo[];
  formModel?: {
    scalars: Record<string, unknown>[];
    groups: Record<string, unknown>[];
    sectionHierarchy: Record<string, unknown>[];
    layoutLines: Record<string, unknown>[];
    layoutMeta: Record<string, unknown>;
    resolvedFields: Record<string, unknown>[];
  } | null;
  warnings: string[];
};

export type FormRepairAction = {
  kind: string;
  priority: 'high' | 'medium' | 'low';
  reason: string;
  targetField?: string;
  groupId?: string;
  fields?: string[];
  llmScope?: string;
  chunkRefs: {
    groupIds: string[];
    scalarLabels: string[];
    lineIds: string[];
  };
};

export type FormRepairPlan = {
  recommended: boolean;
  triggerStage: 'generic_form_understanding' | 'business_mapping';
  strategy: string;
  llmPolicy: string;
  requestedTargetFields: string[];
  redFlagCodes: string[];
  actions: FormRepairAction[];
  targetedChunkCount: number;
};

export type FormExplainability = {
  documentMode: string;
  finalSourceMode?: string | null;
  layoutMeta: Record<string, unknown>;
  qualitySummary: {
    needsAttention?: boolean;
    repairRecommended?: boolean;
    resolvedFieldCount?: number;
    targetFieldCount?: number;
    ambiguousFields?: string[];
    unresolvedFields?: string[];
    unresolvedCriticalFields?: string[];
    repairFields?: string[];
    blockedFields?: string[];
    multipleSelectedSingleChoiceGroups?: string[];
    redFlags?: Array<{
      code: string;
      message?: string;
      fields?: string[];
      groups?: string[];
      resolvedFieldCount?: number;
      targetFieldCount?: number;
    }>;
  };
  repairPlan: FormRepairPlan;
  resolvedFields: Array<{
    field: string;
    status: 'resolved' | 'weak_match' | 'ambiguous' | 'not_found';
    resolvedBy: 'form_resolver' | 'repair_model' | 'repair_apply' | 'legacy_fallback' | 'fallback_blocked' | 'unresolved';
    value?: unknown;
    candidates?: unknown[];
    sourceRef?: Record<string, unknown>;
    confidence?: number | null;
  }>;
  scalarCount: number;
  groupCount: number;
  sectionCount: number;
  layoutLineCount: number;
  repairFields: string[];
};

export type RepairPreviewResult = {
  supported: boolean;
  previewStatus: 'patch_available' | 'inspection_only' | 'ambiguous' | 'no_patch';
  action: FormRepairAction;
  targetFields: Array<{
    name: string;
    type: string;
  }>;
  localChunks: {
    groups: Record<string, unknown>[];
    scalars: Record<string, unknown>[];
    lines: Record<string, unknown>[];
  };
  proposedResolutions: FormExplainability['resolvedFields'];
  proposedPatch: Record<string, unknown>;
  formExplainability?: FormExplainability | null;
  warnings: string[];
};

export type RepairApplyResult = {
  applied: boolean;
  action: FormRepairAction;
  approvedPatch: Record<string, unknown>;
  parsedFile: ParsedFileInfo;
  formExplainability?: FormExplainability | null;
  updatedResolvedFields: FormExplainability['resolvedFields'];
  persistence: {
    persisted: boolean;
    generationId?: number | string | null;
    versionId?: number | null;
    versionNumber?: number | null;
    sessionId?: number | null;
  };
};

export type ParsedSheetInfo = {
  name: string;
  columns: string[];
  rows: Record<string, unknown>[];
};

export type ParsedTextBlockInfo = {
  id: string;
  kind: 'paragraph' | 'line';
  text: string;
  label?: string | null;
};

export type ParsedSectionInfo = {
  title: string;
  text: string;
};

export type ParsedKvPairInfo = {
  label: string;
  value: string;
  confidence: 'high' | 'medium' | 'low';
  sourceText?: string | null;
};

export type SourceCandidateInfo = {
  candidateType: 'table_column' | 'kv_pair' | 'text_fact' | 'text_section';
  label: string;
  value?: unknown;
  sampleValues: unknown[];
  sourceText?: string | null;
  sectionTitle?: string | null;
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
  formExplainability?: FormExplainability | null;
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
  sourceQualityAdjustment?: {
    applied: boolean;
    adjustedCount: number;
    reasons: Record<string, number>;
    affectedTargets: string[];
    strongestPenalty: number;
  } | null;
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
    sourceQualityAdjustment?: GenerationResult['sourceQualityAdjustment'];
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
  formExplainability?: FormExplainability | null;
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
