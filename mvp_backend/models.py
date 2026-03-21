from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ParsedSheet(BaseModel):
    name: str
    columns: list[str]
    rows: list[dict[str, Any]]


class ParsedTextBlock(BaseModel):
    id: str
    kind: Literal['paragraph', 'line']
    text: str
    label: str | None = None


class ParsedSection(BaseModel):
    title: str
    text: str


class ParsedKvPair(BaseModel):
    label: str
    value: str
    confidence: Literal['high', 'medium', 'low'] = 'medium'
    source_text: str | None = None


class SourceCandidate(BaseModel):
    candidate_type: Literal['table_column', 'kv_pair', 'text_fact', 'text_section']
    label: str
    value: Any | None = None
    sample_values: list[Any] = Field(default_factory=list)
    source_text: str | None = None
    section_title: str | None = None


class LayoutToken(BaseModel):
    text: str
    page: int | None = None
    block_id: str | None = None
    line_id: str | None = None
    column_id: int | None = None
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    table_idx: int | None = None
    row_idx: int | None = None
    cell_idx: int | None = None
    paragraph_idx: int | None = None
    font_bold: bool | None = None
    font_size: float | None = None
    source_type: Literal['paragraph', 'table_cell', 'line'] = 'line'


class LayoutLine(BaseModel):
    text: str
    page: int | None = None
    block_id: str | None = None
    line_id: str | None = None
    column_id: int | None = None
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    table_idx: int | None = None
    row_idx: int | None = None
    cell_idx: int | None = None
    paragraph_idx: int | None = None
    font_bold: bool | None = None
    font_size: float | None = None
    source_type: Literal['paragraph', 'table_cell', 'line'] = 'line'
    tokens: list[LayoutToken] = Field(default_factory=list)


class ScalarFieldCandidate(BaseModel):
    label: str
    value: Any
    source_ref: dict[str, Any] = Field(default_factory=dict)
    confidence: Literal['high', 'medium', 'low'] = 'medium'


class OptionItem(BaseModel):
    label: str
    selected: bool = False
    marker_text: str | None = None
    source_ref: dict[str, Any] = Field(default_factory=dict)


class QuestionGroup(BaseModel):
    group_id: str
    question: str
    group_type: Literal['single_choice', 'multi_choice', 'unknown'] = 'unknown'
    options: list[OptionItem] = Field(default_factory=list)
    source_ref: dict[str, Any] = Field(default_factory=dict)


class FormFieldResolution(BaseModel):
    field: str
    status: Literal['resolved', 'weak_match', 'ambiguous', 'not_found'] = 'not_found'
    resolved_by: Literal['form_resolver', 'repair_model', 'repair_apply', 'legacy_fallback', 'fallback_blocked', 'unresolved'] = 'unresolved'
    value: Any | None = None
    candidates: list[Any] = Field(default_factory=list)
    source_ref: dict[str, Any] = Field(default_factory=dict)
    confidence: float | None = None


class FormDocumentModel(BaseModel):
    scalars: list[ScalarFieldCandidate] = Field(default_factory=list)
    groups: list[QuestionGroup] = Field(default_factory=list)
    section_hierarchy: list[dict[str, Any]] = Field(default_factory=list)
    layout_lines: list[LayoutLine] = Field(default_factory=list)
    layout_meta: dict[str, Any] = Field(default_factory=dict)
    resolved_fields: list[FormFieldResolution] = Field(default_factory=list)


class ParsedFile(BaseModel):
    file_name: str
    file_type: str
    columns: list[str]
    rows: list[dict[str, Any]]
    content_type: Literal['table', 'form', 'text', 'image_like', 'mixed', 'unknown'] = 'unknown'
    document_mode: Literal['data_table_mode', 'form_layout_mode'] = 'data_table_mode'
    extraction_status: str = 'unknown'
    raw_text: str = ''
    text_blocks: list[ParsedTextBlock] = Field(default_factory=list)
    sections: list[ParsedSection] = Field(default_factory=list)
    kv_pairs: list[ParsedKvPair] = Field(default_factory=list)
    source_candidates: list[SourceCandidate] = Field(default_factory=list)
    sheets: list[ParsedSheet] = Field(default_factory=list)
    form_model: FormDocumentModel | None = None
    warnings: list[str] = Field(default_factory=list)


class TargetField(BaseModel):
    name: str
    type: Literal['string', 'number', 'boolean', 'object', 'array', 'null', 'any']


class FieldMapping(BaseModel):
    source: str | None
    target: str
    confidence: Literal['high', 'medium', 'low', 'none']
    reason: str
    status: Literal['suggested', 'accepted', 'rejected'] = 'accepted'
    source_of_truth: Literal[
        'deterministic_rule',
        'personal_memory',
        'model_suggestion',
        'global_pattern',
        'semantic_graph',
        'position_fallback',
        'unresolved',
    ] = 'deterministic_rule'
    suggestion_id: int | None = None
    schema_fingerprint_id: int | None = None
    model_confidence_score: float | None = None
    candidate_metadata: dict[str, Any] = Field(default_factory=dict)


class GenerationResult(BaseModel):
    parsed_file: ParsedFile
    target_fields: list[TargetField]
    mappings: list[FieldMapping]
    generated_typescript: str
    preview: list[dict[str, Any]]
    warnings: list[str] = Field(default_factory=list)
    generation_id: int | None = None
    mode: Literal['guest', 'authorized']


class AuthPayload(BaseModel):
    email: str
    password: str
    name: str | None = None


class RegisterPayload(BaseModel):
    email: str
    password: str
    name: str | None = None
    verification_code: str


class EmailCodePayload(BaseModel):
    email: str


class ResetPasswordPayload(BaseModel):
    email: str
    verification_code: str | None = None
    reset_token: str | None = None
    password: str


class VerifyResetCodePayload(BaseModel):
    email: str
    verification_code: str


class EmailChangeCodePayload(BaseModel):
    new_email: str


class ChangeEmailPayload(BaseModel):
    new_email: str
    current_password: str | None = None
    verification_code: str | None = None


class UpdateProfilePayload(BaseModel):
    name: str


class ChangePasswordPayload(BaseModel):
    current_password: str
    new_password: str


class UserProfile(BaseModel):
    id: str
    name: str
    email: str


class AuthenticatedUserProfile(UserProfile):
    access_token: str
    token_type: Literal['bearer'] = 'bearer'


class ManualCorrection(BaseModel):
    correction_type: Literal[
        'mapping_override',
        'value_fix',
        'ignore_field',
        'rename_field',
        'template_edit',
        'target_schema_edit',
        'code_edit',
        'feedback_note',
    ] = 'feedback_note'
    row_index: int | None = None
    field_path: str | None = None
    source_field: str | None = None
    target_field: str | None = None
    original_value: Any | None = None
    corrected_value: Any | None = None
    correction_payload: Any | None = None
    rationale: str | None = None
    confidence_before: float | None = None
    confidence_after: float | None = None
    accepted: bool = True


class CorrectionSessionPayload(BaseModel):
    generation_id: int | None = None
    session_type: Literal['manual_review', 'post_generation_fix', 'template_authoring', 'feedback_loop'] = 'manual_review'
    schema_fingerprint_id: int | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    corrections: list[ManualCorrection] = Field(default_factory=list)


class UserTemplatePayload(BaseModel):
    name: str
    template_kind: Literal['transform', 'mapping', 'postprocess', 'schema', 'hybrid'] = 'transform'
    template_json: dict[str, Any]
    description: str | None = None
    target_json: dict[str, Any] | None = None
    generated_typescript: str | None = None
    prompt_suffix: str | None = None
    schema_fingerprint_id: int | None = None
    is_shared: bool = False
    metadata: dict[str, Any] = Field(default_factory=dict)


class MappingFeedbackItem(BaseModel):
    suggestion_id: int | None = None
    target_field: str
    status: Literal['suggested', 'accepted', 'rejected']
    source_field: str | None = None
    corrected_source_field: str | None = None
    corrected_target_field: str | None = None
    rationale: str | None = None
    confidence_after: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MappingFeedbackPayload(BaseModel):
    generation_id: int
    schema_fingerprint_id: int | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    feedback: list[MappingFeedbackItem] = Field(default_factory=list)


class PatternPromotionPayload(BaseModel):
    min_support_count: int = 3
    min_distinct_users: int = 2
    min_stability_score: float = 0.75
    max_drift_score: float = 0.25
    min_acceptance_rate: float = 0.8
    max_semantic_conflict_rate: float = 0.15
    max_sensitivity_score: float = 0.6
    min_generalizability_score: float = 0.5


class TrainingSnapshotPayload(BaseModel):
    name: str
    min_quality_score: float = 0.8
    include_statuses: list[Literal['candidate', 'approved']] = Field(default_factory=lambda: ['approved'])
    notes: str | None = None


class TrainingRunPayload(BaseModel):
    snapshot_id: int
    model_family: str = 'gigachat'
    base_model: str = 'GigaChat-2-Pro'
    train_params: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None


class TrainingSnapshotExportPayload(BaseModel):
    overwrite: bool = False


class TrainingRunStartPayload(BaseModel):
    trainer_mode: Literal['manifest_only'] | None = None
    auto_activate: bool = False
    serving_provider: Literal['gigachat'] | None = None
    serving_base_url: str | None = None
    serving_model_name: str | None = None


class TrainingRunCompletionPayload(BaseModel):
    artifact_uri: str
    metrics: dict[str, Any] = Field(default_factory=dict)
    notes: str | None = None
    auto_activate: bool = False
    serving_provider: Literal['gigachat'] | None = None
    serving_base_url: str | None = None
    serving_model_name: str | None = None


class TrainingRunActivationPayload(BaseModel):
    provider: Literal['gigachat'] | None = None
    base_url: str | None = None
    model_name: str | None = None
    notes: str | None = None


class GenerationConfirmationPayload(BaseModel):
    generation_id: int
    notes: str | None = None


class DraftJsonFeedbackItem(BaseModel):
    suggestion_id: int | None = None
    source_column: str
    suggested_field: str
    status: Literal['suggested', 'accepted', 'rejected']
    corrected_field: str | None = None
    rationale: str | None = None
    confidence_after: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class DraftJsonFeedbackPayload(BaseModel):
    schema_fingerprint_id: int
    draft_json: dict[str, Any]
    template_name: str | None = None
    save_as_template: bool = True
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    feedback: list[DraftJsonFeedbackItem] = Field(default_factory=list)


class RepairPreviewPayload(BaseModel):
    parsed_file: dict[str, Any]
    action: dict[str, Any]
    target_json: dict[str, Any] | list[Any] | str | None = None


class RepairApplyPayload(BaseModel):
    parsed_file: dict[str, Any]
    action: dict[str, Any]
    approved_patch: dict[str, Any]
    target_json: dict[str, Any] | list[Any] | str | None = None
    generation_id: int | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SourcePreviewRefreshLogPayload(BaseModel):
    file_name: str | None = None
    selected_sheet: str | None = None
    result: Literal['changed', 'unchanged', 'initial', 'error']
    active_sheet_changed: bool = False
    structure_changed: bool = False
    previous_sheet_name: str | None = None
    next_sheet_name: str | None = None
    previous_sheet_count: int = 0
    next_sheet_count: int = 0
    previous_column_count: int = 0
    next_column_count: int = 0
    previous_row_count: int = 0
    next_row_count: int = 0
    details: list[str] = Field(default_factory=list)
    message: str | None = None
