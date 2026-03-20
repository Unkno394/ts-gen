from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ParsedSheet(BaseModel):
    name: str
    columns: list[str]
    rows: list[dict[str, Any]]


class ParsedFile(BaseModel):
    file_name: str
    file_type: str
    columns: list[str]
    rows: list[dict[str, Any]]
    sheets: list[ParsedSheet] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class TargetField(BaseModel):
    name: str
    type: Literal['string', 'number', 'boolean', 'object', 'array', 'null', 'any']


class FieldMapping(BaseModel):
    source: str | None
    target: str
    confidence: Literal['high', 'medium', 'low', 'none']
    reason: str


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


class UserProfile(BaseModel):
    id: str
    name: str
    email: str


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
    user_id: str
    generation_id: int | None = None
    session_type: Literal['manual_review', 'post_generation_fix', 'template_authoring', 'feedback_loop'] = 'manual_review'
    schema_fingerprint_id: int | None = None
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    corrections: list[ManualCorrection] = Field(default_factory=list)


class UserTemplatePayload(BaseModel):
    user_id: str
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
