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
