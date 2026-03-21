from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


def compile_typescript_code(
    code: str,
    *,
    interface_name: str = 'GeneratedRow',
    compiler_path: str | None = None,
) -> dict[str, Any]:
    tsc_path = _resolve_tsc_path(compiler_path)
    if tsc_path is None:
        return {
            'valid': False,
            'compiler_available': False,
            'diagnostics': [
                {
                    'code': 'compiler_unavailable',
                    'message': 'TypeScript compiler is not available in the current runtime.',
                }
            ],
        }

    with tempfile.TemporaryDirectory(prefix='tsgen-tsc-') as temp_dir:
        source_path = Path(temp_dir) / f'{interface_name}.ts'
        source_path.write_text(code, encoding='utf-8')
        command = [
            tsc_path,
            '--pretty',
            'false',
            '--noEmit',
            '--target',
            'ES2020',
            '--module',
            'commonjs',
            '--strict',
            source_path.name,
        ]
        process = subprocess.run(
            command,
            cwd=temp_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        diagnostics = _normalize_tsc_output(process.stdout, process.stderr)
        return {
            'valid': process.returncode == 0,
            'compiler_available': True,
            'diagnostics': diagnostics,
        }


def validate_preview_against_target_schema(
    preview_rows: list[dict[str, Any]],
    target_schema: dict[str, Any],
) -> dict[str, Any]:
    diagnostics: list[dict[str, Any]] = []
    runtime_valid = True
    row_schema = _resolve_preview_row_schema(target_schema)

    for row_index, row in enumerate(preview_rows[:3]):
        try:
            _validate_value_against_schema(
                row,
                row_schema,
                diagnostics=diagnostics,
                path=f'preview[{row_index}]',
            )
        except Exception as exc:  # noqa: BLE001
            runtime_valid = False
            diagnostics.append(
                {
                    'path': f'preview[{row_index}]',
                    'code': 'preview_runtime_error',
                    'message': str(exc),
                }
            )

    return {
        'runtime_valid': runtime_valid,
        'schema_valid': not diagnostics,
        'diagnostics': diagnostics,
        'validated_rows': min(len(preview_rows), 3),
    }


def assess_mapping_operational_status(mapping_stats: dict[str, Any], *, target_field_count: int) -> dict[str, Any]:
    unresolved_count = int(mapping_stats.get('unresolved', 0) or 0)
    resolved_count = max(target_field_count - unresolved_count, 0)
    resolved_ratio = resolved_count / target_field_count if target_field_count else 1.0
    review_heavy_sources = (
        int(mapping_stats.get('model_suggestion', 0) or 0)
        + int(mapping_stats.get('unresolved', 0) or 0)
    )
    review_ratio = review_heavy_sources / target_field_count if target_field_count else 0.0

    if resolved_ratio >= 0.9 and unresolved_count == 0 and review_ratio <= 0.35:
        status = 'high'
    elif resolved_ratio >= 0.7 and unresolved_count <= max(1, target_field_count // 4):
        status = 'medium'
    else:
        status = 'low'

    return {
        'status': status,
        'resolved_count': resolved_count,
        'unresolved_count': unresolved_count,
        'resolved_ratio': round(resolved_ratio, 4),
        'review_ratio': round(review_ratio, 4),
        'stats': dict(mapping_stats),
    }


def assess_mapping_quality(mapping_stats: dict[str, Any], *, target_field_count: int) -> dict[str, Any]:
    # Backward-compatible alias for older callers. Runtime uses this as an operational
    # readiness metric, not as a true accuracy metric.
    return assess_mapping_operational_status(mapping_stats, target_field_count=target_field_count)


def _resolve_tsc_path(explicit_path: str | None) -> str | None:
    candidates: list[str] = []
    if explicit_path:
        candidates.append(explicit_path)

    env_candidate = os.getenv('TSGEN_TSC_BIN')
    if env_candidate:
        candidates.append(env_candidate)

    candidates.extend(
        [
            'tsc',
            str(Path(__file__).resolve().parent.parent / 'front' / 'node_modules' / '.bin' / 'tsc'),
            str(Path(__file__).resolve().parent / 'node_modules' / '.bin' / 'tsc'),
        ]
    )

    for candidate in candidates:
        if not candidate:
            continue
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
        if Path(candidate).exists():
            return str(Path(candidate))
    return None


def _normalize_tsc_output(stdout: str, stderr: str) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    for stream_name, content in (('stdout', stdout), ('stderr', stderr)):
        for raw_line in content.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            diagnostics.append(
                {
                    'source': stream_name,
                    'message': line,
                }
            )
    return diagnostics


def _resolve_preview_row_schema(target_schema: dict[str, Any]) -> dict[str, Any]:
    if target_schema.get('type') == 'array':
        return dict(target_schema.get('items') or {'type': 'any'})
    return target_schema


def _validate_value_against_schema(
    value: Any,
    schema: dict[str, Any],
    *,
    diagnostics: list[dict[str, Any]],
    path: str,
) -> None:
    schema_type = schema.get('type', 'any')
    nullable = bool(schema.get('nullable'))

    if value is None:
        if nullable or schema_type in {'null', 'any'}:
            return
        diagnostics.append(
            {
                'path': path,
                'code': 'null_not_allowed',
                'expected': schema_type,
                'message': f'Value at {path} is null but schema expects {schema_type}.',
            }
        )
        return

    if schema_type == 'any':
        return
    if schema_type == 'string':
        if not isinstance(value, str):
            diagnostics.append(_type_mismatch(path, schema_type, value))
        return
    if schema_type == 'number':
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            diagnostics.append(_type_mismatch(path, schema_type, value))
        return
    if schema_type == 'boolean':
        if not isinstance(value, bool):
            diagnostics.append(_type_mismatch(path, schema_type, value))
        return
    if schema_type == 'object':
        if not isinstance(value, dict):
            diagnostics.append(_type_mismatch(path, schema_type, value))
            return
        properties = dict(schema.get('properties', {}))
        required_fields = list(schema.get('required_fields', []))
        for field_name in required_fields:
            if field_name not in value:
                diagnostics.append(
                    {
                        'path': f'{path}.{field_name}',
                        'code': 'missing_required_field',
                        'message': f'Required field {field_name} is missing at {path}.',
                    }
                )
        for field_name, field_schema in properties.items():
            if field_name not in value:
                continue
            _validate_value_against_schema(
                value[field_name],
                field_schema,
                diagnostics=diagnostics,
                path=f'{path}.{field_name}',
            )
        return
    if schema_type == 'array':
        if not isinstance(value, list):
            diagnostics.append(_type_mismatch(path, schema_type, value))
            return
        item_schema = dict(schema.get('items') or {'type': 'any'})
        for index, item in enumerate(value[:5]):
            _validate_value_against_schema(
                item,
                item_schema,
                diagnostics=diagnostics,
                path=f'{path}[{index}]',
            )
        return
    if schema_type == 'null':
        diagnostics.append(
            {
                'path': path,
                'code': 'expected_null',
                'message': f'Value at {path} must be null.',
            }
        )


def _type_mismatch(path: str, expected: str, value: Any) -> dict[str, Any]:
    return {
        'path': path,
        'code': 'type_mismatch',
        'expected': expected,
        'actual': _python_value_type(value),
        'message': f'Value at {path} has type {_python_value_type(value)}, expected {expected}.',
    }


def _python_value_type(value: Any) -> str:
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, (int, float)):
        return 'number'
    if isinstance(value, str):
        return 'string'
    if isinstance(value, dict):
        return 'object'
    if isinstance(value, list):
        return 'array'
    return type(value).__name__
