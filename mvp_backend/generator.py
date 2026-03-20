from __future__ import annotations

from typing import Any

from models import FieldMapping, TargetField


TS_TYPE_MAP = {
    'string': 'string',
    'number': 'number',
    'boolean': 'boolean',
    'object': 'Record<string, any>',
    'array': 'any[]',
    'null': 'null',
    'any': 'any',
}



def generate_typescript(target_fields: list[TargetField], mappings: list[FieldMapping], interface_name: str = 'GeneratedRow') -> str:
    interface_lines = [f'export interface {interface_name} {{']
    for field in target_fields:
        interface_lines.append(f'  {field.name}: {TS_TYPE_MAP.get(field.type, "any")};')
    interface_lines.append('}')

    mapping_by_target = {m.target: m for m in mappings}

    transform_lines = [
        f'export function transform(row: Record<string, any>): {interface_name} {{',
        '  return {',
    ]
    for field in target_fields:
        mapping = mapping_by_target.get(field.name)
        expr = 'undefined as any'
        if mapping and mapping.source:
            expr = _ts_cast(field.type, f'row[{mapping.source!r}]')
        transform_lines.append(f'    {field.name}: {expr},')
    transform_lines.extend(['  };', '}'])

    transform_all_lines = [
        f'export function transformAll(rows: Record<string, any>[]): {interface_name}[] {{',
        '  return rows.map(transform);',
        '}',
    ]

    return '\n'.join(interface_lines + [''] + transform_lines + [''] + transform_all_lines)



def build_preview(parsed_rows: list[dict[str, Any]], target_fields: list[TargetField], mappings: list[FieldMapping]) -> list[dict[str, Any]]:
    mapping_by_target = {m.target: m for m in mappings}
    result: list[dict[str, Any]] = []
    for row in parsed_rows[:3]:
        out: dict[str, Any] = {}
        for field in target_fields:
            mapping = mapping_by_target.get(field.name)
            raw_value = row.get(mapping.source) if mapping and mapping.source else None
            out[field.name] = _py_cast(field.type, raw_value)
        result.append(out)
    return result



def _ts_cast(field_type: str, expr: str) -> str:
    if field_type == 'number':
        return f'Number({expr})'
    if field_type == 'boolean':
        return f'Boolean({expr})'
    if field_type == 'string':
        return expr
    return expr



def _py_cast(field_type: str, value: Any) -> Any:
    if value is None:
        return None
    if field_type == 'number':
        try:
            text = str(value).replace(' ', '').replace(',', '.')
            return float(text) if '.' in text else int(text)
        except Exception:  # noqa: BLE001
            return value
    if field_type == 'boolean':
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {'1', 'true', 'yes', 'да'}
    if field_type == 'string':
        return str(value)
    return value
