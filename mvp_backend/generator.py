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
    field_tree = _build_field_tree(target_fields)
    interface_lines = [f'export interface {interface_name} {{']
    interface_lines.extend(_render_interface_lines(field_tree, indent='  '))
    interface_lines.append('}')

    mapping_by_target = {m.target: m for m in mappings}

    transform_lines = [
        f'export function transform(row: Record<string, any>): {interface_name} {{',
        '  return {',
    ]
    transform_lines.extend(_render_transform_lines(field_tree, mapping_by_target, indent='    '))
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
    for row in parsed_rows:
        out: dict[str, Any] = {}
        for field in target_fields:
            mapping = mapping_by_target.get(field.name)
            raw_value = row.get(mapping.source) if mapping and mapping.source else None
            _set_nested_value(out, field.name.split('.'), _py_cast(field.type, raw_value))
        result.append(out)
    return result


def _build_field_tree(target_fields: list[TargetField]) -> dict[str, Any]:
    tree: dict[str, Any] = {}
    for field in target_fields:
        segments = [segment for segment in str(field.name).split('.') if segment]
        if not segments:
            continue
        current = tree
        for segment in segments[:-1]:
            current = current.setdefault(segment, {})
        current[segments[-1]] = field
    return tree


def _render_interface_lines(field_tree: dict[str, Any], *, indent: str) -> list[str]:
    lines: list[str] = []
    for key, value in field_tree.items():
        if isinstance(value, TargetField):
            lines.append(f'{indent}{key}: {TS_TYPE_MAP.get(value.type, "any")};')
            continue
        lines.append(f'{indent}{key}: {{')
        lines.extend(_render_interface_lines(value, indent=f'{indent}  '))
        lines.append(f'{indent}}};')
    return lines


def _render_transform_lines(
    field_tree: dict[str, Any],
    mapping_by_target: dict[str, FieldMapping],
    *,
    indent: str,
    prefix: str = '',
) -> list[str]:
    lines: list[str] = []
    for key, value in field_tree.items():
        current_path = f'{prefix}.{key}' if prefix else key
        if isinstance(value, TargetField):
            mapping = mapping_by_target.get(current_path)
            expr = 'undefined as any'
            if mapping and mapping.source:
                expr = _ts_cast(value.type, f'row[{mapping.source!r}]')
            lines.append(f'{indent}{key}: {expr},')
            continue
        lines.append(f'{indent}{key}: {{')
        lines.extend(_render_transform_lines(value, mapping_by_target, indent=f'{indent}  ', prefix=current_path))
        lines.append(f'{indent}}},')
    return lines



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


def _set_nested_value(target: dict[str, Any], path: list[str], value: Any) -> None:
    if not path:
        return
    current = target
    for segment in path[:-1]:
        next_value = current.get(segment)
        if not isinstance(next_value, dict):
            next_value = {}
            current[segment] = next_value
        current = next_value
    current[path[-1]] = value
