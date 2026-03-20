from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from generator import build_preview, generate_typescript
from matcher import map_fields
from parsers import ParseError, infer_target_fields, parse_file
from storage import cleanup_guest_files, get_generation_by_id, get_history, init_db, save_generation


def _ensure_file(path: Path, label: str) -> Path:
    if not path.exists() or not path.is_file():
        raise SystemExit(f'{label} does not exist: {path}')
    return path


def _read_schema(schema_path: Path) -> tuple[list[Any], dict[str, Any]]:
    try:
        return infer_target_fields(schema_path.read_text(encoding='utf-8'))
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc


def _print_warnings(warnings: list[str]) -> None:
    if not warnings:
        return
    print('\nWarnings:')
    for warning in warnings:
        print(f'  - {warning}')


def _print_json(title: str, value: Any) -> None:
    print(f'\n{title}:')
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _mapping_to_json(mappings: list[Any]) -> list[dict[str, Any]]:
    return [mapping.model_dump() if hasattr(mapping, 'model_dump') else dict(mapping) for mapping in mappings]


def cmd_generate(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')
    out_path = Path(args.out)
    init_db()

    try:
        parsed_file = parse_file(input_path, input_path.name)
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    target_fields, target_payload = _read_schema(schema_path)
    mappings, mapping_warnings = map_fields(parsed_file.columns, target_fields)
    code = generate_typescript(target_fields, mappings)
    preview = build_preview(parsed_file.rows, target_fields, mappings)
    warnings = parsed_file.warnings + mapping_warnings

    out_path.write_text(code, encoding='utf-8')

    generation_id: int | None = None
    if not args.guest:
        generation_id = save_generation(
            user_id=args.user_id,
            file_name=parsed_file.file_name,
            file_path=str(input_path),
            file_type=parsed_file.file_type,
            target_json=json.dumps(target_payload, ensure_ascii=False),
            mappings_json=json.dumps(_mapping_to_json(mappings), ensure_ascii=False),
            generated_typescript=code,
            preview_json=json.dumps(preview, ensure_ascii=False),
            warnings_json=json.dumps(warnings, ensure_ascii=False),
            parsed_file_json=json.dumps(
                parsed_file.model_dump() if hasattr(parsed_file, 'model_dump') else parsed_file.dict(),
                ensure_ascii=False,
            ),
            source_columns=list(parsed_file.columns),
        )

    print('TSGen generate')
    print(f'Input file:    {input_path.name}')
    print(f'Columns:       {len(parsed_file.columns)}')
    print(f'Schema fields: {len(target_fields)}')
    print(f'Output:        {out_path}')
    print(f'Mode:          {"guest" if args.guest else "authorized"}')
    if generation_id is not None:
        print(f'History id:    {generation_id}')

    _print_warnings(warnings)

    if args.show_mapping:
        _print_json('Mapping', _mapping_to_json(mappings))
    if args.show_preview:
        _print_json('Preview JSON', preview)


def cmd_preview(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')

    try:
        parsed = parse_file(input_path, input_path.name)
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    print('TSGen preview')
    print(f'File:     {parsed.file_name}')
    print(f'Format:   {parsed.file_type}')
    print(f'Columns:  {len(parsed.columns)}')
    if parsed.columns:
        print(f'Detected: {", ".join(parsed.columns)}')

    _print_json(f'Sample rows (first {min(len(parsed.rows), args.rows)})', parsed.rows[: args.rows])
    _print_warnings(parsed.warnings)


def cmd_explain(args: argparse.Namespace) -> None:
    input_path = _ensure_file(Path(args.input), 'Input file')
    schema_path = _ensure_file(Path(args.schema), 'Schema file')

    try:
        parsed_file = parse_file(input_path, input_path.name)
    except ParseError as exc:
        raise SystemExit(str(exc)) from exc

    target_fields, _ = _read_schema(schema_path)
    mappings, mapping_warnings = map_fields(parsed_file.columns, target_fields)

    print('TSGen explain')
    print(f'Input file: {input_path.name}')
    _print_json('Mapping', _mapping_to_json(mappings))
    _print_warnings(parsed_file.warnings + mapping_warnings)


def cmd_history(args: argparse.Namespace) -> None:
    init_db()
    records = get_history(user_id=args.user_id, limit=args.limit)
    if not records:
        print('History is empty.')
        return

    print(f'TSGen history ({args.user_id})')
    for index, record in enumerate(records, start=1):
        if args.full:
            _print_json(f'Entry #{index}', record)
            continue

        print(f'\n[{index}] {record["id"]}')
        print(f'  created_at: {record["created_at"]}')
        print(f'  file_name:  {record["file_name"]}')
        print(f'  file_type:  {record["file_type"]}')


def cmd_show(args: argparse.Namespace) -> None:
    init_db()
    entry = get_generation_by_id(args.id)
    if not entry:
        raise SystemExit(f'History entry not found: {args.id}')

    print(f'TSGen show — {args.id}')
    print(f'created_at: {entry["created_at"]}')
    print(f'file_name:  {entry["file_name"]}')
    _print_json('Target JSON', json.loads(entry['target_json']))
    _print_json('Warnings', json.loads(entry['warnings_json']))
    print('\nGenerated TS:')
    print(entry['generated_typescript'])


def cmd_cleanup(args: argparse.Namespace) -> None:
    result = cleanup_guest_files(ttl_hours=args.ttl_hours, dry_run=args.dry_run)
    print('TSGen cleanup')
    _print_json('Cleanup result', result)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='TSGen CLI')
    subparsers = parser.add_subparsers(dest='command', required=True)

    generate_parser = subparsers.add_parser('generate', help='Generate TypeScript from input file and schema')
    generate_parser.add_argument('--input', '-i', required=True)
    generate_parser.add_argument('--schema', '-s', required=True)
    generate_parser.add_argument('--out', '-o', default='parser.ts')
    generate_parser.add_argument('--user-id', default='cli-user')
    generate_parser.add_argument('--guest', action='store_true')
    generate_parser.add_argument('--show-preview', action='store_true')
    generate_parser.add_argument('--show-mapping', action='store_true')
    generate_parser.set_defaults(func=cmd_generate)

    preview_parser = subparsers.add_parser('preview', help='Show parsed preview for an input file')
    preview_parser.add_argument('--input', '-i', required=True)
    preview_parser.add_argument('--rows', '-r', type=int, default=5)
    preview_parser.set_defaults(func=cmd_preview)

    explain_parser = subparsers.add_parser('explain', help='Show field mapping to target schema')
    explain_parser.add_argument('--input', '-i', required=True)
    explain_parser.add_argument('--schema', '-s', required=True)
    explain_parser.set_defaults(func=cmd_explain)

    history_parser = subparsers.add_parser('history', help='Show saved history for a user')
    history_parser.add_argument('--user-id', default='cli-user')
    history_parser.add_argument('--limit', '-n', type=int, default=20)
    history_parser.add_argument('--full', action='store_true')
    history_parser.set_defaults(func=cmd_history)

    show_parser = subparsers.add_parser('show', help='Show one history entry')
    show_parser.add_argument('--id', type=int, required=True)
    show_parser.set_defaults(func=cmd_show)

    cleanup_parser = subparsers.add_parser('cleanup', help='Cleanup expired guest files')
    cleanup_parser.add_argument('--ttl-hours', type=int, default=24)
    cleanup_parser.add_argument('--dry-run', action='store_true')
    cleanup_parser.set_defaults(func=cmd_cleanup)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
