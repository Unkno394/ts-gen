from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any
from unittest.mock import patch

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from generator import build_preview, generate_typescript
from learning_pipeline import resolve_generation_mappings_detailed
from parsers import parse_file, parse_target_schema, resolve_generation_source
from storage import get_db, get_history


def _approx_token_count(text: str) -> int:
    normalized = str(text or '')
    if not normalized:
        return 0
    return max(1, math.ceil(len(normalized) / 4))


class TokenProbe:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def fake_call_model_as_json(self, *, instructions: str, payload: dict[str, Any], max_tokens: int) -> tuple[dict[str, Any], list[str]]:
        prompt_text = instructions + '\n' + json.dumps(payload, ensure_ascii=False, sort_keys=True)
        task = str(payload.get('task') or '')
        target = str(payload.get('target_field') or payload.get('task') or '')

        if task == 'choose_best_source_column_for_target_field':
            candidates = [str(item) for item in list(payload.get('candidates') or []) if str(item).strip()]
            response = {
                'target': str(payload.get('target_field') or ''),
                'best_candidate': candidates[0] if candidates else None,
                'confidence': 0.51,
                'reason': 'token_probe_dry_run',
            }
        elif task == 'mapping':
            response = {'mappings': []}
        elif task == 'draft_json':
            response = {'fields': []}
        else:
            response = {}

        response_text = json.dumps(response, ensure_ascii=False, sort_keys=True)
        record = {
            'task': task,
            'target': target,
            'max_tokens': max_tokens,
            'prompt_tokens_approx': _approx_token_count(prompt_text),
            'completion_tokens_approx': _approx_token_count(response_text),
            'total_tokens_approx': _approx_token_count(prompt_text) + _approx_token_count(response_text),
            'candidate_count': len(list(payload.get('candidates') or [])),
        }
        self.calls.append(record)
        return response, ['token_probe_dry_run']


def _format_case_label(file_name: str | None, selected_sheet: str | None, fallback: str) -> str:
    base = str(file_name or '').strip() or fallback
    sheet = str(selected_sheet or '').strip()
    return f'{base} [{sheet}]' if sheet else base


def _render_markdown_table(rows: list[dict[str, Any]], *, columns: list[tuple[str, str]]) -> str:
    headers = [title for _, title in columns]
    lines = [
        '| ' + ' | '.join(headers) + ' |',
        '| ' + ' | '.join(['---'] * len(headers)) + ' |',
    ]
    for row in rows:
        values = [str(row.get(key, '')) for key, _ in columns]
        lines.append('| ' + ' | '.join(values) + ' |')
    return '\n'.join(lines)


def _history_rows(user_id: str, limit: int | None) -> list[dict[str, Any]]:
    items = get_history(user_id, limit=limit)
    rows: list[dict[str, Any]] = []
    for item in items:
        rows.append(
            {
                'generation_id': item['id'],
                'case': _format_case_label(item.get('file_name'), item.get('selected_sheet'), fallback=f'generation-{item["id"]}'),
                'file_type': item.get('file_type') or '',
                'provider': item.get('token_usage_provider') or '',
                'model': item.get('token_usage_model_name') or '',
                'input_tokens': int(item.get('token_usage_input_tokens') or 0),
                'output_tokens': int(item.get('token_usage_output_tokens') or 0),
                'total_tokens': int(item.get('token_usage_total_tokens') or 0),
                'created_at': item.get('created_at') or '',
            }
        )
    return rows


def _list_available_users() -> list[dict[str, Any]]:
    db = get_db()
    rows = db.all(
        '''
        SELECT
            u.external_id AS user_id,
            COUNT(g.id) AS generation_count
        FROM users u
        LEFT JOIN generations g
            ON g.user_id = u.id
        GROUP BY u.id, u.external_id
        ORDER BY generation_count DESC, u.id DESC
        '''
    )
    return [dict(row) for row in rows]


def _summarize_history_rows(rows: list[dict[str, Any]]) -> dict[str, int]:
    return {
        'cases': len(rows),
        'input_tokens': sum(int(row.get('input_tokens') or 0) for row in rows),
        'output_tokens': sum(int(row.get('output_tokens') or 0) for row in rows),
        'total_tokens': sum(int(row.get('total_tokens') or 0) for row in rows),
    }


def _run_probe_case(
    *,
    name: str,
    source_file: Path,
    target_json: Path,
    selected_sheet: str | None = None,
    disable_graph: bool = False,
) -> dict[str, Any]:
    parsed_file = parse_file(source_file, original_name=source_file.name)
    target_fields, _, _, schema_summary = parse_target_schema(target_json.read_text(encoding='utf-8'))
    source_columns, source_rows, source_warnings = resolve_generation_source(
        parsed_file,
        target_fields=target_fields,
        selected_sheet=selected_sheet,
    )

    probe = TokenProbe()
    with patch('model_client._model_enabled', return_value=True), patch(
        'model_client._call_model_as_json',
        side_effect=probe.fake_call_model_as_json,
    ):
        mapping_result = resolve_generation_mappings_detailed(
            source_columns=source_columns,
            source_rows=source_rows,
            target_fields=target_fields,
            user_id=None,
            schema_fingerprint_id=None,
            enable_semantic_graph=not disable_graph,
        )

    mappings = list(mapping_result.get('mappings') or [])
    generated_typescript = generate_typescript(target_fields, mappings)
    preview = build_preview(source_rows, target_fields, mappings)

    prompt_total = sum(int(call['prompt_tokens_approx']) for call in probe.calls)
    completion_total = sum(int(call['completion_tokens_approx']) for call in probe.calls)
    total_tokens = sum(int(call['total_tokens_approx']) for call in probe.calls)

    return {
        'case': name,
        'source_file': str(source_file),
        'target_json': str(target_json),
        'selected_sheet': selected_sheet or '',
        'content_type': parsed_file.content_type,
        'document_mode': parsed_file.document_mode,
        'source_columns': len(source_columns),
        'source_rows': len(source_rows),
        'target_fields': len(target_fields),
        'model_calls': len(probe.calls),
        'prompt_tokens_approx': prompt_total,
        'completion_tokens_approx': completion_total,
        'total_tokens_approx': total_tokens,
        'generated_typescript_chars': len(generated_typescript),
        'preview_rows': len(preview),
        'source_warning_count': len(source_warnings),
        'schema_root_is_array': bool(schema_summary.get('root_is_array')),
    }


def _run_probe_manifest(manifest_path: Path, *, disable_graph: bool) -> list[dict[str, Any]]:
    payload = json.loads(manifest_path.read_text(encoding='utf-8'))
    if not isinstance(payload, list):
        raise ValueError('Manifest must be a JSON array.')

    rows: list[dict[str, Any]] = []
    for index, item in enumerate(payload, start=1):
        if not isinstance(item, dict):
            raise ValueError(f'Manifest item #{index} must be an object.')
        source_file = Path(str(item.get('source_file') or '')).expanduser()
        target_json = Path(str(item.get('target_json') or '')).expanduser()
        if not source_file.exists():
            raise ValueError(f'Source file does not exist: {source_file}')
        if not target_json.exists():
            raise ValueError(f'Target JSON file does not exist: {target_json}')
        rows.append(
            _run_probe_case(
                name=str(item.get('name') or source_file.name),
                source_file=source_file,
                target_json=target_json,
                selected_sheet=str(item.get('selected_sheet') or '').strip() or None,
                disable_graph=disable_graph,
            )
        )
    return rows


def _print_history_report(user_id: str, limit: int | None, fmt: str) -> int:
    rows = _history_rows(user_id, limit)
    summary = _summarize_history_rows(rows)
    if fmt == 'json':
        print(json.dumps({'user_id': user_id, 'summary': summary, 'items': rows}, ensure_ascii=False, indent=2))
        return 0

    if not rows:
        print(f'No history items with token usage for user_id={user_id}')
        return 0

    print(f'Tokenomics for user_id={user_id}')
    print(
        f"Cases: {summary['cases']} | Input: {summary['input_tokens']} | "
        f"Output: {summary['output_tokens']} | Total: {summary['total_tokens']}"
    )
    print()
    print(
        _render_markdown_table(
            rows,
            columns=[
                ('generation_id', 'Generation'),
                ('case', 'Case'),
                ('file_type', 'Type'),
                ('provider', 'Provider'),
                ('model', 'Model'),
                ('input_tokens', 'Input'),
                ('output_tokens', 'Output'),
                ('total_tokens', 'Total'),
                ('created_at', 'Created At'),
            ],
        )
    )
    return 0


def _print_probe_report(rows: list[dict[str, Any]], fmt: str) -> int:
    summary = {
        'cases': len(rows),
        'prompt_tokens_approx': sum(int(row.get('prompt_tokens_approx') or 0) for row in rows),
        'completion_tokens_approx': sum(int(row.get('completion_tokens_approx') or 0) for row in rows),
        'total_tokens_approx': sum(int(row.get('total_tokens_approx') or 0) for row in rows),
    }
    if fmt == 'json':
        print(json.dumps({'summary': summary, 'items': rows}, ensure_ascii=False, indent=2))
        return 0

    print(
        f"Approx tokenomics | Cases: {summary['cases']} | Prompt≈{summary['prompt_tokens_approx']} | "
        f"Completion≈{summary['completion_tokens_approx']} | Total≈{summary['total_tokens_approx']}"
    )
    print()
    print(
        _render_markdown_table(
            rows,
            columns=[
                ('case', 'Case'),
                ('content_type', 'Content Type'),
                ('document_mode', 'Document Mode'),
                ('source_columns', 'Source Cols'),
                ('source_rows', 'Source Rows'),
                ('target_fields', 'Target Fields'),
                ('model_calls', 'Model Calls'),
                ('prompt_tokens_approx', 'Prompt≈'),
                ('completion_tokens_approx', 'Completion≈'),
                ('total_tokens_approx', 'Total≈'),
            ],
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Tokenomics report for TSGen cases.')
    subparsers = parser.add_subparsers(dest='command', required=True)

    history_parser = subparsers.add_parser('history', help='Show exact per-case token usage from saved generation history.')
    history_parser.add_argument('--user-id', default=None, help='External user id from TSGen history.')
    history_parser.add_argument('--limit', type=int, default=None, help='Optional limit for history rows.')
    history_parser.add_argument('--format', choices=('markdown', 'json'), default='markdown')

    probe_parser = subparsers.add_parser('probe', help='Dry-run approximate tokenomics for one case.')
    probe_parser.add_argument('source_file', type=Path)
    probe_parser.add_argument('target_json', type=Path)
    probe_parser.add_argument('--name', default='case-1')
    probe_parser.add_argument('--selected-sheet', default=None)
    probe_parser.add_argument('--disable-graph', action='store_true')
    probe_parser.add_argument('--format', choices=('markdown', 'json'), default='markdown')

    manifest_parser = subparsers.add_parser('manifest', help='Dry-run approximate tokenomics for multiple cases from a JSON manifest.')
    manifest_parser.add_argument('manifest', type=Path, help='JSON array with name/source_file/target_json/selected_sheet.')
    manifest_parser.add_argument('--disable-graph', action='store_true')
    manifest_parser.add_argument('--format', choices=('markdown', 'json'), default='markdown')

    args = parser.parse_args()

    if args.command == 'history':
        if not args.user_id:
            available_users = _list_available_users()
            if not available_users:
                print('No users found in local history database.')
                return 0
            print('Available users:')
            for row in available_users:
                print(f"- {row['user_id']} (generations: {row['generation_count']})")
            print('\nRerun with: python tokenomics_report.py history --user-id <external_user_id>')
            return 0
        return _print_history_report(args.user_id, args.limit, args.format)

    if args.command == 'probe':
        row = _run_probe_case(
            name=str(args.name),
            source_file=args.source_file,
            target_json=args.target_json,
            selected_sheet=args.selected_sheet,
            disable_graph=bool(args.disable_graph),
        )
        return _print_probe_report([row], args.format)

    if args.command == 'manifest':
        rows = _run_probe_manifest(args.manifest, disable_graph=bool(args.disable_graph))
        return _print_probe_report(rows, args.format)

    raise ValueError(f'Unknown command: {args.command}')


if __name__ == '__main__':
    raise SystemExit(main())
