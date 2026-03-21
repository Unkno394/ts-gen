from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from unittest.mock import patch

from generator import build_preview, generate_typescript
from learning_pipeline import resolve_generation_mappings_detailed
from parsers import parse_file, parse_target_schema, resolve_generation_source


def _approx_token_count(text: str) -> int:
    normalized = str(text or '')
    if not normalized:
        return 0
    return max(1, math.ceil(len(normalized) / 4))


class TokenProbe:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def fake_call_model_as_json(self, *, instructions: str, payload: dict, max_tokens: int):
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Dry-run token probe for the normal table + target JSON pipeline.'
    )
    parser.add_argument('source_file', type=Path, help='Path to source table/document file.')
    parser.add_argument('target_json', type=Path, help='Path to target JSON example.')
    parser.add_argument('--selected-sheet', default=None, help='Optional sheet/section name.')
    parser.add_argument('--disable-graph', action='store_true', help='Disable semantic graph during mapping.')
    args = parser.parse_args()

    parsed_file = parse_file(args.source_file, original_name=args.source_file.name)
    target_fields, _, _, schema_summary = parse_target_schema(args.target_json.read_text(encoding='utf-8'))
    source_columns, source_rows, source_warnings = resolve_generation_source(
        parsed_file,
        target_fields=target_fields,
        selected_sheet=args.selected_sheet,
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
            enable_semantic_graph=not args.disable_graph,
        )

    mappings = list(mapping_result.get('mappings') or [])
    generated_typescript = generate_typescript(target_fields, mappings)
    preview = build_preview(source_rows, target_fields, mappings)

    total_prompt = sum(int(call['prompt_tokens_approx']) for call in probe.calls)
    total_completion = sum(int(call['completion_tokens_approx']) for call in probe.calls)
    total_tokens = sum(int(call['total_tokens_approx']) for call in probe.calls)

    print('=== Token Probe Summary ===')
    print(f'source_file: {args.source_file}')
    print(f'target_json: {args.target_json}')
    print(f'content_type: {parsed_file.content_type}')
    print(f'document_mode: {parsed_file.document_mode}')
    print(f'source_columns: {len(source_columns)}')
    print(f'target_fields: {len(target_fields)}')
    print(f'source_rows: {len(source_rows)}')
    print(f'schema_summary: {json.dumps(schema_summary, ensure_ascii=False)}')
    print(f'source_warnings: {len(source_warnings)}')
    print(f'model_calls: {len(probe.calls)}')
    print(f'prompt_tokens_approx: {total_prompt}')
    print(f'completion_tokens_approx: {total_completion}')
    print(f'total_tokens_approx: {total_tokens}')
    print(f'generated_typescript_chars: {len(generated_typescript)}')
    print(f'preview_rows: {len(preview)}')

    if source_warnings:
        print('\n=== Source Warnings ===')
        for warning in source_warnings:
            print(f'- {warning}')

    explainability = mapping_result.get('explainability') or {}
    unresolved_fields = list(explainability.get('unresolved_fields') or [])
    if unresolved_fields:
        print('\n=== Unresolved Fields ===')
        for field in unresolved_fields:
            print(f'- {field}')

    if probe.calls:
        print('\n=== Model Calls ===')
        for index, call in enumerate(probe.calls, start=1):
            print(
                f"{index}. task={call['task']} target={call['target']} "
                f"candidates={call['candidate_count']} max_tokens={call['max_tokens']} "
                f"prompt≈{call['prompt_tokens_approx']} completion≈{call['completion_tokens_approx']} "
                f"total≈{call['total_tokens_approx']}"
            )
    else:
        print('\nNo model calls were needed. This input was resolved by deterministic/memory/graph layers.')

    return 0


if __name__ == '__main__':
    raise SystemExit(main())
