from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from generator import generate_typescript
from models import FieldMapping, TargetField
from validation import compile_typescript_code, validate_preview_against_target_schema

PARSER_IMPORT_ERROR: ModuleNotFoundError | None = None
try:
    from parsers import ParseError, parse_target_schema
except ModuleNotFoundError as exc:
    PARSER_IMPORT_ERROR = exc
    ParseError = ValueError  # type: ignore[misc, assignment]
    parse_target_schema = None  # type: ignore[assignment]


class TargetSchemaParsingTests(unittest.TestCase):
    def setUp(self) -> None:
        if PARSER_IMPORT_ERROR is not None:
            self.skipTest(f'Parser dependencies are unavailable in the current environment: {PARSER_IMPORT_ERROR}')

    def test_parse_target_schema_rejects_duplicate_keys(self) -> None:
        with self.assertRaises(ParseError):
            parse_target_schema('{"name": "", "name": 1}')

    def test_parse_target_schema_rejects_empty_keys(self) -> None:
        with self.assertRaises(ParseError):
            parse_target_schema('{"": ""}')

    def test_parse_target_schema_rejects_conflicting_array_item_types(self) -> None:
        with self.assertRaises(ParseError):
            parse_target_schema('{"items": [1, "two"]}')

    def test_parse_target_schema_supports_array_root_of_objects(self) -> None:
        target_fields, payload, target_schema, summary = parse_target_schema('[{"name": "", "amount": 0}]')

        self.assertEqual([field.name for field in target_fields], ['name', 'amount'])
        self.assertIsInstance(payload, list)
        self.assertEqual(target_schema['type'], 'array')
        self.assertEqual(summary['required_fields'], ['name', 'amount'])


class PreviewValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        if PARSER_IMPORT_ERROR is not None:
            self.skipTest(f'Parser dependencies are unavailable in the current environment: {PARSER_IMPORT_ERROR}')

    def test_preview_validation_flags_missing_required_and_type_mismatch(self) -> None:
        _, _, target_schema, _ = parse_target_schema('{"name": "", "amount": 0, "active": false}')

        preview = [{'name': 'Alice', 'amount': '10'}]
        validation = validate_preview_against_target_schema(preview, target_schema)

        self.assertFalse(validation['schema_valid'])
        codes = {item['code'] for item in validation['diagnostics']}
        self.assertIn('type_mismatch', codes)
        self.assertIn('missing_required_field', codes)


class TypescriptCompilationTests(unittest.TestCase):
    def test_compiler_accepts_valid_generated_code(self) -> None:
        code = generate_typescript(
            [TargetField(name='customerName', type='string')],
            [
                FieldMapping(
                    source='Customer Name',
                    target='customerName',
                    confidence='high',
                    reason='deterministic',
                )
            ],
        )

        result = compile_typescript_code(code)
        if not result['compiler_available']:
            self.skipTest('TypeScript compiler is not available in the current environment')
        self.assertTrue(result['valid'], result['diagnostics'])

    def test_compiler_reports_invalid_code(self) -> None:
        result = compile_typescript_code('export const broken = ;')
        if not result['compiler_available']:
            self.skipTest('TypeScript compiler is not available in the current environment')
        self.assertFalse(result['valid'])
        self.assertTrue(result['diagnostics'])


if __name__ == '__main__':
    unittest.main()
