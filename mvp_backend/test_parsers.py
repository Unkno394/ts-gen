from __future__ import annotations

import shutil
import sys
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from docx import Document

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency in local env
    pd = None

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models import ParsedFile, ParsedSheet
from parsers import ParseError, parse_file, resolve_generation_source


class FakeExcelFile:
    def __init__(self, sheet_names: list[str]) -> None:
        self.sheet_names = sheet_names

    def __enter__(self) -> 'FakeExcelFile':
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None


class DocumentParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_root = BACKEND_DIR / '.test_runtime' / 'documents' / str(uuid.uuid4())
        self.test_root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.test_root, ignore_errors=True)

    def test_docx_table_is_parsed_without_generic_no_columns_warning(self) -> None:
        path = self.test_root / 'table.docx'
        doc = Document()
        table = doc.add_table(rows=2, cols=2)
        table.rows[0].cells[0].text = 'customerName'
        table.rows[0].cells[1].text = 'amount'
        table.rows[1].cells[0].text = 'Alice'
        table.rows[1].cells[1].text = '10'
        doc.save(path)

        parsed = parse_file(path, path.name)

        self.assertEqual(parsed.columns, ['customerName', 'amount'])
        self.assertEqual(parsed.rows, [{'customerName': 'Alice', 'amount': '10'}])
        self.assertNotIn('No columns detected in the file.', parsed.warnings)

    def test_docx_text_fallback_returns_document_warning_without_generic_no_columns_warning(self) -> None:
        path = self.test_root / 'text.docx'
        doc = Document()
        doc.add_paragraph('Just plain text without tables')
        doc.save(path)

        parsed = parse_file(path, path.name)

        self.assertEqual(parsed.columns, [])
        self.assertTrue(any('No tables found in DOCX' in warning for warning in parsed.warnings))
        self.assertTrue(any('Документ загружен.' in warning for warning in parsed.warnings))
        self.assertNotIn('No columns detected in the file.', parsed.warnings)

@unittest.skipIf(pd is None, 'pandas is not installed in the current environment')
class ExcelParserTests(unittest.TestCase):
    def test_numeric_excel_headers_are_converted_to_strings(self) -> None:
        dataframe = pd.DataFrame([['zov', 120, 'sddf']], columns=[1223, 'hsdh', 'sdvsdv'])
        fake_excel = FakeExcelFile(['Sheet1'])

        with patch('parsers.pd.ExcelFile', return_value=fake_excel), patch('parsers.pd.read_excel', return_value=dataframe):
            path = Path('numeric_headers.xlsx')
            parsed = parse_file(path, path.name)

        self.assertEqual(parsed.columns, ['1223', 'hsdh', 'sdvsdv'])
        self.assertEqual(parsed.rows, [{'1223': 'zov', 'hsdh': 120, 'sdvsdv': 'sddf'}])
        self.assertEqual(len(parsed.sheets), 1)
        self.assertEqual(parsed.sheets[0].name, 'Sheet1')
        self.assertEqual(parsed.sheets[0].columns, ['1223', 'hsdh', 'sdvsdv'])
        self.assertTrue(
            any('Excel first row is treated as column headers' in warning for warning in parsed.warnings)
        )

    def test_multiple_excel_sheets_are_merged(self) -> None:
        fake_excel = FakeExcelFile(['Jan', 'Feb'])
        jan = pd.DataFrame([['alice', 10]], columns=['customerName', 'amount'])
        feb = pd.DataFrame([['bob', 20]], columns=['customerName', 'amount'])

        with patch('parsers.pd.ExcelFile', return_value=fake_excel), patch('parsers.pd.read_excel', side_effect=[jan, feb]):
            path = Path('multi_sheet.xlsx')
            parsed = parse_file(path, path.name)

        self.assertEqual(parsed.columns, ['customerName', 'amount'])
        self.assertEqual(
            parsed.rows,
            [
                {'customerName': 'alice', 'amount': 10},
                {'customerName': 'bob', 'amount': 20},
            ],
        )
        self.assertEqual([sheet.name for sheet in parsed.sheets], ['Jan', 'Feb'])
        self.assertEqual(parsed.sheets[0].rows, [{'customerName': 'alice', 'amount': 10}])
        self.assertEqual(parsed.sheets[1].rows, [{'customerName': 'bob', 'amount': 20}])
        self.assertIn('Merged 2 sheets: Jan, Feb', parsed.warnings)

    def test_resolve_generation_source_uses_selected_sheet(self) -> None:
        parsed = ParsedFile(
            file_name='multi_sheet.xlsx',
            file_type='xlsx',
            columns=['1223', 'hsdh', 'sdvsdv', '345435', '234323', '234'],
            rows=[
                {'1223': 'zov', 'hsdh': 120, 'sdvsdv': 'sddf'},
                {'345435': 'avpva', '234323': 'avp', '234': 'byvapavp'},
            ],
            sheets=[
                ParsedSheet(name='Лист1', columns=['1223', 'hsdh', 'sdvsdv'], rows=[{'1223': 'zov', 'hsdh': 120, 'sdvsdv': 'sddf'}]),
                ParsedSheet(name='Лист2', columns=['345435', '234323', '234'], rows=[{'345435': 'avpva', '234323': 'avp', '234': 'byvapavp'}]),
            ],
            warnings=[],
        )

        columns, rows, warnings = resolve_generation_source(parsed, 'Лист2')

        self.assertEqual(columns, ['345435', '234323', '234'])
        self.assertEqual(rows, [{'345435': 'avpva', '234323': 'avp', '234': 'byvapavp'}])
        self.assertEqual(warnings, ['Generated mapping from selected sheet: Лист2'])

    def test_resolve_generation_source_raises_for_missing_sheet(self) -> None:
        parsed = ParsedFile(
            file_name='multi_sheet.xlsx',
            file_type='xlsx',
            columns=['1223', 'hsdh', 'sdvsdv'],
            rows=[{'1223': 'zov', 'hsdh': 120, 'sdvsdv': 'sddf'}],
            sheets=[ParsedSheet(name='Лист1', columns=['1223', 'hsdh', 'sdvsdv'], rows=[{'1223': 'zov', 'hsdh': 120, 'sdvsdv': 'sddf'}])],
            warnings=[],
        )

        with self.assertRaises(ParseError):
            resolve_generation_source(parsed, 'Лист2')


if __name__ == '__main__':
    unittest.main()
