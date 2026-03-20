from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency in local env
    pd = None

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from models import ParsedFile, ParsedSheet
from parsers import ParseError, parse_file, resolve_generation_source


@unittest.skipIf(pd is None, 'pandas is not installed in the current environment')
class ExcelParserTests(unittest.TestCase):
    def test_numeric_excel_headers_are_converted_to_strings(self) -> None:
        dataframe = pd.DataFrame([['zov', 120, 'sddf']], columns=[1223, 'hsdh', 'sdvsdv'])
        fake_excel = type('FakeExcelFile', (), {'sheet_names': ['Sheet1']})()

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
        fake_excel = type('FakeExcelFile', (), {'sheet_names': ['Jan', 'Feb']})()
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
