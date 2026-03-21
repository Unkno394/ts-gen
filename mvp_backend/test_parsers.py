from __future__ import annotations

import shutil
import sys
import types
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

try:
    from docx import Document
except ImportError:  # pragma: no cover - optional dependency in local env
    Document = None
    docx_stub = types.ModuleType('docx')

    class _MissingDocument:
        def __init__(self, *args, **kwargs):
            raise RuntimeError('python-docx is not installed in the current environment')

    docx_stub.Document = _MissingDocument
    sys.modules['docx'] = docx_stub

try:
    import pdfplumber  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover - optional dependency in local env
    pdfplumber_stub = types.ModuleType('pdfplumber')

    class _MissingPdfContext:
        def __enter__(self):
            raise RuntimeError('pdfplumber is not installed in the current environment')

        def __exit__(self, exc_type, exc, tb):
            return None

    def _missing_pdf_open(*args, **kwargs):
        return _MissingPdfContext()

    pdfplumber_stub.open = _missing_pdf_open
    sys.modules['pdfplumber'] = pdfplumber_stub

try:
    import openpyxl  # type: ignore  # noqa: F401
except ImportError:  # pragma: no cover - optional dependency in local env
    openpyxl = None

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

try:
    import pydantic  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    pydantic_stub = types.ModuleType('pydantic')

    class _FieldInfo:
        def __init__(self, default=None, default_factory=None):
            self.default = default
            self.default_factory = default_factory

    def Field(default=None, default_factory=None):
        return _FieldInfo(default=default, default_factory=default_factory)

    class BaseModel:
        def __init__(self, **data):
            annotations = {}
            for cls in reversed(self.__class__.mro()):
                annotations.update(getattr(cls, '__annotations__', {}))
            for key in annotations:
                if key in data:
                    value = data[key]
                else:
                    class_value = getattr(self.__class__, key, None)
                    if isinstance(class_value, _FieldInfo):
                        if class_value.default_factory is not None:
                            value = class_value.default_factory()
                        else:
                            value = class_value.default
                    else:
                        value = class_value
                setattr(self, key, value)

        def dict(self):
            return dict(self.__dict__)

        def model_dump(self):
            return dict(self.__dict__)

        def copy(self, update=None):
            payload = dict(self.__dict__)
            payload.update(update or {})
            return self.__class__(**payload)

        def model_copy(self, update=None):
            return self.copy(update=update)

    pydantic_stub.BaseModel = BaseModel
    pydantic_stub.Field = Field
    sys.modules['pydantic'] = pydantic_stub

from models import ParsedFile, ParsedSheet
from parsers import ParseError, parse_file, resolve_generation_source


class FakeWorksheet:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def iter_rows(self, values_only: bool = True):
        return iter(self._rows)


class FakeWorkbook:
    def __init__(self, sheets: dict[str, FakeWorksheet]) -> None:
        self._sheets = sheets
        self.sheetnames = list(sheets.keys())

    def __getitem__(self, sheet_name: str) -> FakeWorksheet:
        return self._sheets[sheet_name]

    def close(self) -> None:
        return None


@unittest.skipIf(Document is None, 'python-docx is not installed in the current environment')
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

@unittest.skipIf(openpyxl is None, 'openpyxl is not installed in the current environment')
class ExcelParserTests(unittest.TestCase):
    def test_numeric_excel_headers_are_converted_to_strings(self) -> None:
        fake_workbook = FakeWorkbook(
            {
                'Sheet1': FakeWorksheet(
                    [
                        (1223, 'hsdh', 'sdvsdv'),
                        ('zov', 120, 'sddf'),
                    ]
                )
            }
        )

        with patch('parsers.load_workbook', return_value=fake_workbook):
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
        fake_workbook = FakeWorkbook(
            {
                'Jan': FakeWorksheet(
                    [
                        ('customerName', 'amount'),
                        ('alice', 10),
                    ]
                ),
                'Feb': FakeWorksheet(
                    [
                        ('customerName', 'amount'),
                        ('bob', 20),
                    ]
                ),
            }
        )

        with patch('parsers.load_workbook', return_value=fake_workbook):
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
