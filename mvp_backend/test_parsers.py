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

try:
    import fastapi  # type: ignore  # noqa: F401
except ModuleNotFoundError:
    fastapi_stub = types.ModuleType('fastapi')
    fastapi_security_stub = types.ModuleType('fastapi.security')

    class APIRouter:
        def post(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def get(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

        def delete(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator

    class HTTPException(Exception):
        def __init__(self, status_code: int, detail: str, headers=None):
            self.status_code = status_code
            self.detail = detail
            self.headers = headers or {}
            super().__init__(detail)

    class UploadFile:
        def __init__(self, *args, **kwargs):
            self.filename = kwargs.get('filename')

    class HTTPAuthorizationCredentials:
        def __init__(self, credentials: str = ''):
            self.credentials = credentials

    class HTTPBearer:
        def __init__(self, *args, **kwargs):
            self.auto_error = kwargs.get('auto_error', True)

        def __call__(self, *args, **kwargs):
            return None

    def Depends(value=None):
        return value

    def File(*args, **kwargs):
        return None

    def Form(*args, **kwargs):
        return None

    fastapi_stub.APIRouter = APIRouter
    fastapi_stub.Depends = Depends
    fastapi_stub.File = File
    fastapi_stub.Form = Form
    fastapi_stub.HTTPException = HTTPException
    fastapi_stub.UploadFile = UploadFile
    fastapi_stub.status = types.SimpleNamespace(HTTP_401_UNAUTHORIZED=401)
    fastapi_security_stub.HTTPAuthorizationCredentials = HTTPAuthorizationCredentials
    fastapi_security_stub.HTTPBearer = HTTPBearer
    sys.modules['fastapi'] = fastapi_stub
    sys.modules['fastapi.security'] = fastapi_security_stub

from models import ParsedFile, ParsedSheet, RepairApplyPayload, RepairPreviewPayload, TargetField
from parsers import ParseError, _build_form_model, parse_file, resolve_generation_source
from routes import _build_form_explainability, repair_apply, repair_preview


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


class FakePdfPage:
    def __init__(self, *, text: str, words: list[dict[str, object]] | None = None, tables: list[list[list[str]]] | None = None) -> None:
        self._text = text
        self._words = words or []
        self._tables = tables or []

    def extract_text(self):
        return self._text

    def extract_words(self):
        return list(self._words)

    def extract_tables(self):
        return list(self._tables)


class FakePdfContext:
    def __init__(self, pages: list[FakePdfPage]) -> None:
        self.pages = pages

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
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
        self.assertEqual(parsed.content_type, 'text')
        self.assertEqual(parsed.extraction_status, 'text_extracted')
        self.assertNotIn('No columns detected in the file.', parsed.warnings)

    def test_docx_form_text_extracts_kv_pairs_and_candidates(self) -> None:
        path = self.test_root / 'form.docx'
        doc = Document()
        doc.add_paragraph('ФИО: Иванов Иван')
        doc.add_paragraph('Дата рождения: 01.01.1990')
        doc.add_paragraph('Страна налогового резидентства: Германия')
        doc.save(path)

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(parsed)

        self.assertEqual(parsed.content_type, 'form')
        self.assertEqual(parsed.extraction_status, 'text_extracted')
        self.assertEqual([pair.label for pair in parsed.kv_pairs], ['ФИО', 'Дата рождения', 'Страна налогового резидентства'])
        self.assertEqual(columns, ['ФИО', 'Дата рождения', 'Страна налогового резидентства'])
        self.assertEqual(rows, [{'ФИО': 'Иванов Иван', 'Дата рождения': '01.01.1990', 'Страна налогового резидентства': 'Германия'}])
        self.assertIn('Generated mapping from extracted fields/text candidates.', warnings)

    def test_txt_form_extracts_kv_pairs_and_sections(self) -> None:
        path = self.test_root / 'form.txt'
        path.write_text(
            'Анкета клиента\n\nФИО: Иванов Иван\nДата рождения: 01.01.1990\nСтрана налогового резидентства: Германия\n',
            encoding='utf-8',
        )

        parsed = parse_file(path, path.name)
        columns, rows, _warnings = resolve_generation_source(parsed)

        self.assertEqual(parsed.file_type, 'txt')
        self.assertEqual(parsed.content_type, 'form')
        self.assertEqual(parsed.extraction_status, 'text_extracted')
        self.assertGreaterEqual(len(parsed.sections), 1)
        self.assertEqual(columns, ['ФИО', 'Дата рождения', 'Страна налогового резидентства'])
        self.assertEqual(rows[0]['Страна налогового резидентства'], 'Германия')

    def test_image_like_input_reports_ocr_requirement(self) -> None:
        path = self.test_root / 'scan.png'
        path.write_bytes(b'not-a-real-png-but-extension-is-enough')

        parsed = parse_file(path, path.name)

        self.assertEqual(parsed.content_type, 'image_like')
        self.assertEqual(parsed.extraction_status, 'image_parse_not_supported_yet')
        self.assertEqual(parsed.columns, [])
        self.assertEqual(parsed.rows, [])
        self.assertTrue(any('OCR' in warning or 'image' in warning.lower() for warning in parsed.warnings))

    def test_docx_form_like_table_is_resolved_via_form_aware_extraction(self) -> None:
        path = self.test_root / 'form_layout.docx'
        doc = Document()
        table = doc.add_table(rows=8, cols=2)
        table.rows[0].cells[0].text = 'Наименование организации'
        table.rows[0].cells[1].text = 'ООО "Рога и копыта"'
        table.rows[1].cells[0].text = 'ИНН/КИО'
        table.rows[1].cells[1].text = '1234567890'
        table.rows[2].cells[0].text = 'Является ли выгодоприобретатель налоговым резидентом только в РФ'
        table.rows[3].cells[0].text = ' '
        table.rows[3].cells[1].text = 'ДА'
        table.rows[4].cells[0].text = 'X'
        table.rows[4].cells[1].text = 'Не являюсь налоговым резидентом ни в одном государстве'
        table.rows[5].cells[0].text = ' '
        table.rows[5].cells[1].text = 'НЕТ, является налоговым резидентом в иностранном государстве'
        table.rows[6].cells[0].text = 'FATCA статус выгодоприобретателя'
        table.rows[7].cells[0].text = 'X'
        table.rows[7].cells[1].text = 'Иностранным финансовым институтом'
        doc.save(path)

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(
            parsed,
            target_fields=[
                TargetField(name='organizationName', type='string'),
                TargetField(name='innOrKio', type='string'),
                TargetField(name='isResidentRF', type='string'),
                TargetField(name='isTaxResidencyOnlyRF', type='string'),
                TargetField(name='fatcaBeneficiaryOptionList', type='array'),
            ],
        )

        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertIsNotNone(parsed.form_model)
        self.assertTrue(any(group.group_id == 'tax_residency' for group in parsed.form_model.groups))
        self.assertEqual(
            columns,
            [
                'organizationName',
                'innOrKio',
                'isResidentRF',
                'isTaxResidencyOnlyRF',
                'fatcaBeneficiaryOptionList',
            ],
        )
        self.assertEqual(rows[0]['organizationName'], 'ООО "Рога и копыта"')
        self.assertEqual(rows[0]['innOrKio'], '1234567890')
        self.assertEqual(rows[0]['isResidentRF'], 'NOWHERE')
        self.assertEqual(rows[0]['isTaxResidencyOnlyRF'], 'NO')
        self.assertEqual(rows[0]['fatcaBeneficiaryOptionList'], ['IS_FATCA_FOREIGN_INSTITUTE'])
        self.assertIn('Generated mapping from form-aware extraction.', warnings)
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'form_resolver')
        self.assertEqual(parsed.form_model.layout_meta['pipeline_layers']['layout_extraction']['status'], 'completed')
        self.assertEqual(parsed.form_model.layout_meta['pipeline_layers']['generic_form_understanding']['status'], 'completed')
        self.assertEqual(parsed.form_model.layout_meta['pipeline_layers']['business_mapping']['status'], 'completed')
        resolution = next(item for item in parsed.form_model.resolved_fields if item.field == 'organizationName')
        self.assertEqual(resolution.resolved_by, 'form_resolver')

    def test_docx_form_like_single_choice_ambiguity_is_not_silently_resolved(self) -> None:
        path = self.test_root / 'form_layout_ambiguous.docx'
        doc = Document()
        table = doc.add_table(rows=4, cols=2)
        table.rows[0].cells[0].text = 'Является ли выгодоприобретатель налоговым резидентом только в РФ'
        table.rows[1].cells[0].text = 'X'
        table.rows[1].cells[1].text = 'ДА'
        table.rows[2].cells[0].text = 'X'
        table.rows[2].cells[1].text = 'Не являюсь налоговым резидентом ни в одном государстве'
        table.rows[3].cells[0].text = 'ИНН/КИО'
        table.rows[3].cells[1].text = '1234567890'
        doc.save(path)

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(
            parsed,
            target_fields=[
                TargetField(name='isResidentRF', type='string'),
                TargetField(name='innOrKio', type='string'),
            ],
        )

        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertEqual(columns, ['innOrKio'])
        self.assertEqual(rows, [{'innOrKio': '1234567890'}])
        self.assertTrue(any('ambiguous' in warning.lower() for warning in warnings))
        self.assertIsNotNone(parsed.form_model)
        resolution = next(item for item in parsed.form_model.resolved_fields if item.field == 'isResidentRF')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'fallback_blocked')
        self.assertEqual(resolution.status, 'ambiguous')
        self.assertEqual(resolution.resolved_by, 'fallback_blocked')
        self.assertIsNone(resolution.value)

    def test_form_layout_without_target_fields_prefers_generic_form_source(self) -> None:
        path = self.test_root / 'form_layout_generic_source.docx'
        doc = Document()
        table = doc.add_table(rows=8, cols=2)
        table.rows[0].cells[0].text = 'РќР°РёРјРµРЅРѕРІР°РЅРёРµ РѕСЂРіР°РЅРёР·Р°С†РёРё'
        table.rows[0].cells[1].text = 'РћРћРћ "Р РѕРіР° Рё РєРѕРїС‹С‚Р°"'
        table.rows[1].cells[0].text = 'РРќРќ/РљРРћ'
        table.rows[1].cells[1].text = '1234567890'
        table.rows[2].cells[0].text = 'РЇРІР»СЏРµС‚СЃСЏ Р»Рё РІС‹РіРѕРґРѕРїСЂРёРѕР±СЂРµС‚Р°С‚РµР»СЊ РЅР°Р»РѕРіРѕРІС‹Рј СЂРµР·РёРґРµРЅС‚РѕРј С‚РѕР»СЊРєРѕ РІ Р Р¤'
        table.rows[3].cells[0].text = ' '
        table.rows[3].cells[1].text = 'Р”Рђ'
        table.rows[4].cells[0].text = 'X'
        table.rows[4].cells[1].text = 'РќРµ СЏРІР»СЏСЋСЃСЊ РЅР°Р»РѕРіРѕРІС‹Рј СЂРµР·РёРґРµРЅС‚РѕРј РЅРё РІ РѕРґРЅРѕРј РіРѕСЃСѓРґР°СЂСЃС‚РІРµ'
        table.rows[5].cells[0].text = 'FATCA СЃС‚Р°С‚СѓСЃ РІС‹РіРѕРґРѕРїСЂРёРѕР±СЂРµС‚Р°С‚РµР»СЏ'
        table.rows[6].cells[0].text = 'X'
        table.rows[6].cells[1].text = 'РРЅРѕСЃС‚СЂР°РЅРЅС‹Рј С„РёРЅР°РЅСЃРѕРІС‹Рј РёРЅСЃС‚РёС‚СѓС‚РѕРј'
        table.rows[7].cells[0].text = ' '
        table.rows[7].cells[1].text = 'Р‘РѕР»РµРµ 10% Р°РєС†РёР№ РїСЂРёРЅР°РґР»РµР¶Р°С‚ РЅР°Р»РѕРіРѕРїР»Р°С‚РµР»СЊС‰РёРєР°Рј РЎРЁРђ'
        doc.save(path)

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(parsed)

        self.assertEqual(parsed.content_type, 'form')
        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'generic_form_source')
        self.assertGreaterEqual(len(columns), 2)
        self.assertEqual(len(rows), 1)
        self.assertIn('fatca_beneficiary', columns)
        self.assertTrue(any(value == '1234567890' for value in rows[0].values()))
        self.assertIsInstance(rows[0]['fatca_beneficiary'], list)
        self.assertIn('Generated mapping from form-aware extracted fields.', warnings)

    def test_form_critical_field_does_not_fall_back_to_legacy_candidates(self) -> None:
        path = self.test_root / 'critical_fallback_blocked.txt'
        path.write_text(
            'Анкета\n\nИНН/КИО: 1234567890\nФИО: Иванов Иван\n',
            encoding='utf-8',
        )

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(
            parsed,
            target_fields=[TargetField(name='isResidentRF', type='string')],
        )

        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertEqual(columns, [])
        self.assertEqual(rows, [])
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'fallback_blocked')
        self.assertTrue(any('blocked' in warning.lower() for warning in warnings))

    def test_form_non_critical_request_can_use_legacy_fallback_with_provenance(self) -> None:
        path = self.test_root / 'non_critical_fallback.txt'
        path.write_text(
            'Анкета\n\nФИО: Иванов Иван\nДата рождения: 01.01.1990\n',
            encoding='utf-8',
        )

        parsed = parse_file(path, path.name)
        columns, rows, warnings = resolve_generation_source(
            parsed,
            target_fields=[TargetField(name='customerNameNormalized', type='string')],
        )

        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'legacy_fallback')
        self.assertEqual(columns, ['ФИО', 'Дата рождения'])
        self.assertEqual(rows[0]['ФИО'], 'Иванов Иван')
        self.assertTrue(any('fell back' in warning.lower() for warning in warnings))


class PdfAndRepairParserTests(unittest.TestCase):
    def test_pdf_layout_words_are_grouped_and_ground_tax_option(self) -> None:
        fake_pdf = FakePdfContext(
            [
                FakePdfPage(
                    text='Tax residency form with enough extracted text for parsing flow',
                    words=[
                        {'text': 'Является', 'x0': 10, 'x1': 70, 'top': 10, 'bottom': 18},
                        {'text': 'ли', 'x0': 74, 'x1': 84, 'top': 10, 'bottom': 18},
                        {'text': 'выгодоприобретатель', 'x0': 88, 'x1': 180, 'top': 10, 'bottom': 18},
                        {'text': 'налоговым', 'x0': 184, 'x1': 250, 'top': 10, 'bottom': 18},
                        {'text': 'резидентом', 'x0': 254, 'x1': 320, 'top': 10, 'bottom': 18},
                        {'text': 'только', 'x0': 324, 'x1': 365, 'top': 10, 'bottom': 18},
                        {'text': 'в', 'x0': 369, 'x1': 376, 'top': 10, 'bottom': 18},
                        {'text': 'РФ', 'x0': 380, 'x1': 395, 'top': 10, 'bottom': 18},
                        {'text': 'X', 'x0': 14, 'x1': 20, 'top': 30, 'bottom': 38},
                        {'text': 'Не', 'x0': 40, 'x1': 52, 'top': 30, 'bottom': 38},
                        {'text': 'являюсь', 'x0': 56, 'x1': 108, 'top': 30, 'bottom': 38},
                        {'text': 'налоговым', 'x0': 112, 'x1': 178, 'top': 30, 'bottom': 38},
                        {'text': 'резидентом', 'x0': 182, 'x1': 248, 'top': 30, 'bottom': 38},
                        {'text': 'ни', 'x0': 252, 'x1': 264, 'top': 30, 'bottom': 38},
                        {'text': 'в', 'x0': 268, 'x1': 275, 'top': 30, 'bottom': 38},
                        {'text': 'одном', 'x0': 279, 'x1': 316, 'top': 30, 'bottom': 38},
                        {'text': 'государстве', 'x0': 320, 'x1': 388, 'top': 30, 'bottom': 38},
                    ],
                )
            ]
        )

        with patch('pdf_parser.pdfplumber.open', return_value=fake_pdf):
            parsed = parse_file(Path('grounded.pdf'), 'grounded.pdf')
            columns, rows, _warnings = resolve_generation_source(
                parsed,
                target_fields=[TargetField(name='isResidentRF', type='string')],
            )

        self.assertEqual(parsed.document_mode, 'form_layout_mode')
        self.assertIsNotNone(parsed.form_model)
        self.assertEqual(columns, ['isResidentRF'])
        self.assertEqual(rows[0]['isResidentRF'], 'NOWHERE')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'form_resolver')
        tax_group = next(group for group in parsed.form_model.groups if group.group_id == 'tax_residency')
        self.assertTrue(any(option.selected for option in tax_group.options))

    def test_repair_mode_marks_resolution_as_repair_model(self) -> None:
        from form_layout import build_form_document_model

        text = (
            'Является ли выгодоприобретатель налоговым резидентом только в РФ\n'
            'X ДА\n'
            'X Не являюсь налоговым резидентом ни в одном государстве\n'
        )
        parsed = ParsedFile(
            file_name='repair.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text=text,
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                build_form_document_model(
                    file_path=Path('repair.txt'),
                    file_type='txt',
                    raw_text=text,
                    kv_pairs=[],
                    text_blocks=[],
                    sections=[],
                    layout_blocks=[],
                )
            ),
            warnings=[],
        )

        with patch(
            'form_layout.suggest_form_field_repair',
            return_value=({'status': 'resolved', 'enum_value': 'NOWHERE', 'confidence': 0.77}, []),
        ):
            columns, rows, _warnings = resolve_generation_source(
                parsed,
                target_fields=[TargetField(name='isResidentRF', type='string')],
            )

        self.assertEqual(columns, ['isResidentRF'])
        self.assertEqual(rows[0]['isResidentRF'], 'NOWHERE')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'repair_model')
        resolution = next(item for item in parsed.form_model.resolved_fields if item.field == 'isResidentRF')
        self.assertEqual(resolution.resolved_by, 'repair_model')
        self.assertEqual(resolution.status, 'resolved')

    def test_simple_ambiguous_group_does_not_trigger_repair_model(self) -> None:
        from form_layout import build_form_document_model

        text = (
            'Является ли выгодоприобретатель налоговым резидентом только в РФ\n'
            'X ДА\n'
            'X НЕТ\n'
        )
        parsed = ParsedFile(
            file_name='simple_repair.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text=text,
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                build_form_document_model(
                    file_path=Path('simple_repair.txt'),
                    file_type='txt',
                    raw_text=text,
                    kv_pairs=[],
                    text_blocks=[],
                    sections=[],
                    layout_blocks=[],
                )
            ),
            warnings=[],
        )

        with patch('form_layout.suggest_form_field_repair') as repair_mock:
            columns, rows, warnings = resolve_generation_source(
                parsed,
                target_fields=[TargetField(name='isResidentRF', type='string')],
            )

        self.assertEqual(columns, [])
        self.assertEqual(rows, [])
        repair_mock.assert_not_called()
        resolution = next(item for item in parsed.form_model.resolved_fields if item.field == 'isResidentRF')
        self.assertEqual(resolution.resolved_by, 'fallback_blocked')
        self.assertEqual(parsed.form_model.layout_meta['final_source_mode'], 'fallback_blocked')

    def test_pdf_grouping_respects_columns_and_merges_multiline_option(self) -> None:
        fake_pdf = FakePdfContext(
            [
                FakePdfPage(
                    text='Form text with columns and wrapped options for grouping',
                    words=[
                        {'text': 'Является', 'x0': 20, 'x1': 70, 'top': 10, 'bottom': 18},
                        {'text': 'ли', 'x0': 74, 'x1': 84, 'top': 10, 'bottom': 18},
                        {'text': 'выгодоприобретатель', 'x0': 88, 'x1': 180, 'top': 10, 'bottom': 18},
                        {'text': 'налоговым', 'x0': 184, 'x1': 250, 'top': 10, 'bottom': 18},
                        {'text': 'резидентом', 'x0': 254, 'x1': 320, 'top': 10, 'bottom': 18},
                        {'text': 'только', 'x0': 324, 'x1': 365, 'top': 10, 'bottom': 18},
                        {'text': 'в', 'x0': 369, 'x1': 376, 'top': 10, 'bottom': 18},
                        {'text': 'РФ', 'x0': 380, 'x1': 395, 'top': 10, 'bottom': 18},
                        {'text': 'X', 'x0': 20, 'x1': 26, 'top': 28, 'bottom': 36},
                        {'text': 'Не', 'x0': 44, 'x1': 56, 'top': 28, 'bottom': 36},
                        {'text': 'являюсь', 'x0': 60, 'x1': 112, 'top': 28, 'bottom': 36},
                        {'text': 'налоговым', 'x0': 116, 'x1': 182, 'top': 28, 'bottom': 36},
                        {'text': 'резидентом', 'x0': 186, 'x1': 252, 'top': 28, 'bottom': 36},
                        {'text': 'в', 'x0': 56, 'x1': 63, 'top': 40, 'bottom': 48},
                        {'text': 'иностранном', 'x0': 67, 'x1': 135, 'top': 40, 'bottom': 48},
                        {'text': 'государстве', 'x0': 139, 'x1': 207, 'top': 40, 'bottom': 48},
                        {'text': 'Контактное', 'x0': 470, 'x1': 535, 'top': 28, 'bottom': 36},
                        {'text': 'лицо', 'x0': 539, 'x1': 568, 'top': 28, 'bottom': 36},
                    ],
                )
            ]
        )

        with patch('pdf_parser.pdfplumber.open', return_value=fake_pdf):
            parsed = parse_file(Path('columns.pdf'), 'columns.pdf')

        self.assertIsNotNone(parsed.form_model)
        tax_group = next(group for group in parsed.form_model.groups if group.group_id == 'tax_residency')
        self.assertEqual(len(tax_group.options), 1)
        self.assertEqual(
            tax_group.options[0].label,
            'Не являюсь налоговым резидентом в иностранном государстве',
        )
        self.assertEqual(tax_group.source_ref.get('column_id'), 1)
        self.assertTrue(all(option.source_ref.get('column_id') == 1 for option in tax_group.options))

    def test_form_explainability_contains_quality_summary(self) -> None:
        parsed = ParsedFile(
            file_name='explainability.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='ИНН/КИО: 1234567890\n',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [
                        {
                            'group_id': 'tax_residency',
                            'question': 'Является ли выгодоприобретатель налоговым резидентом только в РФ',
                            'group_type': 'single_choice',
                            'options': [
                                {
                                    'label': 'ДА',
                                    'selected': True,
                                    'marker_text': 'X',
                                    'source_ref': {'line_id': 'line-2'},
                                },
                                {
                                    'label': 'НЕТ',
                                    'selected': True,
                                    'marker_text': 'X',
                                    'source_ref': {'line_id': 'line-3'},
                                },
                            ],
                            'source_ref': {'line_id': 'line-1'},
                        }
                    ],
                    'layout_lines': [
                        {'text': 'Является ли выгодоприобретатель налоговым резидентом только в РФ', 'line_id': 'line-1'},
                        {'text': 'X ДА', 'line_id': 'line-2'},
                        {'text': 'X НЕТ', 'line_id': 'line-3'},
                    ],
                    'layout_meta': {},
                    'resolved_fields': [
                        {
                            'field': 'isResidentRF',
                            'status': 'ambiguous',
                            'resolved_by': 'fallback_blocked',
                            'value': None,
                            'candidates': ['YES', 'NO'],
                            'source_ref': {'line_id': 'line-1'},
                            'confidence': 0.31,
                        }
                    ],
                }
            ),
            warnings=[],
        )
        parsed.form_model.layout_meta.update(
            {
                'final_source_mode': 'fallback_blocked',
                'quality_summary': {
                    'needs_attention': True,
                    'repair_recommended': True,
                    'unresolved_critical_fields': ['isResidentRF'],
                    'ambiguous_fields': ['isResidentRF'],
                    'multiple_selected_single_choice_groups': ['tax_residency'],
                    'red_flags': [{'code': 'critical_unresolved'}],
                },
                'requested_target_fields': ['isResidentRF'],
            }
        )

        explainability = _build_form_explainability(parsed)

        self.assertIsNotNone(explainability)
        self.assertEqual(explainability['final_source_mode'], 'fallback_blocked')
        self.assertTrue(explainability['quality_summary']['needs_attention'])
        self.assertEqual(explainability['quality_summary']['red_flags'][0]['code'], 'critical_unresolved')
        self.assertEqual(explainability['section_count'], 0)
        self.assertTrue(explainability['repair_plan']['recommended'])
        self.assertEqual(explainability['repair_plan']['strategy'], 'layout_chunks_then_targeted_repair')
        self.assertTrue(any(action['target_field'] == 'isResidentRF' for action in explainability['repair_plan']['actions'] if 'target_field' in action))
        repair_action = next(action for action in explainability['repair_plan']['actions'] if action.get('target_field') == 'isResidentRF')
        self.assertIn('tax_residency', repair_action['chunk_refs']['group_ids'])
        self.assertIn('line-1', repair_action['chunk_refs']['line_ids'])
        self.assertEqual(explainability['repair_plan']['llm_policy'], 'targeted_local_chunks_only')

    def test_form_explainability_builds_generic_repair_plan_without_business_quality(self) -> None:
        parsed = ParsedFile(
            file_name='generic_explainability.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='Question\nX A\nX B\n',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [
                        {
                            'group_id': 'group_1',
                            'question': 'Question',
                            'group_type': 'single_choice',
                            'options': [
                                {'label': 'A', 'selected': True, 'marker_text': 'X', 'source_ref': {'line_id': 'line-2'}},
                                {'label': 'B', 'selected': True, 'marker_text': 'X', 'source_ref': {'line_id': 'line-3'}},
                            ],
                            'source_ref': {'line_id': 'line-1'},
                        }
                    ],
                    'layout_lines': [
                        {'text': 'Question', 'line_id': 'line-1'},
                        {'text': 'X A', 'line_id': 'line-2'},
                        {'text': 'X B', 'line_id': 'line-3'},
                    ],
                    'layout_meta': {},
                    'resolved_fields': [],
                }
            ),
            warnings=[],
        )

        explainability = _build_form_explainability(parsed)

        self.assertIsNotNone(explainability)
        self.assertTrue(explainability['repair_plan']['recommended'])
        self.assertEqual(explainability['repair_plan']['trigger_stage'], 'generic_form_understanding')
        self.assertTrue(any(action['kind'] == 'review_group_selection' for action in explainability['repair_plan']['actions']))

    def test_repair_preview_returns_targeted_patch_for_tax_group(self) -> None:
        parsed = ParsedFile(
            file_name='repair_preview.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [
                        {
                            'group_id': 'tax_residency',
                            'question': 'Является ли выгодоприобретатель налоговым резидентом только в РФ',
                            'group_type': 'single_choice',
                            'options': [
                                {
                                    'label': 'ДА',
                                    'selected': True,
                                    'marker_text': 'X',
                                    'source_ref': {'line_id': 'line-2'},
                                },
                                {
                                    'label': 'Не являюсь налоговым резидентом ни в одном государстве',
                                    'selected': True,
                                    'marker_text': 'X',
                                    'source_ref': {'line_id': 'line-3'},
                                },
                            ],
                            'source_ref': {'line_id': 'line-1'},
                        }
                    ],
                    'layout_lines': [
                        {'text': 'Является ли выгодоприобретатель налоговым резидентом только в РФ', 'line_id': 'line-1'},
                        {'text': 'X ДА', 'line_id': 'line-2'},
                        {'text': 'X Не являюсь налоговым резидентом ни в одном государстве', 'line_id': 'line-3'},
                    ],
                    'layout_meta': {'requested_target_fields': ['isResidentRF', 'isTaxResidencyOnlyRF']},
                    'resolved_fields': [],
                }
            ),
            warnings=[],
        )
        payload = RepairPreviewPayload(
            parsed_file=parsed.model_dump() if hasattr(parsed, 'model_dump') else parsed.dict(),
            action={
                'kind': 'repair_ambiguous_resolution',
                'target_field': 'isResidentRF',
                'chunk_refs': {
                    'group_ids': ['tax_residency'],
                    'line_ids': ['line-1', 'line-2', 'line-3'],
                    'scalar_labels': [],
                },
            },
        )

        with patch(
            'form_layout.suggest_form_field_repair',
            return_value=({'status': 'resolved', 'enum_value': 'NOWHERE', 'confidence': 0.77}, []),
        ):
            response = repair_preview(payload)

        self.assertTrue(response['supported'])
        self.assertEqual(response['preview_status'], 'patch_available')
        self.assertEqual(response['proposed_patch']['isResidentRF'], 'NOWHERE')
        self.assertEqual(response['proposed_patch']['isTaxResidencyOnlyRF'], 'NO')
        self.assertEqual(response['local_chunks']['groups'][0]['group_id'], 'tax_residency')
        self.assertEqual([item['field'] for item in response['proposed_resolutions']], ['isResidentRF', 'isTaxResidencyOnlyRF'])

    def test_repair_preview_returns_inspection_only_for_rebuild_action(self) -> None:
        parsed = ParsedFile(
            file_name='repair_preview_generic.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [],
                    'layout_lines': [
                        {'text': 'Question', 'line_id': 'line-1'},
                        {'text': 'X A', 'line_id': 'line-2'},
                    ],
                    'layout_meta': {},
                    'resolved_fields': [],
                }
            ),
            warnings=[],
        )
        payload = RepairPreviewPayload(
            parsed_file=parsed.model_dump() if hasattr(parsed, 'model_dump') else parsed.dict(),
            action={
                'kind': 'rebuild_generic_form_understanding',
                'chunk_refs': {
                    'group_ids': [],
                    'line_ids': ['line-1', 'line-2'],
                    'scalar_labels': [],
                },
            },
        )

        response = repair_preview(payload)

        self.assertTrue(response['supported'])
        self.assertEqual(response['preview_status'], 'inspection_only')
        self.assertEqual(response['proposed_patch'], {})
        self.assertEqual(len(response['local_chunks']['lines']), 2)

    def test_repair_apply_updates_local_truth_without_persistence(self) -> None:
        parsed = ParsedFile(
            file_name='repair_apply.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [
                        {
                            'group_id': 'tax_residency',
                            'question': 'Является ли выгодоприобретатель налоговым резидентом только в РФ',
                            'group_type': 'single_choice',
                            'options': [
                                {'label': 'ДА', 'selected': True, 'marker_text': 'X', 'source_ref': {'line_id': 'line-2'}},
                                {'label': 'Не являюсь налоговым резидентом ни в одном государстве', 'selected': True, 'marker_text': 'X', 'source_ref': {'line_id': 'line-3'}},
                            ],
                            'source_ref': {'line_id': 'line-1'},
                        }
                    ],
                    'layout_lines': [
                        {'text': 'Является ли выгодоприобретатель налоговым резидентом только в РФ', 'line_id': 'line-1'},
                        {'text': 'X ДА', 'line_id': 'line-2'},
                        {'text': 'X Не являюсь налоговым резидентом ни в одном государстве', 'line_id': 'line-3'},
                    ],
                    'layout_meta': {'requested_target_fields': ['isResidentRF', 'isTaxResidencyOnlyRF']},
                    'resolved_fields': [],
                }
            ),
            warnings=[],
        )
        payload = RepairApplyPayload(
            parsed_file=parsed.model_dump() if hasattr(parsed, 'model_dump') else parsed.dict(),
            action={
                'kind': 'repair_ambiguous_resolution',
                'target_field': 'isResidentRF',
                'chunk_refs': {'group_ids': ['tax_residency'], 'line_ids': ['line-1', 'line-2', 'line-3'], 'scalar_labels': []},
            },
            approved_patch={'isResidentRF': 'NOWHERE', 'isTaxResidencyOnlyRF': 'NO'},
        )

        response = repair_apply(payload, current_user=None)

        self.assertTrue(response['applied'])
        self.assertFalse(response['persistence']['persisted'])
        self.assertEqual(response['parsed_file']['rows'][0]['isResidentRF'], 'NOWHERE')
        self.assertEqual(response['parsed_file']['rows'][0]['isTaxResidencyOnlyRF'], 'NO')
        self.assertEqual(response['form_explainability']['final_source_mode'], 'repair_apply')
        updated_field = next(item for item in response['updated_resolved_fields'] if item['field'] == 'isResidentRF')
        self.assertEqual(updated_field['resolved_by'], 'repair_apply')

    def test_repair_apply_persists_generation_version_when_generation_id_present(self) -> None:
        parsed = ParsedFile(
            file_name='repair_apply_persist.txt',
            file_type='txt',
            columns=[],
            rows=[],
            content_type='form',
            document_mode='form_layout_mode',
            extraction_status='text_extracted',
            raw_text='',
            text_blocks=[],
            sections=[],
            kv_pairs=[],
            source_candidates=[],
            sheets=[],
            form_model=_build_form_model(
                {
                    'scalars': [],
                    'groups': [],
                    'layout_lines': [],
                    'layout_meta': {'requested_target_fields': ['organizationName']},
                    'resolved_fields': [],
                }
            ),
            warnings=[],
        )
        payload = RepairApplyPayload(
            parsed_file=parsed.model_dump() if hasattr(parsed, 'model_dump') else parsed.dict(),
            action={
                'kind': 'repair_scalar_resolution',
                'target_field': 'organizationName',
                'chunk_refs': {'group_ids': [], 'line_ids': [], 'scalar_labels': []},
            },
            approved_patch={'organizationName': 'ООО "Рога и копыта"'},
            generation_id=42,
            notes='apply repair',
        )

        with patch(
            'routes.apply_generation_repair_patch',
            return_value={'generation_id': 42, 'version_id': 9, 'version_number': 3, 'session_id': 17},
        ) as apply_mock:
            response = repair_apply(payload, current_user={'id': 'user-1'})

        self.assertTrue(response['persistence']['persisted'])
        self.assertEqual(response['persistence']['generation_id'], 42)
        self.assertEqual(response['persistence']['version_id'], 9)
        self.assertEqual(response['persistence']['version_number'], 3)
        self.assertEqual(response['persistence']['session_id'], 17)
        apply_mock.assert_called_once()

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
