from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path

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

from matcher import map_fields, prepare_field_name
from models import TargetField


class MatcherTests(unittest.TestCase):
    def test_uses_column_order_fallback_when_nothing_matches(self) -> None:
        target_fields = [
            TargetField(name='customerName', type='string'),
            TargetField(name='amount', type='number'),
            TargetField(name='createdAt', type='string'),
        ]

        mappings, warnings = map_fields(['1223', 'hsdh', 'sdvsdv'], target_fields)

        self.assertEqual([mapping.source for mapping in mappings], ['1223', 'hsdh', 'sdvsdv'])
        self.assertTrue(all(mapping.confidence == 'low' for mapping in mappings))
        self.assertTrue(all(mapping.reason == 'position_fallback' for mapping in mappings))
        self.assertEqual(
            warnings,
            ['No semantic column matches found. Used column-order fallback because source and target have the same number of fields.'],
        )

    def test_keeps_semantic_matches_without_forcing_order_fallback(self) -> None:
        target_fields = [
            TargetField(name='customerName', type='string'),
            TargetField(name='amount', type='number'),
        ]

        mappings, warnings = map_fields(['customer_name', 'zzz'], target_fields)

        self.assertEqual(mappings[0].source, 'customer_name')
        self.assertEqual(mappings[0].confidence, 'high')
        self.assertIsNone(mappings[1].source)
        self.assertEqual(mappings[1].confidence, 'none')
        self.assertIn('No source column found for target "amount"', warnings)

    def test_preprocessing_exposes_canonical_tokens_for_ru_en_semantics(self) -> None:
        prepared = prepare_field_name('ФИО клиента')
        self.assertIn('name', prepared['canonical_tokens'])
        self.assertIn('customer', prepared['canonical_tokens'])


if __name__ == '__main__':
    unittest.main()
