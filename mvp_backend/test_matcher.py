from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from matcher import map_fields
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


if __name__ == '__main__':
    unittest.main()
