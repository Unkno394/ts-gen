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

from benchmarking import evaluate_graph_benchmark, evaluate_role_benchmark


class BenchmarkingTests(unittest.TestCase):
    def test_role_benchmark_returns_non_empty_summary(self) -> None:
        summary = evaluate_role_benchmark()

        self.assertGreater(summary['case_count'], 0)
        self.assertIn('exact_match_rate', summary)
        self.assertIn('triplet_accuracy', summary)
        self.assertEqual(len(summary['cases']), summary['case_count'])

    def test_graph_benchmark_runs_graph_on_and_off(self) -> None:
        summary = evaluate_graph_benchmark()

        self.assertGreater(summary['case_count'], 0)
        self.assertIn('graph_off', summary)
        self.assertIn('graph_on', summary)
        self.assertIn('case_deltas', summary)
        self.assertEqual(summary['graph_off']['case_count'], summary['case_count'])
        self.assertEqual(summary['graph_on']['case_count'], summary['case_count'])


if __name__ == '__main__':
    unittest.main()
