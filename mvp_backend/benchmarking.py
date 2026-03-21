from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import types
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from unittest.mock import patch

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

import storage
from learning_pipeline import resolve_generation_mappings_detailed
from matcher import prepare_field_name
from models import TargetField

BENCHMARKS_DIR = Path(__file__).resolve().parent / 'benchmarks'
ROLE_BENCHMARK_PATH = BENCHMARKS_DIR / 'role_benchmark.json'
GRAPH_BENCHMARK_PATH = BENCHMARKS_DIR / 'graph_benchmark.json'


def load_role_benchmark_cases(path: Path | None = None) -> list[dict[str, Any]]:
    payload = json.loads((path or ROLE_BENCHMARK_PATH).read_text(encoding='utf-8'))
    return payload if isinstance(payload, list) else []


def load_graph_benchmark_cases(path: Path | None = None) -> list[dict[str, Any]]:
    payload = json.loads((path or GRAPH_BENCHMARK_PATH).read_text(encoding='utf-8'))
    return payload if isinstance(payload, list) else []


def run_graph_case(
    case_id: str,
    *,
    graph_enabled: bool,
    cases: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    graph_cases = cases or load_graph_benchmark_cases()
    case = next((item for item in graph_cases if str(item.get('case_id')) == case_id), None)
    if case is None:
        raise KeyError(f'Graph benchmark case not found: {case_id}')

    with _isolated_storage_runtime():
        result = _run_graph_mapping_benchmark([case], enable_semantic_graph=graph_enabled)

    case_result = dict(result['cases'][0])
    mappings = list(case_result.get('mappings') or [])
    if len(mappings) != 1:
        raise ValueError(f'run_graph_case expects exactly one target mapping, got {len(mappings)} for case {case_id}')

    mapping = dict(mappings[0])
    return {
        'case_id': case_id,
        'category': case_result.get('category'),
        'focus': case_result.get('focus'),
        'expected_graph_behavior': case_result.get('expected_graph_behavior'),
        'graph_enabled': graph_enabled,
        'predicted_source': mapping.get('predicted_source'),
        'predicted_match': mapping.get('predicted_source'),
        'expected_source': mapping.get('expected_source'),
        'is_exact_match': mapping.get('is_exact_match'),
        'is_correctly_unresolved': mapping.get('is_correctly_unresolved'),
        'is_false_positive': mapping.get('is_false_positive'),
        'counts_as_exact_match': mapping.get('counts_as_exact_match'),
        'counts_as_false_positive': mapping.get('counts_as_false_positive'),
        'source_of_truth': mapping.get('source_of_truth'),
        'status': mapping.get('status'),
        'summary': case_result.get('summary'),
        'warnings': case_result.get('warnings'),
        'raw_case': case_result,
    }


def evaluate_role_benchmark(cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    role_cases = cases or load_role_benchmark_cases()
    if not role_cases:
        return {
            'case_count': 0,
            'exact_match_rate': 0.0,
            'triplet_accuracy': 0.0,
            'entity_accuracy': 0.0,
            'attribute_accuracy': 0.0,
            'role_accuracy': 0.0,
            'category_breakdown': {},
            'cases': [],
        }

    exact_match_count = 0
    entity_match_count = 0
    attribute_match_count = 0
    role_match_count = 0
    category_stats: dict[str, dict[str, int]] = {}
    results: list[dict[str, Any]] = []

    for case in role_cases:
        field_name = str(case.get('field_name') or '')
        category = str(case.get('category') or 'uncategorized')
        prepared = prepare_field_name(field_name)
        actual = {
            'entity': prepared.get('entity_token'),
            'attribute': prepared.get('attribute_token'),
            'role': prepared.get('role_label'),
        }
        expected = {
            'entity': case.get('expected_entity'),
            'attribute': case.get('expected_attribute'),
            'role': case.get('expected_role'),
        }
        entity_match = actual['entity'] == expected['entity']
        attribute_match = actual['attribute'] == expected['attribute']
        role_match = actual['role'] == expected['role']
        exact_match = entity_match and attribute_match and role_match

        exact_match_count += 1 if exact_match else 0
        entity_match_count += 1 if entity_match else 0
        attribute_match_count += 1 if attribute_match else 0
        role_match_count += 1 if role_match else 0

        bucket = category_stats.setdefault(
            category,
            {
                'case_count': 0,
                'exact_match_count': 0,
                'entity_match_count': 0,
                'attribute_match_count': 0,
                'role_match_count': 0,
            },
        )
        bucket['case_count'] += 1
        bucket['exact_match_count'] += 1 if exact_match else 0
        bucket['entity_match_count'] += 1 if entity_match else 0
        bucket['attribute_match_count'] += 1 if attribute_match else 0
        bucket['role_match_count'] += 1 if role_match else 0

        results.append(
            {
                'field_name': field_name,
                'category': category,
                'expected': expected,
                'actual': actual,
                'exact_match': exact_match,
                'entity_match': entity_match,
                'attribute_match': attribute_match,
                'role_match': role_match,
                'notes': case.get('notes'),
            }
        )

    case_count = len(role_cases)
    category_breakdown: dict[str, dict[str, Any]] = {}
    for category, bucket in category_stats.items():
        denominator = max(int(bucket['case_count']), 1)
        category_breakdown[category] = {
            'case_count': bucket['case_count'],
            'triplet_accuracy': round(bucket['exact_match_count'] / denominator, 4),
            'entity_accuracy': round(bucket['entity_match_count'] / denominator, 4),
            'attribute_accuracy': round(bucket['attribute_match_count'] / denominator, 4),
            'role_accuracy': round(bucket['role_match_count'] / denominator, 4),
        }

    return {
        'case_count': case_count,
        'exact_match_rate': round(exact_match_count / case_count, 4),
        'triplet_accuracy': round(exact_match_count / case_count, 4),
        'entity_accuracy': round(entity_match_count / case_count, 4),
        'attribute_accuracy': round(attribute_match_count / case_count, 4),
        'role_accuracy': round(role_match_count / case_count, 4),
        'category_breakdown': category_breakdown,
        'cases': results,
    }


def evaluate_graph_benchmark(cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    graph_cases = cases or load_graph_benchmark_cases()
    with _isolated_storage_runtime():
        graph_off = _run_graph_mapping_benchmark(graph_cases, enable_semantic_graph=False)
    with _isolated_storage_runtime():
        graph_on = _run_graph_mapping_benchmark(graph_cases, enable_semantic_graph=True)

    case_deltas = _build_graph_case_deltas(graph_cases, graph_off['cases'], graph_on['cases'])
    improved_case_ids = [case['case_id'] for case in case_deltas if case['outcome'] == 'improved']
    regressed_case_ids = [case['case_id'] for case in case_deltas if case['outcome'] == 'regressed']
    unchanged_case_ids = [case['case_id'] for case in case_deltas if case['outcome'] == 'unchanged']
    category_breakdown = {
        'graph_off': _build_category_breakdown(graph_off['cases']),
        'graph_on': _build_category_breakdown(graph_on['cases']),
    }
    safe_help_cases = [
        case['case_id']
        for case in case_deltas
        if case['outcome'] == 'improved' and float((case.get('delta') or {}).get('false_positive_rate') or 0.0) <= 0.0
    ]

    return {
        'case_count': len(graph_cases),
        'graph_off': graph_off,
        'graph_on': graph_on,
        'delta': {
            'exact_match_rate': round(graph_on['exact_match_rate'] - graph_off['exact_match_rate'], 4),
            'false_positive_rate': round(graph_on['false_positive_rate'] - graph_off['false_positive_rate'], 4),
            'unresolved_rate': round(graph_on['unresolved_rate'] - graph_off['unresolved_rate'], 4),
            'correctly_unresolved_rate': round(
                graph_on['correctly_unresolved_rate'] - graph_off['correctly_unresolved_rate'],
                4,
            ),
            'correct_outcome_rate': round(graph_on['correct_outcome_rate'] - graph_off['correct_outcome_rate'], 4),
            'resolved_rate': round(graph_on['resolved_rate'] - graph_off['resolved_rate'], 4),
            'precision': _subtract_optional_metric(graph_on.get('precision'), graph_off.get('precision')),
            'accepted_after_review_rate': round(graph_on['accepted_after_review_rate'] - graph_off['accepted_after_review_rate'], 4),
        },
        'category_breakdown': category_breakdown,
        'category_delta': _build_category_delta(category_breakdown['graph_off'], category_breakdown['graph_on']),
        'gain_cases': improved_case_ids,
        'regression_cases': regressed_case_ids,
        'safe_help_rate': round(len(safe_help_cases) / max(len(graph_cases), 1), 4),
        'safe_help_cases': safe_help_cases,
        'improved_case_ids': improved_case_ids,
        'regressed_case_ids': regressed_case_ids,
        'unchanged_case_ids': unchanged_case_ids,
        'case_deltas': case_deltas,
    }


def run_all_benchmarks() -> dict[str, Any]:
    return {
        'role_benchmark': evaluate_role_benchmark(),
        'graph_benchmark': evaluate_graph_benchmark(),
    }


def _run_graph_mapping_benchmark(cases: list[dict[str, Any]], *, enable_semantic_graph: bool) -> dict[str, Any]:
    if not cases:
        return {
            'case_count': 0,
            'target_count': 0,
            'true_positive_matches': 0,
            'predicted_matches': 0,
            'correctly_unresolved_count': 0,
            'resolved_count': 0,
            'exact_match_rate': 0.0,
            'false_positive_rate': 0.0,
            'unresolved_rate': 0.0,
            'correctly_unresolved_rate': 0.0,
            'correct_outcome_rate': 0.0,
            'resolved_rate': 0.0,
            'precision': None,
            'accepted_after_review_rate': 0.0,
            'source_of_truth_breakdown': {},
            'cases': [],
        }

    exact_match_count = 0
    false_positive_count = 0
    unresolved_count = 0
    correctly_unresolved_count = 0
    accepted_after_review_count = 0
    target_count = 0
    source_of_truth_breakdown: dict[str, int] = {}
    case_results: list[dict[str, Any]] = []

    with patch('learning_pipeline.rank_mapping_candidate', return_value=(None, [])):
        for case in cases:
            for correction in case.get('seed_corrections', []) or []:
                storage.save_correction_session(
                    user_id=str(case.get('user_id') or 'benchmark-user'),
                    session_type='feedback_loop',
                    corrections=[
                        {
                            'correction_type': 'mapping_override',
                            'source_field': correction.get('source_field'),
                            'target_field': correction.get('target_field'),
                            'confidence_after': correction.get('confidence_after', 1.0),
                            'accepted': correction.get('accepted', True),
                        }
                    ],
                )

            target_fields = [
                TargetField(
                    name=str(item.get('name') or ''),
                    type=str(item.get('type') or 'string'),
                )
                for item in case.get('target_fields', []) or []
                if item.get('name')
            ]
            result = resolve_generation_mappings_detailed(
                source_columns=[str(value) for value in case.get('source_columns', []) or []],
                source_rows=[dict(row) for row in case.get('source_rows', []) or [] if isinstance(row, dict)],
                target_fields=target_fields,
                user_id=str(case.get('user_id') or 'benchmark-user'),
                schema_fingerprint_id=None,
                enable_semantic_graph=enable_semantic_graph,
            )
            expected_sources = case.get('expected_sources') or {}
            mapping_results: list[dict[str, Any]] = []

            for mapping in result['mappings']:
                target_count += 1
                expected_source = expected_sources.get(mapping.target)
                actual_source = mapping.source
                predicted_source = actual_source
                unresolved = actual_source is None
                is_exact_match = actual_source is not None and actual_source == expected_source
                is_correctly_unresolved = actual_source is None and expected_source is None
                is_false_positive = actual_source is not None and actual_source != expected_source
                accepted_after_review = is_exact_match and mapping.status == 'suggested'

                exact_match_count += 1 if is_exact_match else 0
                unresolved_count += 1 if unresolved else 0
                correctly_unresolved_count += 1 if is_correctly_unresolved else 0
                false_positive_count += 1 if is_false_positive else 0
                accepted_after_review_count += 1 if accepted_after_review else 0
                source_of_truth = str(mapping.source_of_truth)
                source_of_truth_breakdown[source_of_truth] = source_of_truth_breakdown.get(source_of_truth, 0) + 1

                mapping_results.append(
                    {
                        'target': mapping.target,
                        'expected_source': expected_source,
                        'actual_source': actual_source,
                        'predicted_source': predicted_source,
                        'predicted_match': predicted_source,
                        'source_of_truth': source_of_truth,
                        'status': mapping.status,
                        'confidence': mapping.confidence,
                        'exact_match': is_exact_match,
                        'is_exact_match': is_exact_match,
                        'is_correctly_unresolved': is_correctly_unresolved,
                        'false_positive': is_false_positive,
                        'is_false_positive': is_false_positive,
                        'unresolved': unresolved,
                        'counts_as_exact_match': is_exact_match,
                        'counts_as_false_positive': is_false_positive,
                        'accepted_after_review': accepted_after_review,
                    }
                )

            case_results.append(
                {
                    'case_id': case.get('case_id'),
                    'category': case.get('category'),
                    'focus': case.get('focus'),
                    'expected_graph_behavior': case.get('expected_graph_behavior'),
                    'enable_semantic_graph': enable_semantic_graph,
                    'mappings': mapping_results,
                    'summary': _summarize_mapping_results(mapping_results),
                    'warnings': result['warnings'],
                }
            )

    denominator = max(target_count, 1)
    resolved_count = target_count - unresolved_count
    correct_outcome_count = exact_match_count + correctly_unresolved_count
    return {
        'case_count': len(cases),
        'target_count': target_count,
        'true_positive_matches': exact_match_count,
        'predicted_matches': resolved_count,
        'correctly_unresolved_count': correctly_unresolved_count,
        'resolved_count': resolved_count,
        'resolved_exact_match_count': exact_match_count,
        'exact_match_rate': round(exact_match_count / denominator, 4),
        'false_positive_rate': round(false_positive_count / denominator, 4),
        'unresolved_rate': round(unresolved_count / denominator, 4),
        'correctly_unresolved_rate': round(correctly_unresolved_count / denominator, 4),
        'correct_outcome_rate': round(correct_outcome_count / denominator, 4),
        'resolved_rate': round(resolved_count / denominator, 4),
        'precision': _compute_precision(exact_match_count, resolved_count),
        'accepted_after_review_rate': round(accepted_after_review_count / denominator, 4),
        'source_of_truth_breakdown': source_of_truth_breakdown,
        'cases': case_results,
    }


def _compute_precision(exact_match_count: int, resolved_count: int) -> float | None:
    if resolved_count <= 0:
        return None
    return round(exact_match_count / resolved_count, 4)


def _subtract_optional_metric(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 4)


def _summarize_mapping_results(mapping_results: list[dict[str, Any]]) -> dict[str, Any]:
    target_count = len(mapping_results)
    exact_match_count = sum(1 for item in mapping_results if item.get('is_exact_match'))
    resolved_exact_match_count = exact_match_count
    false_positive_count = sum(1 for item in mapping_results if item.get('is_false_positive'))
    unresolved_count = sum(1 for item in mapping_results if item.get('unresolved'))
    correctly_unresolved_count = sum(1 for item in mapping_results if item.get('is_correctly_unresolved'))
    accepted_after_review_count = sum(1 for item in mapping_results if item.get('accepted_after_review'))
    resolved_count = target_count - unresolved_count
    correct_outcome_count = exact_match_count + correctly_unresolved_count
    denominator = max(target_count, 1)
    return {
        'target_count': target_count,
        'true_positive_matches': exact_match_count,
        'predicted_matches': resolved_count,
        'resolved_count': resolved_count,
        'exact_match_count': exact_match_count,
        'resolved_exact_match_count': resolved_exact_match_count,
        'false_positive_count': false_positive_count,
        'unresolved_count': unresolved_count,
        'correctly_unresolved_count': correctly_unresolved_count,
        'accepted_after_review_count': accepted_after_review_count,
        'exact_match_rate': round(exact_match_count / denominator, 4),
        'false_positive_rate': round(false_positive_count / denominator, 4),
        'unresolved_rate': round(unresolved_count / denominator, 4),
        'correctly_unresolved_rate': round(correctly_unresolved_count / denominator, 4),
        'correct_outcome_rate': round(correct_outcome_count / denominator, 4),
        'resolved_rate': round(resolved_count / denominator, 4),
        'precision': _compute_precision(exact_match_count, resolved_count),
    }


def _build_category_breakdown(case_results: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for case in case_results:
        category = str(case.get('category') or 'uncategorized')
        grouped.setdefault(category, []).append(case)

    breakdown: dict[str, dict[str, Any]] = {}
    for category, cases in grouped.items():
        target_count = sum(int((case.get('summary') or {}).get('target_count') or 0) for case in cases)
        true_positive_matches = sum(int((case.get('summary') or {}).get('true_positive_matches') or 0) for case in cases)
        predicted_matches = sum(int((case.get('summary') or {}).get('predicted_matches') or 0) for case in cases)
        false_positive_count = sum(int((case.get('summary') or {}).get('false_positive_count') or 0) for case in cases)
        unresolved_count = sum(int((case.get('summary') or {}).get('unresolved_count') or 0) for case in cases)
        correctly_unresolved_count = sum(
            int((case.get('summary') or {}).get('correctly_unresolved_count') or 0) for case in cases
        )
        exact_match_count = sum(int((case.get('summary') or {}).get('exact_match_count') or 0) for case in cases)
        denominator = max(target_count, 1)
        breakdown[category] = {
            'case_count': len(cases),
            'target_count': target_count,
            'true_positive_matches': true_positive_matches,
            'predicted_matches': predicted_matches,
            'exact_match_rate': round(exact_match_count / denominator, 4),
            'false_positive_rate': round(false_positive_count / denominator, 4),
            'unresolved_rate': round(unresolved_count / denominator, 4),
            'correctly_unresolved_rate': round(correctly_unresolved_count / denominator, 4),
            'precision': _compute_precision(true_positive_matches, predicted_matches),
        }

    return breakdown


def _build_category_delta(
    graph_off_breakdown: dict[str, dict[str, Any]],
    graph_on_breakdown: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    category_names = sorted(set(graph_off_breakdown) | set(graph_on_breakdown))
    category_delta: dict[str, dict[str, Any]] = {}
    for category in category_names:
        graph_off_metrics = graph_off_breakdown.get(category, {})
        graph_on_metrics = graph_on_breakdown.get(category, {})
        category_delta[category] = {
            'exact_match_rate': round(
                float(graph_on_metrics.get('exact_match_rate') or 0.0) - float(graph_off_metrics.get('exact_match_rate') or 0.0),
                4,
            ),
            'unresolved_rate': round(
                float(graph_on_metrics.get('unresolved_rate') or 0.0) - float(graph_off_metrics.get('unresolved_rate') or 0.0),
                4,
            ),
            'false_positive_rate': round(
                float(graph_on_metrics.get('false_positive_rate') or 0.0)
                - float(graph_off_metrics.get('false_positive_rate') or 0.0),
                4,
            ),
            'precision': _subtract_optional_metric(
                graph_on_metrics.get('precision'),
                graph_off_metrics.get('precision'),
            ),
        }

    return category_delta


def _build_graph_case_deltas(
    graph_cases: list[dict[str, Any]],
    graph_off_cases: list[dict[str, Any]],
    graph_on_cases: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    off_by_id = {str(case.get('case_id')): case for case in graph_off_cases}
    on_by_id = {str(case.get('case_id')): case for case in graph_on_cases}
    deltas: list[dict[str, Any]] = []

    for benchmark_case in graph_cases:
        case_id = str(benchmark_case.get('case_id') or '')
        graph_off_case = off_by_id.get(case_id, {})
        graph_on_case = on_by_id.get(case_id, {})
        graph_off_summary = dict(graph_off_case.get('summary') or {})
        graph_on_summary = dict(graph_on_case.get('summary') or {})
        exact_delta = round(
            float(graph_on_summary.get('exact_match_rate') or 0.0) - float(graph_off_summary.get('exact_match_rate') or 0.0),
            4,
        )
        false_positive_delta = round(
            float(graph_on_summary.get('false_positive_rate') or 0.0)
            - float(graph_off_summary.get('false_positive_rate') or 0.0),
            4,
        )
        unresolved_delta = round(
            float(graph_on_summary.get('unresolved_rate') or 0.0) - float(graph_off_summary.get('unresolved_rate') or 0.0),
            4,
        )
        resolved_delta = round(
            float(graph_on_summary.get('resolved_rate') or 0.0) - float(graph_off_summary.get('resolved_rate') or 0.0),
            4,
        )
        precision_delta = _subtract_optional_metric(
            graph_on_summary.get('precision'),
            graph_off_summary.get('precision'),
        )

        outcome = 'unchanged'
        if exact_delta > 0 or unresolved_delta < 0 or false_positive_delta < 0:
            outcome = 'improved'
        elif exact_delta < 0 or unresolved_delta > 0 or false_positive_delta > 0:
            outcome = 'regressed'

        deltas.append(
            {
                'case_id': case_id,
                'category': benchmark_case.get('category'),
                'focus': benchmark_case.get('focus'),
                'expected_graph_behavior': benchmark_case.get('expected_graph_behavior'),
                'graph_off': graph_off_summary,
                'graph_on': graph_on_summary,
                'delta': {
                    'exact_match_rate': exact_delta,
                    'false_positive_rate': false_positive_delta,
                    'unresolved_rate': unresolved_delta,
                    'resolved_rate': resolved_delta,
                    'precision': precision_delta,
                },
                'outcome': outcome,
            }
        )

    return deltas


@contextmanager
def _isolated_storage_runtime() -> Iterator[None]:
    temp_root = Path(tempfile.mkdtemp(prefix='tsgen-benchmark-'))
    runtime_dir = temp_root / '.runtime'
    base_dir = runtime_dir / 'storage'
    upload_dir = base_dir / 'uploads'
    guest_dir = upload_dir / 'guest'
    auth_dir = upload_dir / 'authorized'

    if storage._db_client is not None:
        storage._db_client.close()
        storage._db_client = None

    patcher = patch.multiple(
        storage,
        PROJECT_DIR=temp_root,
        RUNTIME_DIR=runtime_dir,
        DB_PATH=runtime_dir / 'app.sqlite',
        LEGACY_DB_PATH=runtime_dir / 'history.db',
        BASE_DIR=base_dir,
        UPLOAD_DIR=upload_dir,
        GUEST_DIR=guest_dir,
        AUTH_DIR=auth_dir,
    )
    patcher.start()
    try:
        storage._db_client = None
        storage.init_db()
        yield
    finally:
        if storage._db_client is not None:
            storage._db_client.close()
            storage._db_client = None
        patcher.stop()
        shutil.rmtree(temp_root, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description='Run offline role and graph benchmarks for TSGen.')
    parser.add_argument(
        'benchmark',
        choices=('all', 'roles', 'graph'),
        default='all',
        nargs='?',
        help='Benchmark set to run.',
    )
    args = parser.parse_args()

    if args.benchmark == 'roles':
        payload = {'role_benchmark': evaluate_role_benchmark()}
    elif args.benchmark == 'graph':
        payload = {'graph_benchmark': evaluate_graph_benchmark()}
    else:
        payload = run_all_benchmarks()

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
