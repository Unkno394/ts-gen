from __future__ import annotations

import sys
import unittest
from pathlib import Path

BACKEND_DIR = Path(__file__).resolve().parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from benchmarking import evaluate_graph_benchmark, evaluate_role_benchmark, run_graph_case


def _index_cases_by_id(cases: list[dict]) -> dict[str, dict]:
    return {str(case.get('case_id')): case for case in cases}


class BenchmarkExpectationTests(unittest.TestCase):
    def test_role_benchmark_reports_triplet_and_component_accuracy(self) -> None:
        summary = evaluate_role_benchmark()

        self.assertIn('triplet_accuracy', summary)
        self.assertIn('entity_accuracy', summary)
        self.assertIn('attribute_accuracy', summary)
        self.assertIn('role_accuracy', summary)
        self.assertIn('category_breakdown', summary)
        self.assertEqual(summary['triplet_accuracy'], summary['exact_match_rate'])
        self.assertGreaterEqual(summary['entity_accuracy'], summary['triplet_accuracy'])
        self.assertGreaterEqual(summary['attribute_accuracy'], summary['triplet_accuracy'])
        self.assertGreaterEqual(summary['role_accuracy'], summary['triplet_accuracy'])
        self.assertGreaterEqual(summary['entity_accuracy'], 0.90)
        self.assertGreaterEqual(summary['attribute_accuracy'], 0.90)
        self.assertGreaterEqual(summary['role_accuracy'], 0.90)
        self.assertGreaterEqual(summary['triplet_accuracy'], 0.85)

    def test_role_benchmark_has_minimum_semantic_coverage(self) -> None:
        summary = evaluate_role_benchmark()
        cases = list(summary['cases'])
        categories = {str(case['category']) for case in cases}
        entities = {str(case['expected']['entity']) for case in cases}
        attributes = {str(case['expected']['attribute']) for case in cases}
        roles = {str(case['expected']['role']) for case in cases}

        self.assertGreaterEqual(summary['case_count'], 29)
        self.assertEqual(
            categories,
            {'core', 'homonym', 'neutral', 'domain', 'mixed_language'},
        )
        self.assertGreaterEqual(len(entities), 6)
        self.assertGreaterEqual(len(attributes), 10)
        self.assertGreaterEqual(len(roles), 5)
        self.assertLess(summary['category_breakdown']['mixed_language']['triplet_accuracy'], 1.0)

    def test_graph_benchmark_reports_precision_and_case_deltas(self) -> None:
        summary = evaluate_graph_benchmark()

        self.assertIn('resolved_rate', summary['graph_off'])
        self.assertIn('resolved_rate', summary['graph_on'])
        self.assertIn('true_positive_matches', summary['graph_off'])
        self.assertIn('predicted_matches', summary['graph_on'])
        self.assertIn('precision', summary['graph_off'])
        self.assertIn('precision', summary['graph_on'])
        self.assertIn('category_breakdown', summary)
        self.assertIn('category_delta', summary)
        self.assertIn('gain_cases', summary)
        self.assertIn('regression_cases', summary)
        self.assertIn('safe_help_rate', summary)
        self.assertIn('case_deltas', summary)
        self.assertEqual(len(summary['case_deltas']), summary['case_count'])

    def test_graph_improves_exact_and_unresolved_without_precision_regression(self) -> None:
        summary = evaluate_graph_benchmark()
        graph_off = summary['graph_off']
        graph_on = summary['graph_on']

        self.assertGreaterEqual(graph_on['exact_match_rate'], graph_off['exact_match_rate'])
        self.assertLessEqual(graph_on['unresolved_rate'], graph_off['unresolved_rate'])
        self.assertLessEqual(graph_on['false_positive_rate'], graph_off['false_positive_rate'])
        self.assertGreaterEqual(graph_on['resolved_rate'], graph_off['resolved_rate'])
        self.assertGreaterEqual(graph_off['precision'], 0.0)
        self.assertLessEqual(graph_off['precision'], 1.0)
        self.assertGreaterEqual(graph_on['precision'], 0.0)
        self.assertLessEqual(graph_on['precision'], 1.0)
        self.assertEqual(graph_off['precision'], graph_off['true_positive_matches'] / graph_off['predicted_matches'])
        self.assertEqual(graph_on['precision'], graph_on['true_positive_matches'] / graph_on['predicted_matches'])
        self.assertGreaterEqual(graph_on['precision'], graph_off['precision'])

    def test_graph_fallback_does_not_introduce_false_positive(self) -> None:
        for case_id in (
            'timestamp_conflict_should_stay_unresolved',
            'amount_vs_revenue_should_stay_unresolved',
            'customer_vs_company_should_stay_unresolved',
            'owner_vs_product_id_should_stay_unresolved',
        ):
            off = run_graph_case(case_id, graph_enabled=False)
            on = run_graph_case(case_id, graph_enabled=True)

            self.assertEqual(off['predicted_source'], None)
            self.assertEqual(on['predicted_source'], None)
            self.assertFalse(off['counts_as_exact_match'])
            self.assertFalse(on['counts_as_exact_match'])
            self.assertFalse(off['counts_as_false_positive'])
            self.assertFalse(on['counts_as_false_positive'])
            self.assertFalse(off['is_exact_match'])
            self.assertFalse(on['is_exact_match'])
            self.assertTrue(off['is_correctly_unresolved'])
            self.assertTrue(on['is_correctly_unresolved'])

    def test_graph_improves_transitive_identifier_case(self) -> None:
        off = run_graph_case('transitive_identifier_match', graph_enabled=False)
        on = run_graph_case('transitive_identifier_match', graph_enabled=True)

        self.assertEqual(off['predicted_source'], None)
        self.assertFalse(off['is_exact_match'])
        self.assertTrue(on['is_exact_match'])
        self.assertEqual(on['predicted_source'], 'id')
        self.assertEqual(on['source_of_truth'], 'semantic_graph')

    def test_concept_cluster_amount_fallback_has_no_claimed_gain_yet(self) -> None:
        off = run_graph_case('concept_cluster_amount_fallback', graph_enabled=False)
        on = run_graph_case('concept_cluster_amount_fallback', graph_enabled=True)

        self.assertEqual(off['is_exact_match'], on['is_exact_match'])
        self.assertEqual(off['predicted_source'], on['predicted_source'])
        self.assertEqual(off['predicted_source'], None)

    def test_cold_start_probes_are_present_and_currently_show_no_gain(self) -> None:
        for case_id in (
            'purchaser_identifier_cold_start',
            'signup_timestamp_cold_start',
            'payment_gross_cold_start',
        ):
            off = run_graph_case(case_id, graph_enabled=False)
            on = run_graph_case(case_id, graph_enabled=True)

            self.assertEqual(off['predicted_source'], None)
            self.assertEqual(on['predicted_source'], None)
            self.assertFalse(off['is_exact_match'])
            self.assertFalse(on['is_exact_match'])

    def test_graph_improvement_is_currently_limited_to_transitive_link_case(self) -> None:
        summary = evaluate_graph_benchmark()
        case_deltas = _index_cases_by_id(summary['case_deltas'])

        self.assertEqual(summary['gain_cases'], ['transitive_identifier_match'])
        self.assertEqual(summary['regression_cases'], [])
        self.assertEqual(summary['safe_help_cases'], ['transitive_identifier_match'])
        self.assertEqual(summary['safe_help_rate'], round(1 / summary['case_count'], 4))
        self.assertEqual(summary['improved_case_ids'], ['transitive_identifier_match'])
        self.assertEqual(case_deltas['transitive_identifier_match']['outcome'], 'improved')
        self.assertEqual(case_deltas['concept_cluster_amount_fallback']['outcome'], 'unchanged')
        self.assertEqual(case_deltas['timestamp_conflict_should_stay_unresolved']['outcome'], 'unchanged')

    def test_graph_category_breakdown_shows_where_gain_exists(self) -> None:
        summary = evaluate_graph_benchmark()
        graph_off = summary['category_breakdown']['graph_off']
        graph_on = summary['category_breakdown']['graph_on']
        category_delta = summary['category_delta']

        self.assertIn('transitive_link', graph_off)
        self.assertIn('cold_start', graph_off)
        self.assertIn('conflict_guard', graph_off)
        self.assertIn('concept_fallback_guard', graph_off)
        self.assertIn('role_fallback_guard', graph_off)

        self.assertGreaterEqual(graph_off['cold_start']['case_count'], 4)
        self.assertLess(graph_off['transitive_link']['exact_match_rate'], graph_on['transitive_link']['exact_match_rate'])
        self.assertEqual(graph_on['conflict_guard']['false_positive_rate'], 0.0)
        self.assertEqual(graph_on['concept_fallback_guard']['false_positive_rate'], 0.0)
        self.assertEqual(graph_on['role_fallback_guard']['false_positive_rate'], 0.0)
        self.assertEqual(graph_off['cold_start']['exact_match_rate'], graph_on['cold_start']['exact_match_rate'])
        self.assertEqual(graph_off['cold_start']['unresolved_rate'], graph_on['cold_start']['unresolved_rate'])
        self.assertEqual(category_delta['conflict_guard']['false_positive_rate'], 0.0)
        self.assertGreater(category_delta['transitive_link']['exact_match_rate'], 0.0)
        self.assertEqual(category_delta['cold_start']['exact_match_rate'], 0.0)
        self.assertEqual(category_delta['cold_start']['unresolved_rate'], 0.0)
        self.assertEqual(category_delta['concept_fallback_guard']['false_positive_rate'], 0.0)


if __name__ == '__main__':
    unittest.main()
