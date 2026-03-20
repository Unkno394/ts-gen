from __future__ import annotations

import re
from difflib import SequenceMatcher

from models import FieldMapping, TargetField


TOKEN_SPLIT_RE = re.compile(r'[^a-zA-Zа-яА-Я0-9]+')
CAMEL_BOUNDARY_RE = re.compile(r'(?<!^)(?=[A-Z])')



def map_fields(source_columns: list[str], target_fields: list[TargetField]) -> tuple[list[FieldMapping], list[str]]:
    warnings: list[str] = []
    mappings: list[FieldMapping] = []
    used_sources: set[str] = set()

    prepared_sources = [
        {
            'original': col,
            'norm': normalize(col),
            'tokens': set(tokenize(col)),
        }
        for col in source_columns
    ]

    for target in target_fields:
        target_norm = normalize(target.name)
        target_tokens = set(tokenize(target.name))

        exact = next((s for s in prepared_sources if s['norm'] == target_norm and s['original'] not in used_sources), None)
        if exact:
            used_sources.add(exact['original'])
            mappings.append(FieldMapping(source=exact['original'], target=target.name, confidence='high', reason='normalized_exact'))
            continue

        contains = []
        for source in prepared_sources:
            if source['original'] in used_sources:
                continue
            if target_norm in source['norm'] or source['norm'] in target_norm:
                contains.append((source['original'], 0.9, 'contains'))
            else:
                overlap = len(target_tokens & source['tokens'])
                if overlap:
                    score = overlap / max(1, len(target_tokens | source['tokens']))
                    contains.append((source['original'], score, 'token_overlap'))

        if contains:
            contains.sort(key=lambda item: item[1], reverse=True)
            best_source, best_score, reason = contains[0]
            used_sources.add(best_source)
            confidence = 'medium' if best_score >= 0.5 else 'low'
            mappings.append(FieldMapping(source=best_source, target=target.name, confidence=confidence, reason=reason))
            continue

        similarities = []
        for source in prepared_sources:
            if source['original'] in used_sources:
                continue
            ratio = SequenceMatcher(None, target_norm, source['norm']).ratio()
            similarities.append((source['original'], ratio))

        if similarities:
            similarities.sort(key=lambda item: item[1], reverse=True)
            best_source, ratio = similarities[0]
            if ratio >= 0.35:
                used_sources.add(best_source)
                mappings.append(FieldMapping(source=best_source, target=target.name, confidence='low', reason='similarity_fallback'))
                warnings.append(f'Low-confidence match for target "{target.name}": "{best_source}"')
                continue

        mappings.append(FieldMapping(source=None, target=target.name, confidence='none', reason='not_found'))
        warnings.append(f'No source column found for target "{target.name}"')

    if _should_use_position_fallback(source_columns, target_fields, mappings):
        fallback_mappings = [
            FieldMapping(
                source=source_columns[index],
                target=target_fields[index].name,
                confidence='low',
                reason='position_fallback',
            )
            for index in range(len(target_fields))
        ]
        return (
            fallback_mappings,
            [
                'No semantic column matches found. Used column-order fallback because source and target have the same number of fields.'
            ],
        )

    return mappings, warnings



def normalize(value: str) -> str:
    return ''.join(tokenize(value))



def tokenize(value: str) -> list[str]:
    with_boundaries = CAMEL_BOUNDARY_RE.sub(' ', value)
    parts = TOKEN_SPLIT_RE.split(with_boundaries.lower())
    return [p for p in parts if p]


def _should_use_position_fallback(
    source_columns: list[str],
    target_fields: list[TargetField],
    mappings: list[FieldMapping],
) -> bool:
    if not source_columns or len(source_columns) != len(target_fields):
        return False

    return all(mapping.source is None for mapping in mappings)
