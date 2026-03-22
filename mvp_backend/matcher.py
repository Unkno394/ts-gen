from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any

from models import FieldMapping, TargetField


TOKEN_SPLIT_RE = re.compile(r'[^a-zA-Zа-яА-Я0-9]+')
CAMEL_BOUNDARY_RE = re.compile(r'(?<=[a-zа-я0-9])(?=[A-ZА-Я])')
DIGIT_BOUNDARY_RE = re.compile(r'(?<=[A-Za-zА-Яа-я])(?=[0-9])|(?<=[0-9])(?=[A-Za-zА-Яа-я])')

CYRILLIC_TO_LATIN = {
    'а': 'a',
    'б': 'b',
    'в': 'v',
    'г': 'g',
    'д': 'd',
    'е': 'e',
    'ё': 'e',
    'ж': 'zh',
    'з': 'z',
    'и': 'i',
    'й': 'y',
    'к': 'k',
    'л': 'l',
    'м': 'm',
    'н': 'n',
    'о': 'o',
    'п': 'p',
    'р': 'r',
    'с': 's',
    'т': 't',
    'у': 'u',
    'ф': 'f',
    'х': 'h',
    'ц': 'ts',
    'ч': 'ch',
    'ш': 'sh',
    'щ': 'sch',
    'ъ': '',
    'ы': 'y',
    'ь': '',
    'э': 'e',
    'ю': 'yu',
    'я': 'ya',
}

SYNONYM_GROUPS = {
    'date': {'date', 'day', 'дата', 'data'},
    'created': {'create', 'created', 'creation', 'создан', 'создания', 'sozdan', 'sozdaniya'},
    'updated': {'update', 'updated', 'lastupdate', 'обновлен', 'обновления', 'obnovlen', 'obnovleniya'},
    'id': {'id', 'identifier', 'code', 'код', 'номер', 'identyfikator', 'nomer', 'kod'},
    'amount': {'amount', 'sum', 'total', 'сумма', 'стоимость', 'итого', 'summa', 'itogo'},
    'revenue': {'revenue', 'income', 'выручка', 'доход', 'vyruchka', 'dohod'},
    'name': {'name', 'title', 'fullname', 'full_name', 'название', 'имя', 'фио', 'nazvanie', 'imya', 'fio'},
    'description': {
        'description',
        'descr',
        'details',
        'comment',
        'описание',
        'комментарий',
        'наименование',
        'opisanie',
        'kommentariy',
        'naimenovanie',
    },
    'product': {'product', 'item', 'sku', 'товар', 'продукт', 'tovar', 'produkt'},
    'quantity': {'quantity', 'qty', 'count', 'количество', 'qty.', 'kolichestvo'},
    'organization': {'organization', 'org', 'company', 'организация', 'компания', 'organizaciya', 'kompaniya'},
    'creator': {'creator', 'author', 'owner', 'создатель', 'автор', 'sozdatel', 'avtor'},
    'responsible': {'responsible', 'manager', 'owner', 'assignee', 'ответственный', 'menedzher', 'otvetstvennyy'},
    'deal': {'deal', 'opportunity', 'сделка', 'sdelka'},
    'source': {'source', 'origin', 'channel', 'источник', 'канал', 'istochnik', 'kanal', 'kanalprodazh'},
    'partner': {'partner', 'vendor', 'supplier', 'партнер', 'партнёр', 'postavshik', 'vendorname'},
    'license': {'license', 'licence', 'лицензия', 'licenziya', 'subscription', 'подписка'},
    'gross': {'gross', 'with', 'vatincluded', 'сндс', 'grossamount'},
    'net': {'net', 'without', 'vatexcluded', 'безндс', 'netamount'},
    'customer': {
        'customer',
        'client',
        'buyer',
        'заказчик',
        'заказчика',
        'клиент',
        'клиента',
        'покупатель',
        'client',
        'klient',
        'klienta',
        'zakazchik',
        'zakazchika',
    },
    'boolean': {'boolean', 'bool', 'flag', 'да', 'нет', 'yes', 'no'},
    'unit': {'unit', 'measure', 'uom', 'единица', 'измерения', 'edinitsa', 'izmereniya'},
}

SYNONYM_GROUPS['plan'] = {'plan', 'planned', 'planovaya', 'planovyy', 'planovoe', 'planovuyu'}
SYNONYM_GROUPS['act'] = {'act', 'acts', 'akt', 'akta', 'aktu'}
SYNONYM_GROUPS['reason'] = {'reason', 'cause', 'prichina', 'prichine'}
SYNONYM_GROUPS['close'] = {'close', 'closed', 'closing', 'zakryt', 'zakryta', 'zakrytiya'}
SYNONYM_GROUPS['updated'].update({'last', 'poslednego', 'posledniy', 'poslednyaya'})
SYNONYM_GROUPS['id'].update({'identifikator'})
SYNONYM_GROUPS['revenue'].update({'vyruchki', 'vyruchke'})
SYNONYM_GROUPS['creator'].update({'sozdal'})
SYNONYM_GROUPS['organization'].update({'organizatsiya'})
SYNONYM_GROUPS['responsible'].update({'person'})
SYNONYM_GROUPS['product'].update({'produktov'})
SYNONYM_GROUPS['license'].update({'litsenziy'})
SYNONYM_GROUPS['boolean'].update({'lid'})
SYNONYM_GROUPS['stage'] = {'stage', 'stadiya', 'stadiyu'}
SYNONYM_GROUPS['transition'] = {'transition', 'perehod', 'perehoda'}
SYNONYM_GROUPS['time'] = {'time', 'vremya'}
SYNONYM_GROUPS['delivery'] = {'delivery', 'postavka', 'postavki'}
SYNONYM_GROUPS['direct'] = {'direct', 'pryamaya', 'pryamoy'}
SYNONYM_GROUPS['distributor'] = {'distributor', 'distribyutor'}
SYNONYM_GROUPS['service'] = {'service', 'uslug', 'uslugi'}
SYNONYM_GROUPS['final'] = {'final', 'itogovaya', 'itogovyy', 'itogovoe'}
SYNONYM_GROUPS['forecast'] = {'forecast', 'prognoz'}
SYNONYM_GROUPS['marketing'] = {'marketing', 'marketingovoe'}
SYNONYM_GROUPS['event'] = {'event', 'meropriyatie'}
SYNONYM_GROUPS['site'] = {'site', 'sayta', 'sayt'}
SYNONYM_GROUPS['lead'] = {'lead', 'lid'}
SYNONYM_GROUPS['supply'] = {'supply', 'postavka'}
SYNONYM_GROUPS['type'] = {'type', 'tip'}
SYNONYM_GROUPS['vat'] = {'vat', 'nds'}

STOPWORD_TOKENS = {'of', 'by', 'with', 'na', 'po', 's', 'param', 'input', 'items', 'rows', 'data'}

CANONICAL_LOOKUP = {
    alias: canonical
    for canonical, aliases in SYNONYM_GROUPS.items()
    for alias in aliases | {canonical}
}

IMPORTANT_DOMAIN_TOKENS = {
    'date',
    'created',
    'updated',
    'id',
    'amount',
    'revenue',
    'name',
    'description',
    'product',
    'quantity',
    'organization',
    'creator',
    'responsible',
    'deal',
    'source',
    'partner',
    'license',
    'gross',
    'net',
    'customer',
    'unit',
}

IMPORTANT_DOMAIN_TOKENS.update({'plan', 'act', 'reason', 'close', 'stage', 'transition', 'time', 'delivery', 'direct', 'distributor', 'service', 'final', 'forecast', 'marketing', 'event', 'site', 'lead', 'supply', 'type', 'vat'})

ALIAS_SENSITIVE_TOKEN_GROUPS = {
    'identifier': {'identifier', 'identifikator', 'identyfikator'},
    'id': {'id'},
    'comment': {'comment', 'kommentariy'},
    'reason': {'reason', 'prichina', 'prichine'},
    'close': {'close', 'closed', 'closing', 'zakryt', 'zakryta', 'zakrytiya'},
}

ROLE_LABEL_BY_TOKEN = {
    'id': 'identifier',
    'name': 'label',
    'description': 'description',
    'date': 'timestamp',
    'created': 'timestamp',
    'updated': 'timestamp',
    'amount': 'numeric_value',
    'revenue': 'numeric_value',
    'quantity': 'numeric_value',
    'boolean': 'flag',
    'unit': 'unit',
}

ATTRIBUTE_STOPWORDS = {
    'deal',
    'customer',
    'organization',
    'product',
    'creator',
    'responsible',
    'source',
    'partner',
    'license',
}


def map_fields(
    source_columns: list[str],
    target_fields: list[TargetField],
    *,
    allow_position_fallback: bool = True,
) -> tuple[list[FieldMapping], list[str]]:
    warnings: list[str] = []
    mappings: list[FieldMapping] = []
    used_sources: set[str] = set()

    prepared_sources = [prepare_field_name(column) for column in source_columns]

    for target in target_fields:
        prepared_target = prepare_field_name(target.name, field_type=target.type)
        deterministic = _find_deterministic_match(
            prepared_target=prepared_target,
            prepared_sources=prepared_sources,
            used_sources=used_sources,
        )

        if deterministic is not None:
            used_sources.add(deterministic['source'])
            mappings.append(
                FieldMapping(
                    source=deterministic['source'],
                    target=target.name,
                    confidence=deterministic['confidence'],
                    reason=deterministic['reason'],
                    status='accepted' if deterministic['confidence'] in {'high', 'medium'} else 'suggested',
                    source_of_truth='deterministic_rule',
                )
            )
            continue

        mappings.append(
            FieldMapping(
                source=None,
                target=target.name,
                confidence='none',
                reason='not_found',
                status='suggested',
                source_of_truth='unresolved',
            )
        )
        warnings.append(f'No source column found for target "{target.name}"')

    if allow_position_fallback and _should_use_position_fallback(source_columns, target_fields, mappings):
        fallback_mappings = build_position_fallback_mappings(source_columns, target_fields)
        return (
            fallback_mappings,
            [
                'No semantic column matches found. Used column-order fallback because source and target have the same number of fields.'
            ],
        )

    return mappings, warnings


def build_position_fallback_mappings(
    source_columns: list[str],
    target_fields: list[TargetField],
) -> list[FieldMapping]:
    return [
        FieldMapping(
            source=source_columns[index],
            target=target_fields[index].name,
            confidence='low',
            reason='position_fallback',
            status='suggested',
            source_of_truth='position_fallback',
        )
        for index in range(len(target_fields))
    ]


def prepare_field_name(value: str, *, field_type: str | None = None) -> dict[str, Any]:
    original = value or ''
    raw_tokens = _raw_tokens(original)
    transliterated_tokens = [_transliterate_token(token) for token in raw_tokens]
    canonical_tokens = [_canonicalize_token(token) for token in transliterated_tokens]
    semantic_parts = _extract_semantic_parts(canonical_tokens)

    normalized_name = ' '.join(transliterated_tokens)
    canonical_name = ' '.join(canonical_tokens)
    return {
        'original_name': original,
        'normalized_name': normalized_name,
        'canonical_name': canonical_name,
        'tokens': raw_tokens,
        'normalized_tokens': transliterated_tokens,
        'canonical_tokens': canonical_tokens,
        'token_set': set(transliterated_tokens),
        'canonical_set': set(canonical_tokens),
        'field_type': field_type,
        'type_hints': _infer_name_type_hints(canonical_tokens),
        'entity_token': semantic_parts['entity_token'],
        'entity_tokens': semantic_parts['entity_tokens'],
        'attribute_token': semantic_parts['attribute_token'],
        'role_label': semantic_parts['role_label'],
        'context_tokens': semantic_parts['context_tokens'],
    }


def normalize(value: str) -> str:
    prepared = prepare_field_name(value)
    return ''.join(prepared['normalized_tokens'])


def tokenize(value: str) -> list[str]:
    return list(prepare_field_name(value)['normalized_tokens'])


def canonicalize_tokens(value: str) -> list[str]:
    return list(prepare_field_name(value)['canonical_tokens'])


def infer_value_type(value: Any) -> str:
    if value is None:
        return 'null'
    if isinstance(value, bool):
        return 'boolean'
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return 'number'
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return 'null'
        lowered = stripped.lower()
        if lowered in {'true', 'false', 'да', 'нет', 'yes', 'no'}:
            return 'boolean'
        numeric_candidate = stripped.replace(' ', '').replace(',', '.')
        try:
            float(numeric_candidate)
        except ValueError:
            pass
        else:
            return 'number'
        if _looks_like_date(stripped):
            return 'date'
    return 'string'


def _find_deterministic_match(
    *,
    prepared_target: dict[str, Any],
    prepared_sources: list[dict[str, Any]],
    used_sources: set[str],
) -> dict[str, str] | None:
    target_normalized = prepared_target['normalized_name']
    target_canonical_tokens = prepared_target['canonical_tokens']
    target_canonical_set = prepared_target['canonical_set']

    exact = next(
        (
            source
            for source in prepared_sources
            if source['original_name'] not in used_sources and source['normalized_name'] == target_normalized
        ),
        None,
    )
    if exact is not None:
        return {'source': exact['original_name'], 'confidence': 'high', 'reason': 'normalized_exact'}

    alias_sensitive = _find_alias_sensitive_match(
        prepared_target=prepared_target,
        prepared_sources=prepared_sources,
        used_sources=used_sources,
    )
    if alias_sensitive is not None:
        return alias_sensitive

    canonical_exact = next(
        (
            source
            for source in prepared_sources
            if source['original_name'] not in used_sources and source['canonical_tokens'] == target_canonical_tokens
        ),
        None,
    )
    if canonical_exact is not None:
        return {'source': canonical_exact['original_name'], 'confidence': 'high', 'reason': 'canonical_exact'}

    token_equal = next(
        (
            source
            for source in prepared_sources
            if source['original_name'] not in used_sources
            and source['canonical_set'] == target_canonical_set
            and len(source['canonical_tokens']) == len(target_canonical_tokens)
        ),
        None,
    )
    if token_equal is not None and target_canonical_tokens:
        return {'source': token_equal['original_name'], 'confidence': 'high', 'reason': 'token_equality'}

    pattern_match = next(
        (
            source
            for source in prepared_sources
            if source['original_name'] not in used_sources and _matches_standard_pattern(prepared_target, source)
        ),
        None,
    )
    if pattern_match is not None:
        return {'source': pattern_match['original_name'], 'confidence': 'medium', 'reason': 'pattern_match'}

    near_matches: list[tuple[dict[str, Any], float]] = []
    for source in prepared_sources:
        if source['original_name'] in used_sources:
            continue
        canonical_overlap = _jaccard_similarity(prepared_target['canonical_set'], source['canonical_set'])
        normalized_similarity = SequenceMatcher(None, prepared_target['canonical_name'], source['canonical_name']).ratio()
        if canonical_overlap >= 0.999 and normalized_similarity >= 0.8:
            near_matches.append((source, 0.95))
        elif canonical_overlap >= 0.75 and normalized_similarity >= 0.88:
            near_matches.append((source, 0.78))

    if near_matches:
        near_matches.sort(key=lambda item: item[1], reverse=True)
        best_source, best_score = near_matches[0]
        return {
            'source': best_source['original_name'],
            'confidence': 'high' if best_score >= 0.9 else 'medium',
            'reason': 'near_exact_rule',
        }

    return None


def _find_alias_sensitive_match(
    *,
    prepared_target: dict[str, Any],
    prepared_sources: list[dict[str, Any]],
    used_sources: set[str],
) -> dict[str, str] | None:
    target_tokens = set(prepared_target['normalized_tokens'])
    target_canonical = set(prepared_target['canonical_tokens'])

    def find_source(predicate: Any) -> dict[str, Any] | None:
        return next(
            (
                source
                for source in prepared_sources
                if source['original_name'] not in used_sources and predicate(set(source['normalized_tokens']), set(source['canonical_tokens']))
            ),
            None,
        )

    if 'identifier' in target_tokens:
        match = find_source(
            lambda source_tokens, source_canonical: bool(source_tokens & ALIAS_SENSITIVE_TOKEN_GROUPS['identifier'])
            and ('deal' not in target_canonical or 'deal' in source_canonical)
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_identifier'}

    if 'id' in target_tokens and 'identifier' not in target_tokens:
        match = find_source(
            lambda source_tokens, source_canonical: 'id' in source_tokens
            and not source_tokens.intersection(ALIAS_SENSITIVE_TOKEN_GROUPS['identifier'])
            and ('deal' not in target_canonical or 'deal' in source_canonical)
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_id'}

    if 'name' in target_canonical:
        match = find_source(
            lambda source_tokens, source_canonical: 'naimenovanie' in source_tokens
            or ('name' in source_canonical and 'description' not in source_canonical)
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_name'}

    if 'description' in target_canonical:
        explicit_description = find_source(
            lambda source_tokens, _source_canonical: bool(source_tokens & {'opisanie', 'description', 'descr', 'comment', 'kommentariy'})
        )
        if explicit_description is not None:
            return {'source': explicit_description['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_description'}

        fallback_description = find_source(
            lambda source_tokens, source_canonical: 'naimenovanie' in source_tokens or 'description' in source_canonical
        )
        if fallback_description is not None:
            return {'source': fallback_description['original_name'], 'confidence': 'medium', 'reason': 'alias_sensitive_description_fallback'}

    if 'invoice' in target_tokens and 'amount' in target_tokens:
        if 'vat' in target_tokens:
            match = find_source(
                lambda _source_tokens, source_canonical: 'amount' in source_canonical and 'act' in source_canonical and 'vat' in source_canonical
            )
            if match is not None:
                return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_invoice_vat'}
        else:
            match = find_source(
                lambda _source_tokens, source_canonical: 'amount' in source_canonical and 'act' in source_canonical and 'vat' not in source_canonical
            )
            if match is not None:
                return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_invoice'}

    if {'final', 'service', 'amount'} <= target_canonical:
        if {'revenue', 'vat'} <= target_canonical:
            match = find_source(
                lambda _source_tokens, source_canonical: {'deal', 'final', 'service', 'amount', 'revenue', 'vat'} <= source_canonical
            )
            if match is not None:
                return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_final_service_revenue_vat'}
        if 'vat' in target_canonical and 'revenue' not in target_canonical:
            match = find_source(
                lambda _source_tokens, source_canonical: {'deal', 'final', 'service', 'amount', 'vat'} <= source_canonical and 'revenue' not in source_canonical
            )
            if match is not None:
                return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_final_service_vat'}
        if 'vat' not in target_canonical and 'revenue' not in target_canonical:
            match = find_source(
                lambda _source_tokens, source_canonical: {'deal', 'final', 'service', 'amount'} <= source_canonical
                and 'vat' not in source_canonical
                and 'revenue' not in source_canonical
            )
            if match is not None:
                return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_final_service'}

    if {'site', 'lead'} <= target_canonical:
        match = find_source(
            lambda _source_tokens, source_canonical: {'deal', 'site', 'lead'} <= source_canonical
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_site_lead'}

    if {'direct', 'supply'} <= target_canonical:
        match = find_source(
            lambda _source_tokens, source_canonical: {'deal', 'direct', 'supply'} <= source_canonical
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_direct_supply'}

    if {'deal', 'stage', 'final'} <= target_canonical:
        match = find_source(
            lambda _source_tokens, source_canonical: {'deal', 'stage'} <= source_canonical
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'medium', 'reason': 'alias_sensitive_stage_final'}

    if 'comment' in target_tokens and 'reason' in target_tokens:
        match = find_source(
            lambda source_tokens, _source_canonical: bool(source_tokens & ALIAS_SENSITIVE_TOKEN_GROUPS['comment'])
            and bool(source_tokens & ALIAS_SENSITIVE_TOKEN_GROUPS['reason'])
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_reason_comment'}

    if 'reason' in target_tokens and 'comment' not in target_tokens:
        match = find_source(
            lambda source_tokens, _source_canonical: bool(source_tokens & ALIAS_SENSITIVE_TOKEN_GROUPS['reason'])
            and not bool(source_tokens & ALIAS_SENSITIVE_TOKEN_GROUPS['comment'])
        )
        if match is not None:
            return {'source': match['original_name'], 'confidence': 'high', 'reason': 'alias_sensitive_reason'}

    return None


def _matches_standard_pattern(prepared_target: dict[str, Any], prepared_source: dict[str, Any]) -> bool:
    target_tokens = prepared_target['canonical_tokens']
    source_tokens = prepared_source['canonical_tokens']
    if not target_tokens or not source_tokens:
        return False

    for keyword in IMPORTANT_DOMAIN_TOKENS:
        if keyword in target_tokens and keyword not in source_tokens:
            continue
        if keyword in target_tokens and keyword in source_tokens:
            remaining_target = [token for token in target_tokens if token != keyword]
            remaining_source = [token for token in source_tokens if token != keyword]
            if not remaining_target:
                return True
            if set(remaining_target) <= set(remaining_source):
                return True
    return False


def _raw_tokens(value: str) -> list[str]:
    with_boundaries = CAMEL_BOUNDARY_RE.sub(' ', value)
    with_boundaries = DIGIT_BOUNDARY_RE.sub(' ', with_boundaries)
    parts = TOKEN_SPLIT_RE.split(with_boundaries.strip().lower())
    return [part for part in parts if part and part not in STOPWORD_TOKENS]


def _transliterate_token(token: str) -> str:
    return ''.join(CYRILLIC_TO_LATIN.get(char, char) for char in token)


def _canonicalize_token(token: str) -> str:
    return CANONICAL_LOOKUP.get(token, token)


def _infer_name_type_hints(tokens: list[str]) -> set[str]:
    hints: set[str] = set()
    token_set = set(tokens)
    if 'date' in token_set or 'created' in token_set or 'updated' in token_set:
        hints.add('date')
    if {'amount', 'revenue', 'quantity'} & token_set:
        hints.add('number')
    if 'id' in token_set:
        hints.add('id')
    if 'boolean' in token_set:
        hints.add('boolean')
    if 'name' in token_set or 'description' in token_set:
        hints.add('string')
    return hints


def _extract_semantic_parts(canonical_tokens: list[str]) -> dict[str, Any]:
    if not canonical_tokens:
        return {
            'entity_token': None,
            'entity_tokens': [],
            'attribute_token': None,
            'role_label': None,
            'context_tokens': [],
        }

    attribute_token = _pick_attribute_token(canonical_tokens)
    role_label = ROLE_LABEL_BY_TOKEN.get(attribute_token) if attribute_token else None

    entity_tokens = [
        token
        for token in canonical_tokens
        if token != attribute_token and token not in ATTRIBUTE_STOPWORDS and token not in {'with', 'without'}
    ]
    if not entity_tokens:
        entity_tokens = [token for token in canonical_tokens if token != attribute_token]

    entity_token = entity_tokens[0] if entity_tokens else None
    context_tokens = sorted({token for token in canonical_tokens if token != attribute_token})
    return {
        'entity_token': entity_token,
        'entity_tokens': entity_tokens,
        'attribute_token': attribute_token,
        'role_label': role_label,
        'context_tokens': context_tokens,
    }


def _pick_attribute_token(canonical_tokens: list[str]) -> str | None:
    for token in reversed(canonical_tokens):
        if token in ROLE_LABEL_BY_TOKEN:
            return token
    return canonical_tokens[-1] if canonical_tokens else None


def _looks_like_date(value: str) -> bool:
    stripped = value.strip()
    if len(stripped) < 8:
        return False
    return bool(
        re.search(r'\d{4}-\d{2}-\d{2}', stripped)
        or re.search(r'\d{2}\.\d{2}\.\d{4}', stripped)
        or re.search(r'\d{2}/\d{2}/\d{4}', stripped)
    )


def _jaccard_similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / max(1, len(left | right))


def _should_use_position_fallback(
    source_columns: list[str],
    target_fields: list[TargetField],
    mappings: list[FieldMapping],
) -> bool:
    if not source_columns or len(source_columns) != len(target_fields):
        return False

    return all(mapping.source is None for mapping in mappings)
