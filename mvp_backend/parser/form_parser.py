from __future__ import annotations

import re
from typing import Any

KV_LIMIT = 32
INLINE_KV_MIN_FIELDS = 2

_LINE_SEPARATORS = (
    re.compile(r'^(?P<label>[^:]{2,80}?)\s*:\s*(?P<value>.+)$'),
    re.compile(r'^(?P<label>[^-]{2,80}?)\s+-\s+(?P<value>.+)$'),
    re.compile(r'^(?P<label>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9 /()#._]{1,60}?)\s{2,}(?P<value>.+)$'),
)

INLINE_FIELD_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ('ФИО', re.compile(r'(?i)\b(?:фио|full\s*name)\b')),
    ('дата рождения', re.compile(r'(?i)\b(?:дата\s+рождения|birth\s*date)\b')),
    ('телефон', re.compile(r'(?i)\b(?:телефон|phone(?:\s*number)?)\b')),
    ('email', re.compile(r'(?i)\b(?:email|e-?mail)\b')),
    ('адрес', re.compile(r'(?i)\b(?:адрес|address)\b')),
    ('цель документа', re.compile(r'(?i)\b(?:цель\s+документа|document\s*purpose)\b')),
)


def extract_kv_pairs(raw_text: Any, *, limit: int = KV_LIMIT) -> list[dict[str, str]]:
    lines = str(raw_text or '').replace('\xa0', ' ').splitlines()
    pairs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for raw_line in lines:
        line = re.sub(r'[ \t]+', ' ', raw_line).strip()
        if not line:
            continue

        parsed = _parse_line(line)
        if parsed is None:
            continue

        label = parsed['label']
        value = parsed['value']
        key = (label.casefold(), value.casefold())
        if key in seen:
            continue
        seen.add(key)
        pairs.append(
            {
                'label': label,
                'value': value,
                'confidence': parsed['confidence'],
                'source_text': line,
            }
        )
        if len(pairs) >= limit:
            break

    inline_pairs = _extract_inline_kv_pairs(raw_text, limit=limit)
    if len(inline_pairs) >= INLINE_KV_MIN_FIELDS and len(pairs) <= 1:
        return inline_pairs

    return pairs


def _parse_line(line: str) -> dict[str, str] | None:
    for pattern in _LINE_SEPARATORS:
        match = pattern.match(line)
        if not match:
            continue

        label = match.group('label').strip(' -:;')
        value = match.group('value').strip()
        if not _is_valid_pair(label, value):
            continue

        confidence = 'high' if ':' in line else 'medium'
        return {'label': label, 'value': value, 'confidence': confidence}

    return None


def _is_valid_pair(label: str, value: str) -> bool:
    if not label or not value:
        return False
    if len(label) > 80 or len(value) > 220:
        return False
    if len(label.split()) > 8:
        return False
    return True


def _extract_inline_kv_pairs(raw_text: Any, *, limit: int) -> list[dict[str, str]]:
    text = re.sub(r'\s+', ' ', str(raw_text or '').replace('\xa0', ' ')).strip()
    if not text:
        return []

    matches: list[tuple[int, int, str]] = []
    for canonical_label, pattern in INLINE_FIELD_PATTERNS:
        for match in pattern.finditer(text):
            matches.append((match.start(), match.end(), canonical_label))

    if len(matches) < INLINE_KV_MIN_FIELDS:
        return []

    matches.sort(key=lambda item: (item[0], item[1]))
    filtered_matches: list[tuple[int, int, str]] = []
    last_end = -1
    for start, end, canonical_label in matches:
        if filtered_matches and start < last_end:
            continue
        filtered_matches.append((start, end, canonical_label))
        last_end = end

    pairs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for index, (_start, end, canonical_label) in enumerate(filtered_matches):
        next_start = filtered_matches[index + 1][0] if index + 1 < len(filtered_matches) else len(text)
        value = text[end:next_start].strip(' ,.;:-')
        if not _is_valid_pair(canonical_label, value):
            continue
        key = (canonical_label.casefold(), value.casefold())
        if key in seen:
            continue
        seen.add(key)
        pairs.append(
            {
                'label': canonical_label,
                'value': value,
                'confidence': 'medium',
                'source_text': text,
            }
        )
        if len(pairs) >= limit:
            break

    return pairs
