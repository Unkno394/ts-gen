from __future__ import annotations

import re
from typing import Any

KV_LIMIT = 32

_LINE_SEPARATORS = (
    re.compile(r'^(?P<label>[^:]{2,80}?)\s*:\s*(?P<value>.+)$'),
    re.compile(r'^(?P<label>[^-]{2,80}?)\s+-\s+(?P<value>.+)$'),
    re.compile(r'^(?P<label>[A-Za-zА-Яа-я0-9][A-Za-zА-Яа-я0-9 /()#._]{1,60}?)\s{2,}(?P<value>.+)$'),
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
