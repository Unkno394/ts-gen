from __future__ import annotations

import re
from typing import Any

TEXT_BLOCK_LIMIT = 24
SECTION_LIMIT = 12
FACT_LIMIT = 16

_WHITESPACE_RE = re.compile(r'[ \t]+')
_FACT_PATTERNS = (
    re.compile(r'^(?P<label>[^.:]{2,60}?)\s+(?:is|are|equals|was|were)\s+(?P<value>[^.]{2,180})\.?$', re.IGNORECASE),
    re.compile(r'^(?P<label>[^.:]{2,60}?)\s+(?:является|составляет|равен|равна|равно)\s+(?P<value>[^.]{2,180})\.?$', re.IGNORECASE),
)


def normalize_text(value: Any) -> str:
    if value is None:
        return ''

    lines = str(value).replace('\xa0', ' ').splitlines()
    normalized_lines: list[str] = []
    last_blank = True
    for raw_line in lines:
        line = _WHITESPACE_RE.sub(' ', raw_line).strip()
        if not line:
            if not last_blank and normalized_lines:
                normalized_lines.append('')
            last_blank = True
            continue
        normalized_lines.append(line)
        last_blank = False
    return '\n'.join(normalized_lines).strip()


def split_text_blocks(raw_text: str, *, limit: int = TEXT_BLOCK_LIMIT) -> list[dict[str, str | None]]:
    paragraphs = _paragraphs(raw_text)
    blocks: list[dict[str, str | None]] = []
    for index, paragraph in enumerate(paragraphs[:limit], start=1):
        blocks.append(
            {
                'id': f'block-{index}',
                'kind': 'paragraph',
                'text': paragraph,
                'label': paragraph if _looks_like_heading(paragraph) else None,
            }
        )
    return blocks


def extract_sections(raw_text: str, *, limit: int = SECTION_LIMIT) -> list[dict[str, str]]:
    paragraphs = _paragraphs(raw_text)
    if not paragraphs:
        return []

    sections: list[dict[str, str]] = []
    current_title = 'Document'
    current_body: list[str] = []

    for paragraph in paragraphs:
        if _looks_like_heading(paragraph):
            if current_body:
                sections.append({'title': current_title, 'text': '\n\n'.join(current_body)})
                if len(sections) >= limit:
                    return sections
            current_title = paragraph
            current_body = []
            continue
        current_body.append(paragraph)

    if current_body:
        sections.append({'title': current_title, 'text': '\n\n'.join(current_body)})

    if not sections:
        sections.append({'title': 'Document', 'text': '\n\n'.join(paragraphs[:3])})
    return sections[:limit]


def extract_text_facts(raw_text: str, *, limit: int = FACT_LIMIT) -> list[dict[str, str]]:
    facts: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    for sentence in _sentences(raw_text):
        for pattern in _FACT_PATTERNS:
            match = pattern.match(sentence)
            if not match:
                continue

            label = _clean_fact_label(match.group('label'))
            value = match.group('value').strip()
            if not _is_valid_fact(label, value):
                continue

            key = (label.casefold(), value.casefold())
            if key in seen:
                continue
            seen.add(key)
            facts.append({'label': label, 'value': value, 'source_text': sentence})
            if len(facts) >= limit:
                return facts
            break

    return facts


def _paragraphs(raw_text: str) -> list[str]:
    normalized = normalize_text(raw_text)
    if not normalized:
        return []
    return [chunk.strip() for chunk in normalized.split('\n\n') if chunk.strip()]


def _sentences(raw_text: str) -> list[str]:
    normalized = normalize_text(raw_text)
    if not normalized:
        return []

    raw_sentences = re.split(r'(?<=[.!?])\s+|\n+', normalized)
    return [sentence.strip() for sentence in raw_sentences if sentence.strip()]


def _looks_like_heading(paragraph: str) -> bool:
    words = paragraph.split()
    if not words or len(words) > 8:
        return False
    if paragraph.endswith(':'):
        return True
    letters = [char for char in paragraph if char.isalpha()]
    if not letters:
        return False
    uppercase_ratio = sum(1 for char in letters if char.isupper()) / max(len(letters), 1)
    return uppercase_ratio > 0.75


def _clean_fact_label(value: str) -> str:
    cleaned = value.strip(' -:;,')
    return cleaned[:60]


def _is_valid_fact(label: str, value: str) -> bool:
    return bool(label) and bool(value) and len(label) <= 60 and len(value) <= 180
