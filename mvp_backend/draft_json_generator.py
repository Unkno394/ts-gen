from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any


CYR_TO_LAT = {
    "а": "a",
    "б": "b",
    "в": "v",
    "г": "g",
    "д": "d",
    "е": "e",
    "ё": "e",
    "ж": "zh",
    "з": "z",
    "и": "i",
    "й": "y",
    "к": "k",
    "л": "l",
    "м": "m",
    "н": "n",
    "о": "o",
    "п": "p",
    "р": "r",
    "с": "s",
    "т": "t",
    "у": "u",
    "ф": "f",
    "х": "h",
    "ц": "ts",
    "ч": "ch",
    "ш": "sh",
    "щ": "sch",
    "ъ": "",
    "ы": "y",
    "ь": "",
    "э": "e",
    "ю": "yu",
    "я": "ya",
}

PLACEHOLDER_TEXT_VALUES = {
    "",
    "-",
    "—",
    "n/a",
    "na",
    "null",
    "none",
    "undefined",
    "unknown",
}
BOOLEAN_TRUE_VALUES = {"true", "1", "yes", "y", "да"}
BOOLEAN_FALSE_VALUES = {"false", "0", "no", "n", "нет"}
DATE_FORMATS = ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y")
SAMPLE_SCAN_LIMIT = 20
SAMPLE_PREVIEW_LIMIT = 3
MAX_REPRESENTATIVE_TEXT_LENGTH = 120


def normalize_column_name(name: str) -> str:
    value = re.sub(r"([a-zа-я0-9])([A-ZА-Я])", r"\1 \2", str(name).strip())
    value = value.lower()
    value = value.replace("ё", "е")
    value = re.sub(r"[^a-zа-я0-9]+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def transliterate_ru_to_lat(text: str) -> str:
    result: list[str] = []
    for ch in text:
        result.append(CYR_TO_LAT.get(ch, ch))
    return "".join(result)


def to_camel_case(text: str) -> str:
    parts = [part for part in text.split(" ") if part]
    if not parts:
        return "field"

    first = parts[0]
    rest = [part[:1].upper() + part[1:] for part in parts[1:]]
    candidate = first + "".join(rest)
    if candidate and candidate[0].isdigit():
        candidate = f"field{candidate}"
    return candidate or "field"


def make_safe_unique_key(base_key: str, used: set[str]) -> str:
    key = base_key or "field"
    if key not in used:
        used.add(key)
        return key

    counter = 2
    while f"{key}{counter}" in used:
        counter += 1

    unique_key = f"{key}{counter}"
    used.add(unique_key)
    return unique_key


def is_empty_value(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def is_placeholder_text(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in PLACEHOLDER_TEXT_VALUES


def looks_like_number(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    text = text.replace(" ", "")
    return re.fullmatch(r"[-+]?\d+([.,]\d+)?", text) is not None


def parse_number(value: Any) -> int | float | None:
    if not looks_like_number(value):
        return None
    text = str(value).strip().replace(" ", "").replace(",", ".")
    try:
        parsed = float(text)
    except ValueError:
        return None
    if parsed.is_integer():
        return int(parsed)
    return parsed


def looks_like_date(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text:
        return False
    return any(re.fullmatch(pattern, text) for pattern in (r"\d{2}\.\d{2}\.\d{4}", r"\d{4}-\d{2}-\d{2}", r"\d{2}/\d{2}/\d{4}", r"\d{2}-\d{2}-\d{4}"))


def normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_boolean(value: Any) -> bool | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized in BOOLEAN_TRUE_VALUES:
        return True
    if normalized in BOOLEAN_FALSE_VALUES:
        return False
    return None


def infer_field_type(values: list[Any]) -> str:
    non_empty = [value for value in values if not is_empty_value(value) and not is_placeholder_text(value)]
    if not non_empty:
        return "null"

    total = len(non_empty)
    if sum(1 for value in non_empty if parse_boolean(value) is not None) == total:
        return "boolean"
    if sum(1 for value in non_empty if looks_like_number(value)) == total:
        return "number"
    return "string"


def default_value_for_type(field_type: str) -> Any:
    if field_type == "number":
        return 0
    if field_type == "boolean":
        return False
    if field_type == "null":
        return None
    return ""


def normalize_sample_value(value: Any, field_type: str) -> Any:
    if field_type == "number":
        return parse_number(value)
    if field_type == "boolean":
        return parse_boolean(value)
    if field_type == "string" and looks_like_date(value):
        return normalize_date(value)
    return str(value).strip() if value is not None else None


def choose_representative_value(values: list[Any], field_type: str, *, total_count: int) -> Any:
    if not values:
        return None

    # Large mostly-empty columns are not useful for sample-based draft JSON.
    if total_count >= 5 and (len(values) / total_count) <= 0.1:
        return None

    if field_type == "number":
        for value in values:
            parsed = parse_number(value)
            if parsed is not None:
                return parsed
        return None

    if field_type == "boolean":
        for value in values:
            parsed = parse_boolean(value)
            if parsed is not None:
                return parsed
        return None

    normalized_dates = [normalize_date(value) for value in values if normalize_date(value) is not None]
    if normalized_dates:
        return normalized_dates[0]

    preferred_texts = [
        str(value).strip()
        for value in values
        if not is_placeholder_text(value)
        and len(str(value).strip()) > 1
        and len(str(value).strip()) <= MAX_REPRESENTATIVE_TEXT_LENGTH
    ]
    if preferred_texts:
        return preferred_texts[0]

    fallback_texts = [str(value).strip() for value in values if not is_placeholder_text(value)]
    if fallback_texts:
        return fallback_texts[0]
    return None


def build_sample_preview(values: list[Any], field_type: str) -> list[Any]:
    preview: list[Any] = []
    for value in values:
        normalized = normalize_sample_value(value, field_type)
        if normalized is None or normalized == "":
            continue
        if normalized in preview:
            continue
        preview.append(normalized)
        if len(preview) >= SAMPLE_PREVIEW_LIMIT:
            break
    return preview


def build_draft_field_suggestions(columns: list[str], rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    suggestions: list[dict[str, Any]] = []
    used_keys: set[str] = set()

    for column in columns:
        normalized = normalize_column_name(column)
        transliterated = transliterate_ru_to_lat(normalized)
        camel_key = to_camel_case(transliterated)
        unique_key = make_safe_unique_key(camel_key, used_keys)

        raw_values = [row.get(column) for row in rows[:SAMPLE_SCAN_LIMIT]]
        non_empty_values = [value for value in raw_values if not is_empty_value(value) and not is_placeholder_text(value)]
        field_type = infer_field_type(non_empty_values)
        representative_value = choose_representative_value(non_empty_values, field_type, total_count=len(raw_values))
        is_sparse_column = bool(raw_values) and len(raw_values) >= 5 and (len(non_empty_values) / len(raw_values)) <= 0.1
        if representative_value is None:
            representative_value = None if is_sparse_column else default_value_for_type(field_type)
        null_ratio = round(
            (sum(1 for value in raw_values if is_empty_value(value) or is_placeholder_text(value)) / len(raw_values)),
            4,
        ) if raw_values else 1.0

        suggestions.append(
            {
                "source_column": column,
                "suggested_key": unique_key,
                "field_type": field_type,
                "default_value": representative_value,
                "representative_value": representative_value,
                "sample_values": build_sample_preview(non_empty_values, field_type),
                "null_ratio": null_ratio,
            }
        )
    return suggestions


def generate_draft_json(columns: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in build_draft_field_suggestions(columns, rows):
        result[str(field["suggested_key"])] = field["default_value"]
    return result


if __name__ == "__main__":
    columns = ["ФИО клиента", "Сумма руб", "Дата заявки"]
    rows = [
        {
            "ФИО клиента": "Иванов Иван",
            "Сумма руб": "120000",
            "Дата заявки": "01.01.2025",
        },
        {
            "ФИО клиента": "Петров Петр",
            "Сумма руб": "95000",
            "Дата заявки": "02.01.2025",
        },
    ]

    draft_json = generate_draft_json(columns, rows)
    print(json.dumps(draft_json, ensure_ascii=False, indent=2))
