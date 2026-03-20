import re
import json
from typing import Any


CYR_TO_LAT = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d",
    "е": "e", "ё": "e", "ж": "zh", "з": "z", "и": "i",
    "й": "y", "к": "k", "л": "l", "м": "m", "н": "n",
    "о": "o", "п": "p", "р": "r", "с": "s", "т": "t",
    "у": "u", "ф": "f", "х": "h", "ц": "ts", "ч": "ch",
    "ш": "sh", "щ": "sch", "ъ": "", "ы": "y", "ь": "",
    "э": "e", "ю": "yu", "я": "ya"
}


def normalize_column_name(name: str) -> str:
    """
    1) убираем мусор
    2) режем пробелы
    3) приводим к предсказуемой форме
    """
    value = str(name).strip().lower()
    value = value.replace("ё", "е")

    # всё, что не буквы/цифры, заменяем на пробел
    value = re.sub(r"[^a-zа-я0-9]+", " ", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value).strip()

    return value


def transliterate_ru_to_lat(text: str) -> str:
    """
    ФИО клиента -> fio klienta
    """
    result = []
    for ch in text:
        if ch in CYR_TO_LAT:
            result.append(CYR_TO_LAT[ch])
        else:
            result.append(ch)
    return "".join(result)


def to_camel_case(text: str) -> str:
    """
    fio klienta -> fioKlienta
    summa rub -> summaRub
    """
    parts = [p for p in text.split(" ") if p]
    if not parts:
        return "field"

    first = parts[0]
    rest = [p[:1].upper() + p[1:] for p in parts[1:]]
    candidate = first + "".join(rest)

    # если начинается с цифры — делаем валидное имя
    if candidate and candidate[0].isdigit():
        candidate = f"field{candidate}"

    return candidate or "field"


def make_safe_unique_key(base_key: str, used: set[str]) -> str:
    """
    Делает имя уникальным:
    fioKlienta, fioKlienta2, fioKlienta3 ...
    """
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


def looks_like_number(value: Any) -> bool:
    if value is None:
        return False

    text = str(value).strip()
    if not text:
        return False

    # убираем пробелы в числе: 120 000
    text = text.replace(" ", "")

    # 120000 / 120000.50 / 120000,50
    if re.fullmatch(r"[-+]?\d+([.,]\d+)?", text):
        return True

    return False


def looks_like_date(value: Any) -> bool:
    if value is None:
        return False

    text = str(value).strip()
    if not text:
        return False

    patterns = [
        r"\d{2}\.\d{2}\.\d{4}",   # 01.01.2025
        r"\d{4}-\d{2}-\d{2}",     # 2025-01-01
        r"\d{2}/\d{2}/\d{4}",     # 01/01/2025
        r"\d{2}-\d{2}-\d{4}",     # 01-01-2025
    ]

    return any(re.fullmatch(pattern, text) for pattern in patterns)


def infer_field_type(values: list[Any]) -> str:
    """
    Возвращает:
    - "number"
    - "string"
    Для MVP дату тоже считаем string.
    """
    non_empty = [v for v in values if not is_empty_value(v)]

    if not non_empty:
        return "string"

    number_count = sum(1 for v in non_empty if looks_like_number(v))
    date_count = sum(1 for v in non_empty if looks_like_date(v))
    total = len(non_empty)

    # Если большинство значений — числа
    if number_count == total:
        return "number"

    # Даты в MVP считаем string
    if date_count == total:
        return "string"

    return "string"


def default_value_for_type(field_type: str) -> Any:
    if field_type == "number":
        return 0
    return ""


def generate_draft_json(columns: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Главная функция:
    columns + rows -> draft JSON
    """
    result: dict[str, Any] = {}
    used_keys: set[str] = set()

    for column in columns:
        normalized = normalize_column_name(column)
        transliterated = transliterate_ru_to_lat(normalized)
        camel_key = to_camel_case(transliterated)
        unique_key = make_safe_unique_key(camel_key, used_keys)

        sample_values = [row.get(column) for row in rows[:10]]
        field_type = infer_field_type(sample_values)
        result[unique_key] = default_value_for_type(field_type)

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
