# DOCX/PDF text parser

Небольшой Python-модуль для извлечения текста и таблиц из DOCX/PDF.

Что делает:
- DOCX: читает текст и таблицы напрямую
- PDF: сначала пробует вытащить текст и таблицы напрямую

## Установка

```bash
pip install -r requirements.txt
```

## Использование

```python
from document_parser import parse_document

result = parse_document("sample.pdf")
print(result)
```

## Формат результата

```json
{
  "file_name": "sample.pdf",
  "file_type": "pdf",
  "content_type": "table",
  "columns": ["ФИО клиента", "Сумма руб", "Дата заявки"],
  "rows": [
    {
      "ФИО клиента": "Иванов Иван",
      "Сумма руб": "120000",
      "Дата заявки": "01.01.2025"
    }
  ],
  "text": "",
  "blocks": [],
  "warnings": []
}
```

## Важная оговорка

Для DOCX и обычных текстовых PDF этого модуля достаточно.
Сканированные PDF без текстового слоя теперь не поддерживаются.
