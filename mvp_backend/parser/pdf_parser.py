from __future__ import annotations

from pathlib import Path
from typing import Any

import pdfplumber


TEXT_MIN_LENGTH = 40



def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()



def extract_tables_from_pdf(path: str | Path) -> tuple[list[str], list[dict[str, str]]] | None:
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table or len(table) < 2:
                    continue

                normalized_rows = []
                for row in table:
                    normalized_row = [clean_text(cell) for cell in row]
                    if any(normalized_row):
                        normalized_rows.append(normalized_row)

                if len(normalized_rows) < 2:
                    continue

                header = normalized_rows[0]
                if not any(header):
                    continue

                columns = [col if col else f"column{idx + 1}" for idx, col in enumerate(header)]
                rows: list[dict[str, str]] = []
                for row in normalized_rows[1:]:
                    padded = row + [""] * max(0, len(columns) - len(row))
                    row_obj = {columns[idx]: padded[idx] for idx in range(len(columns))}
                    if any(v != "" for v in row_obj.values()):
                        rows.append(row_obj)

                if rows:
                    return columns, rows

    return None



def extract_text_from_pdf(path: str | Path) -> str:
    parts: list[str] = []
    with pdfplumber.open(str(path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            text = text.strip()
            if text:
                parts.append(text)
    return "\n\n".join(parts).strip()



def parse_pdf(path: str | Path) -> dict[str, Any]:
    warnings: list[str] = []

    table_result = extract_tables_from_pdf(path)
    if table_result is not None:
        columns, rows = table_result
        extracted_text = extract_text_from_pdf(path)
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": "table",
            "columns": columns,
            "rows": rows,
            "text": extracted_text,
            "blocks": [],
            "warnings": warnings,
        }

    direct_text = extract_text_from_pdf(path)
    if len(direct_text) >= TEXT_MIN_LENGTH:
        warnings.append("No tables found in PDF. Returned extracted text only.")
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": "text",
            "columns": [],
            "rows": [],
            "text": direct_text,
            "blocks": [{"type": "paragraph", "text": direct_text}],
            "warnings": warnings,
        }

    warnings.append("PDF text layer is empty or too small. No extractable text found.")
    return {
        "file_name": Path(path).name,
        "file_type": "pdf",
        "content_type": "text",
        "columns": [],
        "rows": [],
        "text": "",
        "blocks": [],
        "warnings": warnings,
    }
