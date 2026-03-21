from __future__ import annotations

from pathlib import Path
from typing import Any

import pdfplumber


TEXT_MIN_LENGTH = 40



def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split()).strip()



def extract_tables_from_pdf(path: str | Path) -> list[dict[str, Any]]:
    found_tables: list[dict[str, Any]] = []
    with pdfplumber.open(str(path)) as pdf:
        for page_index, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            for table_index, table in enumerate(tables, start=1):
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
                    found_tables.append(
                        {
                            "name": f"Page {page_index} · Table {table_index}",
                            "columns": columns,
                            "rows": rows,
                        }
                    )

    return found_tables



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

    tables = extract_tables_from_pdf(path)
    if tables:
        columns = tables[0]["columns"]
        rows = tables[0]["rows"]
        extracted_text = extract_text_from_pdf(path)
        if len(tables) > 1:
            warnings.append(f"Found {len(tables)} tables in PDF.")
        return {
            "file_name": Path(path).name,
            "file_type": "pdf",
            "content_type": "table",
            "columns": columns,
            "rows": rows,
            "tables": tables,
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
            "tables": [],
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
        "tables": [],
        "text": "",
        "blocks": [],
        "warnings": warnings,
    }
