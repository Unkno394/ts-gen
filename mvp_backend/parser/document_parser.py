from __future__ import annotations

from pathlib import Path
from typing import Any

from docx_parser import parse_docx
from pdf_parser import parse_pdf


class UnsupportedFileTypeError(ValueError):
    pass


def empty_result(file_path: str | Path, file_type: str) -> dict[str, Any]:
    return {
        "file_name": Path(file_path).name,
        "file_type": file_type,
        "content_type": "unknown",
        "columns": [],
        "rows": [],
        "text": "",
        "blocks": [],
        "warnings": [],
    }


SUPPORTED_TYPES = {"pdf", "docx"}


def parse_document(file_path: str | Path) -> dict[str, Any]:
    path = Path(file_path)
    ext = path.suffix.lower().lstrip(".")

    if ext not in SUPPORTED_TYPES:
        raise UnsupportedFileTypeError(
            f"Unsupported file type: {ext}. Supported: {sorted(SUPPORTED_TYPES)}"
        )

    if ext == "docx":
        return parse_docx(path)

    if ext == "pdf":
        return parse_pdf(path)

    raise UnsupportedFileTypeError(f"Unsupported file type: {ext}")


if __name__ == "__main__":
    import json
    import sys

    if len(sys.argv) < 2:
        print("Usage: python document_parser.py /path/to/file.pdf")
        raise SystemExit(1)

    parsed = parse_document(sys.argv[1])
    print(json.dumps(parsed, ensure_ascii=False, indent=2))
