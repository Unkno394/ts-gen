from __future__ import annotations

import base64
import logging
import re
import tempfile
import threading
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

try:
    import cv2  # type: ignore
except ImportError:  # pragma: no cover
    cv2 = None

try:
    import numpy as np  # type: ignore
except ImportError:  # pragma: no cover
    np = None

try:
    from paddleocr import PaddleOCR
except ImportError as exc:  # pragma: no cover
    PaddleOCR = None
    _paddleocr_import_error = str(exc)
else:
    _paddleocr_import_error = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
)
logger = logging.getLogger(__name__)

app = FastAPI(title='TSGen OCR Service', version='0.1.0')
_ocr_instance: PaddleOCR | None = None
_ocr_ready = False
_ocr_startup_error: str | None = None
_ocr_warmup_started = False
_ocr_warmup_in_progress = False
_ocr_init_lock = threading.Lock()
DET_MODEL_DIR = '/opt/ocr-models/det'
REC_MODEL_DIR = '/opt/ocr-models/rec'
CLS_MODEL_DIR = '/opt/ocr-models/cls'
OCR_MIN_CONFIDENCE = 0.42
OCR_SHORT_LINE_MIN_CONFIDENCE = 0.6
OCR_FRAGMENT_Y_THRESHOLD = 14.0
OCR_FRAGMENT_GAP_THRESHOLD = 48.0
SCREENSHOT_TOP_NOISE_RATIO = 0.12
CHECKBOX_MAX_SIZE_RATIO = 0.08
CHECKBOX_MIN_SIZE_PX = 8
OCR_ROW_Y_THRESHOLD = 12.0
OCR_ROW_X_GAP_THRESHOLD = 56.0


class OcrExtractPayload(BaseModel):
    filename: str
    content_base64: str


def _normalize_line_text(value: object) -> str:
    return ' '.join(str(value or '').replace('\n', ' ').replace('\r', ' ').split()).strip()


DATE_RE = re.compile(r'^\d{4}[-./]\d{1,2}[-./]\d{1,2}$')
NUMERICISH_RE = re.compile(r'^[0-9OoОоIlI|lSBs$.,:/-]+$')
CYR_TO_LAT = str.maketrans({
    'А': 'A', 'В': 'B', 'Е': 'E', 'К': 'K', 'М': 'M', 'Н': 'H', 'О': 'O', 'Р': 'P', 'С': 'C', 'Т': 'T', 'У': 'Y', 'Х': 'X',
    'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p', 'с': 'c', 'у': 'y', 'х': 'x', 'к': 'k', 'м': 'm', 'т': 't', 'в': 'b',
})
LAT_TO_CYR = str.maketrans({
    'A': 'А', 'B': 'В', 'C': 'С', 'E': 'Е', 'H': 'Н', 'K': 'К', 'M': 'М', 'O': 'О', 'P': 'Р', 'T': 'Т', 'X': 'Х', 'Y': 'У',
    'a': 'а', 'c': 'с', 'e': 'е', 'o': 'о', 'p': 'р', 'x': 'х', 'y': 'у', 'k': 'к', 'm': 'м', 't': 'т', 'b': 'в',
})


def _looks_like_checkbox_marker(text: str) -> bool:
    return text in {'1', 'I', 'l', '|', '/', '\\', '■', '☑', '☒', '✓', '✔', 'V', 'v', 'X', 'x', '0', 'O', 'o', '□', '☐', '○', '◯'}


def _should_keep_ocr_line(*, text: str, confidence: float) -> bool:
    normalized = _normalize_line_text(text)
    if not normalized:
        return False
    if _looks_like_checkbox_marker(normalized):
        return True
    if confidence < OCR_MIN_CONFIDENCE:
        return False
    alnum_count = sum(1 for char in normalized if char.isalnum())
    if alnum_count == 0:
        return False
    if len(normalized) <= 2 and confidence < OCR_SHORT_LINE_MIN_CONFIDENCE:
        return False
    if len(normalized.split()) <= 2 and len(normalized) <= 8 and confidence < OCR_SHORT_LINE_MIN_CONFIDENCE:
        return False
    if len(set(normalized)) == 1 and len(normalized) >= 3:
        return False
    return True


def _fix_numeric_token(token: str) -> str:
    return (
        token.replace('о', '0')
        .replace('О', '0')
        .replace('o', '0')
        .replace('O', '0')
        .replace('I', '1')
        .replace('l', '1')
        .replace('|', '1')
        .replace('S', '5')
        .replace('s', '5')
    )


def _normalize_mixed_script_token(token: str) -> str:
    if not token:
        return token
    has_cyr = any('А' <= char <= 'я' or char in 'Ёё' for char in token)
    has_lat = any(('A' <= char <= 'Z') or ('a' <= char <= 'z') for char in token)
    if not (has_cyr and has_lat):
        return token
    cyr_count = sum(1 for char in token if ('А' <= char <= 'я') or char in 'Ёё')
    lat_count = sum(1 for char in token if ('A' <= char <= 'Z') or ('a' <= char <= 'z'))
    if cyr_count >= lat_count:
        return token.translate(LAT_TO_CYR)
    return token.translate(CYR_TO_LAT)


def _repair_date_token(token: str) -> str | None:
    normalized = _fix_numeric_token(token).replace('.', '-').replace('/', '-')
    if not DATE_RE.match(normalized):
        return None
    year_str, month_str, day_str = normalized.split('-')
    try:
        year = int(year_str)
        month = int(month_str)
        day = int(day_str)
    except ValueError:
        return None
    if not (1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100):
        return None
    return f'{year:04d}-{month:02d}-{day:02d}'


def _repair_token(token: str) -> str:
    cleaned = _normalize_line_text(token)
    if not cleaned:
        return ''
    if _looks_like_checkbox_marker(cleaned):
        return cleaned
    if DATE_RE.match(cleaned) or any(separator in cleaned for separator in ('-', '.', '/')):
        repaired_date = _repair_date_token(cleaned)
        if repaired_date is not None:
            return repaired_date
    mixed = _normalize_mixed_script_token(cleaned)
    if NUMERICISH_RE.match(mixed):
        return _fix_numeric_token(mixed)
    return mixed


def _is_noise_token(token: str) -> bool:
    normalized = _normalize_line_text(token)
    if not normalized:
        return True
    if _looks_like_checkbox_marker(normalized):
        return False
    alnum_count = sum(1 for char in normalized if char.isalnum())
    if alnum_count == 0:
        return True
    if len(normalized) < 2:
        return True
    if len(normalized) <= 2 and not normalized.isdigit():
        return True
    return False


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bbox_metrics(box: Any) -> tuple[float | None, float | None, float | None, float | None]:
    points = [point for point in (box or []) if isinstance(point, (list, tuple)) and len(point) >= 2]
    x_values = [float(point[0]) for point in points]
    y_values = [float(point[1]) for point in points]
    if not x_values or not y_values:
        return None, None, None, None
    min_x = min(x_values)
    max_x = max(x_values)
    min_y = min(y_values)
    max_y = max(y_values)
    return min_x, min_y, max_x - min_x, max_y - min_y


def _load_image(path: Path) -> Any | None:
    if cv2 is None:
        return None
    image = cv2.imread(str(path))
    if image is None or image.size == 0:
        return None
    return image


def _detect_image_kind(image: Any) -> str:
    if cv2 is None or np is None or image is None:
        return 'unknown'
    height, width = image.shape[:2]
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    edges = cv2.Canny(gray, 60, 180)
    edge_density = float(np.count_nonzero(edges)) / max(edges.size, 1)
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    binary_ratio = float(np.count_nonzero(otsu == 255)) / max(otsu.size, 1)
    top_band = gray[: max(1, int(height * 0.14)), :]
    top_std = float(np.std(top_band)) if top_band.size else 0.0
    aspect_ratio = width / max(height, 1)

    # Screenshot-like: wide, crisp edges, toolbar-like top band.
    if aspect_ratio >= 1.2 and edge_density >= 0.055 and top_std >= 28:
        return 'screenshot'
    # Scanned document: mostly binary page with low texture variation.
    if edge_density <= 0.045 and (binary_ratio <= 0.18 or binary_ratio >= 0.82):
        return 'scanned_document'
    return 'photo'


def _deskew_image(image: Any) -> Any:
    if cv2 is None or np is None:
        return image
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, threshold = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    coords = cv2.findNonZero(threshold)
    if coords is None or len(coords) < 10:
        return image
    angle = cv2.minAreaRect(coords)[-1]
    if angle < -45:
        angle = 90 + angle
    if abs(angle) < 0.7 or abs(angle) > 8.0:
        return image
    height, width = image.shape[:2]
    center = (width // 2, height // 2)
    matrix = cv2.getRotationMatrix2D(center, angle, 1.0)
    return cv2.warpAffine(image, matrix, (width, height), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)


def _detect_content_region(image: Any, *, image_kind: str) -> tuple[int, int, int, int] | None:
    if cv2 is None or np is None or image is None:
        return None
    height, width = image.shape[:2]
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    if image_kind == 'screenshot':
        top_cut = int(height * 0.1)
        if top_cut > 0:
            gray = gray[top_cut:, :]
        edges = cv2.Canny(gray, 50, 150)
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (9, 9))
        mask = cv2.dilate(edges, kernel, iterations=2)
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        best_rect: tuple[int, int, int, int] | None = None
        best_area = 0
        for contour in contours:
            x, y, w, h = cv2.boundingRect(contour)
            area = w * h
            if area < (width * height * 0.12):
                continue
            if w < width * 0.35 or h < height * 0.22:
                continue
            if area > best_area:
                best_area = area
                best_rect = (x, y + top_cut, w, h)
        return best_rect

    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    threshold = cv2.adaptiveThreshold(
        blur,
        255,
        cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY_INV,
        31,
        9,
    )
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (15, 15))
    mask = cv2.dilate(threshold, kernel, iterations=1)
    contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    best_rect = None
    best_area = 0
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        area = w * h
        if area < (width * height * 0.1):
            continue
        if w < width * 0.28 or h < height * 0.18:
            continue
        if area > best_area:
            best_area = area
            best_rect = (x, y, w, h)
    return best_rect


def _crop_to_region(image: Any, region: tuple[int, int, int, int] | None) -> Any:
    if image is None or region is None:
        return image
    x, y, w, h = region
    height, width = image.shape[:2]
    pad_x = max(8, int(w * 0.02))
    pad_y = max(8, int(h * 0.02))
    left = max(0, x - pad_x)
    top = max(0, y - pad_y)
    right = min(width, x + w + pad_x)
    bottom = min(height, y + h + pad_y)
    if right <= left or bottom <= top:
        return image
    return image[top:bottom, left:right]


def _build_zone_variants(
    image: Any,
    *,
    image_kind: str,
    temp_paths: list[Path],
) -> list[dict[str, Any]]:
    if cv2 is None or image is None:
        return []
    height, width = image.shape[:2]
    if height < 120 or width < 120:
        return []

    variants: list[dict[str, Any]] = []

    def add_zone(name: str, top_ratio: float, bottom_ratio: float, weight: float) -> None:
        top = max(0, min(height - 1, int(height * top_ratio)))
        bottom = max(top + 1, min(height, int(height * bottom_ratio)))
        if bottom - top < max(80, int(height * 0.12)):
            return
        zone = image[top:bottom, :]
        zone_path = _write_variant_image(zone, prefix=f'ocr-{name}', temp_paths=temp_paths)
        if zone_path is not None:
            variants.append({'name': name, 'path': zone_path, 'weight': weight})

    if image_kind == 'screenshot':
        add_zone('form_zone', 0.08, 0.72, 1.18)
        add_zone('text_zone', 0.22, 0.88, 1.08)
    elif image_kind == 'scanned_document':
        add_zone('header_zone', 0.0, 0.22, 0.96)
        add_zone('form_zone', 0.12, 0.78, 1.16)
        add_zone('text_zone', 0.25, 0.92, 1.06)
    else:
        add_zone('form_zone', 0.1, 0.76, 1.12)
        add_zone('text_zone', 0.18, 0.9, 1.05)

    return variants


def _write_variant_image(image: Any, *, prefix: str, temp_paths: list[Path]) -> Path | None:
    if cv2 is None:
        return None
    with tempfile.NamedTemporaryFile(suffix='.png', prefix=f'{prefix}-', delete=False) as tmp:
        output_path = Path(tmp.name)
    success = cv2.imwrite(str(output_path), image)
    if not success:
        output_path.unlink(missing_ok=True)
        return None
    temp_paths.append(output_path)
    return output_path


def _build_ocr_variants(path: Path) -> tuple[list[dict[str, Any]], list[Path], str]:
    variants: list[dict[str, Any]] = [{'name': 'original', 'path': path, 'weight': 1.0}]
    temp_paths: list[Path] = []
    image = _load_image(path)
    if image is None or cv2 is None:
        return variants, temp_paths, 'unknown'

    image_kind = _detect_image_kind(image)
    if image_kind in {'photo', 'scanned_document'}:
        image = _deskew_image(image)
    content_region = _detect_content_region(image, image_kind=image_kind)
    cropped_image = _crop_to_region(image, content_region)
    if cropped_image is not None and cropped_image is not image:
        cropped_path = _write_variant_image(cropped_image, prefix='ocr-crop', temp_paths=temp_paths)
        if cropped_path is not None:
            variants.append({'name': 'content_crop', 'path': cropped_path, 'weight': 1.09})
            if image_kind == 'screenshot':
                variants.append({'name': 'content_crop_screenshot', 'path': cropped_path, 'weight': 1.14})
            elif image_kind == 'scanned_document':
                variants.append({'name': 'content_crop_scan', 'path': cropped_path, 'weight': 1.13})
            else:
                variants.append({'name': 'content_crop_photo', 'path': cropped_path, 'weight': 1.1})
        image = cropped_image
    variants.extend(_build_zone_variants(image, image_kind=image_kind, temp_paths=temp_paths))
    height, width = image.shape[:2]
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    if image_kind == 'screenshot':
        adaptive = cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            11,
        )
        adaptive_path = _write_variant_image(adaptive, prefix='ocr-adaptive', temp_paths=temp_paths)
        if adaptive_path is not None:
            variants.append({'name': 'adaptive_threshold', 'path': adaptive_path, 'weight': 1.1})

        scale = 2.0 if max(height, width) < 2000 else 1.5
        upscaled = cv2.resize(gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        upscaled_path = _write_variant_image(upscaled, prefix='ocr-upscaled', temp_paths=temp_paths)
        if upscaled_path is not None:
            variants.append({'name': 'small_text_upscaled', 'path': upscaled_path, 'weight': 1.16})

        form_like = cv2.GaussianBlur(gray, (3, 3), 0)
        form_like = cv2.adaptiveThreshold(
            form_like,
            255,
            cv2.ADAPTIVE_THRESH_MEAN_C,
            cv2.THRESH_BINARY,
            21,
            9,
        )
        kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
        form_like = cv2.morphologyEx(form_like, cv2.MORPH_CLOSE, kernel)
        form_path = _write_variant_image(form_like, prefix='ocr-form', temp_paths=temp_paths)
        if form_path is not None:
            variants.append({'name': 'form_like_screenshot', 'path': form_path, 'weight': 1.2})
    elif image_kind == 'scanned_document':
        adaptive = cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_MEAN_C,
            cv2.THRESH_BINARY,
            27,
            7,
        )
        adaptive_path = _write_variant_image(adaptive, prefix='ocr-scan-threshold', temp_paths=temp_paths)
        if adaptive_path is not None:
            variants.append({'name': 'scanned_threshold', 'path': adaptive_path, 'weight': 1.12})

        scale = 1.5 if max(height, width) < 2400 else 1.25
        upscaled = cv2.resize(adaptive, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        upscaled_path = _write_variant_image(upscaled, prefix='ocr-scan-upscaled', temp_paths=temp_paths)
        if upscaled_path is not None:
            variants.append({'name': 'scanned_upscaled', 'path': upscaled_path, 'weight': 1.14})
    else:
        denoised = cv2.fastNlMeansDenoising(gray, None, 12, 7, 21)
        denoised_path = _write_variant_image(denoised, prefix='ocr-photo-denoised', temp_paths=temp_paths)
        if denoised_path is not None:
            variants.append({'name': 'photo_denoised', 'path': denoised_path, 'weight': 1.08})

        adaptive = cv2.adaptiveThreshold(
            denoised,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            13,
        )
        adaptive_path = _write_variant_image(adaptive, prefix='ocr-photo-threshold', temp_paths=temp_paths)
        if adaptive_path is not None:
            variants.append({'name': 'photo_threshold', 'path': adaptive_path, 'weight': 1.1})

    return variants, temp_paths, image_kind


def _extract_candidates(raw_result: Any, *, variant_name: str, variant_weight: float) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for page_index, page in enumerate(raw_result or [], start=1):
        if not isinstance(page, list):
            continue
        for line_index, item in enumerate(page, start=1):
            if not isinstance(item, list) or len(item) < 2:
                continue
            box = item[0] if isinstance(item[0], list) else []
            recognized = item[1] if isinstance(item[1], (tuple, list)) else []
            text = _normalize_line_text(recognized[0] if len(recognized) > 0 else '')
            confidence = float(recognized[1] if len(recognized) > 1 else 0.0)
            x, y, width, height = _bbox_metrics(box)
            candidates.append(
                {
                    'line_id': f'ocr-{variant_name}-{page_index}-{line_index}',
                    'text': text,
                    'confidence': confidence,
                    'bbox': box,
                    'page': page_index,
                    'variant_name': variant_name,
                    'variant_weight': variant_weight,
                    'x': x,
                    'y': y,
                    'width': width,
                    'height': height,
                }
            )
    return candidates


def _should_merge_fragments(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if int(left.get('page') or 0) != int(right.get('page') or 0):
        return False
    left_y = _safe_float(left.get('y'))
    right_y = _safe_float(right.get('y'))
    if left_y is None or right_y is None or abs(left_y - right_y) > OCR_FRAGMENT_Y_THRESHOLD:
        return False
    left_x = _safe_float(left.get('x'))
    left_width = _safe_float(left.get('width'))
    right_x = _safe_float(right.get('x'))
    if left_x is None or left_width is None or right_x is None:
        return False
    gap = right_x - (left_x + left_width)
    if gap < -4 or gap > OCR_FRAGMENT_GAP_THRESHOLD:
        return False
    left_text = str(left.get('text') or '')
    right_text = str(right.get('text') or '')
    if not left_text or not right_text:
        return False
    if _looks_like_checkbox_marker(left_text) or _looks_like_checkbox_marker(right_text):
        return True
    if len(left_text) <= 3 or len(right_text) <= 3:
        return True
    return gap <= 24


def _merge_fragmented_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        candidates,
        key=lambda item: (
            int(item.get('page') or 0),
            float(item.get('y') or 0.0),
            float(item.get('x') or 0.0),
        ),
    )
    merged: list[dict[str, Any]] = []
    for candidate in ordered:
        if not merged or not _should_merge_fragments(merged[-1], candidate):
            merged.append(dict(candidate))
            continue
        previous = merged[-1]
        previous['text'] = _normalize_line_text(f"{previous.get('text') or ''} {candidate.get('text') or ''}")
        previous['confidence'] = max(float(previous.get('confidence') or 0.0), float(candidate.get('confidence') or 0.0))
        if isinstance(previous.get('bbox'), list) and isinstance(candidate.get('bbox'), list):
            previous['bbox'] = list(previous.get('bbox') or []) + list(candidate.get('bbox') or [])
        x, y, width, height = _bbox_metrics(previous.get('bbox'))
        previous['x'] = x
        previous['y'] = y
        previous['width'] = width
        previous['height'] = height
    return merged


def _looks_like_ui_noise(candidate: dict[str, Any], *, image_height: int | None, repeated_texts: dict[str, int], image_kind: str) -> bool:
    text = str(candidate.get('text') or '')
    normalized = text.casefold()
    token_count = len(normalized.split())
    y = _safe_float(candidate.get('y')) or 0.0
    height = _safe_float(candidate.get('height')) or 0.0
    if not text:
        return True
    if 'http://' in normalized or 'https://' in normalized or 'www.' in normalized:
        return True
    if image_kind == 'screenshot' and any(hint in normalized for hint in ('chrome', 'firefox', 'safari', 'edge', 'вкладк', 'поиск', 'search', 'назад', 'поделиться', 'share', 'копировать')):
        return True
    if image_kind == 'screenshot' and repeated_texts.get(normalized, 0) > 1 and token_count <= 3 and len(text) <= 24:
        return True
    if image_kind == 'screenshot' and image_height and y <= image_height * SCREENSHOT_TOP_NOISE_RATIO:
        if token_count <= 6 and ('.' in text or ':' in text or len(text) <= 24):
            return True
        if sum(1 for char in text if char.isupper()) >= max(4, len(text) // 2):
            return True
    if image_kind == 'screenshot' and image_height and (y + height) >= image_height * 0.94 and token_count <= 5:
        return True
    return False


def _select_best_candidates(candidates: list[dict[str, Any]], *, image_height: int | None, image_kind: str) -> list[dict[str, Any]]:
    merged_candidates = _merge_fragmented_candidates(candidates)
    repeated_texts: dict[str, int] = {}
    for candidate in merged_candidates:
        text_key = str(candidate.get('text') or '').casefold()
        if text_key:
            repeated_texts[text_key] = repeated_texts.get(text_key, 0) + 1

    best_by_text: dict[str, dict[str, Any]] = {}
    for candidate in merged_candidates:
        text = str(candidate.get('text') or '')
        confidence = float(candidate.get('confidence') or 0.0)
        if not _should_keep_ocr_line(text=text, confidence=confidence):
            continue
        if _looks_like_ui_noise(candidate, image_height=image_height, repeated_texts=repeated_texts, image_kind=image_kind):
            continue
        text_key = text.casefold()
        score = confidence * float(candidate.get('variant_weight') or 1.0)
        existing = best_by_text.get(text_key)
        existing_score = float(existing.get('_score') or 0.0) if existing else -1.0
        if existing is None or score > existing_score:
            best_by_text[text_key] = {**candidate, '_score': score}

    selected = list(best_by_text.values())
    selected.sort(key=lambda item: (int(item.get('page') or 0), float(item.get('y') or 0.0), float(item.get('x') or 0.0)))
    for item in selected:
        item.pop('_score', None)
    return selected


def _reconstruct_rows(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(
        candidates,
        key=lambda item: (
            int(item.get('page') or 0),
            float(item.get('y') or 0.0),
            float(item.get('x') or 0.0),
        ),
    )
    rows: list[dict[str, Any]] = []
    for candidate in ordered:
        page = int(candidate.get('page') or 0)
        y = _safe_float(candidate.get('y'))
        if y is None:
            continue
        target_row = None
        for row in reversed(rows):
            if int(row.get('page') or 0) != page:
                continue
            row_y = _safe_float(row.get('y'))
            if row_y is None:
                continue
            if abs(row_y - y) <= OCR_ROW_Y_THRESHOLD:
                target_row = row
                break
            if y - row_y > OCR_ROW_Y_THRESHOLD * 1.5:
                break
        if target_row is None:
            rows.append({'page': page, 'y': y, 'items': [dict(candidate)]})
        else:
            target_row['items'].append(dict(candidate))
            target_row['y'] = min(float(target_row.get('y') or y), y)
    reconstructed: list[dict[str, Any]] = []
    for row_index, row in enumerate(rows, start=1):
        items = sorted(
            [dict(item) for item in list(row.get('items') or []) if isinstance(item, dict)],
            key=lambda item: float(item.get('x') or 0.0),
        )
        tokens: list[str] = []
        previous_right = None
        for item in items:
            token = _repair_token(str(item.get('text') or ''))
            if not token or _is_noise_token(token):
                continue
            x = _safe_float(item.get('x'))
            width = _safe_float(item.get('width'))
            if previous_right is not None and x is not None and x - previous_right > OCR_ROW_X_GAP_THRESHOLD:
                tokens.append('|')
            tokens.append(token)
            if x is not None and width is not None:
                previous_right = x + width
        row_text = _normalize_line_text(' '.join(tokens).replace(' | ', ' | '))
        if not row_text:
            continue
        reconstructed.append(
            {
                'line_id': f"ocr-row-{int(row.get('page') or 1)}-{row_index}",
                'text': row_text,
                'confidence': max(float(item.get('confidence') or 0.0) for item in items) if items else 0.0,
                'bbox': [],
                'page': int(row.get('page') or 1),
            }
        )
    return reconstructed


def _detect_checkbox_candidates(
    image: Any,
    *,
    image_kind: str,
    existing_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if cv2 is None or np is None or image is None:
        return []
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (2, 2))
    binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel)
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    height, width = gray.shape[:2]
    max_dim = max(height, width)

    candidates: list[dict[str, Any]] = []
    seen: set[tuple[int, int, int, int]] = set()
    for contour in contours:
        x, y, w, h = cv2.boundingRect(contour)
        if w < CHECKBOX_MIN_SIZE_PX or h < CHECKBOX_MIN_SIZE_PX:
            continue
        if w > max_dim * CHECKBOX_MAX_SIZE_RATIO or h > max_dim * CHECKBOX_MAX_SIZE_RATIO:
            continue
        aspect_ratio = w / max(h, 1)
        if not 0.55 <= aspect_ratio <= 1.65:
            continue
        area = float(cv2.contourArea(contour))
        box_area = float(w * h)
        if box_area <= 0:
            continue
        fill_ratio = area / box_area
        if not 0.08 <= fill_ratio <= 0.95:
            continue

        related_text = None
        best_score = None
        for text_candidate in existing_candidates:
            text_x = _safe_float(text_candidate.get('x'))
            text_y = _safe_float(text_candidate.get('y'))
            text_h = _safe_float(text_candidate.get('height'))
            if text_x is None or text_y is None or text_h is None:
                continue
            if abs((y + h / 2) - (text_y + text_h / 2)) > max(14.0, text_h * 0.8):
                continue
            if text_x <= x:
                continue
            gap = text_x - (x + w)
            if gap < -2 or gap > 64:
                continue
            score = gap + abs((y + h / 2) - (text_y + text_h / 2))
            if best_score is None or score < best_score:
                best_score = score
                related_text = text_candidate

        if related_text is None:
            continue

        marker_text = 'X' if fill_ratio >= 0.22 else '[ ]'
        marker_key = (x, y, w, h)
        if marker_key in seen:
            continue
        seen.add(marker_key)
        candidates.append(
            {
                'line_id': f'ocr-marker-{len(candidates) + 1}',
                'text': marker_text,
                'confidence': 0.92 if marker_text == 'X' else 0.78,
                'bbox': [[x, y], [x + w, y], [x + w, y + h], [x, y + h]],
                'page': int(related_text.get('page') or 1),
                'variant_name': f'{image_kind}_checkbox_detector',
                'variant_weight': 1.24 if marker_text == 'X' else 1.08,
                'x': float(x),
                'y': float(y),
                'width': float(w),
                'height': float(h),
            }
        )
    return candidates


def _get_ocr() -> PaddleOCR:
    global _ocr_instance, _ocr_ready, _ocr_startup_error, _ocr_warmup_in_progress
    if PaddleOCR is None:
        logger.error('paddleocr import unavailable: import_error=%s', _paddleocr_import_error)
        raise HTTPException(status_code=503, detail='PaddleOCR is not installed in this container.')
    if _ocr_instance is not None:
        return _ocr_instance
    with _ocr_init_lock:
        if _ocr_instance is not None:
            return _ocr_instance
        _ocr_warmup_in_progress = True
        try:
            logger.info(
                'initializing PaddleOCR: lang=ru use_angle_cls=true det_model_dir=%s rec_model_dir=%s cls_model_dir=%s',
                DET_MODEL_DIR,
                REC_MODEL_DIR,
                CLS_MODEL_DIR,
            )
            _ocr_instance = PaddleOCR(
                use_angle_cls=True,
                lang='ru',
                det_model_dir=DET_MODEL_DIR,
                rec_model_dir=REC_MODEL_DIR,
                cls_model_dir=CLS_MODEL_DIR,
                show_log=False,
            )
            logger.info('PaddleOCR initialized successfully')
            _ocr_ready = True
            _ocr_startup_error = None
            return _ocr_instance
        except Exception as exc:  # noqa: BLE001
            _ocr_ready = False
            _ocr_startup_error = str(exc)
            logger.exception('PaddleOCR initialization failed: error=%s', exc)
            raise
        finally:
            _ocr_warmup_in_progress = False


def _warmup_ocr_in_background() -> None:
    global _ocr_warmup_started, _ocr_warmup_in_progress
    if PaddleOCR is None or _ocr_warmup_started:
        return
    _ocr_warmup_started = True

    def runner() -> None:
        global _ocr_ready, _ocr_startup_error, _ocr_warmup_in_progress
        try:
            _ocr_warmup_in_progress = True
            logger.info('ocr service background warmup started')
            _get_ocr()
            _ocr_ready = True
            _ocr_startup_error = None
            logger.info('ocr service background warmup completed successfully')
        except Exception as exc:  # noqa: BLE001
            _ocr_ready = False
            _ocr_startup_error = str(exc)
            logger.exception('ocr service background warmup failed: error=%s', exc)
        finally:
            _ocr_warmup_in_progress = False

    threading.Thread(target=runner, name='ocr-warmup', daemon=True).start()


@app.on_event('startup')
def on_startup() -> None:
    global _ocr_ready, _ocr_startup_error
    logger.info(
        'ocr service startup: paddleocr_available=%s import_error=%s',
        PaddleOCR is not None,
        _paddleocr_import_error,
    )
    if PaddleOCR is None:
        _ocr_ready = False
        _ocr_startup_error = _paddleocr_import_error or 'PaddleOCR import failed.'
        return
    _warmup_ocr_in_background()


@app.get('/health')
def health() -> dict[str, object]:
    return {
        'status': 'ok',
        'paddleocr_available': PaddleOCR is not None,
        'ocr_ready': _ocr_ready,
        'warmup_started': _ocr_warmup_started,
        'warmup_in_progress': _ocr_warmup_in_progress,
        'startup_error': _ocr_startup_error,
        'import_error': _paddleocr_import_error,
    }


@app.post('/ocr/extract')
def extract(payload: OcrExtractPayload) -> dict[str, object]:
    logger.info(
        'ocr extract requested: filename=%s payload_base64_chars=%d',
        payload.filename,
        len(payload.content_base64 or ''),
    )
    try:
        content = base64.b64decode(payload.content_base64.encode('ascii'))
    except Exception as exc:  # noqa: BLE001
        logger.warning('invalid base64 payload: filename=%s error=%s', payload.filename, exc)
        raise HTTPException(status_code=400, detail=f'Invalid base64 payload: {exc}') from exc

    suffix = Path(payload.filename).suffix or '.bin'
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(content)
        tmp_path = Path(tmp.name)
    logger.info('ocr temp file created: filename=%s path=%s size_bytes=%d', payload.filename, tmp_path, len(content))

    variant_temp_paths: list[Path] = []
    try:
        if _ocr_instance is None and not _ocr_warmup_started:
            _warmup_ocr_in_background()
        ocr = _get_ocr()
        image = _load_image(tmp_path)
        image_height = int(image.shape[0]) if image is not None else None
        variants, variant_temp_paths, image_kind = _build_ocr_variants(tmp_path)
        logger.info('ocr image kind detected: filename=%s image_kind=%s variants=%d', payload.filename, image_kind, len(variants))
        all_candidates: list[dict[str, Any]] = []
        for variant in variants:
            logger.info(
                'ocr variant started: filename=%s variant=%s path=%s',
                payload.filename,
                variant['name'],
                variant['path'],
            )
            variant_result = ocr.ocr(str(variant['path']), cls=True)
            candidates = _extract_candidates(
                variant_result,
                variant_name=str(variant['name']),
                variant_weight=float(variant.get('weight') or 1.0),
            )
            logger.info(
                'ocr variant completed: filename=%s variant=%s candidates=%d',
                payload.filename,
                variant['name'],
                len(candidates),
            )
            all_candidates.extend(candidates)
        if image is not None:
            checkbox_candidates = _detect_checkbox_candidates(
                image,
                image_kind=image_kind,
                existing_candidates=all_candidates,
            )
            if checkbox_candidates:
                logger.info(
                    'ocr checkbox candidates detected: filename=%s image_kind=%s candidates=%d',
                    payload.filename,
                    image_kind,
                    len(checkbox_candidates),
                )
                all_candidates.extend(checkbox_candidates)
        selected_candidates = _select_best_candidates(all_candidates, image_height=image_height, image_kind=image_kind)
    except HTTPException:
        logger.exception('ocr extraction unavailable: filename=%s', payload.filename)
        raise
    except Exception as exc:  # noqa: BLE001
        logger.exception('ocr extraction failed: file=%s error=%s', payload.filename, exc)
        raise HTTPException(status_code=500, detail=f'OCR extraction failed: {exc}') from exc
    finally:
        tmp_path.unlink(missing_ok=True)
        logger.info('ocr temp file removed: filename=%s path=%s', payload.filename, tmp_path)
        for variant_path in variant_temp_paths:
            variant_path.unlink(missing_ok=True)

    row_candidates = _reconstruct_rows(selected_candidates)
    lines: list[dict[str, object]] = []
    texts: list[str] = []
    if row_candidates:
        for row in row_candidates:
            text = _normalize_line_text(row.get('text'))
            if not text:
                continue
            texts.append(text)
            lines.append(
                {
                    'line_id': str(row.get('line_id') or ''),
                    'text': text,
                    'confidence': float(row.get('confidence') or 0.0),
                    'bbox': list(row.get('bbox') or []),
                    'page': int(row.get('page') or 1),
                }
            )
    else:
        for index, candidate in enumerate(selected_candidates, start=1):
            text = _repair_token(str(candidate.get('text') or ''))
            if not text or _is_noise_token(text):
                continue
            confidence = float(candidate.get('confidence') or 0.0)
            texts.append(text)
            lines.append(
                {
                    'line_id': str(candidate.get('line_id') or f'ocr-1-{index}'),
                    'text': text,
                    'confidence': confidence,
                    'bbox': list(candidate.get('bbox') or []),
                    'page': int(candidate.get('page') or 1),
                }
            )

    logger.info(
        'ocr extraction completed: filename=%s variants=%d selected_candidates=%d reconstructed_rows=%d text_chars=%d',
        payload.filename,
        len(variants if 'variants' in locals() else []),
        len(selected_candidates),
        len(lines),
        len('\n'.join(texts)),
    )

    return {
        'provider': 'paddleocr',
        'text': '\n'.join(texts),
        'lines': lines,
    }
