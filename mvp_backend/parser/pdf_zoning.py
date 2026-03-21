from __future__ import annotations

from typing import Any


CHECK_MARKERS = {'x', 'X', 'v', 'V', '✓', '✔', '☒', '☑', '■', '1', '□', '☐', '○', '◯', '0'}
REGION_Y_GAP_THRESHOLD = 26.0
HEADING_KEYWORDS = {
    'сведения',
    'анкета',
    'форма',
    'приложение',
    'согласие',
    'подпись',
    'дата',
    'page',
}
QUESTION_HINTS = (
    'является ли',
    'являются ли',
    'is ',
    'are ',
    '?',
)


def classify_pdf_document_zones(
    *,
    tables: list[dict[str, Any]] | None = None,
    layout_lines: list[dict[str, Any]] | None = None,
    raw_text: str = '',
) -> dict[str, Any]:
    tables = [dict(table) for table in (tables or []) if isinstance(table, dict)]
    layout_lines = [dict(line) for line in (layout_lines or []) if isinstance(line, dict)]

    zones: list[dict[str, Any]] = []
    data_tables: list[dict[str, Any]] = []
    form_tables: list[dict[str, Any]] = []
    for index, table in enumerate(tables):
        zone_type, score, signals = _classify_table_zone(table)
        zone = {
            'zone_id': f'table_{index + 1}',
            'zone_type': zone_type,
            'source_kind': 'table',
            'score': round(score, 4),
            'zone_confidence': round(score, 4),
            'signals': signals,
            'table_index': index,
            'table_name': str(table.get('name') or f'Table {index + 1}'),
            'row_count': len(list(table.get('rows') or [])),
            'column_count': len(list(table.get('columns') or [])),
            'page': _extract_table_page(table),
            'column_id': None,
        }
        zones.append(zone)
        if zone_type == 'table':
            data_tables.append(table)
        elif zone_type == 'form':
            form_tables.append(table)

    line_zones = _classify_line_zones(layout_lines)
    region_zones = _build_region_zones(line_zones['zones'])
    zones.extend(region_zones)

    counts = {
        'table': len([zone for zone in zones if zone['zone_type'] == 'table']),
        'form': len([zone for zone in zones if zone['zone_type'] == 'form']),
        'text': len([zone for zone in zones if zone['zone_type'] == 'text']),
        'noise': len([zone for zone in zones if zone['zone_type'] == 'noise']),
    }
    dominant_zone = max(counts, key=counts.get) if any(counts.values()) else 'text'
    has_text = bool(raw_text.strip())

    if data_tables and (counts['form'] or counts['text']):
        content_type = 'mixed'
        extraction_status = 'structured_extracted'
    elif data_tables:
        content_type = 'table'
        extraction_status = 'structured_extracted'
    elif counts['form'] or form_tables:
        content_type = 'form'
        extraction_status = 'text_extracted'
    elif counts['text'] or has_text:
        content_type = 'text'
        extraction_status = 'text_extracted' if has_text else 'text_not_extracted'
    else:
        content_type = 'image_like'
        extraction_status = 'requires_ocr_or_manual_input'

    text_regions = [zone for zone in region_zones if zone['zone_type'] == 'text']
    form_regions = [zone for zone in region_zones if zone['zone_type'] == 'form']
    noise_regions = [zone for zone in region_zones if zone['zone_type'] == 'noise']
    zone_graph = _build_zone_graph(zones)
    parser_outputs = {
        'table': {
            'zones': [zone for zone in zones if zone['source_kind'] == 'table' and zone['zone_type'] == 'table'],
            'tables': data_tables,
        },
        'form': {
            'zones': [zone for zone in zones if zone['zone_type'] == 'form'],
            'tables': form_tables,
            'regions': form_regions,
        },
        'text': {
            'zones': [zone for zone in zones if zone['zone_type'] == 'text'],
            'regions': text_regions,
        },
        'noise': {
            'zones': [zone for zone in zones if zone['zone_type'] == 'noise'],
            'regions': noise_regions,
        },
    }

    return {
        'zones': zones,
        'line_zones': line_zones['zones'],
        'region_zones': region_zones,
        'zone_graph': zone_graph,
        'parser_outputs': parser_outputs,
        'counts': counts,
        'dominant_zone': dominant_zone,
        'data_tables': data_tables,
        'form_tables': form_tables,
        'text_regions': text_regions,
        'form_regions': form_regions,
        'noise_regions': noise_regions,
        'content_type': content_type,
        'extraction_status': extraction_status,
    }


def _classify_table_zone(table: dict[str, Any]) -> tuple[str, float, dict[str, Any]]:
    columns = [str(column or '').strip() for column in list(table.get('columns') or [])]
    rows = [row for row in list(table.get('rows') or []) if isinstance(row, dict)]
    row_count = len(rows)
    column_count = len(columns)
    if not rows or not columns:
        return 'noise', 0.1, {'reason': 'empty_table'}

    non_empty_ratios: list[float] = []
    marker_hits = 0
    short_cell_hits = 0
    data_like_rows = 0
    for row in rows:
        values = [str(row.get(column) or '').strip() for column in columns]
        non_empty = [value for value in values if value]
        non_empty_ratios.append(len(non_empty) / max(column_count, 1))
        marker_hits += sum(1 for value in non_empty if value in CHECK_MARKERS or value.startswith(('X ', 'x ', '[X]', '[x]')))
        short_cell_hits += sum(1 for value in non_empty if len(value.split()) <= 3)
        if len(non_empty) >= max(2, column_count // 2):
            data_like_rows += 1

    avg_fill = sum(non_empty_ratios) / len(non_empty_ratios) if non_empty_ratios else 0.0
    header_joined = ' '.join(columns).lower()
    question_like_header = any(hint in header_joined for hint in QUESTION_HINTS)
    header_short = sum(1 for column in columns if len(column.split()) <= 3) >= max(1, column_count // 2)

    table_score = 0.0
    if row_count >= 2:
        table_score += 0.25
    if data_like_rows >= max(2, row_count // 2):
        table_score += 0.25
    if avg_fill >= 0.55:
        table_score += 0.2
    if header_short and not question_like_header:
        table_score += 0.15

    form_score = 0.0
    if marker_hits > 0:
        form_score += 0.35
    if avg_fill <= 0.6:
        form_score += 0.2
    if question_like_header:
        form_score += 0.2
    if short_cell_hits >= max(4, row_count):
        form_score += 0.15

    if form_score >= table_score and form_score >= 0.35:
        return 'form', form_score, {
            'avg_fill_ratio': round(avg_fill, 4),
            'marker_hits': marker_hits,
            'data_like_rows': data_like_rows,
        }
    if table_score >= 0.35:
        return 'table', table_score, {
            'avg_fill_ratio': round(avg_fill, 4),
            'marker_hits': marker_hits,
            'data_like_rows': data_like_rows,
        }
    return 'noise', max(table_score, form_score, 0.1), {
        'avg_fill_ratio': round(avg_fill, 4),
        'marker_hits': marker_hits,
        'data_like_rows': data_like_rows,
    }


def _classify_line_zones(layout_lines: list[dict[str, Any]]) -> dict[str, Any]:
    zones: list[dict[str, Any]] = []
    for index, line in enumerate(layout_lines):
        text = str(line.get('text') or '').strip()
        if not text:
            continue
        normalized = text.casefold()
        token_count = len(text.split())
        uppercase_ratio = _uppercase_ratio(text)
        zone_type = 'text'
        score = 0.5
        reason = 'paragraph'

        if _looks_like_noise_line(text, normalized=normalized, token_count=token_count, uppercase_ratio=uppercase_ratio):
            zone_type = 'noise'
            score = 0.82
            reason = 'heading_or_boilerplate'
        elif _looks_like_form_line(text, normalized=normalized, token_count=token_count):
            zone_type = 'form'
            score = 0.78
            reason = 'field_or_option'
        elif token_count >= 10:
            zone_type = 'text'
            score = 0.72
            reason = 'paragraph'

        zones.append(
            {
                'zone_id': str(line.get('line_id') or line.get('id') or f'line_{index + 1}'),
                'zone_type': zone_type,
                'source_kind': 'line',
                'score': round(score, 4),
                'zone_confidence': round(score, 4),
                'signals': {'reason': reason, 'token_count': token_count},
                'line_id': str(line.get('line_id') or line.get('id') or f'line_{index + 1}'),
                'page': line.get('page'),
                'column_id': line.get('column_id'),
                'y': line.get('y'),
                'text': text,
            }
        )
    return {'zones': zones}


def _build_region_zones(line_zones: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_lines = sorted(
        [zone for zone in line_zones if zone.get('source_kind') == 'line'],
        key=lambda zone: (
            int(zone.get('page') or 0),
            int(zone.get('column_id') or 0),
            float(zone.get('y') or 0.0),
            str(zone.get('line_id') or ''),
        ),
    )
    regions: list[dict[str, Any]] = []
    for line in ordered_lines:
        page = int(line.get('page') or 0)
        column_id = int(line.get('column_id') or 0)
        y = float(line.get('y') or 0.0)
        target_region = None
        for region in reversed(regions):
            if region['page'] != page or region['column_id'] != column_id:
                continue
            if y - region['max_y'] > REGION_Y_GAP_THRESHOLD:
                break
            target_region = region
            break
        if target_region is None:
            target_region = {
                'page': page,
                'column_id': column_id,
                'min_y': y,
                'max_y': y,
                'lines': [],
            }
            regions.append(target_region)
        target_region['lines'].append(line)
        target_region['min_y'] = min(target_region['min_y'], y)
        target_region['max_y'] = max(target_region['max_y'], y)

    region_zones: list[dict[str, Any]] = []
    for index, region in enumerate(regions, start=1):
        zone_type, score, signals = _classify_region_zone(region['lines'])
        region_zones.append(
            {
                'zone_id': f"region_p{region['page']}_c{region['column_id']}_{index}",
                'zone_type': zone_type,
                'source_kind': 'region',
                'score': round(score, 4),
                'zone_confidence': round(score, 4),
                'signals': signals,
                'page': region['page'],
                'column_id': region['column_id'],
                'line_ids': [str(line.get('line_id') or '') for line in region['lines'] if str(line.get('line_id') or '')],
                'text': '\n'.join(str(line.get('text') or '') for line in region['lines'] if str(line.get('text') or '').strip()),
            }
        )
    return region_zones


def _classify_region_zone(lines: list[dict[str, Any]]) -> tuple[str, float, dict[str, Any]]:
    if not lines:
        return 'noise', 0.1, {'reason': 'empty_region'}

    counts = {'form': 0, 'text': 0, 'noise': 0}
    token_count = 0
    for line in lines:
        zone_type = str(line.get('zone_type') or 'text')
        if zone_type in counts:
            counts[zone_type] += 1
        token_count += int(line.get('signals', {}).get('token_count') or 0)

    line_count = len(lines)
    avg_tokens = token_count / max(line_count, 1)
    joined = ' '.join(str(line.get('text') or '') for line in lines).strip()
    normalized = joined.casefold()

    if counts['noise'] >= max(1, line_count - 1):
        return 'noise', 0.86, {'reason': 'mostly_noise_lines', 'line_count': line_count}
    if counts['form'] >= max(1, line_count // 2):
        return 'form', 0.8, {'reason': 'mostly_form_lines', 'line_count': line_count}
    if avg_tokens >= 9 or line_count >= 3:
        return 'text', 0.74, {'reason': 'paragraph_region', 'line_count': line_count}
    if any(keyword in normalized for keyword in HEADING_KEYWORDS):
        return 'noise', 0.78, {'reason': 'heading_region', 'line_count': line_count}
    return 'text', 0.58, {'reason': 'short_text_region', 'line_count': line_count}


def _looks_like_form_line(text: str, *, normalized: str, token_count: int) -> bool:
    if ':' in text and 1 <= token_count <= 12:
        return True
    if any(marker in text for marker in ('[X]', '[x]', '☒', '☑', '✓', '✔')):
        return True
    if text[:1] in {'X', 'x', 'V', 'v'} and token_count <= 16:
        return True
    if any(hint in normalized for hint in QUESTION_HINTS):
        return True
    return False


def _looks_like_noise_line(text: str, *, normalized: str, token_count: int, uppercase_ratio: float) -> bool:
    if token_count == 0:
        return True
    if uppercase_ratio >= 0.9 and token_count >= 3:
        return True
    if any(keyword in normalized for keyword in HEADING_KEYWORDS) and token_count >= 3:
        return True
    if token_count >= 18:
        return True
    return False


def _uppercase_ratio(text: str) -> float:
    letters = [char for char in text if char.isalpha()]
    if not letters:
        return 0.0
    uppercase = sum(1 for char in letters if char.isupper())
    return uppercase / len(letters)


def _build_zone_graph(zones: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        [dict(zone) for zone in zones if isinstance(zone, dict)],
        key=lambda zone: (
            int(zone.get('page') or 0),
            int(zone.get('column_id') or 0),
            _safe_float(zone.get('y')) or 0.0,
            str(zone.get('zone_id') or ''),
        ),
    )
    nodes = [
        {
            'zone_id': str(zone.get('zone_id') or ''),
            'zone_type': str(zone.get('zone_type') or ''),
            'source_kind': str(zone.get('source_kind') or ''),
            'page': zone.get('page'),
            'column_id': zone.get('column_id'),
            'zone_confidence': zone.get('zone_confidence'),
        }
        for zone in ordered
        if str(zone.get('zone_id') or '')
    ]
    edges: list[dict[str, Any]] = []
    for index, current in enumerate(ordered):
        current_id = str(current.get('zone_id') or '')
        if not current_id:
            continue
        for candidate in ordered[index + 1:]:
            if int(candidate.get('page') or 0) != int(current.get('page') or 0):
                break
            if candidate.get('source_kind') == 'line' or current.get('source_kind') == 'line':
                continue
            candidate_id = str(candidate.get('zone_id') or '')
            if not candidate_id:
                continue
            current_column = int(current.get('column_id') or 0)
            candidate_column = int(candidate.get('column_id') or 0)
            relation = None
            if current_column == candidate_column:
                relation = 'next_in_column'
            elif candidate_column > current_column:
                relation = 'adjacent_column'
            if relation is None:
                continue
            edges.append(
                {
                    'from_zone_id': current_id,
                    'to_zone_id': candidate_id,
                    'relation': relation,
                }
            )
            if relation == 'next_in_column':
                break
    return {'nodes': nodes, 'edges': edges}


def _extract_table_page(table: dict[str, Any]) -> int | None:
    name = str(table.get('name') or '')
    if 'Page ' not in name:
        return None
    try:
        return int(name.split('Page ', 1)[1].split()[0])
    except (IndexError, ValueError):
        return None


def _safe_float(value: Any) -> float | None:
    try:
        if value in (None, ''):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
