from __future__ import annotations

import re
from pathlib import Path
from typing import Any

try:
    from docx import Document
except ImportError:  # pragma: no cover - optional dependency in local env
    Document = None

from model_client import suggest_form_field_repair


CHECK_MARKERS = {'x', 'X', 'v', 'V', '✓', '✔', '☒', '☑', '■', '1'}
UNCHECKED_MARKERS = {'□', '☐', '○', '◯', '0'}
LINE_SPLIT_RE = re.compile(r'[\n\r]+')
SPACE_RE = re.compile(r'\s+')
TOKEN_RE = re.compile(r'[a-zA-Zа-яА-Я0-9]+')
KV_RE = re.compile(r'^(?P<label>[^:]{2,120}?)\s*:\s*(?P<value>.+)$')
PDF_MERGE_Y_THRESHOLD = 6.0
PDF_MERGE_X_THRESHOLD = 32.0
PDF_WRAP_Y_THRESHOLD = 14.0
PDF_WRAP_X_THRESHOLD = 96.0

ORGANIZATION_LABEL_ALIASES = (
    'наименование организации',
    'полное наименование организации',
    'organization name',
    'company name',
    'наименование',
)
INN_LABEL_ALIASES = (
    'инн',
    'кио',
    'инн кио',
    'инн/кио',
    'inn',
    'kio',
    'inn or kio',
)
TAX_GROUP_KEYWORDS = (
    'налогов резидент',
    'налогов резидент рф',
    'tax residenc',
    'resident rf',
)
FATCA_GROUP_KEYWORDS = ('fatca', 'foreign account tax compliance act')

TAX_RESIDENCY_ENUM_MAP = {
    'да': 'YES',
    'yes': 'YES',
    'не являюсь налоговым резидентом ни в одном государстве': 'NOWHERE',
    'не являюсь налоговым резидентом': 'NOWHERE',
    'не являюсь резидентом ни в одном государстве': 'NOWHERE',
    'нет является резидентом иностранных государств': 'NO',
    'нет является налоговым резидентом в иностранном государстве': 'NO',
    'нет': 'NO',
    'no': 'NO',
}

FATCA_OPTION_MAP = {
    'лицом неотделимым от собственника': 'IS_DISREGARDED_ENTITY',
    'иностранным финансовым институтом': 'IS_FATCA_FOREIGN_INSTITUTE',
    'более 10 акций': 'TEN_OR_MORE_PERCENT_IN_USA',
    'более 10 процентов акций': 'TEN_OR_MORE_PERCENT_IN_USA',
    'не применимы': 'STATEMENTS_NOT_APPILCABLE',
}


def extract_layout_layer(
    *,
    file_path: str | Path,
    file_type: str,
    raw_text: str,
    tables: list[dict[str, Any]] | None = None,
    kv_pairs: list[dict[str, Any]] | None = None,
    text_blocks: list[dict[str, Any]] | None = None,
    sections: list[dict[str, Any]] | None = None,
    layout_blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    tables = tables or []
    kv_pairs = kv_pairs or []
    text_blocks = text_blocks or []
    sections = sections or []
    layout_blocks = layout_blocks or []

    seed_scalars: list[dict[str, Any]] = [
        {
            'label': str(pair.get('label') or '').strip(),
            'value': pair.get('value'),
            'source_ref': {
                'source_type': 'line',
                'source_text': str(pair.get('source_text') or ''),
            },
            'confidence': str(pair.get('confidence') or 'high'),
        }
        for pair in kv_pairs
        if str(pair.get('label') or '').strip() and pair.get('value') not in (None, '')
    ]
    layout_lines: list[dict[str, Any]] = []
    raw_table_rows: list[dict[str, Any]] = []
    layout_meta: dict[str, Any] = {
        'file_type': file_type,
        'table_count': len(tables),
        'section_count': len(sections),
        'text_block_count': len(text_blocks),
        'kv_pair_count': len(kv_pairs),
        'reading_order_mode': 'document_order',
        'pipeline_layers': {
            'layout_extraction': {
                'status': 'completed',
                'file_type': file_type,
            },
            'generic_form_understanding': {'status': 'pending'},
            'business_mapping': {'status': 'pending'},
        },
    }

    path = Path(file_path)
    if file_type == 'docx' and Document is not None and path.exists():
        doc = Document(str(path))
        for paragraph_index, paragraph in enumerate(doc.paragraphs):
            text = _clean_text(paragraph.text)
            if not text:
                continue
            layout_lines.append(
                {
                    'text': text,
                    'paragraph_idx': paragraph_index,
                    'source_type': 'paragraph',
                    'tokens': [],
                }
            )
            kv_scalar = _parse_kv_line(text, source_ref={'source_type': 'paragraph', 'paragraph_idx': paragraph_index})
            if kv_scalar is not None:
                seed_scalars.append(kv_scalar)

        table_rows, table_lines, table_meta = _extract_docx_layout_rows(doc)
        raw_table_rows.extend(table_rows)
        layout_lines.extend(table_lines)
        layout_meta.update(table_meta)
        layout_meta['reading_order_mode'] = 'docx_paragraph_table_order'
    else:
        layout_lines.extend(_build_text_layout_lines(raw_text=raw_text, text_blocks=text_blocks, layout_blocks=layout_blocks))
        if any(line.get('page') is not None for line in layout_lines):
            layout_meta['reading_order_mode'] = 'page_column_yx'
        elif layout_lines:
            layout_meta['reading_order_mode'] = 'line_order'

    layout_meta['pipeline_layers']['layout_extraction'].update(
        {
            'layout_line_count': len(layout_lines),
            'raw_table_row_count': len(raw_table_rows),
            'seed_scalar_count': len(seed_scalars),
            'reading_order_mode': layout_meta['reading_order_mode'],
        }
    )
    return {
        'layout_lines': layout_lines,
        'seed_scalars': seed_scalars,
        'raw_table_rows': raw_table_rows,
        'layout_meta': layout_meta,
        'sections': list(sections),
    }


def understand_generic_form(
    *,
    layout_layer: dict[str, Any],
    tables: list[dict[str, Any]] | None = None,
    kv_pairs: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    tables = tables or []
    kv_pairs = kv_pairs or []
    scalars = [dict(item) for item in layout_layer.get('seed_scalars', []) if isinstance(item, dict)]
    layout_lines = [dict(item) for item in layout_layer.get('layout_lines', []) if isinstance(item, dict)]
    raw_table_rows = [dict(item) for item in layout_layer.get('raw_table_rows', []) if isinstance(item, dict)]
    sections = [dict(item) for item in layout_layer.get('sections', []) if isinstance(item, dict)]
    groups: list[dict[str, Any]] = []
    layout_meta = dict(layout_layer.get('layout_meta') or {})

    if raw_table_rows:
        table_scalars, table_groups = _extract_docx_form_understanding(raw_table_rows)
        scalars.extend(table_scalars)
        groups.extend(table_groups)
    else:
        groups.extend(_extract_groups_from_layout_lines(layout_lines))
        scalars.extend(_extract_scalars_from_layout_lines(layout_lines))

    groups = _dedupe_groups(groups)
    scalars = _dedupe_scalars(scalars)
    section_hierarchy = _build_section_hierarchy(sections, groups)

    document_mode = 'data_table_mode'
    if groups or _looks_like_form_layout(kv_pairs=kv_pairs, groups=groups, tables=tables, layout_lines=layout_lines):
        document_mode = 'form_layout_mode'

    if not scalars and not groups and not layout_lines:
        return None

    pipeline_layers = dict(layout_meta.get('pipeline_layers') or {})
    generic_layer = dict(pipeline_layers.get('generic_form_understanding') or {})
    generic_layer.update(
        {
            'status': 'completed',
            'document_mode': document_mode,
            'scalar_count': len(scalars),
            'group_count': len(groups),
            'section_count': len(section_hierarchy),
        }
    )
    pipeline_layers['generic_form_understanding'] = generic_layer
    layout_meta['pipeline_layers'] = pipeline_layers

    return {
        'scalars': scalars,
        'groups': groups,
        'section_hierarchy': section_hierarchy,
        'layout_lines': layout_lines[:120],
        'layout_meta': {
            **layout_meta,
            'document_mode': document_mode,
        },
        'resolved_fields': [],
    }


def build_form_document_model(
    *,
    file_path: str | Path,
    file_type: str,
    raw_text: str,
    tables: list[dict[str, Any]] | None = None,
    kv_pairs: list[dict[str, Any]] | None = None,
    text_blocks: list[dict[str, Any]] | None = None,
    sections: list[dict[str, Any]] | None = None,
    layout_blocks: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    layout_layer = extract_layout_layer(
        file_path=file_path,
        file_type=file_type,
        raw_text=raw_text,
        tables=tables,
        kv_pairs=kv_pairs,
        text_blocks=text_blocks,
        sections=sections,
        layout_blocks=layout_blocks,
    )
    return understand_generic_form(
        layout_layer=layout_layer,
        tables=tables,
        kv_pairs=kv_pairs,
    )


def resolve_business_form_fields(
    *,
    form_model: dict[str, Any],
    target_fields: list[Any],
) -> list[dict[str, Any]]:
    target_names = [str(getattr(field, 'name', field) or '').strip() for field in target_fields]
    scalars = [dict(item) for item in form_model.get('scalars', []) if isinstance(item, dict)]
    groups = [dict(item) for item in form_model.get('groups', []) if isinstance(item, dict)]
    layout_lines = [dict(item) for item in form_model.get('layout_lines', []) if isinstance(item, dict)]

    tax_resolution_cache: dict[str, Any] | None = None
    results: list[dict[str, Any]] = []
    for target_name in target_names:
        lower_name = target_name.casefold()
        if lower_name == 'organizationname':
            results.append(_resolve_scalar_with_aliases(target_name, scalars, ORGANIZATION_LABEL_ALIASES))
            continue
        if lower_name == 'innorkio':
            results.append(_resolve_inn_or_kio(target_name, scalars))
            continue
        if lower_name == 'isresidentrf':
            tax_resolution_cache = tax_resolution_cache or _resolve_tax_residency_group(groups, layout_lines=layout_lines)
            results.append(
                {
                    'field': target_name,
                    'status': tax_resolution_cache['status'],
                    'resolved_by': tax_resolution_cache.get('resolved_by', 'form_resolver'),
                    'value': tax_resolution_cache.get('enum_value'),
                    'candidates': list(tax_resolution_cache.get('candidates', [])),
                    'source_ref': dict(tax_resolution_cache.get('source_ref', {})),
                    'confidence': tax_resolution_cache.get('confidence'),
                }
            )
            continue
        if lower_name == 'istaxresidencyonlyrf':
            tax_resolution_cache = tax_resolution_cache or _resolve_tax_residency_group(groups, layout_lines=layout_lines)
            derived_value = None
            if tax_resolution_cache.get('enum_value') == 'YES':
                derived_value = 'YES'
            elif tax_resolution_cache.get('enum_value') in {'NO', 'NOWHERE'}:
                derived_value = 'NO'
            results.append(
                {
                    'field': target_name,
                    'status': tax_resolution_cache['status'] if derived_value is not None else tax_resolution_cache['status'],
                    'resolved_by': tax_resolution_cache.get('resolved_by', 'form_resolver'),
                    'value': derived_value,
                    'candidates': list(tax_resolution_cache.get('candidates', [])),
                    'source_ref': dict(tax_resolution_cache.get('source_ref', {})),
                    'confidence': tax_resolution_cache.get('confidence'),
                }
            )
            continue
        if lower_name == 'fatcabeneficiaryoptionlist':
            results.append(_resolve_fatca_group(target_name, groups, layout_lines=layout_lines))
            continue

        results.append(_resolve_generic_scalar(target_name, scalars))

    return results


def resolve_form_fields(
    *,
    form_model: dict[str, Any],
    target_fields: list[Any],
) -> list[dict[str, Any]]:
    return resolve_business_form_fields(form_model=form_model, target_fields=target_fields)


def _extract_docx_layout_rows(doc) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    raw_table_rows: list[dict[str, Any]] = []
    layout_lines: list[dict[str, Any]] = []
    form_like_tables = 0

    for table_index, table in enumerate(doc.tables):
        raw_rows: list[list[str]] = []
        for row_index, row in enumerate(table.rows):
            cell_texts = [_clean_text(cell.text) for cell in row.cells]
            raw_rows.append(cell_texts)
            for cell_index, cell_text in enumerate(cell_texts):
                if not cell_text:
                    continue
                layout_lines.append(
                    {
                        'text': cell_text,
                        'table_idx': table_index,
                        'row_idx': row_index,
                        'cell_idx': cell_index,
                        'source_type': 'table_cell',
                        'tokens': [],
                    }
                )

        if _is_form_like_docx_table(raw_rows):
            form_like_tables += 1
            for row_index, row in enumerate(raw_rows):
                raw_table_rows.append(
                    {
                        'table_idx': table_index,
                        'row_idx': row_index,
                        'cells': list(row),
                    }
                )

    return raw_table_rows, layout_lines, {'form_like_table_count': form_like_tables}


def _extract_docx_form_understanding(raw_table_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    grouped_rows: dict[int, list[dict[str, Any]]] = {}
    for item in raw_table_rows:
        table_index = int(item.get('table_idx') or 0)
        grouped_rows.setdefault(table_index, []).append(item)

    scalars: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    for table_index, items in grouped_rows.items():
        ordered_rows = [
            list(item.get('cells', []))
            for item in sorted(items, key=lambda row: int(row.get('row_idx') or 0))
        ]
        table_scalars, table_groups = _extract_table_rows_as_form(ordered_rows, table_index=table_index)
        scalars.extend(table_scalars)
        groups.extend(table_groups)
    return scalars, groups


def _extract_table_rows_as_form(raw_rows: list[list[str]], *, table_index: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    scalars: list[dict[str, Any]] = []
    groups: list[dict[str, Any]] = []
    current_group: dict[str, Any] | None = None

    for row_index, row in enumerate(raw_rows):
        cells = [_clean_text(cell) for cell in row]
        non_empty = [cell for cell in cells if cell]
        if not non_empty:
            continue

        scalar_candidate = _extract_scalar_from_table_row(cells, table_index=table_index, row_index=row_index)
        if scalar_candidate is not None:
            if current_group is not None and current_group.get('options'):
                groups.append(current_group)
                current_group = None
            scalars.append(scalar_candidate)
            continue

        option_candidate = _extract_option_from_table_row(cells, table_index=table_index, row_index=row_index)
        if option_candidate is not None:
            if current_group is None:
                current_group = {
                    'group_id': f'group_{table_index}_{row_index}',
                    'question': 'Unknown question',
                    'group_type': 'unknown',
                    'options': [],
                    'source_ref': {'table_idx': table_index, 'row_idx': row_index, 'source_type': 'table_cell'},
                }
            current_group['options'].append(option_candidate)
            current_group['group_type'] = _infer_group_type(current_group)
            continue

        text = ' '.join(non_empty)
        if _looks_like_question_anchor(text):
            if current_group is not None and current_group.get('options'):
                groups.append(current_group)
            current_group = {
                'group_id': _guess_group_id(text, table_index=table_index, row_index=row_index),
                'question': text,
                'group_type': _guess_group_type_from_question(text),
                'options': [],
                'source_ref': {'table_idx': table_index, 'row_idx': row_index, 'source_type': 'table_cell'},
            }
            continue

        if current_group is not None and not current_group.get('options'):
            current_group['question'] = f"{current_group['question']} {text}".strip()

    if current_group is not None and current_group.get('options'):
        groups.append(current_group)

    return scalars, groups


def _build_text_layout_lines(
    *,
    raw_text: str,
    text_blocks: list[dict[str, Any]],
    layout_blocks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    lines: list[dict[str, Any]] = []
    if layout_blocks:
        for index, block in enumerate(layout_blocks):
            text = _clean_text(block.get('text'))
            if not text:
                continue
            tokens = []
            for token in block.get('tokens', []) or []:
                if not isinstance(token, dict):
                    continue
                tokens.append(
                    {
                        'text': _clean_text(token.get('text')),
                        'x': _safe_float(token.get('x')),
                        'y': _safe_float(token.get('y')),
                        'width': _safe_float(token.get('width')),
                        'height': _safe_float(token.get('height')),
                        'source_type': token.get('source_type') or 'line',
                    }
                )
            lines.append(
                {
                    'text': text,
                    'block_id': str(block.get('id') or f'layout-{index + 1}'),
                    'line_id': str(block.get('id') or f'layout-{index + 1}'),
                    'page': block.get('page'),
                    'column_id': block.get('column_id'),
                    'x': _safe_float(block.get('x')),
                    'y': _safe_float(block.get('y')),
                    'width': _safe_float(block.get('width')),
                    'height': _safe_float(block.get('height')),
                    'source_type': block.get('source_type') or 'line',
                    'tokens': tokens,
                }
            )
        return _merge_pdf_multiline_option_lines(_merge_pdf_marker_lines(lines))
    if text_blocks:
        for index, block in enumerate(text_blocks):
            text = _clean_text(block.get('text'))
            if not text:
                continue
            lines.append(
                {
                    'text': text,
                    'block_id': str(block.get('id') or f'block-{index + 1}'),
                    'column_id': None,
                    'source_type': 'paragraph',
                    'tokens': [],
                }
            )
        return lines

    for index, raw_line in enumerate(LINE_SPLIT_RE.split(raw_text or '')):
        text = _clean_text(raw_line)
        if not text:
            continue
        lines.append(
                {
                    'text': text,
                    'line_id': f'line-{index + 1}',
                    'column_id': None,
                    'source_type': 'line',
                    'tokens': [],
                }
            )
    return lines


def _merge_pdf_marker_lines(layout_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    consumed: set[int] = set()
    ordered = sorted(
        layout_lines,
        key=lambda item: (
            int(item.get('page') or 0),
            float(item.get('y') or 0.0),
            float(item.get('x') or 0.0),
        ),
    )
    for index, line in enumerate(ordered):
        if index in consumed:
            continue
        text = _clean_text(line.get('text'))
        if not _is_marker_cell(text):
            merged.append(line)
            continue
        best_candidate_index = None
        best_distance = None
        for candidate_index in range(index + 1, len(ordered)):
            candidate = ordered[candidate_index]
            if candidate_index in consumed:
                continue
            if candidate.get('page') != line.get('page'):
                continue
            candidate_text = _clean_text(candidate.get('text'))
            if not candidate_text or _is_marker_cell(candidate_text):
                continue
            y_distance = abs(float(candidate.get('y') or 0.0) - float(line.get('y') or 0.0))
            x_distance = float(candidate.get('x') or 0.0) - float(line.get('x') or 0.0)
            if y_distance > PDF_MERGE_Y_THRESHOLD or x_distance < 0 or x_distance > PDF_MERGE_X_THRESHOLD * 8:
                if y_distance > PDF_MERGE_Y_THRESHOLD:
                    break
                continue
            total_distance = y_distance + x_distance / 100.0
            if best_distance is None or total_distance < best_distance:
                best_distance = total_distance
                best_candidate_index = candidate_index
        if best_candidate_index is None:
            merged.append(line)
            continue
        consumed.add(best_candidate_index)
        candidate = ordered[best_candidate_index]
        merged_tokens = list(line.get('tokens', [])) + list(candidate.get('tokens', []))
        merged.append(
            {
                **candidate,
                'text': f'{text} {candidate.get("text")}'.strip(),
                'column_id': candidate.get('column_id') or line.get('column_id'),
                'x': min(
                    value
                    for value in [line.get('x'), candidate.get('x')]
                    if value is not None
                ),
                'width': _safe_span_width(line, candidate),
                'tokens': merged_tokens,
            }
        )
    return merged


def _merge_pdf_multiline_option_lines(layout_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    ordered = sorted(
        layout_lines,
        key=lambda item: (
            int(item.get('page') or 0),
            int(item.get('column_id') or 0),
            float(item.get('y') or 0.0),
            float(item.get('x') or 0.0),
        ),
    )

    index = 0
    while index < len(ordered):
        current = dict(ordered[index])
        current_text = _clean_text(current.get('text'))
        if not current_text:
            index += 1
            continue

        next_index = index + 1
        while next_index < len(ordered):
            candidate = ordered[next_index]
            if not _should_merge_wrapped_option_line(current, candidate):
                break
            current = {
                **current,
                'text': f'{_clean_text(current.get("text"))} {_clean_text(candidate.get("text"))}'.strip(),
                'width': _safe_span_width(current, candidate),
                'height': max(
                    float(current.get('height') or 0.0),
                    float(candidate.get('height') or 0.0),
                ) or None,
                'tokens': list(current.get('tokens', [])) + list(candidate.get('tokens', [])),
            }
            next_index += 1

        merged.append(current)
        index = next_index

    return merged


def _extract_groups_from_layout_lines(layout_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    current_group: dict[str, Any] | None = None

    ordered_lines = sorted(
        layout_lines,
        key=lambda item: (
            int(item.get('page') or 0),
            int(item.get('column_id') or 0),
            float(item.get('y') or 0.0),
            float(item.get('x') or 0.0),
        ),
    )

    for line in ordered_lines:
        text = _clean_text(line.get('text'))
        if not text:
            continue

        if current_group is not None and _line_breaks_group_scope(current_group, line):
            if current_group.get('options'):
                groups.append(current_group)
            current_group = None

        option = _extract_option_from_text_line(text, source_ref=line)
        if option is not None:
            if current_group is None:
                current_group = {
                    'group_id': _guess_group_id(text),
                    'question': 'Unknown question',
                    'group_type': 'unknown',
                    'options': [],
                    'source_ref': dict(line),
                }
            current_group['options'].append(option)
            current_group['group_type'] = _infer_group_type(current_group)
            continue

        if current_group is not None and current_group.get('options') and _is_option_continuation_line(current_group, line):
            last_option = current_group['options'][-1]
            last_option['label'] = f"{_clean_text(last_option.get('label'))} {text}".strip()
            last_option['source_ref'] = {
                **dict(last_option.get('source_ref', {})),
                'continued': True,
            }
            continue

        if _looks_like_question_anchor(text):
            if current_group is not None and current_group.get('options'):
                groups.append(current_group)
            current_group = {
                'group_id': _guess_group_id(text),
                'question': text,
                'group_type': _guess_group_type_from_question(text),
                'options': [],
                'source_ref': dict(line),
            }
            continue

        if current_group is not None and not current_group.get('options'):
            current_group['question'] = f"{current_group['question']} {text}".strip()

    if current_group is not None and current_group.get('options'):
        groups.append(current_group)

    return groups


def _extract_scalars_from_layout_lines(layout_lines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scalars: list[dict[str, Any]] = []
    for line in layout_lines:
        scalar = _parse_kv_line(line.get('text'), source_ref=line)
        if scalar is not None:
            scalars.append(scalar)
    return scalars


def _parse_kv_line(text: Any, *, source_ref: dict[str, Any]) -> dict[str, Any] | None:
    cleaned = _clean_text(text)
    if not cleaned:
        return None
    match = KV_RE.match(cleaned)
    if not match:
        return None
    label = _clean_text(match.group('label'))
    value = _clean_text(match.group('value'))
    if not label or not value:
        return None
    return {
        'label': label,
        'value': value,
        'source_ref': dict(source_ref),
        'confidence': 'high',
    }


def _extract_scalar_from_table_row(cells: list[str], *, table_index: int, row_index: int) -> dict[str, Any] | None:
    non_empty = [(idx, cell) for idx, cell in enumerate(cells) if cell]
    if len(non_empty) != 2:
        return None
    if any(_is_marker_cell(cell) for _, cell in non_empty):
        return None
    label_idx, value_idx = non_empty[0][0], non_empty[1][0]
    label, value = non_empty[0][1], non_empty[1][1]
    if len(_tokenize(label)) > 8 or len(value) > 180:
        return None
    return {
        'label': label,
        'value': value,
        'source_ref': {
            'table_idx': table_index,
            'row_idx': row_index,
            'label_cell_idx': label_idx,
            'value_cell_idx': value_idx,
            'source_type': 'table_cell',
        },
        'confidence': 'medium',
    }


def _extract_option_from_table_row(cells: list[str], *, table_index: int, row_index: int) -> dict[str, Any] | None:
    marker_index = next((index for index, cell in enumerate(cells) if _is_marker_cell(cell)), None)
    if marker_index is None:
        return None
    label_parts = [
        cell
        for index, cell in enumerate(cells)
        if index != marker_index and cell and not _is_marker_cell(cell)
    ]
    if not label_parts:
        return None
    label = ' '.join(label_parts).strip()
    if len(label) < 2:
        return None
    marker_text = cells[marker_index]
    return {
        'label': label,
        'selected': _is_checked_marker(marker_text),
        'marker_text': marker_text,
        'source_ref': {
            'table_idx': table_index,
            'row_idx': row_index,
            'marker_cell_idx': marker_index,
            'source_type': 'table_cell',
        },
    }


def _extract_option_from_text_line(text: str, *, source_ref: dict[str, Any]) -> dict[str, Any] | None:
    marker_match = re.match(r'^\s*([XxVv✓✔☒☑□☐○◯])\s*(.+)$', text)
    if marker_match:
        marker_text = marker_match.group(1)
        return {
            'label': _clean_text(marker_match.group(2)),
            'selected': _is_checked_marker(marker_text),
            'marker_text': marker_text,
            'source_ref': dict(source_ref),
        }
    bracket_match = re.match(r'^\s*\[([XxVv✓✔ ])\]\s*(.+)$', text)
    if bracket_match:
        marker_text = bracket_match.group(1)
        return {
            'label': _clean_text(bracket_match.group(2)),
            'selected': _is_checked_marker(marker_text),
            'marker_text': marker_text,
            'source_ref': dict(source_ref),
        }
    return None


def _resolve_scalar_with_aliases(field_name: str, scalars: list[dict[str, Any]], aliases: tuple[str, ...]) -> dict[str, Any]:
    best_match: dict[str, Any] | None = None
    best_score = 0.0
    near_matches: list[dict[str, Any]] = []
    alias_tokens = [_tokenize(alias) for alias in aliases]

    for scalar in scalars:
        label_tokens = _tokenize(scalar.get('label'))
        score = max((_token_overlap_score(label_tokens, tokens) for tokens in alias_tokens), default=0.0)
        if score >= 0.8 and score > best_score:
            best_match = scalar
            best_score = score
        elif score >= 0.6:
            near_matches.append(scalar)

    if best_match is not None:
        return {
            'field': field_name,
            'status': 'resolved' if best_score >= 0.85 else 'weak_match',
            'resolved_by': 'form_resolver',
            'value': best_match.get('value'),
            'candidates': [],
            'source_ref': dict(best_match.get('source_ref', {})),
            'confidence': round(best_score, 4),
        }
    if near_matches:
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [item.get('value') for item in near_matches if item.get('value') not in (None, '')],
            'source_ref': {},
            'confidence': round(max((_token_overlap_score(_tokenize(item.get('label')), alias_tokens[0]) for item in near_matches), default=0.0), 4),
        }
    return {
        'field': field_name,
        'status': 'not_found',
        'resolved_by': 'form_resolver',
        'value': None,
        'candidates': [],
        'source_ref': {},
        'confidence': None,
    }


def _resolve_inn_or_kio(field_name: str, scalars: list[dict[str, Any]]) -> dict[str, Any]:
    by_label = _resolve_scalar_with_aliases(field_name, scalars, INN_LABEL_ALIASES)
    if by_label['status'] in {'resolved', 'weak_match'} and by_label.get('value') not in (None, ''):
        return by_label

    pattern = re.compile(r'\b[0-9]{8,14}\b')
    candidates: list[str] = []
    source_ref: dict[str, Any] = {}
    for scalar in scalars:
        value = str(scalar.get('value') or '').strip()
        match = pattern.search(value)
        if not match:
            continue
        candidates.append(match.group(0))
        source_ref = dict(scalar.get('source_ref', {}))

    if len(candidates) == 1:
        return {
            'field': field_name,
            'status': 'weak_match',
            'resolved_by': 'form_resolver',
            'value': candidates[0],
            'candidates': [],
            'source_ref': source_ref,
            'confidence': 0.65,
        }
    if len(candidates) > 1:
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': candidates,
            'source_ref': source_ref,
            'confidence': 0.45,
        }
    return by_label


def _resolve_tax_residency_group(groups: list[dict[str, Any]], *, layout_lines: list[dict[str, Any]]) -> dict[str, Any]:
    group = _find_best_group(groups, TAX_GROUP_KEYWORDS)
    if group is None:
        return {
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    selected_options = [option for option in group.get('options', []) if option.get('selected')]
    if not selected_options:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='no_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [_map_option_enum(option.get('label'), TAX_RESIDENCY_ENUM_MAP) for option in group.get('options', [])],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.0,
        }
    if len(selected_options) > 1:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='multiple_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [option.get('label') for option in selected_options],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.35,
        }

    enum_value = _map_option_enum(selected_options[0].get('label'), TAX_RESIDENCY_ENUM_MAP)
    if enum_value is None:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='unmapped_selected', allow_multiple=False):
            repaired = _repair_group_resolution(
                target_field='isResidentRF',
                group=group,
                enum_map=TAX_RESIDENCY_ENUM_MAP,
                layout_lines=layout_lines,
                allow_multiple=False,
            )
            if repaired is not None:
                return repaired
        return {
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'enum_value': None,
            'candidates': [selected_options[0].get('label')],
            'source_ref': dict(selected_options[0].get('source_ref', {})),
            'confidence': 0.35,
        }
    return {
        'status': 'resolved',
        'resolved_by': 'form_resolver',
        'enum_value': enum_value,
        'candidates': [],
        'source_ref': dict(selected_options[0].get('source_ref', {})),
        'confidence': 0.92,
    }


def _resolve_fatca_group(field_name: str, groups: list[dict[str, Any]], *, layout_lines: list[dict[str, Any]]) -> dict[str, Any]:
    group = _find_best_group(groups, FATCA_GROUP_KEYWORDS)
    if group is None:
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    selected_options = [option for option in group.get('options', []) if option.get('selected')]
    if not selected_options:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='no_selected', allow_multiple=True):
            repaired = _repair_group_resolution(
                target_field=field_name,
                group=group,
                enum_map=FATCA_OPTION_MAP,
                layout_lines=layout_lines,
                allow_multiple=True,
            )
            if repaired is not None:
                return {
                    'field': field_name,
                    'status': repaired['status'],
                    'resolved_by': repaired['resolved_by'],
                    'value': repaired['enum_values'],
                    'candidates': [],
                    'source_ref': repaired['source_ref'],
                    'confidence': repaired['confidence'],
                }
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': [],
            'candidates': [],
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.0,
        }

    mapped_values: list[str] = []
    unresolved_labels: list[str] = []
    for option in selected_options:
        mapped = _map_option_enum(option.get('label'), FATCA_OPTION_MAP)
        if mapped is None:
            unresolved_labels.append(str(option.get('label') or ''))
            continue
        if mapped not in mapped_values:
            mapped_values.append(mapped)

    if unresolved_labels:
        if _should_attempt_group_repair(group=group, layout_lines=layout_lines, issue='unmapped_selected', allow_multiple=True):
            repaired = _repair_group_resolution(
                target_field=field_name,
                group=group,
                enum_map=FATCA_OPTION_MAP,
                layout_lines=layout_lines,
                allow_multiple=True,
            )
            if repaired is not None:
                return {
                    'field': field_name,
                    'status': repaired['status'],
                    'resolved_by': repaired['resolved_by'],
                    'value': repaired['enum_values'],
                    'candidates': [],
                    'source_ref': repaired['source_ref'],
                    'confidence': repaired['confidence'],
                }
        return {
            'field': field_name,
            'status': 'ambiguous',
            'resolved_by': 'form_resolver',
            'value': mapped_values or None,
            'candidates': unresolved_labels,
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': 0.45,
        }

    return {
        'field': field_name,
        'status': 'resolved',
        'resolved_by': 'form_resolver',
        'value': mapped_values,
        'candidates': [],
        'source_ref': dict(group.get('source_ref', {})),
        'confidence': 0.9,
    }


def _repair_group_resolution(
    *,
    target_field: str,
    group: dict[str, Any],
    enum_map: dict[str, str],
    layout_lines: list[dict[str, Any]],
    allow_multiple: bool,
) -> dict[str, Any] | None:
    context_lines = _collect_group_context_lines(group, layout_lines)
    repaired, warnings = suggest_form_field_repair(
        target_field=target_field,
        question=str(group.get('question') or ''),
        options=[dict(option) for option in group.get('options', []) if isinstance(option, dict)],
        enum_map=enum_map,
        context_lines=context_lines,
        allow_multiple=allow_multiple,
    )
    if warnings:
        group.setdefault('repair_warnings', []).extend(warnings)
    if not isinstance(repaired, dict):
        return None

    if allow_multiple:
        enum_values = [str(value) for value in repaired.get('enum_values', []) if str(value).strip()]
        if not enum_values:
            return None
        return {
            'status': 'resolved',
            'resolved_by': 'repair_model',
            'enum_values': enum_values,
            'source_ref': dict(group.get('source_ref', {})),
            'confidence': float(repaired.get('confidence') or 0.6),
        }

    enum_value = repaired.get('enum_value')
    if enum_value in (None, ''):
        return None
    return {
        'status': 'resolved',
        'resolved_by': 'repair_model',
        'enum_value': str(enum_value),
        'candidates': [],
        'source_ref': dict(group.get('source_ref', {})),
        'confidence': float(repaired.get('confidence') or 0.6),
    }


def _resolve_generic_scalar(field_name: str, scalars: list[dict[str, Any]]) -> dict[str, Any]:
    target_tokens = _tokenize(field_name)
    best_match: dict[str, Any] | None = None
    best_score = 0.0

    for scalar in scalars:
        label_tokens = _tokenize(scalar.get('label'))
        score = _token_overlap_score(target_tokens, label_tokens)
        if score > best_score:
            best_score = score
            best_match = scalar

    if best_match is None or best_score < 0.55:
        return {
            'field': field_name,
            'status': 'not_found',
            'resolved_by': 'form_resolver',
            'value': None,
            'candidates': [],
            'source_ref': {},
            'confidence': None,
        }

    return {
        'field': field_name,
        'status': 'weak_match' if best_score < 0.82 else 'resolved',
        'resolved_by': 'form_resolver',
        'value': best_match.get('value'),
        'candidates': [],
        'source_ref': dict(best_match.get('source_ref', {})),
        'confidence': round(best_score, 4),
    }


def _find_best_group(groups: list[dict[str, Any]], keywords: tuple[str, ...]) -> dict[str, Any] | None:
    best_group: dict[str, Any] | None = None
    best_score = 0.0
    for group in groups:
        group_id = str(group.get('group_id') or '').strip().lower()
        if group_id == 'tax_residency' and keywords == TAX_GROUP_KEYWORDS:
            return group
        if 'fatca' in group_id and keywords == FATCA_GROUP_KEYWORDS:
            return group
        question_tokens = _tokenize(group.get('question'))
        score = max((_phrase_similarity(question_tokens, _tokenize(keyword)) for keyword in keywords), default=0.0)
        if score > best_score:
            best_score = score
            best_group = group
    if best_group is None or best_score < 0.4:
        return None
    return best_group


def _collect_group_context_lines(group: dict[str, Any], layout_lines: list[dict[str, Any]]) -> list[str]:
    question = _clean_text(group.get('question'))
    option_labels = [_clean_text(option.get('label')) for option in group.get('options', []) if option.get('label')]
    source_ref = dict(group.get('source_ref', {}))
    source_page = source_ref.get('page')
    source_column = source_ref.get('column_id')
    source_y = _safe_float(source_ref.get('y'))
    context: list[str] = []
    for line in layout_lines:
        text = _clean_text(line.get('text'))
        if not text:
            continue
        if source_page is not None and line.get('page') != source_page:
            continue
        if source_column is not None and line.get('column_id') not in {None, source_column}:
            continue
        line_y = _safe_float(line.get('y'))
        if source_y is not None and line_y is not None and abs(line_y - source_y) > 120:
            continue
        if question and question in text:
            context.append(text)
            continue
        if any(label and label in text for label in option_labels):
            context.append(text)
            continue
    if not context:
        if question:
            context.append(question)
        context.extend(option_labels[:6])
    return context[:12]


def _should_attempt_group_repair(
    *,
    group: dict[str, Any],
    layout_lines: list[dict[str, Any]],
    issue: str,
    allow_multiple: bool,
) -> bool:
    del allow_multiple

    options = [option for option in group.get('options', []) if isinstance(option, dict)]
    if len(options) < 2 or len(options) > 8:
        return False

    context_lines = _collect_group_context_lines(group, layout_lines)
    has_wrapped_lines = len(context_lines) > max(len(options) + 2, 5)
    has_long_option = any(len(_tokenize(option.get('label'))) >= 6 for option in options)
    has_unknown_group_type = str(group.get('group_type') or 'unknown') == 'unknown'
    has_marker_variance = any(
        str(option.get('marker_text') or '').strip() not in {'', 'X', 'x', 'V', 'v', '✓', '✔', '☒', '☑'}
        for option in options
    )
    complex_group = has_wrapped_lines or has_long_option or has_unknown_group_type or has_marker_variance

    if issue in {'multiple_selected', 'unmapped_selected'}:
        return complex_group
    if issue == 'no_selected':
        return complex_group and any(_contains_marker(line) for line in context_lines)
    return False


def _map_option_enum(label: Any, enum_map: dict[str, str]) -> str | None:
    label_tokens = _tokenize(label)
    best_value = None
    best_score = 0.0
    for human_label, enum_value in enum_map.items():
        score = _phrase_similarity(label_tokens, _tokenize(human_label))
        if score > best_score:
            best_score = score
            best_value = enum_value
    if best_score < 0.45:
        return None
    return best_value


def _looks_like_form_layout(
    *,
    kv_pairs: list[dict[str, Any]],
    groups: list[dict[str, Any]],
    tables: list[dict[str, Any]],
    layout_lines: list[dict[str, Any]],
) -> bool:
    if groups:
        return True
    if len(kv_pairs) >= 2:
        return True
    if any(_contains_marker(line.get('text')) for line in layout_lines):
        return True
    if len(tables) <= 2 and layout_lines and sum(1 for line in layout_lines if len(_tokenize(line.get('text'))) <= 6) >= 4:
        return True
    return False


def _should_merge_wrapped_option_line(current: dict[str, Any], candidate: dict[str, Any]) -> bool:
    current_text = _clean_text(current.get('text'))
    candidate_text = _clean_text(candidate.get('text'))
    if not current_text or not candidate_text:
        return False
    if current.get('page') != candidate.get('page'):
        return False

    current_column = current.get('column_id')
    candidate_column = candidate.get('column_id')
    if current_column is not None and candidate_column is not None and current_column != candidate_column:
        return False

    if _extract_option_from_text_line(candidate_text, source_ref=candidate) is not None:
        return False
    if _looks_like_question_anchor(candidate_text) or _parse_kv_line(candidate_text, source_ref=candidate) is not None:
        return False
    if _extract_option_from_text_line(current_text, source_ref=current) is None:
        return False

    current_y = _safe_float(current.get('y'))
    candidate_y = _safe_float(candidate.get('y'))
    if current_y is not None and candidate_y is not None and candidate_y - current_y > PDF_WRAP_Y_THRESHOLD:
        return False

    current_x = _safe_float(current.get('x'))
    candidate_x = _safe_float(candidate.get('x'))
    if current_x is not None and candidate_x is not None and abs(candidate_x - current_x) > PDF_WRAP_X_THRESHOLD:
        return False

    return True


def _line_breaks_group_scope(group: dict[str, Any], line: dict[str, Any]) -> bool:
    source_ref = dict(group.get('source_ref', {}))
    source_page = source_ref.get('page')
    line_page = line.get('page')
    if source_page is not None and line_page is not None and source_page != line_page:
        return True

    source_column = source_ref.get('column_id')
    line_column = line.get('column_id')
    if source_column is not None and line_column is not None and source_column != line_column:
        return True

    return False


def _is_option_continuation_line(group: dict[str, Any], line: dict[str, Any]) -> bool:
    if not group.get('options'):
        return False
    text = _clean_text(line.get('text'))
    if not text:
        return False
    if _looks_like_question_anchor(text):
        return False
    if _parse_kv_line(text, source_ref=line) is not None:
        return False

    last_option = group['options'][-1]
    last_source = dict(last_option.get('source_ref', {}))
    if last_source.get('page') is not None and line.get('page') not in {None, last_source.get('page')}:
        return False
    if last_source.get('column_id') is not None and line.get('column_id') not in {None, last_source.get('column_id')}:
        return False

    last_y = _safe_float(last_source.get('y'))
    line_y = _safe_float(line.get('y'))
    if last_y is not None and line_y is not None and abs(line_y - last_y) > PDF_WRAP_Y_THRESHOLD:
        return False

    last_x = _safe_float(last_source.get('x'))
    line_x = _safe_float(line.get('x'))
    if last_x is not None and line_x is not None and abs(line_x - last_x) > PDF_WRAP_X_THRESHOLD:
        return False

    return True


def _is_form_like_docx_table(raw_rows: list[list[str]]) -> bool:
    if not raw_rows:
        return False
    marker_count = sum(1 for row in raw_rows for cell in row if _is_marker_cell(cell))
    empty_count = sum(1 for row in raw_rows for cell in row if not _clean_text(cell))
    total_count = sum(len(row) for row in raw_rows)
    short_text_rows = sum(1 for row in raw_rows if 0 < len([cell for cell in row if _clean_text(cell)]) <= 3)
    if marker_count > 0:
        return True
    if total_count and empty_count / total_count >= 0.3 and short_text_rows >= 2:
        return True
    return False


def _looks_like_question_anchor(text: str) -> bool:
    normalized = _normalize_phrase(text)
    if any(keyword in normalized for keyword in FATCA_GROUP_KEYWORDS):
        return True
    if 'налогов' in normalized and 'резидент' in normalized:
        return True
    return len(_tokenize(text)) >= 5 and len(text) >= 24


def _guess_group_id(text: str, *, table_index: int | None = None, row_index: int | None = None) -> str:
    normalized = _normalize_phrase(text)
    if 'fatca' in normalized:
        return 'fatca_beneficiary'
    if 'налогов' in normalized and 'резидент' in normalized:
        return 'tax_residency'
    suffix = 'group'
    if table_index is not None and row_index is not None:
        suffix = f'group_{table_index}_{row_index}'
    return suffix


def _guess_group_type_from_question(text: str) -> str:
    normalized = _normalize_phrase(text)
    if 'fatca' in normalized:
        return 'multi_choice'
    if any(word in normalized for word in ('один', 'single', 'только')):
        return 'single_choice'
    return 'unknown'


def _infer_group_type(group: dict[str, Any]) -> str:
    guessed = _guess_group_type_from_question(str(group.get('question') or ''))
    if guessed != 'unknown':
        return guessed
    selected_count = sum(1 for option in group.get('options', []) if option.get('selected'))
    if selected_count > 1:
        return 'multi_choice'
    return 'single_choice'


def _build_section_hierarchy(
    sections: list[dict[str, Any]],
    groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    hierarchy: list[dict[str, Any]] = []
    for index, section in enumerate(sections, start=1):
        title = _clean_text(section.get('title') or f'Section {index}')
        text = _clean_text(section.get('text'))
        if not title and not text:
            continue
        normalized_text = _normalize_phrase(text)
        related_groups = [
            str(group.get('group_id') or '')
            for group in groups
            if _normalize_phrase(str(group.get('question') or '')) and _normalize_phrase(str(group.get('question') or '')) in normalized_text
        ]
        hierarchy.append(
            {
                'section_id': f'section_{index}',
                'title': title,
                'level': 1,
                'related_group_ids': related_groups,
            }
        )
    if not hierarchy and groups:
        hierarchy.append(
            {
                'section_id': 'section_form_root',
                'title': 'Form Body',
                'level': 1,
                'related_group_ids': [str(group.get('group_id') or '') for group in groups],
            }
        )
    return hierarchy


def _dedupe_scalars(scalars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for scalar in scalars:
        label = str(scalar.get('label') or '').strip()
        value = str(scalar.get('value') or '').strip()
        key = (_normalize_phrase(label), value.casefold())
        if not label or not value or key in seen:
            continue
        seen.add(key)
        result.append(scalar)
    return result


def _dedupe_groups(groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    for group in groups:
        question = str(group.get('question') or '').strip()
        options = tuple(_normalize_phrase(option.get('label')) for option in group.get('options', []))
        key = (_normalize_phrase(question), options)
        if not question or not options or key in seen:
            continue
        seen.add(key)
        result.append(group)
    return result


def _clean_text(value: Any) -> str:
    return SPACE_RE.sub(' ', str(value or '').replace('\xa0', ' ')).strip()


def _normalize_phrase(value: Any) -> str:
    return ' '.join(_tokenize(value))


def _tokenize(value: Any) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(str(value or '')) if token]


def _token_overlap_score(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_set = set(left)
    right_set = set(right)
    return len(left_set & right_set) / max(len(left_set), len(right_set), 1)


def _token_stem_overlap_score(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_stems = {token[:5] for token in left if token}
    right_stems = {token[:5] for token in right if token}
    if not left_stems or not right_stems:
        return 0.0
    return len(left_stems & right_stems) / max(len(left_stems), len(right_stems), 1)


def _phrase_similarity(left: list[str], right: list[str]) -> float:
    if not left or not right:
        return 0.0
    left_text = ' '.join(left)
    right_text = ' '.join(right)
    if left_text == right_text:
        return 1.0
    overlap = _token_overlap_score(left, right)
    stem_overlap = _token_stem_overlap_score(left, right)
    contains_bonus = 0.15 if left_text in right_text or right_text in left_text else 0.0
    return min(max(overlap, stem_overlap) + contains_bonus, 1.0)


def _contains_marker(value: Any) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    if text in CHECK_MARKERS or text in UNCHECKED_MARKERS:
        return True
    return bool(re.search(r'\[[XxVv✓✔ ]\]', text))


def _is_marker_cell(value: Any) -> bool:
    text = _clean_text(value)
    return text in CHECK_MARKERS or text in UNCHECKED_MARKERS


def _is_checked_marker(value: Any) -> bool:
    return _clean_text(value) in CHECK_MARKERS


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_span_width(left: dict[str, Any], right: dict[str, Any]) -> float | None:
    left_x = _safe_float(left.get('x'))
    right_x = _safe_float(right.get('x'))
    left_width = _safe_float(left.get('width'))
    right_width = _safe_float(right.get('width'))
    if left_x is None and right_x is None:
        return None
    min_x = min(value for value in (left_x, right_x) if value is not None)
    max_x = max(
        value
        for value in (
            None if left_x is None or left_width is None else left_x + left_width,
            None if right_x is None or right_width is None else right_x + right_width,
        )
        if value is not None
    )
    return max(max_x - min_x, 0.0)
