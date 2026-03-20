# TSGen CLI

`cli.py` is a Typer-based command line interface over the backend core.

## Commands

### Generate
```bash
python cli.py generate --input example.csv --schema example_target.json --out parser.ts
```

Options:
- `--guest`
- `--save-history / --no-save-history`
- `--show-preview`
- `--show-mapping`

### Preview
```bash
python cli.py preview --input example.csv
```

### Explain
```bash
python cli.py explain --input example.csv --schema example_target.json
```

### History
```bash
python cli.py history --limit 10
```

### Show
```bash
python cli.py show --id <entry-id>
```

### Cleanup
```bash
python cli.py cleanup --ttl-hours 24
```

## Expected backend module functions

The CLI tries to resolve one of several candidate functions from each module.

### parsers.py
Expected one of:
- `parse_input_file(path)` / `parse_file(path)` / `parse_document(path)`
- `parse_target_json(path)` / `parse_target_schema(path)` / `parse_schema(path)`

### matcher.py
Expected one of:
- `build_mapping(parsed_file, target_schema)`
- `match_fields(parsed_file, target_schema)`
- `match_schema(parsed_file, target_schema)`

### generator.py
Expected one of:
- `generate_typescript(parsed_file, target_schema, mapping)`
- `generate_parser(parsed_file, target_schema, mapping)`
- `generate_code(parsed_file, target_schema, mapping)`

### storage.py
Expected one of:
- `save_generation(record)` / `save_history(record)`
- `get_history(limit=..., include_guest=...)`
- `get_generation_by_id(entry_id)`
- `cleanup_guest_files(ttl_hours=..., dry_run=...)`

## Notes

- The CLI is intentionally defensive and can work with dicts, pydantic models, or simple objects.
- If your backend uses different function names, either add aliases in backend modules or extend candidate lists in `cli.py`.
