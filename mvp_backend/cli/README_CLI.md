# TSGen CLI

`cli.py` is a small `argparse`-based command line wrapper over the backend core.

It does not call HTTP endpoints. It imports backend modules directly:
- `parsers.py`
- `matcher.py`
- `generator.py`
- `storage.py`

## Quick start

Run commands from the backend directory:

```bash
cd mvp_backend
python cli/cli.py --help
```

## Commands

### Generate

Generate TypeScript from an input file and target JSON schema.

```bash
python cli/cli.py generate \
  --input example.csv \
  --schema example_target.json \
  --out parser.ts
```

Options:
- `--input, -i`: input file path
- `--schema, -s`: target JSON file path
- `--out, -o`: output TypeScript file path, default `parser.ts`
- `--user-id`: history owner id, default `cli-user`
- `--guest`: do not save generation to history
- `--show-preview`: print preview JSON
- `--show-mapping`: print field mapping

Notes:
- without `--guest`, the command initializes SQLite runtime storage and saves the result to history
- for `csv/xlsx/xls`, preview is built from parsed rows
- for text `pdf/docx`, parsing still goes through backend parser logic

### Preview

Show parsed preview for an input file.

```bash
python cli/cli.py preview --input example.csv --rows 5
```

Options:
- `--input, -i`: input file path
- `--rows, -r`: number of rows to print, default `5`

### Explain

Show how source fields map to the target schema.

```bash
python cli/cli.py explain --input example.csv --schema example_target.json
```

Options:
- `--input, -i`: input file path
- `--schema, -s`: target JSON file path

### History

Show saved history for one user.

```bash
python cli/cli.py history --user-id cli-user --limit 10
```

Options:
- `--user-id`: user external id, default `cli-user`
- `--limit, -n`: max entries to show, default `20`
- `--full`: print full JSON payload for each entry

### Show

Show one history entry by id.

```bash
python cli/cli.py show --id 1
```

### Cleanup

Clean up expired guest files from runtime storage.

```bash
python cli/cli.py cleanup --ttl-hours 24
python cli/cli.py cleanup --ttl-hours 24 --dry-run
```

Options:
- `--ttl-hours`: guest file TTL in hours
- `--dry-run`: print what would be removed without deleting anything

## Example workflow

```bash
cd mvp_backend
python cli/cli.py preview --input example.csv
python cli/cli.py explain --input example.csv --schema example_target.json
python cli/cli.py generate --input example.csv --schema example_target.json --out parser.ts --show-mapping
python cli/cli.py history --user-id cli-user
```

## Current limitations

- CLI does not expose every web/backend feature
- auth, email verification, profile editing and password reset are web/backend flows, not CLI flows
- CLI currently focuses on parsing, mapping, generation and local history inspection
