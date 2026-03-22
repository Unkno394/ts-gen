# TSGen CLI

`cli.py` — это локальная CLI-обёртка над backend core pipeline.

CLI не ходит в HTTP API, но теперь использует тот же generation flow, что и backend `/api/generate`:

- `parse_file(...)`
- `parse_target_schema(...)`
- `resolve_generation_source(...)`
- `resolve_generation_mappings_detailed(...)`
- `generate_typescript(...)`
- `build_preview(...)`
- `compile_typescript_code(...)`
- `validate_preview_against_target_schema(...)`

То есть `generate` и `explain` теперь ближе к реальному поведению фронта.

## Важно

CLI нужно запускать из Python-окружения backend, где установлены зависимости из [mvp_backend/requirements.txt](/home/user/Desktop/tsgen-desktop/mvp_backend/requirements.txt).

Пример:

```bash
cd /home/user/Desktop/tsgen-desktop/mvp_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python cli/cli.py --help
```

## Команды

### 1. `auth-send-code`

Отправляет код регистрации на email.

```bash
python cli/cli.py auth-send-code --email user@example.com
```

### 2. `auth-register`

Регистрирует пользователя и сохраняет CLI-сессию в `.runtime/cli_session.json`.

```bash
python cli/cli.py auth-register \
  --email user@example.com \
  --password supersecret123 \
  --verification-code 123456 \
  --name "Иван"
```

### 3. `auth-login`

Логинит пользователя и сохраняет CLI-сессию.

```bash
python cli/cli.py auth-login --email user@example.com --password supersecret123
```

### 4. `auth-logout`

Удаляет локальную CLI-сессию.

```bash
python cli/cli.py auth-logout
```

### 5. `auth-profile`

Показывает профиль текущего пользователя из CLI-сессии.

```bash
python cli/cli.py auth-profile
```

### 6. `auth-update-profile`

Обновляет имя пользователя.

```bash
python cli/cli.py auth-update-profile --name "Новое имя"
```

### 7. `auth-change-password`

Меняет пароль текущего пользователя.

```bash
python cli/cli.py auth-change-password \
  --current-password oldpass123 \
  --new-password newpass123
```

### 8. `auth-send-email-change-code`

Отправляет код для смены email.

```bash
python cli/cli.py auth-send-email-change-code --new-email new@example.com
```

### 9. `auth-change-email`

Меняет email по текущему паролю или по коду из письма.

```bash
python cli/cli.py auth-change-email --new-email new@example.com --current-password supersecret123
```

или

```bash
python cli/cli.py auth-change-email --new-email new@example.com --verification-code 123456
```

### 10. `auth-send-reset-code`

Отправляет код для сброса пароля.

```bash
python cli/cli.py auth-send-reset-code --email user@example.com
```

### 11. `auth-verify-reset-code`

Проверяет код сброса и возвращает `reset_token`.

```bash
python cli/cli.py auth-verify-reset-code --email user@example.com --verification-code 123456
```

### 12. `auth-reset-password`

Сбрасывает пароль по коду или `reset_token`.

```bash
python cli/cli.py auth-reset-password \
  --email user@example.com \
  --verification-code 123456 \
  --password newpass123
```

### 13. `generate`

Генерирует `TypeScript` по входному файлу и target JSON.

```bash
python cli/cli.py generate \
  --input example.csv \
  --schema example_target.json \
  --out parser.ts
```

Опции:

- `--input, -i` — путь к входному файлу
- `--schema, -s` — путь к target JSON
- `--out, -o` — куда сохранить `.ts`, по умолчанию `parser.ts`
- `--sheet` — выбранный лист/таблица
- `--user-id` — идентификатор пользователя для history, по умолчанию `cli-user`
- `--guest` — не сохранять результат в history
- `--show-preview` — вывести `Preview JSON`
- `--show-mapping` — вывести найденные mappings
- `--show-validation` — вывести `TS validation` и `Preview validation`
- `--show-quality` — вывести `quality_summary` и `mapping_stats`

Что делает:

- использует тот же mapping pipeline, что и backend
- строит preview
- проверяет `TypeScript`
- проверяет preview против target schema
- при необходимости сохраняет результат в history

### 14. `source-preview`

Показывает resolved source preview максимально близко к backend `source-preview`.

```bash
python cli/cli.py source-preview --input example.csv
```

Опции:

- `--input, -i` — путь к входному файлу
- `--schema, -s` — optional target JSON
- `--sheet` — выбранный лист/таблица

Что выводит:

- тип источника
- document mode
- extraction status
- итоговый `parsed_file`
- warnings

### 15. `draft-json`

Строит `draft JSON` и список field suggestions.

```bash
python cli/cli.py draft-json --input example.csv --show-suggestions
```

Опции:

- `--input, -i`
- `--sheet`
- `--user-id`
- `--show-suggestions`

### 16. `preview`

Показывает resolved source preview.

```bash
python cli/cli.py preview --input example.csv --rows 5
```

Опции:

- `--input, -i` — путь к входному файлу
- `--rows, -r` — сколько строк показать
- `--sheet` — выбранный лист/таблица
- `--schema, -s` — optional target JSON; если передан, preview строится с учётом target fields

Это ближе к backend `source preview`, чем старое простое чтение таблицы.

### 17. `explain`

Показывает mappings и explainability mapping pipeline.

```bash
python cli/cli.py explain \
  --input example.csv \
  --schema example_target.json
```

Опции:

- `--input, -i` — путь к входному файлу
- `--schema, -s` — путь к target JSON
- `--sheet` — выбранный лист/таблица
- `--user-id` — контекст пользователя для personal/global memory

Что выводит:

- итоговые mappings
- `mapping_stats`
- `mapping_sources`
- `suggestions`
- `unresolved_fields`
- warnings

### 18. `repair-plan`

Показывает `form explainability` и список repair actions для form-like документа.

```bash
python cli/cli.py repair-plan \
  --input form.docx \
  --schema target.json
```

### 19. `repair-preview`

Показывает preview для одного repair action.

```bash
python cli/cli.py repair-preview \
  --input form.docx \
  --schema target.json \
  --action-index 0
```

### 20. `repair-apply`

Применяет один repair action.

Если `--patch-file` не передан, CLI попробует взять `proposed_patch` автоматически из repair preview.

```bash
python cli/cli.py repair-apply \
  --input form.docx \
  --schema target.json \
  --action-index 0
```

Опционально можно сохранить результат в history:

```bash
python cli/cli.py repair-apply \
  --input form.docx \
  --schema target.json \
  --action-index 0 \
  --persist \
  --generation-id 42 \
  --user-id cli-user
```

### 21. `history`

Показывает сохранённую историю.

```bash
python cli/cli.py history --user-id cli-user --limit 10
```

Опции:

- `--user-id`
- `--limit, -n`
- `--full`

### 22. `show`

Показывает одну запись history по id.

```bash
python cli/cli.py show --id 1
```

### 23. `cleanup`

Очищает просроченные guest-файлы.

```bash
python cli/cli.py cleanup --ttl-hours 24
python cli/cli.py cleanup --ttl-hours 24 --dry-run
```

## Пример сценария

```bash
cd /home/user/Desktop/tsgen-desktop/mvp_backend
source .venv/bin/activate

python cli/cli.py auth-login --email user@example.com --password supersecret123
python cli/cli.py auth-profile
python cli/cli.py source-preview --input example.csv
python cli/cli.py draft-json --input example.csv --show-suggestions
python cli/cli.py preview --input example.csv --rows 5
python cli/cli.py explain --input example.csv --schema example_target.json
python cli/cli.py generate --input example.csv --schema example_target.json --out parser.ts --show-mapping --show-preview --show-validation --show-quality
python cli/cli.py history --user-id cli-user
```

## Что всё ещё не покрыто CLI

CLI всё ещё не повторяет весь веб-интерфейс полностью.

В CLI сейчас нет:

- full review UI
- фронтовых explainability-виджетов

Но core generation path теперь уже общий с backend.
