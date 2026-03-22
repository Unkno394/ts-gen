# TSGen

![Electron](https://img.shields.io/badge/Electron-Desktop-47848F?logo=electron&logoColor=white)
![React](https://img.shields.io/badge/React-Frontend-61DAFB?logo=react&logoColor=0B1020)
![Python](https://img.shields.io/badge/Python-Backend-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

TSGen — это система, которая принимает исходный файл, целевой `JSON` и генерирует `TypeScript`-трансформер:

- источник: `CSV`, `XLSX/XLS`, `PDF`, `DOCX`, `TXT`, изображения
- результат: `TypeScript`-код + `Preview JSON`
- дополнительно: preview источника, ручные `mapping overrides`, история генераций, explainability и проверка качества

## Что умеет проект

- загружать табличные и документные источники
- извлекать структуру из `PDF/DOCX/TXT`
- для изображений использовать отдельный `LlamaParse`-сервис
- строить preview источника до генерации
- принимать целевой `JSON`:
  - файлом
  - drag-and-drop
  - вставкой вручную в текстовое поле
- генерировать `TypeScript`
- показывать `Preview JSON`
- позволять вручную править соответствия `source -> target`
- хранить историю генераций и пользовательские подтверждения

## Архитектура

Проект состоит из трёх частей:

### 1. Frontend

Путь: [front](/home/user/Desktop/tsgen-desktop/front)

Стек:
- `React`
- `TypeScript`
- `Vite`
- опционально `Electron`

Основной экран находится в [Workspace.tsx](/home/user/Desktop/tsgen-desktop/front/src/components/Workspace.tsx).

Что делает фронт:
- загрузка источника
- загрузка или вставка target JSON
- показ source preview
- запуск генерации
- показ generated TypeScript
- показ preview результата
- ручная правка mappings
- работа с историей и профилем

### 2. Backend

Путь: [mvp_backend](/home/user/Desktop/tsgen-desktop/mvp_backend)

Стек:
- `FastAPI`
- `Pydantic v1`
- `pdfplumber`
- `python-docx`
- `openpyxl`

Точка входа: [app.py](/home/user/Desktop/tsgen-desktop/mvp_backend/app.py)  
Основные роуты: [routes.py](/home/user/Desktop/tsgen-desktop/mvp_backend/routes.py)

Основные задачи backend:
- принять файл
- определить тип источника
- распарсить таблицу или документ
- построить candidate mappings
- сгенерировать `TypeScript`
- собрать `Preview JSON`
- сохранить артефакты генерации в историю

Важные файлы:
- [parsers.py](/home/user/Desktop/tsgen-desktop/mvp_backend/parsers.py) — общий вход в parsing
- [document_parser.py](/home/user/Desktop/tsgen-desktop/mvp_backend/parser/document_parser.py) — document-like parsing
- [generator.py](/home/user/Desktop/tsgen-desktop/mvp_backend/generator.py) — генерация `TypeScript` и preview
- [validation.py](/home/user/Desktop/tsgen-desktop/mvp_backend/validation.py) — валидация TS и preview/schema
- [storage.py](/home/user/Desktop/tsgen-desktop/mvp_backend/storage.py) — история, persistence, auth-данные

### 3. Отдельный `LlamaParse`-сервис

Путь: [llamaparse_service](/home/user/Desktop/tsgen-desktop/llamaparse_service)

Это отдельный HTTP-сервис, чтобы не тащить `llama-parse` и его зависимости в основной backend.

Используется в первую очередь для image-like файлов.

## Текущий pipeline

### Табличные файлы

Для `CSV/XLSX/XLS`:

1. backend читает файл
2. строит `columns/rows`
3. показывает source preview
4. подбирает mappings
5. генерирует `TypeScript`
6. строит `Preview JSON`

### Документные файлы

Для `PDF/DOCX/TXT`:

1. backend парсит текст, секции, блоки и candidate fields
2. пытается понять, это таблица, форма или текстовый документ
3. строит source preview
4. подбирает mappings и генерирует результат

### Изображения

Для `PNG/JPG/...`:

1. backend отправляет файл в `llamaparse_service`
2. если `LlamaParse` вернул usable text, используется он
3. если из ответа удаётся восстановить markdown-таблицу, она поднимается в обычные `columns/rows`
4. если текст слабый, backend может уйти в fallback path

## Структура репозитория

```text
tsgen-desktop/
├─ front/                  React + Vite + Electron UI
├─ mvp_backend/            FastAPI backend
│  ├─ parser/             PDF/DOCX/TXT/image parsing
│  ├─ app.py              FastAPI entrypoint
│  ├─ routes.py           API
│  ├─ parsers.py          parse_file + schema handling
│  ├─ generator.py        TypeScript generation
│  ├─ validation.py       TS/schema validation
│  └─ storage.py          persistence, history, auth
├─ llamaparse_service/     отдельный сервис для llama-parse
├─ docker-compose.yml
└─ README.md
```

## Требования

### Для Docker-запуска

- `Docker`
- `Docker Compose`

### Для локального запуска без Docker

- `Python 3.11`
- `Node.js 20+`
- `npm`

## Настройка `.env`

Основной `.env` лежит в [mvp_backend/.env](/home/user/Desktop/tsgen-desktop/mvp_backend/.env).

Минимально важные параметры:

```env
HOST=0.0.0.0
PORT=8000

TSGEN_MODEL_PROVIDER=gigachat
TSGEN_MODEL_BASE_URL=https://gigachat.devices.sberbank.ru/api/v1
TSGEN_MODEL_NAME=GigaChat-2-Max
TSGEN_GIGACHAT_AUTH_URL=https://ngw.devices.sberbank.ru:9443/api/v2/oauth
TSGEN_GIGACHAT_AUTH_KEY=...
TSGEN_GIGACHAT_SCOPE=GIGACHAT_API_CORP
TSGEN_GIGACHAT_AUTH_SCHEME=Basic
TSGEN_GIGACHAT_CA_BUNDLE=/app/certs/russian_trusted_root_ca_pem.crt
TSGEN_GIGACHAT_SSL_VERIFY=true

LLAMAPARSE_ENABLED=true
LLAMAPARSE_API_KEY=...
```

Если у вас нет нужды в `LlamaParse`, можно выключить:

```env
LLAMAPARSE_ENABLED=false
```

## Запуск через Docker

Из корня проекта:

```bash
docker compose build llamaparse backend frontend
docker compose up -d --force-recreate
```

Проверка статуса:

```bash
docker compose ps
```

Логи:

```bash
docker compose logs -f
```

Логи по сервисам:

```bash
docker compose logs -f backend
docker compose logs -f frontend
docker compose logs -f llamaparse
```

### Адреса после запуска

- frontend: `http://localhost:8080`
- backend: `http://localhost:8000`
- backend health: `http://localhost:8000/health`
- llamaparse health: `http://localhost:8030/health`

## Локальный запуск без Docker

### Backend

```bash
cd /home/user/Desktop/tsgen-desktop/mvp_backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```

### LlamaParse service

```bash
cd /home/user/Desktop/tsgen-desktop/llamaparse_service
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app:app --host 0.0.0.0 --port 8030 --reload
```

### Frontend

```bash
cd /home/user/Desktop/tsgen-desktop/front
npm install
npm run dev
```

## Electron-режим

Если нужен desktop-клиент:

```bash
cd /home/user/Desktop/tsgen-desktop/front
npm install
npm run electron:dev
```

## Основные API

Главные backend-роуты:

- `POST /api/source-preview`
- `POST /api/generate`
- `GET /api/history`
- `GET /api/learning/events`
- `GET /api/learning/memory`
- `GET /health`

Дополнительно есть auth/profile/template/repair/training endpoints.

## Проверка проекта

### Backend unit tests

Из корня проекта:

```bash
python3 -m unittest mvp_backend.test_parsers
python3 -m unittest mvp_backend.test_validation
```

### Быстрая проверка Python-файлов

```bash
python3 -m py_compile \
  mvp_backend/app.py \
  mvp_backend/routes.py \
  mvp_backend/parsers.py \
  mvp_backend/parser/document_parser.py \
  mvp_backend/parser/llamaparse_client.py \
  llamaparse_service/app.py
```

### Frontend build

```bash
cd /home/user/Desktop/tsgen-desktop/front
npm run build
```

## Как пользоваться

Обычный сценарий работы:

1. Загрузить источник
2. Загрузить target JSON или вставить его вручную
3. Нажать `Сгенерировать`
4. Проверить:
   - `Source preview`
   - `Preview JSON`
   - `Mapping overrides`
   - `Качество генерации`
5. При необходимости вручную поправить mappings
6. Скачать:
   - `.ts`
   - `Preview JSON`

## Как уменьшается расход токенов модели

Проект старается не обращаться к модели там, где можно обойтись более дешёвыми слоями.

Расход токенов уменьшается за счёт нескольких механизмов.

### 1. Сначала используются не модельные источники

Перед тем как идти в модель, система пытается закрыть задачу более дешёвыми слоями:

- deterministic matching
- personal memory
- global patterns
- semantic graph candidates
- position fallback

Если нужное соответствие уже известно или его можно уверенно вывести без LLM, модель вообще не вызывается для этой части задачи.

### 2. В модель отправляется не весь источник, а только сжатый контекст

В `model_client.py` используются лимиты:

- `TSGEN_MODEL_SAMPLE_ROWS`
- `TSGEN_MODEL_HINT_LIMIT`
- `TSGEN_MODEL_MAPPING_MAX_TOKENS`
- `TSGEN_MODEL_DRAFT_MAX_TOKENS`

Что это даёт:

- в промпт попадает только ограниченное число sample rows
- количество personal/global hints ограничено
- максимальный размер ответа модели ограничен
- draft JSON и mapping path не разрастаются бесконтрольно

То есть проект не отправляет в GigaChat весь документ целиком, если это не нужно.

### 3. Для mapping используется shortlist кандидатов

В `learning_pipeline.py` нет полного перебора “всех source полей против всех target полей” через модель.

Система сначала:

- строит shortlist кандидатов
- оценивает priors
- отбрасывает слабые варианты
- и только потом вызывает model ranking там, где это действительно нужно

Это уменьшает число model calls.

### 4. Personal memory уменьшает повторные вызовы

Если пользователь уже подтверждал похожие соответствия раньше, система пытается взять их из:

- personal memory

а не спрашивать модель повторно.

То есть на повторяющихся документах расход токенов со временем снижается.

### 5. Global patterns уменьшают расход на типовых кейсах

Когда в проекте накапливаются устойчивые подтверждённые паттерны, они могут использоваться как:

- global patterns

Это помогает не обращаться к модели заново на типичных источниках и target schema.

### 6. Semantic graph сужает поиск

Семантический граф нужен не только для explainability, но и для того, чтобы:

- не ранжировать бессмысленные кандидаты
- снижать число слабых model suggestions
- раньше отсекать semantic conflicts

Это тоже уменьшает расход токенов, потому что модель зовётся уже на более чистом shortlist.

### 7. Token usage измеряется и сохраняется

Во время generation backend собирает usage capture:

- `provider`
- `model_name`
- `input_tokens`
- `output_tokens`
- `total_tokens`
- `estimated_tokens_saved`
- `call_count`

Часть этих данных сохраняется в generation metrics.

`estimated_tokens_saved` сейчас отражает, сколько токенов удалось сэкономить за счёт precached/prefilled частей model interaction.

## Что сохраняется в память системы

Под “памятью” в проекте имеется не один общий blob, а несколько разных слоёв.

### 1. Personal mapping memory

Сохраняются подтверждённые пользователем соответствия:

- какой `source field`
- к какому `target field`
- в каком schema context
- для какого пользователя

Это потом используется в:

- `get_personal_mapping_memory_candidates(...)`

Именно этот слой помогает системе подхватывать привычные для конкретного пользователя mappings без нового вызова модели.

### 2. Global mapping patterns

Сохраняются устойчивые общие паттерны, если одно и то же сопоставление подтверждается достаточно стабильно.

Этот слой потом используется в:

- `get_global_mapping_pattern_candidates(...)`

Он нужен для повторяющихся типовых кейсов уже не на уровне одного пользователя, а шире.

### 3. Semantic graph memory

Сохраняются связи между:

- source fields
- target fields
- semantic roles
- supporting signals

Потом это используется в:

- `get_semantic_graph_mapping_candidates(...)`

Это помогает и качеству, и экономии токенов, потому что граф сужает набор кандидатов ещё до модельного ранжирования.

### 4. Draft JSON naming memory

Для draft JSON сохраняются подтверждённые naming suggestions:

- какая source column
- какое target field name
- насколько это было подтверждено

Это используется при следующих построениях draft JSON, чтобы не просить модель каждый раз заново придумывать имена полей.

### 5. История подтверждений и corrections

В системе также сохраняются:

- generation history
- mapping suggestions
- подтверждения generation
- draft JSON suggestions
- correction sessions
- repair apply changes

То есть память строится не из одной таблицы, а из цепочки артефактов:

- что было предложено
- что было подтверждено
- что было отклонено
- что было исправлено вручную

### 6. Что не сохраняется как “магическая память”

Система не запоминает весь документ целиком как универсальное знание “на будущее”.

Обычно сохраняются именно:

- mappings
- naming suggestions
- pattern-level признаки
- correction outcomes
- generation metrics

Это важное ограничение: память у проекта прикладная и структурированная, а не абстрактная.

## Что важно понимать

## Текущее состояние проекта

Сейчас это рабочий monorepo, в котором уже есть:

- генерация `TypeScript`
- preview результата
- explainability
- ручная коррекция mappings
- история генераций
- backend persistence
- отдельный сервис для `LlamaParse`

Если нужен более детальный разбор backend parser pipeline, смотрите:

- [mvp_backend/parser/README.md](/home/user/Desktop/tsgen-desktop/mvp_backend/parser/README.md)
- [mvp_backend/README.md](/home/user/Desktop/tsgen-desktop/mvp_backend/README.md)

## Тесты

В проекте уже есть набор unit- и integration-like тестов для основных слоёв backend.

### Основные тестовые файлы

- [mvp_backend/test_parsers.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_parsers.py)  
  Главный файл с тестами parser pipeline. Проверяет разбор `docx/pdf/txt/image/csv/xlsx/xls`, form-aware extraction, OCR/LlamaParse routing, explainability и repair flow.

- [mvp_backend/test_generate.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_generate.py)  
  Проверяет генерацию `TypeScript`, сборку preview и базовую связку generation pipeline.

- [mvp_backend/test_validation.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_validation.py)  
  Проверяет validation-слой: компиляцию TS, диагностику и сверку preview с target schema.

- [mvp_backend/test_learning_pipeline.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_learning_pipeline.py)  
  Проверяет memory-aware mapping pipeline, shortlist кандидатов и логику learning/mapping resolution.

- [mvp_backend/test_draft_json_pipeline.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_draft_json_pipeline.py)  
  Проверяет построение `Draft JSON`, naming suggestions и fallback-логику.

- [mvp_backend/test_model_client.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_model_client.py)  
  Проверяет model client, обработку ответов модели, token usage и защиту от некорректного JSON-ответа.

- [mvp_backend/test_matcher.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_matcher.py)  
  Покрывает старые и вспомогательные deterministic matching rules.

- [mvp_backend/test_storage.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_storage.py)  
  Проверяет persistence-слой: history, memory, suggestions, correction sessions и связанные операции хранения.

- [mvp_backend/test_routes_auth.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_routes_auth.py)  
  Проверяет auth/profile routes: регистрация, логин, профиль, смена пароля и связанные ошибки.

- [mvp_backend/test_benchmarking.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_benchmarking.py)  
  Покрывает benchmarking utilities и агрегирующую статистику.

- [mvp_backend/test_benchmark_expectations.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_benchmark_expectations.py)  
  Проверяет ожидаемые benchmark-метрики и контрольные сценарии качества.

### Что именно проверяет `test_parsers.py`

Внутри [mvp_backend/test_parsers.py](/home/user/Desktop/tsgen-desktop/mvp_backend/test_parsers.py) сейчас есть три большие группы.

`DocumentParserTests`

- Проверяет `DOCX` и `TXT` разбор обычных таблиц и text fallback.
- Проверяет form-like `DOCX`: `kv_pairs`, `source_candidates`, generic form understanding и ambiguities.
- Проверяет, что question/option rows не становятся ложными scalar fields.
- Проверяет form-aware source resolution и legacy fallback для критичных/некритичных полей.
- Проверяет image-like path, OCR requirement и OCR/LlamaParse integration.
- Проверяет `parse_target_schema(...)`: нормальный JSON, пустые массивы, wrapper вида `{ "input": [...] }`.

`PdfAndRepairParserTests`

- Проверяет `PDF zoning`: отделение `table/form/text/noise`.
- Проверяет routing в table parser или form parser по типу PDF-региона.
- Проверяет image-based PDF и OCR-based extraction.
- Проверяет OCR post-processing: noise filtering, checkbox detection, table reconstruction, row-band clustering.
- Проверяет markdown-table routing из `LlamaParse`.
- Проверяет explainability:
  - `quality_summary`
  - `pdf_zone_summary`
  - `ocr_zone_summary`
- Проверяет repair flow:
  - `repair_preview`
  - targeted patch
  - inspection-only actions
  - `repair_apply`
  - persistence при наличии `generation_id`

`ExcelParserTests`

- Проверяет `xlsx/xls` sheet parsing.
- Проверяет merge нескольких листов.
- Проверяет split нескольких таблиц внутри одного листа.
- Проверяет `selected_sheet` behavior для `resolve_generation_source(...)`.
- Проверяет ошибки при обращении к несуществующему листу.

### Как запускать

Запуск всего набора parser-тестов:

```bash
python3 -m unittest mvp_backend.test_parsers
```

Запуск отдельного файла:

```bash
python3 -m unittest mvp_backend.test_validation
```

Быстрая проверка синтаксиса после правок:

```bash
python3 -m py_compile mvp_backend/routes.py mvp_backend/parsers.py
```

### Зачем это покрытие нужно

Эти тесты сейчас в первую очередь страхуют проект от регрессий в самых хрупких местах:

- разбор нестандартных документов
- form-aware extraction
- OCR/PDF routing
- schema parsing
- repair pipeline
- generation/validation
- learning/persistence

Именно поэтому даже небольшие правки в parser или generation flow лучше сразу прогонять через соответствующий тестовый файл, а не только через UI.
