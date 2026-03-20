CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY,
  email TEXT,
  external_id TEXT,
  display_name TEXT,
  password_hash TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (email IS NOT NULL OR external_id IS NOT NULL),
  UNIQUE (email),
  UNIQUE (external_id)
);

CREATE TABLE IF NOT EXISTS saved_schemas (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  name_normalized TEXT NOT NULL,
  category TEXT NOT NULL,
  description TEXT,
  schema_json TEXT NOT NULL,
  schema_hash TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (json_valid(schema_json)),
  UNIQUE (user_id, name_normalized),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS generations (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  schema_id INTEGER,
  title TEXT NOT NULL,
  source_payload TEXT NOT NULL,
  source_payload_format TEXT NOT NULL DEFAULT 'json',
  current_version_id INTEGER,
  status TEXT NOT NULL DEFAULT 'draft',
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (source_payload_format IN ('json', 'text', 'csv', 'xml', 'file_ref')),
  CHECK (status IN ('draft', 'processing', 'completed', 'failed')),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_id) REFERENCES saved_schemas(id) ON DELETE SET NULL,
  FOREIGN KEY (current_version_id) REFERENCES generation_versions(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS generation_versions (
  id INTEGER PRIMARY KEY,
  generation_id INTEGER NOT NULL,
  parent_version_id INTEGER,
  version_number INTEGER NOT NULL,
  change_type TEXT NOT NULL,
  note TEXT,
  target_json TEXT NOT NULL,
  generated_typescript TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (version_number > 0),
  CHECK (change_type IN ('initial', 'edited_json', 'regenerate', 'manual_fix')),
  CHECK (json_valid(target_json)),
  UNIQUE (generation_id, version_number),
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (parent_version_id) REFERENCES generation_versions(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS generation_artifacts (
  id INTEGER PRIMARY KEY,
  generation_id INTEGER NOT NULL,
  version_id INTEGER NOT NULL,
  file_name TEXT NOT NULL,
  file_path TEXT,
  file_type TEXT NOT NULL,
  selected_sheet TEXT,
  parsed_file_json TEXT NOT NULL,
  mappings_json TEXT NOT NULL,
  preview_json TEXT NOT NULL,
  warnings_json TEXT NOT NULL,
  legacy_history_id INTEGER UNIQUE,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (json_valid(parsed_file_json)),
  CHECK (json_valid(mappings_json)),
  CHECK (json_valid(preview_json)),
  CHECK (json_valid(warnings_json)),
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (version_id) REFERENCES generation_versions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS mapping_cache (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  schema_id INTEGER,
  schema_scope_key INTEGER GENERATED ALWAYS AS (coalesce(schema_id, 0)) STORED,
  source_field TEXT NOT NULL,
  source_field_normalized TEXT NOT NULL,
  target_field TEXT NOT NULL,
  target_field_normalized TEXT NOT NULL,
  confidence REAL,
  source_of_truth TEXT NOT NULL DEFAULT 'llm',
  usage_count INTEGER NOT NULL DEFAULT 1,
  last_generation_id INTEGER,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  last_used_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CHECK (usage_count >= 0),
  CHECK (source_of_truth IN ('llm', 'user_confirmed', 'system_rule')),
  UNIQUE (user_id, schema_scope_key, source_field_normalized),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_id) REFERENCES saved_schemas(id) ON DELETE SET NULL,
  FOREIGN KEY (last_generation_id) REFERENCES generations(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS generation_metrics (
  id INTEGER PRIMARY KEY,
  generation_id INTEGER NOT NULL,
  version_id INTEGER,
  provider TEXT NOT NULL DEFAULT 'unknown',
  model_name TEXT NOT NULL,
  engine TEXT,
  generation_time_ms INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  cache_hits INTEGER NOT NULL DEFAULT 0,
  cache_misses INTEGER NOT NULL DEFAULT 0,
  estimated_tokens_saved INTEGER NOT NULL DEFAULT 0,
  success INTEGER NOT NULL,
  error_message TEXT,
  cost_usd REAL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (generation_time_ms >= 0),
  CHECK (input_tokens >= 0),
  CHECK (output_tokens >= 0),
  CHECK (total_tokens = input_tokens + output_tokens),
  CHECK (cache_hits >= 0),
  CHECK (cache_misses >= 0),
  CHECK (estimated_tokens_saved >= 0),
  CHECK (success IN (0, 1)),
  CHECK (cost_usd IS NULL OR cost_usd >= 0),
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (version_id) REFERENCES generation_versions(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_schemas_user_updated
  ON saved_schemas (user_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_saved_schemas_user_category
  ON saved_schemas (user_id, category, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_generations_user_updated
  ON generations (user_id, updated_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_generations_user_status_updated
  ON generations (user_id, status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_generations_schema_updated
  ON generations (schema_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_generation_versions_generation_version
  ON generation_versions (generation_id, version_number DESC);

CREATE INDEX IF NOT EXISTS idx_generation_versions_parent
  ON generation_versions (parent_version_id);

CREATE INDEX IF NOT EXISTS idx_generation_artifacts_generation_version
  ON generation_artifacts (generation_id, version_id);

CREATE INDEX IF NOT EXISTS idx_generation_artifacts_legacy
  ON generation_artifacts (legacy_history_id);

CREATE INDEX IF NOT EXISTS idx_mapping_cache_lookup
  ON mapping_cache (user_id, source_field_normalized, schema_scope_key);

CREATE INDEX IF NOT EXISTS idx_mapping_cache_usage
  ON mapping_cache (user_id, usage_count DESC, last_used_at DESC);

CREATE INDEX IF NOT EXISTS idx_mapping_cache_schema_recent
  ON mapping_cache (user_id, schema_id, last_used_at DESC);

CREATE INDEX IF NOT EXISTS idx_generation_metrics_generation_created
  ON generation_metrics (generation_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_generation_metrics_version
  ON generation_metrics (version_id);

CREATE INDEX IF NOT EXISTS idx_generation_metrics_success
  ON generation_metrics (success, created_at DESC);

CREATE TRIGGER IF NOT EXISTS trg_users_set_updated_at
AFTER UPDATE ON users
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE users
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_saved_schemas_set_updated_at
AFTER UPDATE ON saved_schemas
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE saved_schemas
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_generations_set_updated_at
AFTER UPDATE ON generations
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE generations
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_mapping_cache_set_updated_at
AFTER UPDATE ON mapping_cache
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE mapping_cache
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_generation_versions_parent_same_generation
BEFORE INSERT ON generation_versions
FOR EACH ROW
WHEN NEW.parent_version_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM generation_versions parent
    WHERE parent.id = NEW.parent_version_id
      AND parent.generation_id = NEW.generation_id
  )
BEGIN
  SELECT RAISE(ABORT, 'parent_version_id must belong to the same generation');
END;

CREATE TRIGGER IF NOT EXISTS trg_generations_current_version_same_generation
BEFORE UPDATE OF current_version_id ON generations
FOR EACH ROW
WHEN NEW.current_version_id IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM generation_versions version
    WHERE version.id = NEW.current_version_id
      AND version.generation_id = NEW.id
  )
BEGIN
  SELECT RAISE(ABORT, 'current_version_id must belong to the same generation');
END;
