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

CREATE TABLE IF NOT EXISTS schema_fingerprints (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  user_scope_key INTEGER GENERATED ALWAYS AS (coalesce(user_id, 0)) STORED,
  source_generation_id INTEGER,
  source_artifact_id INTEGER,
  source_kind TEXT NOT NULL DEFAULT 'tabular',
  fingerprint_version TEXT NOT NULL DEFAULT 'v1',
  fingerprint_hash TEXT NOT NULL,
  column_signature TEXT,
  normalized_schema_json TEXT NOT NULL,
  normalized_source_text TEXT,
  feature_vector_json TEXT,
  embedding_provider TEXT,
  embedding_model TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (source_kind IN ('tabular', 'document', 'schema', 'template', 'correction_batch', 'mixed')),
  CHECK (json_valid(normalized_schema_json)),
  CHECK (feature_vector_json IS NULL OR json_valid(feature_vector_json)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (user_scope_key, fingerprint_version, fingerprint_hash),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (source_generation_id) REFERENCES generations(id) ON DELETE SET NULL,
  FOREIGN KEY (source_artifact_id) REFERENCES generation_artifacts(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS correction_sessions (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  generation_id INTEGER,
  version_id INTEGER,
  schema_fingerprint_id INTEGER,
  session_type TEXT NOT NULL DEFAULT 'manual_review',
  status TEXT NOT NULL DEFAULT 'open',
  correction_count INTEGER NOT NULL DEFAULT 0,
  acceptance_rate REAL,
  notes TEXT,
  metadata_json TEXT,
  started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  closed_at TEXT,
  CHECK (session_type IN ('manual_review', 'post_generation_fix', 'template_authoring', 'feedback_loop')),
  CHECK (status IN ('open', 'applied', 'discarded', 'merged', 'cancelled')),
  CHECK (correction_count >= 0),
  CHECK (acceptance_rate IS NULL OR (acceptance_rate >= 0 AND acceptance_rate <= 1)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (version_id) REFERENCES generation_versions(id) ON DELETE SET NULL,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS user_corrections (
  id INTEGER PRIMARY KEY,
  session_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  generation_id INTEGER,
  version_id INTEGER,
  schema_fingerprint_id INTEGER,
  correction_type TEXT NOT NULL,
  row_index INTEGER,
  field_path TEXT,
  source_field TEXT,
  source_field_normalized TEXT,
  target_field TEXT,
  target_field_normalized TEXT,
  original_value_json TEXT,
  corrected_value_json TEXT,
  correction_payload_json TEXT,
  rationale TEXT,
  confidence_before REAL,
  confidence_after REAL,
  accepted INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (correction_type IN ('mapping_override', 'value_fix', 'ignore_field', 'rename_field', 'template_edit', 'target_schema_edit', 'code_edit', 'feedback_note')),
  CHECK (original_value_json IS NULL OR json_valid(original_value_json)),
  CHECK (corrected_value_json IS NULL OR json_valid(corrected_value_json)),
  CHECK (correction_payload_json IS NULL OR json_valid(correction_payload_json)),
  CHECK (confidence_before IS NULL OR (confidence_before >= 0 AND confidence_before <= 1)),
  CHECK (confidence_after IS NULL OR (confidence_after >= 0 AND confidence_after <= 1)),
  CHECK (accepted IN (0, 1)),
  FOREIGN KEY (session_id) REFERENCES correction_sessions(id) ON DELETE CASCADE,
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (version_id) REFERENCES generation_versions(id) ON DELETE SET NULL,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS mapping_suggestions (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  generation_id INTEGER,
  schema_fingerprint_id INTEGER,
  source_field TEXT,
  source_field_normalized TEXT,
  target_field TEXT NOT NULL,
  target_field_normalized TEXT NOT NULL,
  confidence REAL,
  reason TEXT,
  status TEXT NOT NULL DEFAULT 'suggested',
  source_of_truth TEXT NOT NULL DEFAULT 'model_suggestion',
  model_provider TEXT,
  model_name TEXT,
  feedback_payload_json TEXT,
  metadata_json TEXT,
  reviewed_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CHECK (status IN ('suggested', 'accepted', 'rejected')),
  CHECK (source_of_truth IN ('deterministic_rule', 'personal_memory', 'model_suggestion', 'global_pattern', 'position_fallback', 'unresolved')),
  CHECK (feedback_payload_json IS NULL OR json_valid(feedback_payload_json)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS draft_json_suggestions (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  schema_fingerprint_id INTEGER,
  source_column TEXT NOT NULL,
  source_column_normalized TEXT NOT NULL,
  suggested_field TEXT NOT NULL,
  suggested_field_normalized TEXT NOT NULL,
  field_type TEXT NOT NULL DEFAULT 'string',
  default_value_json TEXT,
  confidence REAL,
  reason TEXT,
  status TEXT NOT NULL DEFAULT 'suggested',
  source_of_truth TEXT NOT NULL DEFAULT 'model_suggestion',
  model_provider TEXT,
  model_name TEXT,
  feedback_payload_json TEXT,
  metadata_json TEXT,
  reviewed_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (field_type IN ('string', 'number', 'boolean', 'object', 'array', 'null', 'any')),
  CHECK (default_value_json IS NULL OR json_valid(default_value_json)),
  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CHECK (status IN ('suggested', 'accepted', 'rejected')),
  CHECK (source_of_truth IN ('heuristic_fallback', 'personal_memory', 'model_suggestion', 'global_pattern')),
  CHECK (feedback_payload_json IS NULL OR json_valid(feedback_payload_json)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS mapping_memory (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  user_scope_key INTEGER GENERATED ALWAYS AS (coalesce(user_id, 0)) STORED,
  schema_fingerprint_id INTEGER,
  fingerprint_scope_key INTEGER GENERATED ALWAYS AS (coalesce(schema_fingerprint_id, 0)) STORED,
  session_id INTEGER,
  correction_id INTEGER,
  last_generation_id INTEGER,
  source_field TEXT NOT NULL,
  source_field_normalized TEXT NOT NULL,
  target_field TEXT NOT NULL,
  target_field_normalized TEXT NOT NULL,
  transform_hint TEXT,
  weight REAL NOT NULL DEFAULT 1.0,
  confidence REAL,
  usage_count INTEGER NOT NULL DEFAULT 0,
  success_count INTEGER NOT NULL DEFAULT 0,
  failure_count INTEGER NOT NULL DEFAULT 0,
  source_of_truth TEXT NOT NULL DEFAULT 'user_correction',
  metadata_json TEXT,
  last_confirmed_at TEXT,
  last_used_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
  CHECK (usage_count >= 0),
  CHECK (success_count >= 0),
  CHECK (failure_count >= 0),
  CHECK (source_of_truth IN ('user_correction', 'accepted_generation', 'template', 'pattern_candidate', 'model_suggestion', 'imported')),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (user_scope_key, fingerprint_scope_key, source_field_normalized, target_field_normalized),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL,
  FOREIGN KEY (session_id) REFERENCES correction_sessions(id) ON DELETE SET NULL,
  FOREIGN KEY (correction_id) REFERENCES user_corrections(id) ON DELETE SET NULL,
  FOREIGN KEY (last_generation_id) REFERENCES generations(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS few_shot_examples (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  schema_fingerprint_id INTEGER,
  source_generation_id INTEGER,
  source_version_id INTEGER,
  title TEXT NOT NULL,
  example_kind TEXT NOT NULL DEFAULT 'mapping',
  input_summary_json TEXT NOT NULL,
  target_json TEXT NOT NULL,
  mapping_json TEXT,
  output_typescript TEXT,
  quality_score REAL,
  success_count INTEGER NOT NULL DEFAULT 0,
  is_active INTEGER NOT NULL DEFAULT 1,
  metadata_json TEXT,
  last_retrieved_at TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (example_kind IN ('mapping', 'correction_session', 'template', 'generation', 'djson')),
  CHECK (json_valid(input_summary_json)),
  CHECK (json_valid(target_json)),
  CHECK (mapping_json IS NULL OR json_valid(mapping_json)),
  CHECK (quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 1)),
  CHECK (success_count >= 0),
  CHECK (is_active IN (0, 1)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL,
  FOREIGN KEY (source_generation_id) REFERENCES generations(id) ON DELETE SET NULL,
  FOREIGN KEY (source_version_id) REFERENCES generation_versions(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS user_templates (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  schema_fingerprint_id INTEGER,
  name TEXT NOT NULL,
  name_normalized TEXT NOT NULL,
  template_kind TEXT NOT NULL DEFAULT 'transform',
  description TEXT,
  template_json TEXT NOT NULL,
  target_json TEXT,
  generated_typescript TEXT,
  prompt_suffix TEXT,
  usage_count INTEGER NOT NULL DEFAULT 0,
  success_count INTEGER NOT NULL DEFAULT 0,
  last_used_at TEXT,
  is_active INTEGER NOT NULL DEFAULT 1,
  is_shared INTEGER NOT NULL DEFAULT 0,
  metadata_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (template_kind IN ('transform', 'mapping', 'postprocess', 'schema', 'hybrid')),
  CHECK (json_valid(template_json)),
  CHECK (target_json IS NULL OR json_valid(target_json)),
  CHECK (usage_count >= 0),
  CHECK (success_count >= 0),
  CHECK (is_active IN (0, 1)),
  CHECK (is_shared IN (0, 1)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (user_id, name_normalized),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS feature_vectors (
  id INTEGER PRIMARY KEY,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  model_name_key TEXT GENERATED ALWAYS AS (coalesce(model_name, '')) STORED,
  schema_fingerprint_id INTEGER,
  vector_kind TEXT NOT NULL DEFAULT 'dense',
  provider TEXT NOT NULL DEFAULT 'local',
  model_name TEXT,
  dimensions INTEGER NOT NULL DEFAULT 0,
  vector_json TEXT NOT NULL,
  sparse_features_json TEXT,
  feature_norm REAL,
  text_payload TEXT,
  metadata_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (entity_type IN ('schema_fingerprint', 'user_correction', 'mapping_memory', 'few_shot_example', 'user_template', 'pattern_candidate', 'frequent_djson')),
  CHECK (vector_kind IN ('dense', 'sparse', 'hybrid')),
  CHECK (dimensions >= 0),
  CHECK (json_valid(vector_json)),
  CHECK (sparse_features_json IS NULL OR json_valid(sparse_features_json)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (entity_type, entity_id, provider, model_name_key, vector_kind),
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS pattern_candidates (
  id INTEGER PRIMARY KEY,
  candidate_key TEXT NOT NULL,
  pattern_type TEXT NOT NULL DEFAULT 'mapping_rule',
  source_field_normalized TEXT,
  target_field_normalized TEXT,
  schema_hint_hash TEXT,
  proposed_rule_json TEXT NOT NULL,
  evidence_json TEXT,
  status TEXT NOT NULL DEFAULT 'new',
  support_count INTEGER NOT NULL DEFAULT 0,
  distinct_users_count INTEGER NOT NULL DEFAULT 0,
  mean_confidence REAL,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (pattern_type IN ('mapping_rule', 'template_rule', 'correction_rule', 'schema_rule')),
  CHECK (json_valid(proposed_rule_json)),
  CHECK (evidence_json IS NULL OR json_valid(evidence_json)),
  CHECK (status IN ('new', 'reviewing', 'accepted', 'rejected', 'promoted')),
  CHECK (support_count >= 0),
  CHECK (distinct_users_count >= 0),
  CHECK (mean_confidence IS NULL OR (mean_confidence >= 0 AND mean_confidence <= 1)),
  UNIQUE (candidate_key)
);

CREATE TABLE IF NOT EXISTS pattern_stats (
  id INTEGER PRIMARY KEY,
  candidate_id INTEGER NOT NULL,
  recurrence_count INTEGER NOT NULL DEFAULT 0,
  unique_users INTEGER NOT NULL DEFAULT 0,
  accept_count INTEGER NOT NULL DEFAULT 0,
  reject_count INTEGER NOT NULL DEFAULT 0,
  stability_score REAL,
  drift_score REAL,
  first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  last_seen_at TEXT,
  stats_json TEXT,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (recurrence_count >= 0),
  CHECK (unique_users >= 0),
  CHECK (accept_count >= 0),
  CHECK (reject_count >= 0),
  CHECK (stability_score IS NULL OR (stability_score >= 0 AND stability_score <= 1)),
  CHECK (drift_score IS NULL OR (drift_score >= 0 AND drift_score <= 1)),
  CHECK (stats_json IS NULL OR json_valid(stats_json)),
  UNIQUE (candidate_id),
  FOREIGN KEY (candidate_id) REFERENCES pattern_candidates(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS curated_dataset_items (
  id INTEGER PRIMARY KEY,
  source_entity_type TEXT NOT NULL,
  source_entity_id INTEGER NOT NULL,
  item_kind TEXT NOT NULL,
  input_payload_json TEXT NOT NULL,
  target_payload_json TEXT NOT NULL,
  context_json TEXT,
  quality_score REAL,
  selection_reason TEXT,
  status TEXT NOT NULL DEFAULT 'candidate',
  split TEXT NOT NULL DEFAULT 'train',
  snapshot_key TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (source_entity_type IN ('user_correction', 'pattern_candidate', 'few_shot_example', 'user_template', 'mapping_memory')),
  CHECK (item_kind IN ('correction', 'mapping', 'template', 'example', 'pattern')),
  CHECK (json_valid(input_payload_json)),
  CHECK (json_valid(target_payload_json)),
  CHECK (context_json IS NULL OR json_valid(context_json)),
  CHECK (quality_score IS NULL OR (quality_score >= 0 AND quality_score <= 1)),
  CHECK (status IN ('candidate', 'approved', 'rejected', 'exported', 'archived')),
  CHECK (split IN ('train', 'validation', 'test', 'holdout'))
);

CREATE TABLE IF NOT EXISTS dataset_reviews (
  id INTEGER PRIMARY KEY,
  dataset_item_id INTEGER NOT NULL,
  reviewer_user_id INTEGER,
  review_kind TEXT NOT NULL DEFAULT 'manual',
  decision TEXT NOT NULL,
  score REAL,
  notes TEXT,
  metrics_json TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (review_kind IN ('manual', 'automatic', 'benchmark', 'heuristic')),
  CHECK (decision IN ('approved', 'rejected', 'needs_work', 'flagged')),
  CHECK (score IS NULL OR (score >= 0 AND score <= 1)),
  CHECK (metrics_json IS NULL OR json_valid(metrics_json)),
  FOREIGN KEY (dataset_item_id) REFERENCES curated_dataset_items(id) ON DELETE CASCADE,
  FOREIGN KEY (reviewer_user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS training_snapshots (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  snapshot_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  manifest_json TEXT NOT NULL,
  item_count INTEGER NOT NULL DEFAULT 0,
  train_count INTEGER NOT NULL DEFAULT 0,
  validation_count INTEGER NOT NULL DEFAULT 0,
  test_count INTEGER NOT NULL DEFAULT 0,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  finalized_at TEXT,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (status IN ('draft', 'frozen', 'exported', 'consumed')),
  CHECK (json_valid(manifest_json)),
  CHECK (item_count >= 0),
  CHECK (train_count >= 0),
  CHECK (validation_count >= 0),
  CHECK (test_count >= 0),
  UNIQUE (snapshot_hash)
);

CREATE TABLE IF NOT EXISTS model_training_runs (
  id INTEGER PRIMARY KEY,
  snapshot_id INTEGER NOT NULL,
  model_family TEXT NOT NULL,
  base_model TEXT NOT NULL,
  training_job_ref TEXT,
  status TEXT NOT NULL DEFAULT 'queued',
  train_params_json TEXT,
  metrics_json TEXT,
  artifact_uri TEXT,
  started_at TEXT,
  finished_at TEXT,
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
  CHECK (train_params_json IS NULL OR json_valid(train_params_json)),
  CHECK (metrics_json IS NULL OR json_valid(metrics_json)),
  FOREIGN KEY (snapshot_id) REFERENCES training_snapshots(id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS model_deployments (
  id INTEGER PRIMARY KEY,
  training_run_id INTEGER NOT NULL,
  snapshot_id INTEGER NOT NULL,
  model_family TEXT NOT NULL,
  provider TEXT NOT NULL,
  base_url TEXT,
  model_name TEXT NOT NULL,
  artifact_uri TEXT,
  config_json TEXT,
  status TEXT NOT NULL DEFAULT 'inactive',
  notes TEXT,
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  activated_at TEXT,
  deactivated_at TEXT,
  CHECK (provider IN ('ollama', 'openai_compatible', 'gigachat')),
  CHECK (config_json IS NULL OR json_valid(config_json)),
  CHECK (status IN ('inactive', 'active', 'retired')),
  FOREIGN KEY (training_run_id) REFERENCES model_training_runs(id) ON DELETE CASCADE,
  FOREIGN KEY (snapshot_id) REFERENCES training_snapshots(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS uploaded_files (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  generation_id INTEGER,
  artifact_id INTEGER,
  schema_fingerprint_id INTEGER,
  upload_mode TEXT NOT NULL DEFAULT 'guest',
  original_file_name TEXT NOT NULL,
  stored_file_name TEXT,
  storage_path TEXT NOT NULL,
  file_type TEXT NOT NULL,
  mime_type TEXT,
  size_bytes INTEGER NOT NULL DEFAULT 0,
  content_hash TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  metadata_json TEXT,
  uploaded_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  last_accessed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  expires_at TEXT,
  CHECK (upload_mode IN ('guest', 'authorized', 'system')),
  CHECK (status IN ('active', 'processed', 'archived', 'deleted', 'expired')),
  CHECK (size_bytes >= 0),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (storage_path),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
  FOREIGN KEY (generation_id) REFERENCES generations(id) ON DELETE SET NULL,
  FOREIGN KEY (artifact_id) REFERENCES generation_artifacts(id) ON DELETE SET NULL,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS frequent_djson (
  id INTEGER PRIMARY KEY,
  user_id INTEGER,
  user_scope_key INTEGER GENERATED ALWAYS AS (coalesce(user_id, 0)) STORED,
  schema_fingerprint_id INTEGER,
  name TEXT,
  name_normalized TEXT,
  djson_hash TEXT NOT NULL,
  djson_payload TEXT NOT NULL,
  usage_count INTEGER NOT NULL DEFAULT 1,
  success_count INTEGER NOT NULL DEFAULT 0,
  is_shared INTEGER NOT NULL DEFAULT 0,
  metadata_json TEXT,
  last_used_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
  CHECK (json_valid(djson_payload)),
  CHECK (usage_count >= 0),
  CHECK (success_count >= 0),
  CHECK (is_shared IN (0, 1)),
  CHECK (metadata_json IS NULL OR json_valid(metadata_json)),
  UNIQUE (user_scope_key, djson_hash),
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  FOREIGN KEY (schema_fingerprint_id) REFERENCES schema_fingerprints(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_saved_schemas_user_updated
  ON saved_schemas (user_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_saved_schemas_user_category
  ON saved_schemas (user_id, category, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_mapping_suggestions_generation
  ON mapping_suggestions (generation_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mapping_suggestions_target_status
  ON mapping_suggestions (target_field_normalized, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_draft_json_suggestions_schema_status
  ON draft_json_suggestions (schema_fingerprint_id, status, created_at DESC);

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

CREATE INDEX IF NOT EXISTS idx_schema_fingerprints_lookup
  ON schema_fingerprints (user_scope_key, fingerprint_hash, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_schema_fingerprints_generation
  ON schema_fingerprints (source_generation_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_correction_sessions_user_generation
  ON correction_sessions (user_id, generation_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_correction_sessions_status
  ON correction_sessions (status, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_corrections_session_created
  ON user_corrections (session_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_corrections_user_type
  ON user_corrections (user_id, correction_type, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mapping_memory_lookup
  ON mapping_memory (user_scope_key, fingerprint_scope_key, source_field_normalized, weight DESC);

CREATE INDEX IF NOT EXISTS idx_mapping_memory_recent
  ON mapping_memory (user_id, last_used_at DESC, success_count DESC);

CREATE INDEX IF NOT EXISTS idx_few_shot_examples_lookup
  ON few_shot_examples (schema_fingerprint_id, is_active, quality_score DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_few_shot_examples_recent
  ON few_shot_examples (last_retrieved_at DESC, success_count DESC);

CREATE INDEX IF NOT EXISTS idx_user_templates_user_active
  ON user_templates (user_id, is_active, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_user_templates_fingerprint
  ON user_templates (schema_fingerprint_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_feature_vectors_lookup
  ON feature_vectors (entity_type, entity_id, provider, model_name_key);

CREATE INDEX IF NOT EXISTS idx_feature_vectors_schema
  ON feature_vectors (schema_fingerprint_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_pattern_candidates_status
  ON pattern_candidates (status, support_count DESC, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_pattern_candidates_fields
  ON pattern_candidates (source_field_normalized, target_field_normalized);

CREATE INDEX IF NOT EXISTS idx_curated_dataset_items_status_split
  ON curated_dataset_items (status, split, quality_score DESC);

CREATE INDEX IF NOT EXISTS idx_curated_dataset_items_source
  ON curated_dataset_items (source_entity_type, source_entity_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_dataset_reviews_item_created
  ON dataset_reviews (dataset_item_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_training_snapshots_status
  ON training_snapshots (status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_training_runs_snapshot_status
  ON model_training_runs (snapshot_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_deployments_status_activated
  ON model_deployments (status, activated_at DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_deployments_training_run
  ON model_deployments (training_run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_uploaded_files_user_uploaded
  ON uploaded_files (user_id, uploaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_uploaded_files_hash
  ON uploaded_files (content_hash, uploaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_frequent_djson_user_usage
  ON frequent_djson (user_id, usage_count DESC, last_used_at DESC);

CREATE INDEX IF NOT EXISTS idx_frequent_djson_fingerprint
  ON frequent_djson (schema_fingerprint_id, usage_count DESC);

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

CREATE TRIGGER IF NOT EXISTS trg_schema_fingerprints_set_updated_at
AFTER UPDATE ON schema_fingerprints
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE schema_fingerprints
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_mapping_memory_set_updated_at
AFTER UPDATE ON mapping_memory
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE mapping_memory
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_few_shot_examples_set_updated_at
AFTER UPDATE ON few_shot_examples
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE few_shot_examples
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_user_templates_set_updated_at
AFTER UPDATE ON user_templates
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE user_templates
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_pattern_candidates_set_updated_at
AFTER UPDATE ON pattern_candidates
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE pattern_candidates
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_pattern_stats_set_updated_at
AFTER UPDATE ON pattern_stats
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE pattern_stats
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_curated_dataset_items_set_updated_at
AFTER UPDATE ON curated_dataset_items
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE curated_dataset_items
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_training_snapshots_set_updated_at
AFTER UPDATE ON training_snapshots
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE training_snapshots
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_frequent_djson_set_updated_at
AFTER UPDATE ON frequent_djson
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
  UPDATE frequent_djson
  SET updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
  WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_user_corrections_after_insert
AFTER INSERT ON user_corrections
FOR EACH ROW
BEGIN
  UPDATE correction_sessions
  SET correction_count = correction_count + 1
  WHERE id = NEW.session_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_user_corrections_after_delete
AFTER DELETE ON user_corrections
FOR EACH ROW
BEGIN
  UPDATE correction_sessions
  SET correction_count = CASE
    WHEN correction_count > 0 THEN correction_count - 1
    ELSE 0
  END
  WHERE id = OLD.session_id;
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
