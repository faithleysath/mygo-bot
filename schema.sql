CREATE TABLE llm_providers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    api_key TEXT,
    archived_at INTEGER
);
CREATE UNIQUE INDEX active_provider_name ON llm_providers(name) WHERE archived_at IS NULL;
CREATE TABLE llm_models (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL REFERENCES llm_providers,
    model_key TEXT NOT NULL,
    context_window_tokens INTEGER CHECK(context_window_tokens > 0),
    input_modalities TEXT NOT NULL CHECK(json_type(input_modalities)='array'),
    reasoning_efforts TEXT NOT NULL CHECK(json_type(reasoning_efforts)='array'),
    pricing TEXT CHECK(json_type(pricing)='object'),
    archived_at INTEGER
);
CREATE UNIQUE INDEX active_model_key ON llm_models(provider_id, model_key) WHERE archived_at IS NULL;
CREATE VIEW model_catalog AS
SELECT m.*, p.archived_at AS provider_archived_at,
       (m.archived_at IS NULL AND p.archived_at IS NULL) AS available
FROM llm_models m JOIN llm_providers p ON p.id=m.provider_id;
CREATE TABLE mcp_servers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
    shared INTEGER NOT NULL DEFAULT 0 CHECK (shared IN (0,1)),
    transport TEXT NOT NULL CHECK (transport IN ('streamable-http','stdio')),
    allowed_tools TEXT CHECK (allowed_tools IS NULL OR json_type(allowed_tools)='array'),
    config_json TEXT NOT NULL CHECK (json_type(config_json)='object'),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms BETWEEN 1 AND 86400000),
    revision INTEGER NOT NULL CHECK (revision > 0)
);
CREATE TABLE mcp_oauth (
    server_id TEXT PRIMARY KEY REFERENCES mcp_servers(id) ON DELETE CASCADE,
    generation TEXT NOT NULL,
    binding TEXT,
    pending TEXT,
    credentials TEXT,
    staged_credentials TEXT,
    rejected INTEGER NOT NULL DEFAULT 0 CHECK (rejected IN (0,1))
);
-- Credentials belong to a particular endpoint, authentication settings and HTTP headers.
CREATE TRIGGER invalidate_mcp_oauth AFTER UPDATE ON mcp_servers
WHEN NEW.transport != OLD.transport OR NEW.config_json != OLD.config_json
BEGIN DELETE FROM mcp_oauth WHERE server_id=NEW.id; END;
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle','running','pausing','paused')),
    model_id TEXT NOT NULL REFERENCES llm_models,
    reasoning_effort TEXT,
    input_queue_cursor TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    needs_model INTEGER NOT NULL DEFAULT 0 CHECK (needs_model IN (0,1))
);
CREATE TABLE llm_calls (
    id TEXT PRIMARY KEY,
    session_id TEXT REFERENCES sessions,
    model_id TEXT NOT NULL REFERENCES llm_models,
    reasoning_effort TEXT,
    input_end_position INTEGER,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL CHECK(status IN ('in_flight','succeeded','failed','cancelled','interrupted')),
    upstream_response_id TEXT,
    http_status INTEGER,
    error_summary TEXT,
    raw_usage TEXT CHECK(json_type(raw_usage)='object'),
    input_uncached_tokens INTEGER CHECK(input_uncached_tokens>=0),
    cache_read_tokens INTEGER CHECK(cache_read_tokens>=0),
    cache_write_tokens INTEGER CHECK(cache_write_tokens>=0),
    output_tokens INTEGER CHECK(output_tokens>=0),
    pricing_snapshot TEXT CHECK(pricing_snapshot IS NULL OR json_valid(pricing_snapshot)),
    cost_nano_usd INTEGER CHECK(cost_nano_usd>=0),
    origin_node_id TEXT NOT NULL DEFAULT '',
    UNIQUE(session_id, id)
);
CREATE INDEX calls_by_session ON llm_calls(session_id, started_at, id);
CREATE INDEX calls_by_model ON llm_calls(model_id, started_at, id);
CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    mime_type TEXT NOT NULL,
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    byte_len INTEGER NOT NULL CHECK (byte_len > 0),
    data BLOB
);
CREATE INDEX attachments_by_hash ON attachments(content_hash) WHERE data IS NOT NULL;
CREATE TABLE items (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    -- 撤回与工具结果各自引用原始条目。
    target_id TEXT,
    llm_call_id TEXT,
    idempotency_key TEXT,
    codemode_call_item_id TEXT,
    origin_node_id TEXT NOT NULL DEFAULT '',
    CHECK (codemode_call_item_id IS NULL OR kind='codemode_event'
           OR (kind='function_call_output' AND target_id IS codemode_call_item_id)),
    CHECK (kind!='codemode_event' OR (codemode_call_item_id IS NOT NULL AND target_id IS NULL
           AND json_type(payload,'$.task_id') IS 'text'
           AND json_type(payload,'$.sequence') IS 'integer'
           AND coalesce(json_extract(payload,'$.event'),'') IN ('message','lease_warning','completed')
           AND coalesce(json_type(payload,'$.notify'),'') IN ('true','false')
           AND (json_extract(payload,'$.event')='completed' OR json_extract(payload,'$.notify')=1)
           AND json_type(payload,'$.content') IS 'array')),
    FOREIGN KEY (session_id, codemode_call_item_id) REFERENCES items(session_id, id),
    FOREIGN KEY (session_id, llm_call_id) REFERENCES llm_calls(session_id, id),
    UNIQUE (session_id, id),
    UNIQUE (kind, target_id),
    FOREIGN KEY (session_id, target_id) REFERENCES items(session_id, id)
);
CREATE INDEX items_by_llm_call ON items(llm_call_id) WHERE llm_call_id IS NOT NULL;
CREATE INDEX items_by_kind ON items(session_id, kind, id);
CREATE UNIQUE INDEX tools_by_call ON items(session_id,json_extract(payload,'$.call_id')) WHERE kind='function_call';
CREATE UNIQUE INDEX items_by_idempotency ON items(session_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX codemode_receipts ON items(codemode_call_item_id)
    WHERE kind='function_call_output' AND codemode_call_item_id IS NOT NULL;
CREATE UNIQUE INDEX codemode_event_sequence ON items(codemode_call_item_id,json_extract(payload,'$.sequence'))
    WHERE kind='codemode_event';
CREATE UNIQUE INDEX codemode_completed ON items(codemode_call_item_id)
    WHERE kind='codemode_event' AND json_extract(payload,'$.event')='completed';
CREATE INDEX codemode_warnings ON items(codemode_call_item_id,id)
    WHERE kind='codemode_event' AND json_extract(payload,'$.event')='lease_warning';
-- 续租或结束会使未消费的租期提醒失效。提醒内容及已消费的上下文保留。
CREATE TABLE invalidated_warnings (
    item_id TEXT PRIMARY KEY REFERENCES items(id)
);
CREATE INDEX queue_by_session ON items(session_id,id) WHERE kind IN ('user_message','codemode_event');
CREATE TABLE context_items (
    session_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position > 0),
    item_id TEXT NOT NULL UNIQUE,
    PRIMARY KEY (session_id, position),
    FOREIGN KEY (session_id, item_id) REFERENCES items(session_id, id)
);
CREATE VIEW pending AS
SELECT i.id,i.session_id FROM items i JOIN sessions s ON s.id=i.session_id
WHERE i.kind IN ('user_message','codemode_event') AND i.id>s.input_queue_cursor
AND ((i.kind='user_message' AND NOT EXISTS (SELECT 1 FROM items w WHERE w.kind='withdrawal' AND w.target_id=i.id))
  OR (i.kind='codemode_event' AND json_extract(i.payload,'$.notify')
      AND NOT EXISTS (SELECT 1 FROM invalidated_warnings w WHERE w.item_id=i.id)));
CREATE TABLE local_node (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    secret_key BLOB NOT NULL CHECK (length(secret_key) = 32),
    node_id TEXT NOT NULL,
    cluster_secret BLOB NOT NULL CHECK (length(cluster_secret) = 32),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    meta_seq INTEGER NOT NULL DEFAULT 1 CHECK (meta_seq > 0),
    membership_gen INTEGER NOT NULL DEFAULT 1 CHECK (membership_gen > 0)
);
CREATE TABLE cluster_peers (
    node_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    os TEXT NOT NULL DEFAULT '',
    arch TEXT NOT NULL DEFAULT '',
    meta_seq INTEGER NOT NULL DEFAULT 0,
    addrs_json TEXT NOT NULL DEFAULT '[]' CHECK (json_type(addrs_json) = 'array'),
    addrs_ts INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE sync_streams (
    stream_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('provider','model','mcp','session','call')),
    epoch INTEGER NOT NULL DEFAULT 1 CHECK (epoch > 0),
    head_seq INTEGER NOT NULL DEFAULT 0 CHECK (head_seq >= 0),
    head_hash BLOB NOT NULL CHECK (length(head_hash) = 32),
    synced_epoch INTEGER NOT NULL DEFAULT 0 CHECK (synced_epoch >= 0),
    synced_seq INTEGER NOT NULL DEFAULT 0 CHECK (synced_seq >= 0),
    synced_hash BLOB NOT NULL DEFAULT X'0000000000000000000000000000000000000000000000000000000000000000' CHECK (length(synced_hash) = 32),
    checkpoint_seq INTEGER NOT NULL DEFAULT 0 CHECK (checkpoint_seq >= 0),
    watched INTEGER NOT NULL DEFAULT 0 CHECK (watched IN (0,1)),
    deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0,1)),
    retired INTEGER NOT NULL DEFAULT 0 CHECK (retired IN (0,1)),
    reset_gen INTEGER NOT NULL DEFAULT 0 CHECK (reset_gen >= 0),
    head_snapshot TEXT CHECK (head_snapshot IS NULL OR json_valid(head_snapshot)),
    updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX sync_streams_by_kind ON sync_streams(kind, stream_id);
CREATE TABLE sync_commits (
    stream_id TEXT NOT NULL REFERENCES sync_streams(stream_id) ON DELETE CASCADE,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    seq INTEGER NOT NULL CHECK (seq > 0),
    parent_hash BLOB NOT NULL CHECK (length(parent_hash) = 32),
    commit_hash BLOB NOT NULL CHECK (length(commit_hash) = 32),
    author_node_id TEXT NOT NULL,
    op_type TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (stream_id, epoch, seq),
    UNIQUE (stream_id, commit_hash)
);
CREATE TABLE sync_staging_commits (
    stream_id TEXT NOT NULL,
    epoch INTEGER NOT NULL CHECK (epoch > 0),
    seq INTEGER NOT NULL CHECK (seq > 0),
    parent_hash BLOB NOT NULL CHECK (length(parent_hash) = 32),
    commit_hash BLOB NOT NULL CHECK (length(commit_hash) = 32),
    author_node_id TEXT NOT NULL,
    op_type TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    created_at INTEGER NOT NULL,
    PRIMARY KEY (stream_id, epoch, seq),
    UNIQUE (stream_id, commit_hash)
);
CREATE TABLE sync_conflicts (
    stream_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('provider','model','mcp','session','call')),
    local_epoch INTEGER NOT NULL,
    local_seq INTEGER NOT NULL,
    local_hash BLOB NOT NULL,
    remote_epoch INTEGER NOT NULL,
    remote_seq INTEGER NOT NULL,
    remote_hash BLOB NOT NULL,
    remote_node_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    related_local_id TEXT,
    summary_json TEXT CHECK (summary_json IS NULL OR json_valid(summary_json)),
    remote_snapshot TEXT CHECK (remote_snapshot IS NULL OR json_valid(remote_snapshot)),
    detected_at INTEGER NOT NULL
);
CREATE INDEX sync_conflicts_by_local ON sync_conflicts(related_local_id) WHERE related_local_id IS NOT NULL;
CREATE TABLE sync_resolutions (
    stream_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('provider','model','mcp','session','call')),
    chosen TEXT NOT NULL CHECK (chosen IN ('local','cluster')),
    resolver_node_id TEXT NOT NULL,
    local_epoch INTEGER NOT NULL,
    local_seq INTEGER NOT NULL,
    local_hash BLOB NOT NULL CHECK (length(local_hash) = 32),
    remote_epoch INTEGER NOT NULL,
    remote_seq INTEGER NOT NULL,
    remote_hash BLOB NOT NULL CHECK (length(remote_hash) = 32),
    winner_epoch INTEGER NOT NULL,
    winner_seq INTEGER NOT NULL,
    winner_hash BLOB NOT NULL CHECK (length(winner_hash) = 32),
    resolved_at INTEGER NOT NULL
);
CREATE INDEX sync_resolutions_by_time ON sync_resolutions(resolved_at DESC, stream_id);
PRAGMA user_version = 5;
