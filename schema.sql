CREATE TABLE llm_providers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    base_url TEXT NOT NULL,
    api_key TEXT
);
CREATE TABLE llm_models (
    id INTEGER PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES llm_providers,
    model_key TEXT NOT NULL,
    context_window_tokens INTEGER CHECK(context_window_tokens > 0),
    input_modalities TEXT NOT NULL CHECK(json_type(input_modalities)='array'),
    reasoning_efforts TEXT NOT NULL CHECK(json_type(reasoning_efforts)='array'),
    pricing TEXT CHECK(json_type(pricing)='object'),
    UNIQUE(provider_id, model_key)
);
CREATE TABLE config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    default_model_id INTEGER REFERENCES llm_models,
    default_reasoning_effort TEXT
);
INSERT INTO config(id) VALUES(1);
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle','running','pausing','paused')),
    model_id INTEGER REFERENCES llm_models,
    reasoning_effort TEXT,
    user_queue_cursor INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    needs_model INTEGER NOT NULL DEFAULT 0 CHECK (needs_model IN (0,1))
);
CREATE TABLE llm_calls (
    id INTEGER PRIMARY KEY,
    session_id INTEGER REFERENCES sessions,
    model_id INTEGER NOT NULL REFERENCES llm_models,
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
    pricing_snapshot TEXT CHECK(json_valid(pricing_snapshot)),
    cost_nano_usd INTEGER CHECK(cost_nano_usd>=0),
    UNIQUE(session_id, id)
);
CREATE INDEX calls_by_session ON llm_calls(session_id, started_at, id);
CREATE INDEX calls_by_model ON llm_calls(model_id, started_at, id);
CREATE TABLE attachments (
    id INTEGER PRIMARY KEY,
    mime_type TEXT NOT NULL,
    data BLOB NOT NULL
);
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES sessions,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    -- 撤回与工具结果各自引用原始条目。
    target_id INTEGER,
    llm_call_id INTEGER,
    idempotency_key TEXT,
    FOREIGN KEY (session_id, llm_call_id) REFERENCES llm_calls(session_id, id),
    UNIQUE (session_id, id),
    UNIQUE (kind, target_id),
    FOREIGN KEY (session_id, target_id) REFERENCES items(session_id, id)
);
CREATE INDEX items_by_llm_call ON items(llm_call_id) WHERE llm_call_id IS NOT NULL;
CREATE INDEX items_by_kind ON items(session_id, kind, id);
CREATE UNIQUE INDEX tools_by_call ON items(session_id,json_extract(payload,'$.call_id')) WHERE kind='function_call';
CREATE UNIQUE INDEX items_by_idempotency ON items(session_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE context_items (
    position INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL UNIQUE,
    FOREIGN KEY (session_id, item_id) REFERENCES items(session_id, id)
);
CREATE INDEX context_by_session ON context_items(session_id, position);
CREATE VIEW pending AS
SELECT i.id,i.session_id FROM items i JOIN sessions s ON s.id=i.session_id
WHERE i.kind='user_message' AND i.id>s.user_queue_cursor
AND NOT EXISTS (SELECT 1 FROM items w WHERE w.kind='withdrawal' AND w.target_id=i.id);
PRAGMA user_version = 1;
