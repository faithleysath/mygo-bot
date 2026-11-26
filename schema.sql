CREATE TABLE llm_providers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_ts TEXT NOT NULL DEFAULT '',
    base_url TEXT NOT NULL,
    api_key TEXT,
    endpoint_ts TEXT NOT NULL DEFAULT '',
    archived_at INTEGER,
    archived_ts TEXT NOT NULL DEFAULT '',
    sync_rev INTEGER NOT NULL DEFAULT 0 CHECK (sync_rev >= 0)
);
CREATE INDEX llm_providers_active_name ON llm_providers(name) WHERE archived_at IS NULL;
CREATE INDEX llm_providers_by_rev ON llm_providers(sync_rev, id);

CREATE TABLE llm_models (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    model_key TEXT NOT NULL,
    identity_ts TEXT NOT NULL DEFAULT '',
    context_window_tokens INTEGER CHECK(context_window_tokens IS NULL OR context_window_tokens > 0),
    context_window_ts TEXT NOT NULL DEFAULT '',
    input_modalities TEXT NOT NULL CHECK(json_type(input_modalities)='array'),
    input_modalities_ts TEXT NOT NULL DEFAULT '',
    reasoning_efforts TEXT NOT NULL CHECK(json_type(reasoning_efforts)='array'),
    reasoning_efforts_ts TEXT NOT NULL DEFAULT '',
    pricing TEXT CHECK(pricing IS NULL OR json_type(pricing)='object'),
    pricing_ts TEXT NOT NULL DEFAULT '',
    archived_at INTEGER,
    archived_ts TEXT NOT NULL DEFAULT '',
    sync_rev INTEGER NOT NULL DEFAULT 0 CHECK (sync_rev >= 0)
);
CREATE INDEX llm_models_active_key ON llm_models(provider_id, model_key) WHERE archived_at IS NULL;
CREATE INDEX llm_models_by_rev ON llm_models(sync_rev, id);

CREATE VIEW model_catalog AS
SELECT m.id,
       m.provider_id,
       m.model_key,
       m.context_window_tokens,
       m.input_modalities,
       m.reasoning_efforts,
       m.pricing,
       m.archived_at,
       p.archived_at AS provider_archived_at,
       (m.archived_at IS NULL AND p.archived_at IS NULL) AS available
FROM llm_models m
JOIN llm_providers p ON p.id = m.provider_id;

CREATE TABLE mcp_servers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_ts TEXT NOT NULL DEFAULT '',
    description TEXT NOT NULL,
    description_ts TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)),
    enabled_ts TEXT NOT NULL DEFAULT '',
    shared INTEGER NOT NULL DEFAULT 0 CHECK (shared IN (0,1)),
    transport TEXT NOT NULL CHECK (transport IN ('streamable-http','stdio')),
    allowed_tools TEXT CHECK (allowed_tools IS NULL OR json_type(allowed_tools)='array'),
    allowed_tools_ts TEXT NOT NULL DEFAULT '',
    config_json TEXT NOT NULL CHECK (json_type(config_json)='object'),
    timeout_ms INTEGER NOT NULL CHECK (timeout_ms BETWEEN 1 AND 86400000),
    timeout_ts TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL CHECK (revision > 0),
    config_ts TEXT NOT NULL DEFAULT '',
    auth_config_ts TEXT NOT NULL DEFAULT '',
    oauth_epoch_ts TEXT NOT NULL DEFAULT '',
    oauth_ts TEXT NOT NULL DEFAULT '',
    deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0,1)),
    sync_rev INTEGER NOT NULL DEFAULT 0 CHECK (sync_rev >= 0),
    CHECK (deleted = 1 OR name != '')
);
CREATE INDEX mcp_servers_active_name ON mcp_servers(name) WHERE deleted = 0;
CREATE INDEX mcp_servers_by_rev ON mcp_servers(sync_rev, id) WHERE shared = 1;

CREATE TABLE mcp_oauth (
    server_id TEXT PRIMARY KEY,
    generation TEXT NOT NULL,
    auth_config_ts TEXT NOT NULL DEFAULT '',
    auth_fingerprint TEXT NOT NULL DEFAULT '',
    binding TEXT,
    pending TEXT,
    credentials TEXT,
    pending_refresh TEXT CHECK (pending_refresh IS NULL OR json_valid(pending_refresh)),
    rejected INTEGER NOT NULL DEFAULT 0 CHECK (rejected IN (0,1))
);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    title_ts TEXT NOT NULL DEFAULT '',
    created_at INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle','paused')),
    last_error TEXT NOT NULL DEFAULT '',
    status_ts TEXT NOT NULL DEFAULT '',
    local_status TEXT CHECK (local_status IS NULL OR local_status IN ('running','pausing','paused')),
    local_error TEXT NOT NULL DEFAULT '',
    model_id TEXT NOT NULL,
    reasoning_effort TEXT,
    selection_ts TEXT NOT NULL DEFAULT '',
    active_head TEXT,
    active_head_ts TEXT NOT NULL DEFAULT '',
    deleted INTEGER NOT NULL DEFAULT 0 CHECK (deleted IN (0,1)),
    sync_rev INTEGER NOT NULL DEFAULT 0 CHECK (sync_rev >= 0),
    CHECK (deleted = 1 OR model_id != '')
);
CREATE INDEX sessions_active_by_updated ON sessions(updated_at DESC, id) WHERE deleted = 0;
CREATE INDEX sessions_by_rev ON sessions(sync_rev, id);

CREATE TABLE llm_calls (
    id TEXT PRIMARY KEY,
    session_id TEXT,
    model_id TEXT NOT NULL,
    reasoning_effort TEXT,
    input_end_position INTEGER,
    input_head TEXT,
    started_at INTEGER NOT NULL,
    finished_at INTEGER,
    status TEXT NOT NULL CHECK(status IN ('in_flight','succeeded','failed','cancelled','interrupted')),
    upstream_response_id TEXT,
    http_status INTEGER,
    error_summary TEXT,
    raw_usage TEXT CHECK(raw_usage IS NULL OR json_type(raw_usage)='object'),
    input_uncached_tokens INTEGER CHECK(input_uncached_tokens IS NULL OR input_uncached_tokens>=0),
    cache_read_tokens INTEGER CHECK(cache_read_tokens IS NULL OR cache_read_tokens>=0),
    cache_write_tokens INTEGER CHECK(cache_write_tokens IS NULL OR cache_write_tokens>=0),
    output_tokens INTEGER CHECK(output_tokens IS NULL OR output_tokens>=0),
    pricing_snapshot TEXT CHECK(pricing_snapshot IS NULL OR json_valid(pricing_snapshot)),
    cost_nano_usd INTEGER CHECK(cost_nano_usd IS NULL OR cost_nano_usd>=0),
    origin_node_id TEXT NOT NULL DEFAULT '',
    origin_seq INTEGER NOT NULL DEFAULT 1 CHECK (origin_seq > 0)
);
CREATE INDEX calls_by_session ON llm_calls(session_id, started_at, id);
CREATE INDEX calls_by_model ON llm_calls(model_id, started_at, id);
CREATE INDEX calls_by_origin ON llm_calls(origin_node_id, origin_seq, id);
CREATE INDEX calls_in_flight ON llm_calls(id) WHERE status = 'in_flight';

CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    mime_type TEXT NOT NULL,
    content_hash BLOB NOT NULL CHECK (length(content_hash) = 32),
    byte_len INTEGER NOT NULL CHECK (byte_len > 0),
    data BLOB
);
CREATE INDEX attachments_by_hash ON attachments(content_hash) WHERE data IS NOT NULL;
CREATE INDEX attachments_missing ON attachments(id) WHERE data IS NULL;

CREATE TABLE items (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    target_id TEXT,
    llm_call_id TEXT,
    idempotency_key TEXT,
    codemode_call_item_id TEXT,
    origin_node_id TEXT NOT NULL DEFAULT '',
    origin_seq INTEGER NOT NULL DEFAULT 1 CHECK (origin_seq > 0),
    CHECK (codemode_call_item_id IS NULL OR kind='codemode_event'
           OR (kind='function_call_output' AND target_id IS codemode_call_item_id)),
    CHECK (kind!='codemode_event' OR (codemode_call_item_id IS NOT NULL AND target_id IS NULL
           AND json_type(payload,'$.task_id') IS 'text'
           AND json_type(payload,'$.sequence') IS 'integer'
           AND coalesce(json_extract(payload,'$.event'),'') IN ('message','lease_warning','completed')
           AND coalesce(json_type(payload,'$.notify'),'') IN ('true','false')
           AND (json_extract(payload,'$.event')='completed' OR json_extract(payload,'$.notify')=1)
           AND json_type(payload,'$.content') IS 'array'))
);
CREATE INDEX items_by_session_id ON items(session_id, id);
CREATE INDEX items_by_origin ON items(session_id, origin_node_id, origin_seq, id);
CREATE INDEX items_by_target ON items(kind, target_id, id) WHERE target_id IS NOT NULL;
CREATE INDEX items_by_llm_call ON items(llm_call_id) WHERE llm_call_id IS NOT NULL;
CREATE INDEX items_by_kind ON items(session_id, kind, id);
CREATE INDEX tools_by_call ON items(session_id, json_extract(payload,'$.call_id')) WHERE kind='function_call';
CREATE INDEX items_by_idempotency ON items(session_id, idempotency_key, id) WHERE idempotency_key IS NOT NULL;
CREATE INDEX codemode_receipts ON items(codemode_call_item_id)
    WHERE kind='function_call_output' AND codemode_call_item_id IS NOT NULL;
CREATE INDEX codemode_event_sequence ON items(codemode_call_item_id, json_extract(payload,'$.sequence'))
    WHERE kind='codemode_event';
CREATE INDEX codemode_completed ON items(codemode_call_item_id)
    WHERE kind='codemode_event' AND json_extract(payload,'$.event')='completed';
CREATE INDEX codemode_warnings ON items(codemode_call_item_id, id)
    WHERE kind='codemode_event' AND json_extract(payload,'$.event')='lease_warning';
CREATE INDEX queue_by_session ON items(session_id, id) WHERE kind IN ('user_message','codemode_event');

CREATE TABLE context_nodes (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    parent_id TEXT,
    depth INTEGER NOT NULL CHECK (depth > 0),
    item_id TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    origin_seq INTEGER NOT NULL CHECK (origin_seq > 0)
);
CREATE INDEX context_nodes_by_parent ON context_nodes(session_id, parent_id);
CREATE INDEX context_nodes_by_item ON context_nodes(session_id, item_id);
CREATE INDEX context_nodes_by_origin ON context_nodes(session_id, origin_node_id, origin_seq, id);

CREATE TABLE turn_settled (
    context_node_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    origin_seq INTEGER NOT NULL CHECK (origin_seq > 0)
) WITHOUT ROWID;
CREATE INDEX turn_settled_by_origin ON turn_settled(session_id, origin_node_id, origin_seq);

CREATE TABLE invalidated_warnings (
    item_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    origin_seq INTEGER NOT NULL CHECK (origin_seq > 0)
) WITHOUT ROWID;
CREATE INDEX invalidated_warnings_by_origin ON invalidated_warnings(session_id, origin_node_id, origin_seq);

CREATE TABLE active_context (
    session_id TEXT NOT NULL,
    position INTEGER NOT NULL CHECK (position > 0),
    node_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    PRIMARY KEY (session_id, position)
) WITHOUT ROWID;
CREATE INDEX active_context_by_item ON active_context(session_id, item_id);
CREATE INDEX active_context_by_node ON active_context(session_id, node_id);

CREATE TABLE pending_inputs (
    session_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('user_message','codemode_event')),
    PRIMARY KEY (session_id, item_id)
) WITHOUT ROWID;

CREATE TABLE session_leaves (
    session_id TEXT NOT NULL,
    head_id TEXT NOT NULL,
    depth INTEGER NOT NULL CHECK (depth > 0),
    last_item_id TEXT NOT NULL,
    origin_node_id TEXT NOT NULL,
    PRIMARY KEY (session_id, head_id)
) WITHOUT ROWID;

CREATE VIEW pending AS
SELECT i.id, i.session_id
FROM pending_inputs p
JOIN items i ON i.id = p.item_id;

CREATE TABLE local_node (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    secret_key BLOB NOT NULL CHECK (length(secret_key) = 32),
    node_id TEXT NOT NULL,
    cluster_secret BLOB NOT NULL CHECK (length(cluster_secret) = 32),
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'voter' CHECK (role IN ('voter','edge')),
    db_epoch TEXT NOT NULL CHECK (db_epoch != ''),
    meta_seq INTEGER NOT NULL DEFAULT 1 CHECK (meta_seq > 0),
    membership_gen INTEGER NOT NULL DEFAULT 1 CHECK (membership_gen > 0),
    catalog_rev INTEGER NOT NULL DEFAULT 0 CHECK (catalog_rev >= 0),
    voter_epoch INTEGER NOT NULL DEFAULT 1 CHECK (voter_epoch >= 0),
    voter_set_json TEXT NOT NULL DEFAULT '[]' CHECK (json_type(voter_set_json) = 'array'),
    pending_voter_epoch INTEGER NOT NULL DEFAULT 0 CHECK (pending_voter_epoch >= 0),
    pending_voter_set_json TEXT NOT NULL DEFAULT '[]' CHECK (json_type(pending_voter_set_json) = 'array')
);

CREATE TABLE cluster_peers (
    node_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    os TEXT NOT NULL DEFAULT '',
    arch TEXT NOT NULL DEFAULT '',
    role TEXT NOT NULL DEFAULT 'voter' CHECK (role IN ('voter','edge')),
    meta_seq INTEGER NOT NULL DEFAULT 0,
    addrs_json TEXT NOT NULL DEFAULT '[]' CHECK (json_type(addrs_json) = 'array'),
    addrs_ts INTEGER NOT NULL DEFAULT 0,
    last_seen_at INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE sync_vectors (
    scope TEXT NOT NULL,
    node_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 0),
    PRIMARY KEY (scope, node_id)
) WITHOUT ROWID;

CREATE TABLE peer_catalog_cursors (
    peer_node_id TEXT PRIMARY KEY,
    remote_db_epoch TEXT NOT NULL,
    last_remote_rev INTEGER NOT NULL CHECK (last_remote_rev >= 0)
) WITHOUT ROWID;

CREATE TABLE watched_sessions (
    session_id TEXT PRIMARY KEY
) WITHOUT ROWID;

CREATE TABLE session_leases (
    session_id TEXT PRIMARY KEY,
    highest_term INTEGER NOT NULL DEFAULT 0 CHECK (highest_term >= 0),
    holder_node_id TEXT NOT NULL DEFAULT '',
    claim_id TEXT NOT NULL DEFAULT '',
    expires_at_ms INTEGER NOT NULL DEFAULT 0
);

PRAGMA user_version = 10;
