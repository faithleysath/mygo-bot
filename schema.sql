CREATE TABLE config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    base_url TEXT NOT NULL,
    api_key TEXT NOT NULL,
    model TEXT NOT NULL,
    instructions TEXT NOT NULL,
    default_reasoning_effort TEXT CHECK (default_reasoning_effort IN ('low','high','max'))
);
CREATE TABLE sessions (
    id INTEGER PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'idle' CHECK (status IN ('idle','running','pausing','paused')),
    reasoning_effort TEXT CHECK (reasoning_effort IN ('low','high','max')),
    user_queue_cursor INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);
CREATE TABLE items (
    id INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL REFERENCES sessions,
    kind TEXT NOT NULL,
    payload TEXT NOT NULL CHECK (json_valid(payload)),
    -- 工具结果指向原调用，撤回指向原消息，回合结果指向本轮最后纳入的用户消息。
    target_id INTEGER,
    idempotency_key TEXT,
    UNIQUE (session_id, id),
    UNIQUE (kind, target_id),
    FOREIGN KEY (session_id, target_id) REFERENCES items(session_id, id)
);
CREATE INDEX items_by_kind ON items(session_id, kind, id);
CREATE UNIQUE INDEX items_by_idempotency ON items(session_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE TABLE context_items (
    position INTEGER PRIMARY KEY,
    session_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL UNIQUE,
    FOREIGN KEY (session_id, item_id) REFERENCES items(session_id, id)
);
CREATE INDEX context_by_session ON context_items(session_id, position);
