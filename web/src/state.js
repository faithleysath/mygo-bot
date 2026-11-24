export function emptyComposer() {
  return { text: "", attachments: [], version: 0, textVersion: 0, pending: null, error: "" };
}

export function reconcileSubmittedComposer(cur, snapshot) {
  const removed = [];
  const keptAttachments = [];
  for (const a of cur.attachments || []) {
    if (snapshot.attachmentIds?.has(a.id)) removed.push(a);
    else keptAttachments.push(a);
  }
  const curVersion = cur.version ?? 0;
  const curTextVersion = cur.textVersion ?? 0;
  const clearText = curTextVersion === snapshot.textVersion;
  const nextText = clearText ? "" : cur.text;
  const nextPending = cur.pending?.key === snapshot.key ? null : cur.pending;
  const nextError = curVersion === snapshot.version ? "" : cur.error;
  return {
    next: {
      text: nextText,
      attachments: keptAttachments,
      version: curVersion + 1,
      textVersion: clearText ? curTextVersion + 1 : curTextVersion,
      pending: nextPending,
      error: nextError,
    },
    removed,
  };
}

export function buildContentBlocks(draft) {
  const blocks = [];
  if (draft.text.trim()) blocks.push({ type: "text", text: draft.text });
  for (const a of draft.attachments) {
    blocks.push({ type: a.kind, attachment_id: a.id });
  }
  return blocks;
}

export function mergeItems(prev, incoming) {
  if (!incoming || incoming.length === 0) return prev;
  const byId = new Map();
  for (const item of prev) byId.set(item.id, item);
  for (const item of incoming) byId.set(item.id, item);
  return [...byId.values()].sort((a, b) => a.id - b.id);
}

export function applyDraftStarted(prev, ev) {
  const base =
    prev && prev.llm_call_id === ev.llm_call_id
      ? prev
      : { llm_call_id: ev.llm_call_id, items: [] };
  const items = base.items.filter((x) => x.output_index !== ev.output_index);
  items.push({
    output_index: ev.output_index,
    kind: ev.kind || "message",
    name: ev.name || null,
    call_id: ev.call_id || null,
    text: "",
    reasoning: "",
    arguments: "",
  });
  items.sort((a, b) => a.output_index - b.output_index);
  return { llm_call_id: base.llm_call_id, items };
}

export function applyDraftDelta(prev, ev) {
  const base =
    prev && prev.llm_call_id === ev.llm_call_id
      ? prev
      : { llm_call_id: ev.llm_call_id, items: [] };
  const idx = base.items.findIndex((x) => x.output_index === ev.output_index);
  const items = [...base.items];
  const field = ev.field === "reasoning" || ev.field === "arguments" ? ev.field : "text";
  if (idx === -1) {
    const kind = field === "arguments" ? "function_call" : field === "reasoning" ? "reasoning" : "message";
    items.push({
      output_index: ev.output_index,
      kind,
      name: null,
      call_id: null,
      text: "",
      reasoning: "",
      arguments: "",
      [field]: ev.delta || "",
    });
    items.sort((a, b) => a.output_index - b.output_index);
  } else {
    const cur = items[idx];
    items[idx] = { ...cur, [field]: (cur[field] || "") + (ev.delta || "") };
  }
  return { llm_call_id: base.llm_call_id, items };
}

export function dropDraftForSaved(prev, item) {
  if (!prev) return { next: null, droppedIndex: null };
  let droppedIndex = null;
  const items = prev.items.filter((d) => {
    if (droppedIndex !== null) return true;
    if (item.kind === "function_call" && d.kind === "function_call" && d.call_id === item.payload?.call_id) {
      droppedIndex = d.output_index;
      return false;
    }
    if (item.kind === "assistant_message" && d.kind === "message") {
      droppedIndex = d.output_index;
      return false;
    }
    if (item.kind === "reasoning" && d.kind === "reasoning") {
      droppedIndex = d.output_index;
      return false;
    }
    return true;
  });
  return { next: items.length ? { ...prev, items } : null, droppedIndex };
}

export function createMcpForm(s = null) {
  if (!s) {
    return {
      id: null,
      orig: null,
      name: "",
      description: "",
      enabled: true,
      transport: "streamable-http",
      url: "",
      replaceUrl: true,
      headersAction: "keep",
      headersJson: "",
      oauthAction: "keep",
      oauthJson: "",
      command: "",
      argsAction: "replace",
      argsText: "",
      envAction: "keep",
      envJson: "",
      cwd: "",
      toolPolicy: "all",
      allowedTools: [],
      newAllowedTool: "",
      timeout_ms: 60000,
    };
  }
  return {
    id: s.id,
    orig: s,
    name: s.name,
    description: s.description,
    enabled: s.enabled,
    transport: s.transport,
    url: s.url || "",
    replaceUrl: false,
    headersAction: "keep",
    headersJson: "",
    oauthAction: "keep",
    oauthJson: "",
    command: s.command || "",
    argsAction: "keep",
    argsText: "",
    envAction: "keep",
    envJson: "",
    cwd: s.cwd || "",
    toolPolicy:
      s.allowed_tools === null
        ? "all"
        : s.allowed_tools.length === 0
          ? "none"
          : "allowlist",
    allowedTools: [...(s.allowed_tools || [])],
    newAllowedTool: "",
    timeout_ms: s.timeout_ms,
  };
}

function sameAllowedTools(a, b) {
  if (a === null || b === null) return a === b;
  if (a.length !== b.length) return false;
  const setB = new Set(b);
  return a.every((x) => setB.has(x));
}

export function buildMcpPayload(f) {
  let nextAllowed = null;
  if (f.toolPolicy === "none") {
    nextAllowed = [];
  } else if (f.toolPolicy === "allowlist") {
    const raw = Array.isArray(f.allowedTools) ? [...f.allowedTools] : [];
    if (typeof f.newAllowedTool === "string" && f.newAllowedTool.trim()) {
      raw.push(f.newAllowedTool.trim());
    }
    nextAllowed = Array.from(new Set(raw.map((x) => String(x).trim()).filter(Boolean)));
    if (nextAllowed.length === 0) {
      throw new Error("Allowlist mode requires at least one tool name (or select Deny all tools).");
    }
  }

  const timeoutNum = Number(f.timeout_ms);
  if (!Number.isInteger(timeoutNum) || timeoutNum < 1) {
    throw new Error("Timeout must be a positive integer.");
  }

  if (!f.id) {
    const body = {
      name: f.name,
      description: f.description,
      enabled: f.enabled,
      transport: f.transport,
      timeout_ms: timeoutNum,
      allowed_tools: nextAllowed,
    };
    if (f.transport === "streamable-http") {
      body.url = f.url;
      if (f.headersJson.trim()) body.headers = JSON.parse(f.headersJson);
      if (f.oauthAction === "replace") {
        body.oauth = f.oauthJson.trim() ? JSON.parse(f.oauthJson) : {};
      }
    } else {
      body.command = f.command;
      body.args = f.argsText.trim() ? f.argsText.split("\n").filter(Boolean) : [];
      if (f.envJson.trim()) body.env = JSON.parse(f.envJson);
      if (f.cwd.trim()) body.cwd = f.cwd.trim();
    }
    return body;
  }

  const orig = f.orig;
  const body = {};
  if (f.description !== orig.description) body.description = f.description;
  if (f.enabled !== orig.enabled) body.enabled = f.enabled;
  if (timeoutNum !== orig.timeout_ms) body.timeout_ms = timeoutNum;
  if (!sameAllowedTools(nextAllowed, orig.allowed_tools)) {
    body.allowed_tools = nextAllowed;
  }

  const switched = f.transport !== orig.transport;
  if (switched) body.transport = f.transport;

  if (f.transport === "streamable-http") {
    if (
      switched ||
      (!orig.has_url_query && f.url !== (orig.url || "")) ||
      (orig.has_url_query && f.replaceUrl)
    ) {
      body.url = f.url;
    }
    if (f.headersAction === "replace") {
      body.headers = f.headersJson.trim() ? JSON.parse(f.headersJson) : {};
    } else if (f.headersAction === "clear") {
      body.headers = {};
    }
    if (f.oauthAction === "replace") {
      body.oauth = f.oauthJson.trim() ? JSON.parse(f.oauthJson) : {};
    } else if (f.oauthAction === "disable") {
      body.oauth = null;
    }
  } else {
    if (switched || f.command !== (orig.command || "")) body.command = f.command;
    const nextCwd = f.cwd.trim() || null;
    if (switched || nextCwd !== (orig.cwd || null)) body.cwd = nextCwd;
    if (f.argsAction === "replace") {
      body.args = f.argsText.trim() ? f.argsText.split("\n").filter(Boolean) : [];
    } else if (f.argsAction === "clear") {
      body.args = [];
    }
    if (f.envAction === "replace") {
      body.env = f.envJson.trim() ? JSON.parse(f.envJson) : {};
    } else if (f.envAction === "clear") {
      body.env = {};
    }
  }
  return body;
}

export function applySessionEvent(list, name, data) {
  if (name === "session.created") {
    return list.some((s) => s.id === data.id) ? list : [...list, data];
  }
  if (name === "session.updated") {
    return list.map((s) => (s.id === data.id ? data : s));
  }
  if (name === "session.status") {
    return list.map((s) =>
      s.id === data.id
        ? {
            ...s,
            status: data.status,
            last_error: data.reason || "",
            updated_at: data.updated_at ?? s.updated_at,
          }
        : s
    );
  }
  if (name === "session.deleted") {
    return list.filter((s) => s.id !== data.id);
  }
  return list;
}

export function applyLocalPatch(list, updated) {
  return list.map((s) => {
    if (s.id !== updated.id) return s;
    if (
      typeof s.updated_at === "number" &&
      typeof updated.updated_at === "number" &&
      updated.updated_at < s.updated_at
    ) {
      return s;
    }
    return updated;
  });
}

export function createGlobalSessionSync({
  fetchSessions,
  onSnapshot,
  onDelta,
  onLocalPatchApplied,
  onDeleted,
  onError,
  schedule = setTimeout,
  cancelSchedule = clearTimeout,
}) {
  let disposed = false;
  let latestSeq = 0;
  let activeBuffer = null;
  let retryTimer = null;
  let retryDelay = 1000;
  let versionCounter = 0;
  let snapshotVersion = 0;
  const sessionVersions = new Map();
  const deletedIds = new Set();

  const getSessionVersion = (id) => Math.max(snapshotVersion, sessionVersions.get(id) || 0);
  const touchSession = (id) => {
    if (id != null) sessionVersions.set(id, ++versionCounter);
  };

  const clearRetry = () => {
    if (retryTimer !== null) {
      cancelSchedule(retryTimer);
      retryTimer = null;
    }
  };

  const sync = async () => {
    if (disposed) return;
    clearRetry();
    const mySeq = ++latestSeq;
    activeBuffer = { seq: mySeq, events: [] };
    try {
      const list = await fetchSessions();
      if (disposed || mySeq !== latestSeq) return;
      const buffered = activeBuffer?.seq === mySeq ? activeBuffer.events : [];
      activeBuffer = null;
      retryDelay = 1000;
      snapshotVersion = ++versionCounter;
      sessionVersions.clear();
      let merged = list;
      for (const ev of buffered) {
        if (ev.name === "session.deleted") deletedIds.add(ev.data.id);
        merged = applySessionEvent(merged, ev.name, ev.data);
      }
      onError?.("");
      onSnapshot(merged);
    } catch (err) {
      if (disposed || mySeq !== latestSeq) return;
      activeBuffer = null;
      onError?.(err?.message || String(err));
      const delay = retryDelay;
      retryDelay = Math.min(retryDelay * 2, 8000);
      retryTimer = schedule(() => {
        retryTimer = null;
        sync();
      }, delay);
    }
  };

  const onEvent = (name, data) => {
    if (disposed) return;
    touchSession(data?.id);
    if (name === "session.deleted" && data?.id != null) {
      deletedIds.add(data.id);
    }
    if (activeBuffer !== null) {
      activeBuffer.events.push({ name, data });
    }
    if (name === "session.deleted") {
      onDeleted?.(data.id);
    }
    onDelta(name, data);
  };

  const onLocalCreated = (session) => {
    if (disposed || deletedIds.has(session.id)) return false;
    onEvent("session.created", session);
    return true;
  };

  const onLocalPatch = (updated, baseVersion) => {
    if (disposed || deletedIds.has(updated.id)) return false;
    if (getSessionVersion(updated.id) !== baseVersion) return false;
    touchSession(updated.id);
    if (onLocalPatchApplied) onLocalPatchApplied(updated);
    else onDelta("session.updated", updated);
    return true;
  };

  const dispose = () => {
    disposed = true;
    clearRetry();
    activeBuffer = null;
  };

  return {
    sync,
    onEvent,
    onLocalCreated,
    onLocalPatch,
    getSessionVersion,
    touchSession,
    dispose,
  };
}

export function createSessionSync({
  fetchInitial,
  fetchAfter,
  onItems,
  onHasMore,
  onSidecars,
  onError,
  schedule = setTimeout,
  cancelSchedule = clearTimeout,
}) {
  let disposed = false;
  let initialLoaded = false;
  let syncedThroughId = 0;
  let gapOpen = false;
  let inFlight = false;
  let runAgain = false;
  let retryTimer = null;
  let retryDelay = 1000;

  const clearRetry = () => {
    if (retryTimer !== null) {
      cancelSchedule(retryTimer);
      retryTimer = null;
    }
  };

  const triggerSidecars = () => {
    if (!onSidecars) return;
    Promise.resolve()
      .then(() => onSidecars())
      .catch(() => {});
  };

  const runSync = async () => {
    if (disposed) return;
    if (inFlight) {
      runAgain = true;
      return;
    }
    clearRetry();
    inFlight = true;
    let shouldScheduleRetry = false;
    try {
      if (!initialLoaded) {
        const hist = await fetchInitial();
        if (disposed) return;
        onItems(hist);
        onHasMore(hist.length === 50);
        syncedThroughId = hist.length ? hist[hist.length - 1].id : 0;
        initialLoaded = true;
        if (!runAgain) gapOpen = false;
        retryDelay = 1000;
        onError?.("");
        triggerSidecars();
      } else {
        while (!disposed) {
          const page = await fetchAfter(syncedThroughId);
          if (disposed) return;
          if (page.length) {
            onItems(page);
            syncedThroughId = Math.max(syncedThroughId, page[page.length - 1].id);
          }
          if (page.length < 200) break;
        }
        if (!disposed && !runAgain) {
          gapOpen = false;
          retryDelay = 1000;
          onError?.("");
        }
        if (!disposed) triggerSidecars();
      }
    } catch (e) {
      if (disposed) return;
      if (initialLoaded) gapOpen = true;
      onError?.(e?.message || String(e));
      shouldScheduleRetry = !runAgain;
    } finally {
      inFlight = false;
      if (runAgain && !disposed) {
        runAgain = false;
        runSync();
      } else if (shouldScheduleRetry && !disposed) {
        const delay = retryDelay;
        retryDelay = Math.min(retryDelay * 2, 8000);
        retryTimer = schedule(() => {
          retryTimer = null;
          runSync();
        }, delay);
      }
    }
  };

  const onReady = () => {
    if (disposed) return;
    gapOpen = true;
    retryDelay = 1000;
    clearRetry();
    runSync();
  };

  const onResync = () => {
    if (disposed) return;
    gapOpen = true;
    retryDelay = 1000;
    clearRetry();
    runSync();
  };

  const onLiveItem = (item) => {
    if (disposed) return;
    onItems([item]);
    if (initialLoaded && !gapOpen && typeof item?.id === "number") {
      syncedThroughId = Math.max(syncedThroughId, item.id);
    }
  };

  const dispose = () => {
    disposed = true;
    clearRetry();
  };

  const getState = () => ({ initialLoaded, syncedThroughId, gapOpen });

  return { onReady, onResync, onLiveItem, dispose, getState };
}
