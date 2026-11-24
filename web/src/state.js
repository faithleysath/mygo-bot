export function emptyComposer() {
  return { text: "", attachments: [], pending: null, error: "" };
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

export function buildMcpPayload(f) {
  let nextAllowed = null;
  if (f.toolPolicy === "none") {
    nextAllowed = [];
  } else if (f.toolPolicy === "allowlist") {
    nextAllowed = f.allowedTools
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
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
  if (JSON.stringify(nextAllowed) !== JSON.stringify(orig.allowed_tools)) {
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
