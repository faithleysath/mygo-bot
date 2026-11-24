import { errorMessage } from "./api.ts";
import type {
  ActiveDraft,
  ComposerAttachment,
  ComposerDraft,
  ContentBlock,
  DraftDeltaEvent,
  DraftStartedEvent,
  HistoryItem,
  McpFormState,
  McpServerInfo,
  SessionDeletedEvent,
  SessionInfo,
  SessionStatusEvent,
  SubmittedComposerSnapshot,
} from "./types.ts";

export function emptyComposer(): ComposerDraft {
  return { text: "", attachments: [], version: 0, textVersion: 0, pending: null, error: "" };
}

export function reconcileSubmittedComposer(
  cur: ComposerDraft,
  snapshot: SubmittedComposerSnapshot
): { next: ComposerDraft; removed: ComposerAttachment[] } {
  const removed: ComposerAttachment[] = [];
  const keptAttachments: ComposerAttachment[] = [];
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

export function buildContentBlocks(draft: ComposerDraft): ContentBlock[] {
  const blocks: ContentBlock[] = [];
  if (draft.text.trim()) blocks.push({ type: "text", text: draft.text });
  for (const a of draft.attachments) {
    blocks.push({ type: a.kind, attachment_id: a.id });
  }
  return blocks;
}

function compareIds(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

export function mergeItems(prev: HistoryItem[], incoming: HistoryItem[]): HistoryItem[] {
  if (!incoming || incoming.length === 0) return prev;
  const byId = new Map<string, HistoryItem>();
  for (const item of prev) byId.set(item.id, item);
  for (const item of incoming) byId.set(item.id, item);
  return [...byId.values()].sort((a, b) => compareIds(a.id, b.id));
}

export function applyDraftStarted(prev: ActiveDraft | null, ev: DraftStartedEvent): ActiveDraft {
  const base: ActiveDraft =
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

export function applyDraftDelta(prev: ActiveDraft | null, ev: DraftDeltaEvent): ActiveDraft {
  const base: ActiveDraft =
    prev && prev.llm_call_id === ev.llm_call_id
      ? prev
      : { llm_call_id: ev.llm_call_id, items: [] };
  const idx = base.items.findIndex((x) => x.output_index === ev.output_index);
  const items = [...base.items];
  const field: "text" | "reasoning" | "arguments" =
    ev.field === "reasoning" || ev.field === "arguments" ? ev.field : "text";
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

export function dropDraftForSaved(
  prev: ActiveDraft | null,
  item: HistoryItem
): { next: ActiveDraft | null; droppedIndex: number | null } {
  if (!prev) return { next: null, droppedIndex: null };
  let droppedIndex: number | null = null;
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

export function createMcpForm(s: McpServerInfo | null = null): McpFormState {
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

function sameAllowedTools(a: string[] | null, b: string[] | null): boolean {
  if (a === null || b === null) return a === b;
  if (a.length !== b.length) return false;
  const setB = new Set(b);
  return a.every((x) => setB.has(x));
}

export function buildMcpPayload(f: McpFormState): Record<string, unknown> {
  let nextAllowed: string[] | null = null;
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

  if (!f.id || !f.orig) {
    const body: Record<string, unknown> = {
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
  const body: Record<string, unknown> = {};
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

export type SessionDeltaPayload = SessionInfo | SessionStatusEvent | SessionDeletedEvent;

export function applySessionEvent(
  list: SessionInfo[],
  name: string,
  data: SessionDeltaPayload
): SessionInfo[] {
  if (name === "session.created") {
    const created = data as SessionInfo;
    return list.some((s) => s.id === created.id) ? list : [...list, created];
  }
  if (name === "session.updated") {
    const updated = data as SessionInfo;
    return list.map((s) => (s.id === updated.id ? updated : s));
  }
  if (name === "session.status") {
    const st = data as SessionStatusEvent;
    return list.map((s) =>
      s.id === st.id
        ? {
            ...s,
            status: st.status,
            last_error: st.reason || "",
            updated_at: st.updated_at ?? s.updated_at,
          }
        : s
    );
  }
  if (name === "session.deleted") {
    const del = data as SessionDeletedEvent;
    return list.filter((s) => s.id !== del.id);
  }
  return list;
}

export function applyLocalPatch(list: SessionInfo[], updated: SessionInfo): SessionInfo[] {
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

function sameSession(a: SessionInfo, b: SessionInfo): boolean {
  return (
    a.id === b.id &&
    a.title === b.title &&
    a.status === b.status &&
    a.model_id === b.model_id &&
    a.reasoning_effort === b.reasoning_effort &&
    a.last_error === b.last_error &&
    a.updated_at === b.updated_at
  );
}

type TimerId = ReturnType<typeof setTimeout>;
type ScheduleFn = (fn: () => void, ms: number) => TimerId;
type CancelScheduleFn = (id: TimerId) => void;

type BufferedSessionEvent =
  | { kind: "event"; name: string; data: SessionDeltaPayload }
  | { kind: "local_patch"; data: SessionInfo };

export interface GlobalSessionSyncOptions {
  fetchSessions: () => Promise<SessionInfo[]>;
  fetchSession?: (id: string) => Promise<SessionInfo>;
  getSession?: (id: string) => SessionInfo | undefined;
  onSnapshot: (merged: SessionInfo[]) => void;
  onDelta: (name: string, data: SessionDeltaPayload) => void;
  onLocalPatchApplied?: (updated: SessionInfo) => void;
  onDeleted?: (id: string) => void;
  onError?: (msg: string) => void;
  schedule?: ScheduleFn;
  cancelSchedule?: CancelScheduleFn;
}

export interface GlobalSessionSync {
  sync: () => Promise<void>;
  onEvent: {
    (name: "session.created" | "session.updated", data: SessionInfo): void;
    (name: "session.status", data: SessionStatusEvent): void;
    (name: "session.deleted", data: SessionDeletedEvent): void;
    (name: string, data: SessionDeltaPayload): void;
  };
  onLocalCreated: (session: SessionInfo) => boolean;
  onLocalPatch: (updated: SessionInfo, baseVersion: number) => boolean;
  refreshSession: (id: string) => Promise<void>;
  getSessionVersion: (id: string) => number;
  touchSession: (id: string | null | undefined) => void;
  dispose: () => void;
}

export function createGlobalSessionSync({
  fetchSessions,
  fetchSession,
  getSession,
  onSnapshot,
  onDelta,
  onLocalPatchApplied,
  onDeleted,
  onError,
  schedule = setTimeout,
  cancelSchedule = clearTimeout,
}: GlobalSessionSyncOptions): GlobalSessionSync {
  let disposed = false;
  let latestSeq = 0;
  let activeBuffer: { seq: number; events: BufferedSessionEvent[] } | null = null;
  let retryTimer: TimerId | null = null;
  let retryDelay = 1000;
  let versionCounter = 0;
  let snapshotVersion = 0;
  const sessionVersions = new Map<string, number>();
  const deletedIds = new Set<string>();
  const refreshSeqById = new Map<string, number>();
  const refreshRetryTimers = new Map<string, TimerId>();
  const refreshRetryDelays = new Map<string, number>();

  const clearRefreshRetry = (id: string): void => {
    const timer = refreshRetryTimers.get(id);
    if (timer !== undefined) {
      cancelSchedule(timer);
      refreshRetryTimers.delete(id);
    }
    refreshRetryDelays.delete(id);
  };

  const clearAllRefreshRetries = (): void => {
    for (const timer of refreshRetryTimers.values()) {
      cancelSchedule(timer);
    }
    refreshRetryTimers.clear();
    refreshRetryDelays.clear();
  };

  const getSessionVersion = (id: string): number =>
    Math.max(snapshotVersion, sessionVersions.get(id) || 0);

  const touchSession = (id: string | null | undefined): void => {
    if (id != null) {
      sessionVersions.set(id, ++versionCounter);
      clearRefreshRetry(id);
    }
  };

  const clearRetry = (): void => {
    if (retryTimer !== null) {
      cancelSchedule(retryTimer);
      retryTimer = null;
    }
  };

  const sync = async (): Promise<void> => {
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
      clearAllRefreshRetries();
      let merged = list;
      for (const ev of buffered) {
        if (ev.kind === "local_patch") {
          merged = applyLocalPatch(merged, ev.data);
        } else {
          if (ev.name === "session.deleted") deletedIds.add((ev.data as SessionDeletedEvent).id);
          merged = applySessionEvent(merged, ev.name, ev.data);
        }
      }
      onError?.("");
      onSnapshot(merged);
    } catch (err) {
      if (disposed || mySeq !== latestSeq) return;
      activeBuffer = null;
      onError?.(errorMessage(err));
      const delay = retryDelay;
      retryDelay = Math.min(retryDelay * 2, 8000);
      retryTimer = schedule(() => {
        retryTimer = null;
        void sync();
      }, delay);
    }
  };

  const onEvent = (name: string, data: SessionDeltaPayload): void => {
    if (disposed) return;
    const id = data?.id;
    touchSession(id);
    if (name === "session.deleted" && id != null) {
      deletedIds.add(id);
    }
    if (activeBuffer !== null) {
      activeBuffer.events.push({ kind: "event", name, data });
    }
    if (name === "session.deleted" && id != null) {
      onDeleted?.(id);
    }
    onDelta(name, data);
  };

  const runRefresh = async (id: string, isRetry: boolean): Promise<void> => {
    if (disposed || !fetchSession || deletedIds.has(id)) return;
    if (getSession && !getSession(id)) return;
    const prevTimer = refreshRetryTimers.get(id);
    if (prevTimer !== undefined) {
      cancelSchedule(prevTimer);
      refreshRetryTimers.delete(id);
    }
    if (!isRetry) refreshRetryDelays.delete(id);
    const myRefreshSeq = (refreshSeqById.get(id) ?? 0) + 1;
    refreshSeqById.set(id, myRefreshSeq);
    const startVersion = getSessionVersion(id);
    try {
      const fresh = await fetchSession(id);
      if (disposed || deletedIds.has(id)) return;
      if (refreshSeqById.get(id) !== myRefreshSeq) return;
      if (getSessionVersion(id) !== startVersion) return;
      if (getSession && !getSession(id)) return;
      touchSession(id);
      if (activeBuffer !== null) {
        activeBuffer.events.push({ kind: "local_patch", data: fresh });
      }
      if (onLocalPatchApplied) onLocalPatchApplied(fresh);
      else onDelta("session.updated", fresh);
    } catch (err) {
      if (disposed || refreshSeqById.get(id) !== myRefreshSeq) return;
      const status = (err as { status?: number })?.status;
      if (status === 404) {
        clearRefreshRetry(id);
        onEvent("session.deleted", { id });
        return;
      }
      const retryable = typeof status !== "number" || status >= 500;
      if (
        !retryable ||
        deletedIds.has(id) ||
        getSessionVersion(id) !== startVersion ||
        (getSession && !getSession(id))
      ) {
        clearRefreshRetry(id);
        return;
      }
      const delay = refreshRetryDelays.get(id) ?? 1000;
      refreshRetryDelays.set(id, Math.min(delay * 2, 8000));
      const timer = schedule(() => {
        refreshRetryTimers.delete(id);
        if (disposed || deletedIds.has(id)) return;
        if (refreshSeqById.get(id) !== myRefreshSeq) return;
        if (getSessionVersion(id) !== startVersion) return;
        if (getSession && !getSession(id)) return;
        void runRefresh(id, true);
      }, delay);
      refreshRetryTimers.set(id, timer);
    }
  };

  const refreshSession = (id: string): Promise<void> => runRefresh(id, false);

  const onLocalCreated = (session: SessionInfo): boolean => {
    if (disposed || deletedIds.has(session.id)) return false;
    onEvent("session.created", session);
    return true;
  };

  const onLocalPatch = (updated: SessionInfo, baseVersion: number): boolean => {
    if (disposed || deletedIds.has(updated.id)) return false;
    const existing = getSession?.(updated.id);
    if (getSession && !existing) return false;
    if (getSessionVersion(updated.id) === baseVersion) {
      touchSession(updated.id);
      if (activeBuffer !== null) {
        activeBuffer.events.push({ kind: "local_patch", data: updated });
      }
      if (onLocalPatchApplied) onLocalPatchApplied(updated);
      else onDelta("session.updated", updated);
      return true;
    }
    if (existing && sameSession(existing, updated)) return false;
    void refreshSession(updated.id);
    return false;
  };

  const dispose = (): void => {
    disposed = true;
    clearRetry();
    clearAllRefreshRetries();
    activeBuffer = null;
  };

  return {
    sync,
    onEvent,
    onLocalCreated,
    onLocalPatch,
    refreshSession,
    getSessionVersion,
    touchSession,
    dispose,
  };
}

export interface SessionSyncOptions {
  fetchInitial: () => Promise<HistoryItem[]>;
  fetchAfter: (afterId: string) => Promise<HistoryItem[]>;
  onItems: (items: HistoryItem[]) => void;
  onHasMore: (hasMore: boolean) => void;
  onSidecars?: () => Promise<void> | void;
  onError?: (msg: string) => void;
  schedule?: ScheduleFn;
  cancelSchedule?: CancelScheduleFn;
}

export interface SessionSync {
  start: () => void;
  onReady: () => void;
  onResync: () => void;
  onLiveItem: (item: HistoryItem) => void;
  dispose: () => void;
  getState: () => { initialLoaded: boolean; syncedThroughId: string; gapOpen: boolean };
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
}: SessionSyncOptions): SessionSync {
  let disposed = false;
  let initialLoaded = false;
  let syncedThroughId = "";
  let gapOpen = false;
  let inFlight = false;
  let runAgain = false;
  let retryTimer: TimerId | null = null;
  let retryDelay = 1000;

  const clearRetry = (): void => {
    if (retryTimer !== null) {
      cancelSchedule(retryTimer);
      retryTimer = null;
    }
  };

  const triggerSidecars = (): void => {
    if (!onSidecars) return;
    Promise.resolve()
      .then(() => onSidecars())
      .catch(() => {});
  };

  const runSync = async (): Promise<void> => {
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
        syncedThroughId = hist.length ? hist[hist.length - 1].id : "";
        initialLoaded = true;
        if (!runAgain) gapOpen = false;
        retryDelay = 1000;
        onError?.("");
        if (!runAgain) triggerSidecars();
      } else {
        while (!disposed) {
          const page = await fetchAfter(syncedThroughId);
          if (disposed) return;
          if (page.length) {
            onItems(page);
            const last = page[page.length - 1].id;
            if (last > syncedThroughId) syncedThroughId = last;
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
      onError?.(errorMessage(e));
      shouldScheduleRetry = !runAgain;
    } finally {
      inFlight = false;
      if (runAgain && !disposed) {
        runAgain = false;
        void runSync();
      } else if (shouldScheduleRetry && !disposed) {
        const delay = retryDelay;
        retryDelay = Math.min(retryDelay * 2, 8000);
        retryTimer = schedule(() => {
          retryTimer = null;
          void runSync();
        }, delay);
      }
    }
  };

  const start = (): void => {
    if (disposed || initialLoaded || inFlight) return;
    retryDelay = 1000;
    clearRetry();
    void runSync();
  };

  const onReady = (): void => {
    if (disposed) return;
    gapOpen = true;
    retryDelay = 1000;
    clearRetry();
    void runSync();
  };

  const onResync = (): void => {
    if (disposed) return;
    gapOpen = true;
    retryDelay = 1000;
    clearRetry();
    void runSync();
  };

  const onLiveItem = (item: HistoryItem): void => {
    if (disposed) return;
    onItems([item]);
    if (initialLoaded && !gapOpen && typeof item?.id === "string" && item.id > syncedThroughId) {
      syncedThroughId = item.id;
    }
  };

  const dispose = (): void => {
    disposed = true;
    clearRetry();
  };

  const getState = () => ({ initialLoaded, syncedThroughId, gapOpen });

  return { start, onReady, onResync, onLiveItem, dispose, getState };
}
