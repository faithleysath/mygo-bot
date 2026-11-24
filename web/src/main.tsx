import type { TargetedSubmitEvent } from "preact";
import { render } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import {
  ApiError,
  apiFetch,
  connectSse,
  consumeHashToken,
  errorMessage,
  getToken,
  makeIdempotencyKey,
  setToken,
  setUnauthorizedHandler,
} from "./api.ts";
import { Composer } from "./Composer.tsx";
import { I18nContext, readLocalePref, resolveLocale, saveLocalePref, statusText, tr, useI18n } from "./i18n.ts";
import type { LocalePref, Msg } from "./i18n.ts";
import { BrandMark, Icon } from "./Icons.tsx";
import { ContextView, TraceView } from "./Inspector.tsx";
import { Settings } from "./Settings.tsx";
import type { SettingsTabId } from "./Settings.tsx";
import { Sidebar } from "./Sidebar.tsx";
import {
  applyDraftDelta,
  applyDraftStarted,
  applyLocalPatch,
  applySessionEvent,
  buildContentBlocks,
  createGlobalSessionSync,
  createSessionSync,
  dropDraftForSaved,
  emptyComposer,
  mergeItems,
  reconcileSubmittedComposer,
} from "./state.ts";
import type { GlobalSessionSync } from "./state.ts";
import { Timeline } from "./Timeline.tsx";
import type {
  ActiveDraft,
  ActiveTask,
  AppConfig,
  CallsSummary,
  ComposerAttachment,
  ComposerDraft,
  DraftDeltaEvent,
  DraftStartedEvent,
  HistoryItem,
  LlmCallInfo,
  MainTab,
  MediaKind,
  ModelInfo,
  ProviderInfo,
  RetryNotice,
  SessionDeletedEvent,
  SessionInfo,
  SessionStatusEvent,
  SseStatus,
  SubmittedComposerSnapshot,
  ThemePref,
} from "./types.ts";

function readThemePref(): ThemePref {
  try {
    const v = localStorage.getItem("mygo_theme");
    if (v === "light" || v === "dark") return v;
  } catch {}
  return "system";
}

function resolveTheme(pref: ThemePref): "light" | "dark" {
  if (pref === "light" || pref === "dark") return pref;
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function TokenGate({ onSuccess }: { onSuccess: () => void }) {
  const { t } = useI18n();
  const [value, setValue] = useState("");
  const [show, setShow] = useState(false);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const submit = async (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const token = value.trim();
    if (!token) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch<AppConfig>("/config", { token });
      setToken(token);
      onSuccess();
    } catch (ex) {
      setErr(errorMessage(ex) || t("Invalid Bearer token"));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="gate-wrap">
      <form class="gate-card" onSubmit={submit}>
        <div class="brand">
          <BrandMark size={26} />
          <span class="brand-name">MyGo</span>
          <span class="brand-tag">BOT</span>
        </div>
        <p class="muted">{t("Enter the daemon TCP Bearer token to connect.")}</p>
        {err && <div class="err-inline">{err}</div>}
        <div class="token-input-wrap">
          {show ? (
            <input
              key="tok"
              type="text"
              placeholder={t("Bearer token")}
              autoFocus
              value={value}
              onInput={(e) => setValue(e.currentTarget.value)}
            />
          ) : (
            <input
              key="tok"
              type="password"
              placeholder={t("Bearer token")}
              autoFocus
              value={value}
              onInput={(e) => setValue(e.currentTarget.value)}
            />
          )}
          <button
            type="button"
            class="btn-icon"
            title={show ? t("Hide token") : t("Show token")}
            aria-label={show ? t("Hide token") : t("Show token")}
            onClick={() => setShow((v) => !v)}
          >
            <Icon n={show ? "eyeOff" : "eye"} />
          </button>
        </div>
        <button type="submit" class="btn-primary" disabled={busy || !value.trim()}>
          {busy ? t("Verifying…") : t("Connect")}
        </button>
        <div class="muted small">
          {t("Run")} <code class="md-inline-code">mygo service status</code> {t("on the host to view the token.")}
        </div>
      </form>
    </div>
  );
}

function TopBar({
  session,
  sseStatus,
  sidebarCollapsed,
  activeTab,
  itemCount,
  onSelectTab,
  onOpenSidebar,
  onOpenSettings,
  onPatchSession,
}: {
  session: SessionInfo | null;
  sseStatus: SseStatus;
  sidebarCollapsed: boolean;
  activeTab: MainTab;
  itemCount: number;
  onSelectTab: (tab: MainTab) => void;
  onOpenSidebar: () => void;
  onOpenSettings: () => void;
  onPatchSession: (sid: string, patch: Partial<Pick<SessionInfo, "title">>) => void;
}) {
  const { locale, t } = useI18n();
  const [title, setTitle] = useState(session?.title || "");
  useEffect(() => {
    setTitle(session?.title || "");
  }, [session?.id, session?.title]);

  if (!session) return null;

  const commitTitle = () => {
    const next = title.trim();
    if (next !== session.title) {
      onPatchSession(session.id, { title: next });
    } else {
      setTitle(session.title);
    }
  };

  return (
    <header class="topbar-shell">
      <div class="topbar-row">
        <div class="topbar-left">
          <button
            type="button"
            class={`btn-icon ${sidebarCollapsed ? "" : "mobile-only"}`}
            title={t("Show sessions")}
            aria-label={t("Show sessions")}
            onClick={onOpenSidebar}
          >
            <Icon n="sidebar" s={16} />
          </button>
          <input
            class="title-input"
            placeholder={`${t("Session")} ${session.id}`}
            value={title}
            onInput={(e) => setTitle(e.currentTarget.value)}
            onBlur={commitTitle}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.isComposing) e.currentTarget.blur();
            }}
          />
          <span class={`pill pill-${session.status}`}>
            {(session.status === "running" || session.status === "pausing") && <span class="pulse-dot" />}
            {statusText(session.status, locale)}
          </span>
          {sseStatus === "reconnecting" && (
            <span class="pill pill-paused">{t("reconnecting")}</span>
          )}
        </div>

        <div class="topbar-right">
          <button
            type="button"
            class="btn-icon"
            title={t("Settings")}
            aria-label={t("Settings")}
            onClick={onOpenSettings}
          >
            <Icon n="settings" />
          </button>
        </div>
      </div>

      <nav class="view-tabs">
        <button
          type="button"
          class={`view-tab ${activeTab === "chat" ? "active" : ""}`}
          onClick={() => onSelectTab("chat")}
        >
          <Icon n="chat" s={14} />
          <span>{t("Chat")}</span>
        </button>
        <button
          type="button"
          class={`view-tab ${activeTab === "trace" ? "active" : ""}`}
          onClick={() => onSelectTab("trace")}
        >
          <Icon n="activity" s={14} />
          <span>{t("Trace")}</span>
          {itemCount > 0 && <span class="tab-count mono">{itemCount}</span>}
        </button>
        <button
          type="button"
          class={`view-tab ${activeTab === "context" ? "active" : ""}`}
          onClick={() => onSelectTab("context")}
        >
          <Icon n="layers" s={14} />
          <span>{t("Context")}</span>
        </button>
      </nav>
    </header>
  );
}

function revokeAttachments(attachments: ComposerAttachment[]): void {
  for (const a of attachments || []) {
    if (a.preview) URL.revokeObjectURL(a.preview);
  }
}

function App() {
  const [authed, setAuthed] = useState<boolean | null>(null);
  const [themePref, setThemePrefState] = useState<ThemePref>(readThemePref);
  const [localePref, setLocalePrefState] = useState<LocalePref>(readLocalePref);
  const locale = useMemo(() => resolveLocale(localePref), [localePref]);
  const t = (s: Msg) => tr(locale, s);

  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(() => {
    try {
      return localStorage.getItem("mygo_sidebar_collapsed") === "1";
    } catch {
      return false;
    }
  });
  const [settingsState, setSettingsState] = useState<{
    open: boolean;
    tab: SettingsTabId;
    sessionFilter: string;
  }>({ open: false, tab: "models", sessionFilter: "" });

  const [config, setConfig] = useState<AppConfig | null>(null);
  const [providers, setProviders] = useState<ProviderInfo[]>([]);
  const [models, setModels] = useState<ModelInfo[]>([]);
  const [sessions, setSessions] = useState<SessionInfo[]>([]);
  const [activeId, setActiveId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<MainTab>("chat");

  const [items, setItems] = useState<HistoryItem[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [queuedIds, setQueuedIds] = useState<Set<string>>(() => new Set());
  const [draft, setDraft] = useState<ActiveDraft | null>(null);
  const [retryNotice, setRetryNotice] = useState<RetryNotice | null>(null);
  const [tasks, setTasks] = useState<ActiveTask[]>([]);
  const [summary, setSummary] = useState<CallsSummary | null>(null);
  const [callsById, setCallsById] = useState<Map<string, LlmCallInfo>>(() => new Map());
  const [contextRefreshNonce, setContextRefreshNonce] = useState(0);
  const [sseStatus, setSseStatus] = useState<SseStatus>("open");
  const [bannerErr, setBannerErr] = useState("");

  const [composers, setComposers] = useState<Record<string, ComposerDraft>>({});
  const [uploadingBySid, setUploadingBySid] = useState<Record<string, number>>({});
  const [sendingBySid, setSendingBySid] = useState<Record<string, boolean>>({});

  const searchInputRef = useRef<HTMLInputElement>(null);
  const activeIdRef = useRef<string | null>(null);
  activeIdRef.current = activeId;
  const sessionsRef = useRef<SessionInfo[]>([]);
  sessionsRef.current = sessions;
  const composersRef = useRef<Record<string, ComposerDraft>>({});
  composersRef.current = composers;
  const sendingRef = useRef<Record<string, boolean>>({});
  sendingRef.current = sendingBySid;
  const globalSyncRef = useRef<GlobalSessionSync | null>(null);
  const itemsRef = useRef<HistoryItem[]>([]);
  itemsRef.current = items;
  const callsByIdRef = useRef<Map<string, LlmCallInfo>>(callsById);
  callsByIdRef.current = callsById;
  const inFlightCallIdsRef = useRef<Set<string>>(new Set());
  const failedCallIdsRef = useRef<Set<string>>(new Set());
  const sessionEpochRef = useRef(0);
  const loadMoreSeqRef = useRef(0);

  const updateComposer = (sid: string, fn: (cur: ComposerDraft) => ComposerDraft) => {
    setComposers((prev) => {
      const cur = prev[sid] || emptyComposer();
      const next = fn(cur);
      if (next === cur) return prev;
      composersRef.current = { ...prev, [sid]: next };
      return composersRef.current;
    });
  };

  const cleanupComposer = (sid: string) => {
    setComposers((prev) => {
      if (!prev[sid]) return prev;
      revokeAttachments(prev[sid].attachments);
      const next = { ...prev };
      delete next[sid];
      composersRef.current = next;
      return next;
    });
  };

  useEffect(() => {
    const apply = () => {
      const resolved = resolveTheme(themePref);
      document.documentElement.dataset.theme = resolved === "dark" ? "dark" : "";
    };
    apply();
    try {
      if (themePref === "system") localStorage.removeItem("mygo_theme");
      else localStorage.setItem("mygo_theme", themePref);
    } catch {}
    if (themePref !== "system") return;
    const mq = matchMedia("(prefers-color-scheme: dark)");
    mq.addEventListener("change", apply);
    return () => mq.removeEventListener("change", apply);
  }, [themePref]);

  useEffect(() => {
    saveLocalePref(localePref);
    document.documentElement.lang = locale === "zh" ? "zh-CN" : "en";
  }, [localePref, locale]);

  const cycleTheme = () => {
    setThemePrefState((cur) => (cur === "system" ? "light" : cur === "light" ? "dark" : "system"));
  };

  const toggleSidebarCollapse = () => {
    setSidebarCollapsed((prev) => {
      const next = !prev;
      try {
        if (next) localStorage.setItem("mygo_sidebar_collapsed", "1");
        else localStorage.removeItem("mygo_sidebar_collapsed");
      } catch {}
      return next;
    });
  };

  useEffect(() => {
    if (!authed) return;
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        if (settingsState.open) return;
        setSidebarCollapsed(false);
        try {
          localStorage.removeItem("mygo_sidebar_collapsed");
        } catch {}
        setSidebarOpen(true);
        setSearchOpen(true);
        requestAnimationFrame(() => {
          searchInputRef.current?.focus();
          searchInputRef.current?.select();
        });
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [authed, settingsState.open]);

  useEffect(() => {
    setUnauthorizedHandler(() => setAuthed(false));
    consumeHashToken();
    apiFetch<AppConfig>("/config", { token: getToken() })
      .then((cfg) => {
        setConfig(cfg);
        setAuthed(true);
      })
      .catch((e) => {
        if (e instanceof ApiError && e.status === 401) setAuthed(false);
        else setBannerErr(errorMessage(e));
      });
  }, []);

  const fetchAllSessions = async (): Promise<SessionInfo[]> => {
    const all: SessionInfo[] = [];
    let after = "";
    while (true) {
      const q = after ? `after=${encodeURIComponent(after)}&limit=200` : "limit=200";
      const page = await apiFetch<SessionInfo[]>(`/sessions?${q}`);
      all.push(...page);
      if (page.length < 200) break;
      after = page[page.length - 1].id;
    }
    return all;
  };

  const loadCatalog = async (): Promise<void> => {
    const [cfg, prov, mods] = await Promise.all([
      apiFetch<AppConfig>("/config"),
      apiFetch<ProviderInfo[]>("/providers?include_archived=true"),
      apiFetch<ModelInfo[]>("/models?include_archived=true"),
    ]);
    setConfig(cfg);
    setProviders(prov);
    setModels(mods);
  };

  useEffect(() => {
    if (!authed) return;
    let alive = true;
    let selectedInitial = false;
    loadCatalog().catch((e) => alive && setBannerErr(errorMessage(e)));

    const sync = createGlobalSessionSync({
      fetchSessions: fetchAllSessions,
      fetchSession: (id) => apiFetch<SessionInfo>(`/sessions/${id}`),
      getSession: (id) => sessionsRef.current.find((s) => s.id === id),
      onSnapshot: (merged) => {
        if (!alive) return;
        setSessions(merged);
        if (!selectedInitial) {
          if (activeIdRef.current != null) {
            selectedInitial = true;
          } else {
            let saved: string | null = null;
            try {
              saved = localStorage.getItem("mygo_sid");
            } catch {}
            const chosen =
              merged.find((s) => s.id === saved) ||
              [...merged].sort((a, b) => b.updated_at - a.updated_at)[0];
            if (chosen) {
              selectedInitial = true;
              setActiveId(chosen.id);
            }
          }
        } else if (activeIdRef.current != null && !merged.some((s) => s.id === activeIdRef.current)) {
          cleanupComposer(activeIdRef.current);
          setActiveId(null);
        }
      },
      onLocalPatchApplied: (updated) => {
        if (alive) setSessions((prev) => applyLocalPatch(prev, updated));
      },
      onDelta: (name, data) => {
        if (alive) setSessions((prev) => applySessionEvent(prev, name, data));
      },
      onDeleted: (id) => {
        if (!alive) return;
        cleanupComposer(id);
        setActiveId((cur) => (cur === id ? null : cur));
      },
      onError: (msg) => {
        if (alive) setBannerErr(msg);
      },
    });
    globalSyncRef.current = sync;
    void sync.sync();

    const ctrl = new AbortController();
    connectSse("/events", {
      signal: ctrl.signal,
      onEvent: (name, data) => {
        if (name === "ready" || name === "resync") {
          void sync.sync();
        } else if (name === "session.created" || name === "session.updated") {
          sync.onEvent(name, data as SessionInfo);
        } else if (name === "session.status") {
          sync.onEvent(name, data as SessionStatusEvent);
        } else if (name === "session.deleted") {
          sync.onEvent(name, data as SessionDeletedEvent);
        }
      },
    });
    return () => {
      alive = false;
      sync.dispose();
      if (globalSyncRef.current === sync) globalSyncRef.current = null;
      ctrl.abort();
    };
  }, [authed]);

  useEffect(() => {
    if (activeId) {
      try {
        localStorage.setItem("mygo_sid", String(activeId));
      } catch {}
    }
  }, [activeId]);

  const syncVisibleCalls = async (
    sid: string,
    list: HistoryItem[],
    retryFailed = false,
    epoch = sessionEpochRef.current
  ): Promise<void> => {
    if (sessionEpochRef.current !== epoch || activeIdRef.current !== sid) return;
    if (retryFailed) failedCallIdsRef.current.clear();

    const missing: string[] = [];
    const seen = new Set<string>();
    for (const item of list) {
      if (item.kind !== "assistant_message" || item.llm_call_id == null) continue;
      const callId = item.llm_call_id;
      if (seen.has(callId)) continue;
      seen.add(callId);
      if (inFlightCallIdsRef.current.has(callId)) continue;
      if (!retryFailed && failedCallIdsRef.current.has(callId)) continue;
      const existing = callsByIdRef.current.get(callId);
      if (existing && existing.status !== "in_flight") continue;
      missing.push(callId);
    }
    if (missing.length === 0) return;

    for (const id of missing) inFlightCallIdsRef.current.add(id);
    const fetched: LlmCallInfo[] = [];
    let cursor = 0;
    const workerCount = Math.min(6, missing.length);
    try {
      await Promise.all(
        Array.from({ length: workerCount }, async () => {
          while (cursor < missing.length && sessionEpochRef.current === epoch && activeIdRef.current === sid) {
            const callId = missing[cursor++];
            try {
              const call = await apiFetch<LlmCallInfo>(`/calls/${encodeURIComponent(callId)}`);
              if (sessionEpochRef.current !== epoch || activeIdRef.current !== sid) return;
              fetched.push(call);
            } catch {
              if (sessionEpochRef.current === epoch && activeIdRef.current === sid) {
                failedCallIdsRef.current.add(callId);
              }
            }
          }
        })
      );
    } finally {
      if (sessionEpochRef.current === epoch && activeIdRef.current === sid) {
        for (const id of missing) inFlightCallIdsRef.current.delete(id);
      }
    }

    if (sessionEpochRef.current !== epoch || activeIdRef.current !== sid || fetched.length === 0) return;
    setCallsById((prev) => {
      const next = new Map(prev);
      for (const c of fetched) next.set(c.id, c);
      callsByIdRef.current = next;
      return next;
    });
  };

  useEffect(() => {
    if (!authed || !activeId) return;
    void syncVisibleCalls(activeId, items);
  }, [authed, activeId, items]);

  useEffect(() => {
    const epoch = ++sessionEpochRef.current;
    loadMoreSeqRef.current += 1;
    setLoadingMore(false);
    itemsRef.current = [];
    callsByIdRef.current = new Map();
    setCallsById(new Map());
    inFlightCallIdsRef.current.clear();
    failedCallIdsRef.current.clear();
    if (!authed || !activeId) {
      setItems([]);
      setDraft(null);
      setQueuedIds(new Set());
      setTasks([]);
      setSummary(null);
      setHasMore(false);
      return;
    }
    const sid = activeId;
    setItems([]);
    setDraft(null);
    setRetryNotice(null);
    setHasMore(false);
    setBannerErr("");

    let disposed = false;
    let sidecarInFlight = false;
    let sidecarAgain = false;

    const triggerSidecars = (): void => {
      if (disposed || sessionEpochRef.current !== epoch) return;
      if (sidecarInFlight) {
        sidecarAgain = true;
        return;
      }
      sidecarInFlight = true;
      void (async () => {
        try {
          const [queued, sum, tlist] = await Promise.all([
            apiFetch<HistoryItem[]>(`/sessions/${sid}/messages?status=queued&limit=200`),
            apiFetch<CallsSummary>(`/calls/summary?session_id=${encodeURIComponent(sid)}`),
            apiFetch<ActiveTask[]>(`/sessions/${sid}/tasks`),
          ]);
          if (disposed || sessionEpochRef.current !== epoch || activeIdRef.current !== sid) return;
          setQueuedIds(new Set(queued.map((m) => m.id)));
          setSummary(sum);
          setTasks(tlist);
          setContextRefreshNonce((n) => n + 1);
          void syncVisibleCalls(sid, itemsRef.current, true, epoch);
        } catch {
        } finally {
          sidecarInFlight = false;
          if (sidecarAgain && !disposed && sessionEpochRef.current === epoch) {
            sidecarAgain = false;
            triggerSidecars();
          }
        }
      })();
    };

    const closedCalls = new Set<string>();
    const closedItems = new Set<string>();

    const sessionSync = createSessionSync({
      fetchInitial: () => apiFetch<HistoryItem[]>(`/sessions/${sid}/history?tail=true&limit=50`),
      fetchAfter: (afterId) =>
        apiFetch<HistoryItem[]>(
          afterId
            ? `/sessions/${sid}/history?after=${encodeURIComponent(afterId)}&limit=200`
            : `/sessions/${sid}/history?limit=200`
        ),
      onItems: (page) => {
        if (activeIdRef.current === sid && sessionEpochRef.current === epoch) {
          setItems((prev) => mergeItems(prev, page));
        }
      },
      onHasMore: (more) => {
        if (activeIdRef.current === sid && sessionEpochRef.current === epoch) setHasMore(more);
      },
      onSidecars: triggerSidecars,
      onError: (msg) => {
        if (activeIdRef.current === sid && sessionEpochRef.current === epoch) setBannerErr(msg);
      },
    });
    sessionSync.start();

    const ctrl = new AbortController();
    connectSse(`/sessions/${sid}/events`, {
      signal: ctrl.signal,
      onStatus: setSseStatus,
      onEvent: (name, data) => {
        if (activeIdRef.current !== sid || sessionEpochRef.current !== epoch) return;
        if (name === "ready") {
          const sess = data as SessionInfo;
          globalSyncRef.current?.touchSession(sid);
          setSessions((prev) => prev.map((s) => (s.id === sess.id ? sess : s)));
          setDraft(null);
          setRetryNotice(null);
          sessionSync.onReady();
        } else if (name === "stream.snapshot") {
          const snap = data as ActiveDraft;
          if (!closedCalls.has(snap.llm_call_id)) setDraft(snap);
        } else if (name === "item.started") {
          const ev = data as DraftStartedEvent;
          if (closedCalls.has(ev.llm_call_id) || closedItems.has(`${ev.llm_call_id}:${ev.output_index}`)) return;
          setDraft((prev) => applyDraftStarted(prev, ev));
        } else if (name === "item.delta") {
          const ev = data as DraftDeltaEvent;
          if (closedCalls.has(ev.llm_call_id) || closedItems.has(`${ev.llm_call_id}:${ev.output_index}`)) return;
          setRetryNotice(null);
          setDraft((prev) => applyDraftDelta(prev, ev));
        } else if (name === "stream.clear") {
          const ev = data as { llm_call_id?: string } | null;
          if (ev?.llm_call_id != null) closedCalls.add(ev.llm_call_id);
          setDraft((prev) => (prev?.llm_call_id === ev?.llm_call_id ? null : prev));
        } else if (name === "attempt.retry") {
          const ev = data as RetryNotice;
          setDraft((prev) => {
            if (prev?.llm_call_id != null) closedCalls.add(prev.llm_call_id);
            return null;
          });
          setRetryNotice(ev);
        } else if (name === "item.saved") {
          const item = data as HistoryItem;
          sessionSync.onLiveItem(item);
          setDraft((prev) => {
            const { next, droppedIndex } = dropDraftForSaved(prev, item);
            if (droppedIndex !== null && item.llm_call_id != null) {
              closedItems.add(`${item.llm_call_id}:${droppedIndex}`);
            }
            return next;
          });
          if (item.kind === "user_message") {
            setQueuedIds((prev) => new Set([...prev, item.id]));
          } else if (item.kind === "withdrawal" && item.target_id) {
            const targetId = item.target_id;
            setQueuedIds((prev) => {
              const next = new Set(prev);
              next.delete(targetId);
              return next;
            });
          } else {
            triggerSidecars();
          }
        } else if (name === "session.status") {
          const ev = data as SessionStatusEvent;
          setRetryNotice(null);
          if (ev.status === "idle" || ev.status === "paused") {
            setDraft((prev) => {
              if (prev?.llm_call_id != null) closedCalls.add(prev.llm_call_id);
              return null;
            });
          }
          globalSyncRef.current?.touchSession(sid);
          setSessions((prev) =>
            prev.map((s) =>
              s.id === sid
                ? { ...s, status: ev.status, last_error: ev.reason || "", updated_at: ev.updated_at ?? s.updated_at }
                : s
            )
          );
          triggerSidecars();
        } else if (name === "session.updated") {
          const updated = data as SessionInfo;
          globalSyncRef.current?.touchSession(sid);
          setSessions((prev) => prev.map((s) => (s.id === updated.id ? updated : s)));
        } else if (name === "resync") {
          globalSyncRef.current?.touchSession(sid);
          apiFetch<SessionInfo>(`/sessions/${sid}`)
            .then((sess) => {
              if (activeIdRef.current === sid && sessionEpochRef.current === epoch) {
                globalSyncRef.current?.touchSession(sid);
                setSessions((prev) => prev.map((s) => (s.id === sess.id ? sess : s)));
              }
            })
            .catch(() => {});
          sessionSync.onResync();
        }
      },
    });

    return () => {
      disposed = true;
      sessionSync.dispose();
      ctrl.abort();
    };
  }, [authed, activeId]);

  const activeSession = useMemo(
    () => sessions.find((s) => s.id === activeId) || null,
    [sessions, activeId]
  );

  const shouldPollTasks =
    Boolean(authed && activeId) &&
    (tasks.length > 0 || activeSession?.status === "running" || activeSession?.status === "pausing");

  useEffect(() => {
    if (!shouldPollTasks || !activeId) return;
    const sid = activeId;
    const timer = setInterval(() => {
      apiFetch<ActiveTask[]>(`/sessions/${sid}/tasks`)
        .then((list) => {
          if (activeIdRef.current === sid) setTasks(list);
        })
        .catch(() => {});
    }, 2000);
    return () => clearInterval(timer);
  }, [shouldPollTasks, activeId]);

  const modelsById = useMemo(() => new Map(models.map((m) => [m.id, m])), [models]);

  const createSession = async () => {
    setBannerErr("");
    try {
      const s = await apiFetch<SessionInfo>("/sessions", { method: "POST", body: {} });
      const added = globalSyncRef.current
        ? globalSyncRef.current.onLocalCreated(s)
        : (setSessions((prev) => applySessionEvent(prev, "session.created", s)), true);
      if (added) setActiveId(s.id);
    } catch (e) {
      setBannerErr(errorMessage(e));
    }
  };

  const deleteSession = async (sid: string) => {
    setBannerErr("");
    try {
      await apiFetch(`/sessions/${sid}`, { method: "DELETE" });
      if (globalSyncRef.current) {
        globalSyncRef.current.onEvent("session.deleted", { id: sid });
      } else {
        cleanupComposer(sid);
        setSessions((prev) => applySessionEvent(prev, "session.deleted", { id: sid }));
        if (activeId === sid) setActiveId(null);
      }
    } catch (e) {
      setBannerErr(errorMessage(e));
    }
  };

  const patchSession = async (
    sid: string,
    patch: Partial<Pick<SessionInfo, "title" | "model_id" | "reasoning_effort">>
  ) => {
    setBannerErr("");
    const baseVersion = globalSyncRef.current?.getSessionVersion(sid) ?? 0;
    try {
      const updated = await apiFetch<SessionInfo>(`/sessions/${sid}`, { method: "PATCH", body: patch });
      if (globalSyncRef.current) globalSyncRef.current.onLocalPatch(updated, baseVersion);
      else setSessions((prev) => applyLocalPatch(prev, updated));
    } catch (e) {
      setBannerErr(errorMessage(e));
    }
  };

  const loadMore = async () => {
    if (!activeId || !items.length || loadingMore) return;
    const sid = activeId;
    const mySeq = ++loadMoreSeqRef.current;
    const firstId = items[0].id;
    setLoadingMore(true);
    try {
      const older = await apiFetch<HistoryItem[]>(`/sessions/${sid}/history?before=${encodeURIComponent(firstId)}&limit=50`);
      if (activeIdRef.current !== sid || loadMoreSeqRef.current !== mySeq) return;
      setItems((prev) => mergeItems(prev, older));
      setHasMore(older.length === 50);
    } catch (e) {
      if (activeIdRef.current === sid && loadMoreSeqRef.current === mySeq) {
        setBannerErr(errorMessage(e));
      }
    } finally {
      if (activeIdRef.current === sid && loadMoreSeqRef.current === mySeq) {
        setLoadingMore(false);
      }
    }
  };

  const onChangeText = (sid: string, text: string) => {
    updateComposer(sid, (c) => ({
      ...c,
      text,
      version: (c.version ?? 0) + 1,
      textVersion: (c.textVersion ?? 0) + 1,
      pending: null,
      error: "",
    }));
  };

  const onRemoveAttachment = (sid: string, aid: string) => {
    updateComposer(sid, (c) => {
      const target = c.attachments.find((a) => a.id === aid);
      if (target?.preview) URL.revokeObjectURL(target.preview);
      return {
        ...c,
        attachments: c.attachments.filter((a) => a.id !== aid),
        version: (c.version ?? 0) + 1,
        pending: null,
        error: "",
      };
    });
  };

  const onUploadFiles = async (targetSid: string, files: FileList | null) => {
    const list = Array.from(files || []).filter((f) => /^(image|audio|video)\//.test(f.type));
    if (!list.length) {
      updateComposer(targetSid, (c) => ({
        ...c,
        error: t("Only image, audio, or video files are supported."),
      }));
      return;
    }
    updateComposer(targetSid, (c) => ({ ...c, error: "" }));
    setUploadingBySid((prev) => ({ ...prev, [targetSid]: (prev[targetSid] || 0) + 1 }));
    try {
      for (const file of list) {
        const res = await apiFetch<{ id: string }>("/attachments", {
          method: "POST",
          headers: { "Content-Type": file.type },
          body: file,
        });
        const kind = file.type.split("/")[0] as MediaKind;
        const preview = kind === "image" ? URL.createObjectURL(file) : null;
        if (!sessionsRef.current.some((s) => s.id === targetSid)) {
          if (preview) URL.revokeObjectURL(preview);
          return;
        }
        updateComposer(targetSid, (c) => ({
          ...c,
          version: (c.version ?? 0) + 1,
          pending: null,
          attachments: [
            ...c.attachments,
            { id: res.id, kind, name: file.name || `${kind}-${res.id}`, preview },
          ],
        }));
      }
    } catch (e) {
      updateComposer(targetSid, (c) => ({ ...c, error: errorMessage(e) }));
    } finally {
      setUploadingBySid((prev) => {
        const nextCount = (prev[targetSid] || 1) - 1;
        if (nextCount <= 0) {
          const copy = { ...prev };
          delete copy[targetSid];
          return copy;
        }
        return { ...prev, [targetSid]: nextCount };
      });
    }
  };

  const onSubmitComposer = async (targetSid: string, startRun: boolean) => {
    if (sendingRef.current[targetSid]) return;
    const cur = composersRef.current[targetSid] || emptyComposer();
    const content = buildContentBlocks(cur);

    if (content.length === 0) {
      if (!startRun) return;
      setSendingBySid((prev) => ({ ...prev, [targetSid]: true }));
      updateComposer(targetSid, (c) => ({ ...c, error: "" }));
      try {
        await apiFetch(`/sessions/${targetSid}/run`, { method: "PUT" });
      } catch (e) {
        if (sessionsRef.current.some((s) => s.id === targetSid)) {
          updateComposer(targetSid, (c) => ({ ...c, error: errorMessage(e) }));
        }
      } finally {
        setSendingBySid((prev) => ({ ...prev, [targetSid]: false }));
      }
      return;
    }

    const contentJson = JSON.stringify(content);
    const key = cur.pending?.contentJson === contentJson ? cur.pending.key : makeIdempotencyKey();
    const submitted: SubmittedComposerSnapshot = {
      key,
      version: cur.version ?? 0,
      textVersion: cur.textVersion ?? 0,
      attachmentIds: new Set(cur.attachments.map((a) => a.id)),
    };
    updateComposer(targetSid, (c) => ({ ...c, pending: { key, contentJson }, error: "" }));
    setSendingBySid((prev) => ({ ...prev, [targetSid]: true }));

    try {
      try {
        await apiFetch(`/sessions/${targetSid}/messages`, {
          method: "POST",
          headers: { "Idempotency-Key": key },
          body: { content },
        });
      } catch (e) {
        const status = e instanceof ApiError ? e.status : 0;
        const clientRejected = status >= 400 && status < 500;
        if (!sessionsRef.current.some((s) => s.id === targetSid)) return;
        updateComposer(targetSid, (c) => ({
          ...c,
          pending: clientRejected && c.pending?.key === key ? null : c.pending,
          error: errorMessage(e),
        }));
        return;
      }

      if (!sessionsRef.current.some((s) => s.id === targetSid)) return;
      updateComposer(targetSid, (c) => {
        const { next, removed } = reconcileSubmittedComposer(c, submitted);
        revokeAttachments(removed);
        return next;
      });

      if (startRun) {
        try {
          await apiFetch(`/sessions/${targetSid}/run`, { method: "PUT" });
        } catch (e) {
          if (!sessionsRef.current.some((s) => s.id === targetSid)) return;
          updateComposer(targetSid, (c) => ({
            ...c,
            error: `${t("Message queued, but failed to start run:")} ${errorMessage(e)}`,
          }));
        }
      }
    } finally {
      setSendingBySid((prev) => ({ ...prev, [targetSid]: false }));
    }
  };

  const openSidebar = () => {
    setSidebarCollapsed(false);
    try {
      localStorage.removeItem("mygo_sidebar_collapsed");
    } catch {}
    setSidebarOpen(true);
  };

  const openSettings = (tab: SettingsTabId, sid = "") =>
    setSettingsState({ open: true, tab, sessionFilter: sid });

  const activeModel = activeSession?.model_id ? modelsById.get(activeSession.model_id) : undefined;
  const withdrawMsg = (mid: string) => {
    if (!activeSession) return;
    void apiFetch(`/sessions/${activeSession.id}/messages/${mid}`, { method: "DELETE" }).catch((e) =>
      setBannerErr(errorMessage(e))
    );
  };

  return (
    <I18nContext.Provider value={locale}>
      {authed === null ? (
        <div class="gate-wrap"><div class="muted">{t("Connecting…")}</div></div>
      ) : !authed ? (
        <TokenGate onSuccess={() => setAuthed(true)} />
      ) : (
        <div class="layout">
          <Sidebar
            sessions={sessions}
            activeId={activeId}
            open={sidebarOpen}
            collapsed={sidebarCollapsed}
            searchOpen={searchOpen}
            themePref={themePref}
            searchInputRef={searchInputRef}
            onSelect={setActiveId}
            onCreate={createSession}
            onDelete={deleteSession}
            onCycleTheme={cycleTheme}
            onOpenSettingsTab={(tab) => openSettings(tab)}
            onToggleCollapse={toggleSidebarCollapse}
            onSetSearchOpen={setSearchOpen}
            onClose={() => setSidebarOpen(false)}
          />

          <main class="main">
            {activeSession ? (
              <>
                <TopBar
                  session={activeSession}
                  sseStatus={sseStatus}
                  sidebarCollapsed={sidebarCollapsed}
                  activeTab={activeTab}
                  itemCount={items.length}
                  onSelectTab={setActiveTab}
                  onOpenSidebar={openSidebar}
                  onOpenSettings={() => openSettings("models")}
                  onPatchSession={patchSession}
                />
                {(bannerErr || activeSession.last_error) && (
                  <div class="status-banner">
                    <div class="row gap-1">
                      <Icon n="alert" />
                      <span>{bannerErr || activeSession.last_error}</span>
                    </div>
                    <div class="row gap-1">
                      {activeSession.status === "paused" && activeSession.last_error && !bannerErr && (
                        <button
                          type="button"
                          class="btn-ghost btn-xs"
                          onClick={() =>
                            void apiFetch(`/sessions/${activeSession.id}/run`, { method: "PUT" }).catch((e) =>
                              setBannerErr(errorMessage(e))
                            )
                          }
                        >
                          <Icon n="play" s={11} />
                          <span>{t("Resume")}</span>
                        </button>
                      )}
                      {bannerErr && (
                        <button
                          type="button"
                          class="btn-icon"
                          aria-label={t("Dismiss")}
                          onClick={() => setBannerErr("")}
                        >
                          <Icon n="x" s={14} />
                        </button>
                      )}
                    </div>
                  </div>
                )}

                {activeTab === "chat" && (
                  <>
                    <Timeline
                      session={activeSession}
                      items={items}
                      hasMore={hasMore}
                      loadingMore={loadingMore}
                      queuedIds={queuedIds}
                      draft={draft}
                      retryNotice={retryNotice}
                      callsById={callsById}
                      modelLabel={activeModel?.model_key ?? null}
                      effortLabel={activeSession.reasoning_effort}
                      onLoadMore={loadMore}
                      onWithdraw={withdrawMsg}
                      onOpenSettings={() => openSettings("models")}
                    />
                    <Composer
                      session={activeSession}
                      providers={providers}
                      models={models}
                      draft={composers[activeSession.id] || emptyComposer()}
                      uploading={Boolean(uploadingBySid[activeSession.id])}
                      sending={Boolean(sendingBySid[activeSession.id])}
                      tasks={tasks}
                      queuedCount={queuedIds.size}
                      summary={summary}
                      onChangeText={onChangeText}
                      onUploadFiles={onUploadFiles}
                      onRemoveAttachment={onRemoveAttachment}
                      onSubmit={onSubmitComposer}
                      onPause={(mode) =>
                        void apiFetch(`/sessions/${activeSession.id}/run?mode=${mode}`, { method: "DELETE" }).catch((e) =>
                          setBannerErr(errorMessage(e))
                        )
                      }
                      onKillTask={(tid) =>
                        void apiFetch(`/sessions/${activeSession.id}/tasks/${tid}`, { method: "DELETE" }).catch((e) =>
                          setBannerErr(errorMessage(e))
                        )
                      }
                      onPatchSession={patchSession}
                      onOpenUsage={(sid) => openSettings("usage", String(sid))}
                    />
                  </>
                )}

                {activeTab === "trace" && (
                  <TraceView
                    items={items}
                    hasMore={hasMore}
                    loadingMore={loadingMore}
                    queuedIds={queuedIds}
                    draft={draft}
                    onLoadMore={loadMore}
                    onWithdraw={withdrawMsg}
                  />
                )}

                {activeTab === "context" && (
                  <ContextView
                    session={activeSession}
                    items={items}
                    summary={summary}
                    ctxLimit={activeModel?.context_window_tokens}
                    refreshNonce={contextRefreshNonce}
                  />
                )}
              </>
            ) : (
              <div class="welcome">
                <button
                  type="button"
                  class={`btn-icon welcome-menu ${sidebarCollapsed ? "" : "mobile-only"}`}
                  aria-label={t("Show sessions")}
                  onClick={openSidebar}
                >
                  <Icon n="sidebar" s={16} />
                </button>
                <div class="welcome-card">
                  <div class="brand">
                    <BrandMark size={28} />
                    <span class="brand-name">MyGo</span>
                    <span class="brand-tag">BOT</span>
                  </div>
                  <p class="muted">{t("Select a session from the sidebar or create a new one.")}</p>
                  {models.filter((m) => !m.archived_at && !m.provider_archived_at).length === 0 && (
                    <p class="muted small">{t("No available models configured yet — open Settings to add a provider and model.")}</p>
                  )}
                  <div class="row gap-2">
                    <button type="button" class="btn-primary" onClick={createSession}>
                      <Icon n="plus" />
                      <span>{t("New Session")}</span>
                    </button>
                    <button type="button" class="btn-ghost" onClick={() => openSettings("models")}>
                      <Icon n="settings" />
                      <span>{t("Settings")}</span>
                    </button>
                  </div>
                </div>
              </div>
            )}
          </main>

          {settingsState.open && (
            <Settings
              config={config}
              providers={providers}
              models={models}
              sessions={sessions}
              themePref={themePref}
              localePref={localePref}
              initialTab={settingsState.tab}
              initialSessionFilter={settingsState.sessionFilter}
              onSetThemePref={setThemePrefState}
              onSetLocalePref={setLocalePrefState}
              onRefreshCatalog={loadCatalog}
              onSignOut={() => {
                setToken("");
                setSettingsState({ open: false, tab: "models", sessionFilter: "" });
                setAuthed(false);
              }}
              onClose={() => setSettingsState((s) => ({ ...s, open: false }))}
            />
          )}
        </div>
      )}
    </I18nContext.Provider>
  );
}

const root = document.getElementById("app");
if (root) render(<App />, root);
