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
import { ContextView } from "./ContextView.tsx";
import { I18nContext, readLocalePref, resolveLocale, saveLocalePref, statusText, translate, useI18n } from "./i18n.ts";
import type { LocalePref } from "./i18n.ts";
import {
  BrandMark,
  IconActivity,
  IconAlert,
  IconChat,
  IconEye,
  IconEyeOff,
  IconLayers,
  IconPlay,
  IconPlus,
  IconSettings,
  IconSidebar,
  IconX,
} from "./Icons.tsx";
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
import { TraceView } from "./TraceView.tsx";
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
      setErr(errorMessage(ex) || t("invalidToken"));
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
        <p class="muted">{t("gatePrompt")}</p>
        {err && <div class="err-inline">{err}</div>}
        <div class="token-input-wrap">
          {show ? (
            <input
              key="tok"
              type="text"
              placeholder={t("tokenPlaceholder")}
              autoFocus
              value={value}
              onInput={(e) => setValue(e.currentTarget.value)}
            />
          ) : (
            <input
              key="tok"
              type="password"
              placeholder={t("tokenPlaceholder")}
              autoFocus
              value={value}
              onInput={(e) => setValue(e.currentTarget.value)}
            />
          )}
          <button
            type="button"
            class="btn-icon"
            title={show ? t("hideToken") : t("showToken")}
            aria-label={show ? t("hideToken") : t("showToken")}
            onClick={() => setShow((v) => !v)}
          >
            {show ? <IconEyeOff size={15} /> : <IconEye size={15} />}
          </button>
        </div>
        <button type="submit" class="btn-primary" disabled={busy || !value.trim()}>
          {busy ? t("verifying") : t("connect")}
        </button>
        <div class="muted small">
          {t("tokenHintBefore")} <code class="md-inline-code">mygo service status</code> {t("tokenHintAfter")}
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
  onPatchSession: (sid: number, patch: Partial<Pick<SessionInfo, "title">>) => void;
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
            title={t("showSessions")}
            aria-label={t("showSessions")}
            onClick={onOpenSidebar}
          >
            <IconSidebar size={16} />
          </button>
          <input
            class="title-input"
            placeholder={`${t("sessionPrefix")} #${session.id}`}
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
            title={t("settings")}
            aria-label={t("settings")}
            onClick={onOpenSettings}
          >
            <IconSettings size={15} />
          </button>
        </div>
      </div>

      <nav class="view-tabs">
        <button
          type="button"
          class={`view-tab ${activeTab === "chat" ? "active" : ""}`}
          onClick={() => onSelectTab("chat")}
        >
          <IconChat size={14} />
          <span>{t("tabChat")}</span>
        </button>
        <button
          type="button"
          class={`view-tab ${activeTab === "trace" ? "active" : ""}`}
          onClick={() => onSelectTab("trace")}
        >
          <IconActivity size={14} />
          <span>{t("tabTrace")}</span>
          {itemCount > 0 && <span class="tab-count mono">{itemCount}</span>}
        </button>
        <button
          type="button"
          class={`view-tab ${activeTab === "context" ? "active" : ""}`}
          onClick={() => onSelectTab("context")}
        >
          <IconLayers size={14} />
          <span>{t("tabContext")}</span>
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
  const [activeId, setActiveId] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState<MainTab>("chat");

  const [items, setItems] = useState<HistoryItem[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [queuedIds, setQueuedIds] = useState<Set<number>>(() => new Set());
  const [draft, setDraft] = useState<ActiveDraft | null>(null);
  const [retryNotice, setRetryNotice] = useState<RetryNotice | null>(null);
  const [tasks, setTasks] = useState<ActiveTask[]>([]);
  const [summary, setSummary] = useState<CallsSummary | null>(null);
  const [callsById, setCallsById] = useState<Map<number, LlmCallInfo>>(() => new Map());
  const [contextRefreshNonce, setContextRefreshNonce] = useState(0);
  const [sseStatus, setSseStatus] = useState<SseStatus>("open");
  const [bannerErr, setBannerErr] = useState("");

  const [composers, setComposers] = useState<Record<number, ComposerDraft>>({});
  const [uploadingBySid, setUploadingBySid] = useState<Record<number, number>>({});
  const [sendingBySid, setSendingBySid] = useState<Record<number, boolean>>({});

  const searchInputRef = useRef<HTMLInputElement>(null);
  const activeIdRef = useRef<number | null>(null);
  activeIdRef.current = activeId;
  const sessionsRef = useRef<SessionInfo[]>([]);
  sessionsRef.current = sessions;
  const composersRef = useRef<Record<number, ComposerDraft>>({});
  composersRef.current = composers;
  const sendingRef = useRef<Record<number, boolean>>({});
  sendingRef.current = sendingBySid;
  const globalSyncRef = useRef<GlobalSessionSync | null>(null);
  const settledCallCursorRef = useRef(0);

  const updateComposer = (sid: number, fn: (cur: ComposerDraft) => ComposerDraft) => {
    setComposers((prev) => {
      const cur = prev[sid] || emptyComposer();
      const next = fn(cur);
      if (next === cur) return prev;
      composersRef.current = { ...prev, [sid]: next };
      return composersRef.current;
    });
  };

  const cleanupComposer = (sid: number) => {
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
    let after = 0;
    while (true) {
      const page = await apiFetch<SessionInfo[]>(`/sessions?after=${after}&limit=200`);
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
            let saved: number | null = null;
            try {
              saved = Number(localStorage.getItem("mygo_sid")) || null;
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

  const refreshSessionSidecars = async (sid: number): Promise<void> => {
    const fetchNewCalls = async (): Promise<LlmCallInfo[]> => {
      const collected: LlmCallInfo[] = [];
      let after = settledCallCursorRef.current;
      while (true) {
        const page = await apiFetch<LlmCallInfo[]>(`/calls?session_id=${sid}&after=${after}&limit=200`);
        collected.push(...page);
        if (page.length < 200) break;
        after = page[page.length - 1].id;
      }
      return collected;
    };

    const [queued, sum, tlist, newCalls] = await Promise.all([
      apiFetch<HistoryItem[]>(`/sessions/${sid}/messages?status=queued&limit=200`),
      apiFetch<CallsSummary>(`/calls/summary?session_id=${sid}`),
      apiFetch<ActiveTask[]>(`/sessions/${sid}/tasks`),
      fetchNewCalls(),
    ]);
    if (activeIdRef.current !== sid) return;
    setQueuedIds(new Set(queued.map((m) => m.id)));
    setSummary(sum);
    setTasks(tlist);
    if (newCalls.length > 0) {
      let nextCursor = settledCallCursorRef.current;
      let sawInFlight = false;
      for (const c of newCalls) {
        if (c.status === "in_flight") {
          nextCursor = Math.min(sawInFlight ? nextCursor : c.id - 1, c.id - 1);
          sawInFlight = true;
        } else if (!sawInFlight) {
          nextCursor = Math.max(nextCursor, c.id);
        }
      }
      settledCallCursorRef.current = Math.max(0, nextCursor);
      setCallsById((prev) => {
        const next = new Map(prev);
        for (const c of newCalls) next.set(c.id, c);
        return next;
      });
    }
    setContextRefreshNonce((n) => n + 1);
  };

  useEffect(() => {
    settledCallCursorRef.current = 0;
    setCallsById(new Map());
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

    const closedCalls = new Set<number>();
    const closedItems = new Set<string>();

    const sessionSync = createSessionSync({
      fetchInitial: () => apiFetch<HistoryItem[]>(`/sessions/${sid}/history?tail=true&limit=50`),
      fetchAfter: (afterId) => apiFetch<HistoryItem[]>(`/sessions/${sid}/history?after=${afterId}&limit=200`),
      onItems: (page) => {
        if (activeIdRef.current === sid) setItems((prev) => mergeItems(prev, page));
      },
      onHasMore: (more) => {
        if (activeIdRef.current === sid) setHasMore(more);
      },
      onSidecars: () => refreshSessionSidecars(sid),
      onError: (msg) => {
        if (activeIdRef.current === sid) setBannerErr(msg);
      },
    });

    const ctrl = new AbortController();
    connectSse(`/sessions/${sid}/events`, {
      signal: ctrl.signal,
      onStatus: setSseStatus,
      onEvent: (name, data) => {
        if (activeIdRef.current !== sid) return;
        if (name === "ready") {
          const sess = data as SessionInfo;
          globalSyncRef.current?.touchSession(sid);
          setSessions((prev) => prev.map((s) => (s.id === sess.id ? sess : s)));
          setDraft(null);
          setRetryNotice(null);
          sessionSync.onReady();
        } else if (name === "stream.snapshot") {
          const snap = data as ActiveDraft;
          if (!closedCalls.has(snap.llm_call_id)) {
            setDraft(snap);
          }
        } else if (name === "item.started") {
          const ev = data as DraftStartedEvent;
          if (closedCalls.has(ev.llm_call_id) || closedItems.has(`${ev.llm_call_id}:${ev.output_index}`)) {
            return;
          }
          setDraft((prev) => applyDraftStarted(prev, ev));
        } else if (name === "item.delta") {
          const ev = data as DraftDeltaEvent;
          if (closedCalls.has(ev.llm_call_id) || closedItems.has(`${ev.llm_call_id}:${ev.output_index}`)) {
            return;
          }
          setRetryNotice(null);
          setDraft((prev) => applyDraftDelta(prev, ev));
        } else if (name === "stream.clear") {
          const ev = data as { llm_call_id?: number } | null;
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
            refreshSessionSidecars(sid).catch(() => {});
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
          refreshSessionSidecars(sid).catch(() => {});
        } else if (name === "session.updated") {
          const updated = data as SessionInfo;
          globalSyncRef.current?.touchSession(sid);
          setSessions((prev) => prev.map((s) => (s.id === updated.id ? updated : s)));
        } else if (name === "resync") {
          globalSyncRef.current?.touchSession(sid);
          apiFetch<SessionInfo>(`/sessions/${sid}`)
            .then((sess) => {
              if (activeIdRef.current === sid) {
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

  const deleteSession = async (sid: number) => {
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
    sid: number,
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
    const firstId = items[0].id;
    setLoadingMore(true);
    try {
      const older = await apiFetch<HistoryItem[]>(`/sessions/${sid}/history?before=${firstId}&limit=50`);
      if (activeIdRef.current !== sid) return;
      setItems((prev) => mergeItems(prev, older));
      setHasMore(older.length === 50);
    } catch (e) {
      setBannerErr(errorMessage(e));
    } finally {
      setLoadingMore(false);
    }
  };

  const onChangeText = (sid: number, text: string) => {
    updateComposer(sid, (c) => ({
      ...c,
      text,
      version: (c.version ?? 0) + 1,
      textVersion: (c.textVersion ?? 0) + 1,
      pending: null,
      error: "",
    }));
  };

  const onRemoveAttachment = (sid: number, aid: number) => {
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

  const onUploadFiles = async (targetSid: number, files: FileList | null) => {
    const list = Array.from(files || []).filter((f) => /^(image|audio|video)\//.test(f.type));
    if (!list.length) {
      updateComposer(targetSid, (c) => ({
        ...c,
        error: translate(locale, "unsupportedMedia"),
      }));
      return;
    }
    updateComposer(targetSid, (c) => ({ ...c, error: "" }));
    setUploadingBySid((prev) => ({ ...prev, [targetSid]: (prev[targetSid] || 0) + 1 }));
    try {
      for (const file of list) {
        const res = await apiFetch<{ id: number }>("/attachments", {
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

  const onSubmitComposer = async (targetSid: number, startRun: boolean) => {
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
            error: `${translate(locale, "queuedRunFailed")} ${errorMessage(e)}`,
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

  const activeModel = activeSession?.model_id ? modelsById.get(activeSession.model_id) : undefined;

  return (
    <I18nContext.Provider value={locale}>
      <AppView
        authed={authed}
        locale={locale}
        themePref={themePref}
        localePref={localePref}
        sessions={sessions}
        activeId={activeId}
        activeSession={activeSession}
        activeModel={activeModel}
        activeTab={activeTab}
        providers={providers}
        models={models}
        config={config}
        sidebarOpen={sidebarOpen}
        sidebarCollapsed={sidebarCollapsed}
        searchOpen={searchOpen}
        searchInputRef={searchInputRef}
        items={items}
        hasMore={hasMore}
        loadingMore={loadingMore}
        queuedIds={queuedIds}
        draft={draft}
        retryNotice={retryNotice}
        tasks={tasks}
        summary={summary}
        callsById={callsById}
        contextRefreshNonce={contextRefreshNonce}
        sseStatus={sseStatus}
        bannerErr={bannerErr}
        composers={composers}
        uploadingBySid={uploadingBySid}
        sendingBySid={sendingBySid}
        settingsState={settingsState}
        onAuthed={() => setAuthed(true)}
        onSelectSession={setActiveId}
        onSelectTab={setActiveTab}
        onCreateSession={createSession}
        onDeleteSession={deleteSession}
        onPatchSession={patchSession}
        onCycleTheme={cycleTheme}
        onSetThemePref={setThemePrefState}
        onSetLocalePref={setLocalePrefState}
        onToggleSidebarCollapse={toggleSidebarCollapse}
        onOpenSidebar={openSidebar}
        onCloseSidebar={() => setSidebarOpen(false)}
        onSetSearchOpen={setSearchOpen}
        onOpenSettingsTab={(tab, sid = "") =>
          setSettingsState({ open: true, tab, sessionFilter: sid })
        }
        onCloseSettings={() => setSettingsState((s) => ({ ...s, open: false }))}
        onSignOut={() => {
          setToken("");
          setSettingsState({ open: false, tab: "models", sessionFilter: "" });
          setAuthed(false);
        }}
        onRefreshCatalog={loadCatalog}
        onLoadMore={loadMore}
        onWithdraw={(mid) => {
          if (!activeSession) return;
          void apiFetch(`/sessions/${activeSession.id}/messages/${mid}`, { method: "DELETE" }).catch((e) =>
            setBannerErr(errorMessage(e))
          );
        }}
        onClearBannerErr={() => setBannerErr("")}
        onResumePaused={() => {
          if (!activeSession) return;
          void apiFetch(`/sessions/${activeSession.id}/run`, { method: "PUT" }).catch((e) =>
            setBannerErr(errorMessage(e))
          );
        }}
        onChangeText={onChangeText}
        onUploadFiles={onUploadFiles}
        onRemoveAttachment={onRemoveAttachment}
        onSubmitComposer={onSubmitComposer}
        onPause={(mode) => {
          if (!activeSession) return;
          void apiFetch(`/sessions/${activeSession.id}/run?mode=${mode}`, { method: "DELETE" }).catch((e) =>
            setBannerErr(errorMessage(e))
          );
        }}
        onKillTask={(tid) => {
          if (!activeSession) return;
          void apiFetch(`/sessions/${activeSession.id}/tasks/${tid}`, { method: "DELETE" }).catch((e) =>
            setBannerErr(errorMessage(e))
          );
        }}
      />
    </I18nContext.Provider>
  );
}

function AppView(props: {
  authed: boolean | null;
  locale: "zh" | "en";
  themePref: ThemePref;
  localePref: LocalePref;
  sessions: SessionInfo[];
  activeId: number | null;
  activeSession: SessionInfo | null;
  activeModel: ModelInfo | undefined;
  activeTab: MainTab;
  providers: ProviderInfo[];
  models: ModelInfo[];
  config: AppConfig | null;
  sidebarOpen: boolean;
  sidebarCollapsed: boolean;
  searchOpen: boolean;
  searchInputRef: preact.RefObject<HTMLInputElement | null>;
  items: HistoryItem[];
  hasMore: boolean;
  loadingMore: boolean;
  queuedIds: Set<number>;
  draft: ActiveDraft | null;
  retryNotice: RetryNotice | null;
  tasks: ActiveTask[];
  summary: CallsSummary | null;
  callsById: Map<number, LlmCallInfo>;
  contextRefreshNonce: number;
  sseStatus: SseStatus;
  bannerErr: string;
  composers: Record<number, ComposerDraft>;
  uploadingBySid: Record<number, number>;
  sendingBySid: Record<number, boolean>;
  settingsState: { open: boolean; tab: SettingsTabId; sessionFilter: string };
  onAuthed: () => void;
  onSelectSession: (id: number) => void;
  onSelectTab: (tab: MainTab) => void;
  onCreateSession: () => void;
  onDeleteSession: (id: number) => void;
  onPatchSession: (
    sid: number,
    patch: Partial<Pick<SessionInfo, "title" | "model_id" | "reasoning_effort">>
  ) => void;
  onCycleTheme: () => void;
  onSetThemePref: (pref: ThemePref) => void;
  onSetLocalePref: (pref: LocalePref) => void;
  onToggleSidebarCollapse: () => void;
  onOpenSidebar: () => void;
  onCloseSidebar: () => void;
  onSetSearchOpen: (open: boolean) => void;
  onOpenSettingsTab: (tab: SettingsTabId, sid?: string) => void;
  onCloseSettings: () => void;
  onSignOut: () => void;
  onRefreshCatalog: () => Promise<void>;
  onLoadMore: () => void;
  onWithdraw: (mid: number) => void;
  onClearBannerErr: () => void;
  onResumePaused: () => void;
  onChangeText: (sid: number, text: string) => void;
  onUploadFiles: (sid: number, files: FileList | null) => void;
  onRemoveAttachment: (sid: number, aid: number) => void;
  onSubmitComposer: (sid: number, startRun: boolean) => void;
  onPause: (mode: "soft" | "hard") => void;
  onKillTask: (tid: string) => void;
}) {
  const { t } = useI18n();
  if (props.authed === null) {
    return <div class="gate-wrap"><div class="muted">{t("connecting")}</div></div>;
  }
  if (!props.authed) {
    return <TokenGate onSuccess={props.onAuthed} />;
  }

  return (
    <div class="layout">
      <Sidebar
        sessions={props.sessions}
        activeId={props.activeId}
        open={props.sidebarOpen}
        collapsed={props.sidebarCollapsed}
        searchOpen={props.searchOpen}
        themePref={props.themePref}
        searchInputRef={props.searchInputRef}
        onSelect={props.onSelectSession}
        onCreate={props.onCreateSession}
        onDelete={props.onDeleteSession}
        onCycleTheme={props.onCycleTheme}
        onOpenSettingsTab={(tab) => props.onOpenSettingsTab(tab, "")}
        onToggleCollapse={props.onToggleSidebarCollapse}
        onSetSearchOpen={props.onSetSearchOpen}
        onClose={props.onCloseSidebar}
      />

      <main class="main">
        {props.activeSession ? (
          <>
            <TopBar
              session={props.activeSession}
              sseStatus={props.sseStatus}
              sidebarCollapsed={props.sidebarCollapsed}
              activeTab={props.activeTab}
              itemCount={props.items.length}
              onSelectTab={props.onSelectTab}
              onOpenSidebar={props.onOpenSidebar}
              onOpenSettings={() => props.onOpenSettingsTab("models", "")}
              onPatchSession={props.onPatchSession}
            />
            {(props.bannerErr || props.activeSession.last_error) && (
              <div class="status-banner">
                <div class="row gap-1">
                  <IconAlert size={15} />
                  <span>{props.bannerErr || props.activeSession.last_error}</span>
                </div>
                <div class="row gap-1">
                  {props.activeSession.status === "paused" &&
                    props.activeSession.last_error &&
                    !props.bannerErr && (
                      <button
                        type="button"
                        class="btn-ghost btn-xs"
                        onClick={props.onResumePaused}
                      >
                        <IconPlay size={11} />
                        <span>{t("resume")}</span>
                      </button>
                    )}
                  {props.bannerErr && (
                    <button
                      type="button"
                      class="btn-icon"
                      aria-label={t("dismiss")}
                      onClick={props.onClearBannerErr}
                    >
                      <IconX size={14} />
                    </button>
                  )}
                </div>
              </div>
            )}

            {props.activeTab === "chat" && (
              <>
                <Timeline
                  session={props.activeSession}
                  items={props.items}
                  hasMore={props.hasMore}
                  loadingMore={props.loadingMore}
                  queuedIds={props.queuedIds}
                  draft={props.draft}
                  retryNotice={props.retryNotice}
                  callsById={props.callsById}
                  modelLabel={props.activeModel?.model_key ?? null}
                  effortLabel={props.activeSession.reasoning_effort}
                  onLoadMore={props.onLoadMore}
                  onWithdraw={props.onWithdraw}
                  onOpenSettings={() => props.onOpenSettingsTab("models", "")}
                />
                <Composer
                  session={props.activeSession}
                  providers={props.providers}
                  models={props.models}
                  draft={props.composers[props.activeSession.id] || emptyComposer()}
                  uploading={Boolean(props.uploadingBySid[props.activeSession.id])}
                  sending={Boolean(props.sendingBySid[props.activeSession.id])}
                  tasks={props.tasks}
                  queuedCount={props.queuedIds.size}
                  summary={props.summary}
                  onChangeText={props.onChangeText}
                  onUploadFiles={props.onUploadFiles}
                  onRemoveAttachment={props.onRemoveAttachment}
                  onSubmit={props.onSubmitComposer}
                  onPause={props.onPause}
                  onKillTask={props.onKillTask}
                  onPatchSession={props.onPatchSession}
                  onOpenUsage={(sid) => props.onOpenSettingsTab("usage", String(sid))}
                />
              </>
            )}

            {props.activeTab === "trace" && (
              <TraceView
                items={props.items}
                hasMore={props.hasMore}
                loadingMore={props.loadingMore}
                queuedIds={props.queuedIds}
                draft={props.draft}
                onLoadMore={props.onLoadMore}
                onWithdraw={props.onWithdraw}
              />
            )}

            {props.activeTab === "context" && (
              <ContextView
                session={props.activeSession}
                items={props.items}
                summary={props.summary}
                ctxLimit={props.activeModel?.context_window_tokens}
                refreshNonce={props.contextRefreshNonce}
              />
            )}
          </>
        ) : (
          <div class="welcome">
            <button
              type="button"
              class={`btn-icon welcome-menu ${props.sidebarCollapsed ? "" : "mobile-only"}`}
              aria-label={t("showSessions")}
              onClick={props.onOpenSidebar}
            >
              <IconSidebar size={16} />
            </button>
            <div class="welcome-card">
              <div class="brand">
                <BrandMark size={28} />
                <span class="brand-name">MyGo</span>
                <span class="brand-tag">BOT</span>
              </div>
              <p class="muted">{t("welcomePrompt")}</p>
              {props.models.filter((m) => !m.archived_at && !m.provider_archived_at).length === 0 && (
                <p class="muted small">{t("noModelsNotice")}</p>
              )}
              <div class="row gap-2">
                <button type="button" class="btn-primary" onClick={props.onCreateSession}>
                  <IconPlus size={15} />
                  <span>{t("newSession")}</span>
                </button>
                <button
                  type="button"
                  class="btn-ghost"
                  onClick={() => props.onOpenSettingsTab("models", "")}
                >
                  <IconSettings size={15} />
                  <span>{t("settings")}</span>
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      {props.settingsState.open && (
        <Settings
          config={props.config}
          providers={props.providers}
          models={props.models}
          sessions={props.sessions}
          themePref={props.themePref}
          localePref={props.localePref}
          initialTab={props.settingsState.tab}
          initialSessionFilter={props.settingsState.sessionFilter}
          onSetThemePref={props.onSetThemePref}
          onSetLocalePref={props.onSetLocalePref}
          onRefreshCatalog={props.onRefreshCatalog}
          onSignOut={props.onSignOut}
          onClose={props.onCloseSettings}
        />
      )}
    </div>
  );
}

const root = document.getElementById("app");
if (root) render(<App />, root);
