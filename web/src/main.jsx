import { render } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import {
  apiFetch,
  connectSse,
  consumeHashToken,
  formatCost,
  formatTokens,
  getToken,
  makeIdempotencyKey,
  setToken,
  setUnauthorizedHandler,
} from "./api.js";
import { Composer } from "./Composer.jsx";
import { Settings } from "./Settings.jsx";
import { Sidebar } from "./Sidebar.jsx";
import { Timeline } from "./Timeline.jsx";

function TokenGate({ onSuccess }) {
  const [value, setValue] = useState("");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");

  const submit = async (e) => {
    e.preventDefault();
    const token = value.trim();
    if (!token) return;
    setBusy(true);
    setErr("");
    try {
      await apiFetch("/config", { token });
      setToken(token);
      onSuccess();
    } catch (e) {
      setErr(e.message || "Invalid Bearer token");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div class="gate-wrap">
      <form class="gate-card" onSubmit={submit}>
        <div class="brand">
          <span class="brand-mark">M</span>
          <span>MyGo-Bot</span>
        </div>
        <p class="muted">Enter the daemon TCP Bearer token to connect.</p>
        {err && <div class="err-inline">{err}</div>}
        <input
          type="password"
          placeholder="Bearer token"
          autoFocus
          value={value}
          onInput={(e) => setValue(e.target.value)}
        />
        <button type="submit" class="btn-primary" disabled={busy || !value.trim()}>
          {busy ? "Verifying…" : "Connect"}
        </button>
      </form>
    </div>
  );
}

function TopBar({
  session,
  models,
  summary,
  sseStatus,
  onOpenSidebar,
  onPatchSession,
}) {
  const [title, setTitle] = useState(session?.title || "");
  useEffect(() => {
    setTitle(session?.title || "");
  }, [session?.id, session?.title]);

  if (!session) return null;

  const availableModels = models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const currentModel = models.find((m) => m.id === session.model_id);
  const efforts = currentModel?.reasoning_efforts || [];
  const ctxLimit = currentModel?.context_window_tokens;
  const lastIn = summary?.last_input_tokens;

  const commitTitle = () => {
    const next = title.trim();
    if (next !== session.title) {
      onPatchSession(session.id, { title: next });
    } else {
      setTitle(session.title);
    }
  };

  return (
    <header class="topbar">
      <div class="topbar-left">
        <button type="button" class="btn-icon mobile-only" onClick={onOpenSidebar}>
          ☰
        </button>
        <input
          class="title-input"
          placeholder={`Session #${session.id}`}
          value={title}
          onInput={(e) => setTitle(e.target.value)}
          onBlur={commitTitle}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.isComposing) e.currentTarget.blur();
          }}
        />
        <span class={`pill pill-${session.status}`}>{session.status}</span>
        {sseStatus === "reconnecting" && <span class="pill">reconnecting</span>}
      </div>

      <div class="topbar-right">
        <select
          class="select-sm"
          title="Session Model"
          value={session.model_id ?? ""}
          onChange={(e) => {
            const val = e.target.value ? Number(e.target.value) : null;
            onPatchSession(session.id, { model_id: val, reasoning_effort: null });
          }}
        >
          <option value="">No model</option>
          {availableModels.map((m) => (
            <option key={m.id} value={m.id}>{m.model_key}</option>
          ))}
          {currentModel && (currentModel.archived_at || currentModel.provider_archived_at) && (
            <option value={currentModel.id}>{currentModel.model_key} (archived)</option>
          )}
        </select>

        {efforts.length > 0 && (
          <select
            class="select-sm"
            title="Reasoning Effort"
            value={session.reasoning_effort ?? ""}
            onChange={(e) => onPatchSession(session.id, { reasoning_effort: e.target.value || null })}
          >
            <option value="">auto</option>
            {efforts.map((ef) => (
              <option key={ef} value={ef}>{ef}</option>
            ))}
          </select>
        )}

        {summary && (
          <div class="metrics-pill mono" title="Context tokens / total session cost">
            {lastIn != null && (
              <span>
                {formatTokens(lastIn)}
                {ctxLimit ? `/${formatTokens(ctxLimit)}` : " tok"}
              </span>
            )}
            <span>{formatCost(summary.cost_nano_usd)}</span>
          </div>
        )}
      </div>
    </header>
  );
}

function applyDraftStarted(prev, ev) {
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

function applyDraftDelta(prev, ev) {
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

function dropDraftForSaved(prev, item) {
  if (!prev) return null;
  let removed = false;
  const items = prev.items.filter((d) => {
    if (removed) return true;
    if (item.kind === "function_call" && d.kind === "function_call" && d.call_id === item.payload?.call_id) {
      removed = true;
      return false;
    }
    if (item.kind === "assistant_message" && d.kind === "message") {
      removed = true;
      return false;
    }
    if (item.kind === "reasoning" && d.kind === "reasoning") {
      removed = true;
      return false;
    }
    return true;
  });
  return items.length ? { ...prev, items } : null;
}

function App() {
  const [authed, setAuthed] = useState(null);
  const [theme, setTheme] = useState(() =>
    document.documentElement.dataset.theme === "dark" ? "dark" : "light"
  );
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);

  const [config, setConfig] = useState(null);
  const [providers, setProviders] = useState([]);
  const [models, setModels] = useState([]);
  const [sessions, setSessions] = useState([]);
  const [activeId, setActiveId] = useState(null);

  const [items, setItems] = useState([]);
  const [hasMore, setHasMore] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [queuedIds, setQueuedIds] = useState(() => new Set());
  const [draft, setDraft] = useState(null);
  const [retryNotice, setRetryNotice] = useState(null);
  const [tasks, setTasks] = useState([]);
  const [summary, setSummary] = useState(null);
  const [sseStatus, setSseStatus] = useState("open");
  const [bannerErr, setBannerErr] = useState("");

  const activeIdRef = useRef(null);
  activeIdRef.current = activeId;
  const lastItemIdRef = useRef(0);
  lastItemIdRef.current = items.length ? items[items.length - 1].id : 0;

  const toggleTheme = () => {
    const next = theme === "dark" ? "light" : "dark";
    setTheme(next);
    document.documentElement.dataset.theme = next === "dark" ? "dark" : "";
    try {
      localStorage.setItem("mygo_theme", next);
    } catch {}
  };

  useEffect(() => {
    setUnauthorizedHandler(() => setAuthed(false));
    consumeHashToken();
    apiFetch("/config", { token: getToken() })
      .then((cfg) => {
        setConfig(cfg);
        setAuthed(true);
      })
      .catch((e) => {
        if (e.status === 401) setAuthed(false);
        else setBannerErr(e.message);
      });
  }, []);

  const loadSessions = async () => {
    const all = [];
    let after = 0;
    while (true) {
      const page = await apiFetch(`/sessions?after=${after}&limit=200`);
      all.push(...page);
      if (page.length < 200) break;
      after = page[page.length - 1].id;
    }
    setSessions(all);
    return all;
  };

  const loadCatalog = async () => {
    const [cfg, prov, mods] = await Promise.all([
      apiFetch("/config"),
      apiFetch("/providers?include_archived=true"),
      apiFetch("/models?include_archived=true"),
    ]);
    setConfig(cfg);
    setProviders(prov);
    setModels(mods);
  };

  useEffect(() => {
    if (!authed) return;
    let alive = true;
    Promise.all([loadCatalog(), loadSessions()])
      .then(([, list]) => {
        if (!alive) return;
        let saved = null;
        try {
          saved = Number(localStorage.getItem("mygo_sid")) || null;
        } catch {}
        const chosen = list.find((s) => s.id === saved) || [...list].sort((a, b) => b.updated_at - a.updated_at)[0];
        if (chosen) setActiveId(chosen.id);
      })
      .catch((e) => alive && setBannerErr(e.message));

    const ctrl = new AbortController();
    connectSse("/events", {
      signal: ctrl.signal,
      onEvent: (name, data) => {
        if (name === "session.created") {
          setSessions((prev) => (prev.some((s) => s.id === data.id) ? prev : [...prev, data]));
        } else if (name === "session.updated") {
          setSessions((prev) => prev.map((s) => (s.id === data.id ? data : s)));
        } else if (name === "session.status") {
          setSessions((prev) =>
            prev.map((s) =>
              s.id === data.id
                ? { ...s, status: data.status, last_error: data.reason || "", updated_at: data.updated_at ?? s.updated_at }
                : s
            )
          );
        } else if (name === "session.deleted") {
          setSessions((prev) => prev.filter((s) => s.id !== data.id));
          setActiveId((cur) => (cur === data.id ? null : cur));
        } else if (name === "resync") {
          loadSessions().catch(() => {});
        }
      },
    });
    return () => {
      alive = false;
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

  const refreshSessionSidecars = async (sid) => {
    const [queued, sum, tlist] = await Promise.all([
      apiFetch(`/sessions/${sid}/messages?status=queued&limit=200`),
      apiFetch(`/calls/summary?session_id=${sid}`),
      apiFetch(`/sessions/${sid}/tasks`),
    ]);
    if (activeIdRef.current !== sid) return;
    setQueuedIds(new Set(queued.map((m) => m.id)));
    setSummary(sum);
    setTasks(tlist);
  };

  useEffect(() => {
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

    Promise.all([
      apiFetch(`/sessions/${sid}/history?tail=true&limit=50`),
      refreshSessionSidecars(sid),
    ])
      .then(([hist]) => {
        if (activeIdRef.current !== sid) return;
        setItems(hist);
        setHasMore(hist.length === 50);
      })
      .catch((e) => activeIdRef.current === sid && setBannerErr(e.message));

    const ctrl = new AbortController();
    connectSse(`/sessions/${sid}/events`, {
      signal: ctrl.signal,
      onStatus: setSseStatus,
      onEvent: (name, data) => {
        if (activeIdRef.current !== sid) return;
        if (name === "ready") {
          setSessions((prev) => prev.map((s) => (s.id === data.id ? data : s)));
        } else if (name === "stream.snapshot") {
          setDraft(data);
        } else if (name === "item.started") {
          setDraft((prev) => applyDraftStarted(prev, data));
        } else if (name === "item.delta") {
          setRetryNotice(null);
          setDraft((prev) => applyDraftDelta(prev, data));
        } else if (name === "stream.clear") {
          setDraft((prev) => (prev?.llm_call_id === data?.llm_call_id ? null : prev));
        } else if (name === "attempt.retry") {
          setDraft(null);
          setRetryNotice(data);
        } else if (name === "item.saved") {
          setItems((prev) => (prev.some((x) => x.id === data.id) ? prev : [...prev, data]));
          setDraft((prev) => dropDraftForSaved(prev, data));
          if (data.kind === "user_message") {
            setQueuedIds((prev) => new Set([...prev, data.id]));
          } else if (data.kind === "withdrawal" && data.target_id) {
            setQueuedIds((prev) => {
              const next = new Set(prev);
              next.delete(data.target_id);
              return next;
            });
          } else {
            refreshSessionSidecars(sid).catch(() => {});
          }
        } else if (name === "session.status") {
          setRetryNotice(null);
          if (data.status === "idle" || data.status === "paused") setDraft(null);
          setSessions((prev) =>
            prev.map((s) =>
              s.id === sid
                ? { ...s, status: data.status, last_error: data.reason || "", updated_at: data.updated_at ?? s.updated_at }
                : s
            )
          );
          refreshSessionSidecars(sid).catch(() => {});
        } else if (name === "session.updated") {
          setSessions((prev) => prev.map((s) => (s.id === data.id ? data : s)));
        } else if (name === "resync") {
          const after = lastItemIdRef.current;
          const path = after
            ? `/sessions/${sid}/history?after=${after}&limit=200`
            : `/sessions/${sid}/history?tail=true&limit=50`;
          Promise.all([apiFetch(`/sessions/${sid}`), apiFetch(path), refreshSessionSidecars(sid)])
            .then(([sess, more]) => {
              if (activeIdRef.current !== sid) return;
              setSessions((prev) => prev.map((s) => (s.id === sess.id ? sess : s)));
              if (more.length) {
                setItems((prev) => {
                  const seen = new Set(prev.map((x) => x.id));
                  return [...prev, ...more.filter((x) => !seen.has(x.id))];
                });
              }
            })
            .catch(() => {});
        }
      },
    });

    return () => ctrl.abort();
  }, [authed, activeId]);

  const activeSession = useMemo(
    () => sessions.find((s) => s.id === activeId) || null,
    [sessions, activeId]
  );

  const shouldPollTasks =
    Boolean(authed && activeId) &&
    (tasks.length > 0 || activeSession?.status === "running" || activeSession?.status === "pausing");

  useEffect(() => {
    if (!shouldPollTasks) return;
    const sid = activeId;
    const timer = setInterval(() => {
      apiFetch(`/sessions/${sid}/tasks`)
        .then((list) => activeIdRef.current === sid && setTasks(list))
        .catch(() => {});
    }, 2000);
    return () => clearInterval(timer);
  }, [shouldPollTasks, activeId]);

  const modelsById = useMemo(() => new Map(models.map((m) => [m.id, m])), [models]);

  const createSession = async () => {
    setBannerErr("");
    try {
      const s = await apiFetch("/sessions", { method: "POST", body: {} });
      setSessions((prev) => (prev.some((x) => x.id === s.id) ? prev : [...prev, s]));
      setActiveId(s.id);
    } catch (e) {
      setBannerErr(e.message);
    }
  };

  const deleteSession = async (sid) => {
    setBannerErr("");
    try {
      await apiFetch(`/sessions/${sid}`, { method: "DELETE" });
      setSessions((prev) => prev.filter((s) => s.id !== sid));
      if (activeId === sid) setActiveId(null);
    } catch (e) {
      setBannerErr(e.message);
    }
  };

  const patchSession = async (sid, patch) => {
    setBannerErr("");
    try {
      const updated = await apiFetch(`/sessions/${sid}`, { method: "PATCH", body: patch });
      setSessions((prev) => prev.map((s) => (s.id === updated.id ? updated : s)));
    } catch (e) {
      setBannerErr(e.message);
    }
  };

  const loadMore = async () => {
    if (!activeId || !items.length || loadingMore) return;
    const sid = activeId;
    const firstId = items[0].id;
    setLoadingMore(true);
    try {
      const older = await apiFetch(`/sessions/${sid}/history?before=${firstId}&limit=50`);
      if (activeIdRef.current !== sid) return;
      setItems((prev) => {
        const seen = new Set(prev.map((x) => x.id));
        return [...older.filter((x) => !seen.has(x.id)), ...prev];
      });
      setHasMore(older.length === 50);
    } catch (e) {
      setBannerErr(e.message);
    } finally {
      setLoadingMore(false);
    }
  };

  const sendMessage = async (content, startRun) => {
    const sid = activeId;
    await apiFetch(`/sessions/${sid}/messages`, {
      method: "POST",
      headers: { "Idempotency-Key": makeIdempotencyKey() },
      body: { content },
    });
    if (startRun) {
      await apiFetch(`/sessions/${sid}/run`, { method: "PUT" });
    }
  };

  if (authed === null) {
    return <div class="gate-wrap"><div class="muted">Connecting…</div></div>;
  }
  if (!authed) {
    return <TokenGate onSuccess={() => setAuthed(true)} />;
  }

  return (
    <div class="layout">
      <Sidebar
        sessions={sessions}
        activeId={activeId}
        modelsById={modelsById}
        open={sidebarOpen}
        theme={theme}
        onSelect={setActiveId}
        onCreate={createSession}
        onDelete={deleteSession}
        onToggleTheme={toggleTheme}
        onOpenSettings={() => setSettingsOpen(true)}
        onClose={() => setSidebarOpen(false)}
      />

      <main class="main">
        {activeSession ? (
          <>
            <TopBar
              session={activeSession}
              models={models}
              summary={summary}
              sseStatus={sseStatus}
              onOpenSidebar={() => setSidebarOpen(true)}
              onPatchSession={patchSession}
            />
            {(bannerErr || activeSession.last_error) && (
              <div class="status-banner">
                <span>{bannerErr || activeSession.last_error}</span>
                {bannerErr && (
                  <button type="button" class="link-btn" onClick={() => setBannerErr("")}>
                    Dismiss
                  </button>
                )}
              </div>
            )}
            <Timeline
              session={activeSession}
              items={items}
              hasMore={hasMore}
              loadingMore={loadingMore}
              queuedIds={queuedIds}
              draft={draft}
              retryNotice={retryNotice}
              onLoadMore={loadMore}
              onWithdraw={(mid) => apiFetch(`/sessions/${activeSession.id}/messages/${mid}`, { method: "DELETE" })}
            />
            <Composer
              session={activeSession}
              tasks={tasks}
              queuedCount={queuedIds.size}
              onSend={sendMessage}
              onRun={() => apiFetch(`/sessions/${activeSession.id}/run`, { method: "PUT" })}
              onPause={(mode) => apiFetch(`/sessions/${activeSession.id}/run?mode=${mode}`, { method: "DELETE" })}
              onKillTask={(tid) => apiFetch(`/sessions/${activeSession.id}/tasks/${tid}`, { method: "DELETE" })}
            />
          </>
        ) : (
          <div class="welcome">
            <button type="button" class="btn-icon mobile-only welcome-menu" onClick={() => setSidebarOpen(true)}>
              ☰
            </button>
            <div class="welcome-card">
              <div class="brand">
                <span class="brand-mark">M</span>
                <span>MyGo-Bot</span>
              </div>
              <p class="muted">Select a session from the sidebar or create a new one.</p>
              {models.filter((m) => !m.archived_at && !m.provider_archived_at).length === 0 && (
                <p class="muted small">No available models configured yet — open Settings to add a provider and model.</p>
              )}
              <div class="row gap-2">
                <button type="button" class="btn-primary" onClick={createSession}>
                  + New Session
                </button>
                <button type="button" class="btn-ghost" onClick={() => setSettingsOpen(true)}>
                  Settings
                </button>
              </div>
            </div>
          </div>
        )}
      </main>

      {settingsOpen && (
        <Settings
          config={config}
          providers={providers}
          models={models}
          theme={theme}
          onToggleTheme={toggleTheme}
          onRefreshCatalog={loadCatalog}
          onSignOut={() => {
            setToken("");
            setSettingsOpen(false);
            setAuthed(false);
          }}
          onClose={() => setSettingsOpen(false)}
        />
      )}
    </div>
  );
}

render(<App />, document.getElementById("app"));
