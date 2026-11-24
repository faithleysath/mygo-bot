import type { ComponentChildren } from "preact";
import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { apiFetch, errorMessage, formatTokens } from "./api.ts";
import { HighlightedCode } from "./highlight.tsx";
import { formatBytes, useI18n, utf8ByteLength } from "./i18n.ts";
import { Icon } from "./Icons.tsx";
import { ContentBlocks, CopyBtn, extractMessageText, extractReasoningText } from "./Timeline.tsx";
import type {
  ActiveDraft,
  CallsSummary,
  ContentBlock,
  ContextItem,
  HistoryItem,
  HistoryPayload,
  SessionInfo,
} from "./types.ts";

export function Lightbox({ src, onClose }: { src: string | null; onClose: () => void }) {
  const { t } = useI18n();
  useEffect(() => {
    if (!src) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [src, onClose]);

  if (!src) return null;
  return (
    <div class="lightbox" onClick={onClose}>
      <button type="button" class="lightbox-close" aria-label={t("Close preview")} onClick={onClose}>
        <Icon n="x" s={18} />
      </button>
      <img src={src} alt="Preview" onClick={(e) => e.stopPropagation()} />
    </div>
  );
}

function mediaBlocksFromPayload(p: HistoryPayload): ContentBlock[] {
  const candidates = Array.isArray(p.content) ? p.content : Array.isArray(p.output) ? p.output : null;
  if (!candidates) return [];
  return candidates.filter(
    (b) => b && typeof b === "object" && (b.type === "image" || b.type === "audio" || b.type === "video") && b.attachment_id
  );
}

function summarizePayload(kind: string, p: HistoryPayload): string {
  if (kind === "function_call") {
    return `${p.name || "tool"} ${typeof p.arguments === "string" ? p.arguments : ""}`.replace(/\s+/g, " ").trim();
  }
  if (kind === "function_call_output") {
    const out = p.output;
    if (typeof out === "string") return out.replace(/\s+/g, " ").trim();
    const txt = extractMessageText(out).replace(/\s+/g, " ").trim();
    if (txt) return txt;
  }
  if (kind === "codemode_event") {
    const head = `${p.task_id || ""} ${p.event || ""} ${p.sequence != null ? `#${p.sequence}` : ""}`.trim();
    const txt = extractMessageText(p.content).replace(/\s+/g, " ").trim();
    if (txt) return `${head} · ${txt}`;
    const media = mediaBlocksFromPayload(p);
    return media.length ? `${head} · ${media.map((b) => `[${b.type} ${b.attachment_id}]`).join(" ")}` : head;
  }
  if (kind === "reasoning") {
    return extractReasoningText(p).replace(/\s+/g, " ").trim();
  }
  if (kind === "withdrawal") return "";
  const txt = extractMessageText(p.content).replace(/\s+/g, " ").trim();
  if (txt) return txt;
  const media = mediaBlocksFromPayload(p);
  return media.length ? media.map((b) => `[${b.type} ${b.attachment_id}]`).join(" ") : "";
}

interface InspectorRowData {
  key: string | number;
  kind: string;
  payload: HistoryPayload;
  searchText?: string;
  badges: ComponentChildren;
  actions?: ComponentChildren;
}

function InspectorShell({
  rows,
  kindOrder,
  totalCount,
  searchPlaceholder,
  emptyText,
  loading,
  hasMore,
  loadingMore,
  leadingPill,
  extraControls,
  topBanner,
  tail,
  onLoadMore,
}: {
  rows: InspectorRowData[];
  kindOrder?: string[];
  totalCount: number;
  searchPlaceholder: string;
  emptyText: string;
  loading?: boolean;
  hasMore: boolean;
  loadingMore: boolean;
  leadingPill?: ComponentChildren;
  extraControls?: ComponentChildren;
  topBanner?: ComponentChildren;
  tail?: ComponentChildren;
  onLoadMore: () => void;
}) {
  const { t } = useI18n();
  const [kindFilter, setKindFilter] = useState("all");
  const [query, setQuery] = useState("");
  const [expandState, setExpandState] = useState({ open: false, epoch: 0 });
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  const counts = new Map<string, number>();
  for (const r of rows) counts.set(r.kind, (counts.get(r.kind) || 0) + 1);
  const presentKinds = kindOrder
    ? [...kindOrder.filter((k) => counts.has(k)), ...[...counts.keys()].filter((k) => !kindOrder.includes(k))]
    : [...counts.keys()];
  const activeKind = kindFilter !== "all" && counts.has(kindFilter) ? kindFilter : "all";

  useEffect(() => {
    if (kindFilter !== "all" && !counts.has(kindFilter)) setKindFilter("all");
  }, [kindFilter, rows]);

  const q = query.trim().toLowerCase();
  const filtered = rows.filter((r) => {
    if (activeKind !== "all" && r.kind !== activeKind) return false;
    if (!q) return true;
    return (
      String(r.key).toLowerCase().includes(q) ||
      r.kind.toLowerCase().includes(q) ||
      (r.searchText ? r.searchText.toLowerCase().includes(q) : false) ||
      JSON.stringify(r.payload).toLowerCase().includes(q)
    );
  });

  return (
    <div class="inspector-wrap">
      <div class="view-toolbar">
        <div class="view-toolbar-inner">
          <div class="filter-pills">
            {leadingPill}
            <button
              type="button"
              class={`filter-pill ${activeKind === "all" ? "active" : ""}`}
              onClick={() => setKindFilter("all")}
            >
              {t("All")} <span class="mono">{totalCount}</span>
            </button>
            {presentKinds.map((k) => (
              <button
                key={k}
                type="button"
                class={`filter-pill ${activeKind === k ? "active" : ""}`}
                onClick={() => setKindFilter(activeKind === k ? "all" : k)}
              >
                <span class="mono">{k}</span> <span class="mono">{counts.get(k)}</span>
              </button>
            ))}
          </div>

          <div class="toolbar-controls">
            <div class="search-box compact">
              <Icon n="search" s={13} />
              <input
                type="text"
                value={query}
                placeholder={searchPlaceholder}
                onInput={(e) => setQuery(e.currentTarget.value)}
              />
              {query && (
                <button type="button" class="search-clear" onClick={() => setQuery("")}>
                  <Icon n="x" s={12} />
                </button>
              )}
            </div>
            <button
              type="button"
              class="btn-ghost btn-xs"
              onClick={() => setExpandState((s) => ({ open: !s.open, epoch: s.epoch + 1 }))}
            >
              <Icon n="expand" s={12} />
              <span>{expandState.open ? t("Collapse all") : t("Expand all")}</span>
            </button>
            {extraControls}
            {hasMore && (
              <button type="button" class="btn-ghost btn-xs" disabled={loadingMore} onClick={onLoadMore}>
                {loadingMore ? t("Loading…") : t("Load earlier")}
              </button>
            )}
          </div>
        </div>
      </div>

      <div class="inspector-scroll">
        <div class="inspector-inner">
          {topBanner}
          {loading && totalCount === 0 && <div class="empty-hint">{t("Loading…")}</div>}
          {!loading && totalCount === 0 && !tail && <div class="empty-hint">{emptyText}</div>}
          {totalCount > 0 && filtered.length === 0 && (
            <div class="empty-hint">{t("No matching records")}</div>
          )}

          {filtered.map((row) => {
            const json = JSON.stringify(row.payload, null, 2);
            const preview = summarizePayload(row.kind, row.payload);
            const media = mediaBlocksFromPayload(row.payload);
            return (
              <details
                key={`${row.key}-${expandState.epoch}`}
                class="card trace-row"
                open={expandState.open}
              >
                <summary class="card-head">
                  <Icon n="chevron" s={13} cls="chev" />
                  {row.badges}
                  {preview && <span class="muted truncate">{preview}</span>}
                  <span class="card-spacer" />
                  <span class="muted small mono">{formatBytes(utf8ByteLength(json))}</span>
                </summary>
                <div class="card-body">
                  <div class="section-bar">
                    <span class="section-label">{t("Payload JSON")}</span>
                    <div class="row gap-1">
                      {row.actions}
                      <CopyBtn text={json} label={t("Copy")} />
                    </div>
                  </div>
                  <pre class="code-box">
                    <code><HighlightedCode code={json} lang="json" /></code>
                  </pre>
                  {media.length > 0 && <ContentBlocks blocks={media} onPreviewImage={setLightboxSrc} />}
                </div>
              </details>
            );
          })}

          {tail}
        </div>
      </div>

      <Lightbox src={lightboxSrc} onClose={() => setLightboxSrc(null)} />
    </div>
  );
}

const TRACE_KIND_ORDER = [
  "user_message",
  "assistant_message",
  "reasoning",
  "function_call",
  "function_call_output",
  "codemode_event",
  "withdrawal",
];

export function TraceView({
  items,
  hasMore,
  loadingMore,
  queuedIds,
  draft,
  onLoadMore,
  onWithdraw,
}: {
  items: HistoryItem[];
  hasMore: boolean;
  loadingMore: boolean;
  queuedIds: Set<string>;
  draft: ActiveDraft | null;
  onLoadMore: () => void;
  onWithdraw: (mid: string) => void;
}) {
  const { t } = useI18n();
  const withdrawn = useMemo(() => {
    const s = new Set<string>();
    for (const it of items) if (it.kind === "withdrawal" && it.target_id) s.add(it.target_id);
    return s;
  }, [items]);

  const rows: InspectorRowData[] = items.map((item) => {
    const isWithdrawn = item.kind === "user_message" && withdrawn.has(item.id);
    const isQueued = item.kind === "user_message" && !isWithdrawn && queuedIds.has(item.id);
    return {
      key: item.id,
      kind: item.kind,
      payload: item.payload || {},
      searchText: [item.id, item.target_id, item.llm_call_id].filter(Boolean).join(" "),
      badges: (
        <>
          <span class="pill mono">{item.id}</span>
          <span class={`kind-badge kind-${item.kind} mono`}>{item.kind}</span>
          {item.target_id != null && <span class="pill mono">→ {item.target_id}</span>}
          {item.llm_call_id != null && <span class="muted small mono">call {item.llm_call_id}</span>}
          {isWithdrawn && <span class="pill">{t("withdrawn")}</span>}
          {isQueued && <span class="pill pill-queued">{t("queued")}</span>}
        </>
      ),
      actions: isQueued ? (
        <button type="button" class="btn-ghost btn-xs" onClick={() => onWithdraw(item.id)}>
          {t("Withdraw")}
        </button>
      ) : undefined,
    };
  });

  const tail = draft?.items?.length
    ? draft.items.map((d) => (
        <details key={`live-${d.output_index}`} class="card trace-row streaming" open>
          <summary class="card-head">
            <Icon n="chevron" s={13} cls="chev" />
            <span class="pill pill-running">
              <span class="pulse-dot" />
              {t("LIVE")}
            </span>
            <span class="kind-badge mono">{d.kind}</span>
            {d.name && <span class="mono tool-name">{d.name}</span>}
            <span class="muted small mono">call {draft.llm_call_id}</span>
          </summary>
          <div class="card-body">
            <pre class="code-box">
              <code><HighlightedCode code={JSON.stringify(d, null, 2)} lang="json" /></code>
            </pre>
          </div>
        </details>
      ))
    : undefined;

  return (
    <InspectorShell
      rows={rows}
      kindOrder={TRACE_KIND_ORDER}
      totalCount={items.length}
      searchPlaceholder={t("Filter id, kind, text…")}
      emptyText={t("No items recorded in this session yet.")}
      hasMore={hasMore}
      loadingMore={loadingMore}
      tail={tail}
      onLoadMore={onLoadMore}
    />
  );
}

function inferContextKind(c: ContextItem, itemsById: Map<string, HistoryItem>): string {
  const known = itemsById.get(c.item_id)?.kind;
  if (known) return known;
  const p = c.payload || {};
  if (p.role === "user") return "user_message";
  if (p.role === "assistant") return "assistant_message";
  if (typeof p.event === "string" && typeof p.task_id === "string") return "codemode_event";
  if (typeof p.type === "string" && p.type) return p.type;
  return "item";
}

function mergeContext(prev: ContextItem[], incoming: ContextItem[]): ContextItem[] {
  if (!incoming.length) return prev;
  const map = new Map<number, ContextItem>();
  for (const x of prev) map.set(x.position, x);
  for (const x of incoming) map.set(x.position, x);
  return [...map.values()].sort((a, b) => a.position - b.position);
}

export function ContextView({
  session,
  items,
  summary,
  ctxLimit,
  refreshNonce,
}: {
  session: SessionInfo;
  items: HistoryItem[];
  summary: CallsSummary | null;
  ctxLimit: number | null | undefined;
  refreshNonce: number;
}) {
  const { t } = useI18n();
  const [ctxItems, setCtxItems] = useState<ContextItem[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [err, setErr] = useState("");
  const sidRef = useRef(session.id);
  sidRef.current = session.id;
  const refreshNonceRef = useRef(refreshNonce);
  refreshNonceRef.current = refreshNonce;
  const lastPosRef = useRef(0);
  const loadedOnceRef = useRef(false);
  const initSeqRef = useRef(0);
  const loadMoreSeqRef = useRef(0);
  const catchupSeqRef = useRef(0);

  const itemsById = useMemo(() => new Map(items.map((it) => [it.id, it])), [items]);

  const fetchAfterCurrent = async (sid: string, initSeq: number) => {
    const myCatchup = ++catchupSeqRef.current;
    try {
      while (
        sidRef.current === sid &&
        initSeqRef.current === initSeq &&
        catchupSeqRef.current === myCatchup
      ) {
        const page = await apiFetch<ContextItem[]>(
          `/sessions/${sid}/context?after=${lastPosRef.current}&limit=200`
        );
        if (
          sidRef.current !== sid ||
          initSeqRef.current !== initSeq ||
          catchupSeqRef.current !== myCatchup
        ) {
          return;
        }
        if (page.length) {
          setCtxItems((prev) => mergeContext(prev, page));
          lastPosRef.current = Math.max(lastPosRef.current, page[page.length - 1].position);
        }
        if (page.length < 200) break;
      }
    } catch {}
  };

  const loadInitial = async (sid: string) => {
    const myInit = ++initSeqRef.current;
    loadMoreSeqRef.current += 1;
    catchupSeqRef.current += 1;
    loadedOnceRef.current = false;
    setLoadingMore(false);
    setLoading(true);
    setErr("");
    const startNonce = refreshNonceRef.current;
    try {
      const page = await apiFetch<ContextItem[]>(`/sessions/${sid}/context?tail=true&limit=50`);
      if (sidRef.current !== sid || initSeqRef.current !== myInit) return;
      setCtxItems(page);
      setHasMore(page.length === 50);
      lastPosRef.current = page.length ? page[page.length - 1].position : 0;
      loadedOnceRef.current = true;
      if (refreshNonceRef.current !== startNonce) {
        void fetchAfterCurrent(sid, myInit);
      }
    } catch (e) {
      if (sidRef.current === sid && initSeqRef.current === myInit) setErr(errorMessage(e));
    } finally {
      if (sidRef.current === sid && initSeqRef.current === myInit) setLoading(false);
    }
  };

  useEffect(() => {
    lastPosRef.current = 0;
    setCtxItems([]);
    setHasMore(false);
    setLoadingMore(false);
    void loadInitial(session.id);
  }, [session.id]);

  useEffect(() => {
    if (!loadedOnceRef.current) return;
    void fetchAfterCurrent(session.id, initSeqRef.current);
    return () => {
      catchupSeqRef.current += 1;
    };
  }, [refreshNonce, session.id]);

  const loadEarlier = async () => {
    if (!ctxItems.length || loadingMore) return;
    const sid = session.id;
    const myInit = initSeqRef.current;
    const myMore = ++loadMoreSeqRef.current;
    const firstPos = ctxItems[0].position;
    setLoadingMore(true);
    setErr("");
    try {
      const older = await apiFetch<ContextItem[]>(
        `/sessions/${sid}/context?before=${firstPos}&limit=50`
      );
      if (
        sidRef.current !== sid ||
        initSeqRef.current !== myInit ||
        loadMoreSeqRef.current !== myMore
      ) {
        return;
      }
      setCtxItems((prev) => mergeContext(prev, older));
      setHasMore(older.length === 50);
    } catch (e) {
      if (
        sidRef.current === sid &&
        initSeqRef.current === myInit &&
        loadMoreSeqRef.current === myMore
      ) {
        setErr(errorMessage(e));
      }
    } finally {
      if (
        sidRef.current === sid &&
        initSeqRef.current === myInit &&
        loadMoreSeqRef.current === myMore
      ) {
        setLoadingMore(false);
      }
    }
  };

  const rows: InspectorRowData[] = ctxItems.map((c) => {
    const kind = inferContextKind(c, itemsById);
    return {
      key: c.position,
      kind,
      payload: c.payload || {},
      searchText: [`pos #${c.position}`, c.item_id, c.llm_call_id].filter(Boolean).join(" "),
      badges: (
        <>
          <span class="pill mono">pos #{c.position}</span>
          <span class="muted small mono">item {c.item_id}</span>
          <span class={`kind-badge kind-${kind} mono`}>{kind}</span>
          {c.llm_call_id != null && <span class="muted small mono">call {c.llm_call_id}</span>}
        </>
      ),
    };
  });

  const lastIn = summary?.last_input_tokens;
  const pct =
    lastIn != null && ctxLimit ? Math.min(100, Math.round((lastIn / ctxLimit) * 100)) : null;

  return (
    <InspectorShell
      rows={rows}
      totalCount={ctxItems.length}
      searchPlaceholder={t("Filter pos, item_id, text…")}
      emptyText={t("Context window is currently empty.")}
      loading={loading}
      hasMore={hasMore}
      loadingMore={loadingMore}
      leadingPill={
        lastIn != null ? (
          <span class="ctx-chip mono">
            {t("Context window")}: {formatTokens(lastIn)}
            {ctxLimit ? ` / ${formatTokens(ctxLimit)} (${pct}%)` : " tok"}
          </span>
        ) : undefined
      }
      extraControls={
        <button
          type="button"
          class="btn-ghost btn-xs"
          disabled={loading}
          onClick={() => void loadInitial(session.id)}
        >
          <Icon n="refresh" s={12} />
          <span>{t("Refresh")}</span>
        </button>
      }
      topBanner={
        <>
          {err && <div class="err-inline">{err}</div>}
          {!hasMore && (
            <div class="info-strip">
              {t("Static developer instructions and tool definitions are prepended dynamically per call and not stored in context_items.")}
            </div>
          )}
        </>
      }
      onLoadMore={() => void loadEarlier()}
    />
  );
}
