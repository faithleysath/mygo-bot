import { useEffect, useMemo, useRef, useState } from "preact/hooks";
import { apiFetch, errorMessage, formatTokens } from "./api.ts";
import { HighlightedCode } from "./highlight.tsx";
import { formatBytes, useI18n } from "./i18n.ts";
import { IconChevron, IconExpand, IconRefresh, IconSearch, IconX } from "./Icons.tsx";
import { ContentBlocks, CopyBtn, extractMessageText } from "./Timeline.tsx";
import type { CallsSummary, ContentBlock, ContextItem, HistoryItem, HistoryPayload, SessionInfo } from "./types.ts";

function inferContextKind(
  c: ContextItem,
  itemsById: Map<number, HistoryItem>
): string {
  const known = itemsById.get(c.item_id)?.kind;
  if (known) return known;
  const p = c.payload || {};
  if (p.role === "user") return "user_message";
  if (p.role === "assistant") return "assistant_message";
  if (typeof p.event === "string" && typeof p.task_id === "string") return "codemode_event";
  if (typeof p.type === "string" && p.type) return p.type;
  return "item";
}

function summarizeContext(kind: string, p: HistoryPayload): string {
  if (kind === "function_call") {
    return `${p.name || "tool"} ${typeof p.arguments === "string" ? p.arguments : ""}`
      .replace(/\s+/g, " ")
      .trim();
  }
  if (kind === "function_call_output") {
    const out = p.output;
    if (typeof out === "string") return out.replace(/\s+/g, " ").trim();
    return extractMessageText(out).replace(/\s+/g, " ").trim();
  }
  if (kind === "codemode_event") {
    const head = `${p.task_id || ""} ${p.event || ""}`.trim();
    const txt = extractMessageText(p.content).replace(/\s+/g, " ").trim();
    return txt ? `${head} · ${txt}` : head;
  }
  if (kind === "reasoning") {
    const parts: string[] = [];
    for (const k of ["summary", "content"] as const) {
      const arr = p[k];
      if (Array.isArray(arr)) {
        for (const chunk of arr) {
          if (chunk && typeof chunk === "object" && typeof chunk.text === "string") {
            parts.push(chunk.text);
          }
        }
      }
    }
    return parts.join(" ").replace(/\s+/g, " ").trim();
  }
  const txt = extractMessageText(p.content).replace(/\s+/g, " ").trim();
  if (txt) return txt;
  const media = mediaBlocksFromPayload(p);
  return media.length ? media.map((b) => `[${b.type}#${b.attachment_id}]`).join(" ") : "";
}

function mergeContext(prev: ContextItem[], incoming: ContextItem[]): ContextItem[] {
  if (!incoming.length) return prev;
  const map = new Map<number, ContextItem>();
  for (const x of prev) map.set(x.position, x);
  for (const x of incoming) map.set(x.position, x);
  return [...map.values()].sort((a, b) => a.position - b.position);
}

function mediaBlocksFromPayload(p: HistoryPayload): ContentBlock[] {
  const candidates = Array.isArray(p.content) ? p.content : Array.isArray(p.output) ? p.output : null;
  if (!candidates) return [];
  return candidates.filter(
    (b) => b && typeof b === "object" && (b.type === "image" || b.type === "audio" || b.type === "video") && b.attachment_id
  );
}

export interface ContextViewProps {
  session: SessionInfo;
  items: HistoryItem[];
  summary: CallsSummary | null;
  ctxLimit: number | null | undefined;
  refreshNonce: number;
}

export function ContextView({
  session,
  items,
  summary,
  ctxLimit,
  refreshNonce,
}: ContextViewProps) {
  const { t } = useI18n();
  const [ctxItems, setCtxItems] = useState<ContextItem[]>([]);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);
  const [err, setErr] = useState("");
  const [kindFilter, setKindFilter] = useState("all");
  const [query, setQuery] = useState("");
  const [expandState, setExpandState] = useState<{ open: boolean; epoch: number }>({
    open: false,
    epoch: 0,
  });
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  const sidRef = useRef(session.id);
  sidRef.current = session.id;
  const lastPosRef = useRef(0);
  const loadedOnceRef = useRef(false);

  const itemsById = useMemo(() => new Map(items.map((it) => [it.id, it])), [items]);

  const loadInitial = async (sid: number) => {
    setLoading(true);
    setErr("");
    try {
      const page = await apiFetch<ContextItem[]>(`/sessions/${sid}/context?tail=true&limit=50`);
      if (sidRef.current !== sid) return;
      setCtxItems(page);
      setHasMore(page.length === 50);
      lastPosRef.current = page.length ? page[page.length - 1].position : 0;
      loadedOnceRef.current = true;
    } catch (e) {
      if (sidRef.current === sid) setErr(errorMessage(e));
    } finally {
      if (sidRef.current === sid) setLoading(false);
    }
  };

  useEffect(() => {
    loadedOnceRef.current = false;
    lastPosRef.current = 0;
    setCtxItems([]);
    setHasMore(false);
    void loadInitial(session.id);
  }, [session.id]);

  useEffect(() => {
    if (!loadedOnceRef.current) return;
    const sid = session.id;
    let cancelled = false;
    void (async () => {
      try {
        while (!cancelled && sidRef.current === sid) {
          const page = await apiFetch<ContextItem[]>(
            `/sessions/${sid}/context?after=${lastPosRef.current}&limit=200`
          );
          if (cancelled || sidRef.current !== sid) return;
          if (page.length) {
            setCtxItems((prev) => mergeContext(prev, page));
            lastPosRef.current = Math.max(lastPosRef.current, page[page.length - 1].position);
          }
          if (page.length < 200) break;
        }
      } catch {}
    })();
    return () => {
      cancelled = true;
    };
  }, [refreshNonce, session.id]);

  useEffect(() => {
    if (!lightboxSrc) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setLightboxSrc(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [lightboxSrc]);

  const loadEarlier = async () => {
    if (!ctxItems.length || loadingMore) return;
    const sid = session.id;
    const firstPos = ctxItems[0].position;
    setLoadingMore(true);
    setErr("");
    try {
      const older = await apiFetch<ContextItem[]>(
        `/sessions/${sid}/context?before=${firstPos}&limit=50`
      );
      if (sidRef.current !== sid) return;
      setCtxItems((prev) => mergeContext(prev, older));
      setHasMore(older.length === 50);
    } catch (e) {
      if (sidRef.current === sid) setErr(errorMessage(e));
    } finally {
      if (sidRef.current === sid) setLoadingMore(false);
    }
  };

  const tagged = ctxItems.map((c) => ({
    item: c,
    kind: inferContextKind(c, itemsById),
  }));

  const counts = new Map<string, number>();
  for (const row of tagged) {
    counts.set(row.kind, (counts.get(row.kind) || 0) + 1);
  }
  const presentKinds = [...counts.keys()];

  const q = query.trim().toLowerCase();
  const filtered = tagged.filter(({ item, kind }) => {
    if (kindFilter !== "all" && kind !== kindFilter) return false;
    if (!q) return true;
    return (
      `#${item.position}`.includes(q) ||
      `#${item.item_id}`.includes(q) ||
      String(item.position).includes(q) ||
      String(item.item_id).includes(q) ||
      kind.toLowerCase().includes(q) ||
      JSON.stringify(item.payload).toLowerCase().includes(q)
    );
  });

  const lastIn = summary?.last_input_tokens;
  const pct =
    lastIn != null && ctxLimit ? Math.min(100, Math.round((lastIn / ctxLimit) * 100)) : null;

  return (
    <div class="inspector-wrap">
      <div class="view-toolbar">
        <div class="view-toolbar-inner">
          <div class="filter-pills">
            {lastIn != null && (
              <span class="ctx-chip mono">
                {t("ctxOccupied")}: {formatTokens(lastIn)}
                {ctxLimit ? ` / ${formatTokens(ctxLimit)} (${pct}%)` : " tok"}
              </span>
            )}
            <button
              type="button"
              class={`filter-pill ${kindFilter === "all" ? "active" : ""}`}
              onClick={() => setKindFilter("all")}
            >
              {t("all")} <span class="mono">{ctxItems.length}</span>
            </button>
            {presentKinds.map((k) => (
              <button
                key={k}
                type="button"
                class={`filter-pill ${kindFilter === k ? "active" : ""}`}
                onClick={() => setKindFilter(kindFilter === k ? "all" : k)}
              >
                <span class="mono">{k}</span> <span class="mono">{counts.get(k)}</span>
              </button>
            ))}
          </div>

          <div class="toolbar-controls">
            <div class="search-box compact">
              <IconSearch size={13} />
              <input
                type="text"
                value={query}
                placeholder={t("filterContext")}
                onInput={(e) => setQuery(e.currentTarget.value)}
              />
              {query && (
                <button type="button" class="search-clear" onClick={() => setQuery("")}>
                  <IconX size={12} />
                </button>
              )}
            </div>
            <button
              type="button"
              class="btn-ghost btn-xs"
              onClick={() =>
                setExpandState((s) => ({ open: !s.open, epoch: s.epoch + 1 }))
              }
            >
              <IconExpand size={12} />
              <span>{expandState.open ? t("collapseAll") : t("expandAll")}</span>
            </button>
            <button
              type="button"
              class="btn-ghost btn-xs"
              disabled={loading}
              onClick={() => void loadInitial(session.id)}
            >
              <IconRefresh size={12} />
              <span>{t("refresh")}</span>
            </button>
            {hasMore && (
              <button
                type="button"
                class="btn-ghost btn-xs"
                disabled={loadingMore}
                onClick={() => void loadEarlier()}
              >
                {loadingMore ? t("loading") : t("loadEarlier")}
              </button>
            )}
          </div>
        </div>
      </div>

      <div class="inspector-scroll">
        <div class="inspector-inner">
          {err && <div class="err-inline">{err}</div>}
          {!hasMore && <div class="info-strip">{t("contextPrefixNote")}</div>}
          {loading && ctxItems.length === 0 && <div class="empty-hint">{t("loading")}</div>}
          {!loading && ctxItems.length === 0 && <div class="empty-hint">{t("contextEmpty")}</div>}
          {ctxItems.length > 0 && filtered.length === 0 && (
            <div class="empty-hint">{t("noMatchingItems")}</div>
          )}

          {filtered.map(({ item, kind }) => {
            const json = JSON.stringify(item.payload, null, 2);
            const preview = summarizeContext(kind, item.payload || {});
            const media = mediaBlocksFromPayload(item.payload || {});
            return (
              <details
                key={`${item.position}-${expandState.epoch}`}
                class="card trace-row"
                open={expandState.open}
              >
                <summary class="card-head">
                  <IconChevron size={13} cls="chev" />
                  <span class="pill mono">pos #{item.position}</span>
                  <span class="muted small mono">item #{item.item_id}</span>
                  <span class={`kind-badge kind-${kind} mono`}>{kind}</span>
                  {item.llm_call_id != null && (
                    <span class="muted small mono">call #{item.llm_call_id}</span>
                  )}
                  {preview && <span class="muted truncate">{preview}</span>}
                  <span class="card-spacer" />
                  <span class="muted small mono">{formatBytes(json.length)}</span>
                </summary>
                <div class="card-body">
                  <div class="section-bar">
                    <span class="section-label">{t("payloadJson")}</span>
                    <CopyBtn text={json} label={t("copy")} />
                  </div>
                  <pre class="code-box">
                    <code><HighlightedCode code={json} lang="json" /></code>
                  </pre>
                  {media.length > 0 && (
                    <ContentBlocks blocks={media} onPreviewImage={setLightboxSrc} />
                  )}
                </div>
              </details>
            );
          })}
        </div>
      </div>

      {lightboxSrc && (
        <div class="lightbox" onClick={() => setLightboxSrc(null)}>
          <button
            type="button"
            class="lightbox-close"
            aria-label={t("closePreview")}
            onClick={() => setLightboxSrc(null)}
          >
            <IconX size={18} />
          </button>
          <img src={lightboxSrc} alt="Preview" onClick={(e) => e.stopPropagation()} />
        </div>
      )}
    </div>
  );
}
