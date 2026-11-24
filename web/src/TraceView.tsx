import { useEffect, useState } from "preact/hooks";
import { HighlightedCode } from "./highlight.tsx";
import { formatBytes, useI18n } from "./i18n.ts";
import { IconChevron, IconExpand, IconSearch, IconX } from "./Icons.tsx";
import { ContentBlocks, CopyBtn, extractMessageText } from "./Timeline.tsx";
import type { ActiveDraft, ContentBlock, HistoryItem, HistoryPayload } from "./types.ts";

const KIND_ORDER = [
  "user_message",
  "assistant_message",
  "reasoning",
  "function_call",
  "function_call_output",
  "codemode_event",
  "withdrawal",
];

function summarizePayload(kind: string, p: HistoryPayload): string {
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
    const head = `${p.task_id || ""} ${p.event || ""} ${p.sequence != null ? `#${p.sequence}` : ""}`.trim();
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
  if (kind === "withdrawal") return "";
  const txt = extractMessageText(p.content).replace(/\s+/g, " ").trim();
  if (txt) return txt;
  const media = mediaBlocksFromPayload(p);
  return media.length ? media.map((b) => `[${b.type}#${b.attachment_id}]`).join(" ") : "";
}

function mediaBlocksFromPayload(p: HistoryPayload): ContentBlock[] {
  const candidates = Array.isArray(p.content) ? p.content : Array.isArray(p.output) ? p.output : null;
  if (!candidates) return [];
  return candidates.filter(
    (b) => b && typeof b === "object" && (b.type === "image" || b.type === "audio" || b.type === "video") && b.attachment_id
  );
}

export interface TraceViewProps {
  items: HistoryItem[];
  hasMore: boolean;
  loadingMore: boolean;
  queuedIds: Set<number>;
  draft: ActiveDraft | null;
  onLoadMore: () => void;
  onWithdraw: (mid: number) => void;
}

export function TraceView({
  items,
  hasMore,
  loadingMore,
  queuedIds,
  draft,
  onLoadMore,
  onWithdraw,
}: TraceViewProps) {
  const { t } = useI18n();
  const [kindFilter, setKindFilter] = useState<string>("all");
  const [query, setQuery] = useState("");
  const [expandState, setExpandState] = useState<{ open: boolean; epoch: number }>({
    open: false,
    epoch: 0,
  });
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  useEffect(() => {
    if (!lightboxSrc) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setLightboxSrc(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [lightboxSrc]);

  const counts = new Map<string, number>();
  const withdrawn = new Set<number>();
  for (const it of items) {
    counts.set(it.kind, (counts.get(it.kind) || 0) + 1);
    if (it.kind === "withdrawal" && it.target_id) withdrawn.add(it.target_id);
  }

  const presentKinds = [
    ...KIND_ORDER.filter((k) => counts.has(k)),
    ...[...counts.keys()].filter((k) => !KIND_ORDER.includes(k)),
  ];

  const q = query.trim().toLowerCase();
  const filtered = items.filter((it) => {
    if (kindFilter !== "all" && it.kind !== kindFilter) return false;
    if (!q) return true;
    return (
      `#${it.id}`.includes(q) ||
      String(it.id).includes(q) ||
      it.kind.toLowerCase().includes(q) ||
      JSON.stringify(it.payload).toLowerCase().includes(q)
    );
  });

  return (
    <div class="inspector-wrap">
      <div class="view-toolbar">
        <div class="view-toolbar-inner">
          <div class="filter-pills">
            <button
              type="button"
              class={`filter-pill ${kindFilter === "all" ? "active" : ""}`}
              onClick={() => setKindFilter("all")}
            >
              {t("all")} <span class="mono">{items.length}</span>
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
                placeholder={t("filterTrace")}
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
            {hasMore && (
              <button
                type="button"
                class="btn-ghost btn-xs"
                disabled={loadingMore}
                onClick={onLoadMore}
              >
                {loadingMore ? t("loading") : t("loadEarlier")}
              </button>
            )}
          </div>
        </div>
      </div>

      <div class="inspector-scroll">
        <div class="inspector-inner">
          {items.length === 0 && !draft && <div class="empty-hint">{t("traceEmpty")}</div>}
          {items.length > 0 && filtered.length === 0 && (
            <div class="empty-hint">{t("noMatchingItems")}</div>
          )}

          {filtered.map((item) => {
            const json = JSON.stringify(item.payload, null, 2);
            const preview = summarizePayload(item.kind, item.payload || {});
            const isWithdrawn = item.kind === "user_message" && withdrawn.has(item.id);
            const isQueued = item.kind === "user_message" && !isWithdrawn && queuedIds.has(item.id);
            const media = mediaBlocksFromPayload(item.payload || {});

            return (
              <details
                key={`${item.id}-${expandState.epoch}`}
                class="card trace-row"
                open={expandState.open}
              >
                <summary class="card-head">
                  <IconChevron size={13} cls="chev" />
                  <span class="pill mono">#{item.id}</span>
                  <span class={`kind-badge kind-${item.kind} mono`}>{item.kind}</span>
                  {item.target_id != null && (
                    <span class="pill mono">→ #{item.target_id}</span>
                  )}
                  {item.llm_call_id != null && (
                    <span class="muted small mono">call #{item.llm_call_id}</span>
                  )}
                  {isWithdrawn && <span class="pill">{t("withdrawn")}</span>}
                  {isQueued && <span class="pill pill-queued">{t("queued")}</span>}
                  {preview && <span class="muted truncate">{preview}</span>}
                  <span class="card-spacer" />
                  <span class="muted small mono">{formatBytes(json.length)}</span>
                </summary>
                <div class="card-body">
                  <div class="section-bar">
                    <span class="section-label">{t("payloadJson")}</span>
                    <div class="row gap-1">
                      {isQueued && (
                        <button
                          type="button"
                          class="btn-ghost btn-xs"
                          onClick={() => onWithdraw(item.id)}
                        >
                          {t("withdraw")}
                        </button>
                      )}
                      <CopyBtn text={json} label={t("copy")} />
                    </div>
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

          {draft?.items?.map((d) => {
            const json = JSON.stringify(d, null, 2);
            return (
              <details key={`live-${d.output_index}`} class="card trace-row streaming" open>
                <summary class="card-head">
                  <IconChevron size={13} cls="chev" />
                  <span class="pill pill-running">
                    <span class="pulse-dot" />
                    {t("live")}
                  </span>
                  <span class="kind-badge mono">{d.kind}</span>
                  {d.name && <span class="mono tool-name">{d.name}</span>}
                  <span class="muted small mono">call #{draft.llm_call_id}</span>
                </summary>
                <div class="card-body">
                  <pre class="code-box">
                    <code><HighlightedCode code={json} lang="json" /></code>
                  </pre>
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
