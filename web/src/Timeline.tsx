import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { copyText, errorMessage, fetchMediaUrl, formatDuration } from "./api.ts";
import { HighlightedCode } from "./highlight.tsx";
import {
  BrandMark,
  IconAlert,
  IconArrowDown,
  IconBolt,
  IconCheck,
  IconChevron,
  IconCopy,
  IconSpark,
  IconTerminal,
  IconX,
} from "./Icons.tsx";
import { Markdown } from "./markdown.tsx";
import type { ActiveDraft, ContentBlock, HistoryItem, MediaKind, RetryNotice, SessionInfo } from "./types.ts";

function CopyBtn({ text, label }: { text: string; label?: string }) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current !== null) clearTimeout(timerRef.current);
    };
  }, []);

  const onCopy = async () => {
    if (await copyText(text)) {
      if (timerRef.current !== null) clearTimeout(timerRef.current);
      setCopied(true);
      timerRef.current = setTimeout(() => {
        setCopied(false);
        timerRef.current = null;
      }, 1400);
    }
  };

  return (
    <button type="button" class="copy-btn" title={copied ? "Copied" : "Copy"} aria-label="Copy" onClick={onCopy}>
      {copied ? <IconCheck size={12} /> : <IconCopy size={12} />}
      {label && <span>{copied ? "Copied" : label}</span>}
    </button>
  );
}

function extractMessageText(blocks: string | ContentBlock[] | undefined): string {
  if (typeof blocks === "string") return blocks;
  if (!Array.isArray(blocks)) return "";
  const parts: string[] = [];
  for (const b of blocks) {
    if (!b || typeof b !== "object") continue;
    if (typeof b.text === "string" && b.text) parts.push(b.text);
    else if (typeof b.refusal === "string" && b.refusal) parts.push(b.refusal);
  }
  return parts.join("\n\n");
}

function AuthMedia({
  kind,
  attachmentId,
  onPreviewImage,
}: {
  kind: MediaKind;
  attachmentId: number;
  onPreviewImage?: (url: string) => void;
}) {
  const [url, setUrl] = useState<string | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    fetchMediaUrl(attachmentId)
      .then((u) => {
        if (alive) setUrl(u);
      })
      .catch((e) => {
        if (alive) setErr(errorMessage(e));
      });
    return () => {
      alive = false;
    };
  }, [attachmentId]);

  if (err) return <div class="media-err">Attachment #{attachmentId}: {err}</div>;
  if (!url) return <div class="media-loading">Loading {kind} #{attachmentId}…</div>;
  if (kind === "image") {
    return (
      <img
        class="media-img"
        src={url}
        alt={`Attachment #${attachmentId}`}
        title="Click to enlarge"
        onClick={() => onPreviewImage?.(url)}
      />
    );
  }
  if (kind === "audio") return <audio class="media-av" controls src={url} />;
  if (kind === "video") return <video class="media-av" controls src={url} />;
  return null;
}

function ContentBlocks({
  blocks,
  markdown = false,
  onPreviewImage,
}: {
  blocks: string | ContentBlock[] | undefined;
  markdown?: boolean;
  onPreviewImage?: (url: string) => void;
}) {
  if (typeof blocks === "string") {
    return markdown ? <Markdown text={blocks} /> : <pre class="plain-pre">{blocks}</pre>;
  }
  if (!Array.isArray(blocks)) return null;
  return (
    <div class="blocks">
      {blocks.map((b, i) => {
        if (!b || typeof b !== "object") return null;
        if (b.type === "text" || b.type === "input_text" || b.type === "output_text") {
          return markdown ? (
            <Markdown key={i} text={b.text || ""} />
          ) : (
            <div key={i} class="plain-text">{b.text || ""}</div>
          );
        }
        if (b.type === "refusal") {
          return (
            <div key={i} class="refusal-box">
              <strong>Refusal:</strong> {b.refusal || b.text || ""}
            </div>
          );
        }
        if ((b.type === "image" || b.type === "audio" || b.type === "video") && b.attachment_id) {
          return (
            <AuthMedia
              key={i}
              kind={b.type}
              attachmentId={b.attachment_id}
              onPreviewImage={onPreviewImage}
            />
          );
        }
        return <pre key={i} class="plain-pre">{JSON.stringify(b, null, 2)}</pre>;
      })}
    </div>
  );
}

function tryJson(str: unknown): Record<string, unknown> | null {
  if (typeof str !== "string") return null;
  try {
    const parsed = JSON.parse(str);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : null;
  } catch {
    return null;
  }
}

const OMIT_SUMMARY_KEYS = new Set(["id", "status", "duration_ms", "error", "output"]);

function SummaryBody({
  summary,
  extraBlocks,
  rawOut,
  onPreviewImage,
}: {
  summary: Record<string, unknown> | null;
  extraBlocks: ContentBlock[] | null;
  rawOut: unknown;
  onPreviewImage?: (url: string) => void;
}) {
  let restSummary: Record<string, unknown> | null = null;
  if (summary) {
    const entries = Object.entries(summary).filter(([k]) => !OMIT_SUMMARY_KEYS.has(k));
    if (entries.length > 0) restSummary = Object.fromEntries(entries);
  }
  const restJson = restSummary ? JSON.stringify(restSummary, null, 2) : "";
  const outText = typeof summary?.output === "string" ? summary.output : "";
  const hasExtra = Boolean(extraBlocks && extraBlocks.length > 0);
  const rawFallback =
    !summary && !hasExtra
      ? typeof rawOut === "string"
        ? rawOut
        : JSON.stringify(rawOut, null, 2)
      : "";

  return (
    <div class="tool-out">
      <div class="section-bar">
        <span class="section-label">Result</span>
        {outText ? <CopyBtn text={outText} label="Copy" /> : restJson ? <CopyBtn text={restJson} label="Copy" /> : rawFallback ? <CopyBtn text={rawFallback} label="Copy" /> : null}
      </div>
      {typeof summary?.error === "string" && summary.error && (
        <div class="err-inline row gap-1">
          <IconAlert size={14} />
          <span>{summary.error}</span>
        </div>
      )}
      {outText && <pre class="code-box">{outText}</pre>}
      {hasExtra && <ContentBlocks blocks={extraBlocks ?? undefined} onPreviewImage={onPreviewImage} />}
      {restJson && (
        <pre class="code-box">
          <code><HighlightedCode code={restJson} lang="json" /></code>
        </pre>
      )}
      {rawFallback && <pre class="code-box">{rawFallback}</pre>}
      {!outText && !restJson && !hasExtra && !rawFallback && !summary?.error && (
        <div class="muted small">No output</div>
      )}
    </div>
  );
}

function ToolCard({
  callItem,
  outputItem,
  onPreviewImage,
}: {
  callItem: HistoryItem;
  outputItem?: HistoryItem;
  onPreviewImage?: (url: string) => void;
}) {
  const payload = callItem.payload || {};
  const args = tryJson(payload.arguments);
  const name = payload.name || "tool";
  const label =
    typeof args?.label === "string"
      ? args.label
      : args?.id != null
        ? String(args.id)
        : "";

  const rawOut = outputItem?.payload?.output;
  let summary: Record<string, unknown> | null = null;
  let extraBlocks: ContentBlock[] | null = null;
  if (typeof rawOut === "string") {
    summary = tryJson(rawOut);
  } else if (Array.isArray(rawOut) && rawOut.length > 0) {
    if (rawOut[0]?.type === "text") summary = tryJson(rawOut[0].text);
    extraBlocks = summary ? rawOut.slice(1) : rawOut;
  }

  let status = outputItem ? "done" : "running";
  if (typeof summary?.status === "string") status = summary.status;
  else if (summary?.delivery === "background") status = "background";

  const code =
    typeof args?.code === "string"
      ? args.code
      : typeof args?.trigger_code === "string"
        ? args.trigger_code
        : null;
  const codeLabel = typeof args?.trigger_code === "string" ? "Trigger code" : "Code";
  const formattedArgs = args
    ? JSON.stringify(args, null, 2)
    : typeof payload.arguments === "string"
      ? payload.arguments
      : "";
  const hasArgs = Boolean(args ? Object.keys(args).length > 0 : formattedArgs.trim());
  const dur = typeof summary?.duration_ms === "number" ? formatDuration(summary.duration_ms) : "";

  return (
    <details class="card tool-card">
      <summary class="card-head">
        <IconChevron size={13} cls="chev" />
        <IconTerminal size={13} />
        <span class="mono tool-name">{name}</span>
        {label && <span class="muted truncate">{label}</span>}
        <span class="card-spacer" />
        {summary?.id != null && <span class="pill mono">{String(summary.id)}</span>}
        {dur && <span class="pill mono">{dur}</span>}
        <span class={`pill pill-${status}`}>
          {status === "running" && <span class="pulse-dot" />}
          {status}
        </span>
      </summary>
      <div class="card-body">
        {code ? (
          <>
            <div class="section-bar">
              <span class="section-label">{codeLabel}</span>
              <CopyBtn text={code} label="Copy" />
            </div>
            <pre class="code-box">
              <code><HighlightedCode code={code} lang="js" /></code>
            </pre>
          </>
        ) : hasArgs ? (
          <>
            <div class="section-bar">
              <span class="section-label">Arguments</span>
              <CopyBtn text={formattedArgs} label="Copy" />
            </div>
            <pre class="code-box">
              <code><HighlightedCode code={formattedArgs} lang={args ? "json" : undefined} /></code>
            </pre>
          </>
        ) : (
          <div class="muted small">No arguments</div>
        )}
        {outputItem && (
          <SummaryBody
            summary={summary}
            extraBlocks={extraBlocks}
            rawOut={rawOut}
            onPreviewImage={onPreviewImage}
          />
        )}
      </div>
    </details>
  );
}

function ReasoningBlock({
  item,
  streaming = false,
  text = "",
}: {
  item?: HistoryItem;
  streaming?: boolean;
  text?: string;
}) {
  let body = text;
  if (!streaming && item) {
    const p = item.payload || {};
    const parts: string[] = [];
    for (const key of ["summary", "content"] as const) {
      const chunks = p[key];
      if (Array.isArray(chunks)) {
        for (const chunk of chunks) {
          if (chunk && typeof chunk === "object" && "text" in chunk && typeof chunk.text === "string" && chunk.text) {
            parts.push(chunk.text);
          }
        }
      }
    }
    body = parts.join("\n\n");
  }
  if (!body && !streaming) return null;
  const lenHint = body.length > 1000 ? `${(body.length / 1000).toFixed(1)}k chars` : `${body.length} chars`;
  return (
    <details class="thought" open={streaming}>
      <summary>
        <IconChevron size={13} cls="chev" />
        {streaming ? <span class="pulse-dot" /> : <IconSpark size={13} />}
        <span>{streaming ? "Thinking…" : "Thought process"}</span>
        {!streaming && body && <span class="thought-meta">{lenHint}</span>}
      </summary>
      <div class="thought-body">
        <Markdown text={body} />
      </div>
    </details>
  );
}

function EventCard({
  item,
  onPreviewImage,
}: {
  item: HistoryItem;
  onPreviewImage?: (url: string) => void;
}) {
  const p = item.payload || {};
  const content = p.content;
  let evSummary: Record<string, unknown> | null = null;
  let evExtraBlocks: ContentBlock[] | null = null;
  if (p.event === "completed" && Array.isArray(content) && content.length > 0 && content[0]?.type === "text") {
    evSummary = tryJson(content[0].text);
    evExtraBlocks = evSummary ? content.slice(1) : content;
  }
  const evStatus = typeof evSummary?.status === "string" ? evSummary.status : "";
  const evDur = typeof evSummary?.duration_ms === "number" ? formatDuration(evSummary.duration_ms) : "";

  return (
    <details class="card event-card" open>
      <summary class="card-head">
        <IconChevron size={13} cls="chev" />
        <IconBolt size={13} />
        <span class="pill mono">{p.task_id || "task"}</span>
        <span class="mono">{p.event}</span>
        {p.sequence != null && <span class="muted small">#{p.sequence}</span>}
        <span class="card-spacer" />
        {evDur && <span class="pill mono">{evDur}</span>}
        {evStatus && <span class={`pill pill-${evStatus}`}>{evStatus}</span>}
      </summary>
      <div class="card-body">
        {evSummary ? (
          <SummaryBody
            summary={evSummary}
            extraBlocks={evExtraBlocks}
            rawOut={content}
            onPreviewImage={onPreviewImage}
          />
        ) : (
          <ContentBlocks blocks={content} onPreviewImage={onPreviewImage} />
        )}
      </div>
    </details>
  );
}

export interface TimelineProps {
  session: SessionInfo | null;
  items: HistoryItem[];
  hasMore: boolean;
  loadingMore: boolean;
  queuedIds: Set<number>;
  draft: ActiveDraft | null;
  retryNotice: RetryNotice | null;
  modelLabel?: string | null;
  effortLabel?: string | null;
  onLoadMore: () => void;
  onWithdraw: (mid: number) => void;
  onOpenSettings?: () => void;
}

export function Timeline({
  session,
  items,
  hasMore,
  loadingMore,
  queuedIds,
  draft,
  retryNotice,
  modelLabel,
  effortLabel,
  onLoadMore,
  onWithdraw,
  onOpenSettings,
}: TimelineProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  const prevSessionIdRef = useRef<number | null>(null);
  const prevFirstIdRef = useRef<number | null>(null);
  const prevScrollHeightRef = useRef<number>(0);
  const [showJump, setShowJump] = useState(false);
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  useEffect(() => {
    if (!lightboxSrc) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setLightboxSrc(null);
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [lightboxSrc]);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 72;
    stickRef.current = nearBottom;
    setShowJump(!nearBottom);
  };

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const sid = session?.id ?? null;
    const firstId = items[0]?.id ?? null;

    if (sid !== prevSessionIdRef.current) {
      prevSessionIdRef.current = sid;
      prevFirstIdRef.current = firstId;
      prevScrollHeightRef.current = 0;
      stickRef.current = true;
      setShowJump(false);
      el.scrollTop = el.scrollHeight;
      return;
    }

    if (
      prevFirstIdRef.current !== null &&
      firstId !== null &&
      firstId < prevFirstIdRef.current &&
      !stickRef.current &&
      prevScrollHeightRef.current > 0
    ) {
      el.scrollTop += el.scrollHeight - prevScrollHeightRef.current;
      prevScrollHeightRef.current = 0;
    } else if (stickRef.current) {
      el.scrollTop = el.scrollHeight;
    }
    prevFirstIdRef.current = firstId;
  }, [session?.id, items, draft, retryNotice]);

  const handleLoadMore = () => {
    stickRef.current = false;
    if (scrollRef.current) {
      prevScrollHeightRef.current = scrollRef.current.scrollHeight;
    }
    onLoadMore();
  };

  const jumpBottom = () => {
    const el = scrollRef.current;
    if (!el) return;
    stickRef.current = true;
    el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
    setShowJump(false);
  };

  const withdrawn = new Set<number>();
  const callsById = new Set<number>();
  const outputsByTarget = new Map<number, HistoryItem>();

  for (const item of items) {
    if (item.kind === "withdrawal" && item.target_id) withdrawn.add(item.target_id);
    if (item.kind === "function_call") callsById.add(item.id);
    if (item.kind === "function_call_output" && item.target_id) {
      outputsByTarget.set(item.target_id, item);
    }
  }

  const isRunning = session?.status === "running" || session?.status === "pausing";
  const showWorking = isRunning && (!draft || draft.items.length === 0) && !retryNotice;

  return (
    <div class="timeline-wrap">
      <div class="timeline" ref={scrollRef} onScroll={onScroll}>
        <div class="timeline-inner">
          {hasMore && (
            <div class="load-more">
              <button type="button" class="btn-ghost btn-sm" disabled={loadingMore} onClick={handleLoadMore}>
                {loadingMore ? "Loading…" : "Load earlier history"}
              </button>
            </div>
          )}

          {items.length === 0 && !draft && !showWorking && (
            <div class="empty-chat">
              <BrandMark size={36} />
              <h3>{session?.title || `Session #${session?.id}`}</h3>
              <div class="empty-badges">
                {modelLabel ? (
                  <>
                    <span class="pill mono">{modelLabel}</span>
                    {effortLabel && <span class="pill">effort: {effortLabel}</span>}
                  </>
                ) : (
                  <>
                    <span class="pill pill-paused">No model selected</span>
                    {onOpenSettings && (
                      <button type="button" class="link-btn" onClick={onOpenSettings}>
                        Configure model
                      </button>
                    )}
                  </>
                )}
              </div>
              <p>Send a message below to queue input and run the model, or queue multiple inputs before starting.</p>
              <div class="empty-hints">
                <span>Enter send</span>
                <span>·</span>
                <span>Shift+Enter newline</span>
                <span>·</span>
                <span>Paste or drop media</span>
              </div>
            </div>
          )}

          {items.map((item) => {
            if (item.kind === "withdrawal") return null;
            if (item.kind === "function_call_output" && item.target_id && callsById.has(item.target_id)) {
              return null;
            }
            if (item.kind === "user_message") {
              const isWithdrawn = withdrawn.has(item.id);
              const isQueued = !isWithdrawn && queuedIds.has(item.id);
              const msgText = extractMessageText(item.payload?.content);
              return (
                <div key={item.id} class={`msg-row msg-user ${isWithdrawn ? "withdrawn" : ""}`}>
                  <div class="user-bubble">
                    <ContentBlocks blocks={item.payload?.content} onPreviewImage={setLightboxSrc} />
                    {(isWithdrawn || isQueued || msgText) && (
                      <div class="msg-foot">
                        {msgText && (
                          <span class="msg-copy-wrap">
                            <CopyBtn text={msgText} />
                          </span>
                        )}
                        {isWithdrawn && <span class="pill">withdrawn</span>}
                        {isQueued && (
                          <>
                            <span class="pill pill-queued">queued</span>
                            <button type="button" class="link-btn" onClick={() => onWithdraw(item.id)}>
                              Withdraw
                            </button>
                          </>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              );
            }
            if (item.kind === "assistant_message") {
              const msgText = extractMessageText(item.payload?.content);
              return (
                <div key={item.id} class="msg-row msg-assistant">
                  <ContentBlocks blocks={item.payload?.content} markdown onPreviewImage={setLightboxSrc} />
                  {msgText && (
                    <div class="msg-actions">
                      <CopyBtn text={msgText} label="Copy" />
                    </div>
                  )}
                </div>
              );
            }
            if (item.kind === "reasoning") {
              return <ReasoningBlock key={item.id} item={item} />;
            }
            if (item.kind === "function_call") {
              return (
                <ToolCard
                  key={item.id}
                  callItem={item}
                  outputItem={outputsByTarget.get(item.id)}
                  onPreviewImage={setLightboxSrc}
                />
              );
            }
            if (item.kind === "codemode_event") {
              return <EventCard key={item.id} item={item} onPreviewImage={setLightboxSrc} />;
            }
            return (
              <details key={item.id} class="card">
                <summary class="card-head">
                  <IconChevron size={13} cls="chev" />
                  <span class="mono">{item.kind}</span>
                  <span class="muted">#{item.id}</span>
                </summary>
                <div class="card-body">
                  <pre class="code-box">{JSON.stringify(item.payload, null, 2)}</pre>
                </div>
              </details>
            );
          })}

          {draft?.items?.map((d) => {
            if (d.kind === "reasoning") {
              return <ReasoningBlock key={`d-${d.output_index}`} streaming text={d.reasoning} />;
            }
            if (d.kind === "function_call") {
              return (
                <div key={`d-${d.output_index}`} class="card tool-card streaming">
                  <div class="card-head">
                    <IconTerminal size={13} />
                    <span class="mono tool-name">{d.name || "tool"}</span>
                    <span class="card-spacer" />
                    <span class="pill pill-running">
                      <span class="pulse-dot" />
                      streaming
                    </span>
                  </div>
                  {d.arguments && (
                    <div class="card-body">
                      <pre class="code-box">{d.arguments}</pre>
                    </div>
                  )}
                </div>
              );
            }
            return (
              <div key={`d-${d.output_index}`} class="msg-row msg-assistant streaming">
                <Markdown text={d.text || ""} />
                <span class="cursor" />
              </div>
            );
          })}

          {showWorking && (
            <div class="thinking-row">
              <span class="dots" aria-hidden="true">
                <i />
                <i />
                <i />
              </span>
              <span>{session?.status === "pausing" ? "Pausing after current step…" : "Working…"}</span>
            </div>
          )}

          {retryNotice && (
            <div class="retry-banner">
              <IconAlert size={14} />
              <span>Upstream retry #{retryNotice.attempt}: {retryNotice.reason}</span>
            </div>
          )}
        </div>
      </div>

      {showJump && (
        <button type="button" class="jump-btn" onClick={jumpBottom}>
          {Boolean(draft?.items?.length) && <span class="pulse-dot" />}
          <IconArrowDown size={13} />
          <span>Latest</span>
        </button>
      )}

      {lightboxSrc && (
        <div class="lightbox" onClick={() => setLightboxSrc(null)}>
          <button
            type="button"
            class="lightbox-close"
            aria-label="Close preview"
            onClick={() => setLightboxSrc(null)}
          >
            <IconX size={18} />
          </button>
          <img src={lightboxSrc} alt="Attachment preview" onClick={(e) => e.stopPropagation()} />
        </div>
      )}
    </div>
  );
}
