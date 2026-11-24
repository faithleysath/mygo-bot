import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { copyText, errorMessage, fetchMediaUrl, formatDuration, formatTokens, totalCallTokens } from "./api.ts";
import { HighlightedCode } from "./highlight.tsx";
import { formatShortTimeLoc, statusText, useI18n } from "./i18n.ts";
import { BrandMark, Icon } from "./Icons.tsx";
import { Lightbox } from "./Inspector.tsx";
import { Markdown } from "./markdown.tsx";
import type {
  ActiveDraft,
  ContentBlock,
  HistoryItem,
  HistoryPayload,
  LlmCallInfo,
  MediaKind,
  RetryNotice,
  SessionInfo,
} from "./types.ts";

export function CopyBtn({ text, label }: { text: string; label?: string }) {
  const { t } = useI18n();
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
    <button
      type="button"
      class="copy-btn"
      title={copied ? t("Copied") : t("Copy")}
      aria-label={t("Copy")}
      onClick={onCopy}
    >
      <Icon n={copied ? "check" : "copy"} s={12} />
      {label && <span>{copied ? t("Copied") : label}</span>}
    </button>
  );
}

export function extractMessageText(blocks: string | ContentBlock[] | undefined): string {
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

export function extractReasoningText(p: HistoryPayload): string {
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
  return parts.join("\n\n");
}

function AuthMedia({
  kind,
  attachmentId,
  onPreviewImage,
}: {
  kind: MediaKind;
  attachmentId: string;
  onPreviewImage?: (url: string) => void;
}) {
  const { t } = useI18n();
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

  if (err) return <div class="media-err">{attachmentId}: {err}</div>;
  if (!url) return <div class="media-loading">{t("Loading")} {kind} {attachmentId}…</div>;
  if (kind === "image") {
    return (
      <img
        class="media-img"
        src={url}
        alt={attachmentId}
        title={t("Click to enlarge")}
        onClick={() => onPreviewImage?.(url)}
      />
    );
  }
  if (kind === "audio") return <audio class="media-av" controls src={url} />;
  if (kind === "video") return <video class="media-av" controls src={url} />;
  return null;
}

export function ContentBlocks({
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
  const { t } = useI18n();
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
        <span class="section-label">{t("Result")}</span>
        {outText ? (
          <CopyBtn text={outText} label={t("Copy")} />
        ) : restJson ? (
          <CopyBtn text={restJson} label={t("Copy")} />
        ) : rawFallback ? (
          <CopyBtn text={rawFallback} label={t("Copy")} />
        ) : null}
      </div>
      {typeof summary?.error === "string" && summary.error && (
        <div class="err-inline row gap-1">
          <Icon n="alert" s={14} />
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
        <div class="muted small">{t("No output")}</div>
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
  const { locale, t } = useI18n();
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
  const codeLabel = typeof args?.trigger_code === "string" ? t("Trigger code") : t("Code");
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
        <Icon n="chevron" s={13} cls="chev" />
        <Icon n="terminal" s={13} />
        <span class="mono tool-name">{name}</span>
        {label && <span class="muted truncate">{label}</span>}
        <span class="card-spacer" />
        {summary?.id != null && <span class="pill mono">{String(summary.id)}</span>}
        {dur && <span class="pill mono">{dur}</span>}
        <span class={`pill pill-${status}`}>
          {status === "running" && <span class="pulse-dot" />}
          {statusText(status, locale)}
        </span>
      </summary>
      <div class="card-body">
        {code ? (
          <>
            <div class="section-bar">
              <span class="section-label">{codeLabel}</span>
              <CopyBtn text={code} label={t("Copy")} />
            </div>
            <pre class="code-box">
              <code><HighlightedCode code={code} lang="js" /></code>
            </pre>
          </>
        ) : hasArgs ? (
          <>
            <div class="section-bar">
              <span class="section-label">{t("Arguments")}</span>
              <CopyBtn text={formattedArgs} label={t("Copy")} />
            </div>
            <pre class="code-box">
              <code><HighlightedCode code={formattedArgs} lang={args ? "json" : undefined} /></code>
            </pre>
          </>
        ) : (
          <div class="muted small">{t("No arguments")}</div>
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
  const { t } = useI18n();
  const bodyRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);

  const body = !streaming && item ? extractReasoningText(item.payload || {}) : text;

  useLayoutEffect(() => {
    const el = bodyRef.current;
    if (streaming && el && stickRef.current) el.scrollTop = el.scrollHeight;
  }, [streaming, body]);

  if (!body && !streaming) return null;
  const lenHint =
    body.length > 1000 ? `${(body.length / 1000).toFixed(1)}k ${t("chars")}` : `${body.length} ${t("chars")}`;
  const onBodyScroll = () => {
    const el = bodyRef.current;
    if (!el) return;
    stickRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 36;
  };

  return (
    <details class="thought" open={streaming}>
      <summary>
        <Icon n="chevron" s={13} cls="chev" />
        {streaming ? <span class="pulse-dot" /> : <Icon n="spark" s={13} />}
        <span>{streaming ? t("Thinking…") : t("Thought process")}</span>
        {!streaming && body && <span class="thought-meta">{lenHint}</span>}
      </summary>
      <div class="thought-body" ref={bodyRef} onScroll={onBodyScroll}>
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
  const { locale } = useI18n();
  const p = item.payload || {};
  const content = p.content;
  let evSummary: Record<string, unknown> | null = null;
  let evExtraBlocks: ContentBlock[] | null = null;
  if (p.event === "completed" && Array.isArray(content) && content.length > 0 && content[0]?.type === "text") {
    evSummary = tryJson(content[0].text);
    evExtraBlocks = evSummary ? content.slice(1) : content;
  }
  let evPreview = "";
  if (!evSummary) {
    if (typeof content === "string") {
      evPreview = content.replace(/\s+/g, " ").trim();
    } else if (Array.isArray(content)) {
      for (const b of content) {
        if (b && typeof b === "object" && typeof b.text === "string" && b.text.trim()) {
          evPreview = b.text.replace(/\s+/g, " ").trim();
          break;
        }
      }
    }
  }
  const evStatus = typeof evSummary?.status === "string" ? evSummary.status : "";
  const evDur = typeof evSummary?.duration_ms === "number" ? formatDuration(evSummary.duration_ms) : "";

  return (
    <details class="card event-card">
      <summary class="card-head">
        <Icon n="chevron" s={13} cls="chev" />
        <Icon n="bolt" s={13} />
        <span class="pill mono">{p.task_id || "task"}</span>
        <span class="mono">{p.event}</span>
        {p.sequence != null && <span class="muted small">#{p.sequence}</span>}
        {evPreview && <span class="muted truncate">{evPreview}</span>}
        <span class="card-spacer" />
        {evDur && <span class="pill mono">{evDur}</span>}
        {evStatus && <span class={`pill pill-${evStatus}`}>{statusText(evStatus, locale)}</span>}
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
  queuedIds: Set<string>;
  draft: ActiveDraft | null;
  retryNotice: RetryNotice | null;
  callsById: Map<string, LlmCallInfo>;
  modelLabel?: string | null;
  effortLabel?: string | null;
  onLoadMore: () => void;
  onWithdraw: (mid: string) => void;
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
  callsById,
  modelLabel,
  effortLabel,
  onLoadMore,
  onWithdraw,
  onOpenSettings,
}: TimelineProps) {
  const { locale, t } = useI18n();
  const scrollRef = useRef<HTMLDivElement>(null);
  const stickRef = useRef(true);
  const rafRef = useRef(0);
  const prevSessionIdRef = useRef<string | null>(null);
  const prevFirstIdRef = useRef<string | null>(null);
  const prevScrollHeightRef = useRef<number>(0);
  const [showJump, setShowJump] = useState(false);
  const [activeTurn, setActiveTurn] = useState(0);
  const [lightboxSrc, setLightboxSrc] = useState<string | null>(null);

  useEffect(() => {
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, []);

  const updateActiveTurn = () => {
    rafRef.current = 0;
    const el = scrollRef.current;
    if (!el) return;
    const nodes = el.querySelectorAll<HTMLElement>("[data-turn-idx]");
    if (!nodes.length) return;
    const rectTop = el.getBoundingClientRect().top + 80;
    let best = 0;
    for (const n of nodes) {
      const top = n.getBoundingClientRect().top;
      const idx = Number(n.dataset.turnIdx);
      if (top <= rectTop) best = idx;
      else break;
    }
    setActiveTurn(best);
  };

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 72;
    stickRef.current = nearBottom;
    setShowJump(!nearBottom);
    if (!rafRef.current) rafRef.current = requestAnimationFrame(updateActiveTurn);
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
      updateActiveTurn();
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
    updateActiveTurn();
  }, [session?.id, items, draft, retryNotice]);

  const handleLoadMore = () => {
    stickRef.current = false;
    if (scrollRef.current) prevScrollHeightRef.current = scrollRef.current.scrollHeight;
    onLoadMore();
  };

  const jumpBottom = () => {
    const el = scrollRef.current;
    if (!el) return;
    stickRef.current = true;
    el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
    setShowJump(false);
  };

  const withdrawn = new Set<string>();
  const callsSet = new Set<string>();
  const outputsByTarget = new Map<string, HistoryItem>();
  for (const item of items) {
    if (item.kind === "withdrawal" && item.target_id) withdrawn.add(item.target_id);
    if (item.kind === "function_call") callsSet.add(item.id);
    if (item.kind === "function_call_output" && item.target_id) outputsByTarget.set(item.target_id, item);
  }

  const turns: Array<{ id: string; role: "user" | "assistant"; label: string }> = [];
  const turnIdxByItemId = new Map<string, number>();
  for (const item of items) {
    if (item.kind === "user_message" && !withdrawn.has(item.id)) {
      const txt = extractMessageText(item.payload?.content).replace(/\s+/g, " ").trim();
      turnIdxByItemId.set(item.id, turns.length);
      turns.push({ id: item.id, role: "user", label: txt.slice(0, 64) || item.id });
    } else if (item.kind === "assistant_message") {
      const txt = extractMessageText(item.payload?.content).replace(/\s+/g, " ").trim();
      if (txt) {
        turnIdxByItemId.set(item.id, turns.length);
        turns.push({ id: item.id, role: "assistant", label: txt.slice(0, 64) });
      }
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
                {loadingMore ? t("Loading…") : t("Load earlier")}
              </button>
            </div>
          )}

          {items.length === 0 && !draft && !showWorking && (
            <div class="empty-chat">
              <BrandMark size={36} />
              <h3>{session?.title || `${t("Session")} ${session?.id}`}</h3>
              <div class="empty-badges">
                {modelLabel ? (
                  <>
                    <span class="pill mono">{modelLabel}</span>
                    {effortLabel && <span class="pill">{effortLabel}</span>}
                  </>
                ) : (
                  <>
                    <span class="pill pill-paused">{t("No model selected")}</span>
                    {onOpenSettings && (
                      <button type="button" class="link-btn" onClick={onOpenSettings}>
                        {t("Configure model")}
                      </button>
                    )}
                  </>
                )}
              </div>
              <p>{t("Send a message below to queue input and run the model, or queue multiple inputs before starting.")}</p>
              <div class="empty-hints">
                <span>{t("Enter send")}</span>
                <span>·</span>
                <span>{t("Shift+Enter newline")}</span>
                <span>·</span>
                <span>{t("Paste or drop media")}</span>
              </div>
            </div>
          )}

          {items.map((item) => {
            if (item.kind === "withdrawal") return null;
            if (item.kind === "function_call_output" && item.target_id && callsSet.has(item.target_id)) {
              return null;
            }
            const tIdx = turnIdxByItemId.get(item.id);

            if (item.kind === "user_message") {
              const isWithdrawn = withdrawn.has(item.id);
              const isQueued = !isWithdrawn && queuedIds.has(item.id);
              const msgText = extractMessageText(item.payload?.content);
              return (
                <div
                  key={item.id}
                  data-turn-idx={tIdx}
                  class={`msg-row msg-user ${isWithdrawn ? "withdrawn" : ""}`}
                >
                  <div class="user-bubble">
                    <ContentBlocks blocks={item.payload?.content} onPreviewImage={setLightboxSrc} />
                    {(isWithdrawn || isQueued || msgText) && (
                      <div class="msg-foot">
                        {msgText && (
                          <span class="msg-copy-wrap">
                            <CopyBtn text={msgText} />
                          </span>
                        )}
                        {isWithdrawn && <span class="pill">{t("withdrawn")}</span>}
                        {isQueued && (
                          <>
                            <span class="pill pill-queued">{t("queued")}</span>
                            <button type="button" class="link-btn" onClick={() => onWithdraw(item.id)}>
                              {t("Withdraw")}
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
              const call = item.llm_call_id != null ? callsById.get(item.llm_call_id) : undefined;
              const callTokens = call ? totalCallTokens(call) : 0;
              const callTime = call ? formatShortTimeLoc(call.finished_at || call.started_at, locale) : "";
              return (
                <div key={item.id} data-turn-idx={tIdx} class="msg-row msg-assistant">
                  <ContentBlocks blocks={item.payload?.content} markdown onPreviewImage={setLightboxSrc} />
                  {(msgText || call) && (
                    <div class="msg-footer-meta">
                      {msgText && <CopyBtn text={msgText} />}
                      {call && (
                        <span
                          class="meta-chip"
                          title={`${call.id}: in ${call.input_uncached_tokens ?? 0} + cache_read ${call.cache_read_tokens ?? 0}${call.cache_write_tokens ? ` + cache_write ${call.cache_write_tokens}` : ""} → out ${call.output_tokens ?? 0}`}
                        >
                          <Icon n="database" s={12} />
                          <span>{t("Usage")} {formatTokens(callTokens)} tok</span>
                        </span>
                      )}
                      {callTime && <span class="meta-time">{callTime}</span>}
                    </div>
                  )}
                </div>
              );
            }

            if (item.kind === "reasoning") return <ReasoningBlock key={item.id} item={item} />;
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
                  <Icon n="chevron" s={13} cls="chev" />
                  <span class="mono">{item.kind}</span>
                  <span class="muted">{item.id}</span>
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
                    <Icon n="terminal" s={13} />
                    <span class="mono tool-name">{d.name || "tool"}</span>
                    <span class="card-spacer" />
                    <span class="pill pill-running">
                      <span class="pulse-dot" />
                      {t("streaming")}
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
              <span>{session?.status === "pausing" ? t("Pausing after current step…") : t("Working…")}</span>
            </div>
          )}

          {retryNotice && (
            <div class="retry-banner">
              <Icon n="alert" s={14} />
              <span>{t("Upstream retry")} #{retryNotice.attempt}: {retryNotice.reason}</span>
            </div>
          )}
        </div>
      </div>

      {turns.length >= 2 && (
        <nav class="turn-rail" aria-label={t("Conversation turns")}>
          {turns.map((turn, idx) => (
            <button
              key={turn.id}
              type="button"
              class={`turn-tick ${turn.role} ${activeTurn === idx ? "active" : ""}`}
              title={turn.label}
              aria-label={turn.label}
              aria-current={activeTurn === idx ? "location" : undefined}
              onClick={() => {
                scrollRef.current
                  ?.querySelector(`[data-turn-idx="${idx}"]`)
                  ?.scrollIntoView({ behavior: "smooth", block: "start" });
              }}
            />
          ))}
        </nav>
      )}

      {showJump && (
        <button type="button" class="jump-btn" onClick={jumpBottom}>
          {Boolean(draft?.items?.length) && <span class="pulse-dot" />}
          <Icon n="arrowDown" s={13} />
          <span>{t("Latest")}</span>
        </button>
      )}

      <Lightbox src={lightboxSrc} onClose={() => setLightboxSrc(null)} />
    </div>
  );
}
