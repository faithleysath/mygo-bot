import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { fetchMediaUrl } from "./api.js";
import { Markdown } from "./markdown.jsx";

function AuthMedia({ kind, attachmentId }) {
  const [url, setUrl] = useState(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    let alive = true;
    fetchMediaUrl(attachmentId)
      .then((u) => alive && setUrl(u))
      .catch((e) => alive && setErr(e.message));
    return () => {
      alive = false;
    };
  }, [attachmentId]);

  if (err) return <div class="media-err">Attachment #{attachmentId}: {err}</div>;
  if (!url) return <div class="media-loading">Loading {kind} #{attachmentId}…</div>;
  if (kind === "image") return <img class="media-img" src={url} alt={`Attachment #${attachmentId}`} />;
  if (kind === "audio") return <audio class="media-av" controls src={url} />;
  if (kind === "video") return <video class="media-av" controls src={url} />;
  return null;
}

function ContentBlocks({ blocks, markdown = false }) {
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
          return <AuthMedia key={i} kind={b.type} attachmentId={b.attachment_id} />;
        }
        return <pre key={i} class="plain-pre">{JSON.stringify(b, null, 2)}</pre>;
      })}
    </div>
  );
}

function tryJson(str) {
  if (typeof str !== "string") return null;
  try {
    return JSON.parse(str);
  } catch {
    return null;
  }
}

function ToolCard({ callItem, outputItem }) {
  const payload = callItem.payload || {};
  const args = tryJson(payload.arguments);
  const name = payload.name || "tool";
  const label = args?.label || (args?.id ? String(args.id) : "");

  const rawOut = outputItem?.payload?.output;
  let summary = null;
  let extraBlocks = null;
  if (typeof rawOut === "string") {
    summary = tryJson(rawOut);
  } else if (Array.isArray(rawOut) && rawOut.length > 0) {
    if (rawOut[0]?.type === "text") summary = tryJson(rawOut[0].text);
    extraBlocks = summary ? rawOut.slice(1) : rawOut;
  }

  let status = outputItem ? "done" : "running";
  if (summary?.status) status = summary.status;
  else if (summary?.delivery === "background") status = "background";

  const code = args?.code || args?.trigger_code || null;

  return (
    <details class="card tool-card">
      <summary class="card-head">
        <span class="mono">{name}</span>
        {label && <span class="muted truncate">{label}</span>}
        {summary?.id && <span class="pill">{summary.id}</span>}
        <span class={`pill pill-${status}`}>{status}</span>
      </summary>
      <div class="card-body">
        {code ? (
          <pre class="code-box">{code}</pre>
        ) : (
          <pre class="code-box">{args ? JSON.stringify(args, null, 2) : payload.arguments || ""}</pre>
        )}
        {outputItem && (
          <div class="tool-out">
            <div class="section-label">Result</div>
            {summary?.error && <div class="err-inline">{summary.error}</div>}
            {summary?.output && <pre class="code-box">{summary.output}</pre>}
            {extraBlocks && <ContentBlocks blocks={extraBlocks} />}
            {!summary && !extraBlocks && (
              <pre class="code-box">{typeof rawOut === "string" ? rawOut : JSON.stringify(rawOut, null, 2)}</pre>
            )}
          </div>
        )}
      </div>
    </details>
  );
}

function ReasoningBlock({ item, streaming = false, text = "" }) {
  let body = text;
  if (!streaming && item) {
    const p = item.payload || {};
    const parts = [];
    for (const key of ["summary", "content"]) {
      if (Array.isArray(p[key])) {
        for (const chunk of p[key]) {
          if (chunk?.text) parts.push(chunk.text);
        }
      }
    }
    body = parts.join("\n\n");
  }
  if (!body && !streaming) return null;
  return (
    <details class="thought" open={streaming}>
      <summary>
        {streaming && <span class="pulse-dot" />}
        <span>{streaming ? "Thinking…" : "Thought process"}</span>
      </summary>
      <div class="thought-body">{body}</div>
    </details>
  );
}

function EventCard({ item }) {
  const p = item.payload || {};
  return (
    <div class="card event-card">
      <div class="card-head">
        <span class="pill">{p.task_id || "task"}</span>
        <span class="mono">{p.event}</span>
        <span class="muted">#{p.sequence}</span>
      </div>
      <div class="card-body">
        <ContentBlocks blocks={p.content} />
      </div>
    </div>
  );
}

export function Timeline({
  session,
  items,
  hasMore,
  loadingMore,
  queuedIds,
  draft,
  retryNotice,
  onLoadMore,
  onWithdraw,
}) {
  const scrollRef = useRef(null);
  const stickRef = useRef(true);
  const [showJump, setShowJump] = useState(false);

  const onScroll = () => {
    const el = scrollRef.current;
    if (!el) return;
    const nearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 72;
    stickRef.current = nearBottom;
    setShowJump(!nearBottom);
  };

  useLayoutEffect(() => {
    const el = scrollRef.current;
    if (el && stickRef.current) {
      el.scrollTop = el.scrollHeight;
    }
  }, [session?.id, items, draft, retryNotice]);

  const jumpBottom = () => {
    const el = scrollRef.current;
    if (!el) return;
    stickRef.current = true;
    el.scrollTop = el.scrollHeight;
    setShowJump(false);
  };

  const withdrawn = new Set();
  const callsById = new Set();
  const outputsByTarget = new Map();

  for (const item of items) {
    if (item.kind === "withdrawal" && item.target_id) withdrawn.add(item.target_id);
    if (item.kind === "function_call") callsById.add(item.id);
    if (item.kind === "function_call_output" && item.target_id) {
      outputsByTarget.set(item.target_id, item);
    }
  }

  return (
    <div class="timeline-wrap">
      <div class="timeline" ref={scrollRef} onScroll={onScroll}>
        <div class="timeline-inner">
          {hasMore && (
            <div class="load-more">
              <button type="button" class="btn-ghost btn-sm" disabled={loadingMore} onClick={onLoadMore}>
                {loadingMore ? "Loading…" : "Load earlier history"}
              </button>
            </div>
          )}

          {items.length === 0 && !draft && (
            <div class="empty-chat">
              <h3>{session?.title || `Session #${session?.id}`}</h3>
              <p>Send a message below to queue input and run the model, or queue multiple inputs before starting.</p>
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
              return (
                <div key={item.id} class={`msg-row msg-user ${isWithdrawn ? "withdrawn" : ""}`}>
                  <div class="user-bubble">
                    <ContentBlocks blocks={item.payload?.content} />
                    <div class="msg-foot">
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
                  </div>
                </div>
              );
            }
            if (item.kind === "assistant_message") {
              return (
                <div key={item.id} class="msg-row msg-assistant">
                  <ContentBlocks blocks={item.payload?.content} markdown />
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
                />
              );
            }
            if (item.kind === "codemode_event") {
              return <EventCard key={item.id} item={item} />;
            }
            return (
              <details key={item.id} class="card">
                <summary class="card-head">
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
                    <span class="pulse-dot" />
                    <span class="mono">{d.name || "tool"}</span>
                    <span class="pill pill-running">streaming args</span>
                  </div>
                  {d.arguments && <pre class="code-box">{d.arguments}</pre>}
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

          {retryNotice && (
            <div class="retry-banner">
              Upstream retry #{retryNotice.attempt}: {retryNotice.reason}
            </div>
          )}
        </div>
      </div>

      {showJump && (
        <button type="button" class="jump-btn" onClick={jumpBottom}>
          ↓ Latest
        </button>
      )}
    </div>
  );
}
