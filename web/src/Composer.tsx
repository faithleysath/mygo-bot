import type { TargetedClipboardEvent, TargetedDragEvent, TargetedKeyboardEvent } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import {
  IconArrowUp,
  IconChevron,
  IconClock,
  IconPaperclip,
  IconPause,
  IconPlay,
  IconStop,
  IconX,
} from "./Icons.tsx";
import type { ActiveTask, ComposerDraft, SessionInfo } from "./types.ts";

function formatRem(sec: number | null | undefined): string {
  if (sec == null) return "";
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m`;
  return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`;
}

export interface ComposerProps {
  session: SessionInfo;
  draft: ComposerDraft;
  uploading: boolean;
  sending: boolean;
  tasks: ActiveTask[];
  queuedCount: number;
  onChangeText: (sid: number, text: string) => void;
  onUploadFiles: (sid: number, files: FileList | null) => void;
  onRemoveAttachment: (sid: number, aid: number) => void;
  onSubmit: (sid: number, startRun: boolean) => void;
  onPause: (mode: "soft" | "hard") => void;
  onKillTask: (tid: string) => void;
}

export function Composer({
  session,
  draft,
  uploading,
  sending,
  tasks,
  queuedCount,
  onChangeText,
  onUploadFiles,
  onRemoveAttachment,
  onSubmit,
  onPause,
  onKillTask,
}: ComposerProps) {
  const [tasksOpen, setTasksOpen] = useState(false);
  const [dragging, setDragging] = useState(false);
  const dragDepthRef = useRef(0);
  const fileRef = useRef<HTMLInputElement>(null);
  const taRef = useRef<HTMLTextAreaElement>(null);

  const running = session.status === "running" || session.status === "pausing";
  const text = draft.text || "";
  const attachments = draft.attachments || [];
  const error = draft.error || "";
  const hasPayload = Boolean(text.trim() || attachments.length > 0);
  const busy = uploading || sending;

  useEffect(() => {
    dragDepthRef.current = 0;
    setDragging(false);
  }, [session.id]);

  useLayoutEffect(() => {
    const el = taRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  }, [session.id, text]);

  const onKeyDown = (e: TargetedKeyboardEvent<HTMLTextAreaElement>) => {
    if (e.isComposing || e.keyCode === 229) return;
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (hasPayload && !busy) onSubmit(session.id, !running);
    }
  };

  const onPaste = (e: TargetedClipboardEvent<HTMLTextAreaElement>) => {
    const files = e.clipboardData?.files;
    if (files?.length) onUploadFiles(session.id, files);
  };

  const onDragEnter = (e: TargetedDragEvent<HTMLDivElement>) => {
    if (!e.dataTransfer?.types?.includes("Files")) return;
    e.preventDefault();
    dragDepthRef.current += 1;
    setDragging(true);
  };

  const onDragLeave = (e: TargetedDragEvent<HTMLDivElement>) => {
    if (dragDepthRef.current === 0) return;
    e.preventDefault();
    dragDepthRef.current = Math.max(0, dragDepthRef.current - 1);
    if (dragDepthRef.current === 0) setDragging(false);
  };

  const onDrop = (e: TargetedDragEvent<HTMLDivElement>) => {
    e.preventDefault();
    dragDepthRef.current = 0;
    setDragging(false);
    if (e.dataTransfer?.files?.length) onUploadFiles(session.id, e.dataTransfer.files);
  };

  return (
    <div class="composer-wrap">
      {tasks.length > 0 && (
        <div class="tasks-bar">
          <button
            type="button"
            class="tasks-bar-head"
            onClick={() => setTasksOpen((v) => !v)}
          >
            <span class="pulse-dot" />
            <span>{tasks.length} active codemode {tasks.length === 1 ? "task" : "tasks"}</span>
            <span class="card-spacer" />
            <span class="muted small">{tasksOpen ? "Hide" : "Inspect"}</span>
            <IconChevron size={14} cls={`chev ${tasksOpen ? "open" : ""}`} />
          </button>
          {tasksOpen && (
            <div class="tasks-list">
              {tasks.map((t) => (
                <div key={t.id} class="task-row">
                  <div class="task-row-head">
                    <span class="mono">{t.id}</span>
                    {t.label && <span>{t.label}</span>}
                    <span class="pill">{t.delivery}</span>
                    <span class="pill mono task-ttl">
                      <IconClock size={11} />
                      <span>{formatRem(t.remaining_seconds)}</span>
                    </span>
                    <button
                      type="button"
                      class="btn-danger btn-xs"
                      disabled={t.state === "stopping"}
                      onClick={() => onKillTask(t.id)}
                    >
                      <IconStop size={11} />
                      <span>{t.state === "stopping" ? "Stopping…" : "Kill"}</span>
                    </button>
                  </div>
                  {t.running_commands?.map((c) => (
                    <div key={c.id} class="task-cmd">
                      <div class="mono muted small">$ {c.program} {(c.args || []).join(" ")}</div>
                      {c.stdout && <pre class="code-box">{c.stdout}</pre>}
                      {c.stderr && <pre class="code-box err-text">{c.stderr}</pre>}
                    </div>
                  ))}
                  {t.console?.output && (
                    <div>
                      {t.console.truncated && (
                        <div class="muted small">Console tail ({t.console.total_bytes || 0} B total)</div>
                      )}
                      <pre class="code-box">{t.console.output}</pre>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {error && <div class="composer-err">{error}</div>}

      <div
        class={`composer-box ${dragging ? "dragging" : ""}`}
        onDragEnter={onDragEnter}
        onDragOver={(e) => e.preventDefault()}
        onDragLeave={onDragLeave}
        onDrop={onDrop}
      >
        {dragging && <div class="drop-overlay">Drop image, audio, or video to attach</div>}

        {attachments.length > 0 && (
          <div class="attach-strip">
            {attachments.map((a) => (
              <div key={a.id} class="attach-chip">
                {a.preview ? (
                  <img src={a.preview} alt={a.name} />
                ) : (
                  <span class="attach-kind mono">{a.kind}</span>
                )}
                <span class="truncate">{a.name}</span>
                <button
                  type="button"
                  aria-label={`Remove ${a.name}`}
                  onClick={() => onRemoveAttachment(session.id, a.id)}
                >
                  <IconX size={12} />
                </button>
              </div>
            ))}
          </div>
        )}

        <textarea
          ref={taRef}
          rows={1}
          value={text}
          placeholder={
            running
              ? "Steer running session (queues for next step)…"
              : "Message MyGo… (Enter to send, Shift+Enter for newline)"
          }
          onInput={(e) => onChangeText(session.id, e.currentTarget.value)}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
        />

        <div class="composer-actions">
          <div class="row gap-1">
            <input
              ref={fileRef}
              type="file"
              accept="image/*,audio/*,video/*"
              multiple
              hidden
              onChange={(e) => {
                onUploadFiles(session.id, e.currentTarget.files);
                e.currentTarget.value = "";
              }}
            />
            <button
              type="button"
              class="btn-ghost btn-sm"
              disabled={uploading}
              onClick={() => fileRef.current?.click()}
            >
              <IconPaperclip size={14} />
              <span>{uploading ? "Uploading…" : "Media"}</span>
            </button>
            {queuedCount > 0 && (
              <span class="pill pill-queued">{queuedCount} queued</span>
            )}
          </div>

          <div class="row gap-1">
            {running ? (
              <>
                <button
                  type="button"
                  class="btn-ghost btn-sm"
                  disabled={!hasPayload || busy}
                  title="Queue message for the next step"
                  onClick={() => onSubmit(session.id, false)}
                >
                  <IconArrowUp size={13} />
                  <span>{sending ? "Sending…" : "Steer"}</span>
                </button>
                <button
                  type="button"
                  class="btn-ghost btn-sm"
                  disabled={session.status === "pausing"}
                  title="Pause after the current step finishes"
                  onClick={() => onPause("soft")}
                >
                  <IconPause size={13} />
                  <span>{session.status === "pausing" ? "Pausing…" : "Pause"}</span>
                </button>
                <button
                  type="button"
                  class="btn-danger btn-sm"
                  title="Stop the active run immediately"
                  onClick={() => onPause("hard")}
                >
                  <IconStop size={13} />
                  <span>Stop</span>
                </button>
              </>
            ) : (
              <>
                {hasPayload && (
                  <button
                    type="button"
                    class="btn-ghost btn-sm"
                    disabled={busy}
                    title="Add message to queue without starting a run"
                    onClick={() => onSubmit(session.id, false)}
                  >
                    {sending ? "Sending…" : "Queue"}
                  </button>
                )}
                <button
                  type="button"
                  class="btn-primary btn-sm"
                  disabled={busy || (!hasPayload && session.status !== "paused" && queuedCount === 0)}
                  onClick={() => onSubmit(session.id, true)}
                >
                  {hasPayload ? <IconArrowUp size={13} /> : <IconPlay size={13} />}
                  <span>
                    {sending
                      ? "Sending…"
                      : hasPayload
                        ? "Send & Run"
                        : session.status === "paused"
                          ? "Resume"
                          : "Run"}
                  </span>
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
