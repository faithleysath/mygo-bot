import { useRef, useState } from "preact/hooks";
import { apiFetch } from "./api.js";

function formatRem(sec) {
  if (sec == null) return "";
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m`;
  return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`;
}

export function Composer({
  session,
  tasks,
  queuedCount,
  onSend,
  onRun,
  onPause,
  onKillTask,
}) {
  const [text, setText] = useState("");
  const [attachments, setAttachments] = useState([]);
  const [uploading, setUploading] = useState(false);
  const [error, setError] = useState("");
  const [tasksOpen, setTasksOpen] = useState(false);
  const fileRef = useRef(null);
  const taRef = useRef(null);

  const running = session?.status === "running" || session?.status === "pausing";
  const hasPayload = Boolean(text.trim() || attachments.length > 0);

  const resize = () => {
    const el = taRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = `${Math.min(el.scrollHeight, 200)}px`;
  };

  const uploadFiles = async (files) => {
    const list = Array.from(files || []).filter((f) =>
      /^(image|audio|video)\//.test(f.type)
    );
    if (!list.length) {
      setError("Only image, audio, or video files are supported.");
      return;
    }
    setError("");
    setUploading(true);
    try {
      for (const file of list) {
        const res = await apiFetch("/attachments", {
          method: "POST",
          headers: { "Content-Type": file.type },
          body: file,
        });
        const kind = file.type.split("/")[0];
        const preview = kind === "image" ? URL.createObjectURL(file) : null;
        setAttachments((prev) => [
          ...prev,
          { id: res.id, kind, name: file.name || `${kind}-${res.id}`, preview },
        ]);
      }
    } catch (e) {
      setError(e.message);
    } finally {
      setUploading(false);
    }
  };

  const removeAttachment = (id) => {
    setAttachments((prev) => {
      const target = prev.find((a) => a.id === id);
      if (target?.preview) URL.revokeObjectURL(target.preview);
      return prev.filter((a) => a.id !== id);
    });
  };

  const buildContent = () => {
    const blocks = [];
    if (text.trim()) blocks.push({ type: "text", text });
    for (const a of attachments) {
      blocks.push({ type: a.kind, attachment_id: a.id });
    }
    return blocks;
  };

  const clearDraft = () => {
    setText("");
    setAttachments((prev) => {
      for (const a of prev) if (a.preview) URL.revokeObjectURL(a.preview);
      return [];
    });
    if (taRef.current) taRef.current.style.height = "auto";
  };

  const submit = async (startRun) => {
    if (!hasPayload && startRun && !running) {
      setError("");
      try {
        await onRun();
      } catch (e) {
        setError(e.message);
      }
      return;
    }
    if (!hasPayload) return;
    setError("");
    try {
      await onSend(buildContent(), startRun);
      clearDraft();
    } catch (e) {
      setError(e.message);
    }
  };

  const onKeyDown = (e) => {
    if (e.isComposing || e.keyCode === 229) return;
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (hasPayload) submit(!running);
    }
  };

  const onPaste = (e) => {
    const files = e.clipboardData?.files;
    if (files?.length) uploadFiles(files);
  };

  const onDrop = (e) => {
    e.preventDefault();
    if (e.dataTransfer?.files?.length) uploadFiles(e.dataTransfer.files);
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
            <span class="muted">{tasksOpen ? "Hide" : "Inspect"}</span>
          </button>
          {tasksOpen && (
            <div class="tasks-list">
              {tasks.map((t) => (
                <div key={t.id} class="task-row">
                  <div class="task-row-head">
                    <span class="mono">{t.id}</span>
                    {t.label && <span>{t.label}</span>}
                    <span class="pill">{t.delivery}</span>
                    <span class="muted">TTL {formatRem(t.remaining_seconds)}</span>
                    <button
                      type="button"
                      class="btn-danger btn-xs"
                      disabled={t.state === "stopping"}
                      onClick={() => onKillTask(t.id)}
                    >
                      {t.state === "stopping" ? "Stopping…" : "Kill"}
                    </button>
                  </div>
                  {t.running_commands?.map((c) => (
                    <div key={c.id} class="task-cmd">
                      <div class="mono muted">$ {c.program} {(c.args || []).join(" ")}</div>
                      {c.stdout && <pre class="code-box">{c.stdout}</pre>}
                      {c.stderr && <pre class="code-box err-text">{c.stderr}</pre>}
                    </div>
                  ))}
                  {t.console?.output && <pre class="code-box">{t.console.output}</pre>}
                </div>
              ))}
            </div>
          )}
        </div>
      )}

      {error && <div class="composer-err">{error}</div>}

      <div
        class="composer-box"
        onDragOver={(e) => e.preventDefault()}
        onDrop={onDrop}
      >
        {attachments.length > 0 && (
          <div class="attach-strip">
            {attachments.map((a) => (
              <div key={a.id} class="attach-chip">
                {a.preview ? <img src={a.preview} alt={a.name} /> : <span>{a.kind}</span>}
                <span class="truncate">{a.name}</span>
                <button type="button" onClick={() => removeAttachment(a.id)}>×</button>
              </div>
            ))}
          </div>
        )}

        <textarea
          ref={taRef}
          rows={1}
          value={text}
          placeholder={running ? "Steer running session (queues for next step)…" : "Message MyGo… (Enter to send, Shift+Enter for newline)"}
          onInput={(e) => {
            setText(e.target.value);
            resize();
          }}
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
                uploadFiles(e.target.files);
                e.target.value = "";
              }}
            />
            <button
              type="button"
              class="btn-ghost btn-sm"
              disabled={uploading}
              onClick={() => fileRef.current?.click()}
            >
              {uploading ? "Uploading…" : "+ Media"}
            </button>
          </div>

          <div class="row gap-1">
            {running ? (
              <>
                <button
                  type="button"
                  class="btn-ghost btn-sm"
                  disabled={!hasPayload || uploading}
                  onClick={() => submit(false)}
                >
                  Steer
                </button>
                <button
                  type="button"
                  class="btn-ghost btn-sm"
                  disabled={session.status === "pausing"}
                  onClick={() => onPause("soft")}
                >
                  {session.status === "pausing" ? "Pausing…" : "Pause"}
                </button>
                <button
                  type="button"
                  class="btn-danger btn-sm"
                  onClick={() => onPause("hard")}
                >
                  Stop
                </button>
              </>
            ) : (
              <>
                {hasPayload && (
                  <button
                    type="button"
                    class="btn-ghost btn-sm"
                    disabled={uploading}
                    onClick={() => submit(false)}
                  >
                    Queue
                  </button>
                )}
                <button
                  type="button"
                  class="btn-primary btn-sm"
                  disabled={uploading || (!hasPayload && session?.status !== "paused" && queuedCount === 0)}
                  onClick={() => submit(true)}
                >
                  {hasPayload ? "Send & Run" : session?.status === "paused" ? "Resume" : "Run"}
                </button>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
