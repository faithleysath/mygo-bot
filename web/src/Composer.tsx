import type { TargetedClipboardEvent, TargetedDragEvent, TargetedKeyboardEvent } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { formatCost, formatTokens, totalCallTokens, totalInputTokens } from "./api.ts";
import { useI18n } from "./i18n.ts";
import { Icon } from "./Icons.tsx";
import type {
  ActiveTask,
  CallsSummary,
  ComposerDraft,
  ModelInfo,
  ProviderInfo,
  SessionInfo,
} from "./types.ts";

function formatRem(sec: number | null | undefined): string {
  if (sec == null) return "";
  if (sec < 60) return `${sec}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m`;
  return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`;
}

export interface ClusterNodeOption {
  id: string;
  name: string;
  online: boolean;
  isSelf: boolean;
}

export interface ComposerProps {
  session: SessionInfo;
  providers: ProviderInfo[];
  models: ModelInfo[];
  draft: ComposerDraft;
  uploading: boolean;
  sending: boolean;
  tasks: ActiveTask[];
  queuedCount: number;
  summary: CallsSummary | null;
  clusterNodes?: ClusterNodeOption[];
  runNode?: string;
  onChangeRunNode?: (sid: string, node: string) => void;
  onChangeText: (sid: string, text: string) => void;
  onUploadFiles: (sid: string, files: FileList | null) => void;
  onRemoveAttachment: (sid: string, aid: string) => void;
  onSubmit: (sid: string, startRun: boolean) => void;
  onPause: (mode: "soft" | "hard") => void;
  onKillTask: (tid: string) => void;
  onPatchSession: (
    sid: string,
    patch: Partial<Pick<SessionInfo, "model_id" | "reasoning_effort">>
  ) => void;
  onOpenUsage: (sid: string) => void;
}

const RING_CIRCUMFERENCE = 2 * Math.PI * 5.5;

export function Composer({
  session,
  providers,
  models,
  draft,
  uploading,
  sending,
  tasks,
  queuedCount,
  summary,
  clusterNodes = [],
  runNode = "",
  onChangeRunNode,
  onChangeText,
  onUploadFiles,
  onRemoveAttachment,
  onSubmit,
  onPause,
  onKillTask,
  onPatchSession,
  onOpenUsage,
}: ComposerProps) {
  const { t } = useI18n();
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

  const availableModels = models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const activeProviders = providers.filter((p) => !p.archived_at);
  const currentModel = models.find((m) => m.id === session.model_id);
  const efforts = currentModel?.reasoning_efforts || [];
  const ctxLimit = currentModel?.context_window_tokens;
  const lastIn = summary?.last_input_tokens;
  const ctxPct =
    lastIn != null && ctxLimit ? Math.min(100, Math.round((lastIn / ctxLimit) * 100)) : null;
  const totalTokens = totalCallTokens(summary);
  const totalInput = totalInputTokens(summary);
  const cachePct =
    totalInput > 0 && summary ? Math.round((summary.cache_read_tokens / totalInput) * 100) : null;
  const ringLevel = ctxPct == null ? "" : ctxPct >= 90 ? "danger" : ctxPct >= 75 ? "warn" : "";

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
            <span>
              {tasks.length} {t("active codemode")} {tasks.length === 1 ? t("task") : t("tasks")}
            </span>
            <span class="card-spacer" />
            <span class="muted small">{tasksOpen ? t("Hide") : t("Inspect")}</span>
            <Icon n="chevron" s={14} cls={`chev ${tasksOpen ? "open" : ""}`} />
          </button>
          {tasksOpen && (
            <div class="tasks-list">
              {tasks.map((task) => (
                <div key={task.id} class="task-row">
                  <div class="task-row-head">
                    <span class="mono">{task.id}</span>
                    {task.label && <span>{task.label}</span>}
                    <span class="pill">{task.delivery}</span>
                    <span class="pill mono task-ttl">
                      <Icon n="clock" s={11} />
                      <span>{formatRem(task.remaining_seconds)}</span>
                    </span>
                    <button
                      type="button"
                      class="btn-danger btn-xs"
                      disabled={task.state === "stopping"}
                      onClick={() => onKillTask(task.id)}
                    >
                      <Icon n="stop" s={11} />
                      <span>{task.state === "stopping" ? t("Stopping…") : t("Kill")}</span>
                    </button>
                  </div>
                  {task.running_commands?.map((c) => (
                    <div key={c.id} class="task-cmd">
                      <div class="mono muted small">
                        {c.node ? `[${c.node}] ` : ""}$ {c.program}
                      </div>
                      {c.stdout && <pre class="code-box">{c.stdout}</pre>}
                      {c.stderr && <pre class="code-box err-text">{c.stderr}</pre>}
                    </div>
                  ))}
                  {task.console?.output && (
                    <div>
                      {task.console.truncated && (
                        <div class="muted small">
                          {t("Console tail")} ({task.console.total_bytes || 0} B)
                        </div>
                      )}
                      <pre class="code-box">{task.console.output}</pre>
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
        {dragging && <div class="drop-overlay">{t("Drop image, audio, or video to attach")}</div>}

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
                  <Icon n="x" s={12} />
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
              ? t("Steer running session (queues for next step)…")
              : t("Message MyGo… (Enter to send, Shift+Enter for newline)")
          }
          onInput={(e) => onChangeText(session.id, e.currentTarget.value)}
          onKeyDown={onKeyDown}
          onPaste={onPaste}
        />

        <div class="composer-actions">
          <div class="row gap-1 composer-left">
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
              class="composer-icon-btn"
              disabled={uploading}
              title={uploading ? t("Uploading…") : t("Add media")}
              aria-label={t("Add media")}
              onClick={() => fileRef.current?.click()}
            >
              <Icon n="plus" s={16} />
            </button>

            {queuedCount > 0 && (
              <span class="pill pill-queued">
                {queuedCount} {t("queued")}
              </span>
            )}

            {!running && hasPayload && (
              <button
                type="button"
                class="composer-chip-btn"
                disabled={busy}
                title={t("Add message to queue without starting a run")}
                onClick={() => onSubmit(session.id, false)}
              >
                {sending ? t("Sending…") : t("Queue")}
              </button>
            )}

            {running && (
              <>
                <button
                  type="button"
                  class="composer-chip-btn"
                  disabled={session.status === "pausing"}
                  title={t("Pause after the current step finishes")}
                  onClick={() => onPause("soft")}
                >
                  <Icon n="pause" s={12} />
                  <span>{session.status === "pausing" ? t("Pausing…") : t("Pause")}</span>
                </button>
                {hasPayload && (
                  <button
                    type="button"
                    class="composer-chip-btn danger"
                    title={t("Stop the active run immediately")}
                    onClick={() => onPause("hard")}
                  >
                    <Icon n="stop" s={11} />
                    <span>{t("Stop")}</span>
                  </button>
                )}
              </>
            )}
          </div>

          <div class="row gap-1 composer-right">
            {clusterNodes.length > 1 && (
              <select
                class="composer-select"
                title={t("Execution Node")}
                disabled={running || Boolean(session.runner_node_id)}
                value={
                  session.runner_node_id
                    ? session.runner_node_id === clusterNodes.find((n) => n.isSelf)?.id
                      ? ""
                      : session.runner_node_id
                    : runNode
                }
                onChange={(e) => onChangeRunNode?.(session.id, e.currentTarget.value)}
              >
                {clusterNodes.map((n) => (
                  <option
                    key={n.isSelf ? "__self__" : n.id}
                    value={n.isSelf ? "" : n.id}
                    disabled={!n.online}
                  >
                    {n.isSelf ? `${t("This node")} (${n.name})` : `${n.name}${n.online ? "" : ` (${t("Offline")})`}`}
                  </option>
                ))}
              </select>
            )}

            <select
              class="composer-select"
              title={t("Session Model")}
              value={session.model_id}
              onChange={(e) => {
                const val = e.currentTarget.value;
                if (!val) return;
                onPatchSession(session.id, { model_id: val, reasoning_effort: null });
              }}
            >
              {availableModels.length === 0 && !currentModel && (
                <option value="">{t("No available models")}</option>
              )}
              {activeProviders.length > 1
                ? activeProviders.map((p) => {
                    const groupMods = availableModels.filter((m) => m.provider_id === p.id);
                    if (groupMods.length === 0) return null;
                    return (
                      <optgroup key={p.id} label={p.name}>
                        {groupMods.map((m) => (
                          <option key={m.id} value={m.id}>{m.model_key}</option>
                        ))}
                      </optgroup>
                    );
                  })
                : availableModels.map((m) => (
                    <option key={m.id} value={m.id}>{m.model_key}</option>
                  ))}
              {currentModel && (currentModel.archived_at || currentModel.provider_archived_at) && (
                <option value={currentModel.id}>{currentModel.model_key} ({t("archived")})</option>
              )}
            </select>

            {efforts.length > 0 && (
              <select
                class="composer-select"
                title={t("Reasoning Effort")}
                value={session.reasoning_effort ?? ""}
                onChange={(e) =>
                  onPatchSession(session.id, { reasoning_effort: e.currentTarget.value || null })
                }
              >
                <option value="">{t("auto")}</option>
                {efforts.map((ef) => (
                  <option key={ef} value={ef}>{ef}</option>
                ))}
              </select>
            )}

            {running && !hasPayload ? (
              <button
                type="button"
                class="send-circle stop"
                title={t("Stop the active run immediately")}
                aria-label={t("Stop")}
                onClick={() => onPause("hard")}
              >
                <Icon n="stop" s={14} />
              </button>
            ) : running ? (
              <button
                type="button"
                class="send-circle"
                disabled={busy}
                title={t("Queue message for the next step")}
                aria-label={t("Steer")}
                onClick={() => onSubmit(session.id, false)}
              >
                <Icon n="arrowUp" />
              </button>
            ) : hasPayload ? (
              <button
                type="button"
                class="send-circle"
                disabled={busy}
                title={t("Send & Run")}
                aria-label={t("Send & Run")}
                onClick={() => onSubmit(session.id, true)}
              >
                <Icon n="arrowUp" />
              </button>
            ) : (
              <button
                type="button"
                class="send-circle"
                disabled={busy || (session.status !== "paused" && queuedCount === 0)}
                title={session.status === "paused" ? t("Resume") : t("Run")}
                aria-label={session.status === "paused" ? t("Resume") : t("Run")}
                onClick={() => onSubmit(session.id, true)}
              >
                <Icon n="play" s={14} />
              </button>
            )}
          </div>
        </div>
      </div>

      {summary && (
        <button type="button" class="telemetry-bar" onClick={() => onOpenUsage(session.id)}>
          <span>{summary.call_count} {t("calls")}</span>
          <span class="sep">·</span>
          <span class="row gap-1">
            <Icon n="database" s={12} />
            <span>
              {formatTokens(totalTokens)} tok
              {cachePct != null ? ` · ${t("cache hit")} ${cachePct}%` : ""}
            </span>
          </span>
          <span class="sep">·</span>
          <span class="mono">{formatCost(summary.cost_nano_usd)}</span>
          {ctxPct != null && (
            <>
              <span class="sep">·</span>
              <span class={`row gap-1 ${ringLevel}`}>
                <svg width="14" height="14" viewBox="0 0 14 14" class="ring-meter" aria-hidden="true">
                  <circle cx="7" cy="7" r="5.5" class="ring-track" />
                  <circle
                    cx="7"
                    cy="7"
                    r="5.5"
                    class={`ring-fill ${ringLevel}`}
                    stroke-dasharray={RING_CIRCUMFERENCE}
                    stroke-dashoffset={RING_CIRCUMFERENCE * (1 - ctxPct / 100)}
                  />
                </svg>
                <span class="mono">{ctxPct}%</span>
              </span>
            </>
          )}
        </button>
      )}
    </div>
  );
}
