import type { TargetedClipboardEvent, TargetedDragEvent, TargetedKeyboardEvent } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { formatCost, formatTokens } from "./api.ts";
import { useI18n } from "./i18n.ts";
import {
  IconArrowUp,
  IconChevron,
  IconClock,
  IconDatabase,
  IconPause,
  IconPlay,
  IconPlus,
  IconStop,
  IconX,
} from "./Icons.tsx";
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
  onChangeText: (sid: number, text: string) => void;
  onUploadFiles: (sid: number, files: FileList | null) => void;
  onRemoveAttachment: (sid: number, aid: number) => void;
  onSubmit: (sid: number, startRun: boolean) => void;
  onPause: (mode: "soft" | "hard") => void;
  onKillTask: (tid: string) => void;
  onPatchSession: (
    sid: number,
    patch: Partial<Pick<SessionInfo, "model_id" | "reasoning_effort">>
  ) => void;
  onOpenUsage: (sid: number) => void;
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
  const totalTokens = summary
    ? summary.input_uncached_tokens + summary.cache_read_tokens + summary.output_tokens
    : 0;
  const totalInput = summary ? summary.input_uncached_tokens + summary.cache_read_tokens : 0;
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
              {tasks.length} {t("activeTasks")} {tasks.length === 1 ? t("taskWord") : t("tasksWord")}
            </span>
            <span class="card-spacer" />
            <span class="muted small">{tasksOpen ? t("hide") : t("inspect")}</span>
            <IconChevron size={14} cls={`chev ${tasksOpen ? "open" : ""}`} />
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
                      <IconClock size={11} />
                      <span>{formatRem(task.remaining_seconds)}</span>
                    </span>
                    <button
                      type="button"
                      class="btn-danger btn-xs"
                      disabled={task.state === "stopping"}
                      onClick={() => onKillTask(task.id)}
                    >
                      <IconStop size={11} />
                      <span>{task.state === "stopping" ? t("stopping") : t("kill")}</span>
                    </button>
                  </div>
                  {task.running_commands?.map((c) => (
                    <div key={c.id} class="task-cmd">
                      <div class="mono muted small">$ {c.program} {(c.args || []).join(" ")}</div>
                      {c.stdout && <pre class="code-box">{c.stdout}</pre>}
                      {c.stderr && <pre class="code-box err-text">{c.stderr}</pre>}
                    </div>
                  ))}
                  {task.console?.output && (
                    <div>
                      {task.console.truncated && (
                        <div class="muted small">
                          {t("consoleTail")} ({task.console.total_bytes || 0} B)
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
        {dragging && <div class="drop-overlay">{t("dropPrompt")}</div>}

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
          placeholder={running ? t("placeholderRun") : t("placeholderIdle")}
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
              title={uploading ? t("uploading") : t("uploadMedia")}
              aria-label={t("uploadMedia")}
              onClick={() => fileRef.current?.click()}
            >
              <IconPlus size={16} />
            </button>

            {queuedCount > 0 && (
              <span class="pill pill-queued">
                {queuedCount} {t("queuedSuffix")}
              </span>
            )}

            {!running && hasPayload && (
              <button
                type="button"
                class="composer-chip-btn"
                disabled={busy}
                title={t("queueTip")}
                onClick={() => onSubmit(session.id, false)}
              >
                {sending ? t("sending") : t("queue")}
              </button>
            )}

            {running && (
              <>
                <button
                  type="button"
                  class="composer-chip-btn"
                  disabled={session.status === "pausing"}
                  title={t("pauseTip")}
                  onClick={() => onPause("soft")}
                >
                  <IconPause size={12} />
                  <span>{session.status === "pausing" ? t("pausing") : t("pause")}</span>
                </button>
                {hasPayload && (
                  <button
                    type="button"
                    class="composer-chip-btn danger"
                    title={t("stopTip")}
                    onClick={() => onPause("hard")}
                  >
                    <IconStop size={11} />
                    <span>{t("stop")}</span>
                  </button>
                )}
              </>
            )}
          </div>

          <div class="row gap-1 composer-right">
            <select
              class="composer-select"
              title={t("sessionModel")}
              value={session.model_id ?? ""}
              onChange={(e) => {
                const val = e.currentTarget.value ? Number(e.currentTarget.value) : null;
                onPatchSession(session.id, { model_id: val, reasoning_effort: null });
              }}
            >
              <option value="">{t("noModel")}</option>
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
                title={t("reasoningEffort")}
                value={session.reasoning_effort ?? ""}
                onChange={(e) =>
                  onPatchSession(session.id, { reasoning_effort: e.currentTarget.value || null })
                }
              >
                <option value="">{t("autoEffort")}</option>
                {efforts.map((ef) => (
                  <option key={ef} value={ef}>{ef}</option>
                ))}
              </select>
            )}

            {running && !hasPayload ? (
              <button
                type="button"
                class="send-circle stop"
                title={t("stopTip")}
                aria-label={t("stop")}
                onClick={() => onPause("hard")}
              >
                <IconStop size={14} />
              </button>
            ) : running ? (
              <button
                type="button"
                class="send-circle"
                disabled={busy}
                title={t("steerTip")}
                aria-label={t("steer")}
                onClick={() => onSubmit(session.id, false)}
              >
                <IconArrowUp size={15} />
              </button>
            ) : hasPayload ? (
              <button
                type="button"
                class="send-circle"
                disabled={busy}
                title={t("sendAndRun")}
                aria-label={t("sendAndRun")}
                onClick={() => onSubmit(session.id, true)}
              >
                <IconArrowUp size={15} />
              </button>
            ) : (
              <button
                type="button"
                class="send-circle"
                disabled={busy || (session.status !== "paused" && queuedCount === 0)}
                title={session.status === "paused" ? t("resume") : t("run")}
                aria-label={session.status === "paused" ? t("resume") : t("run")}
                onClick={() => onSubmit(session.id, true)}
              >
                <IconPlay size={14} />
              </button>
            )}
          </div>
        </div>
      </div>

      {summary && (
        <button
          type="button"
          class="telemetry-bar"
          onClick={() => onOpenUsage(session.id)}
        >
          <span>{summary.call_count} {t("callsSuffix")}</span>
          <span class="sep">·</span>
          <span class="row gap-1">
            <IconDatabase size={12} />
            <span>
              {formatTokens(totalTokens)} tok
              {cachePct != null ? ` · ${t("cacheHit")} ${cachePct}%` : ""}
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
