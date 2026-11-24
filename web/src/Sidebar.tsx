import { useEffect, useState } from "preact/hooks";
import { formatRelTime } from "./api.ts";
import type { ModelInfo, SessionInfo, Theme } from "./types.ts";

export interface SidebarProps {
  sessions: SessionInfo[];
  activeId: number | null;
  modelsById: Map<number, ModelInfo>;
  open: boolean;
  theme: Theme;
  onSelect: (id: number) => void;
  onCreate: () => void;
  onDelete: (id: number) => void;
  onToggleTheme: () => void;
  onOpenSettings: () => void;
  onClose: () => void;
}

export function Sidebar({
  sessions,
  activeId,
  modelsById,
  open,
  theme,
  onSelect,
  onCreate,
  onDelete,
  onToggleTheme,
  onOpenSettings,
  onClose,
}: SidebarProps) {
  const [confirmId, setConfirmId] = useState<number | null>(null);

  useEffect(() => {
    if (confirmId == null) return;
    const t = setTimeout(() => setConfirmId(null), 3000);
    return () => clearTimeout(t);
  }, [confirmId]);

  const sorted = [...sessions].sort(
    (a, b) => (b.updated_at || 0) - (a.updated_at || 0) || b.id - a.id
  );

  return (
    <>
      {open && <div class="backdrop" onClick={onClose} />}
      <aside class={`sidebar ${open ? "open" : ""}`}>
        <div class="sidebar-head">
          <div class="brand">
            <span class="brand-mark">M</span>
            <span>MyGo</span>
          </div>
          <div class="row gap-1">
            <button type="button" class="btn-icon" title="Toggle theme" onClick={onToggleTheme}>
              {theme === "dark" ? "☀" : "☾"}
            </button>
            <button type="button" class="btn-icon" title="Settings" onClick={onOpenSettings}>
              ⚙
            </button>
            <button type="button" class="btn-primary btn-sm" onClick={onCreate}>
              + New
            </button>
          </div>
        </div>

        <div class="session-list">
          {sorted.length === 0 && (
            <div class="empty-hint">No sessions yet. Create one to begin.</div>
          )}
          {sorted.map((s) => {
            const active = s.id === activeId;
            const busy = s.status === "running" || s.status === "pausing";
            const model = s.model_id ? modelsById.get(s.model_id) : null;
            const dotClass =
              s.status === "paused" && s.last_error ? "dot-error" : `dot-${s.status}`;
            const confirming = confirmId === s.id;

            return (
              <div
                key={s.id}
                class={`session-item ${active ? "active" : ""}`}
                onClick={() => {
                  onSelect(s.id);
                  onClose();
                }}
              >
                <span class={`status-dot ${dotClass}`} title={s.status} />
                <div class="session-meta">
                  <div class="session-title">{s.title || `Session #${s.id}`}</div>
                  <div class="session-sub">
                    <span class="truncate">{model ? model.model_key : "no model"}</span>
                    <span>{formatRelTime(s.updated_at)}</span>
                  </div>
                </div>
                {!busy && (
                  <button
                    type="button"
                    class={`session-del ${confirming ? "confirm" : ""}`}
                    title={confirming ? "Click again to delete" : "Delete session"}
                    onClick={(e) => {
                      e.stopPropagation();
                      if (confirming) {
                        setConfirmId(null);
                        onDelete(s.id);
                      } else {
                        setConfirmId(s.id);
                      }
                    }}
                  >
                    {confirming ? "Del?" : "×"}
                  </button>
                )}
              </div>
            );
          })}
        </div>
      </aside>
    </>
  );
}
