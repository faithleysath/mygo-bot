import type { RefObject } from "preact";
import { useEffect, useState } from "preact/hooks";
import { dateGroupLabel, formatAbsTime, formatRelTime } from "./api.ts";
import {
  BrandMark,
  IconMonitor,
  IconMoon,
  IconPlus,
  IconSearch,
  IconSettings,
  IconSidebar,
  IconSun,
  IconTrash,
  IconX,
} from "./Icons.tsx";
import type { ModelInfo, SessionInfo, ThemePref } from "./types.ts";

export interface SidebarProps {
  sessions: SessionInfo[];
  activeId: number | null;
  modelsById: Map<number, ModelInfo>;
  open: boolean;
  collapsed: boolean;
  themePref: ThemePref;
  searchInputRef: RefObject<HTMLInputElement | null>;
  onSelect: (id: number) => void;
  onCreate: () => void;
  onDelete: (id: number) => void;
  onCycleTheme: () => void;
  onOpenSettings: () => void;
  onToggleCollapse: () => void;
  onClose: () => void;
}

const THEME_LABEL: Record<ThemePref, string> = {
  system: "System",
  light: "Light",
  dark: "Dark",
};

export function Sidebar({
  sessions,
  activeId,
  modelsById,
  open,
  collapsed,
  themePref,
  searchInputRef,
  onSelect,
  onCreate,
  onDelete,
  onCycleTheme,
  onOpenSettings,
  onToggleCollapse,
  onClose,
}: SidebarProps) {
  const [confirmId, setConfirmId] = useState<number | null>(null);
  const [query, setQuery] = useState("");

  useEffect(() => {
    if (confirmId == null) return;
    const t = setTimeout(() => setConfirmId(null), 3000);
    return () => clearTimeout(t);
  }, [confirmId]);

  const sorted = [...sessions].sort(
    (a, b) => (b.updated_at || 0) - (a.updated_at || 0) || b.id - a.id
  );

  const q = query.trim().toLowerCase();
  const filtered = q
    ? sorted.filter((s) => {
        const model = s.model_id ? modelsById.get(s.model_id)?.model_key || "" : "";
        const title = (s.title || `Session #${s.id}`).toLowerCase();
        return (
          title.includes(q) ||
          `#${s.id}`.includes(q) ||
          String(s.id).includes(q) ||
          model.toLowerCase().includes(q)
        );
      })
    : sorted;

  let lastGroup = "";

  return (
    <>
      {open && <div class="backdrop" onClick={onClose} />}
      <aside class={`sidebar ${open ? "open" : ""} ${collapsed ? "collapsed" : ""}`}>
        <div class="sidebar-head">
          <div class="brand">
            <BrandMark size={22} />
            <span>MyGo</span>
          </div>
          <div class="row gap-1">
            <button
              type="button"
              class="btn-icon desktop-only"
              title="Collapse sidebar"
              aria-label="Collapse sidebar"
              onClick={onToggleCollapse}
            >
              <IconSidebar size={15} />
            </button>
            <button
              type="button"
              class="btn-icon mobile-only"
              title="Close sidebar"
              aria-label="Close sidebar"
              onClick={onClose}
            >
              <IconX size={15} />
            </button>
          </div>
        </div>

        <div class="sidebar-actions">
          <button
            type="button"
            class="btn-primary new-session-btn"
            onClick={() => {
              onCreate();
              onClose();
            }}
          >
            <IconPlus size={15} />
            <span>New Session</span>
          </button>

          {sessions.length > 0 && (
            <div class="search-box">
              <IconSearch size={13} />
              <input
                ref={searchInputRef}
                type="text"
                value={query}
                placeholder="Filter sessions… (⌘K)"
                onInput={(e) => setQuery(e.currentTarget.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape" && query) {
                    setQuery("");
                    e.currentTarget.blur();
                  }
                }}
              />
              {query && (
                <button
                  type="button"
                  class="search-clear"
                  aria-label="Clear filter"
                  onClick={() => setQuery("")}
                >
                  <IconX size={12} />
                </button>
              )}
            </div>
          )}
        </div>

        <div class="session-list">
          {sorted.length === 0 && (
            <div class="empty-hint">No sessions yet. Create one to begin.</div>
          )}
          {sorted.length > 0 && filtered.length === 0 && (
            <div class="empty-hint">No matching sessions</div>
          )}
          {filtered.map((s) => {
            const active = s.id === activeId;
            const busy = s.status === "running" || s.status === "pausing";
            const model = s.model_id ? modelsById.get(s.model_id) : null;
            const dotClass =
              s.status === "paused" && s.last_error ? "dot-error" : `dot-${s.status}`;
            const confirming = confirmId === s.id;
            const group = dateGroupLabel(s.updated_at || s.created_at);
            const showGroup = group !== lastGroup;
            lastGroup = group;

            return (
              <div key={s.id}>
                {showGroup && <div class="session-group-label">{group}</div>}
                <div
                  class={`session-item ${active ? "active" : ""}`}
                  onClick={() => {
                    onSelect(s.id);
                    onClose();
                  }}
                >
                  <span
                    class={`status-dot ${dotClass}`}
                    title={s.last_error ? `${s.status}: ${s.last_error}` : s.status}
                  />
                  <div class="session-meta">
                    <div class="session-title">{s.title || `Session #${s.id}`}</div>
                    <div class="session-sub">
                      <span class="truncate">{model ? model.model_key : "no model"}</span>
                      <span title={formatAbsTime(s.updated_at)}>{formatRelTime(s.updated_at)}</span>
                    </div>
                  </div>
                  {!busy && (
                    <button
                      type="button"
                      class={`session-del ${confirming ? "confirm" : ""}`}
                      title={confirming ? "Click again to delete" : "Delete session"}
                      aria-label={confirming ? "Confirm delete session" : "Delete session"}
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
                      {confirming ? "Delete?" : <IconTrash size={13} />}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        <div class="sidebar-foot">
          <button
            type="button"
            class="foot-btn"
            title={`Theme: ${THEME_LABEL[themePref]} (click to switch)`}
            onClick={onCycleTheme}
          >
            {themePref === "system" ? (
              <IconMonitor size={14} />
            ) : themePref === "dark" ? (
              <IconMoon size={14} />
            ) : (
              <IconSun size={14} />
            )}
            <span>{THEME_LABEL[themePref]}</span>
          </button>
          <button type="button" class="foot-btn" onClick={onOpenSettings}>
            <IconSettings size={14} />
            <span>Settings</span>
          </button>
        </div>
      </aside>
    </>
  );
}
