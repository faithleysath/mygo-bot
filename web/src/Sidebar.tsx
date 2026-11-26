import type { RefObject } from "preact";
import { useEffect, useState } from "preact/hooks";
import { formatAbsTime } from "./api.ts";
import { dateGroupLabelLoc, formatRelTimeLoc, statusText, useI18n } from "./i18n.ts";
import { BrandMark, Icon } from "./Icons.tsx";
import type { SettingsTabId } from "./Settings.tsx";
import type { SessionInfo, ThemePref } from "./types.ts";

export interface SidebarProps {
  sessions: SessionInfo[];
  activeId: string | null;
  open: boolean;
  collapsed: boolean;
  searchOpen: boolean;
  nodeNameById?: Map<string, string>;
  themePref: ThemePref;
  searchInputRef: RefObject<HTMLInputElement | null>;
  onSelect: (id: string) => void;
  onCreate: () => void;
  onDelete: (id: string) => void;
  onCycleTheme: () => void;
  onOpenSettingsTab: (tab: SettingsTabId) => void;
  onToggleCollapse: () => void;
  onSetSearchOpen: (open: boolean) => void;
  onClose: () => void;
}

export function Sidebar({
  sessions,
  activeId,
  open,
  collapsed,
  searchOpen,
  nodeNameById,
  themePref,
  searchInputRef,
  onSelect,
  onCreate,
  onDelete,
  onCycleTheme,
  onOpenSettingsTab,
  onToggleCollapse,
  onSetSearchOpen,
  onClose,
}: SidebarProps) {
  const { locale, t } = useI18n();
  const [confirmId, setConfirmId] = useState<string | null>(null);
  const [query, setQuery] = useState("");

  useEffect(() => {
    if (confirmId == null) return;
    const tmr = setTimeout(() => setConfirmId(null), 3000);
    return () => clearTimeout(tmr);
  }, [confirmId]);

  const sorted = [...sessions].sort(
    (a, b) => (b.updated_at || 0) - (a.updated_at || 0) || (b.id > a.id ? 1 : b.id < a.id ? -1 : 0)
  );

  const q = query.trim().toLowerCase();
  const filtered = q
    ? sorted.filter((s) => {
        const title = (s.title || `${t("Session")} ${s.id}`).toLowerCase();
        return title.includes(q) || s.id.toLowerCase().includes(q);
      })
    : sorted;

  const uniqueGroups = new Set(filtered.map((s) => dateGroupLabelLoc(s.updated_at || s.created_at, locale)));
  const showGroups = uniqueGroups.size > 1;
  let lastGroup = "";

  const themeLabel =
    themePref === "system" ? t("System") : themePref === "dark" ? t("Dark") : t("Light");
  const themeIcon = themePref === "system" ? "monitor" : themePref === "dark" ? "moon" : "sun";

  return (
    <>
      {open && <div class="backdrop" onClick={onClose} />}
      <aside class={`sidebar ${open ? "open" : ""} ${collapsed ? "collapsed" : ""}`}>
        <div class="sidebar-head">
          <div class="brand">
            <BrandMark size={22} />
            <span class="brand-name">MyGo</span>
            <span class="brand-tag">BOT</span>
          </div>
          <div class="row gap-1">
            <button
              type="button"
              class="btn-icon desktop-only"
              title={t("Collapse sidebar")}
              aria-label={t("Collapse sidebar")}
              onClick={onToggleCollapse}
            >
              <Icon n="sidebar" />
            </button>
            <button
              type="button"
              class="btn-icon mobile-only"
              title={t("Close sidebar")}
              aria-label={t("Close sidebar")}
              onClick={onClose}
            >
              <Icon n="x" />
            </button>
          </div>
        </div>

        <div class="sidebar-top">
          <button
            type="button"
            class="new-session-btn"
            onClick={() => {
              onCreate();
              onClose();
            }}
          >
            <Icon n="plus" />
            <span>{t("New Session")}</span>
          </button>

          <nav class="sidebar-nav">
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("models"); onClose(); }}>
              <Icon n="sliders" />
              <span>{t("Models & Providers")}</span>
            </button>
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("mcp"); onClose(); }}>
              <Icon n="plug" />
              <span>{t("MCP Servers")}</span>
            </button>
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("cluster"); onClose(); }}>
              <Icon n="globe" />
              <span>{t("Cluster")}</span>
            </button>
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("usage"); onClose(); }}>
              <Icon n="chart" />
              <span>{t("Usage & Cost")}</span>
            </button>
          </nav>
        </div>

        <div class="sessions-head">
          <span class="sessions-label">{t("Sessions")}</span>
          <div class="row gap-1">
            {sessions.length > 0 && (
              <button
                type="button"
                class={`btn-icon btn-xs ${searchOpen || query ? "active-icon" : ""}`}
                title={t("Search sessions")}
                aria-label={t("Search sessions")}
                onClick={() => {
                  const next = !(searchOpen || Boolean(query));
                  onSetSearchOpen(next);
                  if (!next) setQuery("");
                  else requestAnimationFrame(() => searchInputRef.current?.focus());
                }}
              >
                <Icon n="search" s={13} />
              </button>
            )}
            <button
              type="button"
              class="btn-icon btn-xs"
              title={t("New Session")}
              aria-label={t("New Session")}
              onClick={() => {
                onCreate();
                onClose();
              }}
            >
              <Icon n="plus" s={13} />
            </button>
          </div>
        </div>

        {(searchOpen || query) && sessions.length > 0 && (
          <div class="search-wrap">
            <div class="search-box">
              <Icon n="search" s={13} />
              <input
                ref={searchInputRef}
                type="text"
                value={query}
                placeholder={t("Filter sessions… (⌘K)")}
                onInput={(e) => setQuery(e.currentTarget.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") {
                    setQuery("");
                    onSetSearchOpen(false);
                    e.currentTarget.blur();
                  }
                }}
              />
              {query && (
                <button
                  type="button"
                  class="search-clear"
                  aria-label={t("Clear filter")}
                  onClick={() => setQuery("")}
                >
                  <Icon n="x" s={12} />
                </button>
              )}
            </div>
          </div>
        )}

        <div class="session-list">
          {sorted.length === 0 && <div class="empty-hint">{t("No sessions yet.")}</div>}
          {sorted.length > 0 && filtered.length === 0 && (
            <div class="empty-hint">{t("No matching sessions")}</div>
          )}
          {filtered.map((s) => {
            const active = s.id === activeId;
            const busy = s.status === "running" || s.status === "pausing";
            const occupied = busy || Boolean(s.runner_node_id);
            const runnerName = s.runner_node_id
              ? nodeNameById?.get(s.runner_node_id) || s.runner_node_id
              : "";
            const dotClass =
              s.status === "paused" && s.last_error ? "dot-error" : `dot-${s.status}`;
            const confirming = confirmId === s.id;
            const group = dateGroupLabelLoc(s.updated_at || s.created_at, locale);
            const showGroup = showGroups && group !== lastGroup;
            lastGroup = group;
            const baseStatus = runnerName
              ? `${statusText(s.status, locale)} @${runnerName}`
              : statusText(s.status, locale);
            const tip = s.last_error ? `${baseStatus}: ${s.last_error}` : baseStatus;

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
                  <span class={`status-dot ${dotClass}`} title={tip} />
                  <span class="session-title">{s.title || `${t("Session")} ${s.id}`}</span>
                  <span class="session-time" title={formatAbsTime(s.updated_at)}>
                    {formatRelTimeLoc(s.updated_at, locale)}
                  </span>
                  {!occupied && (
                    <button
                      type="button"
                      class={`session-del ${confirming ? "confirm" : ""}`}
                      title={confirming ? t("Click again to delete") : t("Delete session")}
                      aria-label={confirming ? t("Click again to delete") : t("Delete session")}
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
                      {confirming ? t("Delete?") : <Icon n="trash" s={13} />}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>

        <div class="sidebar-foot">
          <button type="button" class="foot-btn" title={themeLabel} onClick={onCycleTheme}>
            <Icon n={themeIcon} s={14} />
            <span>{themeLabel}</span>
          </button>
          <button type="button" class="foot-btn" onClick={() => { onOpenSettingsTab("general"); onClose(); }}>
            <Icon n="settings" s={14} />
            <span>{t("Settings")}</span>
          </button>
        </div>
      </aside>
    </>
  );
}
