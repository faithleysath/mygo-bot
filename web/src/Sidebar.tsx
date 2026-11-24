import type { RefObject } from "preact";
import { useEffect, useState } from "preact/hooks";
import { dateGroupLabelLoc, formatRelTimeLoc, statusText, useI18n } from "./i18n.ts";
import { formatAbsTime } from "./api.ts";
import {
  BrandMark,
  IconChart,
  IconMonitor,
  IconMoon,
  IconPlug,
  IconPlus,
  IconSearch,
  IconSettings,
  IconSidebar,
  IconSliders,
  IconSun,
  IconTrash,
  IconX,
} from "./Icons.tsx";
import type { SettingsTabId } from "./Settings.tsx";
import type { SessionInfo, ThemePref } from "./types.ts";

export interface SidebarProps {
  sessions: SessionInfo[];
  activeId: number | null;
  open: boolean;
  collapsed: boolean;
  searchOpen: boolean;
  themePref: ThemePref;
  searchInputRef: RefObject<HTMLInputElement | null>;
  onSelect: (id: number) => void;
  onCreate: () => void;
  onDelete: (id: number) => void;
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
  const [confirmId, setConfirmId] = useState<number | null>(null);
  const [query, setQuery] = useState("");

  useEffect(() => {
    if (confirmId == null) return;
    const tmr = setTimeout(() => setConfirmId(null), 3000);
    return () => clearTimeout(tmr);
  }, [confirmId]);

  const sorted = [...sessions].sort(
    (a, b) => (b.updated_at || 0) - (a.updated_at || 0) || b.id - a.id
  );

  const q = query.trim().toLowerCase();
  const filtered = q
    ? sorted.filter((s) => {
        const title = (s.title || `${t("sessionPrefix")} #${s.id}`).toLowerCase();
        return title.includes(q) || `#${s.id}`.includes(q) || String(s.id).includes(q);
      })
    : sorted;

  const uniqueGroups = new Set(filtered.map((s) => dateGroupLabelLoc(s.updated_at || s.created_at, locale)));
  const showGroups = uniqueGroups.size > 1;
  let lastGroup = "";

  const themeLabel =
    themePref === "system" ? t("themeSystem") : themePref === "dark" ? t("themeDark") : t("themeLight");

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
              title={t("collapseSidebar")}
              aria-label={t("collapseSidebar")}
              onClick={onToggleCollapse}
            >
              <IconSidebar size={15} />
            </button>
            <button
              type="button"
              class="btn-icon mobile-only"
              title={t("closeSidebar")}
              aria-label={t("closeSidebar")}
              onClick={onClose}
            >
              <IconX size={15} />
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
            <IconPlus size={15} />
            <span>{t("newSession")}</span>
          </button>

          <nav class="sidebar-nav">
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("models"); onClose(); }}>
              <IconSliders size={15} />
              <span>{t("navModels")}</span>
            </button>
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("mcp"); onClose(); }}>
              <IconPlug size={15} />
              <span>{t("navMcp")}</span>
            </button>
            <button type="button" class="nav-item" onClick={() => { onOpenSettingsTab("usage"); onClose(); }}>
              <IconChart size={15} />
              <span>{t("navUsage")}</span>
            </button>
          </nav>
        </div>

        <div class="sessions-head">
          <span class="sessions-label">{t("sessions")}</span>
          <div class="row gap-1">
            {sessions.length > 0 && (
              <button
                type="button"
                class={`btn-icon btn-xs ${searchOpen || query ? "active-icon" : ""}`}
                title={t("searchSessions")}
                aria-label={t("searchSessions")}
                onClick={() => {
                  const next = !(searchOpen || Boolean(query));
                  onSetSearchOpen(next);
                  if (!next) {
                    setQuery("");
                  } else {
                    requestAnimationFrame(() => searchInputRef.current?.focus());
                  }
                }}
              >
                <IconSearch size={13} />
              </button>
            )}
            <button
              type="button"
              class="btn-icon btn-xs"
              title={t("newSession")}
              aria-label={t("newSession")}
              onClick={() => {
                onCreate();
                onClose();
              }}
            >
              <IconPlus size={13} />
            </button>
          </div>
        </div>

        {(searchOpen || query) && sessions.length > 0 && (
          <div class="search-wrap">
            <div class="search-box">
              <IconSearch size={13} />
              <input
                ref={searchInputRef}
                type="text"
                value={query}
                placeholder={t("filterSessions")}
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
                  aria-label={t("clearFilter")}
                  onClick={() => setQuery("")}
                >
                  <IconX size={12} />
                </button>
              )}
            </div>
          </div>
        )}

        <div class="session-list">
          {sorted.length === 0 && <div class="empty-hint">{t("noSessions")}</div>}
          {sorted.length > 0 && filtered.length === 0 && (
            <div class="empty-hint">{t("noMatchingSessions")}</div>
          )}
          {filtered.map((s) => {
            const active = s.id === activeId;
            const busy = s.status === "running" || s.status === "pausing";
            const dotClass =
              s.status === "paused" && s.last_error ? "dot-error" : `dot-${s.status}`;
            const confirming = confirmId === s.id;
            const group = dateGroupLabelLoc(s.updated_at || s.created_at, locale);
            const showGroup = showGroups && group !== lastGroup;
            lastGroup = group;
            const tip = s.last_error ? `${statusText(s.status, locale)}: ${s.last_error}` : statusText(s.status, locale);

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
                  <span class="session-title">{s.title || `${t("sessionPrefix")} #${s.id}`}</span>
                  <span class="session-time" title={formatAbsTime(s.updated_at)}>
                    {formatRelTimeLoc(s.updated_at, locale)}
                  </span>
                  {!busy && (
                    <button
                      type="button"
                      class={`session-del ${confirming ? "confirm" : ""}`}
                      title={confirming ? t("confirmDelete") : t("deleteSession")}
                      aria-label={confirming ? t("confirmDelete") : t("deleteSession")}
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
                      {confirming ? t("deleteShort") : <IconTrash size={13} />}
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
            title={themeLabel}
            onClick={onCycleTheme}
          >
            {themePref === "system" ? (
              <IconMonitor size={14} />
            ) : themePref === "dark" ? (
              <IconMoon size={14} />
            ) : (
              <IconSun size={14} />
            )}
            <span>{themeLabel}</span>
          </button>
          <button type="button" class="foot-btn" onClick={() => { onOpenSettingsTab("general"); onClose(); }}>
            <IconSettings size={14} />
            <span>{t("settings")}</span>
          </button>
        </div>
      </aside>
    </>
  );
}
