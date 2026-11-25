import type { TargetedSubmitEvent } from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import { apiFetch, errorMessage, formatAbsTime, formatCost, formatDuration, formatTokens, totalInputTokens } from "./api.ts";
import { formatRelTimeLoc, statusText, useI18n } from "./i18n.ts";
import type { LocalePref } from "./i18n.ts";
import { Icon } from "./Icons.tsx";
import { buildMcpPayload, createMcpForm } from "./state.ts";
import { CopyBtn } from "./Timeline.tsx";
import type {
  CallsSummary,
  ClusterStatus,
  ClusterSyncSummary,
  LlmCallInfo,
  McpFormState,
  McpOAuthAction,
  McpOAuthLoginInfo,
  McpOAuthStatusResponse,
  McpProbeResult,
  McpSecretAction,
  McpServerInfo,
  McpToolPolicy,
  McpTransport,
  ModelInfo,
  ProviderInfo,
  SessionInfo,
  SyncConflictInfo,
  ThemePref,
} from "./types.ts";

interface ProviderFormState {
  id: string | null;
  name: string;
  base_url: string;
  api_key: string;
  clearKey: boolean;
}

interface ModelFormState {
  id: string | null;
  provider_id: string;
  model_key: string;
  context_window_tokens: number | string;
  input_modalities: string[];
  reasoning_efforts: string;
  pricingText: string;
}

function ModelsTab({
  providers,
  models,
  onRefresh,
}: {
  providers: ProviderInfo[];
  models: ModelInfo[];
  onRefresh: () => Promise<void>;
}) {
  const { t } = useI18n();
  const [showArchived, setShowArchived] = useState(false);
  const [err, setErr] = useState("");
  const [provForm, setProvForm] = useState<ProviderFormState | null>(null);
  const [modelForm, setModelForm] = useState<ModelFormState | null>(null);

  const providersById = useMemo(() => new Map(providers.map((p) => [p.id, p])), [providers]);
  const visibleProviders = showArchived ? providers : providers.filter((p) => !p.archived_at);
  const visibleModels = showArchived ? models : models.filter((m) => !m.archived_at && !m.provider_archived_at);

  const run = async (fn: () => Promise<void>) => {
    setErr("");
    try {
      await fn();
      await onRefresh();
    } catch (e) {
      setErr(errorMessage(e));
    }
  };

  const saveProvider = (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const f = provForm;
    if (!f) return;
    const body: Record<string, unknown> = { name: f.name, base_url: f.base_url };
    if (f.api_key !== "") body.api_key = f.api_key;
    else if (f.clearKey) body.api_key = null;
    void run(async () => {
      if (f.id) await apiFetch(`/providers/${f.id}`, { method: "PATCH", body });
      else await apiFetch("/providers", { method: "POST", body });
      setProvForm(null);
    });
  };

  const saveModel = (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const f = modelForm;
    if (!f) return;
    let pricing: unknown = null;
    if (f.pricingText.trim()) {
      try {
        pricing = JSON.parse(f.pricingText);
      } catch {
        setErr(t("Pricing must be valid JSON"));
        return;
      }
    }
    const body = {
      provider_id: f.provider_id,
      model_key: f.model_key,
      context_window_tokens: f.context_window_tokens ? Number(f.context_window_tokens) : null,
      input_modalities: f.input_modalities,
      reasoning_efforts: f.reasoning_efforts
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean),
      pricing,
    };
    void run(async () => {
      if (f.id) await apiFetch(`/models/${f.id}`, { method: "PATCH", body });
      else await apiFetch("/models", { method: "POST", body });
      setModelForm(null);
    });
  };

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}

      <section class="panel">
        <div class="panel-head">
          <h4>{t("Providers")}</h4>
          <div class="row gap-2">
            <label class="check-row">
              <input type="checkbox" checked={showArchived} onChange={(e) => setShowArchived(e.currentTarget.checked)} />
              {t("Show archived")}
            </label>
            <button
              type="button"
              class="btn-ghost btn-xs"
              onClick={() => setProvForm({ id: null, name: "", base_url: "", api_key: "", clearKey: false })}
            >
              {t("+ Provider")}
            </button>
          </div>
        </div>
        {provForm && (
          <form class="form-box" onSubmit={saveProvider}>
            <div class="grid-2">
              <label class="field">
                <span>{t("Name")}</span>
                <input required value={provForm.name} onInput={(e) => setProvForm({ ...provForm, name: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>{t("Base URL")}</span>
                <input required placeholder="https://api.openai.com/v1" value={provForm.base_url} onInput={(e) => setProvForm({ ...provForm, base_url: e.currentTarget.value })} />
              </label>
            </div>
            <label class="field">
              <span>{t("API Key")} {provForm.id ? t("(leave blank to keep)") : ""}</span>
              <input type="password" value={provForm.api_key} onInput={(e) => setProvForm({ ...provForm, api_key: e.currentTarget.value })} />
            </label>
            {provForm.id && (
              <label class="check-row">
                <input type="checkbox" checked={provForm.clearKey} onChange={(e) => setProvForm({ ...provForm, clearKey: e.currentTarget.checked })} />
                {t("Clear stored key")}
              </label>
            )}
            <div class="row gap-1">
              <button type="submit" class="btn-primary btn-xs">{t("Save")}</button>
              <button type="button" class="btn-ghost btn-xs" onClick={() => setProvForm(null)}>{t("Cancel")}</button>
            </div>
          </form>
        )}
        <div class="item-list">
          {visibleProviders.length === 0 && (
            <div class="empty-hint small">{t("No providers configured yet.")}</div>
          )}
          {visibleProviders.map((p) => (
            <div key={p.id} class="list-row">
              <div>
                <strong>{p.name}</strong>{" "}
                {p.archived_at && <span class="pill">{t("archived")}</span>}
                <div class="muted small">{p.base_url} · {p.has_api_key ? t("key set") : t("no key")}</div>
              </div>
              <div class="row gap-1">
                <button type="button" class="btn-ghost btn-xs" onClick={() => setProvForm({ id: p.id, name: p.name, base_url: p.base_url, api_key: "", clearKey: false })}>
                  {t("Edit")}
                </button>
                {p.archived_at ? (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/providers/${p.id}/restore`, { method: "POST" }).then(() => undefined))}>
                    {t("Restore")}
                  </button>
                ) : (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/providers/${p.id}`, { method: "DELETE" }).then(() => undefined))}>
                    {t("Archive")}
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h4>{t("Models")}</h4>
          <button
            type="button"
            class="btn-ghost btn-xs"
            disabled={visibleProviders.filter((p) => !p.archived_at).length === 0}
            onClick={() =>
              setModelForm({
                id: null,
                provider_id: visibleProviders.find((p) => !p.archived_at)?.id || "",
                model_key: "",
                context_window_tokens: "",
                input_modalities: ["text"],
                reasoning_efforts: "low, medium, high",
                pricingText: "",
              })
            }
          >
            {t("+ Model")}
          </button>
        </div>
        {modelForm && (
          <form class="form-box" onSubmit={saveModel}>
            <div class="grid-2">
              <label class="field">
                <span>{t("Provider")}</span>
                <select value={modelForm.provider_id} onChange={(e) => setModelForm({ ...modelForm, provider_id: e.currentTarget.value })}>
                  {visibleProviders.filter((p) => !p.archived_at).map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>
              <label class="field">
                <span>{t("Model Key")}</span>
                <input required value={modelForm.model_key} onInput={(e) => setModelForm({ ...modelForm, model_key: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>{t("Context Window")}</span>
                <input type="number" min="1" placeholder={t("optional")} value={modelForm.context_window_tokens} onInput={(e) => setModelForm({ ...modelForm, context_window_tokens: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>{t("Efforts (comma-separated)")}</span>
                <input value={modelForm.reasoning_efforts} onInput={(e) => setModelForm({ ...modelForm, reasoning_efforts: e.currentTarget.value })} />
              </label>
            </div>
            <div class="row gap-2">
              {["text", "image", "audio", "video"].map((mod) => (
                <label key={mod} class="check-row">
                  <input
                    type="checkbox"
                    checked={modelForm.input_modalities.includes(mod)}
                    onChange={(e) => {
                      const next = e.currentTarget.checked
                        ? [...modelForm.input_modalities, mod]
                        : modelForm.input_modalities.filter((x) => x !== mod);
                      setModelForm({ ...modelForm, input_modalities: next });
                    }}
                  />
                  {mod}
                </label>
              ))}
            </div>
            <label class="field">
              <span>{t("Pricing JSON (optional)")}</span>
              <textarea rows={2} value={modelForm.pricingText} onInput={(e) => setModelForm({ ...modelForm, pricingText: e.currentTarget.value })} />
            </label>
            <div class="row gap-1">
              <button type="submit" class="btn-primary btn-xs">{t("Save")}</button>
              <button type="button" class="btn-ghost btn-xs" onClick={() => setModelForm(null)}>{t("Cancel")}</button>
            </div>
          </form>
        )}
        <div class="item-list">
          {visibleModels.length === 0 && (
            <div class="empty-hint small">{t("No models configured yet.")}</div>
          )}
          {visibleModels.map((m) => {
            const prov = providersById.get(m.provider_id);
            return (
              <div key={m.id} class="list-row">
                <div>
                  <div class="row gap-1" style="flex-wrap:wrap">
                    <strong class="mono">{m.model_key}</strong>
                    {prov && <span class="pill">{prov.name}</span>}
                    {m.pricing && <span class="pill pill-succeeded">{t("priced")}</span>}
                    {(m.archived_at || m.provider_archived_at) && <span class="pill">{t("archived")}</span>}
                  </div>
                  <div class="muted small">
                    {m.input_modalities.join(", ")} · ctx {m.context_window_tokens ? formatTokens(m.context_window_tokens) : "—"} · {t("efforts")} [{m.reasoning_efforts.join(", ") || t("None")}]
                  </div>
                </div>
                <div class="row gap-1">
                  <button
                    type="button"
                    class="btn-ghost btn-xs"
                    onClick={() =>
                      setModelForm({
                        id: m.id,
                        provider_id: m.provider_id,
                        model_key: m.model_key,
                        context_window_tokens: m.context_window_tokens || "",
                        input_modalities: m.input_modalities,
                        reasoning_efforts: m.reasoning_efforts.join(", "),
                        pricingText: m.pricing ? JSON.stringify(m.pricing) : "",
                      })
                    }
                  >
                    {t("Edit")}
                  </button>
                  {m.archived_at ? (
                    <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/models/${m.id}/restore`, { method: "POST" }).then(() => undefined))}>
                      {t("Restore")}
                    </button>
                  ) : (
                    <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/models/${m.id}`, { method: "DELETE" }).then(() => undefined))}>
                      {t("Archive")}
                    </button>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </section>
    </div>
  );
}

function McpTab({ refreshNonce }: { refreshNonce: number }) {
  const { t } = useI18n();
  const [servers, setServers] = useState<McpServerInfo[]>([]);
  const [oauth, setOauth] = useState<Record<string, string>>({});
  const [probe, setProbe] = useState<Record<string, McpProbeResult>>({});
  const [busyTestId, setBusyTestId] = useState<string | null>(null);
  const [confirmDelId, setConfirmDelId] = useState<string | null>(null);
  const [loginInfo, setLoginInfo] = useState<Record<string, McpOAuthLoginInfo | null>>({});
  const [callbackUrl, setCallbackUrl] = useState<Record<string, string>>({});
  const [form, setForm] = useState<McpFormState | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (confirmDelId == null) return;
    const tmr = setTimeout(() => setConfirmDelId(null), 3000);
    return () => clearTimeout(tmr);
  }, [confirmDelId]);

  const load = async () => {
    const list = await apiFetch<McpServerInfo[]>("/mcp/servers");
    setServers(list);
    const entries = await Promise.all(
      list
        .filter((s) => s.has_oauth)
        .map(async (s) => {
          const info = await apiFetch<McpOAuthStatusResponse>(`/mcp/servers/${s.id}/oauth`);
          return [s.id, info.status] as const;
        })
    );
    setOauth(Object.fromEntries(entries));
  };

  useEffect(() => {
    load().catch((e) => setErr(errorMessage(e)));
  }, [refreshNonce]);

  const run = async (fn: () => Promise<void>) => {
    setErr("");
    try {
      await fn();
      await load();
    } catch (e) {
      setErr(errorMessage(e));
    }
  };

  const save = (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!form) return;
    let body: Record<string, unknown>;
    try {
      body = buildMcpPayload(form);
    } catch (ex) {
      setErr(errorMessage(ex));
      return;
    }
    if (form.id && Object.keys(body).length === 0) {
      setForm(null);
      return;
    }
    const targetId = form.id;
    void run(async () => {
      if (targetId) await apiFetch(`/mcp/servers/${targetId}`, { method: "PATCH", body });
      else await apiFetch("/mcp/servers", { method: "POST", body });
      setForm(null);
    });
  };

  const editingSameTransport = Boolean(form?.id && form.transport === form.orig?.transport);

  const addAllowedTool = () => {
    if (!form) return;
    const name = form.newAllowedTool.trim();
    if (!name) return;
    setForm({
      ...form,
      allowedTools: form.allowedTools.includes(name) ? form.allowedTools : [...form.allowedTools, name],
      newAllowedTool: "",
    });
  };

  const removeAllowedTool = (name: string) => {
    if (!form) return;
    setForm({
      ...form,
      allowedTools: form.allowedTools.filter((x) => x !== name),
    });
  };

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      <div class="panel-head">
        <h4>{t("MCP Servers")}</h4>
        <button type="button" class="btn-primary btn-xs" onClick={() => setForm(createMcpForm())}>
          {t("+ Server")}
        </button>
      </div>

      {form && (
        <form class="form-box" onSubmit={save}>
          <div class="grid-2">
            <label class="field">
              <span>{t("Name")} {form.id ? t("(immutable)") : ""}</span>
              <input required disabled={Boolean(form.id)} value={form.name} onInput={(e) => setForm({ ...form, name: e.currentTarget.value })} />
            </label>
            <label class="field">
              <span>{t("Transport")}</span>
              <select
                value={form.transport}
                onChange={(e) => {
                  const next = e.currentTarget.value as McpTransport;
                  setForm({
                    ...form,
                    transport: next,
                    argsAction: form.id && next === form.orig?.transport ? "keep" : "replace",
                  });
                }}
              >
                <option value="streamable-http">streamable-http</option>
                <option value="stdio">stdio</option>
              </select>
            </label>
            <label class="field">
              <span>{t("Description")}</span>
              <input value={form.description} onInput={(e) => setForm({ ...form, description: e.currentTarget.value })} />
            </label>
            <label class="field">
              <span>{t("Timeout (ms)")}</span>
              <input type="number" min="1" value={form.timeout_ms} onInput={(e) => setForm({ ...form, timeout_ms: e.currentTarget.value })} />
            </label>
          </div>

          {form.transport === "streamable-http" ? (
            <>
              {editingSameTransport && form.orig?.has_url_query && !form.replaceUrl ? (
                <div class="field">
                  <span>{t("URL (query parameters hidden)")}</span>
                  <div class="row gap-2">
                    <span class="mono small truncate">{form.orig.url}?…</span>
                    <label class="check-row">
                      <input type="checkbox" checked={form.replaceUrl} onChange={(e) => setForm({ ...form, replaceUrl: e.currentTarget.checked })} />
                      {t("Replace URL")}
                    </label>
                  </div>
                </div>
              ) : (
                <label class="field">
                  <span>{t("URL")}</span>
                  <div class="row gap-2">
                    <input required value={form.url} onInput={(e) => setForm({ ...form, url: e.currentTarget.value })} style="flex:1" />
                    {editingSameTransport && form.orig?.has_url_query && (
                      <label class="check-row">
                        <input
                          type="checkbox"
                          checked={form.replaceUrl}
                          onChange={(e) =>
                            setForm({
                              ...form,
                              replaceUrl: e.currentTarget.checked,
                              url: form.orig?.url || "",
                            })
                          }
                        />
                        {t("Replace URL")}
                      </label>
                    )}
                  </div>
                </label>
              )}

              {form.id ? (
                <div class="grid-2">
                  <label class="field">
                    <span>{t("Headers")}</span>
                    <select value={form.headersAction} onChange={(e) => setForm({ ...form, headersAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">{form.orig?.has_headers ? t("Keep existing headers") : t("None (unchanged)")}</option>
                      <option value="replace">{t("Replace headers (JSON)")}</option>
                      <option value="clear">{t("Clear headers")}</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>{t("OAuth 2.1")}</span>
                    <select value={form.oauthAction} onChange={(e) => setForm({ ...form, oauthAction: e.currentTarget.value as McpOAuthAction })}>
                      <option value="keep">{form.orig?.has_oauth ? t("Keep existing OAuth config") : t("Disabled (unchanged)")}</option>
                      <option value="replace">{t("Replace / enable OAuth config")}</option>
                      {form.orig?.has_oauth && <option value="disable">{t("Disable OAuth")}</option>}
                    </select>
                  </label>
                </div>
              ) : (
                <label class="check-row">
                  <input
                    type="checkbox"
                    checked={form.oauthAction === "replace"}
                    onChange={(e) => setForm({ ...form, oauthAction: e.currentTarget.checked ? "replace" : "keep" })}
                  />
                  {t("Enable OAuth 2.1")}
                </label>
              )}

              {(!form.id || form.headersAction === "replace") && (
                <label class="field">
                  <span>{t("Headers JSON")} {form.id ? "" : `(${t("optional")})`}</span>
                  <textarea rows={2} placeholder='{"Authorization":"Bearer ..."}' value={form.headersJson} onInput={(e) => setForm({ ...form, headersJson: e.currentTarget.value })} />
                </label>
              )}
              {form.oauthAction === "replace" && (
                <label class="field">
                  <span>{t("OAuth Config JSON (optional)")}</span>
                  <textarea rows={2} placeholder="{}" value={form.oauthJson} onInput={(e) => setForm({ ...form, oauthJson: e.currentTarget.value })} />
                </label>
              )}
            </>
          ) : (
            <>
              <div class="grid-2">
                <label class="field">
                  <span>{t("Command")}</span>
                  <input required value={form.command} onInput={(e) => setForm({ ...form, command: e.currentTarget.value })} />
                </label>
                <label class="field">
                  <span>{t("Working Dir (optional)")}</span>
                  <input value={form.cwd} onInput={(e) => setForm({ ...form, cwd: e.currentTarget.value })} />
                </label>
              </div>
              {form.id ? (
                <div class="grid-2">
                  <label class="field">
                    <span>{t("Args")}</span>
                    <select value={form.argsAction} onChange={(e) => setForm({ ...form, argsAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">{t("Keep existing")} ({form.orig?.arg_count || 0})</option>
                      <option value="replace">{t("Replace args")}</option>
                      <option value="clear">{t("Clear args")}</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>{t("Env")}</span>
                    <select value={form.envAction} onChange={(e) => setForm({ ...form, envAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">{form.orig?.has_env ? t("Keep existing env") : t("None (unchanged)")}</option>
                      <option value="replace">{t("Replace env (JSON)")}</option>
                      <option value="clear">{t("Clear env")}</option>
                    </select>
                  </label>
                </div>
              ) : null}
              {(!form.id || form.argsAction === "replace") && (
                <label class="field">
                  <span>{t("Args (one per line)")}</span>
                  <textarea rows={2} value={form.argsText} onInput={(e) => setForm({ ...form, argsText: e.currentTarget.value })} />
                </label>
              )}
              {(!form.id || form.envAction === "replace") && (
                <label class="field">
                  <span>{t("Env JSON")} {form.id ? "" : `(${t("optional")})`}</span>
                  <textarea rows={2} value={form.envJson} onInput={(e) => setForm({ ...form, envJson: e.currentTarget.value })} />
                </label>
              )}
            </>
          )}

          <label class="field">
            <span>{t("Tool Policy")}</span>
            <select value={form.toolPolicy} onChange={(e) => setForm({ ...form, toolPolicy: e.currentTarget.value as McpToolPolicy })}>
              <option value="all">{t("Allow all tools")}</option>
              <option value="none">{t("Deny all tools")}</option>
              <option value="allowlist">{t("Allow listed tools only")}</option>
            </select>
          </label>
          {form.toolPolicy === "allowlist" && (
            <div class="field">
              <span>{t("Allowed Tools (exact names)")}</span>
              {form.allowedTools.length > 0 && (
                <div class="attach-strip">
                  {form.allowedTools.map((name) => (
                    <div key={name} class="attach-chip tool-chip">
                      <span class="mono">{name}</span>
                      <button type="button" onClick={() => removeAllowedTool(name)}>×</button>
                    </div>
                  ))}
                </div>
              )}
              <div class="row gap-1">
                <input
                  placeholder={t("Exact tool name…")}
                  value={form.newAllowedTool}
                  onInput={(e) => setForm({ ...form, newAllowedTool: e.currentTarget.value })}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" && !e.isComposing) {
                      e.preventDefault();
                      addAllowedTool();
                    }
                  }}
                  style="flex:1"
                />
                <button type="button" class="btn-ghost btn-xs" onClick={addAllowedTool}>
                  {t("+ Add")}
                </button>
              </div>
            </div>
          )}

          <div class="row gap-2">
            <label class="check-row">
              <input type="checkbox" checked={form.enabled} onChange={(e) => setForm({ ...form, enabled: e.currentTarget.checked })} />
              {t("Enabled")}
            </label>
            <label class="check-row">
              <input type="checkbox" checked={form.shared} onChange={(e) => setForm({ ...form, shared: e.currentTarget.checked })} />
              {t("Sync across cluster")}
            </label>
          </div>
          {form.orig?.shared && !form.shared && (
            <div class="muted small">
              {t("Disabling cluster sync detaches this server into a new local copy and removes the old shared server from other nodes.")}
            </div>
          )}
          <div class="row gap-1">
            <button type="submit" class="btn-primary btn-xs">{t("Save")}</button>
            <button type="button" class="btn-ghost btn-xs" onClick={() => setForm(null)}>{t("Cancel")}</button>
          </div>
        </form>
      )}

      <div class="item-list">
        {servers.length === 0 && !form && (
          <div class="empty-hint small">{t("No MCP servers configured yet.")}</div>
        )}
        {servers.map((s) => {
          const testing = busyTestId === s.id;
          const confirmingDel = confirmDelId === s.id;
          const pr = probe[s.id];
          return (
            <div key={s.id} class="panel">
              <div class="list-row">
                <div>
                  <strong>{s.name}</strong> <span class="pill">{s.transport}</span>{" "}
                  {s.shared && <span class="pill pill-succeeded">{t("shared")}</span>}{" "}
                  {!s.enabled && <span class="pill">{t("disabled")}</span>}
                  {s.allowed_tools !== null && (
                    <span class="pill">
                      {s.allowed_tools.length === 0 ? t("tools: none") : `tools: ${s.allowed_tools.length}`}
                    </span>
                  )}
                  {s.has_oauth && <span class="pill">oauth: {oauth[s.id] || "?"}</span>}
                  <div class="muted small">{s.description || s.url || s.command}</div>
                </div>
                <div class="row gap-1">
                  <button
                    type="button"
                    class="btn-ghost btn-xs"
                    disabled={testing}
                    onClick={() => {
                      setBusyTestId(s.id);
                      void run(async () => {
                        try {
                          const res = await apiFetch<McpProbeResult>(`/mcp/servers/${s.id}/test`, { method: "POST" });
                          setProbe((p) => ({ ...p, [s.id]: res }));
                        } finally {
                          setBusyTestId(null);
                        }
                      });
                    }}
                  >
                    {testing ? t("Testing…") : t("Test")}
                  </button>
                  {s.has_oauth && (
                    <>
                      <button
                        type="button"
                        class="btn-ghost btn-xs"
                        onClick={() =>
                          void run(async () => {
                            const res = await apiFetch<McpOAuthLoginInfo>(`/mcp/servers/${s.id}/oauth/login`, {
                              method: "POST",
                            });
                            setLoginInfo((l) => ({ ...l, [s.id]: res }));
                          })
                        }
                      >
                        {t("Login")}
                      </button>
                      <button
                        type="button"
                        class="btn-ghost btn-xs"
                        onClick={() =>
                          void run(() => apiFetch(`/mcp/servers/${s.id}/oauth`, { method: "DELETE" }).then(() => undefined))
                        }
                      >
                        {t("Logout")}
                      </button>
                    </>
                  )}
                  <button type="button" class="btn-ghost btn-xs" onClick={() => setForm(createMcpForm(s))}>
                    {t("Edit")}
                  </button>
                  <button
                    type="button"
                    class="btn-danger btn-xs"
                    onClick={() => {
                      if (confirmingDel) {
                        setConfirmDelId(null);
                        void run(() => apiFetch(`/mcp/servers/${s.id}`, { method: "DELETE" }).then(() => undefined));
                      } else {
                        setConfirmDelId(s.id);
                      }
                    }}
                  >
                    {confirmingDel ? t("Confirm?") : t("Delete")}
                  </button>
                </div>
              </div>

              {pr && (
                <div class={`probe-box ${pr.ok ? "probe-ok" : "probe-err"}`}>
                  {pr.ok ? (
                    <div class="row gap-1" style="flex-wrap:wrap">
                      <span class="pill pill-succeeded">OK</span>
                      <span class="mono small">{pr.elapsed_ms}ms</span>
                      <span class="mono small">v={pr.protocol_version || "?"}</span>
                      {pr.lists.map((l) => (
                        <span key={l.operation} class="pill mono">
                          {l.operation}: {l.count}{l.has_more ? "+" : ""}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <div class="small">
                      <strong>{t("FAILED at")}</strong> <span class="mono">{pr.stage}</span>: {pr.error}
                    </div>
                  )}
                </div>
              )}

              {loginInfo[s.id] && (
                <div class="form-box">
                  <div class="small">
                    {t("Authorize here:")}{" "}
                    <a href={loginInfo[s.id]?.authorization_url} target="_blank" rel="noopener noreferrer">
                      {t("Open OAuth Login")}
                    </a>
                  </div>
                  <div class="row gap-1">
                    <input
                      placeholder={t("Paste redirect URL if remote…")}
                      value={callbackUrl[s.id] || ""}
                      onInput={(e) => setCallbackUrl((c) => ({ ...c, [s.id]: e.currentTarget.value }))}
                    />
                    <button
                      type="button"
                      class="btn-primary btn-xs"
                      onClick={() =>
                        void run(async () => {
                          await apiFetch(`/mcp/servers/${s.id}/oauth/callback`, {
                            method: "POST",
                            body: { url: callbackUrl[s.id] || "" },
                          });
                          setLoginInfo((l) => ({ ...l, [s.id]: null }));
                        })
                      }
                    >
                      {t("Submit Callback")}
                    </button>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function UsageTab({
  models,
  sessions,
  initialSessionId,
}: {
  models: ModelInfo[];
  sessions: SessionInfo[];
  initialSessionId?: string;
}) {
  const { locale, t } = useI18n();
  const [summary, setSummary] = useState<CallsSummary | null>(null);
  const [calls, setCalls] = useState<LlmCallInfo[]>([]);
  const [sessionId, setSessionId] = useState(initialSessionId || "");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const modelsById = useMemo(() => new Map(models.map((m) => [m.id, m])), [models]);
  const sessionsById = useMemo(() => new Map(sessions.map((s) => [s.id, s])), [sessions]);

  const load = async (after = "", append = false, sidOverride?: string) => {
    setErr("");
    setLoading(true);
    const sid = (sidOverride !== undefined ? sidOverride : sessionId).trim();
    const sumQ = sid ? `?session_id=${encodeURIComponent(sid)}` : "";
    const params = new URLSearchParams({ limit: "50" });
    if (sid) params.set("session_id", sid);
    if (after) params.set("after", after);
    try {
      const [sum, list] = await Promise.all([
        apiFetch<CallsSummary>(`/calls/summary${sumQ}`),
        apiFetch<LlmCallInfo[]>(`/calls?${params.toString()}`),
      ]);
      setSummary(sum);
      setCalls((prev) => (append ? [...prev, ...list] : list));
    } catch (e) {
      setErr(errorMessage(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void load("", false, initialSessionId || "");
  }, []);

  const totalInput = totalInputTokens(summary);
  const cacheRate = totalInput > 0 && summary ? Math.round((summary.cache_read_tokens / totalInput) * 100) : 0;

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      <div class="row gap-1" style="flex-wrap:wrap">
        <select
          value={sessionsById.has(sessionId) ? sessionId : ""}
          onChange={(e) => {
            const val = e.currentTarget.value;
            setSessionId(val);
            void load("", false, val);
          }}
        >
          <option value="">{t("All sessions")}</option>
          {sessions.map((s) => (
            <option key={s.id} value={s.id}>
              {s.id} · {s.title || t("Untitled")}
            </option>
          ))}
        </select>
        <input
          placeholder={t("Session ID…")}
          style="width:270px"
          value={sessionId}
          onInput={(e) => setSessionId(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void load("", false);
          }}
        />
        <button type="button" class="btn-ghost btn-sm" disabled={loading} onClick={() => void load("", false)}>
          {loading ? t("Loading…") : t("Refresh")}
        </button>
      </div>

      {summary && (
        <div class="kpi-grid">
          <div class="kpi">
            <span>{t("Total Cost")}</span>
            <strong>{formatCost(summary.cost_nano_usd)}</strong>
          </div>
          <div class="kpi">
            <span>{t("Calls")}</span>
            <strong>
              {summary.succeeded_count} / {summary.call_count}
              {summary.failed_count > 0 && <small class="err-text"> ({summary.failed_count} err)</small>}
            </strong>
          </div>
          <div class="kpi">
            <span>{t("Input")} ({cacheRate}% {t("cached")})</span>
            <strong>{formatTokens(totalInput)}</strong>
          </div>
          <div class="kpi">
            <span>{t("Output")}</span>
            <strong>{formatTokens(summary.output_tokens)}</strong>
          </div>
        </div>
      )}

      <div class="item-list">
        {calls.length === 0 && !loading && (
          <div class="empty-hint small">{t("No calls recorded for this filter.")}</div>
        )}
        {calls.map((c) => {
          const model = modelsById.get(c.model_id);
          const sess = c.session_id != null ? sessionsById.get(c.session_id) : undefined;
          const durMs = c.finished_at != null ? Math.max(0, c.finished_at - c.started_at) : null;
          const expanded = expandedId === c.id;
          return (
            <div key={c.id} class="call-row">
              <div
                class="list-row small call-head"
                onClick={() => setExpandedId(expanded ? null : c.id)}
              >
                <div>
                  <span class="mono">{c.id}</span>{" "}
                  <span class={`pill pill-${c.status}`}>{statusText(c.status, locale)}</span>{" "}
                  <span class="mono">{model ? model.model_key : c.model_id}</span>
                  {c.reasoning_effort && <span class="pill">{c.reasoning_effort}</span>}{" "}
                  <span class="muted">
                    {c.session_id != null ? (sess?.title || c.session_id) : t("deleted")}
                    {durMs != null ? ` · ${formatDuration(durMs)}` : ""}
                    {` · ${formatRelTimeLoc(c.started_at, locale)}`}
                  </span>
                  {c.error_summary && <div class="err-text">{c.error_summary}</div>}
                </div>
                <div class="mono">
                  {formatTokens(totalInputTokens(c))}→{formatTokens(c.output_tokens)} · {formatCost(c.cost_nano_usd)}
                </div>
              </div>
              {expanded && (
                <div class="call-detail small">
                  <div>
                    <span class="muted">{t("Started")}:</span> <span class="mono">{formatAbsTime(c.started_at)}</span>
                    {c.http_status != null && (
                      <> · <span class="muted">HTTP:</span> <span class="mono">{c.http_status}</span></>
                    )}
                    {c.upstream_response_id && (
                      <> · <span class="muted">Upstream ID:</span> <span class="mono">{c.upstream_response_id}</span></>
                    )}
                  </div>
                  <div class="mono muted">
                    uncached {c.input_uncached_tokens ?? 0} · cache_read {c.cache_read_tokens ?? 0} · cache_write {c.cache_write_tokens ?? 0} · out {c.output_tokens ?? 0}
                  </div>
                </div>
              )}
            </div>
          );
        })}
        {calls.length > 0 && calls.length % 50 === 0 && (
          <button type="button" class="btn-ghost btn-sm" onClick={() => void load(calls[calls.length - 1].id, true)}>
            {t("Load more")}
          </button>
        )}
      </div>
    </div>
  );
}

function ClusterTab({ onClusterChanged }: { onClusterChanged: () => void }) {
  const { locale, t } = useI18n();
  const [status, setStatus] = useState<ClusterStatus | null>(null);
  const [conflicts, setConflicts] = useState<SyncConflictInfo[]>([]);
  const [selfName, setSelfName] = useState("");
  const [selfDesc, setSelfDesc] = useState("");
  const [inviteCode, setInviteCode] = useState("");
  const [joinInvite, setJoinInvite] = useState("");
  const [busySync, setBusySync] = useState(false);
  const [busyJoin, setBusyJoin] = useState(false);
  const [resolvingId, setResolvingId] = useState<string | null>(null);
  const [confirmPeerId, setConfirmPeerId] = useState<string | null>(null);
  const [notice, setNotice] = useState("");
  const [err, setErr] = useState("");

  useEffect(() => {
    if (confirmPeerId == null) return;
    const tmr = setTimeout(() => setConfirmPeerId(null), 3000);
    return () => clearTimeout(tmr);
  }, [confirmPeerId]);

  const load = async () => {
    const [st, confs] = await Promise.all([
      apiFetch<ClusterStatus>("/cluster"),
      apiFetch<SyncConflictInfo[]>("/cluster/conflicts"),
    ]);
    setStatus(st);
    setSelfName(st.self.name);
    setSelfDesc(st.self.description);
    setConflicts(confs);
  };

  useEffect(() => {
    load().catch((e) => setErr(errorMessage(e)));
  }, []);

  const saveSelf = async (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    setErr("");
    setNotice("");
    try {
      await apiFetch("/cluster/self", {
        method: "PATCH",
        body: { name: selfName, description: selfDesc },
      });
      await load();
      onClusterChanged();
    } catch (ex) {
      setErr(errorMessage(ex));
    }
  };

  const triggerSync = async () => {
    setErr("");
    setNotice("");
    setBusySync(true);
    try {
      const res = await apiFetch<ClusterSyncSummary>("/cluster/sync", { method: "POST", body: {} });
      setNotice(
        `${t("Synced:")} ${res.peers_queried} ${t("peers queried")}, ${res.streams_updated} ${t("streams updated")}, ${res.conflicts} ${t("conflicts")}`
      );
      await load();
      onClusterChanged();
    } catch (ex) {
      setErr(errorMessage(ex));
    } finally {
      setBusySync(false);
    }
  };

  const generateInvite = async () => {
    setErr("");
    try {
      const res = await apiFetch<{ invite: string }>("/cluster/invite", { method: "POST" });
      setInviteCode(res.invite);
    } catch (ex) {
      setErr(errorMessage(ex));
    }
  };

  const submitJoin = async (e: TargetedSubmitEvent<HTMLFormElement>) => {
    e.preventDefault();
    const inv = joinInvite.trim();
    if (!inv) return;
    setErr("");
    setNotice("");
    setBusyJoin(true);
    try {
      const res = await apiFetch<{ joined: boolean; peer: { name: string; node_id: string } }>("/cluster/join", {
        method: "POST",
        body: { invite: inv },
      });
      setJoinInvite("");
      setNotice(`${t("Joined cluster via")} ${res.peer.name || res.peer.node_id}`);
      await load();
      onClusterChanged();
    } catch (ex) {
      setErr(errorMessage(ex));
    } finally {
      setBusyJoin(false);
    }
  };

  const resolveConflict = async (streamId: string, resolution: "local" | "cluster") => {
    setErr("");
    setNotice("");
    setResolvingId(streamId);
    try {
      await apiFetch(`/cluster/conflicts/${encodeURIComponent(streamId)}/resolve`, {
        method: "POST",
        body: { resolution },
      });
      await load();
      onClusterChanged();
    } catch (ex) {
      setErr(errorMessage(ex));
    } finally {
      setResolvingId(null);
    }
  };

  const removePeer = async (nodeId: string) => {
    setErr("");
    try {
      await apiFetch(`/cluster/peers/${encodeURIComponent(nodeId)}`, { method: "DELETE" });
      await load();
      onClusterChanged();
    } catch (ex) {
      setErr(errorMessage(ex));
    }
  };

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      {notice && <div class="probe-box probe-ok small">{notice}</div>}

      <section class="panel">
        <div class="panel-head">
          <div class="row gap-1">
            <h4>{t("This Node")}</h4>
            {status?.self.local_only && <span class="pill">{t("Local-only")}</span>}
          </div>
          <div class="row gap-1">
            <button type="button" class="btn-ghost btn-xs" disabled={busySync} onClick={triggerSync}>
              <Icon n="refresh" s={12} />
              <span>{busySync ? t("Syncing…") : t("Sync Now")}</span>
            </button>
          </div>
        </div>
        {status && (
          <>
            <div class="row gap-2 small">
              <span class="muted">{t("Node ID")}:</span>
              <code class="md-inline-code mono truncate" style="max-width:440px">{status.self.node_id}</code>
              <CopyBtn text={status.self.node_id} />
              <span class="pill mono">{status.self.os}/{status.self.arch}</span>
            </div>
            <form class="form-box" onSubmit={saveSelf}>
              <div class="grid-2">
                <label class="field">
                  <span>{t("Name")}</span>
                  <input value={selfName} onInput={(e) => setSelfName(e.currentTarget.value)} />
                </label>
                <label class="field">
                  <span>{t("Description")}</span>
                  <input value={selfDesc} onInput={(e) => setSelfDesc(e.currentTarget.value)} />
                </label>
              </div>
              <div class="row gap-1">
                <button type="submit" class="btn-primary btn-xs">{t("Save")}</button>
              </div>
            </form>
          </>
        )}
      </section>

      <section class="panel">
        <div class="panel-head">
          <h4>{t("Peers & Join")}</h4>
          <button type="button" class="btn-ghost btn-xs" onClick={generateInvite}>
            {t("Generate Invite")}
          </button>
        </div>
        {inviteCode && (
          <div class="form-box">
            <div class="row gap-1" style="justify-content:space-between">
              <span class="small muted">{t("Invite code")}</span>
              <CopyBtn text={inviteCode} label={t("Copy")} />
            </div>
            <textarea rows={2} readOnly class="mono small" value={inviteCode} />
          </div>
        )}
        <form class="row gap-1" onSubmit={submitJoin}>
          <input
            style="flex:1"
            placeholder={t("Paste invite code…")}
            value={joinInvite}
            onInput={(e) => setJoinInvite(e.currentTarget.value)}
          />
          <button type="submit" class="btn-primary btn-xs" disabled={busyJoin || !joinInvite.trim()}>
            {busyJoin ? t("Joining…") : t("Join")}
          </button>
        </form>
        <div class="item-list">
          {(!status || status.peers.length === 0) && (
            <div class="empty-hint small">
              {t("No peers registered yet. Generate an invite or join an existing cluster.")}
            </div>
          )}
          {(status?.peers || []).map((p) => {
            const confirming = confirmPeerId === p.node_id;
            return (
              <div key={p.node_id} class="list-row">
                <div style="min-width:0">
                  <div class="row gap-1" style="flex-wrap:wrap">
                    <strong>{p.name || p.node_id.slice(0, 12)}</strong>
                    <span class={`pill ${p.online ? "pill-succeeded" : ""}`}>
                      {p.online ? t("online") : t("offline")}
                    </span>
                    <span class="pill mono">{p.os}/{p.arch}</span>
                  </div>
                  <div class="row gap-1 muted small">
                    <span class="mono">{p.node_id.slice(0, 16)}…</span>
                    <CopyBtn text={p.node_id} />
                    {p.description && <span>· {p.description}</span>}
                    {p.last_seen_at > 0 && <span>· {formatRelTimeLoc(p.last_seen_at, locale)}</span>}
                  </div>
                </div>
                <button
                  type="button"
                  class="btn-danger btn-xs"
                  onClick={() => {
                    if (confirming) {
                      setConfirmPeerId(null);
                      void removePeer(p.node_id);
                    } else {
                      setConfirmPeerId(p.node_id);
                    }
                  }}
                >
                  {confirming ? t("Confirm?") : t("Remove")}
                </button>
              </div>
            );
          })}
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <div class="row gap-1">
            <h4>{t("Sync Conflicts")}</h4>
            {conflicts.length > 0 && <span class="pill pill-failed">{conflicts.length}</span>}
          </div>
        </div>
        <div class="item-list">
          {conflicts.length === 0 && (
            <div class="empty-hint small">{t("No sync conflicts.")}</div>
          )}
          {conflicts.map((c) => {
            const busy = resolvingId === c.stream_id;
            return (
              <div key={c.stream_id} class="form-box">
                <div class="row gap-1" style="justify-content:space-between;flex-wrap:wrap">
                  <div class="row gap-1" style="flex-wrap:wrap">
                    <span class="pill">{c.kind}</span>
                    <strong class="mono small">{c.stream_id}</strong>
                    <span class="pill pill-failed">{c.reason}</span>
                  </div>
                  <span class="muted small">{formatRelTimeLoc(c.detected_at, locale)}</span>
                </div>
                <div class="small mono muted">
                  {t("Local")}: epoch={c.local_epoch} seq={c.local_seq} ({c.local_hash_hex.slice(0, 10)}) ·{" "}
                  {t("Cluster")}: epoch={c.remote_epoch} seq={c.remote_seq} ({c.remote_hash_hex.slice(0, 10)}) via{" "}
                  {c.remote_node_id.slice(0, 12)}…
                </div>
                <div class="row gap-1">
                  <button
                    type="button"
                    class="btn-primary btn-xs"
                    disabled={busy}
                    onClick={() => void resolveConflict(c.stream_id, "local")}
                  >
                    {busy ? t("Resolving…") : t("Keep Local (Force Push)")}
                  </button>
                  <button
                    type="button"
                    class="btn-danger btn-xs"
                    disabled={busy}
                    onClick={() => void resolveConflict(c.stream_id, "cluster")}
                  >
                    {busy ? t("Resolving…") : t("Use Cluster (Overwrite)")}
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </section>
    </div>
  );
}

export type SettingsTabId = "models" | "mcp" | "cluster" | "usage" | "general";

export interface SettingsProps {
  providers: ProviderInfo[];
  models: ModelInfo[];
  sessions: SessionInfo[];
  mcpRefreshNonce: number;
  themePref: ThemePref;
  localePref: LocalePref;
  initialTab?: SettingsTabId;
  initialSessionFilter?: string;
  onSetThemePref: (pref: ThemePref) => void;
  onSetLocalePref: (pref: LocalePref) => void;
  onRefreshCatalog: () => Promise<void>;
  onClusterChanged: () => void;
  onSignOut: () => void;
  onClose: () => void;
}

export function Settings({
  providers,
  models,
  sessions,
  mcpRefreshNonce,
  themePref,
  localePref,
  initialTab,
  initialSessionFilter,
  onSetThemePref,
  onSetLocalePref,
  onRefreshCatalog,
  onClusterChanged,
  onSignOut,
  onClose,
}: SettingsProps) {
  const { t } = useI18n();
  const [tab, setTab] = useState<SettingsTabId>(initialTab || "models");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const tabs = [
    { id: "models" as const, label: t("Models"), icon: "sliders" as const },
    { id: "mcp" as const, label: t("MCP"), icon: "plug" as const },
    { id: "cluster" as const, label: t("Cluster"), icon: "globe" as const },
    { id: "usage" as const, label: t("Usage"), icon: "chart" as const },
    { id: "general" as const, label: t("General"), icon: "settings" as const },
  ];

  return (
    <div class="modal-backdrop" onClick={onClose}>
      <div class="modal" onClick={(e) => e.stopPropagation()}>
        <div class="modal-head">
          <div class="tabs">
            {tabs.map((tb) => (
              <button
                key={tb.id}
                type="button"
                class={`tab-btn ${tab === tb.id ? "active" : ""}`}
                onClick={() => setTab(tb.id)}
              >
                <Icon n={tb.icon} s={14} />
                <span>{tb.label}</span>
              </button>
            ))}
          </div>
          <button type="button" class="btn-icon" aria-label={t("Close settings")} onClick={onClose}>
            <Icon n="x" s={16} />
          </button>
        </div>
        <div class="modal-body">
          {tab === "models" && (
            <ModelsTab providers={providers} models={models} onRefresh={onRefreshCatalog} />
          )}
          {tab === "mcp" && <McpTab refreshNonce={mcpRefreshNonce} />}
          {tab === "cluster" && <ClusterTab onClusterChanged={onClusterChanged} />}
          {tab === "usage" && (
            <UsageTab models={models} sessions={sessions} initialSessionId={initialSessionFilter} />
          )}
          {tab === "general" && (
            <div class="tab-stack">
              <div class="panel list-row">
                <div>
                  <strong>{t("Language")}</strong>
                  <div class="muted small">{t("Interface display language")}</div>
                </div>
                <div class="segmented">
                  {(
                    [
                      ["auto", t("Auto"), true],
                      ["zh", "中文", false],
                      ["en", "English", false],
                    ] as const
                  ).map(([val, label, hasIcon]) => (
                    <button
                      key={val}
                      type="button"
                      class={`seg-btn ${localePref === val ? "active" : ""}`}
                      onClick={() => onSetLocalePref(val)}
                    >
                      {hasIcon && <Icon n="globe" s={13} />}
                      <span>{label}</span>
                    </button>
                  ))}
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>{t("Appearance")}</strong>
                  <div class="muted small">{t("Theme preference across sessions")}</div>
                </div>
                <div class="segmented">
                  {(
                    [
                      ["system", t("System"), "monitor"],
                      ["light", t("Light"), "sun"],
                      ["dark", t("Dark"), "moon"],
                    ] as const
                  ).map(([val, label, icon]) => (
                    <button
                      key={val}
                      type="button"
                      class={`seg-btn ${themePref === val ? "active" : ""}`}
                      onClick={() => onSetThemePref(val)}
                    >
                      <Icon n={icon} s={13} />
                      <span>{label}</span>
                    </button>
                  ))}
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>{t("API Documentation")}</strong>
                  <div class="muted small">{t("Scalar interactive reference & OpenAPI schema (loopback only)")}</div>
                </div>
                <div class="row gap-1">
                  <a class="btn-ghost btn-sm row gap-1" href="/docs" target="_blank" rel="noopener noreferrer">
                    <span>/docs</span>
                    <Icon n="external" s={12} />
                  </a>
                  <a class="btn-ghost btn-sm row gap-1" href="/openapi.json" target="_blank" rel="noopener noreferrer">
                    <span>/openapi.json</span>
                    <Icon n="external" s={12} />
                  </a>
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>{t("Authentication")}</strong>
                  <div class="muted small">{t("Clear stored Bearer token from this browser")}</div>
                </div>
                <button type="button" class="btn-danger btn-sm" onClick={onSignOut}>
                  <Icon n="key" s={13} />
                  <span>{t("Sign out")}</span>
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
