import type { TargetedSubmitEvent } from "preact";
import { useEffect, useMemo, useState } from "preact/hooks";
import { apiFetch, errorMessage, formatAbsTime, formatCost, formatDuration, formatRelTime, formatTokens } from "./api.ts";
import {
  IconChart,
  IconExternal,
  IconKey,
  IconMonitor,
  IconMoon,
  IconPlug,
  IconSettings,
  IconSliders,
  IconSun,
  IconX,
} from "./Icons.tsx";
import { buildMcpPayload, createMcpForm } from "./state.ts";
import type {
  AppConfig,
  CallsSummary,
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
  ThemePref,
} from "./types.ts";

interface ProviderFormState {
  id: number | null;
  name: string;
  base_url: string;
  api_key: string;
  clearKey: boolean;
}

interface ModelFormState {
  id: number | null;
  provider_id: number | string;
  model_key: string;
  context_window_tokens: number | string;
  input_modalities: string[];
  reasoning_efforts: string;
  pricingText: string;
}

function ModelsTab({
  config,
  providers,
  models,
  onRefresh,
}: {
  config: AppConfig | null;
  providers: ProviderInfo[];
  models: ModelInfo[];
  onRefresh: () => Promise<void>;
}) {
  const [showArchived, setShowArchived] = useState(false);
  const [err, setErr] = useState("");
  const [provForm, setProvForm] = useState<ProviderFormState | null>(null);
  const [modelForm, setModelForm] = useState<ModelFormState | null>(null);

  const providersById = useMemo(() => new Map(providers.map((p) => [p.id, p])), [providers]);
  const visibleProviders = showArchived ? providers : providers.filter((p) => !p.archived_at);
  const visibleModels = showArchived ? models : models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const availableModels = models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const selectedDefault = models.find((m) => m.id === config?.default_model_id);

  const run = async (fn: () => Promise<void>) => {
    setErr("");
    try {
      await fn();
      await onRefresh();
    } catch (e) {
      setErr(errorMessage(e));
    }
  };

  const saveDefault = (patch: Partial<AppConfig>) =>
    run(() => apiFetch("/config", { method: "PATCH", body: patch }).then(() => undefined));

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
        setErr("Pricing must be valid JSON");
        return;
      }
    }
    const body = {
      provider_id: Number(f.provider_id),
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
          <h4>Default Selection</h4>
          <label class="check-row">
            <input type="checkbox" checked={showArchived} onChange={(e) => setShowArchived(e.currentTarget.checked)} />
            Show archived
          </label>
        </div>
        <div class="grid-2">
          <label class="field">
            <span>Default Model</span>
            <select
              value={config?.default_model_id ?? ""}
              onChange={(e) =>
                void saveDefault({
                  default_model_id: e.currentTarget.value ? Number(e.currentTarget.value) : null,
                })
              }
            >
              <option value="">None</option>
              {availableModels.map((m) => (
                <option key={m.id} value={m.id}>
                  {m.model_key} (#{m.id})
                </option>
              ))}
            </select>
          </label>
          <label class="field">
            <span>Default Effort</span>
            <select
              disabled={!selectedDefault || selectedDefault.reasoning_efforts.length === 0}
              value={config?.default_reasoning_effort ?? ""}
              onChange={(e) =>
                void saveDefault({ default_reasoning_effort: e.currentTarget.value || null })
              }
            >
              <option value="">Default</option>
              {(selectedDefault?.reasoning_efforts || []).map((ef) => (
                <option key={ef} value={ef}>{ef}</option>
              ))}
            </select>
          </label>
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h4>Providers</h4>
          <button
            type="button"
            class="btn-ghost btn-xs"
            onClick={() => setProvForm({ id: null, name: "", base_url: "", api_key: "", clearKey: false })}
          >
            + Provider
          </button>
        </div>
        {provForm && (
          <form class="form-box" onSubmit={saveProvider}>
            <div class="grid-2">
              <label class="field">
                <span>Name</span>
                <input required value={provForm.name} onInput={(e) => setProvForm({ ...provForm, name: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>Base URL</span>
                <input required placeholder="https://api.openai.com/v1" value={provForm.base_url} onInput={(e) => setProvForm({ ...provForm, base_url: e.currentTarget.value })} />
              </label>
            </div>
            <label class="field">
              <span>API Key {provForm.id ? "(leave blank to keep)" : ""}</span>
              <input type="password" value={provForm.api_key} onInput={(e) => setProvForm({ ...provForm, api_key: e.currentTarget.value })} />
            </label>
            {provForm.id && (
              <label class="check-row">
                <input type="checkbox" checked={provForm.clearKey} onChange={(e) => setProvForm({ ...provForm, clearKey: e.currentTarget.checked })} />
                Clear stored key
              </label>
            )}
            <div class="row gap-1">
              <button type="submit" class="btn-primary btn-xs">Save</button>
              <button type="button" class="btn-ghost btn-xs" onClick={() => setProvForm(null)}>Cancel</button>
            </div>
          </form>
        )}
        <div class="item-list">
          {visibleProviders.length === 0 && (
            <div class="empty-hint small">No providers configured yet.</div>
          )}
          {visibleProviders.map((p) => (
            <div key={p.id} class="list-row">
              <div>
                <strong>{p.name}</strong>{" "}
                {p.archived_at && <span class="pill">archived</span>}
                <div class="muted small">{p.base_url} · {p.has_api_key ? "key set" : "no key"}</div>
              </div>
              <div class="row gap-1">
                <button type="button" class="btn-ghost btn-xs" onClick={() => setProvForm({ id: p.id, name: p.name, base_url: p.base_url, api_key: "", clearKey: false })}>
                  Edit
                </button>
                {p.archived_at ? (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/providers/${p.id}/restore`, { method: "POST" }).then(() => undefined))}>
                    Restore
                  </button>
                ) : (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/providers/${p.id}`, { method: "DELETE" }).then(() => undefined))}>
                    Archive
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </section>

      <section class="panel">
        <div class="panel-head">
          <h4>Models</h4>
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
            + Model
          </button>
        </div>
        {modelForm && (
          <form class="form-box" onSubmit={saveModel}>
            <div class="grid-2">
              <label class="field">
                <span>Provider</span>
                <select value={modelForm.provider_id} onChange={(e) => setModelForm({ ...modelForm, provider_id: e.currentTarget.value })}>
                  {visibleProviders.filter((p) => !p.archived_at).map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>
              <label class="field">
                <span>Model Key</span>
                <input required value={modelForm.model_key} onInput={(e) => setModelForm({ ...modelForm, model_key: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>Context Window</span>
                <input type="number" min="1" placeholder="optional" value={modelForm.context_window_tokens} onInput={(e) => setModelForm({ ...modelForm, context_window_tokens: e.currentTarget.value })} />
              </label>
              <label class="field">
                <span>Efforts (comma-separated)</span>
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
              <span>Pricing JSON (optional)</span>
              <textarea rows={2} value={modelForm.pricingText} onInput={(e) => setModelForm({ ...modelForm, pricingText: e.currentTarget.value })} />
            </label>
            <div class="row gap-1">
              <button type="submit" class="btn-primary btn-xs">Save</button>
              <button type="button" class="btn-ghost btn-xs" onClick={() => setModelForm(null)}>Cancel</button>
            </div>
          </form>
        )}
        <div class="item-list">
          {visibleModels.length === 0 && (
            <div class="empty-hint small">No models configured yet.</div>
          )}
          {visibleModels.map((m) => {
            const prov = providersById.get(m.provider_id);
            return (
              <div key={m.id} class="list-row">
                <div>
                  <div class="row gap-1" style="flex-wrap:wrap">
                    <strong class="mono">{m.model_key}</strong>
                    {prov && <span class="pill">{prov.name}</span>}
                    {m.pricing && <span class="pill pill-succeeded">priced</span>}
                    {(m.archived_at || m.provider_archived_at) && <span class="pill">archived</span>}
                  </div>
                  <div class="muted small">
                    {m.input_modalities.join(", ")} · ctx {m.context_window_tokens ? formatTokens(m.context_window_tokens) : "—"} · efforts [{m.reasoning_efforts.join(", ") || "none"}]
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
                    Edit
                  </button>
                  {m.archived_at ? (
                    <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/models/${m.id}/restore`, { method: "POST" }).then(() => undefined))}>
                      Restore
                    </button>
                  ) : (
                    <button type="button" class="btn-ghost btn-xs" onClick={() => void run(() => apiFetch(`/models/${m.id}`, { method: "DELETE" }).then(() => undefined))}>
                      Archive
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

function McpTab() {
  const [servers, setServers] = useState<McpServerInfo[]>([]);
  const [oauth, setOauth] = useState<Record<number, string>>({});
  const [probe, setProbe] = useState<Record<number, McpProbeResult>>({});
  const [busyTestId, setBusyTestId] = useState<number | null>(null);
  const [confirmDelId, setConfirmDelId] = useState<number | null>(null);
  const [loginInfo, setLoginInfo] = useState<Record<number, McpOAuthLoginInfo | null>>({});
  const [callbackUrl, setCallbackUrl] = useState<Record<number, string>>({});
  const [form, setForm] = useState<McpFormState | null>(null);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (confirmDelId == null) return;
    const t = setTimeout(() => setConfirmDelId(null), 3000);
    return () => clearTimeout(t);
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
  }, []);

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
        <h4>MCP Servers</h4>
        <button
          type="button"
          class="btn-primary btn-xs"
          onClick={() => setForm(createMcpForm())}
        >
          + Server
        </button>
      </div>

      {form && (
        <form class="form-box" onSubmit={save}>
          <div class="grid-2">
            <label class="field">
              <span>Name {form.id ? "(immutable)" : ""}</span>
              <input required disabled={Boolean(form.id)} value={form.name} onInput={(e) => setForm({ ...form, name: e.currentTarget.value })} />
            </label>
            <label class="field">
              <span>Transport</span>
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
              <span>Description</span>
              <input value={form.description} onInput={(e) => setForm({ ...form, description: e.currentTarget.value })} />
            </label>
            <label class="field">
              <span>Timeout (ms)</span>
              <input type="number" min="1" value={form.timeout_ms} onInput={(e) => setForm({ ...form, timeout_ms: e.currentTarget.value })} />
            </label>
          </div>

          {form.transport === "streamable-http" ? (
            <>
              {editingSameTransport && form.orig?.has_url_query && !form.replaceUrl ? (
                <div class="field">
                  <span>URL (query parameters hidden)</span>
                  <div class="row gap-2">
                    <span class="mono small truncate">{form.orig.url}?…</span>
                    <label class="check-row">
                      <input type="checkbox" checked={form.replaceUrl} onChange={(e) => setForm({ ...form, replaceUrl: e.currentTarget.checked })} />
                      Replace URL
                    </label>
                  </div>
                </div>
              ) : (
                <label class="field">
                  <span>URL</span>
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
                        Replace URL
                      </label>
                    )}
                  </div>
                </label>
              )}

              {form.id ? (
                <div class="grid-2">
                  <label class="field">
                    <span>Headers</span>
                    <select value={form.headersAction} onChange={(e) => setForm({ ...form, headersAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">{form.orig?.has_headers ? "Keep existing headers" : "None (unchanged)"}</option>
                      <option value="replace">Replace headers (JSON)</option>
                      <option value="clear">Clear headers</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>OAuth 2.1</span>
                    <select value={form.oauthAction} onChange={(e) => setForm({ ...form, oauthAction: e.currentTarget.value as McpOAuthAction })}>
                      <option value="keep">{form.orig?.has_oauth ? "Keep existing OAuth config" : "Disabled (unchanged)"}</option>
                      <option value="replace">{form.orig?.has_oauth ? "Replace OAuth config" : "Enable / set OAuth config"}</option>
                      {form.orig?.has_oauth && <option value="disable">Disable OAuth</option>}
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
                  Enable OAuth 2.1
                </label>
              )}

              {(!form.id || form.headersAction === "replace") && (
                <label class="field">
                  <span>Headers JSON {form.id ? "" : "(optional)"}</span>
                  <textarea rows={2} placeholder='{"Authorization":"Bearer ..."}' value={form.headersJson} onInput={(e) => setForm({ ...form, headersJson: e.currentTarget.value })} />
                </label>
              )}
              {form.oauthAction === "replace" && (
                <label class="field">
                  <span>OAuth Config JSON (optional)</span>
                  <textarea rows={2} placeholder="{}" value={form.oauthJson} onInput={(e) => setForm({ ...form, oauthJson: e.currentTarget.value })} />
                </label>
              )}
            </>
          ) : (
            <>
              <div class="grid-2">
                <label class="field">
                  <span>Command</span>
                  <input required value={form.command} onInput={(e) => setForm({ ...form, command: e.currentTarget.value })} />
                </label>
                <label class="field">
                  <span>Working Dir (optional)</span>
                  <input value={form.cwd} onInput={(e) => setForm({ ...form, cwd: e.currentTarget.value })} />
                </label>
              </div>
              {form.id ? (
                <div class="grid-2">
                  <label class="field">
                    <span>Args</span>
                    <select value={form.argsAction} onChange={(e) => setForm({ ...form, argsAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">Keep existing ({form.orig?.arg_count || 0} args)</option>
                      <option value="replace">Replace args</option>
                      <option value="clear">Clear args</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>Env</span>
                    <select value={form.envAction} onChange={(e) => setForm({ ...form, envAction: e.currentTarget.value as McpSecretAction })}>
                      <option value="keep">{form.orig?.has_env ? "Keep existing env" : "None (unchanged)"}</option>
                      <option value="replace">Replace env (JSON)</option>
                      <option value="clear">Clear env</option>
                    </select>
                  </label>
                </div>
              ) : null}
              {(!form.id || form.argsAction === "replace") && (
                <label class="field">
                  <span>Args (one per line)</span>
                  <textarea rows={2} value={form.argsText} onInput={(e) => setForm({ ...form, argsText: e.currentTarget.value })} />
                </label>
              )}
              {(!form.id || form.envAction === "replace") && (
                <label class="field">
                  <span>Env JSON {form.id ? "" : "(optional)"}</span>
                  <textarea rows={2} value={form.envJson} onInput={(e) => setForm({ ...form, envJson: e.currentTarget.value })} />
                </label>
              )}
            </>
          )}

          <label class="field">
            <span>Tool Policy</span>
            <select value={form.toolPolicy} onChange={(e) => setForm({ ...form, toolPolicy: e.currentTarget.value as McpToolPolicy })}>
              <option value="all">Allow all tools</option>
              <option value="none">Deny all tools</option>
              <option value="allowlist">Allow listed tools only</option>
            </select>
          </label>
          {form.toolPolicy === "allowlist" && (
            <div class="field">
              <span>Allowed Tools (exact names)</span>
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
                  placeholder="Exact tool name…"
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
                  + Add
                </button>
              </div>
            </div>
          )}

          <label class="check-row">
            <input type="checkbox" checked={form.enabled} onChange={(e) => setForm({ ...form, enabled: e.currentTarget.checked })} />
            Enabled
          </label>
          <div class="row gap-1">
            <button type="submit" class="btn-primary btn-xs">Save</button>
            <button type="button" class="btn-ghost btn-xs" onClick={() => setForm(null)}>Cancel</button>
          </div>
        </form>
      )}

      <div class="item-list">
        {servers.length === 0 && !form && (
          <div class="empty-hint small">No MCP servers configured yet.</div>
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
                  {!s.enabled && <span class="pill">disabled</span>}
                  {s.allowed_tools !== null && (
                    <span class="pill">
                      {s.allowed_tools.length === 0 ? "tools: none" : `tools: ${s.allowed_tools.length}`}
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
                    {testing ? "Testing…" : "Test"}
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
                        Login
                      </button>
                      <button
                        type="button"
                        class="btn-ghost btn-xs"
                        onClick={() =>
                          void run(() => apiFetch(`/mcp/servers/${s.id}/oauth`, { method: "DELETE" }).then(() => undefined))
                        }
                      >
                        Logout
                      </button>
                    </>
                  )}
                  <button
                    type="button"
                    class="btn-ghost btn-xs"
                    onClick={() => setForm(createMcpForm(s))}
                  >
                    Edit
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
                    {confirmingDel ? "Confirm?" : "Delete"}
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
                      <strong>FAILED</strong> at <span class="mono">{pr.stage}</span>: {pr.error}
                    </div>
                  )}
                </div>
              )}

              {loginInfo[s.id] && (
                <div class="form-box">
                  <div class="small">
                    Authorize here:{" "}
                    <a href={loginInfo[s.id]?.authorization_url} target="_blank" rel="noopener noreferrer">
                      Open OAuth Login
                    </a>
                  </div>
                  <div class="row gap-1">
                    <input
                      placeholder="Paste redirect URL if remote…"
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
                      Submit Callback
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
  const [summary, setSummary] = useState<CallsSummary | null>(null);
  const [calls, setCalls] = useState<LlmCallInfo[]>([]);
  const [sessionId, setSessionId] = useState(initialSessionId || "");
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState("");

  const modelsById = useMemo(() => new Map(models.map((m) => [m.id, m])), [models]);
  const sessionsById = useMemo(() => new Map(sessions.map((s) => [s.id, s])), [sessions]);

  const load = async (after = 0, append = false, sidOverride?: string) => {
    setErr("");
    setLoading(true);
    const sid = (sidOverride !== undefined ? sidOverride : sessionId).trim();
    const q = sid ? `?session_id=${encodeURIComponent(sid)}` : "";
    const sep = q ? "&" : "?";
    try {
      const [sum, list] = await Promise.all([
        apiFetch<CallsSummary>(`/calls/summary${q}`),
        apiFetch<LlmCallInfo[]>(`/calls${q}${sep}after=${after}&limit=50`),
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
    void load(0, false, initialSessionId || "");
  }, []);

  const totalInput = summary ? summary.input_uncached_tokens + summary.cache_read_tokens : 0;
  const cacheRate = totalInput > 0 && summary ? Math.round((summary.cache_read_tokens / totalInput) * 100) : 0;

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      <div class="row gap-1" style="flex-wrap:wrap">
        <select
          value={sessionsById.has(Number(sessionId)) ? sessionId : ""}
          onChange={(e) => {
            const val = e.currentTarget.value;
            setSessionId(val);
            void load(0, false, val);
          }}
        >
          <option value="">All sessions</option>
          {sessions.map((s) => (
            <option key={s.id} value={String(s.id)}>
              #{s.id} · {s.title || "Untitled"}
            </option>
          ))}
        </select>
        <input
          placeholder="Session ID…"
          style="width:110px"
          value={sessionId}
          onInput={(e) => setSessionId(e.currentTarget.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") void load(0, false);
          }}
        />
        <button type="button" class="btn-ghost btn-sm" disabled={loading} onClick={() => void load(0, false)}>
          {loading ? "Loading…" : "Refresh"}
        </button>
      </div>

      {summary && (
        <div class="kpi-grid">
          <div class="kpi">
            <span>Total Cost</span>
            <strong>{formatCost(summary.cost_nano_usd)}</strong>
          </div>
          <div class="kpi">
            <span>Calls</span>
            <strong>
              {summary.succeeded_count} / {summary.call_count}
              {summary.failed_count > 0 && <small class="err-text"> ({summary.failed_count} err)</small>}
            </strong>
          </div>
          <div class="kpi">
            <span>Input ({cacheRate}% cached)</span>
            <strong>{formatTokens(totalInput)}</strong>
          </div>
          <div class="kpi">
            <span>Output</span>
            <strong>{formatTokens(summary.output_tokens)}</strong>
          </div>
        </div>
      )}

      <div class="item-list">
        {calls.length === 0 && !loading && (
          <div class="empty-hint small">No calls recorded for this filter.</div>
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
                  <span class="mono">#{c.id}</span>{" "}
                  <span class={`pill pill-${c.status}`}>{c.status}</span>{" "}
                  <span class="mono">{model ? model.model_key : `model #${c.model_id}`}</span>
                  {c.reasoning_effort && <span class="pill">{c.reasoning_effort}</span>}{" "}
                  <span class="muted">
                    {c.session_id != null ? (sess?.title || `sess #${c.session_id}`) : "deleted"}
                    {durMs != null ? ` · ${formatDuration(durMs)}` : ""}
                    {` · ${formatRelTime(c.started_at)}`}
                  </span>
                  {c.error_summary && <div class="err-text">{c.error_summary}</div>}
                </div>
                <div class="mono">
                  {formatTokens((c.input_uncached_tokens || 0) + (c.cache_read_tokens || 0))}→{formatTokens(c.output_tokens)} · {formatCost(c.cost_nano_usd)}
                </div>
              </div>
              {expanded && (
                <div class="call-detail small">
                  <div>
                    <span class="muted">Started:</span> <span class="mono">{formatAbsTime(c.started_at)}</span>
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
            Load more
          </button>
        )}
      </div>
    </div>
  );
}

export type SettingsTabId = "models" | "mcp" | "usage" | "general";

export interface SettingsProps {
  config: AppConfig | null;
  providers: ProviderInfo[];
  models: ModelInfo[];
  sessions: SessionInfo[];
  themePref: ThemePref;
  initialTab?: SettingsTabId;
  initialSessionFilter?: string;
  onSetThemePref: (pref: ThemePref) => void;
  onRefreshCatalog: () => Promise<void>;
  onSignOut: () => void;
  onClose: () => void;
}

export function Settings({
  config,
  providers,
  models,
  sessions,
  themePref,
  initialTab,
  initialSessionFilter,
  onSetThemePref,
  onRefreshCatalog,
  onSignOut,
  onClose,
}: SettingsProps) {
  const [tab, setTab] = useState<SettingsTabId>(initialTab || "models");

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const tabs = [
    { id: "models" as const, label: "Models", icon: <IconSliders size={14} /> },
    { id: "mcp" as const, label: "MCP", icon: <IconPlug size={14} /> },
    { id: "usage" as const, label: "Usage", icon: <IconChart size={14} /> },
    { id: "general" as const, label: "General", icon: <IconSettings size={14} /> },
  ];

  return (
    <div class="modal-backdrop" onClick={onClose}>
      <div class="modal" onClick={(e) => e.stopPropagation()}>
        <div class="modal-head">
          <div class="tabs">
            {tabs.map((t) => (
              <button
                key={t.id}
                type="button"
                class={`tab-btn ${tab === t.id ? "active" : ""}`}
                onClick={() => setTab(t.id)}
              >
                {t.icon}
                <span>{t.label}</span>
              </button>
            ))}
          </div>
          <button type="button" class="btn-icon" aria-label="Close settings" onClick={onClose}>
            <IconX size={16} />
          </button>
        </div>
        <div class="modal-body">
          {tab === "models" && (
            <ModelsTab config={config} providers={providers} models={models} onRefresh={onRefreshCatalog} />
          )}
          {tab === "mcp" && <McpTab />}
          {tab === "usage" && (
            <UsageTab models={models} sessions={sessions} initialSessionId={initialSessionFilter} />
          )}
          {tab === "general" && (
            <div class="tab-stack">
              <div class="panel list-row">
                <div>
                  <strong>Appearance</strong>
                  <div class="muted small">Theme preference across sessions</div>
                </div>
                <div class="segmented">
                  {(
                    [
                      ["system", "System", <IconMonitor size={13} />],
                      ["light", "Light", <IconSun size={13} />],
                      ["dark", "Dark", <IconMoon size={13} />],
                    ] as const
                  ).map(([val, label, icon]) => (
                    <button
                      key={val}
                      type="button"
                      class={`seg-btn ${themePref === val ? "active" : ""}`}
                      onClick={() => onSetThemePref(val)}
                    >
                      {icon}
                      <span>{label}</span>
                    </button>
                  ))}
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>API Documentation</strong>
                  <div class="muted small">Scalar interactive reference & OpenAPI schema (loopback only)</div>
                </div>
                <div class="row gap-1">
                  <a class="btn-ghost btn-sm row gap-1" href="/docs" target="_blank" rel="noopener noreferrer">
                    <span>/docs</span>
                    <IconExternal size={12} />
                  </a>
                  <a class="btn-ghost btn-sm row gap-1" href="/openapi.json" target="_blank" rel="noopener noreferrer">
                    <span>/openapi.json</span>
                    <IconExternal size={12} />
                  </a>
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>Authentication</strong>
                  <div class="muted small">Clear stored Bearer token from this browser</div>
                </div>
                <button type="button" class="btn-danger btn-sm" onClick={onSignOut}>
                  <IconKey size={13} />
                  <span>Sign out</span>
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
