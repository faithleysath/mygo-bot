import { useEffect, useState } from "preact/hooks";
import { apiFetch, formatCost, formatTokens } from "./api.js";
import { buildMcpPayload } from "./state.js";

function ModelsTab({ config, providers, models, onRefresh }) {
  const [showArchived, setShowArchived] = useState(false);
  const [err, setErr] = useState("");
  const [provForm, setProvForm] = useState(null);
  const [modelForm, setModelForm] = useState(null);

  const visibleProviders = showArchived ? providers : providers.filter((p) => !p.archived_at);
  const visibleModels = showArchived ? models : models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const availableModels = models.filter((m) => !m.archived_at && !m.provider_archived_at);
  const selectedDefault = models.find((m) => m.id === config?.default_model_id);

  const run = async (fn) => {
    setErr("");
    try {
      await fn();
      await onRefresh();
    } catch (e) {
      setErr(e.message);
    }
  };

  const saveDefault = (patch) => run(() => apiFetch("/config", { method: "PATCH", body: patch }));

  const saveProvider = (e) => {
    e.preventDefault();
    const f = provForm;
    const body = { name: f.name, base_url: f.base_url };
    if (f.api_key !== "") body.api_key = f.api_key;
    else if (f.clearKey) body.api_key = null;
    run(async () => {
      if (f.id) await apiFetch(`/providers/${f.id}`, { method: "PATCH", body });
      else await apiFetch("/providers", { method: "POST", body });
      setProvForm(null);
    });
  };

  const saveModel = (e) => {
    e.preventDefault();
    const f = modelForm;
    let pricing = null;
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
    run(async () => {
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
            <input type="checkbox" checked={showArchived} onChange={(e) => setShowArchived(e.target.checked)} />
            Show archived
          </label>
        </div>
        <div class="grid-2">
          <label class="field">
            <span>Default Model</span>
            <select
              value={config?.default_model_id ?? ""}
              onChange={(e) => saveDefault({ default_model_id: e.target.value ? Number(e.target.value) : null })}
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
              onChange={(e) => saveDefault({ default_reasoning_effort: e.target.value || null })}
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
                <input required value={provForm.name} onInput={(e) => setProvForm({ ...provForm, name: e.target.value })} />
              </label>
              <label class="field">
                <span>Base URL</span>
                <input required placeholder="https://api.openai.com/v1" value={provForm.base_url} onInput={(e) => setProvForm({ ...provForm, base_url: e.target.value })} />
              </label>
            </div>
            <label class="field">
              <span>API Key {provForm.id ? "(leave blank to keep)" : ""}</span>
              <input type="password" value={provForm.api_key} onInput={(e) => setProvForm({ ...provForm, api_key: e.target.value })} />
            </label>
            {provForm.id && (
              <label class="check-row">
                <input type="checkbox" checked={provForm.clearKey} onChange={(e) => setProvForm({ ...provForm, clearKey: e.target.checked })} />
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
          {visibleProviders.map((p) => (
            <div key={p.id} class="list-row">
              <div>
                <strong>{p.name}</strong> {p.archived_at && <span class="pill">archived</span>}
                <div class="muted small">{p.base_url} · {p.has_api_key ? "key set" : "no key"}</div>
              </div>
              <div class="row gap-1">
                <button type="button" class="btn-ghost btn-xs" onClick={() => setProvForm({ id: p.id, name: p.name, base_url: p.base_url, api_key: "", clearKey: false })}>
                  Edit
                </button>
                {p.archived_at ? (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => run(() => apiFetch(`/providers/${p.id}/restore`, { method: "POST" }))}>
                    Restore
                  </button>
                ) : (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => run(() => apiFetch(`/providers/${p.id}`, { method: "DELETE" }))}>
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
                <select value={modelForm.provider_id} onChange={(e) => setModelForm({ ...modelForm, provider_id: e.target.value })}>
                  {visibleProviders.filter((p) => !p.archived_at).map((p) => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </label>
              <label class="field">
                <span>Model Key</span>
                <input required value={modelForm.model_key} onInput={(e) => setModelForm({ ...modelForm, model_key: e.target.value })} />
              </label>
              <label class="field">
                <span>Context Window</span>
                <input type="number" min="1" placeholder="optional" value={modelForm.context_window_tokens} onInput={(e) => setModelForm({ ...modelForm, context_window_tokens: e.target.value })} />
              </label>
              <label class="field">
                <span>Efforts (comma-separated)</span>
                <input value={modelForm.reasoning_efforts} onInput={(e) => setModelForm({ ...modelForm, reasoning_efforts: e.target.value })} />
              </label>
            </div>
            <div class="row gap-2">
              {["text", "image", "audio", "video"].map((mod) => (
                <label key={mod} class="check-row">
                  <input
                    type="checkbox"
                    checked={modelForm.input_modalities.includes(mod)}
                    onChange={(e) => {
                      const next = e.target.checked
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
              <textarea rows={2} value={modelForm.pricingText} onInput={(e) => setModelForm({ ...modelForm, pricingText: e.target.value })} />
            </label>
            <div class="row gap-1">
              <button type="submit" class="btn-primary btn-xs">Save</button>
              <button type="button" class="btn-ghost btn-xs" onClick={() => setModelForm(null)}>Cancel</button>
            </div>
          </form>
        )}
        <div class="item-list">
          {visibleModels.map((m) => (
            <div key={m.id} class="list-row">
              <div>
                <strong>{m.model_key}</strong> {(m.archived_at || m.provider_archived_at) && <span class="pill">archived</span>}
                <div class="muted small">
                  {m.input_modalities.join(", ")} · ctx {m.context_window_tokens || "—"} · efforts [{m.reasoning_efforts.join(", ")}]
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
                  <button type="button" class="btn-ghost btn-xs" onClick={() => run(() => apiFetch(`/models/${m.id}/restore`, { method: "POST" }))}>
                    Restore
                  </button>
                ) : (
                  <button type="button" class="btn-ghost btn-xs" onClick={() => run(() => apiFetch(`/models/${m.id}`, { method: "DELETE" }))}>
                    Archive
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}

function McpTab() {
  const [servers, setServers] = useState([]);
  const [oauth, setOauth] = useState({});
  const [probe, setProbe] = useState({});
  const [loginInfo, setLoginInfo] = useState({});
  const [callbackUrl, setCallbackUrl] = useState({});
  const [form, setForm] = useState(null);
  const [err, setErr] = useState("");

  const load = async () => {
    const list = await apiFetch("/mcp/servers");
    setServers(list);
    const entries = await Promise.all(
      list.filter((s) => s.has_oauth).map(async (s) => [s.id, (await apiFetch(`/mcp/servers/${s.id}/oauth`)).status])
    );
    setOauth(Object.fromEntries(entries));
  };

  useEffect(() => {
    load().catch((e) => setErr(e.message));
  }, []);

  const run = async (fn) => {
    setErr("");
    try {
      await fn();
      await load();
    } catch (e) {
      setErr(e.message);
    }
  };

  const save = (e) => {
    e.preventDefault();
    let body;
    try {
      body = buildMcpPayload(form);
    } catch (ex) {
      setErr(ex.message);
      return;
    }
    if (form.id && Object.keys(body).length === 0) {
      setForm(null);
      return;
    }
    run(async () => {
      if (form.id) await apiFetch(`/mcp/servers/${form.id}`, { method: "PATCH", body });
      else await apiFetch("/mcp/servers", { method: "POST", body });
      setForm(null);
    });
  };

  const editingSameTransport = form?.id && form.transport === form.orig?.transport;

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      <div class="panel-head">
        <h4>MCP Servers</h4>
        <button
          type="button"
          class="btn-primary btn-xs"
          onClick={() =>
            setForm({
              id: null,
              orig: null,
              name: "",
              description: "",
              enabled: true,
              transport: "streamable-http",
              url: "",
              replaceUrl: true,
              headersAction: "keep",
              headersJson: "",
              oauthAction: "keep",
              oauthJson: "",
              command: "",
              argsAction: "replace",
              argsText: "",
              envAction: "keep",
              envJson: "",
              cwd: "",
              toolPolicy: "all",
              allowedTools: "",
              timeout_ms: 60000,
            })
          }
        >
          + Server
        </button>
      </div>

      {form && (
        <form class="form-box" onSubmit={save}>
          <div class="grid-2">
            <label class="field">
              <span>Name {form.id ? "(immutable)" : ""}</span>
              <input required disabled={Boolean(form.id)} value={form.name} onInput={(e) => setForm({ ...form, name: e.target.value })} />
            </label>
            <label class="field">
              <span>Transport</span>
              <select
                value={form.transport}
                onChange={(e) => {
                  const next = e.target.value;
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
              <input value={form.description} onInput={(e) => setForm({ ...form, description: e.target.value })} />
            </label>
            <label class="field">
              <span>Timeout (ms)</span>
              <input type="number" min="1" value={form.timeout_ms} onInput={(e) => setForm({ ...form, timeout_ms: e.target.value })} />
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
                      <input type="checkbox" checked={form.replaceUrl} onChange={(e) => setForm({ ...form, replaceUrl: e.target.checked })} />
                      Replace URL
                    </label>
                  </div>
                </div>
              ) : (
                <label class="field">
                  <span>URL</span>
                  <div class="row gap-2">
                    <input required value={form.url} onInput={(e) => setForm({ ...form, url: e.target.value })} style="flex:1" />
                    {editingSameTransport && form.orig?.has_url_query && (
                      <label class="check-row">
                        <input type="checkbox" checked={form.replaceUrl} onChange={(e) => setForm({ ...form, replaceUrl: e.target.checked, url: form.orig.url || "" })} />
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
                    <select value={form.headersAction} onChange={(e) => setForm({ ...form, headersAction: e.target.value })}>
                      <option value="keep">{form.orig?.has_headers ? "Keep existing headers" : "None (unchanged)"}</option>
                      <option value="replace">Replace headers (JSON)</option>
                      <option value="clear">Clear headers</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>OAuth 2.1</span>
                    <select value={form.oauthAction} onChange={(e) => setForm({ ...form, oauthAction: e.target.value })}>
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
                    onChange={(e) => setForm({ ...form, oauthAction: e.target.checked ? "replace" : "keep" })}
                  />
                  Enable OAuth 2.1
                </label>
              )}

              {(!form.id || form.headersAction === "replace") && (
                <label class="field">
                  <span>Headers JSON {form.id ? "" : "(optional)"}</span>
                  <textarea rows={2} placeholder='{"Authorization":"Bearer ..."}' value={form.headersJson} onInput={(e) => setForm({ ...form, headersJson: e.target.value })} />
                </label>
              )}
              {form.oauthAction === "replace" && (
                <label class="field">
                  <span>OAuth Config JSON (optional)</span>
                  <textarea rows={2} placeholder="{}" value={form.oauthJson} onInput={(e) => setForm({ ...form, oauthJson: e.target.value })} />
                </label>
              )}
            </>
          ) : (
            <>
              <div class="grid-2">
                <label class="field">
                  <span>Command</span>
                  <input required value={form.command} onInput={(e) => setForm({ ...form, command: e.target.value })} />
                </label>
                <label class="field">
                  <span>Working Dir (optional)</span>
                  <input value={form.cwd} onInput={(e) => setForm({ ...form, cwd: e.target.value })} />
                </label>
              </div>
              {form.id ? (
                <div class="grid-2">
                  <label class="field">
                    <span>Args</span>
                    <select value={form.argsAction} onChange={(e) => setForm({ ...form, argsAction: e.target.value })}>
                      <option value="keep">Keep existing ({form.orig?.arg_count || 0} args)</option>
                      <option value="replace">Replace args</option>
                      <option value="clear">Clear args</option>
                    </select>
                  </label>
                  <label class="field">
                    <span>Env</span>
                    <select value={form.envAction} onChange={(e) => setForm({ ...form, envAction: e.target.value })}>
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
                  <textarea rows={2} value={form.argsText} onInput={(e) => setForm({ ...form, argsText: e.target.value })} />
                </label>
              )}
              {(!form.id || form.envAction === "replace") && (
                <label class="field">
                  <span>Env JSON {form.id ? "" : "(optional)"}</span>
                  <textarea rows={2} value={form.envJson} onInput={(e) => setForm({ ...form, envJson: e.target.value })} />
                </label>
              )}
            </>
          )}

          <div class="grid-2">
            <label class="field">
              <span>Tool Policy</span>
              <select value={form.toolPolicy} onChange={(e) => setForm({ ...form, toolPolicy: e.target.value })}>
                <option value="all">Allow all tools</option>
                <option value="none">Deny all tools</option>
                <option value="allowlist">Allow listed tools only</option>
              </select>
            </label>
            {form.toolPolicy === "allowlist" && (
              <label class="field">
                <span>Allowed Tools (comma-separated)</span>
                <input required value={form.allowedTools} onInput={(e) => setForm({ ...form, allowedTools: e.target.value })} />
              </label>
            )}
          </div>

          <label class="check-row">
            <input type="checkbox" checked={form.enabled} onChange={(e) => setForm({ ...form, enabled: e.target.checked })} />
            Enabled
          </label>
          <div class="row gap-1">
            <button type="submit" class="btn-primary btn-xs">Save</button>
            <button type="button" class="btn-ghost btn-xs" onClick={() => setForm(null)}>Cancel</button>
          </div>
        </form>
      )}

      <div class="item-list">
        {servers.map((s) => (
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
                  onClick={() => run(async () => {
                    const res = await apiFetch(`/mcp/servers/${s.id}/test`, { method: "POST" });
                    setProbe((p) => ({ ...p, [s.id]: res }));
                  })}
                >
                  Test
                </button>
                {s.has_oauth && (
                  <>
                    <button
                      type="button"
                      class="btn-ghost btn-xs"
                      onClick={() => run(async () => {
                        const res = await apiFetch(`/mcp/servers/${s.id}/oauth/login`, { method: "POST" });
                        setLoginInfo((l) => ({ ...l, [s.id]: res }));
                      })}
                    >
                      Login
                    </button>
                    <button
                      type="button"
                      class="btn-ghost btn-xs"
                      onClick={() => run(() => apiFetch(`/mcp/servers/${s.id}/oauth`, { method: "DELETE" }))}
                    >
                      Logout
                    </button>
                  </>
                )}
                <button
                  type="button"
                  class="btn-ghost btn-xs"
                  onClick={() =>
                    setForm({
                      id: s.id,
                      orig: s,
                      name: s.name,
                      description: s.description,
                      enabled: s.enabled,
                      transport: s.transport,
                      url: s.url || "",
                      replaceUrl: false,
                      headersAction: "keep",
                      headersJson: "",
                      oauthAction: "keep",
                      oauthJson: "",
                      command: s.command || "",
                      argsAction: "keep",
                      argsText: "",
                      envAction: "keep",
                      envJson: "",
                      cwd: s.cwd || "",
                      toolPolicy:
                        s.allowed_tools === null
                          ? "all"
                          : s.allowed_tools.length === 0
                            ? "none"
                            : "allowlist",
                      allowedTools: (s.allowed_tools || []).join(", "),
                      timeout_ms: s.timeout_ms,
                    })
                  }
                >
                  Edit
                </button>
                <button
                  type="button"
                  class="btn-danger btn-xs"
                  onClick={() => run(() => apiFetch(`/mcp/servers/${s.id}`, { method: "DELETE" }))}
                >
                  Delete
                </button>
              </div>
            </div>

            {probe[s.id] && (
              <pre class="code-box small">
                {probe[s.id].ok
                  ? `OK (${probe[s.id].elapsed_ms}ms, v=${probe[s.id].protocol_version || "?"}) ` +
                    probe[s.id].lists.map((l) => `${l.operation}:${l.count}${l.has_more ? "+" : ""}`).join(", ")
                  : `FAILED at ${probe[s.id].stage}: ${probe[s.id].error}`}
              </pre>
            )}

            {loginInfo[s.id] && (
              <div class="form-box">
                <div class="small">
                  Authorize here:{" "}
                  <a href={loginInfo[s.id].authorization_url} target="_blank" rel="noopener noreferrer">
                    Open OAuth Login
                  </a>
                </div>
                <div class="row gap-1">
                  <input
                    placeholder="Paste redirect URL if remote…"
                    value={callbackUrl[s.id] || ""}
                    onInput={(e) => setCallbackUrl((c) => ({ ...c, [s.id]: e.target.value }))}
                  />
                  <button
                    type="button"
                    class="btn-primary btn-xs"
                    onClick={() =>
                      run(async () => {
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
        ))}
      </div>
    </div>
  );
}

function UsageTab() {
  const [summary, setSummary] = useState(null);
  const [calls, setCalls] = useState([]);
  const [sessionId, setSessionId] = useState("");
  const [err, setErr] = useState("");

  const load = async (after = 0, append = false) => {
    setErr("");
    const q = sessionId.trim() ? `?session_id=${encodeURIComponent(sessionId.trim())}` : "";
    const sep = q ? "&" : "?";
    try {
      const [sum, list] = await Promise.all([
        apiFetch(`/calls/summary${q}`),
        apiFetch(`/calls${q}${sep}after=${after}&limit=50`),
      ]);
      setSummary(sum);
      setCalls((prev) => (append ? [...prev, ...list] : list));
    } catch (e) {
      setErr(e.message);
    }
  };

  useEffect(() => {
    load(0, false);
  }, []);

  return (
    <div class="tab-stack">
      {err && <div class="err-inline">{err}</div>}
      <div class="row gap-1">
        <input
          placeholder="Filter by session ID…"
          value={sessionId}
          onInput={(e) => setSessionId(e.target.value)}
        />
        <button type="button" class="btn-ghost btn-sm" onClick={() => load(0, false)}>
          Filter
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
            <strong>{summary.succeeded_count} / {summary.call_count}</strong>
          </div>
          <div class="kpi">
            <span>Input / Cached</span>
            <strong>{formatTokens(summary.input_uncached_tokens)} / {formatTokens(summary.cache_read_tokens)}</strong>
          </div>
          <div class="kpi">
            <span>Output</span>
            <strong>{formatTokens(summary.output_tokens)}</strong>
          </div>
        </div>
      )}

      <div class="item-list">
        {calls.map((c) => (
          <div key={c.id} class="list-row small">
            <div>
              <span class="mono">#{c.id}</span>{" "}
              <span class="pill">{c.status}</span>{" "}
              <span class="muted">sess {c.session_id ?? "deleted"} · model {c.model_id}</span>
              {c.error_summary && <div class="err-text">{c.error_summary}</div>}
            </div>
            <div class="mono">
              {formatTokens((c.input_uncached_tokens || 0) + (c.cache_read_tokens || 0))}→{formatTokens(c.output_tokens)} · {formatCost(c.cost_nano_usd)}
            </div>
          </div>
        ))}
        {calls.length > 0 && calls.length % 50 === 0 && (
          <button type="button" class="btn-ghost btn-sm" onClick={() => load(calls[calls.length - 1].id, true)}>
            Load more
          </button>
        )}
      </div>
    </div>
  );
}

export function Settings({
  config,
  providers,
  models,
  theme,
  onToggleTheme,
  onRefreshCatalog,
  onSignOut,
  onClose,
}) {
  const [tab, setTab] = useState("models");

  return (
    <div class="modal-backdrop" onClick={onClose}>
      <div class="modal" onClick={(e) => e.stopPropagation()}>
        <div class="modal-head">
          <div class="tabs">
            {[
              ["models", "Models"],
              ["mcp", "MCP"],
              ["usage", "Usage"],
              ["general", "General"],
            ].map(([id, label]) => (
              <button
                key={id}
                type="button"
                class={`tab-btn ${tab === id ? "active" : ""}`}
                onClick={() => setTab(id)}
              >
                {label}
              </button>
            ))}
          </div>
          <button type="button" class="btn-icon" onClick={onClose}>×</button>
        </div>
        <div class="modal-body">
          {tab === "models" && (
            <ModelsTab config={config} providers={providers} models={models} onRefresh={onRefreshCatalog} />
          )}
          {tab === "mcp" && <McpTab />}
          {tab === "usage" && <UsageTab />}
          {tab === "general" && (
            <div class="tab-stack">
              <div class="panel list-row">
                <div>
                  <strong>Appearance</strong>
                  <div class="muted small">Current theme: {theme}</div>
                </div>
                <button type="button" class="btn-ghost btn-sm" onClick={onToggleTheme}>
                  Switch to {theme === "dark" ? "light" : "dark"}
                </button>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>API Documentation</strong>
                  <div class="muted small">Scalar interactive reference & OpenAPI schema (loopback only)</div>
                </div>
                <div class="row gap-1">
                  <a class="btn-ghost btn-sm" href="/docs" target="_blank" rel="noopener noreferrer">/docs</a>
                  <a class="btn-ghost btn-sm" href="/openapi.json" target="_blank" rel="noopener noreferrer">/openapi.json</a>
                </div>
              </div>
              <div class="panel list-row">
                <div>
                  <strong>Authentication</strong>
                  <div class="muted small">Clear stored Bearer token from this browser</div>
                </div>
                <button type="button" class="btn-danger btn-sm" onClick={onSignOut}>
                  Sign out
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
