const TOKEN_KEY = "mygo_token";
let onUnauthorized = () => {};

export function setUnauthorizedHandler(fn) {
  onUnauthorized = fn;
}

export function getToken() {
  try {
    return localStorage.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

export function setToken(token) {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {}
}

export function consumeHashToken() {
  const hash = location.hash || "";
  if (!hash.startsWith("#")) return "";
  const params = new URLSearchParams(hash.slice(1));
  const token = params.get("token");
  if (!token) return "";
  setToken(token);
  params.delete("token");
  const rest = params.toString();
  history.replaceState(null, "", location.pathname + location.search + (rest ? "#" + rest : ""));
  return token;
}

export function makeIdempotencyKey() {
  return crypto.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export async function apiFetch(path, opts = {}) {
  const headers = new Headers(opts.headers || {});
  const token = opts.token !== undefined ? opts.token : getToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);

  let body = opts.body;
  if (
    body !== undefined &&
    body !== null &&
    typeof body !== "string" &&
    !(body instanceof Uint8Array) &&
    !(body instanceof Blob) &&
    !(body instanceof ArrayBuffer)
  ) {
    if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    body = JSON.stringify(body);
  }

  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body,
    signal: opts.signal,
  });

  if (res.status === 401 && opts.token === undefined) {
    setToken("");
    onUnauthorized();
  }

  const text = res.status === 204 ? "" : await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!res.ok) {
    const msg =
      (data && typeof data === "object" && data.error) ||
      (typeof data === "string" && data) ||
      `HTTP ${res.status}`;
    const err = new Error(msg);
    err.status = res.status;
    throw err;
  }
  return data;
}

export function connectSse(path, { onEvent, onStatus, signal } = {}) {
  let active = true;
  let backoff = 500;
  let currentAbort = null;

  const stop = () => {
    active = false;
    currentAbort?.abort();
  };
  if (signal) {
    if (signal.aborted) return stop;
    signal.addEventListener("abort", stop, { once: true });
  }

  (async () => {
    while (active) {
      currentAbort = new AbortController();
      try {
        const headers = new Headers({ Accept: "text/event-stream" });
        const token = getToken();
        if (token) headers.set("Authorization", `Bearer ${token}`);
        const res = await fetch(path, { headers, signal: currentAbort.signal });
        if (res.status === 401) {
          setToken("");
          onUnauthorized();
          return;
        }
        if (!res.ok || !res.body) {
          throw new Error(`SSE HTTP ${res.status}`);
        }
        onStatus?.("open");
        backoff = 500;

        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";
        let eventName = "";
        let dataLines = [];

        const dispatch = () => {
          if (!eventName && dataLines.length === 0) return;
          const raw = dataLines.join("\n");
          let parsed = raw;
          if (raw) {
            try {
              parsed = JSON.parse(raw);
            } catch {}
          }
          onEvent?.(eventName || "message", parsed);
          eventName = "";
          dataLines = [];
        };

        while (active) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let nl;
          while ((nl = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, nl).replace(/\r$/, "");
            buffer = buffer.slice(nl + 1);
            if (line === "") {
              dispatch();
            } else if (line.startsWith(":")) {
              continue;
            } else if (line.startsWith("event:")) {
              eventName = line.slice(6).trim();
            } else if (line.startsWith("data:")) {
              dataLines.push(line.slice(5).replace(/^ /, ""));
            }
          }
        }
      } catch (err) {
        if (!active || err?.name === "AbortError") break;
        onStatus?.("reconnecting");
      }
      if (!active) break;
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 8000);
    }
  })();

  return stop;
}

const mediaCache = new Map();
const MAX_MEDIA_CACHE = 48;

export async function fetchMediaUrl(id) {
  const key = String(id);
  if (mediaCache.has(key)) {
    const val = mediaCache.get(key);
    mediaCache.delete(key);
    mediaCache.set(key, val);
    return val;
  }
  const headers = new Headers();
  const token = getToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const res = await fetch(`/attachments/${id}`, { headers });
  if (res.status === 401) {
    setToken("");
    onUnauthorized();
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  mediaCache.set(key, url);
  while (mediaCache.size > MAX_MEDIA_CACHE) {
    const firstKey = mediaCache.keys().next().value;
    const oldUrl = mediaCache.get(firstKey);
    mediaCache.delete(firstKey);
    URL.revokeObjectURL(oldUrl);
  }
  return url;
}

export function formatCost(nanoUsd) {
  if (nanoUsd == null) return "—";
  const usd = nanoUsd / 1e9;
  if (usd === 0) return "$0.00";
  if (usd < 0.0001) return `$${usd.toFixed(6)}`;
  if (usd < 0.1) return `$${usd.toFixed(4)}`;
  return `$${usd.toFixed(3)}`;
}

export function formatTokens(n) {
  if (n == null) return "0";
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return String(n);
}

export function formatRelTime(ms) {
  if (!ms) return "";
  const diff = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  if (diff < 60) return "now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  return `${Math.floor(diff / 86400)}d`;
}
