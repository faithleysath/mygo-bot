import type { SseStatus } from "./types.ts";

const TOKEN_KEY = "mygo_token";
let onUnauthorized: () => void = () => {};

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

export function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

export function setUnauthorizedHandler(fn: () => void): void {
  onUnauthorized = fn;
}

export function getToken(): string {
  try {
    return localStorage.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

export function setToken(token: string): void {
  try {
    if (token) localStorage.setItem(TOKEN_KEY, token);
    else localStorage.removeItem(TOKEN_KEY);
  } catch {}
}

export function consumeHashToken(): string {
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

export function makeIdempotencyKey(): string {
  return crypto.randomUUID?.() || `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

export interface ApiFetchOptions {
  method?: string;
  headers?: HeadersInit;
  body?: unknown;
  token?: string;
  signal?: AbortSignal;
}

export async function apiFetch<T = unknown>(path: string, opts: ApiFetchOptions = {}): Promise<T> {
  const headers = new Headers(opts.headers || {});
  const token = opts.token !== undefined ? opts.token : getToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);

  let body: BodyInit | undefined;
  const rawBody = opts.body;
  if (rawBody !== undefined && rawBody !== null) {
    if (
      typeof rawBody === "string" ||
      rawBody instanceof Uint8Array ||
      rawBody instanceof Blob ||
      rawBody instanceof ArrayBuffer
    ) {
      body = rawBody as BodyInit;
    } else {
      if (!headers.has("Content-Type")) headers.set("Content-Type", "application/json");
      body = JSON.stringify(rawBody);
    }
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
  let data: unknown = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = text;
    }
  }

  if (!res.ok) {
    const obj = data && typeof data === "object" ? (data as { error?: unknown }) : null;
    const msg =
      (typeof obj?.error === "string" && obj.error) ||
      (typeof data === "string" && data) ||
      `HTTP ${res.status}`;
    throw new ApiError(msg, res.status);
  }
  return data as T;
}

export interface SseOptions {
  onEvent?: (name: string, data: unknown) => void;
  onStatus?: (status: SseStatus) => void;
  signal?: AbortSignal;
}

export function connectSse(path: string, { onEvent, onStatus, signal }: SseOptions = {}): () => void {
  let active = true;
  let backoff = 500;
  let currentAbort: AbortController | null = null;

  const stop = () => {
    active = false;
    currentAbort?.abort();
  };
  if (signal) {
    if (signal.aborted) return stop;
    signal.addEventListener("abort", stop, { once: true });
  }

  void (async () => {
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
        let dataLines: string[] = [];

        const dispatch = () => {
          if (!eventName && dataLines.length === 0) return;
          const raw = dataLines.join("\n");
          let parsed: unknown = raw;
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
          let nl: number;
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
        if (!active || (err instanceof Error && err.name === "AbortError")) break;
        onStatus?.("reconnecting");
      }
      if (!active) break;
      await new Promise((r) => setTimeout(r, backoff));
      backoff = Math.min(backoff * 2, 8000);
    }
  })();

  return stop;
}

const mediaCache = new Map<string, string>();
const MAX_MEDIA_CACHE = 48;

export async function fetchMediaUrl(id: string): Promise<string> {
  const key = id;
  const cached = mediaCache.get(key);
  if (cached !== undefined) {
    mediaCache.delete(key);
    mediaCache.set(key, cached);
    return cached;
  }
  const headers = new Headers();
  const token = getToken();
  if (token) headers.set("Authorization", `Bearer ${token}`);
  const res = await fetch(`/attachments/${id}`, { headers });
  if (res.status === 401) {
    setToken("");
    onUnauthorized();
  }
  if (!res.ok) throw new ApiError(`HTTP ${res.status}`, res.status);
  const blob = await res.blob();
  const url = URL.createObjectURL(blob);
  mediaCache.set(key, url);
  while (mediaCache.size > MAX_MEDIA_CACHE) {
    const firstKey = mediaCache.keys().next().value;
    if (firstKey === undefined) break;
    const oldUrl = mediaCache.get(firstKey);
    mediaCache.delete(firstKey);
    if (oldUrl) URL.revokeObjectURL(oldUrl);
  }
  return url;
}

export function formatCost(nanoUsd: number | null | undefined): string {
  if (nanoUsd == null) return "—";
  const usd = nanoUsd / 1e9;
  if (usd === 0) return "$0.00";
  if (usd < 0.0001) return `$${usd.toFixed(6)}`;
  if (usd < 0.1) return `$${usd.toFixed(4)}`;
  return `$${usd.toFixed(3)}`;
}

export function formatTokens(n: number | null | undefined): string {
  if (n == null) return "0";
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`;
  return String(n);
}

export function formatDuration(ms: number | null | undefined): string {
  if (ms == null || ms < 0) return "";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const sec = ms / 1000;
  if (sec < 60) return `${sec.toFixed(sec < 10 ? 1 : 0)}s`;
  const mins = Math.floor(sec / 60);
  const rem = Math.round(sec % 60);
  return rem ? `${mins}m ${rem}s` : `${mins}m`;
}

export function formatAbsTime(ms: number | null | undefined): string {
  if (!ms) return "";
  try {
    return new Date(ms).toLocaleString();
  } catch {
    return "";
  }
}

export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}
