export type Theme = "light" | "dark";
export type ThemePref = "system" | "light" | "dark";
export type MainTab = "chat" | "trace" | "context";
export type SseStatus = "open" | "reconnecting";
export type SessionStatus = "idle" | "running" | "pausing" | "paused";

export interface SessionInfo {
  id: string;
  title: string;
  created_at: number;
  updated_at: number;
  status: SessionStatus;
  reasoning_effort: string | null;
  last_error: string;
  model_id: string | null;
}

export interface SessionStatusEvent {
  id?: string;
  status: SessionStatus;
  reason?: string;
  updated_at?: number;
}

export interface SessionDeletedEvent {
  id: string;
}

export interface AppConfig {
  default_model_id: string | null;
  default_reasoning_effort: string | null;
}

export interface ProviderInfo {
  id: string;
  name: string;
  base_url: string;
  has_api_key: boolean;
  archived_at: number | null;
}

export interface ModelInfo {
  id: string;
  provider_id: string;
  model_key: string;
  context_window_tokens: number | null;
  input_modalities: string[];
  reasoning_efforts: string[];
  pricing: Record<string, unknown> | null;
  archived_at: number | null;
  provider_archived_at: number | null;
}

export type McpTransport = "streamable-http" | "stdio";
export type McpToolPolicy = "all" | "none" | "allowlist";
export type McpSecretAction = "keep" | "replace" | "clear";
export type McpOAuthAction = "keep" | "replace" | "disable";

export interface McpServerInfo {
  id: string;
  name: string;
  description: string;
  enabled: boolean;
  transport: McpTransport;
  url: string | null;
  has_url_query: boolean;
  has_headers: boolean;
  has_oauth: boolean;
  command: string | null;
  arg_count: number;
  has_env: boolean;
  cwd: string | null;
  allowed_tools: string[] | null;
  timeout_ms: number;
  revision: number;
}

export interface McpFormState {
  id: string | null;
  orig: McpServerInfo | null;
  name: string;
  description: string;
  enabled: boolean;
  transport: McpTransport;
  url: string;
  replaceUrl: boolean;
  headersAction: McpSecretAction;
  headersJson: string;
  oauthAction: McpOAuthAction;
  oauthJson: string;
  command: string;
  argsAction: McpSecretAction;
  argsText: string;
  envAction: McpSecretAction;
  envJson: string;
  cwd: string;
  toolPolicy: McpToolPolicy;
  allowedTools: string[];
  newAllowedTool: string;
  timeout_ms: number | string;
}

export interface McpProbeListSummary {
  operation: string;
  count: number;
  has_more: boolean;
}

export interface McpProbeResult {
  id: string;
  revision: number;
  ok: boolean;
  stage: string;
  elapsed_ms: number;
  protocol_version: string | null;
  lists: McpProbeListSummary[];
  error: string | null;
}

export interface McpOAuthStatusResponse {
  status: string;
}

export interface McpOAuthLoginInfo {
  authorization_url: string;
}

export interface CallsSummary {
  call_count: number;
  succeeded_count: number;
  failed_count: number;
  input_uncached_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  output_tokens: number;
  cost_nano_usd: number;
  unpriced_succeeded_count: number;
  last_input_tokens: number | null;
}

export interface LlmCallInfo {
  id: string;
  session_id: string | null;
  model_id: string;
  reasoning_effort: string | null;
  input_end_position: number | null;
  started_at: number;
  finished_at: number | null;
  status: string;
  upstream_response_id: string | null;
  http_status: number | null;
  error_summary: string | null;
  input_uncached_tokens: number | null;
  cache_read_tokens: number | null;
  cache_write_tokens: number | null;
  output_tokens: number | null;
  cost_nano_usd: number | null;
}

export interface ContentBlock {
  type?: string;
  text?: string;
  refusal?: string;
  attachment_id?: string;
  [k: string]: unknown;
}

export type HistoryPayload = {
  type?: string;
  role?: string;
  content?: string | ContentBlock[];
  name?: string;
  call_id?: string;
  arguments?: string;
  output?: string | ContentBlock[];
  summary?: Array<{ text?: string }>;
  task_id?: string;
  event?: string;
  sequence?: number;
  notify?: boolean;
  [k: string]: unknown;
};

export interface HistoryItem {
  id: string;
  kind: string;
  payload: HistoryPayload;
  target_id: string | null;
  llm_call_id: string | null;
}

export interface ContextItem {
  position: number;
  item_id: string;
  llm_call_id: string | null;
  payload: HistoryPayload;
}

export interface DraftItem {
  output_index: number;
  kind: string;
  name: string | null;
  call_id: string | null;
  text: string;
  reasoning: string;
  arguments: string;
}

export interface ActiveDraft {
  llm_call_id: string;
  items: DraftItem[];
}

export interface DraftStartedEvent {
  llm_call_id: string;
  output_index: number;
  kind?: string;
  name?: string | null;
  call_id?: string | null;
}

export interface DraftDeltaEvent {
  llm_call_id: string;
  output_index: number;
  field: "text" | "reasoning" | "arguments" | string;
  delta?: string;
}

export interface RetryNotice {
  attempt: number;
  reason: string;
}

export interface RunningCommand {
  id: number;
  program: string;
  args?: string[];
  stdout?: string;
  stderr?: string;
}

export interface ActiveTask {
  id: string;
  label?: string;
  delivery: string;
  remaining_seconds?: number | null;
  started_at?: string;
  expires_at?: string;
  state?: string;
  running_commands?: RunningCommand[];
  console?: {
    total_bytes?: number;
    truncated?: boolean;
    output?: string;
  };
}

export type MediaKind = "image" | "audio" | "video";

export interface ComposerAttachment {
  id: string;
  kind: MediaKind;
  name: string;
  preview: string | null;
}

export interface ComposerPending {
  key: string;
  contentJson: string;
}

export interface ComposerDraft {
  text: string;
  attachments: ComposerAttachment[];
  version: number;
  textVersion: number;
  pending: ComposerPending | null;
  error: string;
}

export interface SubmittedComposerSnapshot {
  key: string;
  version: number;
  textVersion: number;
  attachmentIds: Set<string>;
}
