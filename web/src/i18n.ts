import { createContext } from "preact";
import { useContext } from "preact/hooks";

export type Locale = "zh" | "en";
export type LocalePref = "auto" | "zh" | "en";

const LANG_KEY = "mygo_lang";

export function readLocalePref(): LocalePref {
  try {
    const v = localStorage.getItem(LANG_KEY);
    if (v === "zh" || v === "en") return v;
  } catch {}
  return "auto";
}

export function saveLocalePref(pref: LocalePref): void {
  try {
    if (pref === "auto") localStorage.removeItem(LANG_KEY);
    else localStorage.setItem(LANG_KEY, pref);
  } catch {}
}

export function resolveLocale(pref: LocalePref): Locale {
  if (pref === "zh" || pref === "en") return pref;
  if (typeof navigator !== "undefined" && navigator.language?.toLowerCase().startsWith("zh")) {
    return "zh";
  }
  return "en";
}

const ZH = {
  "Connecting…": "正在连接…",
  "Enter the daemon TCP Bearer token to connect.": "请输入守护进程的 TCP Bearer Token 以连接。",
  "Bearer token": "Bearer token",
  "Show token": "显示 Token",
  "Hide token": "隐藏 Token",
  "Connect": "连接",
  "Verifying…": "验证中…",
  "Run": "运行",
  "on the host to view the token.": "查看宿主机 Token。",
  "Invalid Bearer token": "Bearer token 无效",
  "Select a session from the sidebar or create a new one.": "从左侧选择一个会话，或新建会话开始。",
  "No available models configured yet — open Settings to add a provider and model.": "暂未配置可用模型——请先打开设置添加供应商和模型。",
  "New Session": "新会话",
  "Settings": "设置",
  "Models & Providers": "模型与供应商",
  "MCP Servers": "MCP 服务",
  "Usage & Cost": "用量统计",
  "Sessions": "会话",
  "Filter sessions… (⌘K)": "筛选会话… (⌘K)",
  "Search sessions": "搜索会话",
  "Clear filter": "清除筛选",
  "No sessions yet.": "暂无会话。",
  "No matching sessions": "没有匹配的会话",
  "Collapse sidebar": "收起侧栏",
  "Close sidebar": "关闭侧栏",
  "Show sessions": "展开侧栏",
  "Delete session": "删除会话",
  "Click again to delete": "再次点击确认删除",
  "Delete?": "确认删除?",
  "System": "跟随系统",
  "Light": "浅色",
  "Dark": "深色",
  "Chat": "对话",
  "Trace": "轨迹",
  "Context": "上下文",
  "reconnecting": "重连中",
  "Session": "会话",
  "Resume": "继续运行",
  "Dismiss": "关闭",
  "Load earlier": "加载更早记录",
  "Loading…": "加载中…",
  "No model selected": "未选择模型",
  "Configure model": "配置模型",
  "Send a message below to queue input and run the model, or queue multiple inputs before starting.": "在下方输入消息即可入队并运行模型，也可以先连续入队多条输入再启动。",
  "Enter send": "Enter 发送",
  "Shift+Enter newline": "Shift+Enter 换行",
  "Paste or drop media": "粘贴或拖入媒体",
  "Copy": "复制",
  "Copied": "已复制",
  "withdrawn": "已撤回",
  "queued": "已入队",
  "Withdraw": "撤回",
  "Thought process": "思考过程",
  "Thinking…": "思考中…",
  "chars": "字",
  "Code": "代码",
  "Trigger code": "触发脚本",
  "Arguments": "参数",
  "No arguments": "无参数",
  "Result": "结果",
  "No output": "无输出",
  "streaming": "生成中",
  "Working…": "处理中…",
  "Pausing after current step…": "等待当前步骤完成后暂停…",
  "Upstream retry": "上游重试",
  "Latest": "回到底部",
  "Conversation turns": "对话轮次导航",
  "Close preview": "关闭预览",
  "Click to enlarge": "点击放大",
  "Loading": "正在加载",
  "Usage": "用量",
  "Toggle line wrap": "切换自动换行",
  "All": "全部",
  "Expand all": "全部展开",
  "Collapse all": "全部折叠",
  "Refresh": "刷新",
  "Filter id, kind, text…": "筛选 id、类型、文本…",
  "Filter pos, item_id, text…": "筛选 pos、item_id、文本…",
  "No matching records": "没有匹配的记录",
  "No items recorded in this session yet.": "当前会话还没有任何 items 记录。",
  "Context window is currently empty.": "当前上下文窗口为空。",
  "Static developer instructions and tool definitions are prepended dynamically per call and not stored in context_items.": "静态开发者指令与工具定义在每次请求时动态前置，不写入 context_items。",
  "Payload JSON": "Payload JSON",
  "LIVE": "实时",
  "Context window": "上下文占用",
  "Message MyGo… (Enter to send, Shift+Enter for newline)": "发送消息给 MyGo…（Enter 发送，Shift+Enter 换行）",
  "Steer running session (queues for next step)…": "引导运行中的会话（将排队到下一步）…",
  "Add media": "添加图片或音视频",
  "Uploading…": "上传中…",
  "Steer": "引导",
  "Queue message for the next step": "将消息加入队列，等待下一步处理",
  "Pause": "暂停",
  "Pause after the current step finishes": "等待当前步骤结束后暂停",
  "Pausing…": "暂停中…",
  "Stop": "停止",
  "Stop the active run immediately": "立即终止当前运行",
  "Queue": "仅入队",
  "Add message to queue without starting a run": "将消息加入队列但暂不启动运行",
  "Send & Run": "发送并运行",
  "Sending…": "发送中…",
  "No model": "未选模型",
  "auto": "自动",
  "Session Model": "会话模型",
  "Reasoning Effort": "推理强度",
  "Drop image, audio, or video to attach": "拖放图片、音频或视频即可附件上传",
  "Inspect": "查看",
  "Hide": "收起",
  "Kill": "终止",
  "Stopping…": "停止中…",
  "Console tail": "控制台尾部",
  "calls": "次调用",
  "cache hit": "缓存命中",
  "Only image, audio, or video files are supported.": "仅支持图片、音频或视频文件。",
  "Message queued, but failed to start run:": "消息已入队，但启动运行失败：",
  "Models": "模型",
  "MCP": "MCP",
  "General": "通用",
  "Close settings": "关闭设置",
  "Default Selection": "默认模型配置",
  "Show archived": "显示已归档",
  "Default Model": "默认模型",
  "Default Effort": "默认推理强度",
  "None": "无",
  "Default": "默认",
  "Providers": "供应商",
  "+ Provider": "+ 供应商",
  "Name": "名称",
  "Base URL": "Base URL",
  "API Key": "API Key",
  "(leave blank to keep)": "（留空则保留原密钥）",
  "Clear stored key": "清除已存密钥",
  "Save": "保存",
  "Cancel": "取消",
  "Edit": "编辑",
  "Restore": "恢复",
  "Archive": "归档",
  "archived": "已归档",
  "key set": "已设密钥",
  "no key": "未设密钥",
  "No providers configured yet.": "暂未配置供应商。",
  "+ Model": "+ 模型",
  "Provider": "供应商",
  "Model Key": "Model Key",
  "Context Window": "上下文窗口",
  "Efforts (comma-separated)": "推理强度（逗号分隔）",
  "optional": "可选",
  "Pricing JSON (optional)": "定价 JSON（可选）",
  "No models configured yet.": "暂未配置模型。",
  "priced": "已设价格",
  "efforts": "强度",
  "Pricing must be valid JSON": "定价必须是合法 JSON",
  "+ Server": "+ 服务",
  "(immutable)": "（不可修改）",
  "Transport": "传输方式",
  "Description": "描述",
  "Timeout (ms)": "超时 (ms)",
  "URL (query parameters hidden)": "URL（查询参数已隐藏）",
  "Replace URL": "替换 URL",
  "URL": "URL",
  "Headers": "请求头 Headers",
  "Keep existing headers": "保留现有 Headers",
  "None (unchanged)": "无（未变更）",
  "Replace headers (JSON)": "替换 Headers (JSON)",
  "Clear headers": "清空 Headers",
  "OAuth 2.1": "OAuth 2.1",
  "Keep existing OAuth config": "保留现有 OAuth 配置",
  "Disabled (unchanged)": "未启用（未变更）",
  "Replace / enable OAuth config": "替换 / 启用 OAuth 配置",
  "Disable OAuth": "禁用 OAuth",
  "Enable OAuth 2.1": "启用 OAuth 2.1",
  "Headers JSON": "Headers JSON",
  "OAuth Config JSON (optional)": "OAuth 配置 JSON（可选）",
  "Command": "命令",
  "Working Dir (optional)": "工作目录（可选）",
  "Args": "参数 Args",
  "Keep existing": "保留现有",
  "Replace args": "替换参数",
  "Clear args": "清空参数",
  "Env": "环境变量 Env",
  "Keep existing env": "保留现有 Env",
  "Replace env (JSON)": "替换 Env (JSON)",
  "Clear env": "清空 Env",
  "Args (one per line)": "参数（每行一个）",
  "Env JSON": "Env JSON",
  "Tool Policy": "工具策略",
  "Allow all tools": "允许全部工具",
  "Deny all tools": "禁用全部工具",
  "Allow listed tools only": "仅允许白名单工具",
  "Allowed Tools (exact names)": "允许的工具（精确名称）",
  "Exact tool name…": "精确工具名…",
  "+ Add": "+ 添加",
  "Enabled": "已启用",
  "No MCP servers configured yet.": "暂未配置 MCP 服务。",
  "disabled": "已禁用",
  "tools: none": "工具: 禁用",
  "Test": "测试",
  "Testing…": "测试中…",
  "Login": "登录",
  "Logout": "注销",
  "Delete": "删除",
  "Confirm?": "确认删除?",
  "FAILED at": "失败于",
  "Authorize here:": "点击授权：",
  "Open OAuth Login": "打开 OAuth 登录页",
  "Paste redirect URL if remote…": "如为远程环境请粘贴回调 URL…",
  "Submit Callback": "提交回调",
  "All sessions": "全部会话",
  "Session ID…": "会话 ID…",
  "Total Cost": "总费用",
  "Calls": "调用次数",
  "Input": "输入",
  "cached": "命中",
  "Output": "输出",
  "No calls recorded for this filter.": "当前筛选条件下暂无调用记录。",
  "deleted": "已删除",
  "Started": "开始时间",
  "Load more": "加载更多",
  "Untitled": "未命名会话",
  "Appearance": "外观主题",
  "Theme preference across sessions": "跨会话保留的界面明暗偏好",
  "Language": "界面语言",
  "Interface display language": "切换 WebUI 显示语言",
  "Auto": "自动",
  "API Documentation": "API 文档",
  "Scalar interactive reference & OpenAPI schema (loopback only)": "Scalar 交互式接口文档与 OpenAPI Schema（仅限本机访问）",
  "Authentication": "身份凭据",
  "Clear stored Bearer token from this browser": "清除浏览器本地保存的 Bearer Token",
  "Sign out": "退出登录",
  "active codemode": "个活跃 codemode",
  "task": "任务",
  "tasks": "任务",
  "Cluster": "集群",
  "Sync across cluster": "全集群同步",
  "shared": "已同步",
  "Disabling cluster sync detaches this server into a new local copy and removes the old shared server from other nodes.": "关闭全集群同步会把当前服务分离为新的本地副本，并从其他节点移除旧的共享条目。",
  "Cluster & Sync": "集群与同步",
  "This Node": "当前节点",
  "Node ID": "节点 ID",
  "Local-only": "仅本地模式",
  "Sync Now": "立即同步",
  "Syncing…": "同步中…",
  "Peers & Join": "节点与加入",
  "Generate Invite": "生成邀请码",
  "Invite code": "邀请码",
  "Join Cluster": "加入集群",
  "Paste invite code…": "粘贴邀请码…",
  "Join": "加入",
  "Joining…": "加入中…",
  "No peers registered yet. Generate an invite or join an existing cluster.": "暂无已登记的对等节点。可生成邀请码，或加入已有集群。",
  "online": "在线",
  "offline": "离线",
  "Remove": "移除",
  "Sync Conflicts": "同步冲突",
  "No sync conflicts.": "当前无同步冲突。",
  "Local": "本地",
  "Keep Local (Force Push)": "保留本地（强制推送）",
  "Use Cluster (Overwrite)": "使用集群（覆盖本地）",
  "Resolving…": "处理中…",
  "Synced:": "同步完成：",
  "peers queried": "个节点已询问",
  "streams updated": "条流已更新",
  "conflicts": "条冲突",
  "Joined cluster via": "已通过节点加入集群：",
  "No available models": "无可用模型",
  "This node already has cluster peers. Switching cluster keeps local data and replaces peer membership.": "当前节点已加入其他集群。切换集群会保留本地数据并替换对等节点成员关系。",
  "Force Switch Cluster": "强制切换集群",
  "Local ID": "本地 ID",
  "Add an active model before creating a session.": "请先添加可用模型后再创建会话。",
} as const;

export type Msg = keyof typeof ZH;

export const I18nContext = createContext<Locale>("zh");

export function tr(locale: Locale, s: Msg): string {
  return locale === "zh" ? ZH[s] : s;
}

export function useI18n(): { locale: Locale; t: (s: Msg) => string } {
  const locale = useContext(I18nContext);
  return { locale, t: (s: Msg) => tr(locale, s) };
}

const STATUS_ZH: Record<string, string> = {
  idle: "空闲",
  running: "运行中",
  pausing: "暂停中",
  paused: "已暂停",
  succeeded: "成功",
  failed: "失败",
  cancelled: "已取消",
  interrupted: "已中断",
  expired: "已过期",
  done: "完成",
  background: "后台运行",
  in_flight: "进行中",
  woke: "已唤醒",
  error: "错误",
};

export function statusText(status: string, locale: Locale): string {
  return locale === "zh" ? STATUS_ZH[status] || status : status;
}

export function formatRelTimeLoc(ms: number | null | undefined, locale: Locale): string {
  if (!ms) return "";
  const diff = Math.max(0, Math.floor((Date.now() - ms) / 1000));
  if (locale === "zh") {
    if (diff < 60) return "刚刚";
    if (diff < 3600) return `${Math.floor(diff / 60)}分`;
    if (diff < 86400) return `${Math.floor(diff / 3600)}时`;
    return `${Math.floor(diff / 86400)}天`;
  }
  if (diff < 60) return "now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h`;
  return `${Math.floor(diff / 86400)}d`;
}

export function formatShortTimeLoc(ms: number | null | undefined, locale: Locale): string {
  if (!ms) return "";
  try {
    const d = new Date(ms);
    const hm = `${String(d.getHours()).padStart(2, "0")}:${String(d.getMinutes()).padStart(2, "0")}`;
    if (locale === "zh") return `${d.getMonth() + 1}月${d.getDate()}日 ${hm}`;
    return `${d.toLocaleDateString("en-US", { month: "short", day: "numeric" })} ${hm}`;
  } catch {
    return "";
  }
}

export function dateGroupLabelLoc(ms: number | null | undefined, locale: Locale): string {
  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  if (!ms || ms < startOfToday - 7 * 86400_000) return locale === "zh" ? "更早" : "Older";
  if (ms >= startOfToday) return locale === "zh" ? "今天" : "Today";
  if (ms >= startOfToday - 86400_000) return locale === "zh" ? "昨天" : "Yesterday";
  return locale === "zh" ? "近 7 天" : "Previous 7 days";
}

export function utf8ByteLength(text: string): number {
  let bytes = 0;
  for (let i = 0; i < text.length; i += 1) {
    const code = text.charCodeAt(i);
    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (code >= 0xd800 && code <= 0xdbff && i + 1 < text.length) {
      const next = text.charCodeAt(i + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        i += 1;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

export function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}
