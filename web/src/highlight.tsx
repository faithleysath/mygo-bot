import type { ComponentChildren } from "preact";

const LANG_ALIASES: Record<string, string> = {
  js: "js",
  jsx: "js",
  mjs: "js",
  cjs: "js",
  javascript: "js",
  ts: "js",
  tsx: "js",
  typescript: "js",
  rs: "rust",
  rust: "rust",
  py: "python",
  python: "python",
  sh: "sh",
  bash: "sh",
  zsh: "sh",
  shell: "sh",
  sql: "sql",
  go: "go",
  golang: "go",
  css: "css",
  scss: "css",
  html: "xml",
  xml: "xml",
  svg: "xml",
  yaml: "yaml",
  yml: "yaml",
  toml: "toml",
  json: "json",
  jsonc: "json",
  diff: "diff",
  patch: "diff",
};

const KW_COMMON = new Set([
  "if",
  "else",
  "for",
  "while",
  "do",
  "switch",
  "case",
  "default",
  "break",
  "continue",
  "return",
  "in",
  "as",
  "match",
  "try",
  "catch",
  "finally",
  "throw",
  "import",
  "export",
  "from",
  "type",
  "enum",
  "struct",
  "impl",
  "trait",
  "fn",
  "func",
  "function",
  "def",
  "class",
  "interface",
  "const",
  "let",
  "var",
  "mut",
  "pub",
  "async",
  "await",
  "yield",
  "new",
  "typeof",
  "use",
  "mod",
  "where",
  "ref",
  "self",
  "super",
  "select",
  "from",
  "where",
  "join",
  "on",
  "and",
  "or",
  "not",
  "insert",
  "into",
  "values",
  "update",
  "set",
  "delete",
  "create",
  "table",
  "alter",
  "drop",
  "order",
  "by",
  "group",
  "limit",
  "then",
  "fi",
  "elif",
  "done",
  "esac",
  "local",
  "then",
  "elif",
  "lambda",
  "with",
  "pass",
  "raise",
  "except",
  "package",
  "go",
  "defer",
  "chan",
  "range",
]);

const LIT_SET = new Set([
  "true",
  "false",
  "null",
  "undefined",
  "NaN",
  "Infinity",
  "None",
  "True",
  "False",
  "Some",
  "Ok",
  "Err",
  "this",
  "Self",
]);

function renderDiff(code: string): ComponentChildren[] {
  const lines = code.split("\n");
  const out: ComponentChildren[] = [];
  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const nl = i < lines.length - 1 ? "\n" : "";
    let cls = "";
    if (line.startsWith("@@")) cls = "tok-kw";
    else if (line.startsWith("+++") || line.startsWith("---") || line.startsWith("diff ")) cls = "tok-com";
    else if (line.startsWith("+")) cls = "tok-add";
    else if (line.startsWith("-")) cls = "tok-del";
    if (cls) out.push(<span key={i} class={cls}>{line}{nl}</span>);
    else out.push(line + nl);
  }
  return out;
}

function renderJson(code: string): ComponentChildren[] {
  const out: ComponentChildren[] = [];
  const re =
    /(\/\/[^\n]*|\/\*[\s\S]*?\*\/)|("(?:\\.|[^"\\\n])*")|(-?\b\d+(?:\.\d+)?(?:[eE][+-]?\d+)?\b)|\b(true|false|null)\b/g;
  let last = 0;
  let idx = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(code)) !== null) {
    if (m.index > last) out.push(code.slice(last, m.index));
    const tok = m[0];
    let cls = "";
    if (m[1]) cls = "tok-com";
    else if (m[2]) {
      let j = re.lastIndex;
      while (j < code.length && (code[j] === " " || code[j] === "\t")) j++;
      cls = code[j] === ":" ? "tok-key" : "tok-str";
    } else if (m[3]) cls = "tok-num";
    else if (m[4]) cls = "tok-lit";
    out.push(<span key={idx++} class={cls}>{tok}</span>);
    last = re.lastIndex;
  }
  if (last < code.length) out.push(code.slice(last));
  return out;
}

function renderCode(code: string, family: string): ComponentChildren[] {
  if (family === "xml") {
    const out: ComponentChildren[] = [];
    const re = /(<!--[\s\S]*?-->)|(<\/?[A-Za-z0-9:_-]+)|("(?:\\.|[^"\\\n])*"|'(?:\\.|[^'\\\n])*')|(\b[A-Za-z0-9:_-]+(?==))/g;
    let last = 0;
    let idx = 0;
    let m: RegExpExecArray | null;
    while ((m = re.exec(code)) !== null) {
      if (m.index > last) out.push(code.slice(last, m.index));
      const cls = m[1] ? "tok-com" : m[2] ? "tok-kw" : m[3] ? "tok-str" : "tok-key";
      out.push(<span key={idx++} class={cls}>{m[0]}</span>);
      last = re.lastIndex;
    }
    if (last < code.length) out.push(code.slice(last));
    return out;
  }

  const hashCom = family === "python" || family === "sh" || family === "yaml" || family === "toml";
  const dashCom = family === "sql";
  const strPat =
    family === "rust"
      ? `"(?:\\\\.|[^"\\\\])*"|'(?:\\\\.|[^'\\\\\\n])'`
      : `"(?:\\\\.|[^"\\\\])*"|'(?:\\\\.|[^'\\\\])*'|\`(?:\\\\.|[^\`\\\\])*\``;
  const comPat = hashCom
    ? `#[^\\n]*`
    : dashCom
      ? `--[^\\n]*|/\\*[\\s\\S]*?\\*/`
      : `//[^\\n]*|/\\*[\\s\\S]*?\\*/`;
  const re = new RegExp(
    `(${comPat})|(${strPat})|(\\b0[xXbBoO][0-9a-fA-F_]+\\b|\\b\\d[\\d_]*(?:\\.[\\d_]+)?(?:[eE][+-]?\\d+)?\\b)|(\\b[A-Za-z_$][A-Za-z0-9_$]*\\b)`,
    "g"
  );

  const out: ComponentChildren[] = [];
  let last = 0;
  let idx = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(code)) !== null) {
    if (m.index > last) out.push(code.slice(last, m.index));
    const tok = m[0];
    let cls = "";
    if (m[1]) cls = "tok-com";
    else if (m[2]) cls = "tok-str";
    else if (m[3]) cls = "tok-num";
    else if (m[4]) {
      const low = family === "sql" ? tok.toLowerCase() : tok;
      if (LIT_SET.has(tok)) cls = "tok-lit";
      else if (KW_COMMON.has(low)) cls = "tok-kw";
      else if ((family === "yaml" || family === "toml" || family === "css") && code[re.lastIndex] === ":") {
        cls = "tok-key";
      }
    }
    if (cls) out.push(<span key={idx++} class={cls}>{tok}</span>);
    else out.push(tok);
    last = re.lastIndex;
  }
  if (last < code.length) out.push(code.slice(last));
  return out;
}

export function HighlightedCode({ code, lang }: { code: string; lang?: string }) {
  if (!code || code.length > 24000) return <>{code}</>;
  const family = LANG_ALIASES[(lang || "").toLowerCase()];
  if (!family) return <>{code}</>;
  if (family === "diff") return <>{renderDiff(code)}</>;
  if (family === "json") return <>{renderJson(code)}</>;
  return <>{renderCode(code, family)}</>;
}
