import type { ComponentChildren } from "preact";
import { useEffect, useRef, useState } from "preact/hooks";
import { copyText } from "./api.ts";
import { HighlightedCode } from "./highlight.tsx";
import { useI18n } from "./i18n.ts";
import { IconCheck, IconCopy, IconWrap } from "./Icons.tsx";

const TABLE_SEP_RE = /^\s*\|?(\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$/;
const HR_RE = /^(\*\s*\*\s*\*|-{3,}|_{3,})\s*$/;
const FENCE_OPEN_RE = /^```(.*)$/;
const HEADING_RE = /^(#{1,6})(?:\s+(.*)|\s*)$/;
const UL_RE = /^\s*[-*+]\s+(.*)$/;
const OL_RE = /^\s*\d+\.\s+(.*)$/;
const TASK_RE = /^\[([ xX])\]\s+(.*)$/;

type HeadingTag = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

function isTableStart(line: string, nextLine: string | undefined): boolean {
  return Boolean(line && nextLine && line.includes("|") && TABLE_SEP_RE.test(nextLine));
}

function isBlockStart(line: string, nextLine: string | undefined): boolean {
  if (!line.trim()) return true;
  return (
    FENCE_OPEN_RE.test(line) ||
    HEADING_RE.test(line) ||
    HR_RE.test(line) ||
    line.startsWith(">") ||
    UL_RE.test(line) ||
    OL_RE.test(line) ||
    isTableStart(line, nextLine)
  );
}

function safeUrl(raw: string): string | null {
  try {
    const u = new URL(raw, "https://invalid.local");
    if (["http:", "https:", "mailto:"].includes(u.protocol)) {
      return raw.startsWith("http://") || raw.startsWith("https://") || raw.startsWith("mailto:")
        ? raw
        : null;
    }
  } catch {}
  return null;
}

function renderInline(text: string, keyPrefix: string): ComponentChildren[] {
  const nodes: ComponentChildren[] = [];
  const pattern =
    /(`[^`\n]+`)|(\*\*[^*\n]+?\*\*)|(__[^_\n]+?__)|(~~[^~\n]+?~~)|(\*[^*\n]+?\*)|((?<![A-Za-z0-9_])_[^\s_](?:[^_\n]*[^\s_])?_(?![A-Za-z0-9_]))|(\[[^\]\n]+\]\((?:[^()\s]+|\([^()\s]*\))+\))/g;
  let last = 0;
  let idx = 0;
  let m: RegExpExecArray | null;
  while ((m = pattern.exec(text)) !== null) {
    if (m.index > last) nodes.push(text.slice(last, m.index));
    const tok = m[0];
    const k = `${keyPrefix}-${idx++}`;
    if (m[1]) {
      nodes.push(<code key={k} class="md-inline-code">{tok.slice(1, -1)}</code>);
    } else if (m[2] || m[3]) {
      nodes.push(<strong key={k}>{renderInline(tok.slice(2, -2), k)}</strong>);
    } else if (m[4]) {
      nodes.push(<del key={k}>{renderInline(tok.slice(2, -2), k)}</del>);
    } else if (m[5] || m[6]) {
      nodes.push(<em key={k}>{renderInline(tok.slice(1, -1), k)}</em>);
    } else if (m[7]) {
      const split = tok.indexOf("](");
      const label = tok.slice(1, split);
      const href = safeUrl(tok.slice(split + 2, -1));
      if (href) {
        nodes.push(
          <a key={k} href={href} target="_blank" rel="noopener noreferrer">
            {renderInline(label, k)}
          </a>
        );
      } else {
        nodes.push(tok);
      }
    }
    last = pattern.lastIndex;
  }
  if (last < text.length) nodes.push(text.slice(last));
  return nodes;
}

function CodeBlock({ lang, code }: { lang: string; code: string }) {
  const { t } = useI18n();
  const [copied, setCopied] = useState(false);
  const [wrap, setWrap] = useState(false);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timerRef.current !== null) clearTimeout(timerRef.current);
    };
  }, []);

  const copy = async () => {
    if (await copyText(code)) {
      if (timerRef.current !== null) clearTimeout(timerRef.current);
      setCopied(true);
      timerRef.current = setTimeout(() => {
        setCopied(false);
        timerRef.current = null;
      }, 1400);
    }
  };

  return (
    <div class="md-code-wrap">
      <div class="md-code-head">
        <span class="md-code-lang mono">{lang || t("codeBlock")}</span>
        <div class="row gap-1">
          <button
            type="button"
            class={`copy-btn ${wrap ? "active" : ""}`}
            title={t("toggleWrap")}
            aria-label={t("toggleWrap")}
            onClick={() => setWrap((w) => !w)}
          >
            <IconWrap size={12} />
          </button>
          <button type="button" class="copy-btn" onClick={copy}>
            {copied ? <IconCheck size={12} /> : <IconCopy size={12} />}
            <span>{copied ? t("copied") : t("copy")}</span>
          </button>
        </div>
      </div>
      <pre class={`md-pre ${wrap ? "wrap" : ""}`}>
        <code><HighlightedCode code={code} lang={lang} /></code>
      </pre>
    </div>
  );
}

export function Markdown({ text }: { text: string }) {
  if (!text) return null;
  const lines = String(text).replace(/\r\n?/g, "\n").split("\n");
  const blocks: ComponentChildren[] = [];
  let i = 0;

  while (i < lines.length) {
    const start = i;
    const line = lines[i];
    if (!line.trim()) {
      i += 1;
      continue;
    }

    const fence = line.match(FENCE_OPEN_RE);
    if (fence) {
      const lang = fence[1].trim().split(/\s+/)[0] || "";
      const buf: string[] = [];
      i += 1;
      while (i < lines.length && !lines[i].match(/^```\s*$/)) {
        buf.push(lines[i]);
        i += 1;
      }
      if (i < lines.length) i += 1;
      blocks.push(<CodeBlock key={`b${start}`} lang={lang} code={buf.join("\n")} />);
      continue;
    }

    const heading = line.match(HEADING_RE);
    if (heading) {
      const Tag = `h${heading[1].length}` as HeadingTag;
      blocks.push(<Tag key={`b${start}`}>{renderInline(heading[2] || "", `h${start}`)}</Tag>);
      i += 1;
      continue;
    }

    if (HR_RE.test(line)) {
      blocks.push(<hr key={`b${start}`} />);
      i += 1;
      continue;
    }

    if (line.startsWith(">")) {
      const q: string[] = [];
      while (i < lines.length && lines[i].startsWith(">")) {
        q.push(lines[i].replace(/^>\s?/, ""));
        i += 1;
      }
      blocks.push(
        <blockquote key={`b${start}`}>
          <Markdown text={q.join("\n")} />
        </blockquote>
      );
      continue;
    }

    if (isTableStart(line, lines[i + 1])) {
      const parseRow = (r: string): string[] =>
        r
          .trim()
          .replace(/^\|/, "")
          .replace(/\|$/, "")
          .split("|")
          .map((c) => c.trim());
      const head = parseRow(line);
      i += 2;
      const rows: string[][] = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim()) {
        rows.push(parseRow(lines[i]));
        i += 1;
      }
      blocks.push(
        <div key={`b${start}`} class="md-table-wrap">
          <table>
            <thead>
              <tr>
                {head.map((c, j) => (
                  <th key={j}>{renderInline(c, `th${start}-${j}`)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri}>
                  {head.map((_, ci) => (
                    <td key={ci}>{renderInline(r[ci] || "", `td${start}-${ri}-${ci}`)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
      continue;
    }

    const ulMatch = line.match(UL_RE);
    const olMatch = line.match(OL_RE);
    if (ulMatch || olMatch) {
      const ordered = Boolean(olMatch);
      const Tag = ordered ? "ol" : "ul";
      const items: string[] = [];
      while (i < lines.length) {
        const m = ordered ? lines[i].match(OL_RE) : lines[i].match(UL_RE);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      const hasTask = !ordered && items.some((it) => TASK_RE.test(it));
      blocks.push(
        <Tag key={`b${start}`} class={hasTask ? "md-task-list" : undefined}>
          {items.map((item, j) => {
            const tm = !ordered ? item.match(TASK_RE) : null;
            if (tm) {
              const checked = tm[1].toLowerCase() === "x";
              return (
                <li key={j} class="md-task-item">
                  <span class={`md-checkbox ${checked ? "checked" : ""}`} aria-hidden="true">
                    {checked && <IconCheck size={11} />}
                  </span>
                  <span>{renderInline(tm[2], `li${start}-${j}`)}</span>
                </li>
              );
            }
            return <li key={j}>{renderInline(item, `li${start}-${j}`)}</li>;
          })}
        </Tag>
      );
      continue;
    }

    const para: string[] = [];
    do {
      para.push(lines[i]);
      i += 1;
    } while (i < lines.length && !isBlockStart(lines[i], lines[i + 1]));

    blocks.push(
      <p key={`b${start}`}>
        {para.map((p, pi) => (
          <span key={pi}>
            {pi > 0 && <br />}
            {renderInline(p, `p${start}-${pi}`)}
          </span>
        ))}
      </p>
    );
  }

  return <div class="md">{blocks}</div>;
}
