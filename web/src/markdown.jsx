import { useState } from "preact/hooks";

function safeUrl(raw) {
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

function renderInline(text, keyPrefix) {
  const nodes = [];
  const pattern = /(`[^`\n]+`)|(\*\*[^*\n]+?\*\*)|(~~[^~\n]+?~~)|(\*[^*\n]+?\*)|(\[[^\]\n]+\]\([^)\s]+\))/g;
  let last = 0;
  let idx = 0;
  let m;
  while ((m = pattern.exec(text)) !== null) {
    if (m.index > last) nodes.push(text.slice(last, m.index));
    const tok = m[0];
    const k = `${keyPrefix}-${idx++}`;
    if (m[1]) {
      nodes.push(<code key={k} class="md-inline-code">{tok.slice(1, -1)}</code>);
    } else if (m[2]) {
      nodes.push(<strong key={k}>{renderInline(tok.slice(2, -2), k)}</strong>);
    } else if (m[3]) {
      nodes.push(<del key={k}>{renderInline(tok.slice(2, -2), k)}</del>);
    } else if (m[4]) {
      nodes.push(<em key={k}>{renderInline(tok.slice(1, -1), k)}</em>);
    } else if (m[5]) {
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

function CodeBlock({ lang, code }) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1400);
    } catch {}
  };
  return (
    <div class="md-code-wrap">
      <div class="md-code-head">
        <span>{lang || "text"}</span>
        <button type="button" class="btn-ghost btn-xs" onClick={copy}>
          {copied ? "Copied" : "Copy"}
        </button>
      </div>
      <pre class="md-pre"><code>{code}</code></pre>
    </div>
  );
}

export function Markdown({ text }) {
  if (!text) return null;
  const lines = String(text).replace(/\r\n?/g, "\n").split("\n");
  const blocks = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i];
    if (!line.trim()) {
      i += 1;
      continue;
    }

    const fence = line.match(/^```([a-zA-Z0-9_+-]*)\s*$/);
    if (fence) {
      const lang = fence[1];
      const buf = [];
      i += 1;
      while (i < lines.length && !lines[i].match(/^```\s*$/)) {
        buf.push(lines[i]);
        i += 1;
      }
      if (i < lines.length) i += 1;
      blocks.push(<CodeBlock key={`b${i}`} lang={lang} code={buf.join("\n")} />);
      continue;
    }

    const heading = line.match(/^(#{1,6})\s+(.*)$/);
    if (heading) {
      const Tag = `h${heading[1].length}`;
      blocks.push(<Tag key={`b${i}`}>{renderInline(heading[2], `h${i}`)}</Tag>);
      i += 1;
      continue;
    }

    if (line.match(/^(\*\s*\*\s*\*|-{3,}|_{3,})\s*$/)) {
      blocks.push(<hr key={`b${i}`} />);
      i += 1;
      continue;
    }

    if (line.startsWith(">")) {
      const q = [];
      while (i < lines.length && lines[i].startsWith(">")) {
        q.push(lines[i].replace(/^>\s?/, ""));
        i += 1;
      }
      blocks.push(
        <blockquote key={`b${i}`}>
          <Markdown text={q.join("\n")} />
        </blockquote>
      );
      continue;
    }

    if (
      line.includes("|") &&
      i + 1 < lines.length &&
      lines[i + 1].match(/^\s*\|?(\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$/)
    ) {
      const parseRow = (r) =>
        r
          .trim()
          .replace(/^\|/, "")
          .replace(/\|$/, "")
          .split("|")
          .map((c) => c.trim());
      const head = parseRow(line);
      i += 2;
      const rows = [];
      while (i < lines.length && lines[i].includes("|") && lines[i].trim()) {
        rows.push(parseRow(lines[i]));
        i += 1;
      }
      blocks.push(
        <div key={`b${i}`} class="md-table-wrap">
          <table>
            <thead>
              <tr>
                {head.map((c, j) => (
                  <th key={j}>{renderInline(c, `th${i}-${j}`)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri}>
                  {head.map((_, ci) => (
                    <td key={ci}>{renderInline(r[ci] || "", `td${i}-${ri}-${ci}`)}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      );
      continue;
    }

    const ulMatch = line.match(/^\s*[-*+]\s+(.*)$/);
    const olMatch = line.match(/^\s*\d+\.\s+(.*)$/);
    if (ulMatch || olMatch) {
      const ordered = Boolean(olMatch);
      const Tag = ordered ? "ol" : "ul";
      const items = [];
      while (i < lines.length) {
        const m = ordered
          ? lines[i].match(/^\s*\d+\.\s+(.*)$/)
          : lines[i].match(/^\s*[-*+]\s+(.*)$/);
        if (!m) break;
        items.push(m[1]);
        i += 1;
      }
      blocks.push(
        <Tag key={`b${i}`}>
          {items.map((item, j) => (
            <li key={j}>{renderInline(item, `li${i}-${j}`)}</li>
          ))}
        </Tag>
      );
      continue;
    }

    const para = [];
    while (
      i < lines.length &&
      lines[i].trim() &&
      !lines[i].match(/^(```|#{1,6}\s|>|\s*[-*+]\s|\s*\d+\.\s|(\*\s*\*\s*\*|-{3,}|_{3,})\s*$)/)
    ) {
      para.push(lines[i]);
      i += 1;
    }
    blocks.push(
      <p key={`b${i}`}>
        {para.map((p, pi) => (
          <span key={pi}>
            {pi > 0 && <br />}
            {renderInline(p, `p${i}-${pi}`)}
          </span>
        ))}
      </p>
    );
  }

  return <div class="md">{blocks}</div>;
}
