import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";

const here = dirname(fileURLToPath(import.meta.url));
const outDir = resolve(here, "../src/webui");
mkdirSync(outDir, { recursive: true });

const result = await Bun.build({
  entrypoints: [resolve(here, "src/main.tsx")],
  minify: true,
  target: "browser",
  format: "esm",
  jsx: {
    runtime: "automatic",
    importSource: "preact",
  },
});

if (!result.success) {
  for (const log of result.logs) console.error(log);
  process.exit(1);
}

const jsRaw = Buffer.from(await result.outputs[0].arrayBuffer());
const jsGz = gzipSync(jsRaw, { level: 9 });
writeFileSync(resolve(outDir, "app.js.gz"), jsGz);

function minifyCss(css) {
  let out = "";
  let i = 0;
  let quote = "";
  let inComment = false;
  while (i < css.length) {
    const ch = css[i];
    const next = css[i + 1];
    if (inComment) {
      if (ch === "*" && next === "/") {
        inComment = false;
        i += 2;
      } else {
        i += 1;
      }
      continue;
    }
    if (quote) {
      out += ch;
      if (ch === "\\" && i + 1 < css.length) {
        out += next;
        i += 2;
        continue;
      }
      if (ch === quote) quote = "";
      i += 1;
      continue;
    }
    if (ch === "/" && next === "*") {
      inComment = true;
      i += 2;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      out += ch;
      i += 1;
      continue;
    }
    if (/\s/.test(ch)) {
      while (i < css.length && /\s/.test(css[i])) i += 1;
      const prev = out[out.length - 1] || "";
      const ahead = css[i] || "";
      if (
        prev &&
        ahead &&
        !"{}:;,>~(".includes(prev) &&
        !"}:;,{>~)".includes(ahead)
      ) {
        out += " ";
      }
      continue;
    }
    if (ch === ";" && css[i + 1] === "}") {
      i += 1;
      continue;
    }
    out += ch;
    i += 1;
  }
  return out.trim();
}

const cssSrc = readFileSync(resolve(here, "src/style.css"), "utf8");
const cssRaw = Buffer.from(minifyCss(cssSrc), "utf8");
const cssGz = gzipSync(cssRaw, { level: 9 });
writeFileSync(resolve(outDir, "app.css.gz"), cssGz);

const html = readFileSync(resolve(here, "src/index.html"), "utf8")
  .replace(/>\s+</g, "><")
  .trim();
writeFileSync(resolve(outDir, "index.html"), html, "utf8");

console.log(
  `Built webui -> index.html: ${html.length} B, app.js: ${jsRaw.length} B (${jsGz.length} B gz), app.css: ${cssRaw.length} B (${cssGz.length} B gz), total embedded: ${html.length + jsGz.length + cssGz.length} B`
);
