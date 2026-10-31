// stills.mjs renders one still of every ascii-magic UI piece as an SVG, so
// docs/SCREENS.md can show each piece beside the kaas command it maps to.
//
// Each piece runs from 0 s to the still's time in frame steps (some pieces
// keep state between frames), and the last frame is drawn on the same grid
// internal/termshot uses for kaas's own screenshots.
//
// Usage: node stills.mjs <out-dir> [seconds]
import { mkdirSync, readdirSync, writeFileSync, copyFileSync } from "node:fs";
import { join } from "node:path";
import { createRequire } from "node:module";

const [outDir, secs = "4"] = process.argv.slice(2);
if (!outDir) {
  console.error("usage: node stills.mjs <out-dir> [seconds]");
  process.exit(2);
}
mkdirSync(outDir, { recursive: true });

const require = createRequire(import.meta.url);
const pkgDir = join(require.resolve("ascii-magic-art/package.json"), "..");

const CELL_W = 8.4, CELL_H = 18, PAD_X = 16, PAD_TOP = 44, PAD_BOT = 16;
const BG = "#0d1117", FG = "#e6edf3", CHROME = "#30363d";
const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

function svg(meta, text, color) {
  const rows = text.split("\n");
  const keys = (color || "").split("\n");
  const cols = Math.max(meta.cols ?? 0, ...rows.map((r) => [...r].length));
  const w = PAD_X * 2 + cols * CELL_W, h = PAD_TOP + rows.length * CELL_H + PAD_BOT;
  const pal = meta.palette ?? {};
  const out = [
    `<svg xmlns="http://www.w3.org/2000/svg" width="${w.toFixed(0)}" height="${h.toFixed(0)}" viewBox="0 0 ${w.toFixed(0)} ${h.toFixed(0)}" role="img" aria-label="${esc(meta.name)}">`,
    `<rect width="100%" height="100%" rx="10" fill="${BG}" stroke="${CHROME}"/>`,
    ...["#ff5f57", "#febc2e", "#28c840"].map((c, i) => `<circle cx="${22 + i * 20}" cy="18" r="6" fill="${c}"/>`),
    `<text x="${(w / 2).toFixed(0)}" y="22" fill="#8b949e" font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,monospace" font-size="12" text-anchor="middle">${esc(meta.name)}</text>`,
    `<g font-family="ui-monospace,SFMono-Regular,Menlo,Consolas,'DejaVu Sans Mono',monospace" font-size="14" xml:space="preserve">`,
  ];
  rows.forEach((row, y) => {
    const chars = [...row], ks = [...(keys[y] ?? "")];
    const base = PAD_TOP + y * CELL_H + CELL_H - 5;
    // Group runs of one colour, then place each word at its own column and
    // pin it to its cells, as termshot does, so the grid never drifts.
    let x = 0;
    while (x < chars.length) {
      const k = ks[x] ?? " ";
      let end = x;
      while (end < chars.length && (ks[end] ?? " ") === k) end++;
      const fill = pal[k] ?? FG;
      let col = x;
      for (const word of chars.slice(x, end).join("").split(/(?<= )/)) {
        const wc = [...word];
        const trim = word.trimEnd();
        if (trim) {
          const tw = [...trim].length;
          out.push(`<text x="${(PAD_X + col * CELL_W).toFixed(1)}" y="${base.toFixed(1)}" fill="${fill}" textLength="${(tw * CELL_W).toFixed(1)}" lengthAdjust="spacingAndGlyphs">${esc(trim)}</text>`);
        }
        col += wc.length;
      }
      x = end;
    }
  });
  out.push("</g>", "</svg>", "");
  return out.join("\n");
}

const index = [];
for (const file of readdirSync(join(pkgDir, "pieces")).filter((f) => f.endsWith(".js")).sort()) {
  const mod = await import(join(pkgDir, "pieces", file));
  const { meta } = mod;
  if (meta?.category !== "screens") continue;
  const fps = meta.fps ?? 20;
  const draw = mod.default();
  let f;
  for (let i = 0, n = Math.round(Number(secs) * fps); i <= n; i++) f = draw(i / fps);
  const [text, color] = typeof f === "string" ? [f, ""] : [f.text, f.color];
  writeFileSync(join(outDir, `${meta.slug}.svg`), svg(meta, text, color));
  index.push({ slug: meta.slug, name: meta.name, note: meta.note });
}
writeFileSync(join(outDir, "index.json"), JSON.stringify(index, null, 1) + "\n");
copyFileSync(join(pkgDir, "LICENSE"), join(outDir, "LICENSE.ascii-magic-art"));
console.log(`${index.length} UI pieces → ${outDir}`);
