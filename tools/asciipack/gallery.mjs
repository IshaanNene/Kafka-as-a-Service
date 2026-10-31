// gallery.mjs writes docs/SCREENS.md: every ascii-magic UI piece as an
// image, with the kaas command that uses it, read from design 0002 so the
// gallery and the mapping can't drift apart. Built screens sit beside the
// real kaas screenshot of the same command.
//
// Usage: node gallery.mjs <docs-dir>   (after stills.mjs <docs-dir>/img/ui)
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [docs] = process.argv.slice(2);
if (!docs) {
  console.error("usage: node gallery.mjs <docs-dir>");
  process.exit(2);
}
const pieces = JSON.parse(readFileSync(join(docs, "img/ui/index.json"), "utf8"));
const bySlug = new Map(pieces.map((p) => [p.slug, p]));
const design = readFileSync(join(docs, "design/0002-command-screens.md"), "utf8");

// The kaas screenshot (docs/img, from `task screenshots`) for each built piece.
const SHOTS = {
  "command-palette": "help-kaas-bench-produce",
  "terminal-session": "help-kaas",
  "boot-sequence": "connect",
  "page-500": "unreachable",
  "page-404": "not-found-topic",
  "form-validation-errors": "invalid-flag",
  "toast-stack": "watch",
  "benchmark-table": "bench",
  "split-pane-session": "tui-topics",
  "data-table": "snapshot",
  "file-tree": "topics-tree",
  "disk-partitioner": "topics-describe",
  "process-task-manager": "groups",
};

// Walk the design doc's tables. A row's piece cells are the ones naming UI
// slugs; the first cell is the command when it isn't itself a piece.
const uses = new Map(); // slug -> [{section, who, what, status}]
const order = [];       // [section, [slug]] in the doc's order
let section = "";
for (const line of design.split("\n")) {
  if (line.startsWith("## ")) {
    section = line.slice(3).trim();
    continue;
  }
  if (!line.startsWith("|") || /^\|[-| ]+\|$/.test(line)) continue;
  const cells = line.split("|").slice(1, -1).map((c) => c.trim());
  const slugsIn = (c) => c.split(",").map((s) => s.replace(/\(.*?\)|`.*?`/g, "").trim()).filter((s) => bySlug.has(s));
  const pieceCol = cells.findIndex((c) => slugsIn(c).length > 0);
  if (pieceCol < 0) continue;
  const who = pieceCol > 0 ? cells[0] : "";
  const rest = cells.slice(pieceCol + 1);
  const status = rest.length > 1 ? rest[rest.length - 1] : (section.startsWith("Unused") ? "unused" : "");
  const what = rest.length > 1 ? rest.slice(0, -1).join(" · ") : rest[0] ?? "";
  for (const slug of slugsIn(cells[pieceCol])) {
    if (!uses.has(slug)) {
      uses.set(slug, []);
      let group = order.find(([s]) => s === section);
      if (!group) order.push((group = [section, []]));
      group[1].push(slug);
    }
    uses.get(slug).push({ who, what, status });
  }
}

const anchor = (name) => name.toLowerCase().replace(/[^a-z0-9 -]/g, "").replace(/ /g, "-");
const statusOf = (slug) => {
  const s = uses.get(slug).map((u) => u.status.replace(/\*/g, "")).find(Boolean) ?? "";
  return SHOTS[slug] ? "built" : s.split(" ")[0];
};

const out = [
  "# Screen gallery: all 79 UI pieces",
  "",
  "Every `kaas` command draws its own screen, and each screen is a Go port of one piece from ascii-magic's **UI** category, filled with live cluster data. This page shows all 79 pieces, the command each one belongs to, and, where the screen is built, the real kaas screen beside the piece it came from.",
  "",
  "- The mapping comes from [design 0002](design/0002-command-screens.md); this page is generated from it.",
  "- Piece stills: `node tools/asciipack/stills.mjs docs/img/ui`. Page: `node tools/asciipack/gallery.mjs docs`. Both run under `task gallery`.",
  "- kaas screens: `task screenshots`, real runs against an in-memory Kafka cluster.",
  "- Each piece name links to its animated original on [ascii-magic.com](https://www.ascii-magic.com/ascii) (MIT).",
  "",
  `**Status:** built ${Object.keys(SHOTS).length} · Phase 1, 2, 3 = which product phase builds it · unused = no honest Kafka fit.`,
  "",
  "## Index",
  "",
  "| Piece | Command or state | Status |",
  "|---|---|---|",
];
for (const [, slugs] of order) {
  for (const slug of slugs) {
    const p = bySlug.get(slug);
    const who = [...new Set(uses.get(slug).map((u) => u.who || u.what))].join(", ");
    out.push(`| [${p.name}](#${anchor(p.name)}) | ${who} | ${statusOf(slug)} |`);
  }
}
const missing = pieces.filter((p) => !uses.has(p.slug));
for (const p of missing) out.push(`| [${p.name}](#${anchor(p.name)}) | not mapped yet | |`);
if (missing.length) order.push(["Not mapped yet", missing.map((p) => p.slug)]);

for (const [title, slugs] of order) {
  out.push("", `## ${title}`);
  for (const slug of slugs) {
    const p = bySlug.get(slug);
    out.push("", `### ${p.name}`, "");
    out.push(`[\`${slug}\`](https://www.ascii-magic.com/ascii/${slug}) · **${statusOf(slug) || "—"}** · ${p.note}`, "");
    for (const u of uses.get(slug) ?? []) {
      out.push(u.who ? `- ${u.who}: ${u.what}` : `- ${u.what}`);
    }
    out.push("");
    const piece = `<img src="img/ui/${slug}.svg" alt="ascii-magic ${p.name}" width="100%">`;
    if (SHOTS[slug]) {
      const shot = `<img src="img/${SHOTS[slug]}.svg" alt="kaas screen built from ${p.name}" width="100%">`;
      out.push("<table><tr><th>The piece</th><th>The kaas screen</th></tr>",
        `<tr><td width="50%">${piece}</td><td width="50%">${shot}</td></tr></table>`);
    } else {
      out.push(`<img src="img/ui/${slug}.svg" alt="ascii-magic ${p.name}" width="560">`);
    }
  }
}
out.push("", "---", "", "*Piece stills © ascii-magic-art contributors, MIT ([licence](img/ui/LICENSE.ascii-magic-art)).*", "");
writeFileSync(join(docs, "SCREENS.md"), out.join("\n"));
console.log(`${pieces.length - missing.length} mapped, ${missing.length} unmapped → ${join(docs, "SCREENS.md")}`);
