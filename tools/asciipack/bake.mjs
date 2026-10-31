// bake.mjs samples ascii-magic-art pieces into frame packs for ui/asciipack.
//
// A pack is gzipped JSON: the piece's meta (size, fps, palette) plus one
// {t: text, c: colour keys} entry per frame. The Go player only indexes
// frames by time, so the kaas binary carries no JavaScript.
//
// Usage: node bake.mjs <out-dir> [slug:seconds ...]
import { mkdirSync, writeFileSync, copyFileSync } from "node:fs";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { createRequire } from "node:module";

// One loop of each piece. Seconds match the piece's own period where it has
// one (maintenance-mode cycles every 15 s); others loop cleanly enough.
const DEFAULT = [
  "page-404:12",
  "maintenance-mode:15",
  "packet-sniffer:10",
  "departures-hall-board:12",
  "token-bucket:12",
  "core-memory-plane:12",
];
const MAX_FPS = 15; // terminals gain little above this; keeps packs small

const [outDir, ...args] = process.argv.slice(2);
if (!outDir) {
  console.error("usage: node bake.mjs <out-dir> [slug:seconds ...]");
  process.exit(2);
}
mkdirSync(outDir, { recursive: true });

const require = createRequire(import.meta.url);
const pkgDir = join(require.resolve("ascii-magic-art/package.json"), "..");

for (const spec of args.length ? args : DEFAULT) {
  const [slug, secs] = spec.split(":");
  const mod = await import(join(pkgDir, "pieces", `${slug}.js`));
  const { meta } = mod;
  const fps = Math.min(meta.fps ?? 20, MAX_FPS);
  const n = Math.round(Number(secs) * fps);
  const draw = mod.default();
  const frames = [];
  for (let i = 0; i < n; i++) {
    const f = draw(i / fps);
    frames.push(typeof f === "string" ? { t: f, c: "" } : { t: f.text, c: f.color });
  }
  const pack = {
    slug: meta.slug, name: meta.name, note: meta.note, category: meta.category,
    cols: meta.cols, rows: meta.rows, fps, palette: meta.palette ?? {}, frames,
  };
  const gz = gzipSync(JSON.stringify(pack), { level: 9 });
  writeFileSync(join(outDir, `${slug}.pack.gz`), gz);
  console.log(`${slug}: ${n} frames @ ${fps} fps, ${(gz.length / 1024).toFixed(0)} KiB`);
}

// The pieces are MIT; keep their licence and notice beside the packs.
copyFileSync(join(pkgDir, "LICENSE"), join(outDir, "LICENSE.ascii-magic-art"));
copyFileSync(join(pkgDir, "NOTICE"), join(outDir, "NOTICE.ascii-magic-art"));
