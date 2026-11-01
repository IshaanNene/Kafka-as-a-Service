import { h } from "./h.js";
import { hm, clock } from "./fmt.js";

// Charts are SVG drawn in a fixed 1000-wide viewBox and stretched to the
// box; strokes don't scale, so lines stay 2px. Labels and the hover layer
// are HTML placed by percentage, so text never stretches.

const W = 1000;

function niceMax(v) {
  if (v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  for (const m of [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10]) if (m * p >= v) return m * p;
  return 10 * p;
}

function paths(pts, H, max, t0, t1) {
  const span = Math.max(1, t1 - t0);
  const xy = pts.map((p) => [((p.t - t0) / span) * W, H - (p.v / max) * H]);
  const line = xy.map(([x, y], i) => (i ? "L" : "M") + x.toFixed(1) + " " + y.toFixed(1)).join(" ");
  const area = line + ` L${xy[xy.length - 1][0].toFixed(1)} ${H} L${xy[0][0].toFixed(1)} ${H} Z`;
  return { line, area, xy };
}

function gradient(id) {
  return h("defs", null,
    h("linearGradient", { id, x1: "0", y1: "0", x2: "0", y2: "1" },
      h("stop", { offset: "0", "stop-color": "var(--chart-fill-top)" }),
      h("stop", { offset: "1", "stop-color": "var(--chart-fill-bottom)" })));
}

// sparkline is a cluster card's throughput: shape only, no axes.
export function sparkline(id, pts) {
  const H = 46;
  if (!pts || pts.length < 2) {
    // Nothing to draw yet: leave the space empty rather than draw a
    // baseline that reads as a flat-lined series.
    return h("svg", { class: "spark", viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" });
  }
  // Shape is the point: scale to the series' own range, not from zero.
  const hi = Math.max(...pts.map((p) => p.v)), lo = Math.min(...pts.map((p) => p.v));
  const floor = Math.max(0, lo - (hi - lo || hi * 0.2 || 1) * 0.8);
  const shifted = pts.map((p) => ({ t: p.t, v: p.v - floor }));
  const { line, area } = paths(shifted, H - 2, (hi - floor) * 1.08 || 1, pts[0].t, pts[pts.length - 1].t);
  return h("svg", { class: "spark", viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
    gradient(id),
    h("path", { d: area, fill: `url(#${id})` }),
    h("path", { d: line, fill: "none", stroke: "var(--chart-line)", "stroke-width": "2", "vector-effect": "non-scaling-stroke", "stroke-linejoin": "round" }));
}

// areaChart is one series over time with a crosshair and tooltip.
export function areaChart(id, pts, { label, format, height = 220 } = {}) {
  const H = height;
  if (!pts || pts.length < 2) {
    return h("div", { class: "chart", style: { height: H + "px" } },
      h("div", { class: "chart-empty" }, "Collecting samples… the first points arrive within ten seconds"));
  }
  const t0 = pts[0].t, t1 = pts[pts.length - 1].t;
  const max = niceMax(Math.max(...pts.map((p) => p.v)) * 1.08);
  const { line, area, xy } = paths(pts, H, max, t0, t1);
  const ticks = [0, 0.5, 1].map((f) => max * f);
  const xs = [0, 0.5, 1].map((f) => t0 + (t1 - t0) * f);
  let tip, cross, marker;
  const move = (e) => {
    const r = e.currentTarget.getBoundingClientRect();
    const fx = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width));
    const t = t0 + fx * (t1 - t0);
    let best = 0;
    for (let i = 1; i < pts.length; i++) if (Math.abs(pts[i].t - t) < Math.abs(pts[best].t - t)) best = i;
    const px = (xy[best][0] / W) * 100, py = (xy[best][1] / H) * 100;
    for (const el of [tip, cross, marker]) el.hidden = false;
    cross.style.left = px + "%";
    marker.style.left = px + "%";
    marker.style.top = py + "%";
    tip.style.left = Math.min(88, Math.max(12, px)) + "%";
    tip.style.top = py + "%";
    tip.firstChild.textContent = clock(pts[best].t);
    tip.lastChild.textContent = format(pts[best].v) + (label ? " " + label : "");
  };
  const leave = () => { for (const el of [tip, cross, marker]) el.hidden = true; };
  return h("div", { class: "chart", style: { height: H + "px" }, role: "img", "aria-label": `${label || "value"} over the last ${Math.round((t1 - t0) / 60000)} minutes; latest ${format(pts[pts.length - 1].v)}` },
    h("div", { class: "plot" },
    h("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
      gradient(id),
      ticks.map((v) => h("line", { x1: 0, x2: W, y1: H - (v / max) * H, y2: H - (v / max) * H, stroke: "var(--chart-grid)", "vector-effect": "non-scaling-stroke" })),
      h("path", { d: area, fill: `url(#${id})` }),
      h("path", { d: line, fill: "none", stroke: "var(--chart-line)", "stroke-width": "2", "vector-effect": "non-scaling-stroke", "stroke-linejoin": "round" })),
    xs.map((t, i) => h("span", { class: "x-label", style: { left: i * 50 + "%", transform: `translateX(${-i * 50}%)` } }, hm(t))),
    h("div", { class: "cross", hidden: true, ref: (el) => (cross = el) }),
    h("div", { class: "marker", hidden: true, ref: (el) => (marker = el) }),
    h("div", { class: "tip", hidden: true, ref: (el) => (tip = el) }, h("div", { class: "t" }, ""), h("div", { class: "v" }, "")),
    h("div", { class: "hit", onmousemove: move, onmouseleave: leave })),
    yAxis(ticks, max, format));
}

// yAxis is the value labels in a gutter right of the plot, each centred
// on its gridline, so no label ever sits on the data.
function yAxis(ticks, max, format) {
  return h("div", { class: "y-axis", "aria-hidden": "true" },
    ticks.map((v) => h("span", { class: "y-label", style: { top: (100 - (v / max) * 100) + "%" } }, format(v))));
}

// bar is a value against a maximum, in a table cell.
export function bar(v, max, tone) {
  const pct = max > 0 ? Math.max(v > 0 ? 2 : 0, Math.min(100, (v / max) * 100)) : 0;
  return h("div", { class: "bar " + (tone || "") }, h("i", { style: { width: pct + "%" } }));
}

// ─────────────────────────── multi-series ───────────────────────────

// lineChart is two or more series over the same time, on one axis: the
// first filled, the rest as lines (dashed from the second on, so they
// differ by more than colour). Hover shows every series at that moment.
export function lineChart(id, series, { format, height = 240, unit = "" } = {}) {
  const H = height;
  const live = series.filter((s) => s.pts && s.pts.length > 1);
  if (!live.length) {
    return h("div", { class: "chart", style: { height: H + "px" } },
      h("div", { class: "chart-empty" }, "Collecting samples… the first points arrive within ten seconds"));
  }
  const t0 = Math.min(...live.map((s) => s.pts[0].t));
  const t1 = Math.max(...live.map((s) => s.pts[s.pts.length - 1].t));
  const max = niceMax(Math.max(...live.flatMap((s) => s.pts.map((p) => p.v))) * 1.1);
  const drawn = live.map((s) => ({ ...s, ...paths(s.pts, H, max, t0, t1) }));
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((f) => max * f);
  const xs = [0, 0.25, 0.5, 0.75, 1].map((f) => t0 + (t1 - t0) * f);
  let tip, cross;
  const markers = [];
  const move = (e) => {
    const r = e.currentTarget.getBoundingClientRect();
    const fx = Math.min(1, Math.max(0, (e.clientX - r.left) / r.width));
    const t = t0 + fx * (t1 - t0);
    cross.hidden = tip.hidden = false;
    cross.style.left = fx * 100 + "%";
    tip.style.left = Math.min(84, Math.max(16, fx * 100)) + "%";
    tip.firstChild.textContent = clock(t);
    drawn.forEach((s, i) => {
      let best = 0;
      for (let j = 1; j < s.pts.length; j++) if (Math.abs(s.pts[j].t - t) < Math.abs(s.pts[best].t - t)) best = j;
      const m = markers[i];
      m.hidden = false;
      m.style.left = (s.xy[best][0] / W) * 100 + "%";
      m.style.top = (s.xy[best][1] / H) * 100 + "%";
      tip.children[i + 1].lastChild.textContent = format(s.pts[best].v) + unit;
    });
  };
  const leave = () => { cross.hidden = tip.hidden = true; markers.forEach((m) => (m.hidden = true)); };
  const latest = drawn.map((s) => `${s.label} ${format(s.pts[s.pts.length - 1].v)}${unit}`).join(", ");
  return h("div", { class: "chart", style: { height: H + "px" }, role: "img", "aria-label": `${latest}, over the last ${Math.max(1, Math.round((t1 - t0) / 60000))} minutes` },
    h("div", { class: "plot" },
    h("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
      h("defs", null, h("linearGradient", { id, x1: "0", y1: "0", x2: "0", y2: "1" },
        h("stop", { offset: "0", "stop-color": drawn[0].color, "stop-opacity": ".13" }),
        h("stop", { offset: "1", "stop-color": drawn[0].color, "stop-opacity": "0" }))),
      ticks.map((v) => h("line", { x1: 0, x2: W, y1: H - (v / max) * H, y2: H - (v / max) * H, stroke: "var(--chart-grid)", "vector-effect": "non-scaling-stroke" })),
      h("path", { d: drawn[0].area, fill: `url(#${id})` }),
      drawn.map((s, i) => h("path", {
        d: s.line, fill: "none", stroke: s.color, "stroke-width": i ? "2.5" : "2", "vector-effect": "non-scaling-stroke",
        "stroke-linejoin": "round", "stroke-dasharray": i ? "6 4" : null,
      }))),
    xs.map((t, i) => h("span", { class: "x-label", style: { left: i * 25 + "%", transform: `translateX(${-i * 25}%)` } }, t1 - t0 < 600000 ? clock(t) : hm(t))),
    h("div", { class: "cross", hidden: true, ref: (el) => (cross = el) }),
    drawn.map((s, i) => h("div", { class: "marker", hidden: true, style: { background: s.color }, ref: (el) => (markers[i] = el) })),
    h("div", { class: "tip multi", hidden: true, ref: (el) => (tip = el) },
      h("div", { class: "t" }, ""),
      drawn.map((s, i) => h("div", { class: "row-v" }, h("i", { class: "sw" + (i ? " dashed" : ""), style: { "--c": s.color } }), h("span", null, s.label), h("b", null, "")))),
    h("div", { class: "hit", onmousemove: move, onmouseleave: leave })),
    yAxis(ticks, max, format));
}

// ─────────────────────────── uptime ───────────────────────────

const slotWord = { healthy: "healthy", degraded: "degraded", critical: "partitions offline", unreachable: "unreachable", "": "no data" };

// uptimeStrip is the last hour a minute a bar, coloured by the worst status
// in that minute. Each bar names its minute and status for hover and for
// screen readers.
export function uptimeStrip(slots, { label } = {}) {
  return h("div", { class: "uptime", role: "list", "aria-label": label || "Status for each minute of the last hour" },
    (slots || []).map((s) => h("i", {
      key: "u" + s.t, role: "listitem", class: "s-" + (s.s || "none"),
      title: `${hm(s.t)} · ${slotWord[s.s || ""]}`, "aria-label": `${hm(s.t)} ${slotWord[s.s || ""]}`,
    })));
}

// ─────────────────────────── gauge ───────────────────────────

// ring is a share out of 100 as an arc: availability, mostly.
export function ring(pct, { size = 132, stroke = 10, tone = "ok" } = {}) {
  const r = (size - stroke) / 2, c = 2 * Math.PI * r;
  const f = pct == null || pct < 0 ? 0 : Math.max(0, Math.min(100, pct)) / 100;
  return h("svg", { class: "ring tone-" + tone, viewBox: `0 0 ${size} ${size}`, width: size, height: size, "aria-hidden": "true" },
    h("circle", { cx: size / 2, cy: size / 2, r, class: "ring-track", "stroke-width": stroke, fill: "none" }),
    h("circle", {
      cx: size / 2, cy: size / 2, r, class: "ring-value", "stroke-width": stroke, fill: "none", "stroke-linecap": "butt",
      "stroke-dasharray": `${(c * f).toFixed(2)} ${c.toFixed(2)}`, transform: `rotate(-90 ${size / 2} ${size / 2})`,
    }));
}

// ─────────────────────────── treemap ───────────────────────────

// treemap lays items out by value as rectangles kept close to square
// (squarified), returning each with x, y, w, h as fractions of the box.
export function squarify(items, W0 = 1, H0 = 1) {
  const total = items.reduce((a, b) => a + b.value, 0);
  if (total <= 0) return [];
  const area = W0 * H0;
  const rest = items.filter((i) => i.value > 0).sort((a, b) => b.value - a.value).map((i) => ({ ...i, a: (i.value / total) * area }));
  const out = [];
  let x = 0, y = 0, w = W0, hh = H0;
  const worst = (row, side) => {
    const s = row.reduce((a, b) => a + b.a, 0);
    const mx = Math.max(...row.map((r) => r.a)), mn = Math.min(...row.map((r) => r.a));
    return Math.max((side * side * mx) / (s * s), (s * s) / (side * side * mn));
  };
  while (rest.length) {
    const side = Math.min(w, hh);
    const row = [rest.shift()];
    while (rest.length && worst([...row, rest[0]], side) <= worst(row, side)) row.push(rest.shift());
    const s = row.reduce((a, b) => a + b.a, 0);
    if (w >= hh) {
      const cw = s / hh;
      let cy = y;
      for (const r of row) { const rh = r.a / cw; out.push({ ...r, x, y: cy, w: cw, h: rh }); cy += rh; }
      x += cw; w -= cw;
    } else {
      const ch = s / w;
      let cx = x;
      for (const r of row) { const rw = r.a / ch; out.push({ ...r, x: cx, y, w: rw, h: ch }); cx += rw; }
      y += ch; hh -= ch;
    }
  }
  return out;
}

// ─────────────────────────── change ───────────────────────────

// change compares the mean of the last minute with the minute `back`
// minutes earlier: +0.12 for 12% up. null without enough history.
export function change(pts, back = 15) {
  if (!pts || pts.length < 4) return null;
  const end = pts[pts.length - 1].t;
  const mean = (a, b) => {
    const xs = pts.filter((p) => p.t > a && p.t <= b).map((p) => p.v);
    return xs.length ? xs.reduce((s, v) => s + v, 0) / xs.length : null;
  };
  const now = mean(end - 60000, end);
  const span = end - pts[0].t;
  const ago = Math.min(back * 60000, span - 60000);
  if (ago < 120000) return null;
  const then = mean(end - ago - 60000, end - ago);
  if (now == null || then == null || then === 0) return null;
  return { ratio: (now - then) / then, minutes: Math.round(ago / 60000) };
}
