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
    return h("svg", { class: "spark", viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
      h("line", { x1: 0, x2: W, y1: H - 1, y2: H - 1, stroke: "var(--line-strong)", "stroke-dasharray": "4 6", "vector-effect": "non-scaling-stroke" }));
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
    h("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", "aria-hidden": "true" },
      gradient(id),
      ticks.map((v) => h("line", { x1: 0, x2: W, y1: H - (v / max) * H, y2: H - (v / max) * H, stroke: "var(--chart-grid)", "vector-effect": "non-scaling-stroke" })),
      h("path", { d: area, fill: `url(#${id})` }),
      h("path", { d: line, fill: "none", stroke: "var(--chart-line)", "stroke-width": "2", "vector-effect": "non-scaling-stroke", "stroke-linejoin": "round" })),
    ticks.slice(1).map((v) => h("span", { class: "y-label", style: { top: (100 - (v / max) * 100) + "%" } }, format(v))),
    xs.map((t, i) => h("span", { class: "x-label", style: { left: i * 50 + "%", transform: `translateX(${-i * 50}%)` } }, hm(t))),
    h("div", { class: "cross", hidden: true, ref: (el) => (cross = el) }),
    h("div", { class: "marker", hidden: true, ref: (el) => (marker = el) }),
    h("div", { class: "tip", hidden: true, ref: (el) => (tip = el) }, h("div", { class: "t" }, ""), h("div", { class: "v" }, "")),
    h("div", { class: "hit", onmousemove: move, onmouseleave: leave }));
}

// bar is a value against a maximum, in a table cell.
export function bar(v, max, tone) {
  const pct = max > 0 ? Math.max(v > 0 ? 2 : 0, Math.min(100, (v / max) * 100)) : 0;
  return h("div", { class: "bar " + (tone || "") }, h("i", { style: { width: pct + "%" } }));
}
