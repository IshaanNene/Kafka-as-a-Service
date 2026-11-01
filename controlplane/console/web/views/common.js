import { h } from "../lib/h.js";
import { icon, sevIcon } from "../lib/icons.js";
import { ago, signed, words } from "../lib/fmt.js";
import { sparkline, change } from "../lib/charts.js";
import { state, changed } from "../lib/store.js";
import { navigate } from "../lib/router.js";

export const link = (href, props, ...kids) => h("a", { href, "data-link": "", ...props }, ...kids);

export const clusterHref = (name, tab, rest) =>
  "/clusters/" + encodeURIComponent(name) + (tab && tab !== "overview" ? "/" + tab : "") + (rest ? "/" + encodeURIComponent(rest) : "");

// hero is a page's header: the eyebrow, then the title with the page's
// actions on the same line, then the lede. Every page's header starts at
// the same offset.
export function hero({ eyebrow, title, lede, actions, below }) {
  const acts = [actions].flat().filter(Boolean);
  return h("header", { class: "hero" },
    h("div", { class: "eyebrow" }, h("span", { class: "bar" }), eyebrow),
    h("div", { class: "hero-row" },
      h("h1", { class: "title" }, title),
      acts.length ? h("div", { class: "hero-actions" }, acts) : null),
    lede ? h("p", { class: "lede" }, lede) : null,
    below);
}

export function stat(label, value, { unit, note, hot, ic } = {}) {
  return h("div", { class: "stat" + (hot ? " hot" : "") },
    h("div", { class: "stat-label" }, ic ? icon(ic) : null, label),
    h("div", { class: "stat-value" }, value, unit ? h("small", null, unit) : null),
    h("div", { class: "stat-note" }, note || ""));
}

export function feed(events, { showCluster = true, limit = 14 } = {}) {
  const list = events.slice(-limit).reverse();
  if (!list.length) {
    return empty("activity", "Quiet so far", "Leader moves, ISR changes, new topics, drift and applies show up here as they happen.");
  }
  const now = Date.now();
  return h("ul", { class: "feed" }, list.map((e) =>
    h("li", { key: "e" + e.seq, class: "sev-" + e.severity },
      h("span", { class: "ico" }, icon(sevIcon[e.severity] || "info")),
      h("div", { class: "what" },
        h("div", null, h("b", null, words(e.kind)), " ", h("span", { class: "txt" }, e.text)),
        showCluster ? link(clusterHref(e.cluster), { class: "cl" }, e.cluster) : null),
      h("time", { class: "when", datetime: e.at, title: new Date(e.at).toLocaleString() }, ago(Date.parse(e.at), now)))));
}


export function empty(ic, title, text, action) {
  return h("div", { class: "empty" },
    h("div", { class: "art" }, icon(ic)),
    h("h3", null, title),
    h("p", null, text),
    action || null);
}

export function copyCode(cmd) {
  const copied = state.ui.copied === cmd;
  return h("div", { class: "code" },
    h("span", { class: "prompt" }, "$"),
    h("code", null, cmd),
    h("button", {
      class: "btn sm ghost", "aria-label": "Copy command",
      onclick: () => {
        navigator.clipboard?.writeText(cmd);
        state.ui.copied = cmd;
        changed();
        setTimeout(() => { if (state.ui.copied === cmd) { state.ui.copied = null; changed(); } }, 1600);
      },
    }, icon("copy"), copied ? "Copied" : "Copy"));
}

export function envBadge(env) {
  return env ? h("span", { class: "env " + env }, env === "production" ? "prod" : env) : null;
}

export function loading() {
  return h("div", { class: "stack" },
    h("div", { class: "skeleton", style: { height: "120px" } }),
    h("div", { class: "skeleton", style: { height: "260px" } }));
}

// inRange keeps the points inside the time range the user picked.
export function inRange(pts) {
  if (!pts || !pts.length) return pts || [];
  const from = pts[pts.length - 1].t - state.ui.range * 60000;
  return pts.filter((p) => p.t >= from);
}

export function rangeSwitch() {
  return h("div", { class: "seg", role: "group", "aria-label": "Time range" },
    [[15, "15m"], [60, "1h"]].map(([m, label]) => h("button", {
      key: label, "aria-pressed": state.ui.range === m ? "true" : "false",
      onclick: () => { state.ui.range = m; try { localStorage.setItem("kaas.range", String(m)); } catch { /* private mode */ } changed(); },
    }, label)));
}

// delta is a change badge: up is good for traffic, so it's green; a fall
// is amber, never red, since less traffic isn't by itself a fault.
export function delta(pts) {
  const c = change(pts);
  if (!c) return null;
  const up = c.ratio > 0.005, down = c.ratio < -0.005;
  return h("span", { class: "chg", title: `compared with ${c.minutes} minutes ago` },
    up || down ? h("span", { class: "arr", "aria-hidden": "true" }, up ? "↑" : "↓") : null, signed(c.ratio), h("span", { class: "sr-only" }, ` compared with ${c.minutes} minutes ago`));
}

// kpi is a headline number with what it's about, how it's moving, and
// its recent shape.
export function kpi({ label, value, unit, foot, spark, sparkId, color, ic, viz }) {
  return h("div", { class: "kpi" },
    h("div", { class: "kpi-label" }, color ? h("i", { class: "key", style: { "--c": color } }) : ic ? icon(ic) : null, label),
    h("div", { class: "kpi-value" }, value, unit ? h("small", null, unit) : null),
    h("div", { class: "kpi-foot" }, foot),
    spark ? h("div", { class: "kpi-spark", style: color ? { "--chart-line": color, "--chart-fill-top": `color-mix(in oklab, ${color} 16%, transparent)`, "--chart-fill-bottom": "transparent" } : null },
      sparkline(sparkId, spark)) : null,
    viz ? h("div", { class: "kpi-viz" }, viz) : null);
}

// meter is a value against a threshold: the bar fills to the threshold
// mark and past it, turning to warning there, with both ends labelled so
// the colour is never the only signal.
export function meter(v, limit, { label, over }) {
  const full = limit * 2;
  const w = Math.min(100, (Math.max(0, v) / full) * 100);
  const bad = v > limit;
  return h("div", { class: "meter" + (bad ? " over" : "") },
    h("div", { class: "meter-track" }, h("i", { style: { width: w.toFixed(1) + "%" } }), h("b", { class: "mark", title: label })),
    h("div", { class: "meter-lbl" }, h("span", null, bad ? over : "within target"), h("span", null, label)));
}

// split is one bar cut into parts, for a total and what makes it up,
// with a caption that says the part worth saying. Parts are neutral steps,
// each named in its tooltip.
export function split(parts, caption) {
  const total = parts.reduce((a, p) => a + p.value, 0) || 1;
  return h("div", { class: "ksplit" },
    h("div", { class: "ksplit-bar" }, parts.filter((p) => p.value > 0).map((p, i) =>
      h("i", { key: p.label, class: "p" + i, title: `${p.label}: ${p.text} (${share(p.value / total)})`, style: { flex: `${p.value / total} 1 0` } }))),
    h("div", { class: "ksplit-lbl" }, caption));
}

export function share(f) {
  return f > 0 && f < 0.005 ? "<1%" : Math.round(f * 100) + "%";
}

const sevWord = { critical: "Critical", warning: "Warning", info: "Advice" };

// insightList is recommendations, most severe first: what's wrong, what
// it means for the business, and where to look.
export function insightList(items, { limit, showCluster = true, expanded = false } = {}) {
  const list = limit ? items.slice(0, limit) : items;
  if (!list.length) {
    return h("div", { class: "ins-empty" }, h("span", { class: "sev" }, icon("ok")),
      h("div", null, h("b", null, "Nothing needs you right now. "), h("span", { class: "muted" }, "Replication, consumers, leadership and drift all look right.")));
  }
  return h("ul", { class: "ins" }, list.map((x) => {
    const body = [
      h("span", { class: "sev", title: sevWord[x.severity] }, icon(sevIcon[x.severity] || "info"), h("span", { class: "sr-only" }, sevWord[x.severity])),
      h("div", { class: "ins-body" },
        h("div", { class: "t" }, x.title, expanded && showCluster ? h("span", { class: "chip mono" }, x.cluster) : null),
        expanded ? h("div", { class: "d" }, x.detail) : null,
        h("div", { class: "i" }, x.impact),
        expanded && (x.command || x.href) ? h("div", { class: "ins-actions" },
          x.command ? copyCode(x.command) : null,
          x.href ? link(x.href, { class: "btn sm outline" }, "Open in the console", icon("arrow")) : null) : null),
      expanded ? null : h("div", { class: "meta" }, showCluster ? h("span", { class: "mono" }, x.cluster) : null, x.href ? icon("arrow") : null),
    ];
    return h("li", { key: x.id, class: "sev-" + x.severity },
      x.href && !expanded ? link(x.href, { class: "ins-row" }, body) : h("div", { class: "ins-row" + (expanded ? " wide" : "") }, body));
  }));
}

export { navigate, words };
