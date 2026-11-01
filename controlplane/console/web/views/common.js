import { h } from "../lib/h.js";
import { icon, sevIcon } from "../lib/icons.js";
import { ago } from "../lib/fmt.js";
import { state, changed } from "../lib/store.js";

export const link = (href, props, ...kids) => h("a", { href, "data-link": "", ...props }, ...kids);

export const clusterHref = (name, tab, rest) =>
  "/clusters/" + encodeURIComponent(name) + (tab && tab !== "overview" ? "/" + tab : "") + (rest ? "/" + encodeURIComponent(rest) : "");

export function hero({ eyebrow, title, lede, actions, below }) {
  return h("header", { class: "hero" },
    h("div", { class: "hero-row" },
      h("div", { class: "grow" },
        h("div", { class: "eyebrow" }, h("span", { class: "bar" }), eyebrow),
        h("h1", { class: "title" }, title),
        lede ? h("p", { class: "lede" }, lede) : null),
      actions ? h("div", { class: "hero-actions" }, actions) : null),
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

// "LeaderMoved" → "Leader moved".
export function words(kind) {
  const s = kind.replace(/([a-z])([A-Z])/g, "$1 $2").replace("I S R", "ISR");
  return s.charAt(0) + s.slice(1).toLowerCase().replace(/\bisr\b/g, "ISR");
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
