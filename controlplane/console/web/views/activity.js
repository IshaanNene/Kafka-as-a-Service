import { h } from "../lib/h.js";
import { plural } from "../lib/fmt.js";
import { state, changed } from "../lib/store.js";
import { hero, feed } from "./common.js";

const sevs = [["all", "All"], ["critical", "Critical"], ["warning", "Warnings"], ["info", "Info"], ["ok", "Recovered"]];

export function activityPage() {
  const ui = state.ui;
  const names = state.meta?.clusters || [];
  const list = state.events.filter((e) => (ui.sev === "all" || e.severity === ui.sev) && (ui.eventCluster === "all" || e.cluster === ui.eventCluster));
  const crit = state.events.filter((e) => e.severity === "critical").length;
  return [
    hero({
      eyebrow: "Activity",
      title: h("span", null, "What changed, ", h("span", { class: "accent" }, "as it happened"), "."),
      lede: `Leader moves, ISR changes, topics and brokers coming and going, drift and applies across ${plural(names.length, "cluster")}, streamed live. ${crit ? plural(crit, "critical event") + " so far." : "Nothing critical so far."}`,
    }),
    h("div", { class: "toolbar" },
      h("div", { class: "seg", role: "group", "aria-label": "Severity" }, sevs.map(([id, label]) =>
        h("button", { key: id, "aria-pressed": ui.sev === id ? "true" : "false", onclick: () => { ui.sev = id; changed(); } }, label))),
      h("div", { class: "seg", role: "group", "aria-label": "Cluster" }, ["all", ...names].map((n) =>
        h("button", { key: n, "aria-pressed": ui.eventCluster === n ? "true" : "false", onclick: () => { ui.eventCluster = n; changed(); } }, n === "all" ? "Every cluster" : n))),
      h("span", { class: "right muted" }, plural(list.length, "event"))),
    h("section", { class: "card" }, feed(list, { limit: 500 })),
  ];
}
