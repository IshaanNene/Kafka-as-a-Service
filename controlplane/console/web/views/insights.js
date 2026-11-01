import { h } from "../lib/h.js";
import { plural } from "../lib/fmt.js";
import { state, changed } from "../lib/store.js";
import { hero, insightList, loading } from "./common.js";

const sevs = [["all", "All"], ["critical", "Critical"], ["warning", "Warnings"], ["info", "Advice"]];

export function insightsPage() {
  const f = state.fleet;
  if (!f) return loading();
  const ui = state.ui;
  const names = state.meta?.clusters || [];
  const list = f.insights.filter((x) => (ui.insightSev === "all" || x.severity === ui.insightSev) && (ui.insightCluster === "all" || x.cluster === ui.insightCluster));
  const count = (s) => f.insights.filter((x) => x.severity === s).length;
  const crit = count("critical"), warn = count("warning");
  return [
    hero({
      eyebrow: "Recommendations",
      title: f.insights.length
        ? h("span", null, plural(f.insights.length, "thing"), " worth ", h("span", { class: "accent" }, "your time"), ".")
        : h("span", null, "Nothing needs ", h("span", { class: "accent" }, "you"), " right now."),
      lede: `Read from every cluster's live state each refresh: replication, consumers that stopped or fell behind, topics nobody reads, leadership skew and drift from git. ${crit ? plural(crit, "critical") + ", " : ""}${warn ? plural(warn, "warning") + "." : "No warnings."}`,
    }),
    h("div", { class: "toolbar" },
      h("div", { class: "seg", role: "group", "aria-label": "Severity" }, sevs.map(([id, label]) =>
        h("button", { key: id, "aria-pressed": ui.insightSev === id ? "true" : "false", onclick: () => { ui.insightSev = id; changed(); } },
          label, id !== "all" && count(id) ? " " + count(id) : ""))),
      h("div", { class: "seg", role: "group", "aria-label": "Cluster" }, ["all", ...names].map((n) =>
        h("button", { key: n, "aria-pressed": ui.insightCluster === n ? "true" : "false", onclick: () => { ui.insightCluster = n; changed(); } }, n === "all" ? "Every cluster" : n))),
      h("span", { class: "right muted" }, plural(list.length, "recommendation"))),
    h("section", { class: "card" }, insightList(list, { expanded: true })),
  ];
}
