import { h } from "../lib/h.js";
import { icon, status } from "../lib/icons.js";
import { compact, rate, plural, int } from "../lib/fmt.js";
import { areaChart, sparkline } from "../lib/charts.js";
import { state } from "../lib/store.js";
import { hero, stat, feed, link, clusterHref, envBadge, loading } from "./common.js";

export function fleetPage() {
  const f = state.fleet;
  if (!f) return loading();
  const t = f.totals;
  const unhealthy = t.clusters - t.healthy;
  const title = unhealthy === 0
    ? h("span", null, "Every cluster is ", h("span", { class: "accent" }, "healthy"), ".")
    : h("span", null, plural(unhealthy, "cluster"), " ", unhealthy === 1 ? "needs" : "need", " ", h("span", { class: "accent" }, "attention"), ".");
  return [
    hero({
      eyebrow: "Fleet overview",
      title,
      lede: `${plural(t.clusters, "cluster")}, ${plural(t.brokers, "broker")} and ${plural(t.partitions, "partition")}, moving ${rate(t.throughput)} records a second.` +
        (t.drift ? ` ${plural(t.drift, "change")} would bring them in line with git.` : " Everything matches git."),
      actions: [
        link("/changes", { class: "btn" }, icon("changes"), "Review changes"),
        link("/activity", { class: "btn ghost" }, icon("activity"), "Activity"),
      ],
    }),
    h("section", { class: "stats", "aria-label": "Fleet totals" },
      stat("Clusters", h("span", { class: "num" }, `${t.healthy}/${t.clusters}`), { ic: "cluster", note: "healthy" }),
      stat("Throughput", h("span", { class: "num" }, rate(t.throughput)), { ic: "bolt", unit: "rec/s", hot: true, note: "produced across the fleet" }),
      stat("Partitions", h("span", { class: "num" }, compact(t.partitions)), { ic: "topics", note: `${int(t.topics)} topics` }),
      stat("Under-replicated", h("span", { class: "num" }, int(t.urp + t.offline)), { ic: t.urp + t.offline ? "warn" : "ok", note: t.offline ? `${t.offline} offline` : "partitions short of replicas" }),
      stat("Consumer lag", h("span", { class: "num" }, compact(t.lag)), { ic: "consumers", unit: "records", note: "behind, across every group" }),
      stat("Drift from git", h("span", { class: "num" }, int(t.drift)), { ic: "changes", unit: t.drift === 1 ? "change" : "changes", note: "planned by GitOps" })),
    h("div", { class: "grid-2 mt" },
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, "Fleet throughput"), h("span", { class: "right muted mono" }, "records/s · last 10 min")),
        h("div", { class: "card-body", style: { "padding-bottom": "34px" } }, areaChart("g-fleet", f.history, { label: "rec/s", format: rate }))),
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, "Live activity"), h("span", { class: "right" }, link("/activity", { class: "btn sm ghost" }, "All", icon("arrow")))),
        h("div", { style: { "margin-top": "8px", "max-height": "300px", overflow: "auto" } }, feed(state.events, { limit: 8 })))),
    h("h2", { class: "section" }, "Clusters", h("span", { class: "muted" }, `${t.clusters} in this fleet`)),
    h("div", { class: "grid-cards" }, f.clusters.map(clusterCard)),
  ];
}

function driftLine(c) {
  if (!c.gitops) return h("span", { class: "muted" }, "No desired-state file");
  if (c.blocked) return h("span", { class: "chip crit" }, icon("crit"), "Blocked: " + short(c.blocked));
  if (!c.drift) return h("span", { class: "chip ok" }, icon("ok"), "In sync with git");
  const r = c.driftRisk;
  return h("span", { class: "row" },
    h("span", { class: "chip warn" }, icon("changes"), plural(c.drift, "change")),
    r.destructive ? h("span", { class: "chip crit" }, `${r.destructive} destructive`) : null);
}

function short(s) {
  return s.length > 38 ? s.slice(0, 36) + "…" : s;
}

export function clusterCard(c) {
  const down = c.status === "unreachable" || c.status === "connecting";
  return link(clusterHref(c.name), { key: c.name, class: `card cluster-card status-${c.status}`, "aria-label": `${c.name}: ${c.status}` },
    h("div", { class: "cc-top" },
      h("div", null,
        h("div", { class: "name" }, c.name),
        h("div", { class: "where" }, c.region || "—", c.clusterId ? h("span", { class: "mono muted" }, "· " + c.clusterId.slice(0, 14)) : null)),
      envBadge(c.env)),
    status(c.status, c.reason),
    down
      ? h("div", { class: "callout crit", style: { "font-size": "12.5px" } }, icon("offline"),
        h("div", { class: "body" }, h("b", null, "No answer. "), c.error ? short(c.error.replace(/^.*?: /, "")) : "Retrying every second."))
      : h("div", { class: "cc-spark" }, sparkline("s-" + c.name, c.history)),
    h("div", { class: "cc-metrics" },
      metric("Brokers", int(c.brokers)),
      metric("Partitions", compact(c.partitions)),
      metric("Rec/s", rate(c.throughput)),
      metric("Lag", compact(c.lag))),
    h("div", { class: "cc-foot" }, driftLine(c),
      h("span", { class: "right muted" }, c.hasRegistry ? `${c.schemas} schemas` : "", c.hasRegistry && c.acls ? " · " : "", c.acls ? `${c.acls} ACLs` : "")));
}

function metric(k, v) {
  return h("div", { class: "cc-metric" }, h("div", { class: "k" }, k), h("div", { class: "v" }, v));
}
