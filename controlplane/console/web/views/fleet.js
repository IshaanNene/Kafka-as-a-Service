import { h } from "../lib/h.js";
import { icon, status } from "../lib/icons.js";
import { compact, rate, plural, int, money, pct, age, bytes } from "../lib/fmt.js";
import { lineChart, sparkline, uptimeStrip, ring } from "../lib/charts.js";
import { dots, project, locate, MAP } from "../lib/geo.js";
import { state } from "../lib/store.js";
import { hero, feed, link, clusterHref, envBadge, loading, kpi, delta, insightList, inRange, rangeSwitch, navigate, meter, split, share } from "./common.js";

const S1 = "var(--s1)", S2 = "var(--s2)";

export function fleetPage() {
  const f = state.fleet;
  if (!f) return loading();
  const t = f.totals;
  const down = f.clusters.filter((c) => c.status !== "healthy" && c.status !== "connecting");
  const urgent = f.insights.filter((x) => x.severity === "critical" || x.severity === "warning");
  const regions = new Set(f.clusters.map((c) => c.region).filter(Boolean));
  const title = down.length === 0
    ? h("span", null, "Every cluster is ", h("span", { class: "accent" }, "healthy"), ".")
    : h("span", null, plural(down.length, "cluster"), " ", down.length === 1 ? "needs" : "need", " ", h("span", { class: "accent" }, "attention"), ".");
  return [
    hero({
      eyebrow: [`${state.meta?.name || "Fleet"} · ${plural(t.clusters, "cluster")} in ${plural(regions.size, "region")}`],
      title,
      lede: down.length
        ? [icon("crit"), h("span", null, down.map((c) => `${c.name}: ${c.reason}`).join(" · "))]
        : `${int(t.brokers)} brokers serving ${compact(t.throughput)} records a second, with ${t.drift ? plural(t.drift, "change") + " waiting in git" : "everything matching git"}.`,
      actions: [
        urgent.length ? link("/insights", { class: "btn" }, icon("warn"), urgent.length + " need action") : null,
        t.drift ? link("/changes", { class: "btn primary" }, icon("changes"), "Review " + plural(t.drift, "change")) : null,
      ],
    }),
    summary(f),
    h("div", { class: "grid-2 mt" },
      h("section", { class: "card", "aria-label": "Where the fleet runs" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("fleet"), "Global footprint"),
          h("div", { class: "right map-legend" },
            h("span", { class: "status-healthy" }, h("i"), "Healthy"),
            h("span", { class: "status-degraded" }, h("i"), "Degraded"),
            h("span", { class: "status-critical" }, h("i"), "Down"))),
        worldMap(f.clusters),
        regionShare(f.clusters)),
      h("section", { class: "card", "aria-label": "Recommendations" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("bolt"), "Recommendations"),
          h("span", { class: "right" }, f.insights.length ? h("span", { class: "nav-count" + (urgent.length ? " hot" : "") }, f.insights.length) : null)),
        h("div", { class: "mt" }, insightList(f.insights, { limit: 5 })),
        f.insights.length > 5 ? h("div", { class: "card-foot" }, link("/insights", { class: "card-link" }, "All " + plural(f.insights.length, "recommendation"), icon("arrow"))) : null)),
    h("div", { class: "grid-2 mt" },
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("activity"), "Traffic"),
          h("div", { class: "right" },
            h("div", { class: "series-legend" },
              h("span", null, h("i", { class: "sw", style: { "--c": S1 } }), "Produced"),
              h("span", null, h("i", { class: "sw dashed", style: { "--c": S2 } }), "Consumed")),
            rangeSwitch())),
        h("div", { class: "card-sub" }, "Records a second across every cluster"),
        h("div", { class: "card-body", style: { "padding-bottom": "36px" } },
          lineChart("g-fleet", [
            { label: "Produced", pts: inRange(f.history), color: S1 },
            { label: "Consumed", pts: inRange(f.consumed), color: S2 },
          ], { format: rate, unit: "/s", height: 230 }))),
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("activity"), "Live activity"),
          h("span", { class: "right" }, link("/activity", { class: "card-link" }, "All", icon("arrow")))),
        h("div", { style: { "margin-top": "8px" } }, feed(state.events, { limit: 5 })))),
    h("h2", { class: "section" }, "Clusters", h("span", { class: "muted" }, "availability over the last hour, traffic and freshness, live")),
    clustersTable(f.clusters),
  ];
}

// summary is the band under the title: availability as a ring, then the
// four numbers a business asks about Kafka: what goes in, what comes out,
// how fresh it is, and what it costs.
function summary(f) {
  const t = f.totals;
  const up = f.clusters.filter((c) => c.status === "healthy" || c.status === "degraded").length;
  const tone = t.availability < 0 ? "none" : t.availability >= 99.9 ? "ok" : t.availability >= 95 ? "warn" : "crit";
  const consumers = f.clusters.reduce((a, c) => a + c.groups, 0);
  return h("section", { class: "card", "aria-label": "Fleet summary" },
    h("div", { class: "band" },
      h("div", { class: "gauge" },
        h("div", { class: "ring-wrap" }, ring(t.availability, { tone }),
          h("div", { class: "ring-label" }, h("div", null, h("b", null, pct(t.availability)), h("span", null, "available")))),
        h("div", null,
          h("h3", null, `${up} of ${plural(t.clusters, "cluster")} serving`),
          h("p", null, "Share of the last hour each cluster answered with every partition online, averaged."),
          h("div", { class: "chips" },
            t.urp + t.offline ? h("span", { class: "chip warn" }, icon("warn"), `${int(t.urp + t.offline)} partitions short`) : h("span", { class: "chip ok" }, icon("ok"), "Fully replicated"),
            t.drift ? h("span", { class: "chip" }, icon("changes"), plural(t.drift, "change") + " from git") : null))),
      kpi({
        label: "Produced", color: S1, value: h("span", { class: "num" }, rate(t.throughput)), unit: "rec/s",
        foot: [delta(f.history), h("span", null, "into " + plural(t.topics, "topic"))], spark: inRange(f.history), sparkId: "k-in",
      }),
      kpi({
        label: "Consumed", color: S2, value: h("span", { class: "num" }, rate(t.consumeRate)), unit: "rec/s",
        foot: [delta(f.consumed), h("span", null, "by " + plural(consumers, "group"))], spark: inRange(f.consumed), sparkId: "k-out",
      }),
      kpi({
        label: "Data freshness", ic: "connecting", value: h("span", { class: "num" }, t.freshness < 0 ? "—" : age(t.freshness)),
        unit: t.freshness > 0 ? "behind" : "",
        foot: t.freshness > 0 && t.freshGroup
          ? h("span", null, "worst: ", h("b", { class: "mono" }, t.freshGroup), " on ", t.freshCluster)
          : h("span", null, "every consumer is caught up"),
        viz: t.freshness >= 0 ? meter(t.freshness, 60, { label: "stale after 1m", over: "past target" }) : null,
      }),
      f.pricing
        ? kpi({
          label: "Est. run cost", ic: "bolt", value: h("span", { class: "num" }, money(t.cost, f.pricing.currency)), unit: "/ month",
          foot: h("span", { title: `brokers × ${money(f.pricing.brokerHour, f.pricing.currency)}/h, storage × ${f.pricing.gibMonth} ${f.pricing.currency}/GiB-month` },
            `${plural(t.brokers, "broker")}${t.disk ? " · " + bytes(t.disk) : ""}`),
          viz: costSplit(t, f.pricing),
        })
        : kpi({
          label: "Consumer backlog", ic: "consumers", value: h("span", { class: "num" }, compact(t.lag)), unit: "records",
          foot: "behind, across every group",
        })),
    h("div", { class: "inventory" },
      inv("Brokers", int(t.brokers)), inv("Topics", int(t.topics)), inv("Partitions", compact(t.partitions)),
      inv("Consumer groups", int(consumers)), inv("Schemas", int(t.schemas)), inv("Backlog", compact(t.lag))));
}

// costSplit says where the estimate comes from: brokers or storage.
function costSplit(t, p) {
  const compute = t.brokers * p.brokerHour * 730;
  const storage = (t.disk / 2 ** 30) * p.gibMonth;
  const total = compute + storage || 1;
  return split([
    { label: "Brokers", value: compute, text: money(compute, p.currency) },
    { label: "Storage", value: storage, text: money(storage, p.currency) },
  ], `brokers ${share(compute / total)} · storage ${share(storage / total)}`);
}

function inv(k, v) {
  return h("div", null, h("b", null, v), k);
}

// ─────────────────────────── map ───────────────────────────

let land = null;

function worldMap(clusters) {
  // The land never changes: build it once and reuse the node.
  land ||= h("svg", { key: "land", class: "land", viewBox: `0 0 ${MAP.w} ${MAP.h}`, preserveAspectRatio: "xMidYMid meet", "aria-hidden": "true" },
    dots().map(([x, y]) => h("circle", { cx: x.toFixed(1), cy: y.toFixed(1), r: "1.9" })));
  const placed = [], unplaced = [];
  for (const c of clusters) {
    const at = locate(c.region);
    if (!at) { unplaced.push(c); continue; }
    const [x, y] = project(at.lat, at.lon);
    placed.push({ c, at, x: (x / MAP.w) * 100, y: (y / MAP.h) * 100 });
  }
  placed.sort((a, b) => a.x - b.x);
  const maxRate = Math.max(1, ...clusters.map((c) => c.throughput));
  // Neighbours close enough for their labels to collide put the later
  // one's label on the left.
  placed.forEach((p, i) => {
    // Of two neighbours, the western one labels to its left.
    p.flip = placed.some((q) => q !== p && q.x > p.x && q.x - p.x < 14 && Math.abs(q.y - p.y) < 7) || p.x > 82;
    // Two clusters in one city sit side by side.
    const twins = placed.slice(0, i).filter((q) => Math.abs(q.x - p.x) < 0.8 && Math.abs(q.y - p.y) < 0.8).length;
    p.y += twins * 4;
  });
  return [
    h("div", { class: "map" }, land,
      placed.map(({ c, at, x, y, flip }) => {
        const d = 9 + Math.round(9 * Math.sqrt(c.throughput / maxRate));
        return link(clusterHref(c.name), {
          key: "n-" + c.name, class: `node status-${c.status}${flip ? " flip" : ""}`,
          style: { left: x + "%", top: y + "%", "--d": d + "px" },
          "aria-label": `${c.name} in ${at.place}: ${c.status}, ${rate(c.throughput)} records a second`,
        },
        h("span", { class: "halo" }), h("span", { class: "core" }),
        h("span", { class: "tag" }, c.name, h("span", { class: "num" }, c.status === "unreachable" ? "down" : rate(c.throughput) + "/s")));
      })),
    unplaced.length ? h("div", { class: "unplaced" }, "Not on the map (no known region): ", unplaced.map((c) => c.name).join(", ")) : null,
  ];
}

// ─────────────────────────── clusters ───────────────────────────

function clustersTable(clusters) {
  return h("section", { class: "card" }, h("div", { class: "table-wrap" },
    h("table", { class: "data" },
      h("thead", null, h("tr", null,
        h("th", null, "Cluster"), h("th", null, "Status"), h("th", null, "Availability · 1h"), h("th", null, "Produced"),
        h("th", { class: "r" }, "Freshness"), h("th", { class: "r" }, "Brokers"), h("th", null, "GitOps"),
        state.fleet.pricing ? h("th", { class: "r" }, "Cost / mo") : null)),
      h("tbody", null, clusters.map((c) => {
        const at = locate(c.region);
        const go = () => navigate(clusterHref(c.name));
        return h("tr", { key: c.name, class: "clickable status-" + c.status, tabindex: "0", onclick: go, onkeydown: (e) => { if (e.key === "Enter") go(); } },
          h("td", null, h("div", { class: "cl-cell" },
            h("span", { class: "dot ring" }),
            h("div", null,
              h("div", { class: "nm" }, c.name, " ", envBadge(c.env)),
              h("div", { class: "wh" }, c.region || "no region", at ? h("span", null, "· " + at.place) : null)))),
          h("td", null, status(c.status)),
          h("td", null, h("div", { class: "avail" }, uptimeStrip(c.uptime, { label: c.name + " status for each minute of the last hour" }), h("span", { class: "num" }, pct(c.availability)))),
          h("td", null, c.status === "unreachable" ? h("span", { class: "muted" }, "—")
            : h("div", { class: "spark-cell" }, h("div", { class: "spark-box", style: { "--chart-line": S1, "--chart-fill-top": "color-mix(in oklab, var(--s1) 16%, transparent)", "--chart-fill-bottom": "transparent" } }, sparkline("t-" + c.name, inRange(c.history))),
              h("span", { class: "num" }, rate(c.throughput) + "/s"))),
          h("td", { class: "r num" }, c.freshness < 0 ? "—" : age(c.freshness)),
          h("td", { class: "r num" }, c.status === "unreachable" ? "—" : int(c.brokers)),
          h("td", null, gitops(c)),
          state.fleet.pricing ? h("td", { class: "r num" }, c.cost ? money(c.cost, state.fleet.pricing.currency) : "—") : null);
      })))));
}

// regionShare says where the records go, a bar per cluster sized by its
// share of what the fleet produces, under the map that says where it runs.
function regionShare(clusters) {
  const total = clusters.reduce((a, c) => a + c.throughput, 0);
  const rows = [...clusters].sort((a, b) => b.throughput - a.throughput);
  return h("div", { class: "tshare" },
    h("div", { class: "tshare-head" }, h("span", null, "Share of traffic"), h("span", null, rate(total) + "/s in all")),
    rows.map((c) => {
      const p = total ? (c.throughput / total) * 100 : 0;
      const at = locate(c.region);
      return link(clusterHref(c.name), { key: c.name, class: "tshare-row status-" + c.status },
        h("span", { class: "dot" }),
        h("span", { class: "nm" }, c.name, h("span", { class: "muted" }, at ? at.place : c.region || "")),
        h("span", { class: "bar s1" }, h("i", { style: { width: p.toFixed(1) + "%" } })),
        h("span", { class: "num" }, c.status === "unreachable" ? "down" : Math.round(p) + "%"));
    }));
}

function gitops(c) {
  if (!c.gitops) return h("span", { class: "muted" }, "Not managed");
  if (c.blocked) return h("span", { class: "chip crit", title: c.blocked }, icon("crit"), "Blocked");
  if (!c.drift) return h("span", { class: "chip ok" }, icon("ok"), "In sync");
  return h("span", { class: "chip " + (c.driftRisk.destructive ? "warn" : "") }, icon("changes"), plural(c.drift, "change"));
}
