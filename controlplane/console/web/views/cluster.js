import { h } from "../lib/h.js";
import { icon, status } from "../lib/icons.js";
import { compact, rate, int, bytes, duration, plural, quota, ago, age, pct, money, hm } from "../lib/fmt.js";
import { bar, lineChart, uptimeStrip, ring, squarify } from "../lib/charts.js";
import { locate } from "../lib/geo.js";
import { state, changed } from "../lib/store.js";
import { hero, stat, feed, link, clusterHref, envBadge, empty, loading, kpi, delta, insightList, inRange, rangeSwitch, meter, split, share } from "./common.js";
import { gitopsTab } from "./gitops.js";
import { navigate } from "../lib/router.js";

const tabs = [
  ["overview", "Overview", "overview"],
  ["topics", "Topics", "topics"],
  ["consumers", "Consumers", "consumers"],
  ["access", "Access", "access"],
  ["schemas", "Schemas", "schemas"],
  ["gitops", "GitOps", "gitops"],
];

export function clusterPage(name, tab, topic) {
  const v = state.cluster[name];
  const card = v?.card || state.fleet?.clusters.find((c) => c.name === name);
  if (!card) {
    if (state.fleet) return empty("cluster", `No cluster named ${name}`, "It isn't in this console's fleet.", link("/", { class: "btn" }, "Back to the fleet"));
    return loading();
  }
  const counts = {
    topics: card.topics, consumers: card.groups, access: card.acls,
    schemas: card.hasRegistry ? card.schemas : null, gitops: card.gitops ? card.drift : null,
  };
  const body = !v ? loading()
    : tab === "topics" ? topicsTab(v, name)
      : tab === "consumers" ? consumersTab(v)
        : tab === "access" ? accessTab(name)
          : tab === "schemas" ? schemasTab(name)
            : tab === "gitops" ? gitopsTab(name)
              : overviewTab(v);
  const at = locate(card.region);
  return [
    hero({
      eyebrow: ["Cluster", envBadge(card.env), card.region ? h("span", null, card.region + (at ? " · " + at.place : "")) : null],
      title: card.name,
      lede: status(card.status, card.reason),
      actions: [
        card.gitops && card.drift ? link(clusterHref(name, "gitops"), { class: "btn primary" }, icon("changes"), "Review " + plural(card.drift, "change")) : null,
      ],
      below: [
        h("dl", { class: "facts" },
          fact("Brokers", card.brokers ? int(card.brokers) : null),
          fact("Controller", card.controller >= 0 ? "#" + card.controller : null),
          fact("Cluster ID", card.clusterId || null, true),
          fact("Available, 1h", card.availability >= 0 ? pct(card.availability) : null),
          fact("Est. cost", card.cost ? money(card.cost, state.fleet?.pricing?.currency) + " / mo" : null)),
        h("nav", { class: "tabs", "aria-label": "Cluster sections" }, tabs.map(([id, label, ic]) =>
          link(clusterHref(name, id), { key: id, "aria-current": tab === id ? "page" : null },
            icon(ic, "nav-icon"), label,
            counts[id] != null ? h("span", { class: "nav-count" + (id === "gitops" && counts[id] ? " info" : "") }, int(counts[id])) : null))),
      ],
    }),
    card.status === "unreachable" ? unreachable(card) : null,
    body,
    topic ? topicDrawer(name, topic) : null,
  ];
}

function unreachable(card) {
  return h("div", { class: "callout crit", style: { "margin-bottom": "16px" } }, icon("offline"),
    h("div", { class: "body" },
      h("b", null, "The brokers aren't answering. "),
      "kaas keeps retrying; the last snapshot, if there was one, is folded away below.",
      card.error ? h("div", { class: "code" }, h("code", null, card.error)) : null));
}

// ─────────────────────────── overview ───────────────────────────

const S1 = "var(--series-produced)", S2 = "var(--series-consumed)";

// fact is one label and value in a page header's facts row.
function fact(label, value, mono) {
  return value == null ? null : h("div", { class: "fact" }, h("dt", null, label), h("dd", { class: mono ? "mono" : "num" }, value));
}

function overviewTab(v) {
  const c = v.card;
  const tone = c.availability < 0 ? "none" : c.availability >= 99.9 ? "ok" : c.availability >= 50 ? "warn" : "crit";
  const summary = h("section", { class: "card", "aria-label": "Cluster summary" },
    h("div", { class: "band" },
      h("div", { class: "gauge" },
        h("div", { class: "ring-wrap" }, ring(c.availability, { tone, size: 112, stroke: 9 }),
          h("div", { class: "ring-label" }, h("div", null, h("b", null, pct(c.availability)), h("span", null, "available")))),
        h("div", null,
          h("h3", null, statusLine(c)),
          h("p", null, c.urp + c.offline ? `${int(c.urp + c.offline)} of ${int(c.partitions)} partitions short of replicas.` : `All ${int(c.partitions)} partitions fully replicated.`))),
      kpi({ label: "Produced", color: S1, value: h("span", { class: "num" }, rate(c.throughput)), unit: "rec/s", foot: [delta(c.history), h("span", null, "into " + plural(c.topics, "topic"))], spark: inRange(c.history), sparkId: "c-in-" + c.name }),
      kpi({ label: "Consumed", color: S2, value: h("span", { class: "num" }, rate(c.consumeRate)), unit: "rec/s", foot: [delta(c.consumed), h("span", null, "by " + plural(c.groups, "group"))], spark: inRange(c.consumed), sparkId: "c-out-" + c.name }),
      kpi({
        label: "Data freshness", ic: "connecting", value: h("span", { class: "num" }, c.freshness < 0 ? "—" : age(c.freshness)), unit: c.freshness > 0 ? "behind" : "",
        foot: c.freshness > 0 && c.freshGroup ? h("span", null, "worst: ", h("b", { class: "mono" }, c.freshGroup)) : "every consumer is caught up",
        viz: c.freshness >= 0 ? meter(c.freshness, 60, { label: "stale after 1m", over: "past target" }) : null,
      }),
      kpi({ label: "Retained", ic: "file", value: h("span", { class: "num" }, compact(c.records)), unit: "records", foot: c.disk ? bytes(c.disk) + " on disk" : "between log start and end", viz: retainedSplit(v) })));
  const lastHour = h("section", { class: "card mt" },
    h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("ok"), "Last hour"),
      h("span", { class: "right muted" }, "a bar a minute · hover for the time and status")),
    h("div", { class: "card-body" }, uptimeStrip(c.uptime), uptimeScale(c.uptime)));
  const traffic = h("section", { class: "card" },
    h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("activity"), "Traffic"),
      h("div", { class: "right" },
        h("div", { class: "series-legend" },
          h("span", null, h("i", { class: "sw", style: { "--c": S1 } }), "Produced"),
          h("span", null, h("i", { class: "sw dashed", style: { "--c": S2 } }), "Consumed")),
        rangeSwitch())),
    h("div", { class: "card-sub" }, "Records a second"),
    h("div", { class: "card-body" },
      lineChart("g-" + c.name, [
        { label: "Produced", pts: inRange(c.history), color: S1 },
        { label: "Consumed", pts: inRange(c.consumed), color: S2 },
      ], { format: rate, unit: "/s", height: 220 })));
  const recs = h("section", { class: "card fit" },
    h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("bolt"), "Recommendations"),
      v.insights.length ? h("span", { class: "right" }, h("span", { class: "nav-count" }, v.insights.length)) : null),
    h("div", { class: "mt" }, insightList(v.insights, { showCluster: false, limit: 5, expanded: c.status === "unreachable" })));
  const more = [
    h("div", { class: "grid-2 mt" },
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("topics"), "Where the traffic goes"),
          h("span", { class: "right muted" }, "topics sized by records a second")),
        topicMap(v)),
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("broker"), "Brokers"),
          h("span", { class: "right muted" }, "share of partition leaders")),
        rack(v),
        h("div", { class: "card-foot" },
          icon("lock"), h("span", { class: "mono" }, `${int(v.budget.granted)} requests sent · ${int(v.budget.denied)} skipped`),
          h("span", { class: "right" }, v.snapshotAt ? "snapshot " + ago(v.snapshotAt) : "")))),
    h("section", { class: "card mt" },
      h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("activity"), "Recent events")),
      h("div", { style: { "margin-top": "8px" } }, feed(v.events, { showCluster: false, limit: 6 })),
      v.events.length > 6 ? h("div", { class: "card-foot" }, link("/activity", { class: "card-link" }, "All activity", icon("arrow"))) : null),
  ];
  // A cluster that isn't answering has nothing live to show: lead with
  // what's wrong and what to do, and fold the last snapshot away.
  if (c.status === "unreachable") {
    return [
      recs,
      lastHour,
      h("details", { class: "card mt stale" },
        h("summary", { class: "card-head" }, h("h3", { class: "card-title" }, icon("file"), "Last known state"),
          h("span", { class: "right muted" }, v.snapshotAt ? "snapshot " + ago(v.snapshotAt) : "no snapshot yet"), icon("chevron", "fold")),
        h("div", { class: "stale-body" }, summary, more)),
    ];
  }
  return [
    summary,
    lastHour,
    h("div", { class: "grid-2 mt" }, traffic, recs),
    ...more,
  ];
}

function statusLine(c) {
  return { healthy: "Healthy and serving", degraded: "Serving, with replicas behind", critical: "Partitions are offline", unreachable: "Not answering", connecting: "Connecting" }[c.status] || c.status;
}

function uptimeScale(slots) {
  if (!slots?.length) return null;
  return h("div", { class: "uptime-scale" }, h("span", null, hm(slots[0].t)), h("span", null, hm(slots[Math.floor(slots.length / 2)].t)), h("span", null, "now"));
}

// retainedSplit is which topics hold the retained records: the two
// largest by name, then the rest.
function retainedSplit(v) {
  const ts = v.topics.filter((t) => !t.internal && t.records > 0).sort((a, b) => b.records - a.records);
  if (!ts.length) return null;
  const parts = ts.slice(0, 2).map((t) => ({ label: t.name, value: t.records, text: compact(t.records) + " records" }));
  const rest = ts.slice(2).reduce((a, t) => a + t.records, 0);
  if (rest) parts.push({ label: "other", value: rest, text: compact(rest) + " records" });
  return split(parts, h("span", null, h("b", { class: "mono" }, ts[0].name), " holds ", share(ts[0].records / ts.reduce((a, t) => a + t.records, 0))));
}

function topicMap(v) {
  const items = v.topics.filter((t) => !t.internal && t.rate > 0).map((t) => ({ key: t.name, value: t.rate, t }));
  if (!items.length) return h("div", { class: "card-body muted" }, "No topic is receiving records right now.");
  const max = Math.max(...items.map((i) => i.value));
  const cells = squarify(items, 100, 100);
  return h("div", { class: "tmap", role: "list", "aria-label": "Topics by records a second" }, cells.map((r) => {
    const k = 10 + Math.round(30 * Math.sqrt(r.value / max));
    const tiny = r.w < 9 || r.h < 14;
    return link(clusterHref(v.card.name, "topics", r.key), {
      key: "tm-" + r.key, class: "tm" + (tiny ? " tiny" : ""), role: "listitem",
      style: { left: r.x + "%", top: r.y + "%", width: r.w + "%", height: r.h + "%", "--k": k + "%" },
      title: `${r.key}: ${rate(r.value)} records a second, ${int(r.t.partitions)} partitions`,
    }, h("span", { class: "n" }, r.key), h("span", { class: "v" }, rate(r.value) + "/s"));
  }));
}

function rack(v) {
  const leaders = v.brokers.reduce((a, b) => a + b.leaders, 0) || 1;
  if (!v.brokers.length) return h("div", { class: "card-body muted" }, "No brokers in the last snapshot.");
  return h("ul", { class: "rack" }, v.brokers.map((b) => h("li", { key: "b" + b.id, class: "status-" + (b.online ? "healthy" : "critical") },
    h("div", { class: "who" }, h("span", { class: "dot ring" }),
      h("div", null, "Broker " + b.id, h("small", null, b.controller ? "controller" : b.rack || (b.online ? "online" : "offline")))),
    h("div", { class: "share" },
      h("div", { class: "lbl" }, h("span", null, `${int(b.leaders)} leaders · ${int(b.replicas)} replicas`), h("span", { class: "num" }, Math.round((100 * b.leaders) / leaders) + "%")),
      bar(b.leaders, leaders, "s1")),
    h("div", { class: "disk num muted" }, b.behind ? h("span", { class: "chip warn" }, `${b.behind} behind`) : b.disk ? bytes(b.disk) : "—"))));
}

// ─────────────────────────── topics ───────────────────────────

const topicSorts = { rate: (t) => -t.rate, records: (t) => -t.records, partitions: (t) => -t.partitions, name: null };

function topicsTab(v, cluster) {
  const ui = state.ui;
  const q = ui.topicQuery.trim().toLowerCase();
  let rows = v.topics.filter((t) => (ui.showInternal || !t.internal) && (!q || t.name.toLowerCase().includes(q)));
  const key = topicSorts[ui.topicSort];
  rows = [...rows].sort((a, b) => (key ? key(a) - key(b) : 0) || a.name.localeCompare(b.name));
  const maxRate = Math.max(1, ...rows.map((t) => t.rate));
  const sortBtn = (id, label) => h("button", { onclick: () => { ui.topicSort = id; changed(); }, "aria-pressed": ui.topicSort === id ? "true" : "false" }, label);
  return [
    h("div", { class: "toolbar" },
      h("label", { class: "search" }, icon("search"), h("span", { class: "sr-only" }, "Filter topics"),
        h("input", { class: "input", placeholder: "Filter topics", value: ui.topicQuery, oninput: (e) => { ui.topicQuery = e.target.value; changed(); } })),
      h("div", { class: "seg", role: "group", "aria-label": "Sort by" }, sortBtn("rate", "Busiest"), sortBtn("records", "Largest"), sortBtn("partitions", "Partitions"), sortBtn("name", "Name")),
      h("div", { class: "right" },
        h("label", { class: "row dim", style: { "font-size": "13px" } },
          h("input", { type: "checkbox", checked: ui.showInternal, onchange: (e) => { ui.showInternal = e.target.checked; changed(); } }), "Internal topics"),
        h("span", { class: "muted" }, plural(rows.length, "topic")))),
    h("section", { class: "card" }, rows.length === 0
      ? empty("topics", q ? "No topic matches" : "No topics yet", q ? `Nothing on ${cluster} contains “${ui.topicQuery}”.` : "Create one with kaas topics create, or declare it in this cluster's desired-state file.")
      : h("div", { class: "table-wrap" }, h("table", { class: "data" },
        h("thead", null, h("tr", null, h("th", null, "Topic"), h("th", null, "Throughput"), h("th", { class: "r" }, "Partitions"), h("th", { class: "r" }, "RF"), h("th", { class: "r" }, "Records"), h("th", { class: "r" }, "Readers"), h("th", null, "Health"))),
        h("tbody", null, rows.map((t) => h("tr", {
          key: t.name, class: "clickable", tabindex: "0",
          onclick: () => navigate(clusterHref(cluster, "topics", t.name)),
          onkeydown: (e) => { if (e.key === "Enter") navigate(clusterHref(cluster, "topics", t.name)); },
        },
        h("td", { class: "name mono" }, t.name, t.internal ? h("span", { class: "sub" }, "internal") : null),
        h("td", null, h("div", { class: "bar-cell" }, bar(t.rate, maxRate), h("span", { class: "num" }, rate(t.rate) + "/s"))),
        h("td", { class: "r num" }, int(t.partitions)),
        h("td", { class: "r num" }, int(t.replication)),
        h("td", { class: "r num" }, compact(t.records)),
        h("td", { class: "r num" }, int(t.groups)),
        h("td", null, health(t)))))))),
  ];
}

function health(t) {
  if (t.offline) return h("span", { class: "chip crit" }, icon("crit"), `${t.offline} offline`);
  if (t.urp) return h("span", { class: "chip warn" }, icon("warn"), `${t.urp} under-replicated`);
  return h("span", { class: "chip ok" }, icon("ok"), "In sync");
}

function topicDrawer(cluster, name) {
  const v = state.topic[cluster + "/" + name];
  const close = () => navigate(clusterHref(cluster, "topics"));
  let body;
  if (!v) body = loading();
  else if (v.missing) body = empty("topics", "No such topic", `${name} isn't on ${cluster}; it may have just been deleted.`);
  else {
    const r = v.row;
    body = [
      h("section", { class: "stats" },
        stat("Throughput", h("span", { class: "num" }, rate(r.rate)), { unit: "rec/s", hot: true }),
        stat("Records", h("span", { class: "num" }, compact(r.records))),
        stat("Partitions", h("span", { class: "num" }, int(r.partitions)), { note: `replication ${r.replication}` })),
      h("div", null,
        h("h2", { class: "section", style: { margin: "0 0 12px" } }, "Partitions",
          h("span", { class: "right legend" }, h("span", null, h("i", { style: { background: "var(--text)" } }), "leader"), h("span", null, h("i", { style: { background: "var(--success)" } }), "in sync"), h("span", null, h("i", { style: { background: "var(--danger)" } }), "out of sync"))),
        h("div", { class: "pmap" }, v.partitions.map((p) => {
          const out = p.replicas.filter((x) => !p.isr.includes(x));
          const cls = p.leader < 0 ? "offline" : out.length ? "urp" : "";
          const n = p.end >= 0 && p.start >= 0 ? p.end - p.start : null;
          return h("div", { key: "p" + p.id, class: "pcell " + cls, title: `partition ${p.id}: leader ${p.leader}, replicas ${p.replicas.join(", ")}, in sync ${p.isr.join(", ")}` },
            h("div", { class: "pid" }, "#" + p.id, h("span", { class: "lead" }, p.leader < 0 ? "no leader" : "on " + p.leader)),
            h("div", { class: "reps" }, p.replicas.map((x) => h("span", { class: "rep " + (x === p.leader ? "lead" : out.includes(x) ? "out" : "") }))),
            h("div", { class: "cnt" }, n == null ? "—" : compact(n) + " rec"));
        }))),
      h("div", null,
        h("h2", { class: "section", style: { margin: "0 0 12px" } }, "Read by", h("span", { class: "muted" }, plural(v.readers.length, "group"))),
        v.readers.length === 0
          ? h("p", { class: "muted" }, "No consumer group has committed offsets for this topic.")
          : h("div", { class: "card" }, groupTable(v.readers, false))),
    ];
  }
  return [
    h("div", { class: "scrim", onclick: close }),
    h("aside", { class: "drawer", role: "dialog", "aria-modal": "true", "aria-label": "Topic " + name },
      h("div", { class: "drawer-head" },
        h("div", { style: { flex: "1", "min-width": "0" } }, h("div", { class: "eyebrow" }, h("span", { class: "bar" }), "Topic on " + cluster), h("h2", { class: "mono" }, name)),
        h("button", { class: "btn sm ghost", onclick: close, "aria-label": "Close" }, icon("close"))),
      h("div", { class: "drawer-body" }, body)),
  ];
}

// ─────────────────────────── consumers ───────────────────────────

function consumersTab(v) {
  const ui = state.ui;
  const q = ui.groupQuery.trim().toLowerCase();
  const rows = v.groups.filter((g) => !q || g.name.toLowerCase().includes(q) || g.topics.some((t) => t.toLowerCase().includes(q)));
  return [
    h("div", { class: "toolbar" },
      h("label", { class: "search" }, icon("search"), h("span", { class: "sr-only" }, "Filter groups"),
        h("input", { class: "input", placeholder: "Filter groups or topics", value: ui.groupQuery, oninput: (e) => { ui.groupQuery = e.target.value; changed(); } })),
      h("div", { class: "right muted" }, plural(rows.length, "group"), " · most behind first")),
    v.groupsError ? h("div", { class: "callout warn", style: { "margin-bottom": "12px" } }, icon("warn"), h("div", { class: "body" }, "Couldn't read consumer groups: " + v.groupsError)) : null,
    h("section", { class: "card" }, rows.length === 0
      ? empty("consumers", "No consumer groups", "Groups appear once a consumer joins one or commits offsets.")
      : groupTable(rows, true)),
  ];
}

function groupTable(rows, withTopics) {
  const maxLag = Math.max(1, ...rows.map((g) => g.lag));
  return h("div", { class: "table-wrap" }, h("table", { class: "data" },
    h("thead", null, h("tr", null, h("th", null, "Group"), h("th", null, "State"), h("th", { class: "r" }, "Members"), withTopics ? h("th", null, "Topics") : null, h("th", null, "Lag"), h("th", { class: "r" }, "Data age"), h("th", { class: "r" }, "Consuming"), h("th", { class: "r" }, "Caught up in"))),
    h("tbody", null, rows.map((g) => h("tr", { key: g.name },
      h("td", { class: "name" }, g.name, g.error ? h("span", { class: "sub", style: { color: "var(--danger)" } }, g.error) : null),
      h("td", null, stateChip(g.state)),
      h("td", { class: "r num" }, int(g.members)),
      withTopics ? h("td", { class: "mono dim" }, g.topics.join(", ") || "—") : null,
      h("td", null, h("div", { class: "bar-cell" }, bar(g.lag, maxLag, g.catchup < 0 ? "warn" : ""), h("span", { class: "num" }, compact(g.lag)))),
      h("td", { class: "r num", title: "how far behind the newest record its reads are" }, g.behind < 0 ? "—" : age(g.behind)),
      h("td", { class: "r num" }, g.rate > 0 ? rate(g.rate) + "/s" : "—"),
      h("td", { class: "r" }, catchup(g)))))));
}

function stateChip(s) {
  const tone = { Stable: "ok", Empty: "", PreparingRebalance: "warn", CompletingRebalance: "warn", Dead: "crit" }[s] ?? "";
  const word = { PreparingRebalance: "Rebalancing", CompletingRebalance: "Rebalancing" }[s] || s || "Unknown";
  return h("span", { class: "chip " + tone }, word);
}

function catchup(g) {
  if (g.lag <= 0) return h("span", { class: "chip ok" }, icon("ok"), "Caught up");
  if (g.catchup < 0) return h("span", { class: "chip warn" }, icon("warn"), g.members ? "Not draining" : "No members");
  return h("span", { class: "num" }, duration(g.catchup));
}

// ─────────────────────────── access ───────────────────────────

function accessTab(name) {
  const a = state.access[name];
  if (!a) return loading();
  const q = state.ui.aclQuery.trim().toLowerCase();
  const acls = a.acls.filter((x) => !q || `${x.principal} ${x.name} ${x.operation} ${x.resource}`.toLowerCase().includes(q));
  const byPrincipal = new Map();
  for (const x of acls) {
    if (!byPrincipal.has(x.principal)) byPrincipal.set(x.principal, []);
    byPrincipal.get(x.principal).push(x);
  }
  const noAuth = a.aclsError && /authorizer/i.test(a.aclsError);
  return [
    h("div", { class: "toolbar" },
      h("label", { class: "search" }, icon("search"), h("span", { class: "sr-only" }, "Filter ACLs"),
        h("input", { class: "input", placeholder: "Filter principals, resources, operations", value: state.ui.aclQuery, oninput: (e) => { state.ui.aclQuery = e.target.value; changed(); } })),
      h("div", { class: "right muted" }, `${plural(acls.length, "ACL")} for ${plural(byPrincipal.size, "principal")}`)),
    h("section", { class: "card" },
      h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("access", "nav-icon"), "Access control lists")),
      noAuth ? h("div", { class: "card-body" }, h("div", { class: "callout warn" }, icon("warn"), h("div", { class: "body" }, h("b", null, "No authorizer. "), "This cluster lets every client do everything; ACLs have no effect until an authorizer is configured on the brokers.")))
        : a.aclsError ? h("div", { class: "card-body" }, h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, a.aclsError)))
          : acls.length === 0 ? empty("access", q ? "No ACL matches" : "No ACLs", q ? "Try a principal like User:billing." : "Declare them in the desired-state file and apply the plan.")
            : h("div", { class: "table-wrap mt" }, h("table", { class: "data" },
              h("thead", null, h("tr", null, h("th", null, "Permission"), h("th", null, "Operation"), h("th", null, "Resource"), h("th", null, "Name"), h("th", null, "Host"))),
              h("tbody", null, [...byPrincipal].map(([p, list]) => [
                h("tr", { key: "p:" + p, class: "group-row" }, h("td", { colspan: "5" }, h("span", { class: "row" }, icon("consumers", "nav-icon"), h("span", { class: "mono" }, p), h("span", { class: "muted", style: { "font-weight": "400" } }, plural(list.length, "grant"))))),
                list.map((x) => h("tr", { key: [x.principal, x.resource, x.name, x.pattern, x.operation, x.permission, x.host].join("|") },
                  h("td", null, x.permission === "deny" ? h("span", { class: "chip crit" }, icon("crit"), "Deny") : h("span", { class: "chip ok" }, icon("ok"), "Allow")),
                  h("td", { class: "mono" }, x.operation),
                  h("td", { class: "dim" }, x.resource),
                  h("td", { class: "mono" }, x.name, x.pattern === "prefixed" ? h("span", { class: "chip mono", title: "prefixed: every name starting with this", style: { "margin-left": "6px" } }, "prefix*") : null),
                  h("td", { class: "mono dim" }, x.host === "*" ? "any" : x.host))),
              ]))))),
    h("h2", { class: "section" }, "Client quotas", h("span", { class: "muted" }, "limits Kafka enforces by throttling")),
    h("section", { class: "card" },
      a.quotasError ? h("div", { class: "card-body" }, h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, a.quotasError)))
        : a.quotas.length === 0 ? empty("lock", "No quotas", "Every client can produce and fetch as fast as the brokers allow.")
          : h("div", { class: "table-wrap" }, h("table", { class: "data" },
            h("thead", null, h("tr", null, h("th", null, "Entity"), h("th", null, "Produce"), h("th", null, "Fetch"), h("th", null, "Request time"), h("th", null, "Other"))),
            h("tbody", null, a.quotas.map((x) => {
              const v = x.values;
              const other = Object.keys(v).filter((k) => !["producer_byte_rate", "consumer_byte_rate", "request_percentage"].includes(k));
              return h("tr", { key: x.entity },
                h("td", { class: "mono name" }, x.entity),
                h("td", { class: "num" }, v.producer_byte_rate != null ? quota("producer_byte_rate", v.producer_byte_rate) : "—"),
                h("td", { class: "num" }, v.consumer_byte_rate != null ? quota("consumer_byte_rate", v.consumer_byte_rate) : "—"),
                h("td", { class: "num" }, v.request_percentage != null ? quota("request_percentage", v.request_percentage) : "—"),
                h("td", { class: "mono dim" }, other.map((k) => `${k} ${quota(k, v[k])}`).join(", ") || "—"));
            }))))),
  ];
}

// ─────────────────────────── schemas ───────────────────────────

function schemasTab(name) {
  const s = state.schemas[name];
  if (!s) return loading();
  if (!s.configured) {
    return h("section", { class: "card" }, empty("schemas", "No schema registry", "Add schema_registry: https://… to this cluster in the fleet file to browse its subjects, versions and compatibility here."));
  }
  if (s.error && !s.subjects.length) {
    return h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, h("b", null, "Couldn't read the registry. "), s.error));
  }
  if (!s.subjects.length) return h("section", { class: "card" }, empty("schemas", "No subjects", "The registry is reachable but holds no schemas yet."));
  const sel = s.subjects.find((x) => x.name === state.ui.schemaSel) || s.subjects[0];
  return h("div", { class: "split" },
    h("section", { class: "card" },
      h("div", { class: "card-head" }, h("h3", { class: "card-title" }, plural(s.subjects.length, "subject"))),
      h("ul", { class: "list" }, s.subjects.map((x) => h("li", { key: x.name },
        h("button", { "aria-current": x === sel ? "true" : "false", onclick: () => { state.ui.schemaSel = x.name; changed(); } },
          h("span", { class: "t" }, x.name),
          h("span", { class: "chip mono" }, "v" + x.version),
          h("span", { class: "s" }, typeChip(x.type), x.compatibility.toLowerCase().replace(/_/g, " "))))))),
    h("section", { class: "card" },
      h("div", { class: "card-head", style: { "padding-bottom": "14px", "border-bottom": "1px solid var(--border)" } },
        h("div", null,
          h("h3", { class: "card-title mono" }, sel.name),
          h("div", { class: "row muted wrap", style: { "margin-top": "6px", "font-size": "12px" } },
            typeChip(sel.type), `version ${sel.version} of ${sel.versions}`, "·", `id ${sel.id}`, "·", h("span", { class: "chip" }, sel.compatibility)))),
      h("pre", { class: "schema" }, highlight(pretty(sel.schema, sel.type)))));
}

function typeChip(t) {
  return h("span", { class: "chip mono" }, t.toLowerCase());
}

function pretty(s, type) {
  if (type === "PROTOBUF") return s;
  try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; }
}

// highlight splits JSON or proto text into coloured spans; text nodes only.
function highlight(src) {
  const out = [];
  const re = /("(?:[^"\\]|\\.)*")(\s*:)?|\b(-?\d+(?:\.\d+)?|true|false|null)\b|([{}[\],;=])/g;
  let last = 0, m;
  while ((m = re.exec(src))) {
    if (m.index > last) out.push(src.slice(last, m.index));
    if (m[1]) out.push(h("span", { class: m[2] ? "k" : "s" }, m[1]), m[2] || "");
    else if (m[3]) out.push(h("span", { class: "n" }, m[3]));
    else out.push(h("span", { class: "p" }, m[4]));
    last = re.lastIndex;
  }
  out.push(src.slice(last));
  return out;
}
