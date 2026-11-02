// Managed clusters: the Kafka clusters the operator runs on Kubernetes.
// Create one, roll its brokers, size it by hand or let it follow load.

import { h } from "../lib/h.js";
import { icon } from "../lib/icons.js";
import { rate, plural, ago } from "../lib/fmt.js";
import { lineChart, sparkline } from "../lib/charts.js";
import { state, changed, api, poll, toast } from "../lib/store.js";
import { hero, link, empty, loading, copyCode, navigate } from "./common.js";

export const serviceHref = (name) => "/manage/" + encodeURIComponent(name);

const phaseWord = {
  provisioning: "Provisioning", ready: "Ready", rolling: "Rolling", scaling: "Scaling", degraded: "Degraded", failed: "Failed",
};
const phaseTone = { provisioning: "info", ready: "ok", rolling: "info", scaling: "info", degraded: "warn", failed: "crit" };
const phaseIcon = { provisioning: "connecting", ready: "ok", rolling: "connecting", scaling: "connecting", degraded: "warn", failed: "crit" };

function phaseChip(s) {
  return h("span", { class: "chip " + phaseTone[s.phase] }, icon(phaseIcon[s.phase] || "info"), phaseWord[s.phase] || s.phase);
}

// busy is whether a change is under way, so the page offers no others.
const busy = (s) => s.phase === "provisioning" || s.phase === "rolling" || s.phase === "scaling";
const writable = () => !!state.meta?.allowApply;
const last = (s) => (s.load.length ? s.load[s.load.length - 1].v : null);
const capacity = (s) => (s.autoscale ? s.brokers.desired * s.autoscale.target : null);

function sizing(s) {
  if (!s.autoscale) return plural(s.brokers.desired, "broker");
  const a = s.autoscale;
  return `${a.min}–${a.max} brokers at ${rate(a.target)}/s each`;
}

// nodes is one pip per broker, shaped and coloured by its state.
function nodes(s, big) {
  return h("div", { class: "pips" + (big ? " big" : ""), role: "list", "aria-label": `${s.brokers.ready} of ${s.brokers.desired} brokers ready` },
    s.brokers.nodes.map((n) => h("span", {
      key: n.id, role: "listitem", class: "pip st-" + n.state, title: `Broker ${n.id}: ${n.state}`,
    }, big ? [h("b", null, n.id), h("small", null, n.state)] : null)));
}

// ─────────────────────────── list ───────────────────────────

export function managePage() {
  const v = state.services;
  if (!v) return loading();
  const list = v.services;
  const attention = list.filter((s) => s.phase === "degraded" || s.phase === "failed");
  const moving = list.filter(busy);
  return [
    hero({
      eyebrow: "Managed clusters · " + v.where,
      title: !list.length ? h("span", null, "No managed ", h("span", { class: "accent" }, "clusters"), " yet.")
        : attention.length ? h("span", null, h("span", { class: "accent" }, plural(attention.length, "cluster")), " need", attention.length === 1 ? "s" : "", " attention.")
          : moving.length ? h("span", null, plural(list.length, "cluster"), ", ", h("span", { class: "accent" }, `${moving.length} changing`), ".")
            : h("span", null, plural(list.length, "cluster"), ", ", h("span", { class: "accent" }, "all ready"), "."),
      lede: "Each is a KafkaService the operator turns into a Strimzi cluster. It restarts brokers one at a time on request and, with autoscaling on, adds brokers as load grows and removes them once it has stayed low, moving partitions first.",
      actions: writable() ? [h("button", { class: "btn primary", onclick: () => openCreate() }, icon("plus"), "New cluster")] : [],
    }),
    !list.length ? h("section", { class: "card" }, empty("cluster", "Nothing to manage yet",
      "Create a cluster here, or apply a KafkaService with kubectl.", copyCode("kubectl apply -f deploy/operator/example.yaml"))) : null,
    list.length ? h("section", { class: "card" },
      h("table", { class: "data svc-table" },
        h("thead", null, h("tr", null, ["Cluster", "State", "Brokers", "Load", "Sizing", "Last change"].map((t) => h("th", { key: t }, t)))),
        h("tbody", null, list.map((s) => {
          const l = last(s), cap = capacity(s);
          return h("tr", { key: s.name, class: "clickable", onclick: (e) => { if (!e.target.closest("a")) navigate(serviceHref(s.name)); } },
            h("td", null, link(serviceHref(s.name), { class: "svc-name" }, s.name), h("div", { class: "muted mono sm" }, "Kafka " + s.version + " · " + s.storage)),
            h("td", null, phaseChip(s), s.message ? h("div", { class: "muted sm" }, s.message) : null),
            h("td", null, h("div", { class: "num" }, `${s.brokers.ready}/${s.brokers.desired}`), nodes(s)),
            h("td", { class: "svc-load" }, s.load.length > 1 ? sparkline("svc-" + s.name, s.load.slice(-60).map((p) => ({ t: p.t, v: p.v }))) : null,
              h("div", { class: "num" }, l == null ? "—" : rate(l) + "/s", cap ? h("span", { class: "muted" }, ` · ${Math.round((100 * l) / cap)}% of capacity`) : null)),
            h("td", null, s.autoscale ? h("span", { class: "chip info" }, icon("bolt"), "Autoscale") : h("span", { class: "chip" }, "Fixed"), h("div", { class: "muted sm" }, sizing(s))),
            h("td", { class: "muted sm" }, s.history[0] ? [h("div", null, s.history[0].detail), h("div", null, ago(Date.parse(s.history[0].at)))] : "—"));
        })))) : null,
  ];
}

// ─────────────────────────── one cluster ───────────────────────────

export function servicePage(name) {
  const v = state.services;
  if (!v) return loading();
  const s = v.services.find((x) => x.name === name);
  if (!s) return empty("cluster", `No managed cluster named ${name}`, "It isn't in " + v.where + ".", link("/manage", { class: "btn" }, "All managed clusters"));
  const monitored = v.monitored.includes(s.name);
  const fact = (label, value, mono) => h("div", { class: "fact" }, h("dt", null, label), h("dd", { class: mono ? "mono" : "num" }, value));
  return [
    hero({
      eyebrow: "Managed cluster" + (s.namespace ? " · namespace " + s.namespace : ""),
      title: s.name,
      // The callout below carries the message while anything is under way.
      lede: h("span", { class: "svc-phase" }, phaseChip(s), s.message && s.phase === "ready" ? h("span", { class: "muted" }, s.message) : null),
      actions: [
        monitored ? link("/clusters/" + encodeURIComponent(s.name), { class: "btn outline" }, icon("overview"), "Topics and consumers") : null,
        writable() ? h("button", { class: "btn outline", disabled: busy(s), title: busy(s) ? "Wait for the current change to finish" : null, onclick: () => openRoll(s) }, icon("connecting"), "Restart brokers") : null,
      ],
      below: h("dl", { class: "facts" },
        fact("Kafka", s.version),
        fact("Brokers", `${s.brokers.ready} of ${s.brokers.desired} ready`),
        fact("Disk per broker", s.storage),
        fact("Bootstrap", s.bootstrap || "—", true),
        fact("Created", ago(Date.parse(s.created)))),
    }),
    progress(s),
    h("div", { class: "grid-2" },
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("activity"), "Load and capacity"),
          h("div", { class: "right series-legend" },
            h("span", null, h("i", { class: "sw", style: { "--c": "var(--series-produced)" } }), "Records a second"),
            capacity(s) ? h("span", null, h("i", { class: "sw dashed", style: { "--c": "var(--text-subtle)" } }), "Capacity now") : null)),
        h("div", { class: "card-sub" }, s.autoscale ? `Capacity is ${plural(s.brokers.desired, "broker")} × ${rate(s.autoscale.target)}/s, the most autoscaling lets each broker take` : "Produced across every topic, measured from partition high-water marks"),
        h("div", { class: "card-body" }, s.load.length > 1 ? loadChart(s) : h("div", { class: "muted" }, "Measured once the cluster is serving."))),
      sizeCard(s)),
    h("div", { class: "grid-2 mt" },
      h("section", { class: "card" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("broker"), "Brokers"),
          h("span", { class: "right muted" }, "plus 3 KRaft controllers")),
        h("div", { class: "card-body" }, nodes(s, true))),
      h("section", { class: "card fit" },
        h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("file"), "History")),
        s.history.length ? h("ul", { class: "svc-history" }, s.history.map((e, i) =>
          h("li", { key: i + e.at, class: "k-" + e.kind },
            h("span", { class: "dot" }),
            h("div", null, h("b", null, e.kind), " ", e.detail),
            h("span", { class: "muted mono sm" }, ago(Date.parse(e.at)))))) : h("div", { class: "card-body muted" }, "Nothing yet."))),
  ];
}

function loadChart(s) {
  const pts = s.load.slice(-120);
  const series = [{ label: "Load", pts, color: "var(--series-produced)" }];
  const cap = capacity(s);
  if (cap) series.push({ label: "Capacity", pts: pts.map((p) => ({ t: p.t, v: cap })), color: "var(--text-subtle)" });
  return lineChart("svc-load-" + s.name, series, { format: rate, unit: "/s", height: 220 });
}

// progress is the change under way, if any.
function progress(s) {
  if (s.phase === "failed") {
    return h("div", { class: "callout crit mb" }, icon("crit"), h("div", { class: "body" }, h("b", null, "The operator can't make this cluster match its spec. "), s.message));
  }
  if (!busy(s) && s.phase !== "degraded") return null;
  let done = s.brokers.ready, total = s.brokers.desired;
  if (s.roll) ({ done, total } = s.roll);
  const what = { provisioning: "Creating the cluster", rolling: "Restarting brokers one at a time", scaling: "Resizing", degraded: "Brokers missing" }[s.phase];
  return h("div", { class: "callout " + (s.phase === "degraded" ? "warn" : "info") + " mb" }, icon(s.phase === "degraded" ? "warn" : "connecting"),
    h("div", { class: "body" },
      h("div", { class: "row" }, h("b", null, what + ". "), h("span", null, s.message || ""), h("span", { class: "right num" }, `${done} / ${total}`)),
      h("div", { class: "progress" }, h("i", { style: { width: total ? (100 * done) / total + "%" : "0%" } }))));
}

// ─────────────────────────── sizing ───────────────────────────

// The size form keeps a draft per cluster, so a refresh mid-edit
// doesn't throw away what's been typed.
function draft(s) {
  const d = (state.ui.size ||= {});
  if (!d[s.name]) {
    const a = s.autoscale;
    d[s.name] = {
      mode: a ? "auto" : "fixed", brokers: s.brokers.desired,
      min: a?.min ?? Math.max(3, Math.min(s.brokers.desired, 3)), max: a?.max ?? Math.max(s.brokers.desired, 6),
      target: a?.target ?? 10000, after: a?.scaleDownAfter ?? "10m",
    };
  }
  return d[s.name];
}

function sizeCard(s) {
  const d = draft(s);
  const set = (k) => (e) => { d[k] = e.target.type === "number" ? Number(e.target.value) : e.target.value; d.dirty = true; changed(); };
  const num = (k, label, extra = {}) => h("label", { class: "field" }, h("span", null, label),
    h("input", { class: "input", type: "number", value: d[k], oninput: set(k), disabled: !writable(), ...extra }));
  const save = async () => {
    d.busy = true; d.error = null; changed();
    try {
      const n = encodeURIComponent(s.name);
      if (d.mode === "auto") {
        await post(`/services/${n}/autoscale`, { autoscale: { min: d.min, max: d.max, target: d.target, scaleDownAfter: d.after } });
      } else {
        await post(`/services/${n}/scale`, { brokers: d.brokers });
      }
      delete state.ui.size[s.name];
      toast({ severity: "info", kind: "Resized", cluster: s.name, text: d.mode === "auto" ? `autoscaling ${d.min}–${d.max} brokers` : plural(d.brokers, "broker") });
      poll(true);
    } catch (e) {
      d.error = e.message;
    }
    d.busy = false;
    changed();
  };
  return h("section", { class: "card fit" },
    h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("bolt"), "Size"),
      h("div", { class: "right" }, h("div", { class: "seg", role: "group", "aria-label": "Sizing" },
        [["auto", "Autoscale"], ["fixed", "Fixed"]].map(([id, label]) =>
          h("button", { key: id, "aria-pressed": d.mode === id ? "true" : "false", disabled: !writable(), onclick: () => { d.mode = id; d.dirty = true; changed(); } }, label))))),
    h("div", { class: "card-body" },
      d.mode === "auto"
        ? [h("div", { class: "fields" },
          num("min", "Fewest brokers", { min: 1, max: 30 }),
          num("max", "Most brokers", { min: 1, max: 30 }),
          num("target", "Records a second per broker", { min: 1, step: 1000 }),
          h("label", { class: "field" }, h("span", null, "Remove brokers after load is low for"),
            h("input", { class: "input", value: d.after, oninput: set("after"), disabled: !writable(), spellcheck: "false" }))),
        s.autoscale && s.reason ? h("p", { class: "muted sm reason" }, icon("info"), h("span", null, h("b", null, "Now: "), s.reason)) : null]
        : [h("div", { class: "fields" }, num("brokers", "Brokers", { min: 1, max: 30 })),
          h("p", { class: "muted sm reason" }, icon("info"), d.brokers < s.brokers.desired
            ? "Partitions move off the brokers being removed before they stop."
            : d.brokers > s.brokers.desired ? "Partitions move on to the new brokers once they're up." : "The cluster stays this size whatever its load.")],
      d.error ? h("div", { class: "callout crit mt-sm" }, icon("crit"), h("div", { class: "body" }, d.error)) : null,
      writable() ? h("div", { class: "row mt-sm" },
        h("button", { class: "btn primary", disabled: !d.dirty || d.busy || busy(s), onclick: save }, d.busy ? "Saving…" : "Save"),
        d.dirty ? h("button", { class: "btn ghost", onclick: () => { delete state.ui.size[s.name]; changed(); } }, "Reset") : null,
        busy(s) ? h("span", { class: "muted sm" }, "Wait for the current change to finish") : null)
        : h("p", { class: "muted sm" }, "Read-only console: change the KafkaService with kubectl, or start the console with --allow-apply.")));
}

async function post(path, body) {
  return api(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
}

// ─────────────────────────── dialogs ───────────────────────────

function openCreate() {
  state.ui.modal = { kind: "create", name: "", version: "4.2.0", brokers: 3, storage: "100Gi", auto: false, min: 3, max: 6, target: 10000, after: "10m" };
  changed();
}

function openRoll(s) {
  state.ui.modal = { kind: "roll", cluster: s.name, brokers: s.brokers.desired, typed: "" };
  changed();
}

export function manageModal() {
  const m = state.ui.modal;
  if (!m || (m.kind !== "create" && m.kind !== "roll")) return null;
  const close = () => { state.ui.modal = null; changed(); };
  const run = async (f) => {
    m.busy = true; m.error = null; changed();
    try {
      await f();
      state.ui.modal = null;
      poll(true);
    } catch (e) {
      m.error = e.message;
      m.busy = false;
    }
    changed();
  };
  const set = (k) => (e) => { m[k] = e.target.type === "number" ? Number(e.target.value) : e.target.type === "checkbox" ? e.target.checked : e.target.value; changed(); };
  const field = (k, label, attrs = {}) => h("label", { class: "field" }, h("span", null, label),
    h("input", { class: "input", value: m[k], oninput: set(k), spellcheck: "false", autocomplete: "off", ...attrs }));
  let body, foot;
  if (m.kind === "roll") {
    const ok = m.typed === m.cluster;
    body = [
      h("h2", { id: "modal-title" }, `Restart every broker on ${m.cluster}?`),
      h("p", null, `Strimzi restarts the ${m.brokers} brokers and 3 controllers one at a time, waiting for each to rejoin and its partitions to catch up before the next. Clients reconnect on their own; nothing is lost.`),
      h("p", null, "Type ", h("b", { class: "mono" }, m.cluster), " to confirm."),
      h("input", {
        class: "input", value: m.typed, "aria-label": "Cluster name", spellcheck: "false", autocomplete: "off",
        ref: (el) => { if (!m.focused) { m.focused = true; setTimeout(() => el.focus()); } },
        oninput: set("typed"),
        onkeydown: (e) => { if (e.key === "Enter" && ok && !m.busy) go(); if (e.key === "Escape") close(); },
      }),
    ];
    const go = () => run(() => post(`/services/${encodeURIComponent(m.cluster)}/roll`, { confirm: m.typed }));
    foot = h("button", { class: "btn primary", disabled: !ok || m.busy, onclick: go }, m.busy ? "Starting…" : "Restart brokers");
  } else {
    const go = () => run(() => post("/services", {
      name: m.name, version: m.version, brokers: m.brokers, storage: m.storage,
      autoscale: m.auto ? { min: m.min, max: m.max, target: m.target, scaleDownAfter: m.after } : undefined,
    }).then(() => navigate(serviceHref(m.name))));
    body = [
      h("h2", { id: "modal-title" }, "New Kafka cluster"),
      h("p", null, "Three KRaft controllers and the brokers below, in the console's namespace. It takes a few minutes for the brokers to start."),
      h("div", { class: "fields" },
        field("name", "Name", { placeholder: "orders", ref: (el) => { if (!m.focused) { m.focused = true; setTimeout(() => el.focus()); } } }),
        h("label", { class: "field" }, h("span", null, "Kafka version"),
          h("select", { class: "input", value: m.version, onchange: set("version") }, ["4.2.0", "4.1.1", "4.0.1"].map((v) => h("option", { key: v, value: v, selected: m.version === v }, v)))),
        field("brokers", m.auto ? "Brokers to start with" : "Brokers", { type: "number", min: 1, max: 30 }),
        field("storage", "Disk per broker", { placeholder: "100Gi" })),
      h("label", { class: "check" }, h("input", { type: "checkbox", checked: m.auto, onchange: set("auto") }), "Autoscale on load"),
      m.auto ? h("div", { class: "fields" },
        field("min", "Fewest brokers", { type: "number", min: 1, max: 30 }),
        field("max", "Most brokers", { type: "number", min: 1, max: 30 }),
        field("target", "Records a second per broker", { type: "number", min: 1, step: 1000 }),
        field("after", "Remove brokers after low load for")) : null,
    ];
    foot = h("button", { class: "btn primary", disabled: !m.name || m.busy, onclick: go }, m.busy ? "Creating…" : "Create cluster");
  }
  return [
    h("div", { class: "scrim", onclick: close }),
    h("div", { class: "modal" + (m.kind === "create" ? " wide" : ""), role: "dialog", "aria-modal": "true", "aria-labelledby": "modal-title" },
      h("div", { class: "modal-body" }, body,
        m.error ? h("div", { class: "callout crit mt-sm" }, icon("crit"), h("div", { class: "body" }, m.error)) : null),
      h("div", { class: "modal-foot" }, h("button", { class: "btn ghost", onclick: close }, "Cancel"), foot)),
  ];
}

