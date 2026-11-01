import { h } from "../lib/h.js";
import { icon } from "../lib/icons.js";
import { plural, ago, configValue } from "../lib/fmt.js";
import { state, changed, api, poll } from "../lib/store.js";
import { hero, link, clusterHref, empty, copyCode, envBadge, loading } from "./common.js";

const adds = new Set(["create-topic", "create-acl"]);
const removes = new Set(["delete-acl", "delete-topic", "remove-quota", "unset-config"]);

function opClass(o) {
  if (adds.has(o.kind) || ((o.kind === "set-config" || o.kind === "set-quota") && !o.from)) return "add";
  if (removes.has(o.kind)) return "remove";
  return "change";
}
const sign = { add: "+", change: "~", remove: "−" };

function riskChip(r) {
  const [cls, ic] = { safe: ["ok", "ok"], caution: ["warn", "warn"], destructive: ["crit", "crit"] }[r];
  return h("span", { class: "chip risk " + cls }, icon(ic), r);
}

function shown(o, v) {
  return o.kind.endsWith("config") ? configValue(o.key, v) : v;
}

export function gitopsTab(name) {
  const v = state.plans[name];
  if (!v) return loading();
  if (!v.configured) {
    return h("section", { class: "card" }, empty("gitops", "Not managed from git yet",
      "Give this cluster a desired-state file (desired: in the fleet file) declaring its topics, configs, ACLs and quotas. The console then shows how the cluster differs, checked by the controller.",
      copyCode("kaas plan -f gitops/" + name + ".yaml -b <brokers>")));
  }
  if (v.error && !v.plan) {
    return h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, h("b", null, "Couldn't plan. "), v.error));
  }
  const p = v.plan;
  if (!p) return loading();
  const n = { safe: 0, caution: 0, destructive: 0 };
  for (const o of p.ops) n[o.risk]++;
  const file = (v.desired || "").split("/").pop();
  const results = state.ui.results[name];
  return h("div", { class: "stack" },
    v.error ? h("div", { class: "callout warn" }, icon("warn"), h("div", { class: "body" }, h("b", null, "The latest plan failed; showing the previous one. "), v.error)) : null,
    results ? resultsCard(name, results) : null,
    h("section", { class: "card" },
      h("div", { class: "card-head", style: { "padding-bottom": "14px", "border-bottom": p.ops.length ? "1px solid var(--border)" : "0" } },
        h("div", { class: "plan-head", style: { flex: "1" } },
          h("h3", { class: "card-title" }, p.ops.length ? plural(p.ops.length, "change") + " to match " : "In sync with ", h("span", { class: "mono" }, file)),
          n.safe ? h("span", { class: "chip ok" }, `${n.safe} safe`) : null,
          n.caution ? h("span", { class: "chip warn" }, `${n.caution} caution`) : null,
          n.destructive ? h("span", { class: "chip crit" }, `${n.destructive} destructive`) : null,
          h("span", { class: "plan-id", title: "The plan's ID covers its changes; apply refuses a plan that changed since it was reviewed" }, "plan " + p.id),
          p.checked ? h("span", { class: "chip mono", title: "Each change was validated by the controller without being made" }, icon("ok"), "checked") : null),
        h("span", { class: "right muted", style: { "font-size": "12px" } }, "planned " + ago(Date.parse(p.at)))),
      p.ops.length === 0
        ? h("div", { class: "card-body" }, h("div", { class: "callout ok" }, icon("ok"), h("div", { class: "body" }, h("b", null, "Nothing to do. "), "Every topic, config, ACL and quota the file declares is on the cluster as declared.")))
        : h("ul", { class: "ops" }, p.ops.map((o, i) => {
          const cls = opClass(o);
          return h("li", { key: i + o.kind + o.target + (o.key || ""), class: "op " + cls },
            h("span", { class: "sign", "aria-label": cls }, sign[cls]),
            h("div", { style: { "min-width": "0" } },
              h("div", { class: "line1" }, h("span", { class: "kind" }, o.kind.replace("-", " ")), h("span", { class: "target" }, o.target), o.key ? h("span", { class: "target dim" }, o.key) : null),
              o.from || o.to ? h("div", { class: "delta" },
                o.from ? h("span", { class: "from" }, shown(o, o.from)) : null,
                o.from && o.to ? h("span", { class: "arrow" }, "→") : null,
                o.to ? h("span", { class: "to" }, shown(o, o.to)) : null) : null,
              h("div", { class: "detail" }, o.detail),
              o.problem ? h("div", { class: "problem" }, icon("crit"), "Refused by the controller: " + o.problem) : null),
            riskChip(o.risk));
        }))),
    p.violations.length ? h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" },
      h("b", null, plural(p.violations.length, "policy violation") + " block this plan"),
      h("ul", null, p.violations.map((x) => h("li", { key: x.policy + x.topic }, h("span", { class: "mono" }, x.policy), " refuses ", h("span", { class: "mono" }, x.topic), ": ", x.message))))) : null,
    p.errors.length ? h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, h("b", null, "The file can't be planned whole"), h("ul", null, p.errors.map((e) => h("li", { key: e }, e))))) : null,
    p.warnings.length ? h("div", { class: "callout warn" }, icon("warn"), h("div", { class: "body" }, h("b", null, "Warnings"), h("ul", null, p.warnings.map((e) => h("li", { key: e }, e))))) : null,
    p.unmanaged.topics + p.unmanaged.acls + p.unmanaged.quotas > 0
      ? h("p", { class: "muted", style: { margin: "0", "font-size": "12.5px" } },
        `Left alone because the file doesn't declare them and doesn't prune: ${plural(p.unmanaged.topics, "topic")}, ${plural(p.unmanaged.acls, "ACL")}, ${plural(p.unmanaged.quotas, "quota")}.`) : null,
    p.ops.length ? applyBox(name, v, p) : null);
}

function applyBox(name, v, p) {
  if (v.blocked) {
    return h("div", { class: "callout crit" }, icon("lock"), h("div", { class: "body" }, h("b", null, "Can't be applied. "), v.blocked));
  }
  if (!state.meta.allowApply) {
    return h("div", { class: "callout accent" }, icon("gitops"), h("div", { class: "body" },
      h("b", null, "This console is read-only. "), "Merge the change to the desired-state file and let CI apply exactly this plan:",
      copyCode(`kaas apply -f ${v.desired.split("/").pop()} --plan ${p.id} --yes`)));
  }
  return h("div", { class: "callout accent" }, icon("play"), h("div", { class: "body" },
    h("div", { class: "row wrap" },
      h("div", { style: { flex: "1", "min-width": "240px" } }, h("b", null, "Apply from here. "), "Additions run first and removals last; it stops at the first failure."),
      h("button", { class: "btn primary", disabled: v.applying, onclick: () => { state.ui.modal = { kind: "apply", cluster: name, plan: p, typed: "" }; changed(); } },
        icon("play"), v.applying ? "Applying…" : "Apply " + plural(p.ops.length, "change")))));
}

function resultsCard(name, r) {
  const done = r.results.filter((x) => x.done).length;
  return h("div", { class: "callout " + (r.error ? "crit" : "ok") }, icon(r.error ? "crit" : "ok"), h("div", { class: "body" },
    h("div", { class: "row" }, h("b", null, r.error ? `Stopped after ${done} of ${r.results.length} changes` : `Applied ${plural(done, "change")}`),
      h("button", { class: "btn sm ghost", style: { "margin-left": "auto" }, onclick: () => { delete state.ui.results[name]; changed(); } }, "Dismiss")),
    r.error ? h("div", { style: { "margin-top": "4px" } }, r.error) : null,
    h("ul", null, r.results.map((x, i) => h("li", { key: i }, x.done ? "✓ " : x.error ? "✗ " : "· ", h("span", { class: "mono" }, `${x.op.kind} ${x.op.target}${x.op.key ? " " + x.op.key : ""}`), x.error ? " — " + x.error : "")))));
}

export function applyModal() {
  const m = state.ui.modal;
  if (!m || m.kind !== "apply") return null;
  const ok = m.typed === m.cluster;
  const close = () => { state.ui.modal = null; changed(); };
  const n = m.plan.ops.filter((o) => o.risk === "destructive").length;
  const go = async () => {
    m.busy = true; m.error = null; changed();
    try {
      const r = await api(`/clusters/${encodeURIComponent(m.cluster)}/apply`, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ plan: m.plan.id, confirm: m.typed }),
      });
      state.ui.results[m.cluster] = r;
      state.ui.modal = null;
    } catch (e) {
      if (e.body?.results) { state.ui.results[m.cluster] = e.body; state.ui.modal = null; } else { m.error = e.message; m.busy = false; }
    }
    poll(true);
    changed();
  };
  return [
    h("div", { class: "scrim", onclick: close }),
    h("div", { class: "modal", role: "dialog", "aria-modal": "true", "aria-labelledby": "apply-title" },
      h("div", { class: "modal-body" },
        h("h2", { id: "apply-title" }, `Apply ${plural(m.plan.ops.length, "change")} to ${m.cluster}?`),
        h("p", null, n ? h("span", null, h("b", { style: { color: "var(--danger)" } }, `${n} destructive. `), "Deleted data and revoked access can't be brought back. ") : null,
          "Type ", h("b", { class: "mono" }, m.cluster), " to confirm."),
        h("input", {
          class: "input", autofocus: true, value: m.typed, "aria-label": "Cluster name", spellcheck: "false", autocomplete: "off",
          ref: (el) => { if (!m.focused) { m.focused = true; setTimeout(() => el.focus()); } },
          oninput: (e) => { m.typed = e.target.value; changed(); },
          onkeydown: (e) => { if (e.key === "Enter" && m.typed === m.cluster && !m.busy) go(); if (e.key === "Escape") close(); },
        }),
        m.error ? h("div", { class: "callout crit mt" }, icon("crit"), h("div", { class: "body" }, m.error)) : null),
      h("div", { class: "modal-foot" },
        h("button", { class: "btn ghost", onclick: close }, "Cancel"),
        h("button", { class: n ? "btn danger" : "btn primary", disabled: !ok || m.busy, onclick: go }, m.busy ? "Applying…" : "Apply plan " + m.plan.id.slice(0, 7)))),
  ];
}

// The changes page: every cluster's drift on one screen.
export function changesPage() {
  const f = state.fleet;
  if (!f) return loading();
  const managed = f.clusters.filter((c) => c.gitops);
  const drifting = managed.filter((c) => c.drift);
  return [
    hero({
      eyebrow: "GitOps",
      title: drifting.length ? h("span", null, h("span", { class: "accent" }, plural(f.totals.drift, "change")), " between git and the fleet") : h("span", null, "The fleet ", h("span", { class: "accent" }, "matches git"), "."),
      lede: "Each managed cluster's desired-state file is re-read and compared with the cluster every 30 seconds. Every change is checked by the controller and the file's policies before anyone sees it.",
    }),
    managed.length === 0 ? h("section", { class: "card" }, empty("gitops", "No cluster is managed from git",
      "Point a cluster at a desired-state file with desired: in the fleet file.", copyCode("kaas console --fleet fleet.yaml"))) : null,
    h("div", { class: "stack" },
      drifting.map(driftCard),
    // Clusters that match git need no more than a line each.
    managed.length > drifting.length ? h("section", { class: "card" },
      h("div", { class: "card-head" }, h("h3", { class: "card-title" }, icon("ok"), "In sync"), h("span", { class: "right muted" }, plural(managed.length - drifting.length, "cluster"))),
      h("ul", { class: "synced" }, managed.filter((c) => !c.drift).map((c) => {
        const v = state.plans[c.name];
        return h("li", { key: c.name, class: `status-${c.status}` },
          h("span", { class: "dot" }),
          link(clusterHref(c.name, "gitops"), { class: "name" }, c.name),
          h("span", { class: "mono muted" }, (v?.desired || "").split("/").pop()),
          envBadge(c.env),
          v?.plan ? h("span", { class: "plan-id" }, "plan " + v.plan.id) : h("span"),
          link(clusterHref(c.name, "gitops"), { class: "btn sm outline" }, "Open", icon("arrow")));
      }))) : null),
  ];
}

function driftCard(c) {
  const v = state.plans[c.name];
  const p = v?.plan;
  return h("section", { key: c.name, class: `card gcard status-${c.status}${c.blocked ? " blocked" : ""}` },
    h("div", { class: "gcard-head" },
      h("div", { class: "gcard-title" },
        link(clusterHref(c.name, "gitops"), { class: "name" }, c.name),
        c.blocked ? h("span", { class: "chip crit" }, icon("lock"), "Blocked") : h("span", { class: "chip warn" }, icon("warn"), plural(c.drift, "change")),
        envBadge(c.env)),
      link(clusterHref(c.name, "gitops"), { class: "btn sm outline" }, "Review", icon("arrow"))),
    h("div", { class: "gcard-meta" },
      h("span", { class: "mono" }, (v?.desired || "").split("/").pop()),
      c.driftRisk.safe ? h("span", null, `${c.driftRisk.safe} safe`) : null,
      c.driftRisk.caution ? h("span", null, `${c.driftRisk.caution} caution`) : null,
      c.driftRisk.destructive ? h("span", { class: "danger" }, `${c.driftRisk.destructive} destructive`) : null,
      p ? h("span", { class: "plan-id" }, "plan " + p.id) : null),
    c.blocked ? h("div", { class: "gcard-blocked" }, c.blocked) : null,
    p && p.ops.length ? h("ul", { class: "ops compact" }, p.ops.slice(0, 4).map((o, i) => {
      const cls = opClass(o);
      return h("li", { key: i, class: "op " + cls },
        h("span", { class: "sign", "aria-label": cls }, sign[cls]),
        h("div", { class: "line1" }, h("span", { class: "kind" }, o.kind.replace("-", " ")), h("span", { class: "target" }, o.target), o.key ? h("span", { class: "target dim" }, o.key) : null),
        riskChip(o.risk));
    }), p.ops.length > 4 ? h("li", { class: "op more" }, link(clusterHref(c.name, "gitops"), { class: "card-link" }, `${p.ops.length - 4} more`, icon("arrow"))) : null) : null);
}
