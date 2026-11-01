import { h } from "../lib/h.js";
import { icon } from "../lib/icons.js";
import { state, changed } from "../lib/store.js";
import { navigate } from "../lib/router.js";
import { clusterHref } from "./common.js";

// The command palette (⌘K): jump to any page, cluster, tab, topic or group.
function items() {
  const out = [
    { grp: "Go to", ic: "fleet", label: "Fleet overview", href: "/" },
    { grp: "Go to", ic: "changes", label: "Changes from git", href: "/changes" },
    { grp: "Go to", ic: "activity", label: "Activity", href: "/activity" },
    { grp: "Go to", ic: "bolt", label: "Recommendations", href: "/insights" },
  ];
  for (const c of state.fleet?.clusters || []) {
    out.push({ grp: "Clusters", ic: "cluster", label: c.name, hint: [c.env, c.region].filter(Boolean).join(" · "), href: clusterHref(c.name) });
    for (const [tab, ic] of [["topics", "topics"], ["consumers", "consumers"], ["access", "access"], ["schemas", "schemas"], ["gitops", "gitops"]]) {
      out.push({ grp: "Clusters", ic, label: `${c.name} › ${tab}`, href: clusterHref(c.name, tab), quiet: true });
    }
  }
  for (const [name, v] of Object.entries(state.cluster)) {
    for (const t of v.topics || []) if (!t.internal) out.push({ grp: "Topics", ic: "topics", label: t.name, hint: name, href: clusterHref(name, "topics", t.name) });
    for (const g of v.groups || []) out.push({ grp: "Consumer groups", ic: "consumers", label: g.name, hint: name, href: clusterHref(name, "consumers") });
  }
  return out;
}

// score is a forgiving subsequence match: every typed character in order,
// with runs and word starts ranked higher.
function score(q, s) {
  s = s.toLowerCase();
  let i = 0, sc = 0, run = 0;
  for (const ch of q) {
    const j = s.indexOf(ch, i);
    if (j < 0) return -1;
    run = j === i ? run + 1 : 0;
    sc += 1 + run * 2 + (j === 0 || /[\s.\-_›]/.test(s[j - 1]) ? 3 : 0);
    i = j + 1;
  }
  return sc - s.length * 0.01;
}

function matches() {
  const p = state.ui.palette;
  const q = p.q.trim().toLowerCase();
  const all = items().filter((x) => q || !x.quiet);
  if (!q) return all.slice(0, 40);
  return all.map((x) => ({ x, s: score(q, x.label + " " + (x.hint || "")) })).filter((m) => m.s >= 0).sort((a, b) => b.s - a.s).slice(0, 40).map((m) => m.x);
}

export function openPalette() {
  state.ui.palette = { q: "", sel: 0 };
  changed();
}

export function palette() {
  const p = state.ui.palette;
  if (!p) return null;
  const list = matches();
  p.sel = Math.min(p.sel, Math.max(0, list.length - 1));
  const close = () => { state.ui.palette = null; changed(); };
  const go = (x) => { state.ui.palette = null; navigate(x.href); };
  let lastGrp = null;
  return [
    h("div", { class: "scrim", style: { "z-index": "60" }, onclick: close }),
    h("div", { class: "palette", role: "dialog", "aria-modal": "true", "aria-label": "Go to" },
      h("div", { class: "palette-input" }, icon("search"),
        h("input", {
          value: p.q, placeholder: "Jump to a cluster, topic, group or page…", "aria-label": "Search", spellcheck: "false", autocomplete: "off",
          role: "combobox", "aria-expanded": "true", "aria-controls": "palette-list",
          ref: (el) => { if (!p.focused) { p.focused = true; setTimeout(() => el.focus()); } },
          oninput: (e) => { p.q = e.target.value; p.sel = 0; changed(); },
          onkeydown: (e) => {
            if (e.key === "ArrowDown") { p.sel = Math.min(list.length - 1, p.sel + 1); e.preventDefault(); changed(); }
            else if (e.key === "ArrowUp") { p.sel = Math.max(0, p.sel - 1); e.preventDefault(); changed(); }
            else if (e.key === "Enter" && list[p.sel]) go(list[p.sel]);
            else if (e.key === "Escape") close();
          },
        }),
        h("kbd", null, "esc")),
      h("ul", { class: "palette-list", id: "palette-list", role: "listbox" },
        list.length === 0 ? h("li", { class: "grp" }, "Nothing matches") : null,
        list.map((x, i) => {
          const head = x.grp !== lastGrp ? h("li", { key: "g" + x.grp, class: "grp", role: "presentation" }, x.grp) : null;
          lastGrp = x.grp;
          return [head, h("li", { key: x.href + x.label },
            h("button", {
              role: "option", "aria-selected": i === p.sel ? "true" : "false",
              ref: (el) => { if (i === p.sel) el.scrollIntoView({ block: "nearest" }); },
              onmousemove: () => { if (p.sel !== i) { p.sel = i; changed(); } }, onclick: () => go(x),
            }, icon(x.ic), x.label, x.hint ? h("span", { class: "hint" }, x.hint) : null))];
        })),
      h("div", { class: "palette-foot" }, h("span", null, h("kbd", null, "↑"), " ", h("kbd", null, "↓"), " move"), h("span", null, h("kbd", null, "↵"), " open"), h("span", null, h("kbd", null, "/"), " or ", h("kbd", null, "⌘K"), " anywhere"))),
  ];
}
