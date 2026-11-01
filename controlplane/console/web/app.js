// The KaaS console: a shell (sidebar, top bar) around one page, redrawn
// from the store whenever data or the user's state changes.

import { h, render } from "./lib/h.js";
import { icon, logo, sevIcon } from "./lib/icons.js";
import { state, onChange, start } from "./lib/store.js";
import { startRouter } from "./lib/router.js";
import { link, clusterHref, words } from "./views/common.js";
import { fleetPage } from "./views/fleet.js";
import { clusterPage } from "./views/cluster.js";
import { changesPage, applyModal } from "./views/gitops.js";
import { activityPage } from "./views/activity.js";
import { palette, openPalette } from "./views/palette.js";

// ─────────────────────────── theme ───────────────────────────

const media = matchMedia("(prefers-color-scheme: light)");
function applyTheme() {
  const t = state.ui.theme === "system" ? (media.matches ? "light" : "dark") : state.ui.theme;
  document.documentElement.dataset.theme = t;
}
try { state.ui.theme = localStorage.getItem("kaas.theme") || "system"; } catch { /* private mode */ }
media.addEventListener("change", applyTheme);
applyTheme();
function setTheme(t) {
  state.ui.theme = t;
  try { localStorage.setItem("kaas.theme", t); } catch { /* private mode */ }
  applyTheme();
  draw();
}

// ─────────────────────────── shell ───────────────────────────

function sidebar() {
  const r = state.route;
  const f = state.fleet;
  const drift = f?.totals.drift || 0;
  return h("aside", { class: "sidebar" },
    link("/", { class: "brand", "aria-label": "KaaS console home" }, logo(), h("span", { class: "brand-word" }, "kaas", h("em", null, ".")), h("span", { class: "brand-tag" }, "console")),
    h("nav", { class: "nav", "aria-label": "Main" },
      link("/", { "aria-current": r.page === "fleet" ? "page" : null }, icon("fleet", "nav-icon"), "Fleet"),
      link("/changes", { "aria-current": r.page === "changes" ? "page" : null }, icon("changes", "nav-icon"), "Changes", drift ? h("span", { class: "nav-count hot" }, drift) : null),
      link("/activity", { "aria-current": r.page === "activity" ? "page" : null }, icon("activity", "nav-icon"), "Activity")),
    h("div", { class: "nav-label clusters-nav" }, "Clusters"),
    h("nav", { class: "nav clusters-nav", "aria-label": "Clusters" }, (f?.clusters || []).map((c) =>
      link(clusterHref(c.name, r.cluster === c.name ? null : null), { key: c.name, class: "cluster-link status-" + c.status, "aria-current": r.cluster === c.name ? "page" : null, title: `${c.name}: ${c.reason}` },
        h("span", { class: "dot ring" }), h("span", null, c.name), h("span", { class: "sr-only" }, c.status),
        c.drift ? h("span", { class: "nav-count" }, c.drift) : null))),
    h("div", { class: "sidebar-foot" },
      h("div", { class: "mode" + (state.meta?.allowApply ? " write" : "") }, h("span", { class: "dot" }),
        state.meta?.allowApply ? "Plans can be applied" : "Read-only", state.meta?.demo ? h("span", { class: "chip volt", style: { "margin-left": "auto", height: "18px", "font-size": "10.5px" } }, "demo") : null),
      h("div", { class: "theme-switch", role: "group", "aria-label": "Theme" },
        [["light", "Light"], ["system", "Auto"], ["dark", "Dark"]].map(([id, label]) =>
          h("button", { key: id, "aria-pressed": state.ui.theme === id ? "true" : "false", onclick: () => setTheme(id) }, label))),
      h("div", { class: "version" }, "kaas " + (state.meta?.version || ""))));
}

function crumbs() {
  const r = state.route;
  const here = (t) => h("span", { class: "here" }, t);
  const sep = h("span", { class: "sep" }, "/");
  if (r.page === "cluster") {
    return [link("/", null, "Fleet"), sep, r.tab === "overview" && !r.topic ? here(r.cluster) : link(clusterHref(r.cluster), null, r.cluster),
      r.tab !== "overview" ? [h("span", { class: "sep" }, "/"), r.topic ? link(clusterHref(r.cluster, r.tab), null, r.tab) : here(r.tab)] : null,
      r.topic ? [h("span", { class: "sep" }, "/"), here(r.topic)] : null];
  }
  return here({ fleet: "Fleet", changes: "Changes", activity: "Activity" }[r.page]);
}

function topbar() {
  return h("header", { class: "topbar" },
    h("nav", { class: "crumbs", "aria-label": "Breadcrumb" }, crumbs()),
    h("div", { class: "spacer" }),
    state.error ? h("span", { class: "chip crit", title: state.error }, icon("crit"), "Can't reach the console server") : null,
    h("button", { class: "search-btn", onclick: openPalette, "aria-label": "Search (Command K)" }, icon("search"), h("span", null, "Jump to…"), h("kbd", null, "⌘K")),
    h("span", { class: "live" + (state.live ? "" : " off"), title: state.live ? "Events stream live" : "Reconnecting to the event stream" },
      h("span", { class: "pulse" }), state.live ? "Live" : "Offline"));
}

function page() {
  const r = state.route;
  try {
    switch (r.page) {
      case "cluster": return clusterPage(r.cluster, r.tab, r.topic);
      case "changes": return changesPage();
      case "activity": return activityPage();
      default: return fleetPage();
    }
  } catch (e) {
    // One broken view mustn't take the shell with it.
    console.error(e);
    return h("div", { class: "callout crit" }, icon("crit"), h("div", { class: "body" }, h("b", null, "This page failed to draw. "), String(e.message || e)));
  }
}

function toasts() {
  return h("div", { class: "toasts", role: "status", "aria-live": "polite" }, state.ui.toasts.map((t) =>
    h("div", { key: t.id, class: "toast sev-" + t.severity }, icon(sevIcon[t.severity] || "info"),
      h("div", null, h("div", { class: "t" }, `${words(t.kind)} on ${t.cluster}`), h("div", { class: "s" }, t.text)))));
}

const root = document.getElementById("app");
let queued = false;
function draw() {
  queued = false;
  render(root, h("div", { class: "shell" },
    sidebar(),
    h("div", { class: "main" }, topbar(), h("main", { class: "page", id: "main" }, page())),
    applyModal(), palette(), toasts()));
  const r = state.route;
  document.title = (r.page === "cluster" ? r.topic || r.cluster : { fleet: "Fleet", changes: "Changes", activity: "Activity" }[r.page]) + " · kaas console";
}
onChange(() => {
  if (!queued) {
    queued = true;
    requestAnimationFrame(draw);
  }
});

document.addEventListener("keydown", (e) => {
  const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement?.tagName);
  if ((e.key === "k" && (e.metaKey || e.ctrlKey)) || (e.key === "/" && !typing && !state.ui.palette)) {
    e.preventDefault();
    openPalette();
  } else if (e.key === "Escape" && state.route.topic && !state.ui.palette && !state.ui.modal) {
    history.back();
  }
});

startRouter();
draw();
start().catch((e) => {
  state.error = e.message;
  draw();
});
