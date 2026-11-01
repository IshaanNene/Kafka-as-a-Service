import { state, changed, poll } from "./store.js";

const tabs = new Set(["overview", "topics", "consumers", "access", "schemas", "gitops"]);

// parse turns a path into a route:
//   /                              the fleet
//   /changes, /activity
//   /clusters/:name[/:tab]
//   /clusters/:name/topics/:topic  the topics tab with a topic open
export function parse(path) {
  const parts = path.split("/").filter(Boolean).map(decodeURIComponent);
  if (parts[0] === "changes") return { page: "changes" };
  if (parts[0] === "activity") return { page: "activity" };
  if (parts[0] === "clusters" && parts[1]) {
    const tab = tabs.has(parts[2]) ? parts[2] : "overview";
    return { page: "cluster", cluster: parts[1], tab, topic: tab === "topics" ? parts[3] || null : null };
  }
  return { page: "fleet" };
}

export function navigate(path, { replace } = {}) {
  if (path === location.pathname) return;
  const prev = state.route;
  history[replace ? "replaceState" : "pushState"](null, "", path);
  state.route = parse(path);
  // A drawer opening or closing keeps the page where it was.
  const samePage = prev.cluster === state.route.cluster && prev.tab === state.route.tab;
  if (!samePage) window.scrollTo(0, 0);
  changed();
  poll(true);
}

export function startRouter() {
  state.route = parse(location.pathname);
  window.addEventListener("popstate", () => {
    state.route = parse(location.pathname);
    changed();
    poll(true);
  });
  // Plain links with data-link navigate in place.
  document.addEventListener("click", (e) => {
    const a = e.target.closest("a[data-link]");
    if (!a || e.metaKey || e.ctrlKey || e.shiftKey || e.button !== 0) return;
    e.preventDefault();
    navigate(a.getAttribute("href"));
  });
}
