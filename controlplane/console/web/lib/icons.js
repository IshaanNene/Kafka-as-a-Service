import { h } from "./h.js";

// Line icons on a 24-unit grid, 1.75 stroke, drawn for this console.
const paths = {
  fleet: ["M3 7.5 12 3l9 4.5-9 4.5z", "M3 12l9 4.5 9-4.5", "M3 16.5 12 21l9-4.5"],
  changes: ["M6 3v12", "M18 9a3 3 0 1 0 0-6 3 3 0 0 0 0 6z", "M6 21a3 3 0 1 0 0-6 3 3 0 0 0 0 6z", "M18 9a9 9 0 0 1-9 9"],
  activity: ["M3 12h4l3-8 4 16 3-8h4"],
  cluster: ["M12 2.5 20.5 7.25v9.5L12 21.5 3.5 16.75v-9.5z", "M12 12 20.5 7.25", "M12 12v9.5", "M12 12 3.5 7.25"],
  search: ["M11 18a7 7 0 1 0 0-14 7 7 0 0 0 0 14z", "m20 20-4-4"],
  overview: ["M4 13a8 8 0 0 1 16 0", "M12 13l4-4", "M4 17h16"],
  topics: ["M8 6h13", "M8 12h13", "M8 18h13", "M3.5 6h.01", "M3.5 12h.01", "M3.5 18h.01"],
  consumers: ["M16 21v-2a4 4 0 0 0-4-4H6a4 4 0 0 0-4 4v2", "M9 11a4 4 0 1 0 0-8 4 4 0 0 0 0 8z", "M22 21v-2a4 4 0 0 0-3-3.87", "M16 3.13a4 4 0 0 1 0 7.75"],
  access: ["M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z", "m9 12 2 2 4-4"],
  schemas: ["M8 3H7a2 2 0 0 0-2 2v5a2 2 0 0 1-2 2 2 2 0 0 1 2 2v5a2 2 0 0 0 2 2h1", "M16 21h1a2 2 0 0 0 2-2v-5a2 2 0 0 1 2-2 2 2 0 0 1-2-2V5a2 2 0 0 0-2-2h-1"],
  gitops: ["M12 15a3 3 0 1 0 0-6 3 3 0 0 0 0 6z", "M3 12h6", "M15 12h6"],
  ok: ["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z", "m8 12 3 3 5-6"],
  warn: ["M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z", "M12 9v4", "M12 17h.01"],
  crit: ["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z", "m15 9-6 6", "m9 9 6 6"],
  info: ["M12 22a10 10 0 1 0 0-20 10 10 0 0 0 0 20z", "M12 16v-4", "M12 8h.01"],
  offline: ["M2 2l20 20", "M8.5 16.5a5 5 0 0 1 7 0", "M2 8.82a15 15 0 0 1 4.17-2.65", "M10.66 5c4.01-.36 8.14.9 11.34 3.76", "M16.85 11.25a10 10 0 0 1 2.22 1.68", "M5 13a10 10 0 0 1 5.24-2.76", "M12 20h.01"],
  connecting: ["M21 12a9 9 0 1 1-6.22-8.56"],
  arrow: ["M5 12h14", "m13 6 6 6-6 6"],
  copy: ["M9 9h11v11H9z", "M5 15H4V4h11v1"],
  sun: ["M12 17a5 5 0 1 0 0-10 5 5 0 0 0 0 10z", "M12 1v2", "M12 21v2", "M4.2 4.2l1.4 1.4", "M18.4 18.4l1.4 1.4", "M1 12h2", "M21 12h2", "M4.2 19.8l1.4-1.4", "M18.4 5.6l1.4-1.4"],
  moon: ["M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"],
  close: ["M18 6 6 18", "m6 6 12 12"],
  play: ["m6 3 14 9-14 9z"],
  lock: ["M5 11h14v10H5z", "M8 11V7a4 4 0 0 1 8 0v4"],
  broker: ["M3 4h18v6H3z", "M3 14h18v6H3z", "M7 7h.01", "M7 17h.01"],
  bolt: ["M13 2 3 14h9l-1 8 10-12h-9z"],
  file: ["M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z", "M14 2v6h6"],
  plus: ["M12 5v14", "M5 12h14"],
  minus: ["M5 12h14"],
  tilde: ["M4 14c2-4 5-4 8 0s6 4 8 0"],
  chevron: ["m6 9 6 6 6-6"],
};

export function icon(name, cls) {
  return h("svg", { class: "ic" + (cls ? " " + cls : ""), viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", "stroke-width": "1.75", "stroke-linecap": "round", "stroke-linejoin": "round", "aria-hidden": "true" },
    (paths[name] || []).map((d) => h("path", { d })));
}

// The KaaS mark: a partition log — three records, the newest lit — that
// reads as a forward arrow.
export function logo() {
  return h("svg", { class: "brand-mark", viewBox: "0 0 32 32", "aria-hidden": "true" },
    h("rect", { x: "1", y: "1", width: "30", height: "30", rx: "9", fill: "var(--text)" }),
    h("rect", { x: "7", y: "9", width: "11", height: "3.4", rx: "1.7", fill: "var(--bg)" }),
    h("rect", { x: "7", y: "14.3", width: "15", height: "3.4", rx: "1.7", fill: "var(--bg)" }),
    h("rect", { x: "7", y: "19.6", width: "11", height: "3.4", rx: "1.7", fill: "var(--bg)" }),
    h("circle", { cx: "23.6", cy: "16", r: "2.6", fill: "var(--brand)" }));
}

// A status: icon, word, and why — never colour alone.
const statusIcon = { healthy: "ok", degraded: "warn", critical: "crit", unreachable: "offline", connecting: "connecting" };
const statusWord = { healthy: "Healthy", degraded: "Degraded", critical: "Critical", unreachable: "Unreachable", connecting: "Connecting" };

export function status(s, why) {
  return h("span", { class: `status status-${s}` }, icon(statusIcon[s] || "info"), statusWord[s] || s,
    why ? h("span", { class: "why" }, "· " + why) : null);
}

export const sevIcon = { ok: "ok", info: "info", warning: "warn", critical: "crit" };
