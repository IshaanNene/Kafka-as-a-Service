// The store: what the server last said, what the user is doing, and the
// loop that keeps the two fresh. Views read it; nothing else writes it.

export const state = {
  meta: null,
  fleet: null,
  cluster: {}, // name → ClusterView
  access: {},
  schemas: {},
  plans: {},
  topic: {}, // "cluster/topic" → TopicView
  events: [], // newest last, deduplicated by seq
  live: false,
  error: null, // the last failed fetch, shown in the top bar
  route: { page: "fleet" },
  ui: {
    theme: "system",
    topicQuery: "",
    showInternal: false,
    topicSort: "rate",
    groupQuery: "",
    aclQuery: "",
    schemaSel: null,
    sev: "all",
    eventCluster: "all",
    palette: null, // { q, sel }
    modal: null, // { kind: "apply", cluster, plan, typed, busy, error }
    results: {}, // cluster → apply results
    toasts: [],
    copied: null,
  },
};

let redraw = () => {};
export function onChange(f) { redraw = f; }
export function changed() { redraw(); }

export async function api(path, opts) {
  const r = await fetch("/api" + path, { headers: { Accept: "application/json" }, ...opts });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) {
    const e = new Error(body.error || r.statusText);
    e.status = r.status;
    e.body = body;
    throw e;
  }
  return body;
}

function addEvents(es) {
  const seen = new Set(state.events.map((e) => e.seq));
  let fresh = false;
  for (const e of es) {
    if (!seen.has(e.seq)) {
      state.events.push(e);
      fresh = true;
    }
  }
  if (fresh) {
    state.events.sort((a, b) => a.seq - b.seq);
    if (state.events.length > 500) state.events.splice(0, state.events.length - 500);
  }
}

// What the current route needs, fetched together.
async function refresh() {
  const r = state.route;
  const jobs = [api("/fleet").then((f) => { state.fleet = f; addEvents(f.events); })];
  if (r.cluster) {
    const c = encodeURIComponent(r.cluster);
    jobs.push(api(`/clusters/${c}`).then((v) => (state.cluster[r.cluster] = v)));
    if (r.tab === "access") jobs.push(api(`/clusters/${c}/access`).then((v) => (state.access[r.cluster] = v)));
    if (r.tab === "schemas") jobs.push(api(`/clusters/${c}/schemas`).then((v) => (state.schemas[r.cluster] = v)));
    if (r.tab === "gitops") jobs.push(api(`/clusters/${c}/plan`).then((v) => (state.plans[r.cluster] = v)));
    if (r.topic) {
      jobs.push(api(`/clusters/${c}/topics/${encodeURIComponent(r.topic)}`)
        .then((v) => (state.topic[r.cluster + "/" + r.topic] = v))
        .catch((e) => { if (e.status === 404) state.topic[r.cluster + "/" + r.topic] = { missing: true }; else throw e; }));
    }
  }
  if (r.page === "changes" && state.meta) {
    for (const name of state.meta.clusters) {
      jobs.push(api(`/clusters/${encodeURIComponent(name)}/plan`).then((v) => (state.plans[name] = v)));
    }
  }
  const res = await Promise.allSettled(jobs);
  const failed = res.find((x) => x.status === "rejected");
  state.error = failed ? failed.reason.message : null;
  changed();
}

let timer = null;
let inflight = null;
export function poll(now) {
  clearTimeout(timer);
  if (now || !inflight) {
    inflight = refresh().finally(() => {
      inflight = null;
      timer = setTimeout(poll, document.hidden ? 15000 : 2500);
    });
  }
}

export async function start() {
  state.meta = await api("/meta");
  connectEvents();
  poll(true);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) poll(true); });
}

// Events arrive over SSE; a warning or worse also pops a toast and
// refreshes at once rather than on the next tick.
function connectEvents() {
  const es = new EventSource("/api/events");
  es.onopen = () => { state.live = true; changed(); };
  es.onerror = () => { state.live = false; changed(); };
  es.onmessage = (m) => {
    const e = JSON.parse(m.data);
    addEvents([e]);
    if (e.severity === "warning" || e.severity === "critical" || e.kind === "PlanApplied") {
      toast(e);
      poll(true);
    }
    changed();
  };
}

export function toast(e) {
  const t = { id: Math.random(), ...e };
  state.ui.toasts = [...state.ui.toasts.slice(-2), t];
  setTimeout(() => {
    state.ui.toasts = state.ui.toasts.filter((x) => x.id !== t.id);
    changed();
  }, 6000);
}
