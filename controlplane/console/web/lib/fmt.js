// Number, time and size formatting. Every number on screen goes through
// here so the same quantity always reads the same way.

const nf = new Intl.NumberFormat("en", { maximumFractionDigits: 1 });
const nf0 = new Intl.NumberFormat("en", { maximumFractionDigits: 0 });

export function compact(n) {
  if (n == null || Number.isNaN(n)) return "—";
  const a = Math.abs(n);
  if (a >= 1e12) return nf.format(n / 1e12) + "T";
  if (a >= 1e9) return nf.format(n / 1e9) + "B";
  if (a >= 1e6) return nf.format(n / 1e6) + "M";
  if (a >= 1e4) return nf.format(n / 1e3) + "k";
  return nf0.format(n);
}

export function int(n) {
  return n == null ? "—" : nf0.format(n);
}

export function rate(n) {
  if (n == null) return "—";
  if (n > 0 && n < 1) return "<1";
  return compact(n);
}

export function bytes(n) {
  if (!n || n < 0) return "—";
  const u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i === 0 ? nf0 : nf).format(n) + " " + u[i];
}

export function duration(s) {
  if (s == null || s < 0) return "—";
  if (s < 1) return "<1s";
  if (s < 60) return Math.round(s) + "s";
  const two = (big, bigU, small, smallU) => big + bigU + (small ? " " + small + smallU : "");
  if (s < 3600) return two(Math.floor(s / 60), "m", Math.round(s % 60), "s");
  if (s < 86400) return two(Math.floor(s / 3600), "h", Math.floor((s % 3600) / 60), "m");
  return two(Math.floor(s / 86400), "d", Math.floor((s % 86400) / 3600), "h");
}

export function ago(ms, now = Date.now()) {
  const s = Math.max(0, (now - ms) / 1000);
  if (s < 5) return "now";
  if (s < 60) return Math.floor(s) + "s ago";
  if (s < 3600) return Math.floor(s / 60) + "m ago";
  if (s < 86400) return Math.floor(s / 3600) + "h ago";
  return Math.floor(s / 86400) + "d ago";
}

export function clock(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

export function hm(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", hour12: false });
}

export function plural(n, word, many) {
  return n === 1 ? `1 ${word}` : `${int(n)} ${many || word + "s"}`;
}

// Quota values: byte rates as sizes per second, others as plain numbers.
export function quota(key, v) {
  if (key.endsWith("_byte_rate")) return bytes(v) + "/s";
  if (key === "request_percentage") return nf.format(v) + "%";
  return nf.format(v) + "/s";
}

// Config values Kafka keeps in milliseconds or bytes, said in words.
export function configValue(key, v) {
  if (v == null || v === "") return "";
  const n = Number(v);
  if (!Number.isFinite(n) || !/^-?\d+$/.test(v)) return v;
  if (n === -1 && (key.startsWith("retention.") || key.endsWith(".bytes"))) return "unlimited";
  if (key.endsWith(".ms")) return duration(n / 1000);
  if (key.endsWith(".bytes")) return bytes(n);
  return v;
}

// Money, whole units: $1,533 a month reads better than $1,532.87.
export function money(v, currency = "USD") {
  if (v == null || !Number.isFinite(v)) return "—";
  try {
    return new Intl.NumberFormat("en", { style: "currency", currency, maximumFractionDigits: 0 }).format(v);
  } catch {
    return nf0.format(v) + " " + currency;
  }
}

// A percentage with as many decimals as it takes to not round 99.95 to 100.
export function pct(v) {
  if (v == null || v < 0) return "—";
  if (v >= 99.995) return "100%";
  if (v >= 99) return v.toFixed(2) + "%";
  return v.toFixed(1) + "%";
}

// How old data is: under ten seconds to a tenth, then as a duration.
export function age(s) {
  if (s == null || s < 0) return "—";
  if (s === 0) return "live";
  if (s < 10) return s.toFixed(1) + "s";
  return duration(s);
}

// A change as a signed percentage: +12%, −3%, ±0%.
export function signed(r) {
  const p = Math.round(r * 100);
  return p === 0 ? "±0%" : (p > 0 ? "+" : "−") + Math.abs(p) + "%";
}

// "LeaderMoved" → "Leader moved", "ISRShrunk" → "ISR shrunk".
export function words(kind) {
  const s = kind.replace(/([a-z])([A-Z])/g, "$1 $2").replace(/([A-Z])([A-Z][a-z])/g, "$1 $2");
  const w = s.toLowerCase().replace(/\b(isr|acls?)\b/g, (m) => m.toUpperCase());
  return w.charAt(0).toUpperCase() + w.slice(1);
}
