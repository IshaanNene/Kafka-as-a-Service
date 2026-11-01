// Unit tests for the console's formatting and routing, run with
// `node --test controlplane/console/webtest` (no dependencies).
import { test } from "node:test";
import assert from "node:assert/strict";
import { compact, rate, bytes, duration, configValue, quota, plural } from "../web/lib/fmt.js";
import { parse } from "../web/lib/router.js";

test("compact numbers", () => {
  assert.equal(compact(0), "0");
  assert.equal(compact(9999), "9,999");
  assert.equal(compact(12_345), "12.3k");
  assert.equal(compact(4_200_000), "4.2M");
  assert.equal(compact(null), "—");
});

test("rates under one aren't rounded to zero", () => {
  assert.equal(rate(0.3), "<1");
  assert.equal(rate(0), "0");
  assert.equal(rate(1500), "1,500");
});

test("bytes use binary units", () => {
  assert.equal(bytes(512), "512 B");
  assert.equal(bytes(10 * 1024 * 1024), "10 MiB");
  assert.equal(bytes(0), "—");
});

test("durations drop a zero second unit", () => {
  assert.equal(duration(42), "42s");
  assert.equal(duration(90), "1m 30s");
  assert.equal(duration(3600), "1h");
  assert.equal(duration(30 * 86400), "30d");
  assert.equal(duration(-1), "—");
});

test("config values are said in words", () => {
  assert.equal(configValue("retention.ms", "604800000"), "7d");
  assert.equal(configValue("retention.bytes", "-1"), "unlimited");
  assert.equal(configValue("segment.bytes", "1073741824"), "1 GiB");
  assert.equal(configValue("cleanup.policy", "compact"), "compact");
});

test("quota values", () => {
  assert.equal(quota("producer_byte_rate", 1048576), "1 MiB/s");
  assert.equal(quota("request_percentage", 50), "50%");
});

test("plural", () => {
  assert.equal(plural(1, "change"), "1 change");
  assert.equal(plural(3, "change"), "3 changes");
  assert.equal(plural(2, "policy", "policies"), "2 policies");
});

test("routes", () => {
  assert.deepEqual(parse("/"), { page: "fleet" });
  assert.deepEqual(parse("/changes"), { page: "changes" });
  assert.deepEqual(parse("/clusters/prod-eu"), { page: "cluster", cluster: "prod-eu", tab: "overview", topic: null });
  assert.deepEqual(parse("/clusters/prod-eu/gitops"), { page: "cluster", cluster: "prod-eu", tab: "gitops", topic: null });
  assert.deepEqual(parse("/clusters/prod-eu/topics/orders.v2"), { page: "cluster", cluster: "prod-eu", tab: "topics", topic: "orders.v2" });
  assert.deepEqual(parse("/clusters/a%2Fb/nonsense"), { page: "cluster", cluster: "a/b", tab: "overview", topic: null });
  assert.deepEqual(parse("/whatever"), { page: "fleet" });
});
