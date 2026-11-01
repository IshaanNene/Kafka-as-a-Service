// Unit tests for the chart helpers that do arithmetic: the treemap layout
// and the change badge. Run with `node --test controlplane/console/webtest`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { squarify, change } from "../web/lib/charts.js";

test("squarify tiles the whole box in proportion", () => {
  const items = [{ value: 6 }, { value: 3 }, { value: 2 }, { value: 1 }];
  const rects = squarify(items, 400, 300);
  assert.equal(rects.length, items.length);
  const area = rects.reduce((a, r) => a + r.w * r.h, 0);
  assert.ok(Math.abs(area - 400 * 300) < 1e-6, `covers ${area}`);
  for (const [i, r] of rects.entries()) {
    assert.ok(Math.abs((r.w * r.h) / (400 * 300) - items[i].value / 12) < 1e-6, `item ${i} has its share`);
    assert.ok(r.x >= -1e-9 && r.y >= -1e-9 && r.x + r.w <= 400 + 1e-6 && r.y + r.h <= 300 + 1e-6, `item ${i} inside`);
  }
  assert.deepEqual(squarify([{ value: 0 }], 10, 10), []);
});

test("change compares the latest minute with the one before", () => {
  assert.equal(change([{ t: 0, v: 1 }]), null);
  const pts = [];
  for (let s = 0; s <= 20 * 60; s += 10) pts.push({ t: s * 1000, v: s < 10 * 60 ? 100 : 150 });
  const c = change(pts, 15);
  assert.ok(c, "enough points");
  assert.ok(c.ratio > 0.4 && c.ratio < 0.6, `ratio ${c.ratio}`);
});
