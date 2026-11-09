// Static checks for the marketing site in site/. No dependencies, no browser.
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

const site = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'site');
const html = readFileSync(join(site, 'index.html'), 'utf8');
const js = readFileSync(join(site, 'main.js'), 'utf8');
const css = readFileSync(join(site, 'styles.css'), 'utf8');

const ids = [...html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]);
const idSet = new Set(ids);
const text = html
  .replace(/<script[\s\S]*?<\/script>/g, ' ')
  .replace(/<svg[\s\S]*?<\/svg>/g, ' ')
  .replace(/<[^>]+>/g, ' ');

test('ids are unique', () => {
  assert.deepEqual(ids.filter((id, i) => ids.indexOf(id) !== i), []);
});

test('in-page references resolve', () => {
  const refs = [
    ...[...html.matchAll(/\shref="#([^"]+)"/g)].map((m) => m[1]),
    ...[...html.matchAll(/\sfor="([^"]+)"/g)].map((m) => m[1]),
    ...[...html.matchAll(/\saria-(?:controls|labelledby|describedby)="([^"]+)"/g)].flatMap((m) => m[1].split(/\s+/)),
  ];
  assert.deepEqual([...new Set(refs.filter((r) => !idSet.has(r)))], []);
});

test('ids used by main.js exist', () => {
  const used = [...js.matchAll(/\$\('([\w-]+)'\)/g)].map((m) => m[1]);
  assert.ok(used.length > 0, 'expected $() lookups in main.js');
  assert.deepEqual([...new Set(used.filter((id) => !idSet.has(id)))], []);
});

test('local assets exist', () => {
  const urls = [
    ...[...html.matchAll(/\s(?:src|href)="([^"]+)"/g)].map((m) => m[1]),
    ...[...html.matchAll(/\ssrcset="([^"]+)"/g)].flatMap((m) => m[1].split(',').map((s) => s.trim().split(/\s+/)[0])),
    ...[...css.matchAll(/url\(['"]?([^'")]+)['"]?\)/g)].map((m) => m[1]),
  ];
  const local = urls.filter((u) => !/^(#|https?:|mailto:|data:)/.test(u)).map((u) => u.split(/[?#]/)[0]);
  assert.ok(local.length > 0, 'expected local assets');
  assert.deepEqual([...new Set(local.filter((u) => !existsSync(join(site, u))))], []);
});

test('images have alt text and reserved dimensions', () => {
  const bad = [...html.matchAll(/<img\b[^>]*>/g)]
    .map((m) => m[0])
    .filter((t) => !/\salt="/.test(t) || !/\swidth="/.test(t) || !/\sheight="/.test(t));
  assert.deepEqual(bad, []);
});

test('copy avoids banned words and unsupported claims', () => {
  const banned = /\b(revolutionary|seamless(ly)?|unlock(s|ing)?|supercharge(s|d)?|SOC ?2|HIPAA|SSO|air-?gapped)\b/gi;
  assert.deepEqual([...new Set([...text.matchAll(banned)].map((m) => m[0]))], []);
});

test('motion and focus rules stay in the stylesheet', () => {
  assert.match(css, /prefers-reduced-motion/);
  assert.match(css, /:focus-visible/);
});
