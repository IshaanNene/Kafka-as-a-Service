// A small view layer: h() describes elements, render() morphs the live DOM
// to match. Elements that survive keep their focus, scroll and selection,
// so views can re-render on every data refresh. Text always goes in as text
// nodes; nothing here sets innerHTML.

const SVG = "http://www.w3.org/2000/svg";

export function h(tag, props, ...children) {
  return { tag, props: props || {}, children: flat(children) };
}

function flat(xs, out = []) {
  for (const x of xs) {
    if (x == null || x === false || x === true) continue;
    if (Array.isArray(x)) flat(x, out);
    else out.push(typeof x === "object" ? x : String(x));
  }
  return out;
}

export function render(parent, vnodes) {
  patchChildren(parent, flat([vnodes]), parent.namespaceURI === SVG);
}

function create(v, svg) {
  if (typeof v === "string") return document.createTextNode(v);
  svg = svg || v.tag === "svg";
  const el = svg ? document.createElementNS(SVG, v.tag) : document.createElement(v.tag);
  el._p = {};
  patchProps(el, v.props, svg);
  patchChildren(el, v.children, svg && v.tag !== "foreignObject");
  if (v.props.ref) v.props.ref(el);
  el._v = v;
  return el;
}

function same(node, v) {
  if (typeof v === "string") return node.nodeType === 3;
  return node.nodeType === 1 && node.localName === v.tag && node._key === v.props.key;
}

function patch(node, v, svg) {
  if (typeof v === "string") {
    if (node.nodeValue !== v) node.nodeValue = v;
    return;
  }
  // A view that hands back the very same vnode (a cached, unchanging
  // part such as the map's land) has nothing to patch.
  if (node._v === v) return;
  svg = svg || v.tag === "svg";
  patchProps(node, v.props, svg);
  patchChildren(node, v.children, svg && v.tag !== "foreignObject");
  if (v.props.ref) v.props.ref(node);
  node._v = v;
}

function patchChildren(parent, vs, svg) {
  const keyed = new Map();
  for (const n of parent.childNodes) if (n._key != null) keyed.set(n._key, n);
  for (let i = 0; i < vs.length; i++) {
    const v = vs[i];
    const at = parent.childNodes[i] || null;
    let node = null;
    const key = typeof v === "object" ? v.props.key : undefined;
    if (key != null) {
      const k = keyed.get(key);
      if (k && k.localName === v.tag) node = k;
    } else if (at && at._key == null && same(at, v)) {
      node = at;
    }
    if (node) patch(node, v, svg);
    else node = create(v, svg);
    if (node !== at) parent.insertBefore(node, at);
  }
  while (parent.childNodes.length > vs.length) parent.removeChild(parent.lastChild);
}

function patchProps(el, props, svg) {
  const prev = el._p || {};
  for (const name in prev) {
    if (!(name in props)) setProp(el, name, undefined, prev[name], svg);
  }
  for (const name in props) {
    const val = props[name];
    if (prev[name] !== val || name === "value" || name === "checked") setProp(el, name, val, prev[name], svg);
  }
  el._p = props;
  el._key = props.key;
}

function setProp(el, name, val, old, svg) {
  if (name === "key" || name === "ref") return;
  if (name.startsWith("on")) {
    el[name.toLowerCase()] = val || null;
    return;
  }
  if (name === "style") {
    const o = old || {};
    const n = val || {};
    for (const k in o) if (!(k in n)) el.style.removeProperty(k);
    for (const k in n) if (o[k] !== n[k]) el.style.setProperty(k, n[k]);
    return;
  }
  if (name === "value") {
    // Don't fight the user while they type.
    if (el.value !== (val ?? "") && document.activeElement !== el) el.value = val ?? "";
    return;
  }
  if (name === "checked") {
    el.checked = !!val;
    return;
  }
  if (name === "class" && !svg) {
    el.className = val || "";
    return;
  }
  if (val == null || val === false) el.removeAttribute(name);
  else el.setAttribute(name, val === true ? "" : String(val));
}
