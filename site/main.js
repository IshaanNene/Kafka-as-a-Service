// KaaS site. No dependencies.
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var all = function (sel, el) { return Array.prototype.slice.call((el || document).querySelectorAll(sel)); };
  var root = document.documentElement, live = $('live');
  var replay = null;

  /* ---- theme ---- */
  function themeUI() {
    var dark = root.dataset.theme === 'dark';
    $('theme-ic').setAttribute('href', dark ? '#i-sun' : '#i-moon');
    $('theme').setAttribute('aria-label', 'Switch to ' + (dark ? 'light' : 'dark') + ' theme');
    if (replay) replay.restyle();
  }
  $('theme').addEventListener('click', function () {
    root.dataset.theme = root.dataset.theme === 'dark' ? 'light' : 'dark';
    try { localStorage.setItem('kaas-theme', root.dataset.theme); } catch (e) {}
    themeUI();
  });

  /* ---- navigation ---- */
  
  var dds = all('.dd');
  function closeDDs(except) {
    dds.forEach(function (d) {
      if (d === except) return;
      d.classList.remove('open');
      d.querySelector('.dd-btn').setAttribute('aria-expanded', 'false');
    });
  }
  function openDD(d) { closeDDs(d); d.classList.add('open'); d.querySelector('.dd-btn').setAttribute('aria-expanded', 'true'); }
  var hoverable = matchMedia('(hover: hover)').matches;
  dds.forEach(function (d) {
    var btn = d.querySelector('.dd-btn'), timer;
    btn.addEventListener('click', function () {
      if (d.classList.contains('open') && !d.dataset.hovered) { closeDDs(); return; }
      openDD(d); delete d.dataset.hovered;
    });
    if (hoverable) {
      d.addEventListener('mouseenter', function () { clearTimeout(timer); if (!d.classList.contains('open')) d.dataset.hovered = '1'; openDD(d); });
      d.addEventListener('mouseleave', function () { timer = setTimeout(function () { delete d.dataset.hovered; closeDDs(); }, 140); });
    }
    d.addEventListener('focusout', function (e) { if (e.relatedTarget && !d.contains(e.relatedTarget)) closeDDs(); });
  });
  document.addEventListener('click', function (e) { if (!e.target.closest('.dd')) closeDDs(); });

  var sheet = $('sheet'), burger = $('burger');
  function setSheet(open) {
    sheet.hidden = !open;
    burger.setAttribute('aria-expanded', String(open));
    burger.setAttribute('aria-label', open ? 'Close menu' : 'Open menu');
    burger.querySelector('use').setAttribute('href', open ? '#i-x' : '#i-menu');
    document.body.style.overflow = open ? 'hidden' : '';
  }
  burger.addEventListener('click', function () { setSheet(sheet.hidden); });
  sheet.addEventListener('click', function (e) { if (e.target.closest('a')) setSheet(false); });
  addEventListener('resize', function () { if (innerWidth > 1000 && !sheet.hidden) setSheet(false); });
  document.addEventListener('keydown', function (e) {
    if (e.key !== 'Escape') return;
    if (!sheet.hidden) { setSheet(false); burger.focus(); }
    var open = all('.dd.open')[0];
    if (open) { closeDDs(); open.querySelector('.dd-btn').focus(); }
  });

  /* ---- copy buttons ---- */
  function fallbackCopy(text) {
    var ta = document.createElement('textarea');
    ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
    document.body.appendChild(ta); ta.select();
    var ok = false;
    try { ok = document.execCommand('copy'); } catch (e) {}
    ta.remove();
    return ok ? Promise.resolve() : Promise.reject();
  }
  all('.copy').forEach(function (btn) {
    var code = btn.parentNode.querySelector('code'), timer;
    btn.setAttribute('aria-label', 'Copy command: ' + code.textContent.slice(0, 40));
    btn.addEventListener('click', function () {
      var text = code.textContent;
      var attempt = navigator.clipboard ? navigator.clipboard.writeText(text).catch(function () { return fallbackCopy(text); }) : fallbackCopy(text);
      attempt.then(function () {
        btn.textContent = 'Copied'; btn.classList.add('done'); live.textContent = 'Copied to clipboard';
      }, function () {
        btn.textContent = 'Failed'; live.textContent = 'Could not copy. Select the command and copy it by hand.';
      }).then(function () {
        clearTimeout(timer);
        timer = setTimeout(function () { btn.textContent = 'Copy'; btn.classList.remove('done'); live.textContent = ''; }, 1800);
      });
    });
  });

  /* ---- tabs: one behaviour for every tablist ---- */
  function placeBar(list) {
    var bar = list.querySelector('.tab-bar'), sel = list.querySelector('[aria-selected="true"]');
    if (!bar || !sel) return;
    bar.style.width = sel.offsetWidth + 'px';
    bar.style.transform = 'translateX(' + sel.offsetLeft + 'px)';
  }
  function select(tab, focus) {
    var list = tab.parentNode, tabs = all('[role=tab]', list);
    tabs.forEach(function (t) {
      var on = t === tab, panel = $(t.getAttribute('aria-controls'));
      t.setAttribute('aria-selected', String(on));
      t.tabIndex = on ? 0 : -1;
      panel.classList.toggle('on', on);
    });
    placeBar(list);
    if (list.scrollWidth > list.clientWidth) list.scrollLeft = tab.offsetLeft - 24; // keep the choice visible on phones
    if (focus) tab.focus();
  }
  all('[role=tablist]').forEach(function (list) {
    var tabs = all('[role=tab]', list);
    tabs.forEach(function (t, i) {
      t.addEventListener('click', function () { select(t); });
      t.addEventListener('keydown', function (e) {
        var n = e.key === 'ArrowRight' ? i + 1 : e.key === 'ArrowLeft' ? i - 1 : e.key === 'Home' ? 0 : e.key === 'End' ? tabs.length - 1 : null;
        if (n === null) return;
        e.preventDefault();
        select(tabs[(n + tabs.length) % tabs.length], true);
      });
    });
    placeBar(list);
  });
  function allBars() { all('[role=tablist]').forEach(placeBar); }
  addEventListener('resize', allBars);
  if (document.fonts && document.fonts.ready) document.fonts.ready.then(allBars);

  // Menu and footer links can open a specific product tab.
  all('[data-product]').forEach(function (a) {
    a.addEventListener('click', function () {
      var tab = document.querySelector('[aria-controls="' + a.dataset.product + '"]');
      if (tab) select(tab);
      closeDDs();
    });
  });

  /* ---- scroll reveals and the sticky outcome panels ---- */
  if ('IntersectionObserver' in window) {
    var once = new IntersectionObserver(function (es) {
      es.forEach(function (e) { if (e.isIntersecting) { e.target.classList.add('in'); once.unobserve(e.target); } });
    }, { rootMargin: '0px 0px -8% 0px' });
    all('.r').forEach(function (el) { once.observe(el); });

    var outs = all('.out'), stages = all('.stage');
    var spy = new IntersectionObserver(function (es) {
      es.forEach(function (e) {
        if (!e.isIntersecting) return;
        var i = outs.indexOf(e.target);
        outs.forEach(function (o, n) { o.classList.toggle('on', n === i); });
        stages.forEach(function (s, n) { s.classList.toggle('on', n === i); });
      });
    }, { rootMargin: '-42% 0px -52% 0px' });
    outs.forEach(function (o) { spy.observe(o); });
    outs[0].classList.add('on');
  } else {
    all('.out,.stage').forEach(function (o) { o.classList.add('on'); });
  }

  /* ---- pricing slider ---- */
  var slider = $('brokers');
  function price() {
    var n = +slider.value;
    $('broker-count').textContent = n;
    $('total').textContent = '$' + (n * 99).toLocaleString('en-US');
    slider.setAttribute('aria-valuetext', n + (n === 1 ? ' broker, ' : ' brokers, ') + '$' + (n * 99) + ' a month');
    slider.style.setProperty('--p', ((n - 1) / 49 * 100) + '%');
  }
  slider.addEventListener('input', price); price();

  /* ---- the autoscale replay ----
     Everything drawn is a pure function of t, so the loop is exact, the
     scrubber can seek anywhere, and reduced motion is one call at the end. */
  var T = 38, TARGET = 20; // seconds; thousand records/s per broker, from example.yaml
  var MOVES = 6, UP_AT = 12.5, DOWN_AT = 29, GAP = 0.6, DUR = 1.2, WAIT_FROM = 22.7, WAIT_TO = 29;

  function clamp01(x) { return x < 0 ? 0 : x > 1 ? 1 : x; }
  function ramp(t, a, b) { var x = clamp01((t - a) / (b - a)); return x * x * (3 - 2 * x); }
  function loadAt(t) { return 38 + 36 * ramp(t, 4, 9) - 36 * ramp(t, 21, 25) + 0.7 * Math.sin(t / T * Math.PI * 12); }
  function presence(t) { return ramp(t, 9, 11.5) - ramp(t, 33.3, 35.3); }
  function hash(a, b) { var x = Math.sin(a * 127.1 + b * 311.7) * 43758.5453; return x - Math.floor(x); }
  function moved(t, i) { return ramp(t, UP_AT + i * GAP, UP_AT + i * GAP + DUR) - ramp(t, DOWN_AT + i * GAP, DOWN_AT + i * GAP + DUR); }

  var EVENTS = [
    [0, 'Serving on <b>3 brokers</b>'],
    [6.9, '<b>load needs 4 brokers</b>'],
    [8, 'Autoscaled 3 → 4 brokers'],
    [16.9, 'Partitions moved. <b>Ready</b>'],
    [22.7, 'load fits 3; watching for 10m'],
    [33.3, 'Partitions moved off broker 3'],
    [35.3, 'Autoscaled 4 → 3 brokers']
  ];

  function phase(t) {
    var n;
    if (t < 6.9) return ['Ready', 'load fits 3 brokers'];
    if (t < 8) return ['Ready', 'load needs 4 brokers'];
    if (t < UP_AT) return ['Scaling', 'Autoscaled 3 → 4 brokers: load needs 4 brokers'];
    if (t < 16.9) { n = Math.min(MOVES, Math.floor((t - UP_AT - DUR) / GAP) + 1); return ['Scaling', 'moving partitions onto broker 3 · ' + Math.max(0, n) + ' of ' + MOVES]; }
    if (t < WAIT_FROM) return ['Ready', '4 of 4 ready · load fits 4 brokers'];
    if (t < WAIT_TO) {
      var left = Math.ceil(600 * (1 - (t - WAIT_FROM) / (WAIT_TO - WAIT_FROM)));
      return ['Ready', 'load fits 3; watching for ' + Math.floor(left / 60) + 'm ' + String(left % 60).padStart(2, '0') + 's before removing brokers'];
    }
    if (t < 33.3) { n = Math.min(MOVES, Math.floor((t - DOWN_AT - DUR) / GAP) + 1); return ['Scaling', 'moving partitions off broker 3 · ' + Math.max(0, n) + ' of ' + MOVES]; }
    if (t < 35.3) return ['Scaling', 'broker 3 is empty; removing it'];
    return ['Ready', 'Autoscaled 4 → 3 brokers: load stayed low'];
  }

  replay = (function () {
    var cv = $('scope'), ctx = cv.getContext('2d'), scrub = $('scrub'), playBtn = $('play');
    var w = 0, h = 0, c = {}, font = 10, histState = '';
    var motion = matchMedia('(prefers-reduced-motion: reduce)');
    var now = motion.matches ? T - 0.01 : 0, paused = motion.matches, static_ = motion.matches;
    var playing = false, onScreen = true, last = 0;
    var els = { state: $('r-state'), dot: $('r-dot'), brokers: $('r-brokers'), load: $('r-load'), need: $('r-need'), msg: $('r-msg') };
    function set(el, v) { if (el.textContent !== v) el.textContent = v; }

    $('hist').innerHTML = EVENTS.map(function (e, i) {
      return '<li><button type="button" data-t="' + e[0] + '"><span>' + i + '</span><div>' + e[1] + '</div></button></li>';
    }).join('');
    var rows = all('#hist li');
    $('hist').addEventListener('click', function (e) {
      var b = e.target.closest('button');
      if (!b) return;
      seek(Math.min(T - 0.01, +b.dataset.t + 0.02));
    });
    function seek(t) { now = t; static_ = false; paused = true; draw(now); syncScrub(); update(); }

    function restyle() {
      var s = getComputedStyle(cv);
      ['acc', 'ink', 'ink-2', 'ink-3', 'line', 'line-2', 'cell', 'panel'].forEach(function (k) { c[k] = s.getPropertyValue('--' + k).trim(); });
      draw(now);
    }
    function resize() {
      var r = cv.getBoundingClientRect(), d = Math.min(devicePixelRatio || 1, 2);
      if (!r.width) return;
      w = r.width; h = r.height;
      cv.width = Math.round(w * d); cv.height = Math.round(h * d);
      ctx.setTransform(d, 0, 0, d, 0, 0);
      font = w < 420 ? 9 : 10;
      draw(now);
    }
    function text(s, x, y, col, align) { ctx.fillStyle = col; ctx.textAlign = align || 'left'; ctx.fillText(s, x, y); }

    function draw(t) {
      if (!w) return;
      ctx.clearRect(0, 0, w, h);
      ctx.font = font + 'px "Martian Mono", ui-monospace, Menlo, monospace';
      var small = w < 520, pad = small ? 10 : 16, p = presence(t), load = loadAt(t);
      var top = 14, chartH = small ? 78 : 92, rowsB = h - chartH - 24, g = 6, rh = (rowsB - top - g * 3) / 4;
      var busX = pad + (small ? 10 : 18), labelX = busX + 12, rowL = labelX + (small ? 28 : 76), rowR = w - pad, cg = 3;
      var cw = (rowR - rowL - cg * 7) / 8, ch = rh - 7;
      function rowY(b) { return top + b * (rh + g); }
      function cell(b, s) { return { x: rowL + s * (cw + cg), y: rowY(b) }; }
      var mid = rowY(1) + rh / 2 + (rh + g) * .5;

      // Producer stem, bus and branches.
      ctx.strokeStyle = c['line-2']; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad - 6, mid + .5); ctx.lineTo(busX + .5, mid + .5);
      ctx.moveTo(busX + .5, rowY(0) + ch / 2); ctx.lineTo(busX + .5, rowY(2) + ch / 2);
      for (var d = 0; d < 3; d++) { ctx.moveTo(busX, rowY(d) + ch / 2 + .5); ctx.lineTo(labelX - 3, rowY(d) + ch / 2 + .5); }
      ctx.stroke();
      if (p > 0) {
        ctx.globalAlpha = clamp01(p);
        ctx.beginPath(); ctx.moveTo(busX + .5, rowY(2) + ch / 2); ctx.lineTo(busX + .5, rowY(3) + ch / 2 + .5); ctx.lineTo(labelX - 3, rowY(3) + ch / 2 + .5); ctx.stroke();
        ctx.globalAlpha = 1;
      }

      // Records in flight along stem, bus, branch.
      var N = 90, period = T / 26, targets = p >= 1 ? 4 : 3;
      ctx.fillStyle = c.acc;
      for (var k = 0; k < N; k++) {
        var raw = t / period + k / N, cyc = Math.floor(raw), u = raw - cyc;
        if (hash(k, cyc) > load / 90) continue;
        var tgt = Math.floor(hash(k + 7, cyc) * targets), ty = rowY(tgt) + ch / 2;
        var a = busX - (pad - 6), b2 = Math.abs(ty - mid), c3 = labelX - 3 - busX, L = a + b2 + c3, s = u * L;
        var sz = 3;
        if (s < a) ctx.fillRect(pad - 6 + s, mid - 1, sz, sz);
        else if (s < a + b2) ctx.fillRect(busX - 1, mid + (ty > mid ? 1 : -1) * (s - a), sz, sz);
        else ctx.fillRect(busX + (s - a - b2), ty - 1, sz, sz);
      }

      // Broker rows. The fourth row's place is always reserved.
      var util = load / (TARGET * (p >= 1 ? 4 : 3));
      for (var br = 0; br < 4; br++) {
        var y = rowY(br), there = br < 3 || p > 0, st = 'running';
        if (br === 3) { if (!there) st = 'not running'; else if (t < 11.5) st = 'starting'; else if (t >= DOWN_AT) st = 'draining'; }
        ctx.globalAlpha = br === 3 && there ? Math.max(.35, clamp01(p)) : 1;
        text(small ? 'b' + br : 'broker ' + br, labelX, y + font + 1, br === 3 && !there ? c['ink-3'] : c.ink);
        if (!small) text(st, labelX, y + font * 2 + 6, st === 'running' || st === 'not running' ? c['ink-3'] : c.acc);
        ctx.globalAlpha = 1;
        ctx.strokeStyle = c.line;
        for (var sl = 0; sl < 8; sl++) {
          var q = cell(br, sl);
          if (br < 3 && sl < 6) {
            ctx.fillStyle = c.cell; ctx.fillRect(q.x, q.y, cw, ch);
            var tick = Math.floor(t * 5), f = t * 5 - tick;
            if (hash(br * 8 + sl, tick) < load / 420) { ctx.globalAlpha = (1 - f) * .85; ctx.fillStyle = c.acc; ctx.fillRect(q.x, q.y, cw, ch); ctx.globalAlpha = 1; }
            if (cw > 22) text(String(br * 6 + sl), q.x + 5, q.y + font + 3, c['ink-2']);
          } else ctx.strokeRect(q.x + .5, q.y + .5, cw - 1, ch - 1);
        }
        var uy = y + ch + 3, ub = rowR - rowL, u2 = br === 3 ? (t >= 12.5 && t < DOWN_AT + 3 ? util : 0) : util;
        ctx.fillStyle = c.line; ctx.fillRect(rowL, uy, ub, 2);
        ctx.fillStyle = c.acc; ctx.fillRect(rowL, uy, ub * Math.min(1, u2), 2);
      }

      // Migrating partitions, drawn last so they cross over the rows.
      for (var i = 0; i < MOVES; i++) {
        var m = moved(t, i), from = cell(i % 3, 6 + Math.floor(i / 3)), to = cell(3, i), moving = m > .001 && m < .999;
        var mx = from.x + (to.x - from.x) * m, my = from.y + (to.y - from.y) * m;
        ctx.fillStyle = moving ? c.acc : c.cell; ctx.fillRect(mx, my, cw, ch);
        if (cw > 22) text(String(18 + i), mx + 5, my + font + 3, moving ? c.panel : c['ink-2']);
      }

      // Load against capacity.
      var cL = pad, cR = w - pad, cW = cR - cL, cT = rowsB + 24, cB = h - 8, MAX = 90;
      function X(v) { return cL + cW * v / T; }
      function Y(v) { return cB - (cB - cT) * v / MAX; }
      ctx.strokeStyle = c.line; ctx.beginPath(); ctx.moveTo(cL, rowsB + 8.5); ctx.lineTo(cR, rowsB + 8.5); ctx.moveTo(cL, cB + .5); ctx.lineTo(cR, cB + .5); ctx.stroke();
      text('records/s', cL, cT - 1, c.acc);
      text('- - capacity', cR, cT - 1, c['ink-2'], 'right');
      ctx.strokeStyle = c['ink-2']; ctx.setLineDash([3, 3]); ctx.beginPath();
      ctx.moveTo(X(0), Y(60)); ctx.lineTo(X(11.5), Y(60)); ctx.lineTo(X(11.5), Y(80));
      ctx.lineTo(X(35.3), Y(80)); ctx.lineTo(X(35.3), Y(60)); ctx.lineTo(X(T), Y(60)); ctx.stroke();
      ctx.setLineDash([]);
      ctx.strokeStyle = c.acc; ctx.lineWidth = 1.75; ctx.beginPath();
      var end = Math.min(t, T);
      for (var v = 0; v <= end + .001; v += .15) { if (v === 0) ctx.moveTo(X(v), Y(loadAt(v))); else ctx.lineTo(X(v), Y(loadAt(v))); }
      ctx.stroke(); ctx.lineWidth = 1;
      ctx.fillStyle = c.acc; ctx.fillRect(X(end) - 3, Y(loadAt(end)) - 3, 6, 6);

      // Readouts.
      var ph = phase(t), rate = Math.round(load * 10) * 100;
      set(els.state, ph[0]);
      var cls = 'status ' + (ph[0] === 'Ready' ? 'ok' : 'warn');
      if (els.dot.className !== cls) els.dot.className = cls;
      set(els.brokers, (t >= 11.5 && t < 35.3 ? 4 : 3) + ' of ' + (p > 0 ? 4 : 3));
      set(els.load, rate.toLocaleString('en-US'));
      set(els.need, Math.max(3, Math.ceil(load / TARGET)) + ' brokers');
      set(els.msg, static_ && t > T - 1 ? 'Scaled 3 → 4 on load, then 4 → 3 once load stayed low.' : ph[1]);

      var seen = EVENTS.filter(function (e) { return e[0] <= t; }).length, key = String(seen);
      if (key !== histState) {
        histState = key;
        rows.forEach(function (li, n) { li.classList.toggle('seen', n < seen); li.classList.toggle('now', n === seen - 1); });
      }
      syncScrub();
    }
    function syncScrub() {
      if (document.activeElement !== scrub || playing) { var v = String(Math.round(now * 10)); if (scrub.value !== v) scrub.value = v; }
      scrub.style.setProperty('--p', (now / T * 100) + '%');
    }
    function frame(ms) {
      if (!playing) return;
      var dt = last ? Math.min(.1, (ms - last) / 1000) : 0;
      last = ms; now = (now + dt) % T;
      draw(now);
      requestAnimationFrame(frame);
    }
    function update() {
      $('play-ic').setAttribute('href', paused ? '#i-play' : '#i-pause');
      playBtn.setAttribute('aria-label', (paused ? 'Play' : 'Pause') + ' replay');
      var should = !paused && onScreen && !document.hidden;
      if (should && !playing) { playing = true; last = 0; requestAnimationFrame(frame); }
      if (!should) playing = false;
    }
    playBtn.addEventListener('click', function () { paused = !paused; static_ = false; update(); });
    scrub.addEventListener('input', function () { seek(+scrub.value / 10); });
    motion.addEventListener('change', function () {
      if (motion.matches) { paused = true; static_ = true; now = T - .01; draw(now); } else { paused = false; static_ = false; }
      update();
    });
    document.addEventListener('visibilitychange', update);
    if ('IntersectionObserver' in window) new IntersectionObserver(function (e) { onScreen = e[0].isIntersecting; update(); }).observe(cv);
    if ('ResizeObserver' in window) new ResizeObserver(resize).observe(cv); else addEventListener('resize', resize);
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(function () { draw(now); });

    restyle(); resize(); update();
    return { restyle: restyle };
  })();

  /* ---- the rail is the log: committed offset and lag follow the reader ---- */
  (function () {
    var links = all('#log a'), secs = links.map(function (a) { return document.querySelector(a.getAttribute('href')); });
    var logEl = $('log'), offEl = $('lag-off'), lagEl = $('lag-n'), cur = -1;
    secs[0] = document.querySelector('.hero');
    function tick() {
      var line = innerHeight * .35, i = 0;
      secs.forEach(function (s, n) { if (s && s.getBoundingClientRect().top <= line) i = n; });
      if (scrollY + innerHeight >= document.documentElement.scrollHeight - 80) i = links.length - 1;
      if (i === cur) return;
      cur = i;
      links.forEach(function (a, n) {
        if (n === i) a.setAttribute('aria-current', 'true'); else a.removeAttribute('aria-current');
        a.parentNode.classList.toggle('past', n < i);
      });
      offEl.textContent = String(i).padStart(2, '0');
      lagEl.textContent = String(links.length - 1 - i);
      logEl.style.setProperty('--done', (links[i].parentNode.offsetTop) + 'px');
    }
    addEventListener('scroll', tick, { passive: true }); addEventListener('resize', tick); tick();
  })();

  /* ---- boundary diagram: each component explains itself ---- */
  (function () {
    var nodes = all('#bound .node'), note = $('bound-note');
    nodes.forEach(function (n) {
      function pick() { nodes.forEach(function (o) { o.classList.toggle('on', o === n); }); note.textContent = n.dataset.note; }
      n.addEventListener('click', pick); n.addEventListener('mouseenter', pick); n.addEventListener('focus', pick);
    });
  })();

  themeUI();
})();
