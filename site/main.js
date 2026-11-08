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
  matchMedia('(prefers-color-scheme: light)').addEventListener('change', function (e) {
    try { if (localStorage.getItem('kaas-theme')) return; } catch (err) {}
    root.dataset.theme = e.matches ? 'light' : 'dark';
    themeUI();
  });

  /* ---- navigation ---- */
  var nav = $('nav');
  function stuck() { nav.classList.toggle('stuck', scrollY > 8); }
  addEventListener('scroll', stuck, { passive: true }); stuck();

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
  addEventListener('resize', function () { if (innerWidth >= 900 && !sheet.hidden) setSheet(false); });
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
    var pad = list.classList.contains('big') ? 18 : 0;
    bar.style.width = (sel.offsetWidth - pad) + 'px';
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
    [6.9, '<b>load needs 4 brokers</b>'],
    [8, '<b>Autoscaled</b> 3 → 4 brokers'],
    [16.9, '<b>Partitions moved</b> onto broker 3'],
    [22.7, '<b>load fits 3;</b> watching for 10m'],
    [33.3, '<b>Partitions moved</b> off broker 3'],
    [35.3, '<b>Autoscaled</b> 4 → 3 brokers']
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
    var w = 0, h = 0, c = {}, font = 10, histCount = -1;
    var motion = matchMedia('(prefers-reduced-motion: reduce)');
    var now = motion.matches ? T - 0.01 : 0, paused = motion.matches, static_ = motion.matches;
    var playing = false, onScreen = true, last = 0, scrubbing = false;
    var els = { state: $('r-state'), dot: $('r-dot'), brokers: $('r-brokers'), load: $('r-load'), need: $('r-need'), msg: $('r-msg'), side: $('side-rate') };
    function set(el, v) { if (el.textContent !== v) el.textContent = v; }

    function restyle() {
      var s = getComputedStyle(cv);
      ['accent', 'ink', 'ink-2', 'ink-3', 'line', 'line-2', 'warn', 'bg-2'].forEach(function (k) { c[k] = s.getPropertyValue('--' + k).trim(); });
      draw(now);
    }
    function resize() {
      var r = cv.getBoundingClientRect(), d = Math.min(devicePixelRatio || 1, 2);
      if (!r.width) return;
      w = r.width; h = r.height;
      cv.width = Math.round(w * d); cv.height = Math.round(h * d);
      ctx.setTransform(d, 0, 0, d, 0, 0);
      font = w < 420 ? 9.5 : 10.5;
      draw(now);
    }
    function text(s, x, y, col, align) { ctx.fillStyle = col; ctx.textAlign = align || 'left'; ctx.fillText(s, x, y); }

    function draw(t) {
      if (!w) return;
      ctx.clearRect(0, 0, w, h);
      ctx.font = font + 'px "Martian Mono", ui-monospace, Menlo, monospace';
      var pad = w < 420 ? 12 : 18;
      var busY = 26, topY = 52, bH = h * 0.64 - 40, chartT = h * 0.74, chartB = h - 10;
      var p = presence(t), pitch = Math.min((w - pad * 2) / 4, 160), bw = pitch - (w < 420 ? 6 : 12);
      var nEff = 3 + p, cx = w / 2, load = loadAt(t);
      function bx(i) { return cx + (i - (nEff - 1) / 2) * pitch; }

      var inner = bw - 12, cg = 3, cw = (inner - cg) / 2, hdr = font * 3.2, footer = 14;
      var ch = Math.min(24, (bH - hdr - footer - cg * 3 - 4) / 4);
      function slot(i, s) { return { x: bx(i) - bw / 2 + 6 + (s % 2) * (cw + cg), y: topY + hdr + Math.floor(s / 2) * (ch + cg) }; }

      // producer bus
      ctx.strokeStyle = c.line; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad, busY); ctx.lineTo(w - pad, busY); ctx.stroke();
      text('producers', pad, busY - 8, c['ink-2']);
      for (var d = 0; d < 4; d++) {
        if (d === 3 && p <= 0) continue;
        ctx.globalAlpha = d === 3 ? p : 1;
        ctx.beginPath(); ctx.moveTo(bx(d), busY); ctx.lineTo(bx(d), topY); ctx.stroke();
      }
      ctx.globalAlpha = 1;

      // records in flight
      var N = 110, period = T / 24, x0 = pad, dy = topY - busY, targets = p >= 1 ? 4 : 3;
      ctx.fillStyle = c.accent;
      for (var k = 0; k < N; k++) {
        var raw = t / period + k / N, cyc = Math.floor(raw), u = raw - cyc;
        if (hash(k, cyc) > load / 80) continue;
        var tgt = Math.floor(hash(k + 7, cyc) * targets), tx = bx(tgt);
        var run = tx - x0, L = run + dy, s = u * (w - pad - x0 + dy);
        if (s > L) continue;
        if (s < run) ctx.fillRect(x0 + s, busY - 1, 7, 2);
        else ctx.fillRect(tx - 1, busY + (s - run), 2, 7);
      }

      // brokers
      var util = load / (TARGET * (p >= 1 ? 4 : 3)), count = p > 0 ? 4 : 3;
      for (var b = 0; b < count; b++) {
        var bl = bx(b) - bw / 2;
        ctx.globalAlpha = b === 3 ? clamp01(p) : 1;
        ctx.fillStyle = c['bg-2']; ctx.fillRect(bl, topY, bw, bH);
        ctx.strokeStyle = b === 3 && p < 1 ? c.accent : c['line-2'];
        ctx.lineWidth = 1; ctx.strokeRect(bl + .5, topY + .5, bw - 1, bH - 1);
        text(bw < 90 ? 'b' + b : 'broker ' + b, bl + 6, topY + font * 1.5, c.ink);
        var st = 'running';
        if (b === 3) { if (t < 11.5) st = 'starting'; else if (t >= DOWN_AT) st = 'draining'; }
        text(st, bl + 6, topY + font * 2.75, st === 'running' ? c['ink-2'] : c.warn);

        ctx.strokeStyle = c.line;
        for (var sl = 0; sl < 8; sl++) { var q = slot(b, sl); ctx.strokeRect(q.x + .5, q.y + .5, cw - 1, ch - 1); }
        if (b < 3) {
          ctx.fillStyle = c['line-2'];
          for (var r = 0; r < 6; r++) { var pos = slot(b, r); ctx.fillRect(pos.x, pos.y, cw, ch); }
        }
        var by = topY + bH - 8, bar = bw - 12;
        ctx.fillStyle = c.line; ctx.fillRect(bl + 6, by, bar, 3);
        var u2 = b === 3 ? (t >= 12.5 && t < DOWN_AT + 3 ? util : 0) : util;
        ctx.fillStyle = u2 > 1 ? c.warn : c.accent;
        ctx.fillRect(bl + 6, by, bar * Math.min(1, u2), 3);
        ctx.globalAlpha = 1;
      }

      // migrating partitions: drawn last so they cross over the brokers
      for (var i = 0; i < MOVES; i++) {
        var m = moved(t, i), from = slot(i % 3, 6 + Math.floor(i / 3)), to = slot(3, i);
        var lift = Math.sin(m * Math.PI) * bH * 0.28;
        ctx.fillStyle = m > 0.001 && m < 0.999 ? c.accent : c['line-2'];
        ctx.fillRect(from.x + (to.x - from.x) * m, from.y + (to.y - from.y) * m - lift, cw, ch);
      }

      // load vs capacity
      var cL = pad, cR = w - pad, cW = cR - cL, MAX = 90;
      function X(s) { return cL + cW * s / T; }
      function Y(v) { return chartB - (chartB - chartT) * v / MAX; }
      ctx.strokeStyle = c.line; ctx.beginPath(); ctx.moveTo(cL, chartB + .5); ctx.lineTo(cR, chartB + .5); ctx.stroke();
      text('records/s', cL, chartT - 2, c.accent);
      text('- - capacity', cR, chartT - 2, c['ink-2'], 'right');
      ctx.strokeStyle = c['ink-2']; ctx.setLineDash([4, 4]); ctx.beginPath();
      ctx.moveTo(X(0), Y(60)); ctx.lineTo(X(11.5), Y(60)); ctx.lineTo(X(11.5), Y(80));
      ctx.lineTo(X(35.3), Y(80)); ctx.lineTo(X(35.3), Y(60)); ctx.lineTo(X(T), Y(60)); ctx.stroke();
      ctx.setLineDash([]);
      ctx.strokeStyle = c.accent; ctx.lineWidth = 1.75; ctx.beginPath();
      var end = Math.min(t, T);
      for (var a = 0; a <= end + 0.001; a += 0.15) { var xx = X(a), yy = Y(loadAt(a)); if (a === 0) ctx.moveTo(xx, yy); else ctx.lineTo(xx, yy); }
      ctx.stroke(); ctx.lineWidth = 1;
      ctx.fillStyle = c.accent; ctx.beginPath(); ctx.arc(X(end), Y(loadAt(end)), 3, 0, 7); ctx.fill();

      // readouts
      var ph = phase(t), rate = Math.round(load * 10) * 100;
      set(els.state, ph[0]);
      var cls = 'status ' + (ph[0] === 'Ready' ? 'ok' : 'warn');
      if (els.dot.className !== cls) els.dot.className = cls;
      set(els.brokers, (t >= 11.5 && t < 35.3 ? 4 : 3) + ' of ' + (p > 0 ? 4 : 3));
      set(els.load, rate.toLocaleString('en-US'));
      set(els.side, (rate / 1000).toFixed(1) + 'k/s');
      set(els.need, Math.max(3, Math.ceil(load / TARGET)) + ' brokers');
      if (!(static_ && t > T - 1)) set(els.msg, ph[1]);
      else set(els.msg, 'Scaled 3 → 4 on load, then 4 → 3 once load stayed low.');

      // history feed: rebuilt only when the number of past events changes
      var seen = EVENTS.filter(function (e) { return e[0] <= t; });
      if (seen.length !== histCount) {
        histCount = seen.length;
        $('hist').innerHTML = (seen.length ? seen.slice().reverse() : [[0, '<b>Created</b> serving on 3 brokers']]).slice(0, 5)
          .map(function (e) { return '<li>' + e[1] + '</li>'; }).join('');
      }
      if (!scrubbing) syncScrub();
    }
    function syncScrub() {
      var v = String(Math.round(now * 10));
      if (scrub.value !== v) scrub.value = v;
      scrub.style.setProperty('--p', (now / T * 100) + '%');
    }

    function frame(ms) {
      if (!playing) return;
      var dt = last ? Math.min(0.1, (ms - last) / 1000) : 0;
      last = ms; now = (now + dt) % T;
      draw(now);
      requestAnimationFrame(frame);
    }
    function update() {
      $('play-ic').setAttribute('href', paused ? '#i-play' : '#i-pause');
      playBtn.setAttribute('aria-label', (paused ? 'Play' : 'Pause') + ' replay');
      var should = !paused && !scrubbing && onScreen && !document.hidden;
      if (should && !playing) { playing = true; last = 0; requestAnimationFrame(frame); }
      if (!should) playing = false;
    }
    playBtn.addEventListener('click', function () { paused = !paused; static_ = false; update(); });
    scrub.addEventListener('input', function () {
      now = +scrub.value / 10; static_ = false; paused = true;
      scrub.style.setProperty('--p', (now / T * 100) + '%');
      draw(now); update();
    });
    motion.addEventListener('change', function () {
      if (motion.matches) { paused = true; static_ = true; now = T - 0.01; draw(now); } else { paused = false; static_ = false; }
      update();
    });
    document.addEventListener('visibilitychange', update);
    if ('IntersectionObserver' in window) new IntersectionObserver(function (e) { onScreen = e[0].isIntersecting; update(); }).observe(cv);
    if ('ResizeObserver' in window) new ResizeObserver(resize).observe(cv); else addEventListener('resize', resize);
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(function () { draw(now); });

    restyle(); resize(); update();
    return { restyle: restyle };
  })();

  themeUI();
})();
