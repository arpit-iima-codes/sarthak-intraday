(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const theadEl = $("thead");
  const rowsEl = $("rows");
  const tableWrap = $("tablewrap");
  const emptyEl = $("empty");

  // --- columns ---------------------------------------------------------
  const COLS = [
    { k: "symbol",   label: "Symbol",         grp: "",       cls: "col-sym" },
    { k: "yclose",   label: "Y.Close",        grp: "static", cls: "num" },
    { k: "s_open",   label: "Open",           grp: "static", cls: "num" },
    { k: "s_high",   label: "High",           grp: "static", cls: "num" },
    { k: "s_low",    label: "Low",            grp: "static", cls: "num" },
    { k: "s_pct",    label: "% ↑ close", grp: "static", cls: "num pct" },
    { k: "mom_pct",  label: "% open→high", grp: "static", cls: "num pct" },
    { k: "ltp",      label: "LTP",            grp: "live",   cls: "num live" },
    { k: "live_pct", label: "% ↑ 10:00 high", grp: "live", cls: "num live pct" },
  ];
  const BY_KEY = Object.fromEntries(COLS.map((c) => [c.k, c]));
  const ALL_KEYS = COLS.map((c) => c.k);
  const LS_KEY = "scanner.colorder.v3";   // v3 adds the momentum column
  const LS_VIEW = "scanner.view";

  function loadOrder() {
    try {
      const s = JSON.parse(localStorage.getItem(LS_KEY));
      if (Array.isArray(s)) {
        const kept = s.filter((k) => BY_KEY[k]);
        for (const k of ALL_KEYS) if (!kept.includes(k)) kept.push(k);
        return kept;
      }
    } catch {}
    return ALL_KEYS.slice();
  }
  function saveOrder() {
    try {
      localStorage.setItem(LS_KEY, JSON.stringify(order));
    } catch {}
  }

  let order = loadOrder();
  const cols = () => order.map((k) => BY_KEY[k]);

  let latest = null;
  let sortKey = "live_pct";
  let sortDir = -1;
  let clockBase = null;
  let dragging = false;
  let freezeLabel = "10:00";
  let view = (() => {
    try {
      return localStorage.getItem(LS_VIEW) === "all" ? "all" : "momentum";
    } catch {
      return "momentum";
    }
  })();

  // --- formatting -----------------------------------------------------
  const num = (v, d = 2) =>
    v === null || v === undefined || Number.isNaN(v)
      ? "—"
      : Number(v).toLocaleString("en-IN", { minimumFractionDigits: d, maximumFractionDigits: d });
  const pct = (v) => (v === null || v === undefined ? "—" : (v > 0 ? "+" : "") + num(v) + "%");
  const pctClass = (v) => (v === null || v === undefined ? "" : v > 0 ? "up" : v < 0 ? "down" : "");
  const disp = (sym) => String(sym).replace(/^[A-Z]+:/, "").replace(/-(EQ|INDEX)$/, "");
  const ago = (s) => {
    if (s === null || s === undefined) return "";
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  };

  // --- header --------------------------------------------------------
  const GRP_LABEL = () => ({
    static: "Static — frozen at " + freezeLabel,
    live: "Live",
  });

  function renderHeader() {
    const cs = cols();

    // group row: coalesce consecutive same-group columns
    let groupRow = "<tr class='grp'>";
    let i = 0;
    while (i < cs.length) {
      const g = cs[i].grp;
      let span = 1;
      while (i + span < cs.length && cs[i + span].grp === g) span++;
      const label = g ? GRP_LABEL()[g] : "";
      groupRow += `<th colspan="${span}" class="grp-${g || "none"}">${label}</th>`;
      i += span;
    }
    groupRow += "</tr>";

    const headRow =
      "<tr>" +
      cs
        .map((c) => {
          const sorted = c.k === sortKey ? " sorted" : "";
          const dir = c.k === sortKey ? (sortDir === 1 ? "asc" : "desc") : "";
          return `<th draggable="true" data-k="${c.k}" data-dir="${dir}"
                    class="${c.cls}${sorted}">${c.label}</th>`;
        })
        .join("") +
      "</tr>";

    theadEl.innerHTML = groupRow + headRow;
    bindHeader();
  }

  function bindHeader() {
    theadEl.querySelectorAll("th[data-k]").forEach((th) => {
      const key = th.dataset.k;

      th.addEventListener("click", () => {
        if (dragging) return;
        if (key === sortKey) sortDir = -sortDir;
        else {
          sortKey = key;
          sortDir = key === "symbol" ? 1 : -1;
        }
        renderHeader();
        renderRows();
      });

      th.addEventListener("dragstart", (e) => {
        dragging = true;
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain", key);
        th.classList.add("dragging");
      });
      th.addEventListener("dragend", () => {
        th.classList.remove("dragging");
        theadEl.querySelectorAll(".drop-target").forEach((e) => e.classList.remove("drop-target"));
        setTimeout(() => (dragging = false), 0);
      });
      th.addEventListener("dragover", (e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
        th.classList.add("drop-target");
      });
      th.addEventListener("dragleave", () => th.classList.remove("drop-target"));
      th.addEventListener("drop", (e) => {
        e.preventDefault();
        th.classList.remove("drop-target");
        const from = e.dataTransfer.getData("text/plain");
        if (!from || from === key) return;
        const fi = order.indexOf(from);
        order.splice(fi, 1);
        order.splice(order.indexOf(key), 0, from);
        saveOrder();
        renderHeader();
        renderRows();
      });
    });
  }

  // --- rows ---------------------------------------------------------
  // --- momentum view ---------------------------------------------------
  // "momentum" shows only what the engine may trade today; "all" is the whole
  // universe. The shortlist is computed server-side and shipped in the
  // snapshot, so this view and the engine can never disagree.
  function renderMomNote(all, shown) {
    const note = $("momnote");
    if (!note) return;
    if (view !== "momentum" || !all.length) {
      note.hidden = true;
      return;
    }
    const at = latest.freeze_time || freezeLabel;
    const top = latest.momentum_top;
    const floor = latest.momentum_min_pct;
    if (!latest.frozen) {
      note.textContent =
        `The ${at} high is still forming, so today's movers aren't ranked yet. ` +
        `Switch to Full Universe to watch all ${all.length} symbols meanwhile.`;
      note.className = "mom-note";
    } else if (!shown.length) {
      note.textContent =
        `Nothing gained ${floor}% or more from the open to the ${at} high today, ` +
        `so the engine has nothing to trade.`;
      note.className = "mom-note mom-note-warn";
    } else {
      note.textContent =
        `Top ${top} movers that gained at least ${floor}% from the open to the ${at} high` +
        ` — ${shown.length} qualified. The engine can only buy these.`;
      note.className = "mom-note";
    }
    note.hidden = false;
  }

  // --- rebuild progress ------------------------------------------------
  // Present while a mid-day history reconstruction is running (a restart, or
  // recovery after the feed/token was down). Blocks the table entirely until
  // frozen (data isn't trustworthy yet); after that it's just a background
  // fill and the table is already usable, so the note shrinks to a strip.
  function renderRebuild() {
    const card = $("rebuilding");
    const fill = $("fillnote");
    if (!card || !fill) return;
    const rb = latest.rebuild;
    if (!rb) {
      card.hidden = true;
      fill.hidden = true;
      return;
    }
    const pct = rb.total ? Math.min(100, Math.round((rb.done / rb.total) * 100)) : 0;
    const mins = rb.elapsed_s ? Math.floor(rb.elapsed_s / 60) : 0;
    const doneStr = rb.done.toLocaleString("en-IN");
    const totalStr = rb.total.toLocaleString("en-IN");
    if (!latest.frozen) {
      card.hidden = false;
      fill.hidden = true;
      $("rebuild-bar-fill").style.width = pct + "%";
      $("rebuild-meta").textContent =
        `${doneStr} / ${totalStr} symbols · ${pct}%` + (mins ? ` · ${mins}m elapsed` : "");
    } else {
      card.hidden = true;
      fill.hidden = false;
      fill.textContent =
        `Still filling in the rest of the universe in the background — ${doneStr} / ${totalStr} done (${pct}%).`;
    }
  }

  function renderRows() {
    if (!latest) return;
    const all = latest.rows || [];
    const hasUniverse = all.length > 0;
    const blockedByRebuild = !!latest.rebuild && !latest.frozen;

    renderRebuild();
    emptyEl.hidden = hasUniverse || blockedByRebuild;

    if (blockedByRebuild) {
      tableWrap.hidden = true;
      $("momnote").hidden = true;
      return;
    }

    let rows = all;
    if (view === "momentum") {
      const keep = new Set(latest.momentum || []);
      rows = all.filter((r) => keep.has(r.symbol));
    }
    renderMomNote(all, rows);

    tableWrap.hidden = !(hasUniverse && rows.length);
    if (!rows.length) return;

    rows = rows.slice();
    rows.sort((a, b) => {
      let x = a[sortKey], y = b[sortKey];
      if (sortKey === "symbol") return sortDir * disp(x).localeCompare(disp(y));
      x = x === null || x === undefined ? -Infinity : x;
      y = y === null || y === undefined ? -Infinity : y;
      return sortDir * (x - y);
    });

    const cs = cols();
    rowsEl.innerHTML = rows
      .map(
        (r) =>
          "<tr>" +
          cs
            .map((c) => {
              const v = r[c.k];
              if (c.k === "symbol")
                return `<td class="col-sym" title="${r.symbol}">${disp(r.symbol)}</td>`;
              if (c.cls.includes("pct"))
                return `<td class="${c.cls} ${pctClass(v)}">${pct(v)}</td>`;
              return `<td class="${c.cls}">${num(v)}</td>`;
            })
            .join("") +
          "</tr>"
      )
      .join("");
  }

  // --- status strip ------------------------------------------------
  const SOCK = {
    connected: ["ok", "connected"],
    connecting: ["warn", "connecting…"],
    disconnected: ["bad", "disconnected"],
    stale: ["warn", "feed stalled"],
    error: ["bad", "error"],
    "no-credentials": ["bad", "no broker session"],
    idle: ["dim", "idle (market closed)"],
  };

  function renderStatus(s) {
    if (s.day) $("daylabel").textContent = s.day;
    if (s.freeze_time && s.freeze_time !== freezeLabel) {
      freezeLabel = s.freeze_time;
      if (!dragging) renderHeader();
    }
    $("freezetime") && ($("freezetime").textContent = s.freeze_time);

    const fz = $("freeze");
    const sess = s.data_date
      ? new Date(s.data_date + "T00:00:00").toLocaleDateString("en-GB", {
          day: "2-digit",
          month: "short",
        })
      : null;
    if (s.frozen) {
      const t = s.frozen_at ? new Date(s.frozen_at).toLocaleTimeString("en-GB") : "";
      fz.textContent = "static: " + (sess ? sess + " session · " : "") + "frozen " + t;
      fz.className = "chip chip-ok";
    } else if (s.rebuild) {
      const rb = s.rebuild;
      const pct = rb.total ? Math.round((rb.done / rb.total) * 100) : 0;
      fz.textContent = `static: rebuilding ${rb.done}/${rb.total} (${pct}%)`;
      fz.className = "chip chip-warn";
    } else if (sess && s.data_stale) {
      fz.textContent = "static: " + sess + " session · forms " + s.freeze_time;
      fz.className = "chip chip-warn";
    } else {
      fz.textContent =
        "static: " + (sess ? sess + " · " : "") + "forming until " + s.freeze_time;
      fz.className = "chip chip-dim";
    }

    const sk = s.socket || {};
    const [cls, label] = SOCK[sk.status] || ["dim", sk.status || "—"];
    let txt = "socket: " + label;
    if (sk.status === "connected" && sk.last_msg_seconds != null)
      txt += " · tick " + ago(sk.last_msg_seconds);
    else if (["disconnected", "stale", "error"].includes(sk.status))
      txt += " · " + ago(sk.for_seconds);
    const pill = $("sockpill");
    pill.textContent = txt;
    pill.className = "chip chip-" + cls;
  }

  function render() {
    if (!latest) return;
    renderStatus(latest);
    renderRows();
  }

  // --- clock ------------------------------------------------------
  function tickClock() {
    const d = clockBase
      ? new Date(clockBase.serverMs + (Date.now() - clockBase.localMs))
      : new Date();
    $("clock").textContent = d.toLocaleTimeString("en-GB", { timeZone: "Asia/Kolkata" });
  }
  setInterval(tickClock, 1000);
  tickClock();

  // --- view tabs -------------------------------------------------------
  function syncTabs() {
    for (const el of document.querySelectorAll(".viewtab")) {
      const on = el.dataset.view === view;
      el.classList.toggle("is-on", on);
      el.setAttribute("aria-selected", on ? "true" : "false");
    }
  }
  for (const el of document.querySelectorAll(".viewtab")) {
    el.addEventListener("click", () => {
      view = el.dataset.view === "all" ? "all" : "momentum";
      try {
        localStorage.setItem(LS_VIEW, view);
      } catch {}
      syncTabs();
      renderRows();
    });
  }
  syncTabs();

  // --- reset layout --------------------------------------------------
  $("resetcols").addEventListener("click", () => {
    order = ALL_KEYS.slice();
    saveOrder();
    renderHeader();
    renderRows();
  });

  // --- SSE ------------------------------------------------------------
  function connect() {
    const es = new EventSource("/api/scanner/stream");
    es.onmessage = (ev) => {
      try {
        latest = JSON.parse(ev.data);
      } catch {
        return;
      }
      clockBase = { serverMs: new Date(latest.server_time).getTime(), localMs: Date.now() };
      render();
    };
    es.onerror = () => {
      const pill = $("sockpill");
      pill.textContent = "stream: reconnecting…";
      pill.className = "chip chip-warn";
    };
  }

  renderHeader();
  connect();

  fetch("/api/scanner/state")
    .then((r) => r.json())
    .then((s) => {
      if (!latest) {
        latest = s;
        clockBase = { serverMs: new Date(s.server_time).getTime(), localMs: Date.now() };
        render();
      }
    })
    .catch(() => {});
})();
