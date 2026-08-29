(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const rowsEl = $("rows");
  const tableWrap = $("tablewrap");
  const emptyEl = $("empty");

  let latest = null;            // last snapshot
  let sortKey = "live_pct";
  let sortDir = -1;             // 1 asc, -1 desc
  let clockBase = null;         // {serverMs, localMs}

  // ---- formatting -------------------------------------------------------
  const num = (v, d = 2) =>
    v === null || v === undefined || Number.isNaN(v)
      ? "—"
      : Number(v).toLocaleString("en-IN", { minimumFractionDigits: d, maximumFractionDigits: d });

  const pct = (v) => (v === null || v === undefined ? "—" : (v > 0 ? "+" : "") + num(v) + "%");

  const pctClass = (v) => (v === null || v === undefined ? "" : v > 0 ? "up" : v < 0 ? "down" : "");

  // "NSE:SBIN-EQ" -> "SBIN", "NSE:NIFTY50-INDEX" -> "NIFTY50"
  const disp = (sym) =>
    String(sym).replace(/^[A-Z]+:/, "").replace(/-(EQ|INDEX)$/, "");

  const ago = (s) => {
    if (s === null || s === undefined) return "";
    if (s < 60) return s + "s ago";
    if (s < 3600) return Math.floor(s / 60) + "m ago";
    return Math.floor(s / 3600) + "h ago";
  };

  // ---- socket pill -----------------------------------------------------
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
    $("clock");
    if (s.day) $("daylabel").textContent = s.day;
    $("freezetime") && ($("freezetime").textContent = s.freeze_time);
    document.querySelectorAll(".ft").forEach((e) => (e.textContent = s.freeze_time));

    const fz = $("freeze");
    if (s.frozen) {
      const t = s.frozen_at ? new Date(s.frozen_at).toLocaleTimeString("en-GB") : "";
      fz.textContent = "static: frozen " + t;
      fz.className = "chip chip-ok";
    } else {
      fz.textContent = "static: forming until " + s.freeze_time;
      fz.className = "chip chip-dim";
    }

    const sk = s.socket || {};
    const [cls, label] = SOCK[sk.status] || ["dim", sk.status || "—"];
    const pill = $("sockpill");
    let txt = "socket: " + label;
    if (sk.status === "connected" && sk.last_msg_seconds != null)
      txt += " · tick " + ago(sk.last_msg_seconds);
    else if (["disconnected", "stale", "error"].includes(sk.status))
      txt += " · " + ago(sk.for_seconds);
    pill.textContent = txt;
    pill.className = "chip chip-" + cls;
  }

  // ---- table ----------------------------------------------------------
  function renderRows(s) {
    const rows = s.rows || [];
    const has = rows.length > 0;
    tableWrap.hidden = !has;
    emptyEl.hidden = has;
    if (!has) return;

    rows.sort((a, b) => {
      let x = a[sortKey], y = b[sortKey];
      if (sortKey === "symbol") return sortDir * disp(x).localeCompare(disp(y));
      x = x === null || x === undefined ? -Infinity : x;
      y = y === null || y === undefined ? -Infinity : y;
      return sortDir * (x - y);
    });

    rowsEl.innerHTML = rows
      .map(
        (r) => `<tr>
        <td class="col-sym" title="${r.symbol}">${disp(r.symbol)}</td>
        <td class="num">${num(r.yclose)}</td>
        <td class="num">${num(r.s_open)}</td>
        <td class="num">${num(r.s_high)}</td>
        <td class="num">${num(r.s_low)}</td>
        <td class="num">${num(r.s_ltp)}</td>
        <td class="num ${pctClass(r.s_pct)}">${pct(r.s_pct)}</td>
        <td class="num live">${num(r.ltp)}</td>
        <td class="num live ${pctClass(r.live_pct)}">${pct(r.live_pct)}</td>
      </tr>`
      )
      .join("");

    document.querySelectorAll(".scan-table thead th[data-k]").forEach((th) => {
      th.classList.toggle("sorted", th.dataset.k === sortKey);
      th.dataset.dir = th.dataset.k === sortKey ? (sortDir === 1 ? "asc" : "desc") : "";
    });
  }

  function render() {
    if (!latest) return;
    renderStatus(latest);
    renderRows(latest);
  }

  // ---- clock (ticks locally between snapshots) ------------------------
  function tickClock() {
    let d;
    if (clockBase) {
      d = new Date(clockBase.serverMs + (Date.now() - clockBase.localMs));
    } else {
      d = new Date();
    }
    $("clock").textContent = d.toLocaleTimeString("en-GB", { timeZone: "Asia/Kolkata" });
  }
  setInterval(tickClock, 1000);
  tickClock();

  // ---- sorting -------------------------------------------------------
  document.querySelectorAll(".scan-table thead th[data-k]").forEach((th) => {
    th.addEventListener("click", () => {
      const k = th.dataset.k;
      if (k === sortKey) sortDir = -sortDir;
      else {
        sortKey = k;
        sortDir = k === "symbol" ? 1 : -1;
      }
      render();
    });
  });

  // ---- SSE ----------------------------------------------------------
  let es;
  function connect() {
    es = new EventSource("/api/scanner/stream");
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
      // EventSource auto-reconnects; reflect the gap in the pill
      const pill = $("sockpill");
      pill.textContent = "stream: reconnecting…";
      pill.className = "chip chip-warn";
    };
  }
  connect();

  // initial one-shot so the page isn't blank for a second
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
