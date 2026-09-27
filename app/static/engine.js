(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const strip = $("eng-strip");
  if (!strip) return;                       // only present on the scanner page

  const POLL_MS = 2000;

  // confirm before arming live orders (target is set by render())
  const modeForm = $("eng-mode-form");
  if (modeForm) {
    modeForm.addEventListener("submit", (e) => {
      if (
        $("eng-mode-target").value === "live" &&
        !confirm("Switch the engine to LIVE? Real Fyers orders will be placed on the next entry.")
      ) {
        e.preventDefault();
      }
    });
  }

  const inr = (v, d = 0) =>
    v === null || v === undefined || Number.isNaN(v)
      ? "—"
      : Number(v).toLocaleString("en-IN", { minimumFractionDigits: d, maximumFractionDigits: d });
  const inr2 = (v) => inr(v, 2);
  const money = (v) =>
    v === null || v === undefined
      ? "—"
      : (v < 0 ? "−" : v > 0 ? "+" : "") + "₹" + inr(Math.abs(v), 0);
  const signed = (v) =>
    v === null || v === undefined
      ? "—"
      : (v < 0 ? "−" : v > 0 ? "+" : "") + "₹" + inr(Math.abs(v), 2);
  const dir = (v) => (v === null || v === undefined || v === 0 ? "" : v > 0 ? "up" : "down");
  const disp = (s) => String(s || "").replace(/^[A-Z]+:/, "").replace(/-(EQ|INDEX)$/, "");
  const REASON = { target: "target ✓", stop: "stop ✕", eod: "squared off", kill: "closed", manual: "closed manually" };

  // manual close — on-screen confirm modal, then let the form post normally
  const posRows = $("pos-rows");
  const closeModal = $("close-modal");
  const closeModalText = $("close-modal-text");
  const closeModalCancel = $("close-modal-cancel");
  const closeModalConfirm = $("close-modal-confirm");
  let pendingCloseForm = null;

  function hideCloseModal() {
    closeModal.hidden = true;
    pendingCloseForm = null;
  }

  if (posRows && closeModal) {
    posRows.addEventListener("submit", (e) => {
      if (!e.target.classList.contains("pos-close-form")) return;
      e.preventDefault();
      pendingCloseForm = e.target;
      closeModalText.textContent =
        `Close ${e.target.dataset.sym || "this position"} now, at market?`;
      closeModal.hidden = false;
    });
    closeModalCancel.addEventListener("click", hideCloseModal);
    closeModal.addEventListener("click", (e) => {
      if (e.target === closeModal) hideCloseModal();   // backdrop click
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape" && !closeModal.hidden) hideCloseModal();
    });
    closeModalConfirm.addEventListener("click", () => {
      const form = pendingCloseForm;
      hideCloseModal();
      if (form) form.submit();   // .submit() bypasses the listener above, no loop
    });
  }

  function render(d) {
    strip.hidden = false;

    const armed = !!d.armed;
    $("eng-dot").className = "pulse" + (armed ? "" : " off");
    $("eng-state").textContent = armed ? "Engine armed" : "Engine killed";

    const tgl = $("eng-toggle");
    tgl.textContent = armed ? "Kill engine" : "Start engine";
    tgl.className = "btn btn-sm " + (armed ? "btn-ghost" : "btn-primary");

    const live = d.mode === "live";
    const modeBtn = $("eng-mode");
    modeBtn.textContent = String(d.mode || "").toUpperCase();
    modeBtn.className = "badge eng-mode-toggle" + (live ? " badge-live" : "");
    modeBtn.title = live ? "Switch to paper" : "Switch to live orders";
    $("eng-mode-target").value = live ? "paper" : "live";

    $("eng-style").textContent = d.style === "eod" ? "hold to close" : "bracket";

    const p = d.pnl || {};
    const realised = $("eng-realised");
    realised.textContent = money(p.realised);
    realised.className = "stat-val " + dir(p.realised);
    const unreal = $("eng-unrealised");
    unreal.textContent = money(p.unrealised);
    unreal.className = "stat-val " + dir(p.unrealised);

    const c = d.counts || {};
    $("eng-slots").textContent = `Open ${c.open ?? "—"} / ${c.max_positions ?? "—"}`;
    const cap = d.capital || {};
    $("eng-free").textContent = cap.free != null ? "₹" + inr(cap.free) : "—";
    const bal = $("pos-bal");
    if (bal) bal.textContent = cap.available_balance != null ? "₹" + inr(cap.available_balance) : "—";

    const pend = d.pending || [];
    const pe = $("eng-pending");
    pe.hidden = pend.length === 0;
    pe.textContent = pend.length ? `queued: ${pend.join(", ")}` : "";

    // pending orders — straight from the broker's live order book (live mode only;
    // paper has no real broker, so this stays empty there)
    const pending = d.pending_orders || [];
    const obEmpty = $("ob-empty");
    if (obEmpty) obEmpty.hidden = pending.length > 0;
    const obBody = $("ob-rows");
    if (obBody) {
      obBody.innerHTML = pending
        .map((o) => {
          const buy = o.side === "BUY";
          const side = `<span class="chip ${buy ? "chip-ok" : "chip-bad"}">${o.side || "?"}</span>`;
          return `<tr>
            <td class="dim">${o.at || "—"}</td>
            <td>${side}</td>
            <td class="col-sym" title="${o.symbol || ""}">${disp(o.symbol)}</td>
            <td>${o.qty ?? "—"}</td>
            <td>${o.price != null ? inr2(o.price) : "—"}</td>
            <td class="note">${o.status || ""}</td>
          </tr>`;
        })
        .join("");
    }

    // positions table
    const rows = d.positions || [];
    const emptyEl = $("pos-empty");
    if (emptyEl) emptyEl.hidden = rows.length > 0;
    const body = $("pos-rows");
    if (body) {
      body.innerHTML = rows
        .map((p) => {
          const open = p.status === "open";
          const px = open ? p.ltp : p.exit_price;
          const status = open
            ? '<span class="chip chip-ok">open</span>'
            : `<span class="chip chip-dim">${REASON[p.exit_reason] || "closed"}</span>`;
          const action = open
            ? `<form method="post" action="/engine/close" class="pos-close-form" data-sym="${disp(p.symbol)}">
                 <input type="hidden" name="pos_id" value="${p.id}">
                 <button type="submit" class="pos-close-btn" title="Close now, at market">✕</button>
               </form>`
            : "";

          // live-mode fills we never got a broker confirmation for — the price
          // shown is our best guess, not a verified fill
          const entryUnconfirmed = p.mode === "live" && p.fill_confirmed === false;
          const exitUnconfirmed = !open && p.mode === "live" && p.exit_confirmed === false;
          let warnTitle = "";
          if (entryUnconfirmed && exitUnconfirmed) warnTitle = "Entry and exit fills not confirmed by the broker";
          else if (entryUnconfirmed) warnTitle = "Entry fill not confirmed by the broker";
          else if (exitUnconfirmed) warnTitle = "Exit fill not confirmed by the broker";
          const warnFlag = warnTitle ? ` <span class="unconf-flag" title="${warnTitle}">⚠</span>` : "";

          return `<tr class="${open ? "" : "closed"}">
            <td class="col-sym" title="${p.symbol}">${disp(p.symbol)}${warnFlag}</td>
            <td class="num">${p.qty}</td>
            <td class="num">${inr2(p.entry_price)}</td>
            <td class="num">${inr2(px)}</td>
            <td class="num dim">${inr2(p.target_price)}</td>
            <td class="num dim">${inr2(p.stop_price)}</td>
            <td class="num ${dir(p.pnl)}">${signed(p.pnl)}</td>
            <td>${status}</td>
            <td class="col-act">${action}</td>
          </tr>`;
        })
        .join("");
    }

    let note = "";
    if (!d.in_session) note = "Market closed — engine idle.";
    else if (!d.scanner_frozen) note = "Waiting for the 10:00 freeze.";
    else if (d.past_square_off) note = "Past square-off — flattening, no new entries.";
    else if (armed) note = "Buys any symbol above on a fresh cross of its 10:00 high.";
    else note = "Not taking entries. Open positions are still managed.";
    $("eng-note").textContent = note;
  }

  async function poll() {
    try {
      const r = await fetch("/api/engine/state", { cache: "no-store" });
      render(await r.json());
    } catch { /* keep last render */ }
  }

  // broker reconciliation strip — real Fyers /positions P&L, independent of
  // however the engine (paper or live) thinks its own book stands. Polled
  // separately and slower since it's a sanity check, not a trading signal.
  const BROKER_POLL_MS = 4000;

  function renderBroker(d) {
    const strip = $("broker-check");
    if (!strip) return;
    if (!d || !d.connected) {
      strip.hidden = true;
      return;
    }
    strip.hidden = false;
    const val = $("broker-pnl");
    const note = $("broker-check-note");
    if (d.error) {
      val.textContent = "—";
      val.className = "stat-val";
      note.textContent = d.error;
      return;
    }
    val.textContent = d.pnl != null ? money(d.pnl) : "—";
    val.className = "stat-val " + dir(d.pnl);
    note.textContent = "live Fyers account · all open positions, not only this engine's";
  }

  async function pollBroker() {
    try {
      const r = await fetch("/api/positions/state", { cache: "no-store" });
      renderBroker(await r.json());
    } catch { /* keep last render */ }
  }

  poll();
  setInterval(poll, POLL_MS);
  pollBroker();
  setInterval(pollBroker, BROKER_POLL_MS);
})();
