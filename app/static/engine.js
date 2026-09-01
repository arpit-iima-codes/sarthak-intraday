(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const POLL_MS = 2000;

  const inr = (v, d = 2) =>
    v === null || v === undefined || Number.isNaN(v)
      ? "—"
      : Number(v).toLocaleString("en-IN", { minimumFractionDigits: d, maximumFractionDigits: d });
  const money = (v, d = 0) =>
    v === null || v === undefined
      ? "—"
      : (v < 0 ? "−" : v > 0 ? "+" : "") + "₹" + inr(Math.abs(v), d);
  const rupee = (v) => (v === null || v === undefined ? "—" : "₹" + inr(v, 0));
  const dir = (v) => (v === null || v === undefined || v === 0 ? "" : v > 0 ? "up" : "down");
  const disp = (s) => String(s || "").replace(/^[A-Z]+:/, "").replace(/-(EQ|INDEX)$/, "");
  const clock = (iso) => {
    try { return new Date(iso).toLocaleTimeString("en-GB", { timeZone: "Asia/Kolkata" }); }
    catch { return "--:--:--"; }
  };

  const REASON = { target: "target ✓", stop: "stop ✕", eod: "closed (EOD)", kill: "closed" };

  function render(d) {
    $("eng-clock").textContent = clock(d.server_time);

    const armed = d.armed;
    $("eng-dot").className = "pulse" + (armed ? "" : " off");
    $("eng-state").textContent = armed
      ? "Armed — watching for breakouts"
      : "Killed — no new entries";
    $("eng-mode").textContent = (d.mode || "").toUpperCase();
    $("eng-mode").className = "badge" + (d.mode === "live" ? " badge-live" : "");
    $("eng-style").textContent = d.style === "eod" ? "hold to close" : "bracket";

    let note = "";
    if (!d.in_session) note = "Market closed — engine idle.";
    else if (!d.scanner_frozen) note = "Waiting for the scanner to freeze the 10:00 high.";
    else if (d.past_square_off) note = "Past square-off — no new entries; open positions being flattened.";
    $("eng-note").textContent = note ||
      "Buys any scanner symbol on a live cross above its 10:00 high.";

    const p = d.pnl || {};
    $("eng-pnl").textContent = money(p.total);
    $("eng-pnl").className = "stat-val " + dir(p.total);
    $("eng-realised").textContent = money(p.realised);
    $("eng-realised").className = "stat-val " + dir(p.realised);

    const c = d.counts || {};
    $("eng-slots").textContent = `${c.open ?? "—"} / ${c.max_positions ?? "—"}`;
    const cap = d.capital || {};
    $("eng-deployed").textContent = rupee(cap.deployed);
    $("eng-free").textContent = rupee(cap.free);

    const pend = d.pending || [];
    const pe = $("eng-pending");
    pe.hidden = pend.length === 0;
    pe.textContent = pend.length ? `queued: ${pend.join(", ")}` : "";

    const rows = d.positions || [];
    $("eng-empty").hidden = rows.length > 0;
    $("eng-rows").innerHTML = rows
      .map((r) => {
        const open = r.status === "open";
        const status = open
          ? `<span class="chip chip-ok">open</span>`
          : `<span class="chip chip-dim">${REASON[r.exit_reason] || "closed"}</span>`;
        const paper = r.mode === "paper" ? ` <span class="chip-dim">paper</span>` : "";
        return `
      <tr class="${open ? "" : "closed"}">
        <td class="col-sym" title="${r.symbol}">${disp(r.symbol)}${paper}</td>
        <td class="num">${r.qty}</td>
        <td class="num">${inr(r.entry_price)}</td>
        <td class="num">${inr(open ? r.ltp : r.exit_price)}</td>
        <td class="num dim">${inr(r.target_price)}</td>
        <td class="num dim">${inr(r.stop_price)}</td>
        <td class="num ${dir(r.pnl)}">${money(r.pnl, 2)}</td>
        <td>${status}</td>
      </tr>`;
      })
      .join("");

    const errs = d.errors || [];
    $("eng-errbox").hidden = errs.length === 0;
    $("eng-errs").innerHTML = errs
      .map((e) => `<li class="bad"><span class="mono vsym">${clock(e.at)}</span>
        <span class="vdim">${e.msg}</span></li>`)
      .join("");
  }

  async function poll() {
    try {
      const r = await fetch("/api/engine/state", { cache: "no-store" });
      render(await r.json());
    } catch { /* keep last render */ }
  }

  poll();
  setInterval(poll, POLL_MS);
})();
