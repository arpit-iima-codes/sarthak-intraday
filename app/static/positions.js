(() => {
  "use strict";

  const $ = (id) => document.getElementById(id);
  const rowsEl = $("pos-rows");
  const balEl = $("pos-bal");
  const emptyEl = $("pos-empty");
  const errEl = $("pos-err");

  const POLL_MS = 3000;

  const inr = (v, d = 2) =>
    v === null || v === undefined || Number.isNaN(v)
      ? "—"
      : Number(v).toLocaleString("en-IN", { minimumFractionDigits: d, maximumFractionDigits: d });
  const signed = (v) => (v === null || v === undefined ? "—" : (v > 0 ? "+" : "") + inr(v));
  const dir = (v) => (v === null || v === undefined || v === 0 ? "" : v > 0 ? "up" : "down");
  const disp = (s) => String(s || "").replace(/^[A-Z]+:/, "").replace(/-(EQ|INDEX)$/, "");

  function render(d) {
    const msg = !d.connected ? "Not connected to broker." : d.error || "";
    errEl.textContent = msg;
    errEl.hidden = !msg;

    balEl.textContent =
      d.available_balance === null || d.available_balance === undefined
        ? "—"
        : "₹" + inr(d.available_balance, 0);

    const pos = d.positions || [];
    emptyEl.hidden = pos.length > 0 || !!msg;

    rowsEl.innerHTML = pos
      .map(
        (p) => `
      <tr class="${p.qty ? "" : "closed"}">
        <td class="col-sym" title="${p.symbol}">${disp(p.symbol)}</td>
        <td class="num">${p.qty === null || p.qty === undefined ? "—" : p.qty}</td>
        <td class="num">${inr(p.entry)}</td>
        <td class="num dim">${inr(p.target)}</td>
        <td class="num dim">${inr(p.sl)}</td>
        <td class="num ${dir(p.pnl)}">${signed(p.pnl)}</td>
      </tr>`
      )
      .join("");
  }

  async function poll() {
    try {
      const r = await fetch("/api/positions/state", { cache: "no-store" });
      render(await r.json());
    } catch {
      /* transient — keep the last render */
    }
  }

  poll();
  setInterval(poll, POLL_MS);
})();
