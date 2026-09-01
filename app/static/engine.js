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
  const money = (v) =>
    v === null || v === undefined
      ? "—"
      : (v < 0 ? "−" : v > 0 ? "+" : "") + "₹" + inr(Math.abs(v), 0);
  const dir = (v) => (v === null || v === undefined || v === 0 ? "" : v > 0 ? "up" : "down");

  function render(d) {
    strip.hidden = false;

    const armed = !!d.armed;
    $("eng-dot").className = "pulse" + (armed ? "" : " off");
    $("eng-state").textContent = armed ? "Engine armed" : "Engine killed";

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
    $("eng-slots").textContent = `${c.open ?? "—"}/${c.max_positions ?? "—"}`;
    $("eng-free").textContent = d.capital ? "₹" + inr(d.capital.free) : "—";

    const pend = d.pending || [];
    const pe = $("eng-pending");
    pe.hidden = pend.length === 0;
    pe.textContent = pend.length ? `queued: ${pend.join(", ")}` : "";

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

  poll();
  setInterval(poll, POLL_MS);
})();
