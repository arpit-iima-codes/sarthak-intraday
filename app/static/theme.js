(() => {
  "use strict";

  const btn = document.getElementById("themebtn");
  if (!btn) return;
  const root = document.documentElement;

  const current = () => root.getAttribute("data-theme") || "dark";

  btn.addEventListener("click", () => {
    const next = current() === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    try {
      localStorage.setItem("theme", next);
    } catch (e) {
      /* storage blocked — theme still applies for this page load */
    }
  });
})();
