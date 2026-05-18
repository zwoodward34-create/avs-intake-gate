/* Project Search — AVS Intake Gate */

(function () {
  "use strict";

  function $(id) { return document.getElementById(id); }

  // ── Results rendering ───────────────────────────────────────────────────────

  function buildTitle(row, colMap) {
    const name = colMap.name ? String(row[colMap.name] || "").trim() : "";
    const id   = colMap.id   ? String(row[colMap.id]   || "").trim() : "";
    if (id && name) return `${id} — ${name}`;
    return id || name || "Project";
  }

  function makeKVPair(label, value) {
    const wrap = document.createElement("div");
    wrap.className = "pf-kv-row";
    const k = document.createElement("span");
    k.className = "pf-k";
    k.textContent = label;
    const v = document.createElement("span");
    v.className = "pf-v";
    v.textContent = value;
    wrap.appendChild(k);
    wrap.appendChild(v);
    return wrap;
  }

  function renderResults(rows, colMap) {
    const el = $("pf-results");
    el.innerHTML = "";

    if (!rows.length) {
      el.innerHTML = `<div class="pf-empty">No matching projects found.</div>`;
      return;
    }

    const displayFields = [
      ["Company",    colMap.company],
      ["Type",       colMap.type],
      ["Wall",       colMap.wallSystem],
      ["Roof",       colMap.roof],
      ["Slab",       colMap.slab],
      ["Foundation", colMap.foundation],
    ];

    // Exclude internal annotation keys from display
    const usedKeys = new Set([...Object.values(colMap).filter(Boolean), "_intake_id"]);

    for (const row of rows) {
      const intakeId = row["_intake_id"];

      const card = document.createElement("div");
      card.className = "pf-card card";

      // If this project has a matching intake record, make the whole card clickable
      if (intakeId) {
        card.style.cursor = "pointer";
        card.style.transition = "box-shadow 0.12s, border-color 0.12s";
        card.addEventListener("mouseenter", () => {
          card.style.boxShadow = "0 0 0 2px var(--accent)";
          card.style.borderColor = "var(--accent)";
        });
        card.addEventListener("mouseleave", () => {
          card.style.boxShadow = "";
          card.style.borderColor = "";
        });
        card.addEventListener("click", () => {
          window.location.href = "/intakes/" + intakeId;
        });
      }

      // Header: title + optional "View Intake →" badge
      const titleRow = document.createElement("div");
      titleRow.style.cssText = "display:flex;align-items:center;justify-content:space-between;gap:8px";

      const title = document.createElement("div");
      title.className = "pf-card-title";
      title.style.margin = "0";
      title.textContent = buildTitle(row, colMap);
      titleRow.appendChild(title);

      if (intakeId) {
        const badge = document.createElement("span");
        badge.textContent = "View Intake →";
        badge.style.cssText = [
          "font-size:10px", "font-weight:700", "letter-spacing:.4px",
          "padding:3px 9px", "border-radius:20px",
          "background:var(--accent)", "color:#fff",
          "white-space:nowrap", "flex-shrink:0",
        ].join(";");
        titleRow.appendChild(badge);
      }

      card.appendChild(titleRow);

      const kv = document.createElement("div");
      kv.className = "pf-kv";

      for (const [label, key] of displayFields) {
        if (!key) continue;
        const val = String(row[key] || "").trim();
        if (!val) continue;
        kv.appendChild(makeKVPair(label, val));
      }

      const extras = Object.entries(row)
        .filter(([k, v]) => !usedKeys.has(k) && String(v || "").trim())
        .slice(0, 4);

      if (extras.length) {
        const divider = document.createElement("div");
        divider.className = "pf-kv-divider";
        kv.appendChild(divider);
        for (const [k, v] of extras) {
          kv.appendChild(makeKVPair(k, String(v).trim()));
        }
      }

      card.appendChild(kv);
      el.appendChild(card);
    }
  }

  function setStatus(msg, kind) {
    const el = $("pf-status");
    if (!el) return;
    el.textContent = msg;
    el.className = "pf-status" + (kind === "error" ? " pf-status-error" : "");
  }

  function setCount(msg) {
    const el = $("pf-count");
    if (el) el.textContent = msg;
  }

  // ── Natural-language search ─────────────────────────────────────────────────

  async function doNLSearch() {
    const query = ($("pf-query").value || "").trim();
    if (!query) return;

    setStatus("Searching…", "");
    setCount("Searching…");
    $("pf-results").innerHTML = "";
    $("pf-search-btn").disabled = true;

    try {
      const res = await fetch("/api/nl-search-projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query }),
      });
      const data = await res.json();

      if (!res.ok || !data.ok) {
        throw new Error(data.detail || data.error || `Server error (${res.status})`);
      }

      const truncNote = data.truncated ? " (showing first matches only)" : "";
      setCount(`${data.returned} match${data.returned === 1 ? "" : "es"} out of ${data.total}${truncNote}`);
      setStatus("", "");
      renderResults(data.rows, data.col_map);
    } catch (err) {
      setStatus(err.message ?? String(err), "error");
      setCount("");
    } finally {
      $("pf-search-btn").disabled = false;
    }
  }

  // ── Init ────────────────────────────────────────────────────────────────────

  function init() {
    const btn = $("pf-search-btn");
    const input = $("pf-query");
    if (btn) btn.addEventListener("click", doNLSearch);
    if (input) input.addEventListener("keydown", function(e) {
      if (e.key === "Enter") doNLSearch();
    });
  }

  document.addEventListener("DOMContentLoaded", init);
})();
