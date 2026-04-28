/* Project Search — AVS Intake Gate */
/* globals: PAGE_TYPE_OPTIONS, PAGE_TOTAL */

(function () {
  "use strict";

  // injected by the template
  const TYPE_OPTIONS = PAGE_TYPE_OPTIONS;
  const FIELD_KEYS = ["type", "wallSystem", "roof", "slab", "foundation"];

  function $(id) { return document.getElementById(id); }

  // ── Dropdown helpers ────────────────────────────────────────────────────────

  function fillSelect(el, options, { placeholder = "(Any)", disabled = false } = {}) {
    const prev = el.value;
    el.innerHTML = "";
    const any = document.createElement("option");
    any.value = "";
    any.textContent = placeholder;
    el.appendChild(any);
    for (const opt of options) {
      const o = document.createElement("option");
      o.value = opt;
      o.textContent = opt;
      el.appendChild(o);
    }
    el.disabled = disabled;
    if (prev && Array.from(el.options).some(o => o.value === prev)) {
      el.value = prev;
    }
  }

  function uniq(arr) { return Array.from(new Set(arr)); }

  function refreshDependentDropdowns() {
    const type = $("pf-type").value;
    const opts = type && TYPE_OPTIONS[type] ? TYPE_OPTIONS[type] : null;

    fillSelect($("pf-wall"), opts
      ? opts.wallSystem
      : uniq([...TYPE_OPTIONS.BTS.wallSystem, ...TYPE_OPTIONS.TI.wallSystem]));

    fillSelect($("pf-roof"), opts
      ? opts.roof
      : uniq([...TYPE_OPTIONS.BTS.roof, ...TYPE_OPTIONS.TI.roof]));

    fillSelect($("pf-slab"), opts
      ? opts.slab
      : uniq([...TYPE_OPTIONS.BTS.slab, ...TYPE_OPTIONS.TI.slab]));

    const foundationOpts = opts ? opts.foundation : uniq([...TYPE_OPTIONS.BTS.foundation, ...TYPE_OPTIONS.TI.foundation]);
    fillSelect($("pf-foundation"), foundationOpts, {
      disabled: foundationOpts.length === 0,
      placeholder: foundationOpts.length === 0 ? "(N/A for TI)" : "(Any)",
    });
  }

  // ── Results rendering ───────────────────────────────────────────────────────

  function renderPills(filters) {
    const el = $("pf-active-filters");
    el.innerHTML = "";
    const LABELS = { type: "Type", wallSystem: "Wall", roof: "Roof", slab: "Slab", foundation: "Foundation", company: "Company" };
    const active = Object.entries(filters).filter(([, v]) => v);
    if (!active.length) {
      const p = document.createElement("span");
      p.className = "pill";
      p.textContent = "No filters";
      el.appendChild(p);
      return;
    }
    for (const [k, v] of active) {
      const p = document.createElement("span");
      p.className = "pill";
      p.textContent = `${LABELS[k] ?? k}: ${v}`;
      el.appendChild(p);
    }
  }

  function buildTitle(row, colMap) {
    const name = colMap.name ? String(row[colMap.name] || "").trim() : "";
    const id   = colMap.id   ? String(row[colMap.id]   || "").trim() : "";
    if (id && name) return `${id} — ${name}`;
    return id || name || "Project";
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

    const usedKeys = new Set(Object.values(colMap).filter(Boolean));

    for (const row of rows) {
      const card = document.createElement("div");
      card.className = "pf-card card";

      const title = document.createElement("div");
      title.className = "pf-card-title";
      title.textContent = buildTitle(row, colMap);
      card.appendChild(title);

      const kv = document.createElement("div");
      kv.className = "pf-kv";

      for (const [label, key] of displayFields) {
        if (!key) continue;
        const val = String(row[key] || "").trim();
        if (!val) continue;
        kv.appendChild(makeKVPair(label, val));
      }

      // Extra fields (up to 4)
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

  function setStatus(msg, kind) {
    const el = $("pf-status");
    el.textContent = msg;
    el.className = "pf-status" + (kind === "error" ? " pf-status-error" : "");
  }

  function setCount(msg) {
    $("pf-count").textContent = msg;
  }

  // ── Search ──────────────────────────────────────────────────────────────────

  async function doSearch() {
    const filters = {
      type:        $("pf-type").value,
      wallSystem:  $("pf-wall").disabled ? "" : $("pf-wall").value,
      roof:        $("pf-roof").value,
      slab:        $("pf-slab").value,
      foundation:  $("pf-foundation").disabled ? "" : $("pf-foundation").value,
      company:     $("pf-company").value.trim(),
    };

    renderPills(filters);
    setStatus("Searching…", "");
    setCount("Searching…");
    $("pf-results").innerHTML = "";
    $("pf-find").disabled = true;

    const params = new URLSearchParams();
    for (const [k, v] of Object.entries(filters)) {
      if (v) params.set(k, v);
    }

    try {
      const res = await fetch(`/api/past-projects?${params}`);
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
      $("pf-find").disabled = false;
    }
  }

  function doReset() {
    $("pf-type").value = "";
    $("pf-company").value = "";
    refreshDependentDropdowns();
    renderPills({});
    setStatus("", "");
    setCount(`${PAGE_TOTAL} projects loaded. Choose filters and click Find.`);
    $("pf-results").innerHTML = "";
  }

  // ── Init ────────────────────────────────────────────────────────────────────

  function init() {
    refreshDependentDropdowns();
    $("pf-type").addEventListener("change", refreshDependentDropdowns);
    $("pf-find").addEventListener("click", doSearch);
    $("pf-reset").addEventListener("click", doReset);

    // Allow Enter key in any select or text input to trigger search
    document.querySelectorAll(".pf-select, .pf-text").forEach(el => {
      el.addEventListener("keydown", e => { if (e.key === "Enter") doSearch(); });
    });

    renderPills({});
  }

  document.addEventListener("DOMContentLoaded", init);
})();
