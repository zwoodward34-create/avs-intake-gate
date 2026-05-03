// Detailed screening collapsible
(function () {
  function initCollapsible() {
    var toggle = document.getElementById('detailed-screening-toggle');
    var body = document.getElementById('detailed-screening-body');
    if (!toggle || !body) return;

    // Set initial natural height so CSS transition works
    body.style.maxHeight = body.scrollHeight + 'px';
    body.style.opacity = '1';

    toggle.addEventListener('click', function () {
      var expanded = toggle.getAttribute('aria-expanded') === 'true';
      if (expanded) {
        body.style.maxHeight = body.scrollHeight + 'px'; // pin before collapsing
        requestAnimationFrame(function () {
          body.classList.add('collapsed');
          toggle.setAttribute('aria-expanded', 'false');
        });
      } else {
        body.classList.remove('collapsed');
        body.style.maxHeight = body.scrollHeight + 'px';
        toggle.setAttribute('aria-expanded', 'true');
      }
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initCollapsible);
  } else {
    initCollapsible();
  }
})();

function copyToClipboard(text) {
  if (!text) return;
  navigator.clipboard.writeText(text).then(() => {
    const el = document.getElementById("copy-status");
    if (el) {
      el.textContent = "Copied.";
      setTimeout(() => (el.textContent = ""), 1200);
    }
  });
}

function buildMoEmail() {
  const box = document.getElementById("mo-email");
  if (!box) return;
  const text = (box.textContent || box.innerText || "").trim();
  if (!text) return;
  navigator.clipboard.writeText(text).then(() => {
    const btn = document.getElementById('copy-email-btn');
    const btnText = document.getElementById('copy-btn-text');
    const btnCheck = document.getElementById('copy-btn-check');
    if (btnText) btnText.style.display = 'none';
    if (btnCheck) btnCheck.style.display = 'inline';
    if (btn) btn.classList.add('copied');
    setTimeout(() => {
      if (btnText) btnText.style.display = 'inline';
      if (btnCheck) btnCheck.style.display = 'none';
      if (btn) btn.classList.remove('copied');
    }, 2000);
  });
}

// ── Burn Health View ──────────────────────────────────────

async function loadBurnHealth() {
  const loading    = document.getElementById("bh-loading");
  const list       = document.getElementById("bh-list");
  const empty      = document.getElementById("bh-empty");
  const summary    = document.getElementById("burn-health-summary");
  const refreshBtn = document.getElementById("refresh-btn");

  if (loading)    loading.style.display  = "block";
  if (list)       list.style.display     = "none";
  if (empty)      empty.style.display    = "none";
  if (summary)    summary.style.display  = "none";
  if (refreshBtn) refreshBtn.disabled    = true;

  try {
    const res = await fetch("/api/burn-health");
    if (!res.ok) throw new Error("HTTP " + res.status);
    const projects = await res.json();

    if (loading) loading.style.display = "none";

    if (!projects.length) {
      if (empty) empty.style.display = "block";
      return;
    }

    _renderBurnSummary(projects);
    if (summary) summary.style.display = "grid";

    if (list) {
      list.innerHTML = _buildBurnList(projects);
      list.style.display = "block";
    }
  } catch (err) {
    console.error("Burn health load failed:", err);
    if (loading) loading.textContent = "Failed to load — check the console.";
  } finally {
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

function _renderBurnSummary(projects) {
  const counts = { over_budget: 0, at_risk: 0, watch: 0, on_track: 0 };
  projects.forEach(p => { if (counts[p.risk] !== undefined) counts[p.risk]++; });
  _bhSetText("bh-total-count", projects.length);
  _bhSetText("bh-over-count",  counts.over_budget);
  _bhSetText("bh-risk-count",  counts.at_risk);
  _bhSetText("bh-watch-count", counts.watch);
  _bhSetText("bh-good-count",  counts.on_track);
}

function _buildBurnList(projects) {
  const fmt = n => new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD", maximumFractionDigits: 0,
  }).format(n);

  const riskLabel = { on_track: "On Track", watch: "Watch", at_risk: "At Risk", over_budget: "Over Budget" };
  const riskClass = { on_track: "on-track", watch: "watch", at_risk: "at-risk", over_budget: "over-budget" };
  const trackClass = { on_track: "", watch: "bh-watch", at_risk: "bh-at-risk", over_budget: "bh-over-budget" };

  let html = `
    <div class="bh-header-row">
      <div class="bh-header-cell">Project</div>
      <div class="bh-header-cell">Fee</div>
      <div class="bh-header-cell">Burn</div>
      <div class="bh-header-cell">Projected</div>
    </div>`;

  for (const p of projects) {
    const currentWidth   = Math.min(p.current_burn_pct,   100).toFixed(1);
    const projectedWidth = Math.min(p.projected_burn_pct, 100).toFixed(1);

    let daysHtml = "";
    if (p.days_remaining !== null && p.days_remaining !== undefined) {
      if (p.days_remaining < 0) {
        daysHtml = `<div class="bh-days-remaining bh-days-past">${Math.abs(p.days_remaining)}d past end date</div>`;
      } else if (p.days_remaining <= 14) {
        daysHtml = `<div class="bh-days-remaining bh-days-tight">${p.days_remaining}d remaining</div>`;
      } else {
        daysHtml = `<div class="bh-days-remaining">${p.days_remaining}d remaining</div>`;
      }
    }

    html += `
      <a class="bh-row" href="/intakes/${p.intake_id}">
        <div class="bh-project-info">
          <div class="bh-project-num">#${p.project_number}</div>
          <div class="bh-project-name">${_bhEsc(p.project_name)}</div>
          <div class="bh-project-client">${_bhEsc(p.client || "—")}</div>
        </div>
        <div class="bh-fee">
          <div class="bh-fee-label">Approved</div>
          ${fmt(p.approved_fee)}
        </div>
        <div class="bh-burn-area">
          <div class="bh-burn-labels">
            <span class="bh-burn-current-label">Current: ${fmt(p.current_burn_value)} (${p.current_burn_pct}%)</span>
            <span class="bh-burn-projected-label">Projected: ${p.projected_burn_pct}%</span>
          </div>
          <div class="bh-burn-track ${trackClass[p.risk]}">
            <div class="bh-fill-projected" style="width:${projectedWidth}%"></div>
            <div class="bh-fill-current"   style="width:${currentWidth}%"></div>
          </div>
          ${daysHtml}
        </div>
        <div class="bh-status-col">
          <div class="bh-projected-pct">${p.projected_burn_pct}%</div>
          <div class="bh-risk-badge ${riskClass[p.risk]}">${riskLabel[p.risk]}</div>
        </div>
      </a>`;
  }

  return html;
}

function _bhSetText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function _bhEsc(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}
