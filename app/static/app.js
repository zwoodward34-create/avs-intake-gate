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
