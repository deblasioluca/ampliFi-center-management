/**
 * UI prompt helpers — replaces window.alert / confirm / prompt.
 *
 * Why? Native browser dialogs:
 *   - Look unbranded and platform-specific (the user reported "Auf
 *     192.168.20.122:8180 wird Folgendes angezeigt:" headers in their
 *     screenshots — that is the browser, not the app).
 *   - Block the entire JS thread and can't be styled.
 *   - Don't support multi-field forms, validation messages, async
 *     workflows, or nested dialogs.
 *
 * This module exposes three Promise-based replacements plus a passive
 * toast for non-blocking notifications:
 *
 *   await uiAlert({ message: 'Saved!' })
 *   const ok = await uiConfirm({ message: 'Delete this template?' })
 *   const name = await uiPrompt({ label: 'Wave code:', defaultValue: 'WAVE-1' })
 *   uiToast('Saved!', { kind: 'success' })   // non-blocking, auto-dismisses
 *
 * uiAlert/uiConfirm/uiPrompt all reject (Promise rejects) when the user
 * cancels — callers can use `.catch(() => null)` if they want graceful
 * "user cancelled" handling. Keyboard: Esc cancels, Enter confirms.
 *
 * The module also exposes window.uiAlert etc. for inline-script callers.
 */
(function () {
  'use strict';

  function escapeHtml(s) {
    if (s == null) return '';
    return String(s)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  // ── Modal infrastructure ─────────────────────────────────────────────

  function openModal(opts) {
    return new Promise(function (resolve, reject) {
      const overlay = document.createElement('div');
      overlay.className =
        'fixed inset-0 bg-black/50 z-[100] flex items-center justify-center p-4';
      overlay.setAttribute('role', 'dialog');
      overlay.setAttribute('aria-modal', 'true');

      const dialog = document.createElement('div');
      dialog.className = 'bg-white rounded-lg shadow-2xl w-full max-w-md';
      dialog.innerHTML = opts.bodyHtml;

      overlay.appendChild(dialog);
      document.body.appendChild(overlay);

      const cleanup = function () {
        if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
        document.removeEventListener('keydown', onKey);
      };
      const settle = function (resolved, value) {
        cleanup();
        if (resolved) resolve(value);
        else reject(value);
      };

      function onKey(e) {
        if (e.key === 'Escape') {
          e.preventDefault();
          settle(false, new Error('cancelled'));
        } else if (e.key === 'Enter' && opts.allowEnter !== false) {
          // Avoid triggering Enter inside textarea.
          if (e.target.tagName === 'TEXTAREA') return;
          e.preventDefault();
          opts.onEnter && opts.onEnter(settle);
        }
      }
      document.addEventListener('keydown', onKey);

      // Click outside to cancel (only for non-destructive dialogs).
      if (opts.cancelOnBackdrop !== false) {
        overlay.addEventListener('click', function (e) {
          if (e.target === overlay) settle(false, new Error('cancelled'));
        });
      }

      // Hand control back to caller for wiring buttons + auto-focus.
      opts.bind(dialog, settle);
    });
  }

  // ── uiAlert ──────────────────────────────────────────────────────────

  function uiAlert(opts) {
    if (typeof opts === 'string') opts = { message: opts };
    opts = opts || {};
    const title = opts.title || 'Notice';
    const okLabel = opts.okLabel || 'OK';
    const kind = opts.kind || 'info'; // info | warning | danger | success
    const tones = {
      info: 'border-amplifi-200 bg-amplifi-50 text-amplifi-900',
      warning: 'border-amber-200 bg-amber-50 text-amber-900',
      danger: 'border-red-200 bg-red-50 text-red-900',
      success: 'border-green-200 bg-green-50 text-green-900',
    };
    const tone = tones[kind] || tones.info;

    return openModal({
      bodyHtml: `
        <div class="px-5 py-3 border-b">
          <h3 class="font-semibold text-base">${escapeHtml(title)}</h3>
        </div>
        <div class="px-5 py-4">
          <div class="text-sm border-l-4 ${tone} px-3 py-2">${escapeHtml(opts.message || '')}</div>
        </div>
        <div class="px-5 py-3 border-t flex justify-end gap-2">
          <button class="ui-ok btn-amplifi text-sm">${escapeHtml(okLabel)}</button>
        </div>
      `,
      onEnter: function (settle) { settle(true, undefined); },
      bind: function (dialog, settle) {
        const okBtn = dialog.querySelector('.ui-ok');
        okBtn.addEventListener('click', function () { settle(true, undefined); });
        setTimeout(function () { okBtn.focus(); }, 50);
      },
    });
  }

  // ── uiConfirm ────────────────────────────────────────────────────────

  function uiConfirm(opts) {
    if (typeof opts === 'string') opts = { message: opts };
    opts = opts || {};
    const title = opts.title || 'Please confirm';
    const okLabel = opts.okLabel || 'OK';
    const cancelLabel = opts.cancelLabel || 'Cancel';
    const danger = !!opts.danger;
    const okClass = danger
      ? 'bg-red-600 hover:bg-red-700 text-white'
      : 'btn-amplifi';

    return openModal({
      bodyHtml: `
        <div class="px-5 py-3 border-b">
          <h3 class="font-semibold text-base">${escapeHtml(title)}</h3>
        </div>
        <div class="px-5 py-4 text-sm">${escapeHtml(opts.message || '')}</div>
        <div class="px-5 py-3 border-t flex justify-end gap-2">
          <button class="ui-cancel btn-secondary text-sm">${escapeHtml(cancelLabel)}</button>
          <button class="ui-ok ${okClass} text-sm px-3 py-1.5 rounded">${escapeHtml(okLabel)}</button>
        </div>
      `,
      onEnter: function (settle) { settle(true, true); },
      bind: function (dialog, settle) {
        dialog.querySelector('.ui-cancel').addEventListener('click', function () { settle(false, false); });
        const okBtn = dialog.querySelector('.ui-ok');
        okBtn.addEventListener('click', function () { settle(true, true); });
        setTimeout(function () { okBtn.focus(); }, 50);
      },
    }).then(function (v) { return v === true; }, function () { return false; });
  }

  // ── uiPrompt ─────────────────────────────────────────────────────────

  function uiPrompt(opts) {
    if (typeof opts === 'string') opts = { label: opts };
    opts = opts || {};
    const title = opts.title || 'Input required';
    const label = opts.label || 'Value:';
    const defaultValue = opts.defaultValue != null ? String(opts.defaultValue) : '';
    const placeholder = opts.placeholder || '';
    const okLabel = opts.okLabel || 'OK';
    const cancelLabel = opts.cancelLabel || 'Cancel';
    const required = opts.required !== false;
    const inputType = opts.type || 'text';
    const helpText = opts.helpText || '';

    return openModal({
      bodyHtml: `
        <div class="px-5 py-3 border-b">
          <h3 class="font-semibold text-base">${escapeHtml(title)}</h3>
        </div>
        <div class="px-5 py-4 space-y-2">
          <label class="block text-xs text-gray-500 mb-1">${escapeHtml(label)}</label>
          <input class="ui-input input text-sm w-full" type="${escapeHtml(inputType)}"
                 value="${escapeHtml(defaultValue)}" placeholder="${escapeHtml(placeholder)}" />
          ${helpText ? `<p class="text-[11px] text-gray-400">${escapeHtml(helpText)}</p>` : ''}
          <p class="ui-error text-xs text-red-600 hidden"></p>
        </div>
        <div class="px-5 py-3 border-t flex justify-end gap-2">
          <button class="ui-cancel btn-secondary text-sm">${escapeHtml(cancelLabel)}</button>
          <button class="ui-ok btn-amplifi text-sm">${escapeHtml(okLabel)}</button>
        </div>
      `,
      onEnter: function (settle) {
        const input = document.querySelector('.ui-input');
        if (!input) return;
        const v = input.value.trim();
        if (required && !v) {
          showInputError('Required.');
          return;
        }
        settle(true, v);
      },
      bind: function (dialog, settle) {
        const input = dialog.querySelector('.ui-input');
        const errEl = dialog.querySelector('.ui-error');
        function showErr(msg) {
          errEl.textContent = msg;
          errEl.classList.remove('hidden');
        }
        // Expose for onEnter via a hidden helper.
        window.__uiPromptShowErr = showErr;

        dialog.querySelector('.ui-cancel').addEventListener('click', function () {
          settle(false, new Error('cancelled'));
        });
        dialog.querySelector('.ui-ok').addEventListener('click', function () {
          const v = input.value.trim();
          if (required && !v) {
            showErr('Required.');
            return;
          }
          settle(true, v);
        });
        setTimeout(function () {
          input.focus();
          input.select();
        }, 50);
      },
    });

    function showInputError(msg) {
      if (typeof window.__uiPromptShowErr === 'function') {
        window.__uiPromptShowErr(msg);
      }
    }
  }

  // ── uiToast ──────────────────────────────────────────────────────────
  // Non-blocking notifications. Auto-stacks bottom-right.

  function ensureToastContainer() {
    let c = document.getElementById('ui-toast-container');
    if (!c) {
      c = document.createElement('div');
      c.id = 'ui-toast-container';
      c.className = 'fixed bottom-4 right-4 z-[200] flex flex-col gap-2 items-end pointer-events-none';
      document.body.appendChild(c);
    }
    return c;
  }

  function uiToast(message, opts) {
    opts = opts || {};
    const kind = opts.kind || 'info'; // info | success | warning | danger
    const duration = opts.duration != null ? opts.duration : 4000;
    const styles = {
      info: 'bg-white border-gray-300 text-gray-800',
      success: 'bg-green-50 border-green-300 text-green-900',
      warning: 'bg-amber-50 border-amber-300 text-amber-900',
      danger: 'bg-red-50 border-red-300 text-red-900',
    };
    const tone = styles[kind] || styles.info;

    const c = ensureToastContainer();
    const el = document.createElement('div');
    el.className =
      'pointer-events-auto border rounded shadow-lg px-4 py-2 text-sm max-w-sm flex items-start gap-2 ' + tone;
    el.innerHTML = `
      <span class="flex-1">${escapeHtml(message)}</span>
      <button class="ui-toast-close text-gray-400 hover:text-gray-700 leading-none" aria-label="Dismiss">×</button>
    `;
    c.appendChild(el);

    let dismissed = false;
    const dismiss = function () {
      if (dismissed) return;
      dismissed = true;
      el.style.transition = 'opacity 200ms ease, transform 200ms ease';
      el.style.opacity = '0';
      el.style.transform = 'translateY(-4px)';
      setTimeout(function () {
        if (el.parentNode) el.parentNode.removeChild(el);
      }, 220);
    };
    el.querySelector('.ui-toast-close').addEventListener('click', dismiss);
    if (duration > 0) setTimeout(dismiss, duration);

    return { dismiss: dismiss };
  }

  // ── Expose ──────────────────────────────────────────────────────────
  window.uiAlert = uiAlert;
  window.uiConfirm = uiConfirm;
  window.uiPrompt = uiPrompt;
  window.uiToast = uiToast;
})();
