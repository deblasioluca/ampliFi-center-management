/**
 * EmployeePicker — single-employee picker with server-side search.
 *
 * Replaces manual Reviewer Name + Email text inputs. When the user picks an
 * employee, the picker exposes both their display name and email address so
 * the calling page can fill its own form fields (or just read them on submit).
 *
 * Usage:
 *
 *   <div id="reviewer-picker"></div>
 *   <script src="/employee-picker.js" is:inline></script>
 *   <script is:inline>
 *     const picker = mountEmployeePicker('reviewer-picker', {
 *       placeholder: 'Search reviewer by name, GPN, email…',
 *       onChange: (emp) => {
 *         // emp is null when cleared, otherwise:
 *         // { gpn, display_name, email_address, ou_desc, ... }
 *         document.getElementById('hidden-name').value = emp ? emp.display_name : '';
 *         document.getElementById('hidden-email').value = emp ? emp.email_address : '';
 *       },
 *     });
 *     // Read selection: picker.getSelected()
 *     // Programmatic clear: picker.clear()
 *   </script>
 *
 * Search is debounced (250ms) and queries /api/employees?search=… server-side
 * — no full-table dump on the client.
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

  function mountEmployeePicker(target, options) {
    options = options || {};
    const root = typeof target === 'string' ? document.getElementById(target) : target;
    if (!root) throw new Error('EmployeePicker: target not found');

    const opts = {
      placeholder: options.placeholder || 'Search employee…',
      onChange: typeof options.onChange === 'function' ? options.onChange : function () {},
      pageSize: options.pageSize || 25,
      activeOnly: options.activeOnly !== false,
    };

    const token = localStorage.getItem('amplifi_token');

    const state = { selected: null, results: [], loading: false, error: null, open: false };
    let debounceTimer = null;

    root.classList.add('emp-root');
    root.innerHTML = `
      <div class="emp-control border rounded bg-white px-2 py-1 flex items-center gap-2 cursor-text">
        <div class="emp-display flex-1 text-sm"></div>
        <button type="button" class="emp-clear text-gray-400 hover:text-gray-700 text-sm hidden" aria-label="Clear">×</button>
        <span class="emp-caret text-gray-400 text-xs">▾</span>
      </div>
      <div class="emp-dropdown hidden border rounded mt-1 bg-white shadow-lg max-h-80 overflow-y-auto">
        <input type="text" class="emp-search w-full px-2 py-1.5 text-sm border-b outline-none"
               placeholder="${escapeHtml(opts.placeholder)}" />
        <div class="emp-list"></div>
      </div>
    `;
    const controlEl = root.querySelector('.emp-control');
    const displayEl = root.querySelector('.emp-display');
    const clearBtn = root.querySelector('.emp-clear');
    const dropdownEl = root.querySelector('.emp-dropdown');
    const searchEl = root.querySelector('.emp-search');
    const listEl = root.querySelector('.emp-list');

    function renderDisplay() {
      if (state.selected) {
        const e = state.selected;
        displayEl.innerHTML = `
          <span class="font-medium">${escapeHtml(e.display_name || e.bs_name || e.gpn)}</span>
          <span class="text-xs text-gray-500 ml-2">${escapeHtml(e.email_address || '')}</span>
        `;
        clearBtn.classList.remove('hidden');
      } else {
        displayEl.innerHTML = `<span class="text-xs text-gray-400">${escapeHtml(opts.placeholder)}</span>`;
        clearBtn.classList.add('hidden');
      }
    }

    function renderList() {
      if (state.loading) {
        listEl.innerHTML = `<div class="px-3 py-2 text-xs text-gray-400">Searching…</div>`;
        return;
      }
      if (state.error) {
        listEl.innerHTML = `<div class="px-3 py-2 text-xs text-red-600">${escapeHtml(state.error)}</div>`;
        return;
      }
      if (!state.results.length) {
        listEl.innerHTML = `<div class="px-3 py-2 text-xs text-gray-400">${state.lastQuery ? 'No matches' : 'Type to search'}</div>`;
        return;
      }
      listEl.innerHTML = state.results
        .map(
          (e) => `
        <div class="emp-item px-3 py-2 cursor-pointer hover:bg-amplifi-50 border-b last:border-b-0" data-gpn="${escapeHtml(e.gpn)}">
          <div class="flex items-baseline justify-between">
            <span class="text-sm font-medium">${escapeHtml(e.display_name || e.bs_name || e.gpn)}</span>
            <span class="text-[10px] text-gray-400 font-mono">${escapeHtml(e.gpn)}</span>
          </div>
          <div class="flex items-center justify-between text-xs text-gray-500 mt-0.5">
            <span class="truncate" title="${escapeHtml(e.email_address || '')}">${escapeHtml(e.email_address || '—')}</span>
            ${e.ou_desc ? `<span class="text-[11px] text-gray-400 ml-2 truncate">${escapeHtml(e.ou_desc)}</span>` : ''}
          </div>
        </div>
      `
        )
        .join('');

      listEl.querySelectorAll('.emp-item').forEach((el) => {
        el.addEventListener('click', () => {
          const gpn = el.getAttribute('data-gpn');
          const emp = state.results.find((x) => x.gpn === gpn);
          if (emp) {
            state.selected = emp;
            state.open = false;
            dropdownEl.classList.add('hidden');
            searchEl.value = '';
            state.results = [];
            renderDisplay();
            opts.onChange(emp);
          }
        });
      });
    }

    async function doSearch(q) {
      state.lastQuery = q;
      if (!q || q.length < 2) {
        state.results = [];
        renderList();
        return;
      }
      state.loading = true;
      renderList();
      try {
        const url = `/api/employees?search=${encodeURIComponent(q)}&size=${opts.pageSize}`;
        const r = await fetch(url, { headers: { Authorization: 'Bearer ' + token } });
        if (r.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const data = await r.json();
        let items = data.items || [];
        if (opts.activeOnly) {
          items = items.filter((e) => !e.emp_status || /active|aktiv/i.test(e.emp_status));
        }
        state.results = items;
        state.loading = false;
        state.error = null;
      } catch (err) {
        state.loading = false;
        state.error = err.message || 'Search failed';
      }
      renderList();
    }

    controlEl.addEventListener('click', (e) => {
      if (e.target === clearBtn) return;
      state.open = true;
      dropdownEl.classList.remove('hidden');
      setTimeout(() => searchEl.focus(), 0);
    });

    clearBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      state.selected = null;
      renderDisplay();
      opts.onChange(null);
    });

    searchEl.addEventListener('input', (e) => {
      const q = e.target.value;
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => doSearch(q), 250);
    });

    searchEl.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        state.open = false;
        dropdownEl.classList.add('hidden');
        searchEl.blur();
      }
    });

    document.addEventListener('click', (e) => {
      if (!root.contains(e.target)) {
        state.open = false;
        dropdownEl.classList.add('hidden');
      }
    });

    // ── Public API ───────────────────────────────────────────────────
    renderDisplay();

    return {
      getSelected: () => state.selected,
      clear: () => {
        state.selected = null;
        renderDisplay();
        opts.onChange(null);
      },
      setEnabled: (enabled) => {
        root.style.opacity = enabled ? '1' : '0.5';
        root.style.pointerEvents = enabled ? 'auto' : 'none';
      },
    };
  }

  window.mountEmployeePicker = mountEmployeePicker;
})();
