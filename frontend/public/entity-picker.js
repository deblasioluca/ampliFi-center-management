/**
 * EntityPicker — multi-select entity (company-code) picker with search and
 * country grouping. Replaces manual comma-separated text inputs.
 *
 * Usage in any Astro page:
 *
 *   <div id="my-picker" class="entity-picker"></div>
 *   <script src="/entity-picker.js" is:inline></script>
 *   <script is:inline>
 *     const picker = mountEntityPicker('my-picker', {
 *       initialCcodes: ['DE01', 'US01'],
 *       multi: true,
 *       onChange: (ccodes) => console.log(ccodes),
 *     });
 *     // Read current selection: picker.getCcodes()
 *     // Programmatic update: picker.setCcodes(['JP01'])
 *     // Disable when 'full scope' is checked: picker.setEnabled(false)
 *   </script>
 *
 * The picker fetches /api/entities once on mount with the JWT token from
 * localStorage and renders a chip + search UI. Up to ~500 entities load
 * comfortably; for larger sets switch to server-side search-as-you-type
 * (the endpoint already supports ?search=).
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

  function mountEntityPicker(target, options) {
    options = options || {};
    const root =
      typeof target === 'string' ? document.getElementById(target) : target;
    if (!root) {
      throw new Error('EntityPicker: target element not found');
    }
    const opts = {
      multi: options.multi !== false,
      initialCcodes: Array.isArray(options.initialCcodes)
        ? options.initialCcodes
        : [],
      placeholder:
        options.placeholder || 'Search company codes, names, countries…',
      onChange: typeof options.onChange === 'function' ? options.onChange : function () {},
      pageSize: options.pageSize || 500,
    };

    const token = localStorage.getItem('amplifi_token');

    // ── State ────────────────────────────────────────────────────────
    const state = {
      entities: [], // [{ccode, name, country, region}]
      selected: new Set(opts.initialCcodes),
      search: '',
      open: false,
      enabled: true,
      loading: true,
      error: null,
    };

    // ── DOM ──────────────────────────────────────────────────────────
    root.classList.add('ep-root');
    root.innerHTML = `
      <div class="ep-control border rounded bg-white">
        <div class="ep-chips flex flex-wrap gap-1 p-1 min-h-[34px] items-center cursor-text"></div>
        <input type="text" class="ep-search hidden w-full px-2 py-1 text-sm border-t outline-none"
               placeholder="${escapeHtml(opts.placeholder)}" />
        <div class="ep-dropdown hidden border-t max-h-72 overflow-y-auto"></div>
      </div>
      <div class="ep-status text-xs text-gray-400 mt-1"></div>
    `;
    const chipsEl = root.querySelector('.ep-chips');
    const searchEl = root.querySelector('.ep-search');
    const dropdownEl = root.querySelector('.ep-dropdown');
    const statusEl = root.querySelector('.ep-status');

    // ── Fetch entities ───────────────────────────────────────────────
    async function loadEntities() {
      state.loading = true;
      renderChips();
      try {
        const r = await fetch(
          `/api/entities?size=${opts.pageSize}`,
          { headers: { Authorization: 'Bearer ' + token } }
        );
        if (r.status === 401) {
          window.location.href = '/login';
          return;
        }
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        const data = await r.json();
        const items = Array.isArray(data) ? data : data.items || [];
        state.entities = items
          .filter((e) => e.is_active !== false)
          .map((e) => ({
            ccode: e.ccode,
            name: e.name || '',
            country: e.country || '',
            region: e.region || '',
          }))
          .sort((a, b) => a.ccode.localeCompare(b.ccode));
        state.loading = false;
        state.error = null;
      } catch (err) {
        state.loading = false;
        state.error = err.message || 'Failed to load entities';
      }
      renderChips();
      renderDropdown();
      renderStatus();
    }

    // ── Filtering ────────────────────────────────────────────────────
    function filtered() {
      const q = state.search.trim().toLowerCase();
      if (!q) return state.entities;
      return state.entities.filter(
        (e) =>
          e.ccode.toLowerCase().includes(q) ||
          e.name.toLowerCase().includes(q) ||
          e.country.toLowerCase().includes(q) ||
          e.region.toLowerCase().includes(q)
      );
    }

    function groupByCountry(list) {
      const groups = {};
      list.forEach((e) => {
        const k = e.country || '(no country)';
        if (!groups[k]) groups[k] = [];
        groups[k].push(e);
      });
      return Object.keys(groups)
        .sort()
        .map((k) => [k, groups[k]]);
    }

    // ── Renderers ────────────────────────────────────────────────────
    function renderChips() {
      const chips = [];
      const selectedArray = Array.from(state.selected);
      selectedArray.forEach((ccode) => {
        const e = state.entities.find((x) => x.ccode === ccode);
        const label = e ? `${ccode} — ${e.name || '(no name)'}` : ccode;
        chips.push(`
          <span class="ep-chip inline-flex items-center gap-1 bg-amplifi-100 text-amplifi-800 px-2 py-0.5 rounded text-xs">
            <span title="${escapeHtml(label)}">${escapeHtml(ccode)}</span>
            ${state.enabled ? `<button type="button" class="ep-chip-remove text-amplifi-500 hover:text-amplifi-800" data-ccode="${escapeHtml(ccode)}" aria-label="Remove ${escapeHtml(ccode)}">×</button>` : ''}
          </span>
        `);
      });
      const placeholder = !selectedArray.length
        ? `<span class="ep-placeholder text-xs text-gray-400 px-2">${state.enabled ? 'Click to select entities…' : 'Disabled (full scope)'}</span>`
        : '';
      chipsEl.innerHTML = chips.join('') + placeholder;

      chipsEl.querySelectorAll('.ep-chip-remove').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const ccode = btn.getAttribute('data-ccode');
          state.selected.delete(ccode);
          renderChips();
          renderDropdown();
          opts.onChange(getCcodes());
        });
      });
    }

    function renderDropdown() {
      if (!state.open) {
        dropdownEl.classList.add('hidden');
        searchEl.classList.add('hidden');
        return;
      }
      searchEl.classList.remove('hidden');
      dropdownEl.classList.remove('hidden');

      if (state.loading) {
        dropdownEl.innerHTML = `<div class="px-3 py-2 text-xs text-gray-400">Loading entities…</div>`;
        return;
      }
      if (state.error) {
        dropdownEl.innerHTML = `<div class="px-3 py-2 text-xs text-red-600">${escapeHtml(state.error)}</div>`;
        return;
      }
      const list = filtered();
      if (!list.length) {
        dropdownEl.innerHTML = `<div class="px-3 py-2 text-xs text-gray-400">No matches</div>`;
        return;
      }

      const groups = groupByCountry(list);
      const html = groups
        .map(([country, entities]) => {
          const allSelected = entities.every((e) => state.selected.has(e.ccode));
          const someSelected = entities.some((e) => state.selected.has(e.ccode));
          const groupAction = opts.multi
            ? `<button type="button" class="ep-group-toggle text-[10px] text-amplifi-600 hover:underline" data-country="${escapeHtml(country)}">
                 ${allSelected ? 'Unselect all' : someSelected ? 'Select all' : 'Select all'}
               </button>`
            : '';
          const items = entities
            .map((e) => {
              const isSelected = state.selected.has(e.ccode);
              return `
                <div class="ep-item flex items-center gap-2 px-3 py-1.5 cursor-pointer hover:bg-amplifi-50 ${isSelected ? 'bg-amplifi-50' : ''}"
                     data-ccode="${escapeHtml(e.ccode)}">
                  <input type="checkbox" class="ep-item-cb pointer-events-none" ${isSelected ? 'checked' : ''} />
                  <span class="text-xs font-mono w-14">${escapeHtml(e.ccode)}</span>
                  <span class="text-xs text-gray-700 flex-1 truncate" title="${escapeHtml(e.name)}">${escapeHtml(e.name) || '<span class="italic text-gray-400">no name</span>'}</span>
                  ${e.region ? `<span class="text-[10px] text-gray-400">${escapeHtml(e.region)}</span>` : ''}
                </div>
              `;
            })
            .join('');
          return `
            <div class="ep-group">
              <div class="ep-group-head sticky top-0 bg-gray-50 border-b px-3 py-1 flex items-center justify-between text-[11px] uppercase tracking-wider text-gray-500">
                <span>${escapeHtml(country)} <span class="text-gray-400">(${entities.length})</span></span>
                ${groupAction}
              </div>
              ${items}
            </div>
          `;
        })
        .join('');

      dropdownEl.innerHTML = html;

      // Bind item clicks
      dropdownEl.querySelectorAll('.ep-item').forEach((el) => {
        el.addEventListener('click', () => {
          const ccode = el.getAttribute('data-ccode');
          if (!opts.multi) {
            state.selected.clear();
            state.selected.add(ccode);
            state.open = false;
            state.search = '';
            searchEl.value = '';
          } else if (state.selected.has(ccode)) {
            state.selected.delete(ccode);
          } else {
            state.selected.add(ccode);
          }
          renderChips();
          renderDropdown();
          opts.onChange(getCcodes());
        });
      });

      // Bind group select-all
      dropdownEl.querySelectorAll('.ep-group-toggle').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const country = btn.getAttribute('data-country');
          const inGroup = state.entities.filter(
            (x) => (x.country || '(no country)') === country
          );
          const allSelected = inGroup.every((x) => state.selected.has(x.ccode));
          if (allSelected) {
            inGroup.forEach((x) => state.selected.delete(x.ccode));
          } else {
            inGroup.forEach((x) => state.selected.add(x.ccode));
          }
          renderChips();
          renderDropdown();
          opts.onChange(getCcodes());
        });
      });
    }

    function renderStatus() {
      if (state.loading) {
        statusEl.textContent = 'Loading…';
      } else if (state.error) {
        statusEl.textContent = state.error;
      } else {
        statusEl.textContent = `${state.entities.length} entities available · ${state.selected.size} selected`;
      }
    }

    // ── Event wiring ─────────────────────────────────────────────────
    chipsEl.addEventListener('click', () => {
      if (!state.enabled) return;
      state.open = true;
      renderDropdown();
      setTimeout(() => searchEl.focus(), 0);
    });

    searchEl.addEventListener('input', (e) => {
      state.search = e.target.value;
      renderDropdown();
    });

    searchEl.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        state.open = false;
        renderDropdown();
        searchEl.blur();
      } else if (e.key === 'Enter') {
        e.preventDefault();
        const list = filtered();
        if (list.length === 1) {
          const ccode = list[0].ccode;
          if (state.selected.has(ccode)) state.selected.delete(ccode);
          else state.selected.add(ccode);
          renderChips();
          renderDropdown();
          opts.onChange(getCcodes());
        }
      }
    });

    document.addEventListener('click', (e) => {
      if (!root.contains(e.target)) {
        state.open = false;
        renderDropdown();
      }
    });

    // ── Public API ────────────────────────────────────────────────────
    function getCcodes() {
      return Array.from(state.selected);
    }

    function setCcodes(codes) {
      state.selected = new Set(Array.isArray(codes) ? codes : []);
      renderChips();
      renderDropdown();
      renderStatus();
      opts.onChange(getCcodes());
    }

    function setEnabled(enabled) {
      state.enabled = !!enabled;
      if (!state.enabled) state.open = false;
      root.classList.toggle('ep-disabled', !state.enabled);
      root.style.opacity = state.enabled ? '1' : '0.5';
      root.style.pointerEvents = state.enabled ? 'auto' : 'none';
      renderChips();
      renderDropdown();
    }

    // ── Boot ──────────────────────────────────────────────────────────
    renderChips();
    renderStatus();
    loadEntities();

    return { getCcodes: getCcodes, setCcodes: setCcodes, setEnabled: setEnabled };
  }

  window.mountEntityPicker = mountEntityPicker;
})();
