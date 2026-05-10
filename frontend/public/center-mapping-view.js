/**
 * CenterMappingView — displays Legacy CC/PC → Target CC/PC relationships
 * with Target PC rowspan grouping (multiple target CCs → one target PC).
 *
 * Usage:
 *   new CenterMappingView({
 *     containerId: 'mapping-container',
 *     scope: 'cleanup',
 *     authHeaders: { Authorization: 'Bearer ...' },
 *   }).load();
 */

/* eslint-disable no-var */
(function (root) {
  'use strict';

  function esc(t) {
    var d = document.createElement('div');
    d.textContent = t || '';
    return d.innerHTML;
  }

  function CenterMappingView(opts) {
    this.containerId = opts.containerId;
    this.scope = opts.scope || '';
    this.authHeaders = opts.authHeaders || {};
    this.apiBase = opts.apiBase || '';
    this._data = null;
    this._search = '';
  }

  CenterMappingView.prototype.load = function () {
    var self = this;
    var container = document.getElementById(this.containerId);
    if (!container) return;
    container.innerHTML = '<p class="text-sm text-gray-400 py-4">Loading mapping data...</p>';

    var url = this.apiBase + '/api/center-mappings/overview';
    var params = [];
    if (this.scope) params.push('scope=' + encodeURIComponent(this.scope));
    if (this._search) params.push('search=' + encodeURIComponent(this._search));
    if (params.length) url += '?' + params.join('&');

    fetch(url, { headers: this.authHeaders })
      .then(function (r) { return r.json(); })
      .then(function (data) {
        self._data = data;
        self.render();
      })
      .catch(function () {
        container.innerHTML = '<p class="text-sm text-red-500">Failed to load mapping data.</p>';
      });
  };

  CenterMappingView.prototype.render = function () {
    var self = this;
    var container = document.getElementById(this.containerId);
    if (!container) return;

    var items = (this._data && this._data.items) || [];
    var total = (this._data && this._data.total) || 0;

    // Toolbar
    var html = '<div class="flex items-center gap-3 px-3 py-2 border-b bg-gray-50">';
    html += '<span class="text-xs text-gray-500 font-medium">Center Mapping Overview</span>';
    html += '<span class="text-xs text-gray-400">(' + total + ' rows)</span>';
    html += '<input type="text" id="' + this.containerId + '-search" class="input text-xs w-48 ml-auto" placeholder="Search..." value="' + esc(this._search) + '">';
    html += '<button id="' + this.containerId + '-csv" class="btn-secondary text-xs py-1">CSV</button>';
    html += '</div>';

    if (!items.length) {
      html += '<div class="flex items-center justify-center h-48 text-gray-400 text-sm">';
      html += 'No mapping data available. Upload center mappings first.</div>';
      container.innerHTML = html;
      self._bindEvents();
      return;
    }

    // Group by target_pc for rowspan
    var groups = [];
    var currentPC = null;
    var currentGroup = null;
    items.forEach(function (row) {
      if (row.target_pc !== currentPC) {
        currentGroup = { target_pc: row.target_pc, rows: [] };
        groups.push(currentGroup);
        currentPC = row.target_pc;
      }
      currentGroup.rows.push(row);
    });

    // Table
    html += '<div class="overflow-auto" style="max-height:calc(100vh - 200px)">';
    html += '<table class="w-full text-xs border-collapse">';
    html += '<thead class="bg-gray-100 sticky top-0">';
    html += '<tr>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b">Legacy CC</th>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b">Legacy CC Name</th>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b">Legacy PC</th>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b border-l-2 border-l-blue-200">Target CC</th>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b">Target CC Name</th>';
    html += '<th class="px-3 py-2 text-left font-medium text-gray-600 border-b">Target PC</th>';
    html += '</tr></thead><tbody>';

    groups.forEach(function (group) {
      var rowCount = group.rows.length;
      group.rows.forEach(function (row, idx) {
        var borderClass = idx === 0 && groups.indexOf(group) > 0 ? ' border-t-2 border-t-gray-300' : ' border-b border-gray-100';
        html += '<tr class="hover:bg-blue-50' + borderClass + '">';
        html += '<td class="px-3 py-1.5 font-mono">' + esc(row.legacy_cc) + '</td>';
        html += '<td class="px-3 py-1.5 text-gray-600">' + esc(row.legacy_cc_name) + '</td>';
        html += '<td class="px-3 py-1.5 font-mono">' + esc(row.legacy_pc) + '</td>';
        html += '<td class="px-3 py-1.5 font-mono border-l-2 border-l-blue-200">' + esc(row.target_cc) + '</td>';
        html += '<td class="px-3 py-1.5 text-gray-600">' + esc(row.target_cc_name) + '</td>';
        // Target PC — only render on first row of group (rowspan)
        if (idx === 0) {
          html += '<td class="px-3 py-1.5 font-mono font-medium text-blue-700 bg-blue-50/50" rowspan="' + rowCount + '">' + esc(row.target_pc) + '</td>';
        }
        html += '</tr>';
      });
    });

    html += '</tbody></table></div>';
    container.innerHTML = html;
    self._bindEvents();
  };

  CenterMappingView.prototype._bindEvents = function () {
    var self = this;
    var searchInput = document.getElementById(this.containerId + '-search');
    if (searchInput) {
      var debounce = null;
      searchInput.addEventListener('input', function () {
        clearTimeout(debounce);
        debounce = setTimeout(function () {
          self._search = searchInput.value.trim();
          self.load();
        }, 300);
      });
    }
    var csvBtn = document.getElementById(this.containerId + '-csv');
    if (csvBtn) {
      csvBtn.addEventListener('click', function () { self._downloadCSV(); });
    }
  };

  CenterMappingView.prototype._downloadCSV = function () {
    var items = (this._data && this._data.items) || [];
    if (!items.length) return;

    // In CSV, repeat Target PC for every row (no rowspan)
    var cols = ['legacy_cc', 'legacy_cc_name', 'legacy_pc', 'target_cc', 'target_cc_name', 'target_pc'];
    var headers = ['Legacy CC', 'Legacy CC Name', 'Legacy PC', 'Target CC', 'Target CC Name', 'Target PC'];

    var lines = [];
    lines.push(headers.map(function (h) { return '"' + h + '"'; }).join(','));
    items.forEach(function (row) {
      lines.push(cols.map(function (c) {
        var v = row[c];
        if (v == null) return '';
        return '"' + String(v).replace(/"/g, '""') + '"';
      }).join(','));
    });

    var blob = new Blob([lines.join('\n')], { type: 'text/csv' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = 'center-mapping-overview.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  CenterMappingView.prototype.setScope = function (scope) {
    this.scope = scope;
    this.load();
  };

  root.CenterMappingView = CenterMappingView;
})(window);
