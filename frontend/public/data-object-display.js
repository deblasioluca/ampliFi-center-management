/**
 * DataObjectDisplay — central display engine for all data objects.
 *
 * Provides tabular and hierarchical views with:
 *   - Display config integration (ExplorerDisplayConfig columns + labels)
 *   - Hierarchy columns (L0..Lx) in tabular view
 *   - Hierarchy picker with auto-select and "no hierarchy" option
 *   - Hierarchical tree view with leaves, indentation, collapse/expand all
 *   - Entity hierarchy connection to centers (via entity in center info)
 *   - GL account artificial hierarchy types (type A: 1 char, type B: 5 chars)
 *   - Sorting, filtering, pagination, CSV download
 *
 * Usage:
 *   const display = new DataObjectDisplay({
 *     objectType: 'cost-centers',          // matches ExplorerDisplayConfig key
 *     containerId: 'my-container',         // DOM element ID
 *     apiBase: '',                          // base URL for API calls
 *     authHeaders: { Authorization: '...' },
 *     dataEndpoint: '/api/legacy/cost-centers', // where to fetch rows
 *     hierarchyEndpoint: '/api/legacy/hierarchies', // for hierarchy picker
 *     identityField: 'cctr',               // primary key field shown per row
 *     entityField: 'ccode',                // for entity hierarchy connection
 *     profitCenterField: 'pctr',           // for PC hierarchy connection
 *     onRowClick: function(row) {},        // optional click handler
 *     showHierarchyPicker: true,           // show hierarchy dropdown
 *     showViewToggle: true,                // show tabular/hierarchical toggle
 *     showSearch: true,                    // show search input
 *     showPagination: true,                // show pagination controls
 *     pageSize: 200,                       // rows per page
 *     extraColumns: [],                    // additional columns prepended
 *     extraQueryParams: {},                // added to every data fetch
 *     hierarchyTypes: null,                // null = all, or ['0101','0106'] etc.
 *   });
 *   display.load();
 */

/* eslint-disable no-var */
(function (root) {
  'use strict';

  // ── Inject DOD keyframe CSS (once) ─────────────────────────────────
  if (!document.getElementById('dod-keyframes')) {
    var style = document.createElement('style');
    style.id = 'dod-keyframes';
    style.textContent = '@keyframes dodSlideIn{from{transform:translateX(100%)}to{transform:translateX(0)}}';
    document.head.appendChild(style);
  }

  // ── Helpers ──────────────────────────────────────────────────────────

  function esc(t) {
    var d = document.createElement('div');
    d.textContent = t || '';
    return d.innerHTML;
  }

  function escAttr(s) {
    return esc(s).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
  }

  function fmt(n) {
    return (n || 0).toLocaleString();
  }

  // Normalise setclass codes — uploaded data may use short aliases
  // (CC, PC, ENT) instead of the standard 4-digit SAP codes.
  var SETCLASS_ALIASES = {
    'CC': '0101', 'PC': '0104', 'ENT': '0106', 'ENTITY': '0106'
  };
  function normaliseSetclass(raw) {
    if (!raw) return '';
    var upper = String(raw).trim().toUpperCase();
    return SETCLASS_ALIASES[upper] || upper;
  }

  // ── DataObjectDisplay constructor ───────────────────────────────────

  function DataObjectDisplay(opts) {
    this.objectType = opts.objectType || 'cost-centers';
    this.containerId = opts.containerId;
    this.apiBase = opts.apiBase || '';
    this.authHeaders = opts.authHeaders || {};
    this.dataEndpoint = opts.dataEndpoint || '';
    this.hierarchyEndpoint = opts.hierarchyEndpoint || '/api/legacy/hierarchies';
    this.identityField = opts.identityField || 'id';
    this.entityField = opts.entityField || 'ccode';
    this.profitCenterField = opts.profitCenterField || 'pctr';
    this.onRowClick = opts.onRowClick || null;
    this.showHierarchyPicker = opts.showHierarchyPicker !== false;
    this.showViewToggle = opts.showViewToggle !== false;
    this.showSearch = opts.showSearch !== false;
    this.showPagination = opts.showPagination !== false;
    this.subtitle = opts.subtitle || '';
    this.showCSV = opts.showCSV !== false;
    this.pageSize = opts.pageSize || 200;
    this.hierPageSize = opts.hierPageSize || 10000;
    this.extraColumns = opts.extraColumns || [];
    this.extraQueryParams = opts.extraQueryParams || {};
    this.hierarchyTypes = opts.hierarchyTypes || null;
    this.glHierarchyMode = opts.glHierarchyMode || null;
    this.columns = opts.columns || null;
    var _alwaysExclude = ['scope', 'data_category'];
    this.excludeColumns = _alwaysExclude.concat(opts.excludeColumns || []);
    this.inlineHierarchies = opts.inlineHierarchies || false;
    this.includeBalances = opts.includeBalances || false;
    this.onDataLoad = opts.onDataLoad || null;
    this.showBalanceColumns = opts.showBalanceColumns || false;
    // Custom toolbar buttons: [{label, className, onclick, title}]
    this.toolbarButtons = opts.toolbarButtons || [];
    // Extra filter widgets: [{id, type:'text'|'select'|'number', placeholder, options}]
    this.extraFilters = opts.extraFilters || [];
    // Callback when extra filter changes
    this.onExtraFilterChange = opts.onExtraFilterChange || null;
    // Callback when hierarchy picker changes (receives hierId or null)
    this.onHierarchyChange = opts.onHierarchyChange || null;
    // Title shown above the table
    this.title = opts.title || '';
    // Row action buttons: [{label, className, title, onclick(row, self)}]
    this.rowActions = opts.rowActions || [];
    // Built-in detail panel on row click (shows all fields in slide-out)
    this.showDetailOnClick = opts.showDetailOnClick !== false;
    // Last loaded timestamp (ISO string) for the data in this table
    this.lastLoadedAt = opts.lastLoadedAt || null;

    // State
    this._view = opts.defaultView || 'tabular';
    this._page = 1;
    this._search = '';
    this._sort = { col: null, dir: 'asc' };
    this._hierPickerId = null;
    this._hierData = null;
    this._hierAutoSelected = false;
    this._hierOptions = [];
    this._data = null;
    this._displayConfig = null;
    this._columnFilters = {};        // { col: Set of selected values } for Excel-style
    this._columnFilterSearch = {};   // { col: search text } for filtering within dropdown
    this._openFilterCol = null;      // which column's filter dropdown is open
    this._expandedNodes = {};
    this._allExpanded = true;
    this._selectedHierNode = null;
    this._idSeq = 0;
    this._hierInlined = false;
    this._hierDetailItems = [];
  }

  // ── Display config loading ──────────────────────────────────────────

  DataObjectDisplay.prototype.loadDisplayConfig = function (cb) {
    var self = this;
    fetch(this.apiBase + '/api/explore/display-config/' + encodeURIComponent(this.objectType), {
      headers: this.authHeaders,
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        self._displayConfig = d;
        if (cb) cb(d);
      })
      .catch(function () {
        self._displayConfig = { table_columns: [], column_labels: {}, all_columns: [] };
        if (cb) cb(self._displayConfig);
      });
  };

  // ── Get effective table columns ─────────────────────────────────────

  DataObjectDisplay.prototype.getTableColumns = function () {
    var excl = this.excludeColumns;
    function applyExclude(arr) {
      if (!excl.length) return arr;
      return arr.filter(function (c) { return excl.indexOf(c) < 0; });
    }
    // Check display config first
    if (this._displayConfig) {
      var cols = this._displayConfig.table_columns;
      if (cols && cols.length) return applyExclude(cols);
      cols = (this._displayConfig.all_columns || []).slice(0, 10);
      if (cols.length) return applyExclude(cols);
    }
    // Fallback: use caller-specified columns
    if (this.columns && this.columns.length) {
      return applyExclude(this.columns.map(function (c) { return c.key || c; }));
    }
    // Auto-discover from first data item
    if (this._data && this._data.items && this._data.items.length) {
      var first = this._data.items[0];
      var auto = Object.keys(first).filter(function (k) {
        return k !== 'levels' && k !== 'monthly_balances';
      });
      return applyExclude(auto);
    }
    return [];
  };

  // Column label — checks display config, then fallback columns, then raw key
  DataObjectDisplay.prototype.colLabel = function (col) {
    if (this._displayConfig) {
      var labels = this._displayConfig.column_labels || {};
      if (labels[col]) return labels[col];
    }
    if (this.columns) {
      for (var i = 0; i < this.columns.length; i++) {
        if ((this.columns[i].key || this.columns[i]) === col) {
          return this.columns[i].label || col;
        }
      }
    }
    return col;
  };

  // ── Hierarchy picker loading ────────────────────────────────────────

  DataObjectDisplay.prototype.loadHierarchyOptions = function (cb) {
    var self = this;
    fetch(this.apiBase + this.hierarchyEndpoint + '?size=200', {
      headers: this.authHeaders,
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        var items = d.items || [];
        self._hierOptions = items;
        if (cb) cb(items);
      })
      .catch(function () {
        self._hierOptions = [];
        if (cb) cb([]);
      });
  };

  // ── Build hierarchy picker HTML (inline select, no wrapper div) ─────

  DataObjectDisplay.prototype.buildHierarchyPickerHtml = function () {
    var items = this._hierOptions;
    if (!items.length && !this.glHierarchyMode) return '';

    var html = '<select class="input text-xs flex-shrink-0" style="max-width:400px" data-dod-role="hier-picker">';
    html += '<option value="">(none — no hierarchy)</option>';

    if (this.glHierarchyMode) {
      html += '<option value="__gl_type_a">GL Type A — 1st character</option>';
      html += '<option value="__gl_type_b">GL Type B — first 5 chars</option>';
      html += '</select>';
      return html;
    }

    // Group by normalised setclass
    var groups = { '0101': [], '0106': [], '0104': [], other: [] };
    var groupLabels = {
      '0101': 'CC hierarchies',
      '0106': 'Entity hierarchies',
      '0104': 'PC hierarchies',
      other: 'Other',
    };

    var filterTypes = this.hierarchyTypes;
    items.forEach(function (h) {
      var norm = normaliseSetclass(h.setclass);
      if (filterTypes && filterTypes.indexOf(norm) < 0) return;
      var key = groups[norm] ? norm : 'other';
      groups[key].push(h);
    });

    ['0101', '0106', '0104', 'other'].forEach(function (key) {
      var list = groups[key];
      if (!list.length) return;
      html += '<optgroup label="' + escAttr(groupLabels[key]) + '">';
      list.forEach(function (h) {
        var label = esc(h.label || h.setname || '');
        if (h.description) label += ' — ' + esc(h.description);
        html += '<option value="' + h.id + '">' + label + '</option>';
      });
      html += '</optgroup>';
    });

    html += '</select>';

    // Auto-select only on first load (not every render)
    if (!this._hierAutoSelected) {
      this._hierAutoSelected = true;
      if (!filterTypes) {
        if (items.length === 1) {
          this._hierPickerId = items[0].id;
        }
      } else {
        var filtered = items.filter(function (h) { return filterTypes.indexOf(normaliseSetclass(h.setclass)) >= 0; });
        if (filtered.length === 1) {
          this._hierPickerId = filtered[0].id;
        }
      }
    }

    return html;
  };

  // ── View toggle HTML ────────────────────────────────────────────────

  DataObjectDisplay.prototype.buildViewToggleHtml = function () {
    var tabularActive = this._view === 'tabular';
    return '<div class="inline-flex rounded-md border border-gray-300 text-xs">' +
      '<button data-dod-role="view-tabular" class="px-3 py-1.5' +
      (tabularActive ? ' bg-amplifi-50 text-amplifi-700 font-medium' : ' text-gray-600 hover:bg-gray-50') +
      ' whitespace-nowrap">Tabular</button>' +
      '<button data-dod-role="view-hierarchy" class="px-3 py-1.5 border-l border-gray-300' +
      (!tabularActive ? ' bg-amplifi-50 text-amplifi-700 font-medium' : ' text-gray-600 hover:bg-gray-50') +
      ' whitespace-nowrap">Hierarchical</button>' +
      '</div>';
  };

  // ── Toolbar HTML ────────────────────────────────────────────────────

  DataObjectDisplay.prototype.buildToolbarHtml = function () {
    var self = this;
    var html = '';
    // Title row (heading + last loaded indicator)
    if (this.title) {
      html += '<div class="flex items-center justify-between mb-2"><h3 class="text-base font-semibold">' + esc(this.title);
      if (this.subtitle) html += ' <span class="text-xs font-normal text-gray-500">' + esc(this.subtitle).replace(/ampliFi/g, '<span style="color:#E60000">a</span>mpliF<span style="color:#E60000">i</span>') + '</span>';
      html += '</h3>';
      if (this.lastLoadedAt) {
        var d = new Date(this.lastLoadedAt);
        var ts = d.toLocaleDateString() + ' ' + d.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
        html += '<span class="text-[10px] text-gray-400" title="Last data load timestamp">Last loaded: ' + esc(ts) + '</span>';
      }
      html += '</div>';
    }
    // Single condensed controls row
    html += '<div class="flex items-center gap-2 mb-2 flex-wrap">';
    if (this.showViewToggle) {
      html += this.buildViewToggleHtml();
    }
    if (this.showSearch) {
      html += '<input type="text" data-dod-role="search" placeholder="Search..." class="input text-xs py-1 flex-1" style="min-width:120px;max-width:220px" value="' + escAttr(this._search) + '" />';
    }
    if (this.showCSV) {
      html += '<button data-dod-role="csv" class="btn-secondary text-xs py-1 flex-shrink-0" title="Download as CSV">CSV</button>';
    }
    if (this._view === 'hierarchy') {
      html += '<button data-dod-role="expand-all" class="btn-secondary text-xs py-1 flex-shrink-0">Expand All</button>';
      html += '<button data-dod-role="collapse-all" class="btn-secondary text-xs py-1 flex-shrink-0">Collapse All</button>';
    }
    // Hierarchy picker inline
    if (this.showHierarchyPicker && (this._hierOptions.length || this.glHierarchyMode)) {
      html += this.buildHierarchyPickerHtml();
    }
    // Extra filter widgets
    this.extraFilters.forEach(function (f) {
      if (f.type === 'select') {
        html += '<select data-dod-role="extra-filter" data-dod-filter-id="' + escAttr(f.id) + '" class="input text-xs py-1 ' + (f.className || 'w-32') + '">';
        (f.options || []).forEach(function (opt) {
          var val = typeof opt === 'string' ? opt : opt.value;
          var label = typeof opt === 'string' ? opt : opt.label;
          html += '<option value="' + escAttr(val) + '">' + esc(label) + '</option>';
        });
        html += '</select>';
      } else {
        html += '<input type="' + (f.type || 'text') + '" data-dod-role="extra-filter" data-dod-filter-id="' + escAttr(f.id) + '"' +
          ' placeholder="' + escAttr(f.placeholder || '') + '"' +
          ' class="input text-xs py-1 ' + (f.className || 'w-32') + '" />';
      }
    });
    // Active filter count
    var activeFilters = Object.keys(this._columnFilters).length;
    if (activeFilters > 0) {
      html += '<span class="text-xs text-blue-600 font-medium flex-shrink-0">' + activeFilters + ' filter(s)</span>';
      html += '<button data-dod-role="clear-all-filters" class="text-xs text-red-500 hover:text-red-700 underline flex-shrink-0">Clear</button>';
    }
    // Toolbar buttons (Delete All, etc.) — right-aligned
    if (this.toolbarButtons.length) {
      html += '<span class="ml-auto"></span>';
      this.toolbarButtons.forEach(function (btn, i) {
        html += '<button data-dod-role="toolbar-btn-' + i + '" class="' +
          escAttr(btn.className || 'btn-secondary text-xs') + ' flex-shrink-0"' +
          (btn.title ? ' title="' + escAttr(btn.title) + '"' : '') + '>' +
          esc(btn.label) + '</button>';
      });
    }
    html += '</div>';

    return html;
  };

  // ── Data loading ────────────────────────────────────────────────────

  DataObjectDisplay.prototype._showLoading = function () {
    var container = document.getElementById(this.containerId);
    if (!container) return;
    // Show loading indicator without clearing existing content
    var existing = container.querySelector('[data-dod-loading]');
    if (!existing) {
      var el = document.createElement('div');
      el.setAttribute('data-dod-loading', '1');
      el.className = 'text-sm text-gray-500 py-3 flex items-center gap-2';
      el.innerHTML = '<svg class="animate-spin h-4 w-4 text-blue-500" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg> Loading...';
      container.insertBefore(el, container.firstChild);
    }
  };

  DataObjectDisplay.prototype.loadData = function (cb) {
    var self = this;
    if (!this.dataEndpoint) { if (cb) cb(null); return; }
    this._showLoading();

    // Use larger page size in hierarchical mode
    var effectiveSize = (this._view === 'hierarchy') ? this.hierPageSize : this.pageSize;

    var params = [];
    params.push('page=' + this._page);
    params.push('size=' + effectiveSize);
    if (this._search) params.push('search=' + encodeURIComponent(this._search));

    // Add hierarchy_id if selected (for server-side path resolution)
    if (this._hierPickerId && typeof this._hierPickerId === 'number') {
      params.push('hierarchy_id=' + this._hierPickerId);
    }

    // For inline hierarchy mode, always request include_hierarchies
    // so the picker is populated and tree data is available.
    if (this.inlineHierarchies) {
      params.push('include_hierarchies=true');
      this._hierInlined = true;
    }

    // Include balances
    if (this.includeBalances) {
      params.push('include_balances=true');
    }

    // Add extra query params
    var eq = this.extraQueryParams;
    Object.keys(eq).forEach(function (k) {
      if (eq[k] != null && eq[k] !== '') params.push(k + '=' + encodeURIComponent(eq[k]));
    });

    var fetchHeaders = Object.assign({}, this.authHeaders);
    var fetchFn = window.apiFetch || fetch;
    fetchFn(this.apiBase + this.dataEndpoint + '?' + params.join('&'), {
      headers: fetchHeaders,
    })
      .then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return r.json();
      })
      .then(function (d) {
        self._data = d;
        // For inline hierarchies, populate hierarchy options from response
        if (self.inlineHierarchies && d.hierarchies) {
          self._hierOptions = d.hierarchies;
          // If hierarchies are inlined with nodes/leaves, use them for tree
          if (d.hierarchies_inlined && self._hierPickerId) {
            var picked = d.hierarchies.filter(function (h) { return h.id === self._hierPickerId; })[0];
            if (picked && picked.nodes) {
              self._hierData = picked;
            }
          }
        }
        if (self.onDataLoad) self.onDataLoad(d);
        if (cb) cb(d);
      })
      .catch(function (err) {
        self._data = { items: [], total: 0 };
        if (cb) cb(self._data, err);
      });
  };

  // ── Load hierarchy tree data (nodes + leaves) ───────────────────────

  DataObjectDisplay.prototype.loadHierarchyTree = function (hierId, cb) {
    var self = this;
    fetch(this.apiBase + '/api/legacy/hierarchies/' + hierId + '/tree', {
      headers: this.authHeaders,
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        self._hierData = d;
        if (cb) cb(d);
      })
      .catch(function () {
        self._hierData = null;
        if (cb) cb(null);
      });
  };

  // ── Build hierarchy level map for tabular view ──────────────────────

  DataObjectDisplay.prototype.buildHierLevelMap = function () {
    // When no hierarchy is selected, return empty
    if (!this._hierPickerId) return { levels: [], map: {} };

    // For GL artificial hierarchies
    if (this._hierPickerId === '__gl_type_a' || this._hierPickerId === '__gl_type_b') {
      return this._buildGLHierLevelMap();
    }

    var data = this._data;
    if (!data || !data.items) return { levels: [], map: {} };

    // Mode 1: Server returned levels per item (from hierarchy_id param)
    var maxDepth = data.hierarchy_max_depth || 0;
    if (maxDepth > 0 && data.items.length && data.items[0].levels) {
      var levelCols = [];
      for (var i = 0; i < maxDepth; i++) {
        levelCols.push('L' + i);
      }
      var map = {};
      var items = data.items || [];
      items.forEach(function (it) {
        var key = it[this.identityField] || it.id;
        if (it.levels && it.levels.length) {
          var obj = {};
          for (var j = 0; j < maxDepth; j++) {
            obj['L' + j] = it.levels[j] || '';
          }
          map[key] = obj;
        }
      }.bind(this));
      return { levels: levelCols, map: map };
    }

    // Mode 2: Inline hierarchies — build paths client-side from
    // nodes/leaves data (Data Browser mode)
    var hiers = (data.hierarchies) || [];
    var hier = null;
    for (var h = 0; h < hiers.length; h++) {
      if (hiers[h].id === this._hierPickerId) { hier = hiers[h]; break; }
    }
    if (!hier || !hier.nodes) return { levels: [], map: {} };

    return this._buildClientSideHierLevelMap(hier);
  };

  // Build L0..Lx map from inline hierarchy nodes/leaves (same logic
  // that was previously duplicated in data/index.astro's buildHierLevelMap)
  DataObjectDisplay.prototype._buildClientSideHierLevelMap = function (hier) {
    var self = this;
    var childMap = {};
    (hier.nodes || []).forEach(function (n) {
      var parent = n.parent_setname || n.parent;
      var child = n.child_setname || n.child;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'node', name: child, seq: n.seq || 0 });
    });
    (hier.leaves || []).forEach(function (lf) {
      var parent = lf.setname || lf.parent;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'leaf', value: lf.value, seq: lf.seq || 0 });
    });

    // Find roots
    var allChildren = {};
    (hier.nodes || []).forEach(function (n) {
      allChildren[n.child_setname || n.child] = 1;
    });
    var roots = [];
    (hier.nodes || []).forEach(function (n) {
      var p = n.parent_setname || n.parent;
      if (!allChildren[p] && roots.indexOf(p) < 0) roots.push(p);
    });
    if (!roots.length && hier.nodes && hier.nodes.length) {
      roots.push(hier.nodes[0].parent_setname || hier.nodes[0].parent);
    }

    // DFS to build paths: leaf value -> [L0, L1, ..., Ln]
    var leafPaths = {};
    var maxDepth = 0;
    function dfs(nodeName, path) {
      var children = childMap[nodeName] || [];
      children.forEach(function (c) {
        if (c.type === 'leaf') {
          leafPaths[c.value] = path.slice();
          if (path.length > maxDepth) maxDepth = path.length;
        } else {
          dfs(c.name, path.concat([c.name]));
        }
      });
    }
    roots.forEach(function (r) { dfs(r, [r]); });

    var levelCols = [];
    for (var i = 0; i < maxDepth; i++) {
      levelCols.push('L' + i);
    }

    // Map items to their level columns, respecting setclass for key lookup
    var normSetclass = normaliseSetclass(hier.setclass);
    var result = {};
    var allItems = (this._data && this._data.items) || [];

    function levelObj(p) {
      var obj = {};
      for (var i = 0; i < maxDepth; i++) {
        obj['L' + i] = p[i] || '';
      }
      return obj;
    }

    if (normSetclass === '0106') {
      // Entity hierarchy — leaf key is ccode
      allItems.forEach(function (it) {
        var key = it[self.identityField] || it.id;
        var lookup = it[self.entityField];
        if (key && lookup && leafPaths[lookup]) {
          result[key] = levelObj(leafPaths[lookup]);
        }
      });
    } else if (normSetclass === '0104') {
      // PC hierarchy — leaf key is pctr
      allItems.forEach(function (it) {
        var key = it[self.identityField] || it.id;
        var lookup = it[self.profitCenterField];
        if (key && lookup && leafPaths[lookup]) {
          result[key] = levelObj(leafPaths[lookup]);
        }
      });
    } else {
      // CC hierarchy — leaf key IS identity field
      Object.keys(leafPaths).forEach(function (leafVal) {
        result[leafVal] = levelObj(leafPaths[leafVal]);
      });
    }

    return { levels: levelCols, map: result, hierLabel: hier.label || hier.setname || 'Hierarchy' };
  };

  // ── GL artificial hierarchy map ─────────────────────────────────────

  DataObjectDisplay.prototype._buildGLHierLevelMap = function () {
    var items = (this._data && this._data.items) || [];
    var isTypeA = this._hierPickerId === '__gl_type_a';
    var field = 'saknr'; // GL account number field

    var groups = {};
    items.forEach(function (it) {
      var acct = String(it[field] || '');
      var key = isTypeA ? (acct.charAt(0) || '?') : (acct.substring(0, 5) || '?????');
      if (!groups[key]) groups[key] = [];
      groups[key].push(it);
    });

    var map = {};
    items.forEach(function (it) {
      var acct = String(it[field] || '');
      var key = isTypeA ? (acct.charAt(0) || '?') : (acct.substring(0, 5) || '?????');
      map[it.id || acct] = { L0: key };
    });

    return { levels: ['L0'], map: map, isGL: true };
  };

  // ── Sort helpers ────────────────────────────────────────────────────

  DataObjectDisplay.prototype.sortItems = function (items, hierMap) {
    if (!this._sort.col) return items;
    var col = this._sort.col;
    var dir = this._sort.dir === 'desc' ? -1 : 1;
    var isLevelCol = col.match(/^L\d+$/);
    var self = this;
    return items.slice().sort(function (a, b) {
      var va, vb;
      if (isLevelCol && hierMap) {
        var ka = a[self.identityField] || a.id;
        var kb = b[self.identityField] || b.id;
        va = (hierMap[ka] || {})[col] || '';
        vb = (hierMap[kb] || {})[col] || '';
      } else {
        va = a[col]; vb = b[col];
      }
      if (va == null) va = '';
      if (vb == null) vb = '';
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir;
      return String(va).localeCompare(String(vb)) * dir;
    });
  };

  // ── Apply column filters (Excel-style: set of selected values) ─────

  DataObjectDisplay.prototype.applyFilters = function (items, hierMap) {
    var filters = this._columnFilters;
    var keys = Object.keys(filters);
    if (!keys.length) return items;
    var self = this;
    return items.filter(function (row) {
      for (var i = 0; i < keys.length; i++) {
        var colKey = keys[i];
        var allowed = filters[colKey];
        if (!allowed || !allowed.size) continue;
        var cell;
        if (colKey.match(/^L\d+$/) && hierMap) {
          var k = row[self.identityField] || row.id;
          cell = String((hierMap[k] || {})[colKey] || '');
        } else {
          cell = String(row[colKey] != null ? row[colKey] : '');
        }
        if (!allowed.has(cell)) return false;
      }
      return true;
    });
  };

  // Collect unique values for a column across all items (unfiltered)
  DataObjectDisplay.prototype._getUniqueValues = function (col) {
    var items = (this._data && this._data.items) || [];
    var vals = {};
    items.forEach(function (row) {
      var v = String(row[col] != null ? row[col] : '');
      vals[v] = (vals[v] || 0) + 1;
    });
    return Object.keys(vals).sort(function (a, b) {
      return a.localeCompare(b, undefined, { numeric: true });
    }).map(function (v) { return { value: v, count: vals[v] }; });
  };

  // Build the Excel-style filter dropdown HTML for a column
  DataObjectDisplay.prototype._buildFilterDropdown = function (col) {
    var self = this;
    var uniqueVals = this._getUniqueValues(col);
    var searchText = (this._columnFilterSearch[col] || '').toLowerCase();
    var activeSet = this._columnFilters[col];
    var isFiltered = activeSet && activeSet.size > 0;

    var filtered = uniqueVals;
    if (searchText) {
      filtered = uniqueVals.filter(function (v) {
        return v.value.toLowerCase().indexOf(searchText) >= 0;
      });
    }

    var html = '<div class="dod-filter-dropdown" data-dod-filter-col="' + escAttr(col) + '" ' +
      'style="position:absolute;top:100%;left:0;z-index:50;min-width:200px;max-width:320px;' +
      'background:white;border:1px solid #d1d5db;border-radius:6px;box-shadow:0 4px 12px rgba(0,0,0,0.15);padding:8px">';

    // Search within filter
    html += '<input type="text" data-dod-role="filter-search" data-dod-filter-col="' + escAttr(col) + '" ' +
      'placeholder="Search values..." class="w-full border rounded px-2 py-1 text-xs mb-2" ' +
      'value="' + escAttr(this._columnFilterSearch[col] || '') + '" />';

    // Select All / Clear buttons
    html += '<div class="flex items-center gap-2 mb-2 text-[10px]">';
    html += '<button data-dod-role="filter-select-all" data-dod-filter-col="' + escAttr(col) + '" ' +
      'class="text-blue-600 hover:underline">Select All</button>';
    html += '<button data-dod-role="filter-clear" data-dod-filter-col="' + escAttr(col) + '" ' +
      'class="text-red-500 hover:underline">Clear</button>';
    if (isFiltered) {
      html += '<button data-dod-role="filter-remove" data-dod-filter-col="' + escAttr(col) + '" ' +
        'class="text-gray-500 hover:underline ml-auto">Remove Filter</button>';
    }
    html += '</div>';

    // Checkbox list
    html += '<div style="max-height:240px;overflow-y:auto" class="border-t pt-1">';
    var maxShow = 500;
    var shown = 0;
    filtered.forEach(function (v) {
      if (shown >= maxShow) return;
      shown++;
      var checked = !isFiltered || (activeSet && activeSet.has(v.value));
      html += '<label class="flex items-center gap-1.5 py-0.5 px-1 text-xs hover:bg-gray-50 rounded cursor-pointer">';
      html += '<input type="checkbox" data-dod-role="filter-cb" data-dod-filter-col="' + escAttr(col) + '" ' +
        'data-dod-filter-val="' + escAttr(v.value) + '"' + (checked ? ' checked' : '') +
        ' class="rounded border-gray-300 text-blue-600" />';
      html += '<span class="truncate flex-1">' + esc(v.value || '(empty)') + '</span>';
      html += '<span class="text-[10px] text-gray-400 flex-shrink-0">' + v.count + '</span>';
      html += '</label>';
    });
    if (filtered.length > maxShow) {
      html += '<div class="text-[10px] text-gray-400 px-1 py-1">... and ' + (filtered.length - maxShow) + ' more values</div>';
    }
    if (!filtered.length) {
      html += '<div class="text-xs text-gray-400 py-2 text-center">No matching values</div>';
    }
    html += '</div>';

    // Apply button
    html += '<div class="border-t mt-2 pt-2 flex justify-end">';
    html += '<button data-dod-role="filter-apply" data-dod-filter-col="' + escAttr(col) + '" ' +
      'class="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700">Apply</button>';
    html += '</div>';

    html += '</div>';
    return html;
  };

  // ── Render entry point ──────────────────────────────────────────────

  DataObjectDisplay.prototype.render = function () {
    var container = document.getElementById(this.containerId);
    if (!container) return;

    // Remove loading indicator
    var loadingEl = container.querySelector('[data-dod-loading]');
    if (loadingEl) loadingEl.remove();

    var data = this._data;
    if (!data) {
      container.innerHTML = '<span class="text-sm text-gray-400">Loading...</span>';
      return;
    }

    var items = data.items || [];
    if (!items.length) {
      var html = this.buildToolbarHtml();
      html += '<span class="text-gray-400 text-sm">No data found for the current scope/filters.</span>';
      container.innerHTML = html;
      this._bindToolbarEvents(container);
      return;
    }

    // Build hierMap early so filters on L-columns work
    var hierLevels = [];
    var hierMap = {};
    if (this._hierPickerId) {
      var hld = this.buildHierLevelMap();
      hierLevels = hld.levels || [];
      hierMap = hld.map || {};
    }
    // Cache for _renderTabular to reuse
    this._cachedHierLevels = hierLevels;
    this._cachedHierMap = hierMap;

    var filtered = this.applyFilters(items, hierMap);

    var html = this.buildToolbarHtml();

    if (this._view === 'tabular') {
      html += this._renderTabular(filtered);
    } else {
      html += this._renderHierarchical(filtered);
    }

    container.innerHTML = html;
    this._bindToolbarEvents(container);
  };

  // ── Tabular rendering ───────────────────────────────────────────────

  DataObjectDisplay.prototype._renderTabular = function (items) {
    var self = this;
    var cols = this.getTableColumns();

    // Reuse cached hierarchy data from render()
    var hierLevels = this._cachedHierLevels || [];
    var hierMap = this._cachedHierMap || {};

    var sorted = this.sortItems(items, hierMap);
    this._lastRenderedItems = sorted;

    // Header
    var html = '<div class="overflow-x-auto overflow-y-auto" style="max-height:calc(100vh - 480px)">';
    html += '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50">';

    // Hierarchy level headers
    hierLevels.forEach(function (lv) {
      var isLvFiltered = self._columnFilters[lv] && self._columnFilters[lv].size > 0;
      html += '<th class="py-1.5 px-2 text-left font-medium bg-amplifi-50 relative" style="position:relative">' +
        '<div class="flex items-center gap-1">' +
        '<span class="cursor-pointer flex-1" data-dod-sort="' + escAttr(lv) + '">' + esc(lv) + self._sortIcon(lv) + '</span>' +
        '<button data-dod-role="filter-toggle" data-dod-filter-col="' + escAttr(lv) + '" ' +
        'class="cursor-pointer text-xs px-1 py-0.5 rounded border border-gray-300 hover:bg-blue-100 hover:border-blue-400 leading-none' +
        (isLvFiltered ? ' bg-blue-100 text-blue-700 border-blue-400 font-bold' : ' text-gray-500 bg-white') + '" title="Filter column">&#9660;</button>' +
        '</div>';
      if (self._openFilterCol === lv) {
        html += self._buildFilterDropdown(lv);
      }
      html += '</th>';
    });

    // Data columns with Excel-style filter icon
    cols.forEach(function (col) {
      var isColFiltered = self._columnFilters[col] && self._columnFilters[col].size > 0;
      html += '<th class="py-1.5 px-2 text-left font-medium relative" style="position:relative">' +
        '<div class="flex items-center gap-1">' +
        '<span class="cursor-pointer flex-1" data-dod-sort="' + escAttr(col) + '">' +
        esc(self.colLabel(col)) + self._sortIcon(col) + '</span>' +
        '<button data-dod-role="filter-toggle" data-dod-filter-col="' + escAttr(col) + '" ' +
        'class="cursor-pointer text-xs px-1 py-0.5 rounded border border-gray-300 hover:bg-blue-100 hover:border-blue-400 leading-none' +
        (isColFiltered ? ' bg-blue-100 text-blue-700 border-blue-400 font-bold' : ' text-gray-500 bg-white') + '" title="Filter column">&#9660;</button>' +
        '</div>';
      if (self._openFilterCol === col) {
        html += self._buildFilterDropdown(col);
      }
      html += '</th>';
    });

    // Actions column header
    if (self.rowActions.length) {
      html += '<th class="py-1.5 px-2 text-left font-medium text-gray-500">Actions</th>';
    }
    html += '</tr></thead><tbody>';

    // Rows
    sorted.forEach(function (row, rowIdx) {
      var rowClass = 'border-b hover:bg-gray-50 cursor-pointer';
      if (row.is_excluded) rowClass += ' opacity-60 bg-orange-50';
      html += '<tr class="' + rowClass + '" data-dod-row-id="' + (row.id || '') + '" data-dod-row-idx="' + rowIdx + '">';

      // Hierarchy level cells
      var key = row[self.identityField] || row.id;
      var lvls = hierMap[key] || {};
      hierLevels.forEach(function (lv) {
        html += '<td class="py-1.5 px-2 font-mono text-amplifi-700 whitespace-nowrap">' + esc(lvls[lv] || '') + '</td>';
      });

      // Data cells
      var firstDataCell = true;
      cols.forEach(function (col) {
        var val = row[col];
        var display = '';
        if (col === 'is_excluded') {
          display = val ? '<span class="inline-block px-1.5 py-0.5 bg-orange-200 text-orange-800 text-[10px] font-semibold rounded">EXCLUDED</span>' : '';
        } else if (val === true) display = '<span class="text-green-600">Yes</span>';
        else if (val === false) display = '<span class="text-red-500">No</span>';
        else if (val != null) display = esc(String(val));
        // Add exclusion badge to first cell
        if (firstDataCell && row.is_excluded && col !== 'is_excluded') {
          display = '<span class="inline-block mr-1 px-1 py-0 bg-orange-200 text-orange-800 text-[9px] font-bold rounded" title="Excluded from migration">&#x26D4;</span>' + display;
        }
        firstDataCell = false;
        html += '<td class="py-1.5 px-2 whitespace-nowrap" title="' + escAttr(String(val || '')) + '">' + display + '</td>';
      });

      // Action buttons
      if (self.rowActions.length) {
        html += '<td class="py-1.5 px-2 whitespace-nowrap">';
        self.rowActions.forEach(function (act, actIdx) {
          html += '<button data-dod-role="row-action" data-dod-action-idx="' + actIdx + '" data-dod-row-idx="' + rowIdx + '" ' +
            'class="' + (act.className || 'text-xs px-2 py-0.5 rounded border border-gray-300 hover:bg-gray-100 mr-1') + '" ' +
            'title="' + escAttr(act.title || act.label) + '">' + esc(act.label) + '</button>';
        });
        html += '</td>';
      }

      html += '</tr>';
    });

    html += '</tbody></table></div>';

    // Pagination
    if (this.showPagination && this._data.total) {
      html += this._renderPager();
    }

    return html;
  };

  // ── Sort icon helper ────────────────────────────────────────────────

  DataObjectDisplay.prototype._sortIcon = function (col) {
    if (this._sort.col !== col) return '';
    return this._sort.dir === 'asc' ? ' &#9650;' : ' &#9660;';
  };

  // ── Pagination HTML ─────────────────────────────────────────────────

  DataObjectDisplay.prototype._renderPager = function () {
    var total = this._data.total || 0;
    var pages = Math.ceil(total / this.pageSize);
    if (pages <= 1) return '';

    var html = '<div class="flex items-center justify-between mt-3 text-sm">';
    html += '<span class="text-xs text-gray-500">' + fmt(total) + ' total</span>';
    html += '<div class="flex items-center gap-1">';
    if (this._page > 1) {
      html += '<button data-dod-page="' + (this._page - 1) + '" class="btn-sm text-xs">← Prev</button>';
    }
    html += '<span class="text-xs text-gray-500">Page ' + this._page + ' of ' + pages + '</span>';
    if (this._page < pages) {
      html += '<button data-dod-page="' + (this._page + 1) + '" class="btn-sm text-xs">Next →</button>';
    }
    html += '</div></div>';
    return html;
  };

  // ── Hierarchical view rendering ─────────────────────────────────────

  DataObjectDisplay.prototype._renderHierarchical = function (items) {
    var self = this;

    // For GL artificial hierarchies
    if (this._hierPickerId === '__gl_type_a' || this._hierPickerId === '__gl_type_b') {
      return this._renderGLHierarchical(items);
    }

    // Prefer /tree endpoint data (enriched); fall back to inline hierarchy data.
    var hierData = this._hierData;
    if (!hierData && this._hierPickerId && this._data && this._data.hierarchies) {
      var inlineHier = this._data.hierarchies.filter(function (h) { return h.id === self._hierPickerId; })[0];
      if (inlineHier && inlineHier.nodes) hierData = inlineHier;
    }
    if (!this._hierPickerId || !hierData) {
      if (this._hierPickerId && !hierData) {
        return '<span class="text-gray-400 text-sm"><svg class="animate-spin inline h-4 w-4 mr-1 text-blue-500" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>Loading hierarchy tree...</span>';
      }
      return '<span class="text-gray-400 text-sm">Select a hierarchy from the picker to view hierarchical display.</span>';
    }

    var setclass = hierData.setclass || '';
    var normSetclass = normaliseSetclass(setclass);

    // ── Strategy: Use /tree endpoint's nested tree if available ──
    // The tree endpoint returns enriched nodes with leaf_count and items
    // (with name lookups). This avoids needing to load ALL data items.
    if (hierData.tree && hierData.tree.length) {
      return this._renderHierFromTree(hierData, items);
    }

    // ── Fallback: inline hierarchy mode (Data Browser) ──
    // Build leaf-items map from loaded items
    var leafItemsMap = {};
    if (normSetclass === '0106') {
      items.forEach(function (it) {
        var key = it[self.entityField];
        if (!key) return;
        if (!leafItemsMap[key]) leafItemsMap[key] = [];
        leafItemsMap[key].push(it);
      });
    } else if (normSetclass === '0104') {
      items.forEach(function (it) {
        var key = it[self.profitCenterField];
        if (!key) return;
        if (!leafItemsMap[key]) leafItemsMap[key] = [];
        leafItemsMap[key].push(it);
      });
    } else {
      items.forEach(function (it) {
        var key = it[self.identityField];
        if (!key) return;
        leafItemsMap[key] = [it];
      });
    }

    // Build child map from flat nodes/leaves
    var childMap = {};
    var nodes = hierData.nodes || [];
    var leaves = hierData.leaves || [];

    nodes.forEach(function (n) {
      var parent = n.parent_setname || n.parent;
      var child = n.child_setname || n.child;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'node', name: child, seq: n.seq || 0 });
    });
    leaves.forEach(function (lf) {
      var parent = lf.setname || lf.parent;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'leaf', value: lf.value, seq: lf.seq || 0 });
    });

    // Find roots
    var allChildren = {};
    nodes.forEach(function (n) { allChildren[n.child_setname || n.child] = 1; });
    var roots = [];
    nodes.forEach(function (n) {
      var parent = n.parent_setname || n.parent;
      if (!allChildren[parent] && roots.indexOf(parent) < 0) roots.push(parent);
    });
    if (!roots.length && nodes.length) {
      roots.push(nodes[0].parent_setname || nodes[0].parent);
    }

    function countLeaves(nodeName) {
      var ch = childMap[nodeName] || [];
      var cnt = 0;
      ch.forEach(function (c) {
        if (c.type === 'leaf') cnt += (leafItemsMap[c.value] || []).length;
        else cnt += countLeaves(c.name);
      });
      return cnt;
    }

    var html = '<div class="flex gap-3" style="height:calc(100vh - 480px);min-height:200px">';
    html += '<div class="w-80 flex-shrink-0 border rounded bg-white overflow-auto">';

    var idSeq = 0;
    function buildTree(nodeName, depth) {
      idSeq++;
      var nodeId = self.containerId + '-tn-' + idSeq;
      var pad = depth * 12;
      var lc = countLeaves(nodeName);
      var isSelected = self._selectedHierNode === nodeName;
      var isExpanded = self._allExpanded || self._expandedNodes[nodeName] !== false;

      var out = '<div style="padding-left:' + pad + 'px" class="py-0.5 flex items-center gap-1">';
      out += '<span class="text-[10px] text-gray-400 cursor-pointer select-none" data-dod-toggle="' + escAttr(nodeId) + '">' +
        (isExpanded ? '&#9660;' : '&#9654;') + '</span>';
      out += '<span data-dod-hier-node="' + escAttr(nodeName) + '" class="text-xs cursor-pointer hover:text-blue-600 px-1 rounded' +
        (isSelected ? ' font-bold text-blue-700 bg-blue-50' : ' text-gray-800') + '">';
      out += esc(nodeName) + ' <span class="text-[10px] text-gray-400">(' + lc + ')</span></span>';
      if (self.rowActions.length) {
        out += '<button data-dod-role="node-add" data-dod-node-name="' + escAttr(nodeName) + '" class="text-[10px] px-1 py-0 rounded border border-gray-300 text-gray-500 hover:bg-blue-50 hover:text-blue-600 ml-1" title="Add new under this node">+</button>';
      }
      out += '</div>';

      var children = childMap[nodeName] || [];
      children.sort(function (a, b) { return (a.seq || 0) - (b.seq || 0); });
      out += '<div id="' + escAttr(nodeId) + '" class="tree-children' + (isExpanded ? '' : ' hidden') + '">';

      children.forEach(function (c) {
        if (c.type === 'node') {
          out += buildTree(c.name, depth + 1);
        } else {
          var leafItems = leafItemsMap[c.value] || [];
          var leafPad = (depth + 1) * 12;
          if (leafItems.length) {
            leafItems.forEach(function (item) {
              var isLeafSelected = self._selectedHierNode === ('__leaf__' + c.value);
              out += '<div style="padding-left:' + leafPad + 'px" class="py-0.5">' +
                '<span data-dod-hier-node="__leaf__' + escAttr(c.value) + '" class="text-xs cursor-pointer hover:text-blue-600 px-1 rounded font-medium' +
                (isLeafSelected ? ' text-blue-700 bg-blue-50' : ' text-gray-700') + '">' +
                esc(item[self.identityField] || c.value) +
                (item.txtsh ? ' — ' + esc(item.txtsh) : (item.name ? ' — ' + esc(item.name) : '')) +
                '</span></div>';
            });
          } else {
            out += '<div style="padding-left:' + leafPad + 'px" class="py-0.5">' +
              '<span class="text-xs text-gray-400 px-1">' + esc(c.value) + '</span></div>';
          }
        }
      });
      out += '</div>';
      return out;
    }

    roots.forEach(function (root) { html += buildTree(root, 0); });

    // Unassigned items
    var assigned = {};
    leaves.forEach(function (lf) { assigned[lf.value] = 1; });
    var unassignedItems;
    if (normSetclass === '0106') {
      unassignedItems = items.filter(function (it) { return it[self.entityField] && !assigned[it[self.entityField]]; });
    } else if (normSetclass === '0104') {
      unassignedItems = items.filter(function (it) { return it[self.profitCenterField] && !assigned[it[self.profitCenterField]]; });
    } else {
      unassignedItems = items.filter(function (it) { return it[self.identityField] && !assigned[it[self.identityField]]; });
    }
    if (unassignedItems.length) {
      var isUA = self._selectedHierNode === '__unassigned__';
      html += '<div data-dod-hier-node="__unassigned__" class="mt-2 pt-2 border-t px-2 py-1 cursor-pointer hover:bg-gray-50' +
        (isUA ? ' bg-blue-50 font-bold text-blue-700' : ' text-gray-500') + '">' +
        'Unassigned (' + unassignedItems.length + ')</div>';
    }

    html += '</div>';

    // Right panel — detail table
    html += '<div class="flex-1 border rounded bg-white overflow-auto" id="' + self.containerId + '-hier-detail">';

    // Show detail for selected node
    if (this._selectedHierNode) {
      html += this._renderHierDetail(items, leafItemsMap, childMap, leaves);
    } else {
      html += '<div class="flex items-center justify-center h-full text-gray-400 text-sm">Select a node in the tree to view details.</div>';
    }

    html += '</div></div>';
    return html;
  };

  // ── Hierarchical detail panel ───────────────────────────────────────

  DataObjectDisplay.prototype._renderHierDetail = function (items, leafItemsMap, childMap, leaves) {
    var self = this;
    var cols = this.getTableColumns();
    var selected = this._selectedHierNode;

    var detailItems = [];

    if (selected === '__unassigned__') {
      var assigned = {};
      leaves.forEach(function (lf) { assigned[lf.value] = 1; });
      var setclass = normaliseSetclass((this._hierData && this._hierData.setclass) || '');
      if (setclass === '0106') {
        detailItems = items.filter(function (it) { return it[self.entityField] && !assigned[it[self.entityField]]; });
      } else if (setclass === '0104') {
        detailItems = items.filter(function (it) { return it[self.profitCenterField] && !assigned[it[self.profitCenterField]]; });
      } else {
        detailItems = items.filter(function (it) { return it[self.identityField] && !assigned[it[self.identityField]]; });
      }
    } else if (selected && selected.indexOf('__leaf__') === 0) {
      var leafVal = selected.substring(8);
      detailItems = leafItemsMap[leafVal] || [];
    } else if (selected) {
      // Collect all leaves under this node recursively
      function collectLeaves(nodeName) {
        var result = [];
        var ch = childMap[nodeName] || [];
        ch.forEach(function (c) {
          if (c.type === 'leaf') {
            var li = leafItemsMap[c.value] || [];
            result = result.concat(li);
          } else {
            result = result.concat(collectLeaves(c.name));
          }
        });
        return result;
      }
      detailItems = collectLeaves(selected);
    }

    if (!detailItems.length) {
      return '<div class="flex items-center justify-center h-full text-gray-400 text-sm">No items under this node.</div>';
    }

    var html = '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50">';
    cols.forEach(function (col) {
      html += '<th class="py-1.5 px-2 text-left font-medium">' + esc(self.colLabel(col)) + '</th>';
    });
    html += '</tr></thead><tbody>';

    detailItems.forEach(function (row) {
      html += '<tr class="border-b hover:bg-gray-50">';
      cols.forEach(function (col) {
        var val = row[col];
        var display = '';
        if (val === true) display = '<span class="text-green-600">Yes</span>';
        else if (val === false) display = '<span class="text-red-500">No</span>';
        else if (val != null) display = esc(String(val));
        html += '<td class="py-1.5 px-2">' + display + '</td>';
      });
      html += '</tr>';
    });

    html += '</tbody></table>';
    return html;
  };

  // ── Hierarchical view from /tree endpoint (enriched nodes) ──────────

  DataObjectDisplay.prototype._renderHierFromTree = function (hierData, items) {
    var self = this;
    var treeNodes = hierData.tree || [];

    // Split panel: tree left, detail right
    var html = '<div class="flex gap-3" style="height:calc(100vh - 480px);min-height:200px">';
    html += '<div class="w-80 flex-shrink-0 border rounded bg-white overflow-auto">';

    var idSeq = 0;

    function countAll(node) {
      var cnt = node.leaf_count || (node.items || []).length;
      (node.children || []).forEach(function (c) { cnt += countAll(c); });
      return cnt;
    }

    function buildNode(node, depth) {
      idSeq++;
      var nodeId = self.containerId + '-tt-' + idSeq;
      var pad = depth * 12;
      var lc = countAll(node);
      var hasChildren = (node.children && node.children.length) || (node.items && node.items.length);
      var isSelected = self._selectedHierNode === node.setname;
      var isExpanded = self._allExpanded || self._expandedNodes[node.setname] !== false;

      var out = '<div style="padding-left:' + pad + 'px" class="py-0.5 flex items-center gap-1">';
      if (hasChildren) {
        out += '<span class="text-[10px] text-gray-400 cursor-pointer select-none" data-dod-toggle="' + escAttr(nodeId) + '">' +
          (isExpanded ? '&#9660;' : '&#9654;') + '</span>';
      } else {
        out += '<span class="text-[10px] w-3"></span>';
      }
      out += '<span data-dod-hier-node="' + escAttr(node.setname) + '" class="text-xs cursor-pointer hover:text-blue-600 px-1 rounded' +
        (isSelected ? ' font-bold text-blue-700 bg-blue-50' : ' text-gray-800') + '">';
      out += esc(node.setname) + ' <span class="text-[10px] text-gray-400">(' + lc + ')</span></span></div>';

      out += '<div id="' + escAttr(nodeId) + '" class="tree-children' + (isExpanded ? '' : ' hidden') + '">';

      // Child nodes
      (node.children || []).forEach(function (c) { out += buildNode(c, depth + 1); });

      // Leaf items directly under this node
      (node.items || []).forEach(function (item) {
        var leafPad = (depth + 1) * 12;
        var leafKey = item.value || item.id_field || '';
        var isLeafSelected = self._selectedHierNode === ('__leaf__' + leafKey);
        out += '<div style="padding-left:' + leafPad + 'px" class="py-0.5">' +
          '<span data-dod-hier-node="__leaf__' + escAttr(leafKey) + '" class="text-xs cursor-pointer hover:text-blue-600 px-1 rounded font-medium' +
          (isLeafSelected ? ' text-blue-700 bg-blue-50' : ' text-gray-700') + '">' +
          esc(leafKey) + (item.name ? ' — ' + esc(item.name) : '') +
          '</span></div>';
      });

      out += '</div>';
      return out;
    }

    treeNodes.forEach(function (root) { html += buildNode(root, 0); });
    html += '</div>';

    // Right panel — detail table
    html += '<div class="flex-1 border rounded bg-white overflow-auto">';
    if (this._selectedHierNode) {
      html += this._renderHierTreeDetail(hierData, items);
    } else {
      html += '<div class="flex items-center justify-center h-full text-gray-400 text-sm">Select a node in the tree to view details.</div>';
    }
    html += '</div></div>';
    return html;
  };

  // Detail panel for tree-based hierarchical view
  DataObjectDisplay.prototype._renderHierTreeDetail = function (hierData, items) {
    var self = this;
    var selected = this._selectedHierNode;
    var cols = this.getTableColumns();

    // Find the selected node in the tree and collect all leaf items under it
    function findNode(nodes, name) {
      for (var i = 0; i < nodes.length; i++) {
        if (nodes[i].setname === name) return nodes[i];
        var found = findNode(nodes[i].children || [], name);
        if (found) return found;
      }
      return null;
    }

    function collectItems(node) {
      var result = (node.items || []).slice();
      (node.children || []).forEach(function (c) {
        result = result.concat(collectItems(c));
      });
      return result;
    }

    function collectLeafValues(node) {
      var vals = (node.items || []).map(function (x) { return x.value || x.id_field || ''; });
      (node.children || []).forEach(function (c) {
        vals = vals.concat(collectLeafValues(c));
      });
      return vals;
    }

    var treeLeafItems = [];
    var leafValues = [];
    if (selected && selected.indexOf('__leaf__') === 0) {
      var leafVal = selected.substring(8);
      leafValues = [leafVal];
      function findLeaf(nodes) {
        for (var i = 0; i < nodes.length; i++) {
          var node = nodes[i];
          var it = (node.items || []).filter(function (x) { return (x.value || x.id_field) === leafVal; });
          if (it.length) return it;
          var found = findLeaf(node.children || []);
          if (found) return found;
        }
        return null;
      }
      treeLeafItems = findLeaf(hierData.tree || []) || [];
    } else if (selected) {
      var node = findNode(hierData.tree || [], selected);
      if (node) {
        treeLeafItems = collectItems(node);
        leafValues = collectLeafValues(node);
      }
    }

    // Cross-reference loaded data items with leaf values for richer detail.
    var idField = this.identityField || 'cctr';
    var entityField = this.entityField;
    var matchedItems = [];
    if (leafValues.length && items && items.length) {
      var leafSet = {};
      leafValues.forEach(function (v) { leafSet[v] = true; });
      matchedItems = items.filter(function (row) {
        if (leafSet[row[idField]]) return true;
        if (entityField && leafSet[row[entityField]]) return true;
        return false;
      });
    }

    // Always fetch full items from the data endpoint when a node is selected.
    // Tree leaf items only have basic fields — the endpoint returns full detail.
    if (leafValues.length && this.dataEndpoint) {
      // If we already have full matches for all leaves, use them directly
      if (matchedItems.length >= leafValues.length) {
        // All items matched — skip fetch, use matched items below
      } else {
        // Need to fetch full data
        var loadingHtml = '<div class="flex items-center justify-center h-full text-gray-400 text-sm">' +
          '<svg class="animate-spin inline h-4 w-4 mr-1 text-blue-500" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4" fill="none"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>' +
          'Loading ' + leafValues.length + ' items...</div>';
        this._fetchNodeItems(leafValues, treeLeafItems);
        return loadingHtml;
      }
    }

    // Prefer loaded data items if they exist (they have more detail), else use tree items
    var detailItems = matchedItems.length ? matchedItems : treeLeafItems;

    if (!detailItems.length) {
      return '<div class="flex items-center justify-center h-full text-gray-400 text-sm">No items under this node.</div>';
    }

    // Build detail table columns
    var detailCols = [];
    var first = detailItems[0];
    Object.keys(first).forEach(function (k) {
      if (k !== 'levels' && k !== 'monthly_balances') detailCols.push(k);
    });
    // Use configured cols if they exist and match, otherwise use auto-discovered
    if (cols.length) {
      var itemCols = Object.keys(first);
      var overlap = cols.filter(function (c) { return itemCols.indexOf(c) >= 0; });
      if (overlap.length) detailCols = overlap;
    }
    // Apply excludeColumns
    var excl = this.excludeColumns;
    if (excl.length) {
      detailCols = detailCols.filter(function (c) { return excl.indexOf(c) < 0; });
    }

    var html = '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50">';
    detailCols.forEach(function (col) {
      html += '<th class="py-1.5 px-2 text-left font-medium whitespace-nowrap">' + esc(self.colLabel(col)) + '</th>';
    });
    if (self.rowActions.length) {
      html += '<th class="py-1.5 px-2 text-left font-medium whitespace-nowrap">Actions</th>';
    }
    html += '</tr></thead><tbody>';

    detailItems.forEach(function (row, rowIdx) {
      html += '<tr class="border-b hover:bg-gray-50" data-dod-row-idx="' + rowIdx + '">';
      detailCols.forEach(function (col) {
        var val = row[col];
        var display = '';
        if (val === true) display = '<span class="text-green-600">Yes</span>';
        else if (val === false) display = '<span class="text-red-500">No</span>';
        else if (val != null) display = esc(String(val));
        html += '<td class="py-1.5 px-2 whitespace-nowrap">' + display + '</td>';
      });
      if (self.rowActions.length) {
        html += '<td class="py-1.5 px-2 whitespace-nowrap">';
        self.rowActions.forEach(function (act, actIdx) {
          html += '<button data-dod-role="row-action" data-dod-action-idx="' + actIdx + '" data-dod-row-idx="' + rowIdx + '" ' +
            'class="' + (act.className || 'text-xs px-2 py-0.5 rounded border border-gray-300 hover:bg-gray-100 mr-1') + '" ' +
            'title="' + escAttr(act.title || act.label) + '">' + esc(act.label) + '</button>';
        });
        html += '</td>';
      }
      html += '</tr>';
    });

    html += '</tbody></table>';
    // Store detail items for row-action click resolution
    self._hierDetailItems = detailItems;
    return html;
  };

  // ── Fetch items for selected hierarchy node on demand ────────────────

  DataObjectDisplay.prototype._fetchNodeItems = function (leafValues, fallbackItems) {
    var self = this;
    // Avoid duplicate fetches for the same selection
    var cacheKey = leafValues.slice().sort().join(',');
    if (this._lastNodeFetchKey === cacheKey) return;
    this._lastNodeFetchKey = cacheKey;

    // Build search query from leaf values (batch in chunks to avoid URL length issues)
    // Use a POST-style approach via search param with comma-separated values
    var params = [];
    params.push('page=1');
    // Limit to a reasonable batch — fetch up to 500 items for the selected node
    params.push('size=500');
    if (leafValues.length <= 200) {
      params.push('search_values=' + encodeURIComponent(leafValues.join(',')));
    } else {
      // Too many values — use search to narrow by the first few unique prefixes
      // Fallback: just use a large page fetch without filter
      params.push('size=5000');
    }

    // Add scope/category params if present
    var eq = this.extraQueryParams;
    Object.keys(eq).forEach(function (k) {
      if (eq[k] != null && eq[k] !== '') params.push(k + '=' + encodeURIComponent(eq[k]));
    });

    var fetchFn = window.apiFetch || fetch;
    fetchFn(this.apiBase + this.dataEndpoint + '?' + params.join('&'), {
      headers: this.authHeaders,
    })
      .then(function (r) { return r.json(); })
      .then(function (d) {
        var fetchedItems = d.items || [];
        if (fetchedItems.length) {
          // Filter to only items matching our leaf values
          var leafSet = {};
          leafValues.forEach(function (v) { leafSet[v] = true; });
          var idField = self.identityField || 'cctr';
          var entityField = self.entityField;
          var matched = fetchedItems.filter(function (row) {
            if (leafSet[row[idField]]) return true;
            if (entityField && leafSet[row[entityField]]) return true;
            return false;
          });
          if (matched.length) {
            self._nodeDetailCache = matched;
          } else {
            self._nodeDetailCache = fetchedItems.length ? fetchedItems : fallbackItems;
          }
        } else {
          self._nodeDetailCache = fallbackItems;
        }
        // Re-render the detail panel only
        self._updateDetailPanel();
      })
      .catch(function () {
        self._nodeDetailCache = fallbackItems;
        self._updateDetailPanel();
      });
  };

  DataObjectDisplay.prototype._updateDetailPanel = function () {
    var container = document.getElementById(this.containerId);
    if (!container) return;
    // Find the detail panel — it's the second child of the flex container
    var detailEl = container.querySelector('.flex-1.border.rounded.bg-white.overflow-auto');
    if (!detailEl) return;

    var items = this._nodeDetailCache || [];
    if (!items.length) {
      detailEl.innerHTML = '<div class="flex items-center justify-center h-full text-gray-400 text-sm">No items under this node.</div>';
      return;
    }

    var self = this;
    var cols = this.getTableColumns();
    // Build detail table columns
    var detailCols = [];
    var first = items[0];
    Object.keys(first).forEach(function (k) {
      if (k !== 'levels' && k !== 'monthly_balances') detailCols.push(k);
    });
    if (cols.length) {
      var itemCols = Object.keys(first);
      var overlap = cols.filter(function (c) { return itemCols.indexOf(c) >= 0; });
      if (overlap.length) detailCols = overlap;
    }
    var excl = this.excludeColumns;
    if (excl.length) {
      detailCols = detailCols.filter(function (c) { return excl.indexOf(c) < 0; });
    }

    var html = '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50">';
    detailCols.forEach(function (col) {
      html += '<th class="py-1.5 px-2 text-left font-medium whitespace-nowrap">' + esc(self.colLabel(col)) + '</th>';
    });
    if (self.rowActions.length) {
      html += '<th class="py-1.5 px-2 text-left font-medium whitespace-nowrap">Actions</th>';
    }
    html += '</tr></thead><tbody>';

    items.forEach(function (row, rowIdx) {
      html += '<tr class="border-b hover:bg-gray-50" data-dod-row-idx="' + rowIdx + '">';
      detailCols.forEach(function (col) {
        var val = row[col];
        var display = '';
        if (val === true) display = '<span class="text-green-600">Yes</span>';
        else if (val === false) display = '<span class="text-red-500">No</span>';
        else if (val != null) display = esc(String(val));
        html += '<td class="py-1.5 px-2 whitespace-nowrap">' + display + '</td>';
      });
      if (self.rowActions.length) {
        html += '<td class="py-1.5 px-2 whitespace-nowrap">';
        self.rowActions.forEach(function (act, actIdx) {
          html += '<button data-dod-role="row-action" data-dod-action-idx="' + actIdx + '" data-dod-row-idx="' + rowIdx + '" ' +
            'class="' + (act.className || 'text-xs px-2 py-0.5 rounded border border-gray-300 hover:bg-gray-100 mr-1') + '" ' +
            'title="' + escAttr(act.title || act.label) + '">' + esc(act.label) + '</button>';
        });
        html += '</td>';
      }
      html += '</tr>';
    });
    html += '</tbody></table>';
    // Store detail items for row-action resolution
    self._hierDetailItems = items;
    detailEl.innerHTML = html;

    // Bind row action listeners on the newly rendered detail panel
    if (self.rowActions.length) {
      detailEl.querySelectorAll('[data-dod-role="row-action"]').forEach(function (btn) {
        btn.addEventListener('click', function (e) {
          e.stopPropagation();
          var actIdx = parseInt(this.dataset.dodActionIdx, 10);
          var rowIdx = parseInt(this.dataset.dodRowIdx, 10);
          var act = self.rowActions[actIdx];
          var row = items[rowIdx];
          if (act && act.onclick && row) {
            act.onclick(row, self);
          }
        });
      });
    }
  };

  // ── GL artificial hierarchical view ─────────────────────────────────

  DataObjectDisplay.prototype._renderGLHierarchical = function (items) {
    var self = this;
    var isTypeA = this._hierPickerId === '__gl_type_a';
    var field = 'saknr';

    // Group items by prefix
    var groups = {};
    var groupOrder = [];
    items.forEach(function (it) {
      var acct = String(it[field] || '');
      var key = isTypeA ? (acct.charAt(0) || '?') : (acct.substring(0, 5) || '?????');
      if (!groups[key]) {
        groups[key] = [];
        groupOrder.push(key);
      }
      groups[key].push(it);
    });
    groupOrder.sort();

    var cols = this.getTableColumns();

    var html = '<div class="overflow-y-auto" style="max-height:calc(100vh - 480px)">';

    groupOrder.forEach(function (key) {
      var groupItems = groups[key];
      var nodeId = self.containerId + '-gl-' + escAttr(key);
      var isExpanded = self._allExpanded || self._expandedNodes[key] !== false;

      html += '<div class="border-b">';
      html += '<div class="flex items-center gap-2 px-3 py-2 bg-gray-50 cursor-pointer" data-dod-gl-toggle="' + escAttr(nodeId) + '">';
      html += '<span class="text-[10px] text-gray-400 dod-arrow">' + (isExpanded ? '&#9660;' : '&#9654;') + '</span>';
      html += '<span class="text-sm font-medium text-gray-800">' + esc(key) + '</span>';
      html += '<span class="text-xs text-gray-400">(' + groupItems.length + ' accounts)</span>';
      html += '</div>';

      html += '<div id="' + escAttr(nodeId) + '"' + (isExpanded ? '' : ' class="hidden"') + '>';
      html += '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50/50">';
      cols.forEach(function (col) {
        html += '<th class="py-1 px-2 text-left font-medium text-gray-600">' + esc(self.colLabel(col)) + '</th>';
      });
      html += '</tr></thead><tbody>';
      groupItems.forEach(function (row) {
        html += '<tr class="border-b hover:bg-gray-50">';
        cols.forEach(function (col) {
          var val = row[col];
          html += '<td class="py-1 px-2">' + esc(String(val || '')) + '</td>';
        });
        html += '</tr>';
      });
      html += '</tbody></table></div></div>';
    });

    html += '</div>';
    return html;
  };

  // ── Event binding ───────────────────────────────────────────────────

  DataObjectDisplay.prototype._bindToolbarEvents = function (container) {
    var self = this;

    // View toggle
    var tabBtn = container.querySelector('[data-dod-role="view-tabular"]');
    var hierBtn = container.querySelector('[data-dod-role="view-hierarchy"]');
    if (tabBtn) tabBtn.addEventListener('click', function () {
      self._view = 'tabular';
      self._page = 1;
      // Re-fetch with normal page size
      self.loadData(function () { self.render(); });
    });
    if (hierBtn) hierBtn.addEventListener('click', function () {
      self._view = 'hierarchy';
      self._page = 1;
      self.loadData(function () {
        // Always load tree data for hierarchical view when a hierarchy is selected
        if (self._hierPickerId && typeof self._hierPickerId === 'number') {
          self.loadHierarchyTree(self._hierPickerId, function () { self.render(); });
        } else {
          self.render();
        }
      });
    });

    // Search
    var searchEl = container.querySelector('[data-dod-role="search"]');
    if (searchEl) {
      var timer;
      searchEl.addEventListener('input', function () {
        clearTimeout(timer);
        var val = this.value;
        timer = setTimeout(function () {
          self._search = val;
          self._page = 1;
          self.loadData(function () { self.render(); });
        }, 300);
      });
    }

    // Hierarchy picker
    var hierPicker = container.querySelector('[data-dod-role="hier-picker"]');
    if (hierPicker) {
      // Set current value
      if (self._hierPickerId) hierPicker.value = String(self._hierPickerId);

      hierPicker.addEventListener('change', function () {
        var val = this.value;
        if (val === '' || val === null) {
          self._hierPickerId = null;
          self._hierData = null;
        } else if (val.indexOf('__gl_') === 0) {
          self._hierPickerId = val;
          self._hierData = null;
        } else {
          self._hierPickerId = parseInt(val, 10);
          self._hierData = null;
        }

        // Let caller react to hierarchy changes (e.g. Hierarchies tab switches endpoint)
        if (self.onHierarchyChange) self.onHierarchyChange(self._hierPickerId);

        // Always re-fetch data (for hierarchy_id param or inline hierarchies)
        self.loadData(function () {
          // Always load tree data when in hierarchical view with a selected hierarchy
          if (self._view === 'hierarchy' && self._hierPickerId && typeof self._hierPickerId === 'number') {
            self.loadHierarchyTree(self._hierPickerId, function () { self.render(); });
          } else {
            self.render();
          }
        });
      });
    }

    // Sorting
    container.querySelectorAll('[data-dod-sort]').forEach(function (th) {
      th.addEventListener('click', function (e) {
        if (e.target.tagName === 'INPUT' || e.target.tagName === 'BUTTON' || e.target.closest('[data-dod-role="filter-toggle"]')) return;
        var col = this.dataset.dodSort;
        if (self._sort.col === col) {
          self._sort.dir = self._sort.dir === 'asc' ? 'desc' : 'asc';
        } else {
          self._sort = { col: col, dir: 'asc' };
        }
        self.render();
      });
    });

    // Excel-style filter toggle (open/close dropdown)
    container.querySelectorAll('[data-dod-role="filter-toggle"]').forEach(function (el) {
      el.addEventListener('click', function (e) {
        e.stopPropagation();
        var col = this.dataset.dodFilterCol;
        if (self._openFilterCol === col) {
          self._openFilterCol = null;
        } else {
          self._openFilterCol = col;
        }
        self.render();
        // Focus the search input in the newly opened dropdown
        if (self._openFilterCol) {
          var searchInput = document.querySelector('[data-dod-role="filter-search"][data-dod-filter-col="' + col + '"]');
          if (searchInput) searchInput.focus();
        }
      });
    });

    // Filter search within dropdown
    container.querySelectorAll('[data-dod-role="filter-search"]').forEach(function (inp) {
      inp.addEventListener('input', function (e) {
        e.stopPropagation();
        self._columnFilterSearch[this.dataset.dodFilterCol] = this.value;
        // Re-render just the dropdown by re-rendering the whole thing
        self.render();
        // Re-focus
        var col = this.dataset.dodFilterCol;
        var newInp = document.querySelector('[data-dod-role="filter-search"][data-dod-filter-col="' + col + '"]');
        if (newInp) { newInp.focus(); newInp.selectionStart = newInp.selectionEnd = newInp.value.length; }
      });
      inp.addEventListener('click', function (e) { e.stopPropagation(); });
    });

    // Filter Select All button
    container.querySelectorAll('[data-dod-role="filter-select-all"]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var col = this.dataset.dodFilterCol;
        var dropdown = container.querySelector('.dod-filter-dropdown[data-dod-filter-col="' + col + '"]');
        if (dropdown) {
          dropdown.querySelectorAll('[data-dod-role="filter-cb"]').forEach(function (cb) {
            cb.checked = true;
          });
        }
      });
    });

    // Filter Clear button (uncheck all)
    container.querySelectorAll('[data-dod-role="filter-clear"]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var col = this.dataset.dodFilterCol;
        var dropdown = container.querySelector('.dod-filter-dropdown[data-dod-filter-col="' + col + '"]');
        if (dropdown) {
          dropdown.querySelectorAll('[data-dod-role="filter-cb"]').forEach(function (cb) {
            cb.checked = false;
          });
        }
      });
    });

    // Filter Remove (clear filter entirely for this column)
    container.querySelectorAll('[data-dod-role="filter-remove"]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var col = this.dataset.dodFilterCol;
        delete self._columnFilters[col];
        delete self._columnFilterSearch[col];
        self._openFilterCol = null;
        self.render();
      });
    });

    // Filter Apply — collect checked values and apply
    container.querySelectorAll('[data-dod-role="filter-apply"]').forEach(function (btn) {
      btn.addEventListener('click', function (e) {
        e.stopPropagation();
        var col = this.dataset.dodFilterCol;
        var dropdown = container.querySelector('.dod-filter-dropdown[data-dod-filter-col="' + col + '"]');
        if (!dropdown) return;
        var selected = new Set();
        dropdown.querySelectorAll('[data-dod-role="filter-cb"]:checked').forEach(function (cb) {
          selected.add(cb.dataset.dodFilterVal);
        });
        // If all values are selected, treat as no filter
        var allVals = self._getUniqueValues(col);
        if (selected.size >= allVals.length) {
          delete self._columnFilters[col];
        } else if (selected.size === 0) {
          // Empty selection = show nothing (keep as filter)
          self._columnFilters[col] = selected;
        } else {
          self._columnFilters[col] = selected;
        }
        self._openFilterCol = null;
        delete self._columnFilterSearch[col];
        self.render();
      });
    });

    // Filter checkbox clicks should not close dropdown
    container.querySelectorAll('[data-dod-role="filter-cb"]').forEach(function (cb) {
      cb.addEventListener('click', function (e) { e.stopPropagation(); });
    });

    // Clear All Filters button
    var clearAllBtn = container.querySelector('[data-dod-role="clear-all-filters"]');
    if (clearAllBtn) clearAllBtn.addEventListener('click', function () {
      self._columnFilters = {};
      self._columnFilterSearch = {};
      self._openFilterCol = null;
      self.render();
    });

    // Close filter dropdown when clicking outside
    var closeHandler = function (e) {
      if (self._openFilterCol && !e.target.closest('.dod-filter-dropdown') && !e.target.closest('[data-dod-role="filter-toggle"]')) {
        self._openFilterCol = null;
        self.render();
        document.removeEventListener('click', closeHandler);
      }
    };
    if (self._openFilterCol) {
      setTimeout(function () { document.addEventListener('click', closeHandler); }, 0);
    }

    // Custom toolbar buttons
    this.toolbarButtons.forEach(function (btn, i) {
      var el = container.querySelector('[data-dod-role="toolbar-btn-' + i + '"]');
      if (el && btn.onclick) {
        el.addEventListener('click', function () { btn.onclick(self); });
      }
    });

    // Row action buttons (tabular + hierarchical detail)
    if (self.rowActions.length) {
      container.querySelectorAll('[data-dod-role="row-action"]').forEach(function (btn) {
        btn.addEventListener('click', function (e) {
          e.stopPropagation();
          var actIdx = parseInt(this.dataset.dodActionIdx, 10);
          var rowIdx = parseInt(this.dataset.dodRowIdx, 10);
          var act = self.rowActions[actIdx];
          var items = (self._view === 'hierarchical' && self._hierDetailItems && self._hierDetailItems.length)
            ? self._hierDetailItems
            : (self._lastRenderedItems || (self._data ? self._data.items : []) || []);
          var row = items[rowIdx];
          if (act && act.onclick && row) {
            act.onclick(row, self);
          }
        });
      });
      // Node-add buttons (hierarchical view)
      container.querySelectorAll('[data-dod-role="node-add"]').forEach(function (btn) {
        btn.addEventListener('click', function (e) {
          e.stopPropagation();
          var nodeName = this.dataset.dodNodeName;
          var act = self.rowActions[0]; // First action = "Add"
          if (act && act.onclick) {
            act.onclick({ _hierNode: nodeName }, self);
          }
        });
      });
    }

    // Extra filter widgets
    container.querySelectorAll('[data-dod-role="extra-filter"]').forEach(function (el) {
      el.addEventListener('change', function () {
        if (self.onExtraFilterChange) {
          self.onExtraFilterChange(this.dataset.dodFilterId, this.value, self);
        }
      });
      el.addEventListener('input', function () {
        if (self.onExtraFilterChange) {
          self.onExtraFilterChange(this.dataset.dodFilterId, this.value, self);
        }
      });
    });

    // Pagination
    container.querySelectorAll('[data-dod-page]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        self._page = parseInt(this.dataset.dodPage, 10);
        self.loadData(function () { self.render(); });
      });
    });

    // Tree node click
    container.querySelectorAll('[data-dod-hier-node]').forEach(function (el) {
      el.addEventListener('click', function () {
        self._selectedHierNode = this.dataset.dodHierNode;
        self.render();
      });
    });

    // Tree toggle (expand/collapse)
    container.querySelectorAll('[data-dod-toggle]').forEach(function (el) {
      el.addEventListener('click', function (e) {
        e.stopPropagation();
        var targetId = this.dataset.dodToggle;
        var target = document.getElementById(targetId);
        if (target) {
          target.classList.toggle('hidden');
          this.innerHTML = target.classList.contains('hidden') ? '&#9654;' : '&#9660;';
        }
      });
    });

    // GL hierarchy toggle (expand/collapse — updates only arrow, preserves node text)
    container.querySelectorAll('[data-dod-gl-toggle]').forEach(function (el) {
      el.addEventListener('click', function (e) {
        e.stopPropagation();
        var targetId = this.dataset.dodGlToggle;
        var target = document.getElementById(targetId);
        if (target) {
          target.classList.toggle('hidden');
          var arrow = this.querySelector('.dod-arrow');
          if (arrow) arrow.innerHTML = target.classList.contains('hidden') ? '&#9654;' : '&#9660;';
        }
      });
    });

    // Expand All / Collapse All
    var expandBtn = container.querySelector('[data-dod-role="expand-all"]');
    var collapseBtn = container.querySelector('[data-dod-role="collapse-all"]');
    if (expandBtn) expandBtn.addEventListener('click', function () {
      self._allExpanded = true;
      self._expandedNodes = {};
      self.render();
    });
    if (collapseBtn) collapseBtn.addEventListener('click', function () {
      self._allExpanded = false;
      self._expandedNodes = {};
      // Collapse all tree-children
      container.querySelectorAll('.tree-children').forEach(function (el) {
        el.classList.add('hidden');
      });
      container.querySelectorAll('[data-dod-toggle]').forEach(function (el) {
        el.innerHTML = '&#9654;';
      });
    });

    // CSV download
    var csvBtn = container.querySelector('[data-dod-role="csv"]');
    if (csvBtn) csvBtn.addEventListener('click', function () {
      self._downloadCSV();
    });

    // Row click — detail panel or custom handler
    if (self.showDetailOnClick || self.onRowClick) {
      container.querySelectorAll('[data-dod-row-id]').forEach(function (tr) {
        tr.style.cursor = 'pointer';
        tr.addEventListener('click', function (e) {
          if (e.target.closest('button')) return; // ignore button clicks
          var rowId = this.dataset.dodRowId;
          var items = (self._data && self._data.items) || [];
          var row = items.find(function (it) { return String(it.id) === rowId; });
          if (!row) return;
          if (self.onRowClick) self.onRowClick(row);
          if (self.showDetailOnClick) self._showDetailPanel(row);
        });
      });
    }
  };

  // ── Detail panel (slide-out) ─────────────────────────────────────────

  DataObjectDisplay.prototype._showDetailPanel = function (row) {
    var self = this;
    // Remove existing panel
    var existing = document.getElementById('dod-detail-overlay');
    if (existing) existing.remove();

    // Collect all fields from the row
    var fields = Object.keys(row).filter(function (k) {
      return k !== 'id' && self.excludeColumns.indexOf(k) === -1;
    });

    var html = '<div id="dod-detail-overlay" class="fixed inset-0 z-50 flex justify-end" style="background:rgba(0,0,0,0.3)">';
    html += '<div class="bg-white w-full max-w-lg h-full overflow-y-auto shadow-xl" style="animation:dodSlideIn .2s ease-out">';
    html += '<div class="sticky top-0 bg-white border-b px-4 py-3 flex items-center justify-between">';
    html += '<h3 class="font-semibold text-sm">Record Detail</h3>';
    html += '<button id="dod-detail-close" class="text-gray-400 hover:text-gray-700 text-xl leading-none">&times;</button>';
    html += '</div>';
    html += '<div class="px-4 py-3">';
    html += '<table class="w-full text-xs">';
    fields.forEach(function (key) {
      var val = row[key];
      var label = self.colLabel(key);
      var displayVal = val == null ? '<span class="text-gray-300 italic">null</span>' : esc(String(val));
      html += '<tr class="border-b border-gray-100">';
      html += '<td class="py-1.5 pr-3 text-gray-500 font-medium whitespace-nowrap align-top">' + esc(label) + '</td>';
      html += '<td class="py-1.5 text-gray-900 break-all">' + displayVal + '</td>';
      html += '</tr>';
    });
    html += '</table>';
    html += '</div></div></div>';

    document.body.insertAdjacentHTML('beforeend', html);

    // Close handlers
    document.getElementById('dod-detail-close').addEventListener('click', function () {
      document.getElementById('dod-detail-overlay').remove();
    });
    document.getElementById('dod-detail-overlay').addEventListener('click', function (e) {
      if (e.target === this) this.remove();
    });
    // ESC key
    var escHandler = function (e) {
      if (e.key === 'Escape') {
        var overlay = document.getElementById('dod-detail-overlay');
        if (overlay) overlay.remove();
        document.removeEventListener('keydown', escHandler);
      }
    };
    document.addEventListener('keydown', escHandler);
  };

  // ── CSV download ────────────────────────────────────────────────────

  DataObjectDisplay.prototype._downloadCSV = function () {
    var items = (this._data && this._data.items) || [];
    if (!items.length) return;
    var self = this;

    // Show option dialog: general (visible columns) vs detailed (all fields)
    var existingDialog = document.getElementById('dod-csv-dialog');
    if (existingDialog) existingDialog.remove();

    var html = '<div id="dod-csv-dialog" class="fixed inset-0 z-50 flex items-center justify-center" style="background:rgba(0,0,0,0.3)">';
    html += '<div class="bg-white rounded-lg shadow-xl p-5 max-w-sm w-full mx-4">';
    html += '<h3 class="font-semibold text-sm mb-3">Download CSV</h3>';
    html += '<p class="text-xs text-gray-500 mb-4">Choose which columns to include:</p>';
    html += '<div class="space-y-2">';
    html += '<button data-csv-mode="general" class="w-full text-left px-3 py-2 border rounded hover:bg-gray-50 text-sm">';
    html += '<span class="font-medium">General View</span><br><span class="text-xs text-gray-500">Only visible table columns</span></button>';
    html += '<button data-csv-mode="detailed" class="w-full text-left px-3 py-2 border rounded hover:bg-gray-50 text-sm">';
    html += '<span class="font-medium">Detailed View</span><br><span class="text-xs text-gray-500">All fields (full export)</span></button>';
    html += '</div>';
    html += '<button id="dod-csv-cancel" class="mt-3 text-xs text-gray-400 hover:text-gray-600">Cancel</button>';
    html += '</div></div>';

    document.body.insertAdjacentHTML('beforeend', html);

    var dialog = document.getElementById('dod-csv-dialog');
    dialog.querySelector('#dod-csv-cancel').addEventListener('click', function () { dialog.remove(); });
    dialog.addEventListener('click', function (e) { if (e.target === dialog) dialog.remove(); });

    dialog.querySelectorAll('[data-csv-mode]').forEach(function (btn) {
      btn.addEventListener('click', function () {
        var mode = this.dataset.csvMode;
        dialog.remove();
        self._doCSVDownload(mode);
      });
    });
  };

  DataObjectDisplay.prototype._doCSVDownload = function (mode) {
    var items = (this._data && this._data.items) || [];
    if (!items.length) return;
    var self = this;
    var cols;

    if (mode === 'detailed') {
      // All fields from data, excluding internal ones
      var allKeys = {};
      items.forEach(function (row) {
        Object.keys(row).forEach(function (k) {
          if (k !== 'id' && self.excludeColumns.indexOf(k) === -1) allKeys[k] = true;
        });
      });
      cols = Object.keys(allKeys);
    } else {
      cols = this.getTableColumns();
    }

    var csvLines = [];
    csvLines.push(cols.map(function (c) { return '"' + self.colLabel(c).replace(/"/g, '""') + '"'; }).join(','));
    items.forEach(function (row) {
      csvLines.push(cols.map(function (c) {
        var v = row[c];
        if (v == null) return '';
        return '"' + String(v).replace(/"/g, '""') + '"';
      }).join(','));
    });

    var blob = new Blob([csvLines.join('\n')], { type: 'text/csv' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = this.objectType + (mode === 'detailed' ? '-detailed' : '') + '.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  // ── Main load entry point ───────────────────────────────────────────

  DataObjectDisplay.prototype.load = function () {
    var self = this;
    var container = document.getElementById(this.containerId);
    if (container) {
      container.innerHTML = '<span class="text-sm text-gray-400">Loading...</span>';
    }

    // Load display config and hierarchy options in parallel, then data
    var configLoaded = false;
    var hiersLoaded = false;

    function tryRender() {
      if (!configLoaded || !hiersLoaded) return;
      self.loadData(function () {
        // If hierarchy is auto-selected and we're in hierarchy view, load tree
        if (self._view === 'hierarchy' && self._hierPickerId && typeof self._hierPickerId === 'number') {
          self.loadHierarchyTree(self._hierPickerId, function () { self.render(); });
        } else {
          self.render();
        }
      });
    }

    this.loadDisplayConfig(function () {
      configLoaded = true;
      tryRender();
    });

    if (this.showHierarchyPicker && !this.inlineHierarchies) {
      // Non-inline mode: fetch hierarchy options separately
      this.loadHierarchyOptions(function () {
        hiersLoaded = true;
        tryRender();
      });
    } else {
      // Inline mode: hierarchies come from the data endpoint
      hiersLoaded = true;
      tryRender();
    }
  };

  // ── Reload with updated params ──────────────────────────────────────

  DataObjectDisplay.prototype.reload = function (newParams) {
    if (newParams) {
      Object.keys(newParams).forEach(function (k) {
        this.extraQueryParams[k] = newParams[k];
      }.bind(this));
    }
    this._page = 1;
    var self = this;
    this.loadData(function () { self.render(); });
  };

  // ── Set last loaded timestamp (can be called after init) ────────────

  DataObjectDisplay.prototype.setLastLoaded = function (isoTimestamp) {
    this.lastLoadedAt = isoTimestamp || null;
    this.render();
  };

  // ── Export to global scope ──────────────────────────────────────────

  root.DataObjectDisplay = DataObjectDisplay;

})(window);
