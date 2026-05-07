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
    this.showCSV = opts.showCSV !== false;
    this.pageSize = opts.pageSize || 200;
    this.hierPageSize = opts.hierPageSize || 10000;
    this.extraColumns = opts.extraColumns || [];
    this.extraQueryParams = opts.extraQueryParams || {};
    this.hierarchyTypes = opts.hierarchyTypes || null;
    this.glHierarchyMode = opts.glHierarchyMode || null;
    this.columns = opts.columns || null;
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
    // Title shown above the table
    this.title = opts.title || '';

    // State
    this._view = opts.defaultView || 'tabular';
    this._page = 1;
    this._search = '';
    this._sort = { col: null, dir: 'asc' };
    this._hierPickerId = null;
    this._hierData = null;
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
    // Check display config first
    if (this._displayConfig) {
      var cols = this._displayConfig.table_columns;
      if (cols && cols.length) return cols;
      cols = (this._displayConfig.all_columns || []).slice(0, 10);
      if (cols.length) return cols;
    }
    // Fallback: use caller-specified columns
    if (this.columns && this.columns.length) {
      return this.columns.map(function (c) { return c.key || c; });
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

  // ── Build hierarchy picker HTML ─────────────────────────────────────

  DataObjectDisplay.prototype.buildHierarchyPickerHtml = function () {
    var items = this._hierOptions;
    if (!items.length && !this.glHierarchyMode) return '';

    var html = '<div class="flex items-center gap-2 mb-2">';
    html += '<label class="text-xs text-gray-500 whitespace-nowrap">Hierarchy:</label>';
    html += '<select class="input text-xs w-72" data-dod-role="hier-picker">';
    html += '<option value="">(none — no L columns)</option>';

    if (this.glHierarchyMode) {
      // GL account artificial hierarchies
      html += '<option value="__gl_type_a">GL Type A — first character</option>';
      html += '<option value="__gl_type_b">GL Type B — first 5 characters</option>';
      html += '</select></div>';
      return html;
    }

    // Group by normalised setclass
    var groups = { '0101': [], '0106': [], '0104': [], other: [] };
    var groupLabels = {
      '0101': 'Cost Center hierarchies (CC)',
      '0106': 'Entity hierarchies',
      '0104': 'Profit Center hierarchies (PC)',
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
        html += '<option value="' + h.id + '">' +
          esc(h.setname || h.label || '') +
          (h.description ? ' — ' + esc(h.description) : '') +
          '</option>';
      });
      html += '</optgroup>';
    });

    html += '</select></div>';

    // Auto-select if only one hierarchy
    if (!filterTypes) {
      // Count total available
      var total = items.length;
      if (total === 1) {
        this._hierPickerId = items[0].id;
      }
    } else {
      var filtered = items.filter(function (h) { return filterTypes.indexOf(h.setclass) >= 0; });
      if (filtered.length === 1) {
        this._hierPickerId = filtered[0].id;
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
    var html = '';
    // Title row with action buttons
    if (this.title || this.toolbarButtons.length) {
      html += '<div class="flex items-center justify-between mb-2 gap-3 flex-wrap">';
      if (this.title) {
        html += '<h3 class="text-base font-semibold flex-shrink-0">' + esc(this.title) + '</h3>';
      }
      if (this.toolbarButtons.length) {
        html += '<div class="flex items-center gap-2 flex-wrap ml-auto">';
        this.toolbarButtons.forEach(function (btn, i) {
          html += '<button data-dod-role="toolbar-btn-' + i + '" class="' +
            escAttr(btn.className || 'btn-secondary text-xs') + '"' +
            (btn.title ? ' title="' + escAttr(btn.title) + '"' : '') + '>' +
            esc(btn.label) + '</button>';
        });
        html += '</div>';
      }
      html += '</div>';
    }
    // Controls row
    html += '<div class="flex items-center gap-2 mb-2 flex-wrap">';
    if (this.showViewToggle) {
      html += this.buildViewToggleHtml();
    }
    if (this.showSearch) {
      html += '<input type="text" data-dod-role="search" placeholder="Search..." class="input text-sm w-48 flex-shrink-0" value="' + escAttr(this._search) + '" />';
    }
    // Extra filter widgets
    this.extraFilters.forEach(function (f) {
      if (f.type === 'select') {
        html += '<select data-dod-role="extra-filter" data-dod-filter-id="' + escAttr(f.id) + '" class="input text-sm ' + (f.className || 'w-32') + '">';
        (f.options || []).forEach(function (opt) {
          var val = typeof opt === 'string' ? opt : opt.value;
          var label = typeof opt === 'string' ? opt : opt.label;
          html += '<option value="' + escAttr(val) + '">' + esc(label) + '</option>';
        });
        html += '</select>';
      } else {
        html += '<input type="' + (f.type || 'text') + '" data-dod-role="extra-filter" data-dod-filter-id="' + escAttr(f.id) + '"' +
          ' placeholder="' + escAttr(f.placeholder || '') + '"' +
          ' class="input text-sm ' + (f.className || 'w-32') + '" />';
      }
    });
    if (this.showCSV) {
      html += '<button data-dod-role="csv" class="btn-secondary text-xs flex-shrink-0" title="Download as CSV">CSV</button>';
    }
    if (this._view === 'hierarchy') {
      html += '<button data-dod-role="expand-all" class="btn-secondary text-xs flex-shrink-0">Expand All</button>';
      html += '<button data-dod-role="collapse-all" class="btn-secondary text-xs flex-shrink-0">Collapse All</button>';
    }
    // Show active filter count
    var activeFilters = Object.keys(this._columnFilters).length;
    if (activeFilters > 0) {
      html += '<span class="text-xs text-blue-600 font-medium">' + activeFilters + ' filter(s) active</span>';
      html += '<button data-dod-role="clear-all-filters" class="text-xs text-red-500 hover:text-red-700 underline">Clear All</button>';
    }
    html += '</div>';

    if (this.showHierarchyPicker && (this._hierOptions.length || this.glHierarchyMode)) {
      html += this.buildHierarchyPickerHtml();
    }

    return html;
  };

  // ── Data loading ────────────────────────────────────────────────────

  DataObjectDisplay.prototype.loadData = function (cb) {
    var self = this;
    if (!this.dataEndpoint) { if (cb) cb(null); return; }

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

    // Header
    var html = '<div class="overflow-x-auto overflow-y-auto" style="max-height:calc(100vh - 480px)">';
    html += '<table class="w-full text-xs"><thead><tr class="border-b bg-gray-50">';

    // Hierarchy level headers
    hierLevels.forEach(function (lv) {
      var isLvFiltered = self._columnFilters[lv] && self._columnFilters[lv].size > 0;
      html += '<th class="py-1.5 px-2 text-left font-medium bg-amplifi-50 relative" style="position:relative">' +
        '<div class="flex items-center gap-1">' +
        '<span class="cursor-pointer flex-1" data-dod-sort="' + escAttr(lv) + '">' + esc(lv) + self._sortIcon(lv) + '</span>' +
        '<span data-dod-role="filter-toggle" data-dod-filter-col="' + escAttr(lv) + '" ' +
        'class="cursor-pointer text-[10px] px-0.5 rounded hover:bg-blue-100' +
        (isLvFiltered ? ' text-blue-600 font-bold' : ' text-gray-400') + '" title="Filter">&#9660;</span>' +
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
        '<span data-dod-role="filter-toggle" data-dod-filter-col="' + escAttr(col) + '" ' +
        'class="cursor-pointer text-[10px] px-0.5 rounded hover:bg-blue-100' +
        (isColFiltered ? ' text-blue-600 font-bold' : ' text-gray-400') + '" title="Filter">&#9660;</span>' +
        '</div>';
      if (self._openFilterCol === col) {
        html += self._buildFilterDropdown(col);
      }
      html += '</th>';
    });

    html += '</tr></thead><tbody>';

    // Rows
    sorted.forEach(function (row) {
      html += '<tr class="border-b hover:bg-gray-50 cursor-pointer" data-dod-row-id="' + (row.id || '') + '">';

      // Hierarchy level cells
      var key = row[self.identityField] || row.id;
      var lvls = hierMap[key] || {};
      hierLevels.forEach(function (lv) {
        html += '<td class="py-1.5 px-2 font-mono text-amplifi-700">' + esc(lvls[lv] || '') + '</td>';
      });

      // Data cells
      cols.forEach(function (col) {
        var val = row[col];
        var display = '';
        if (val === true) display = '<span class="text-green-600">Yes</span>';
        else if (val === false) display = '<span class="text-red-500">No</span>';
        else if (val != null) display = esc(String(val));
        html += '<td class="py-1.5 px-2" title="' + escAttr(String(val || '')) + '">' + display + '</td>';
      });

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

    // Need a selected hierarchy with tree data. Try inline first.
    var hierData = this._hierData;
    if (!hierData && this._hierPickerId && this.inlineHierarchies && this._data && this._data.hierarchies) {
      var inlineHier = this._data.hierarchies.filter(function (h) { return h.id === self._hierPickerId; })[0];
      if (inlineHier && inlineHier.nodes) hierData = inlineHier;
    }
    if (!this._hierPickerId || !hierData) {
      if (this._hierPickerId && !hierData) {
        return '<span class="text-gray-400 text-sm">Loading hierarchy tree...</span>';
      }
      return '<span class="text-gray-400 text-sm">Select a hierarchy from the picker above to view hierarchical display.</span>';
    }

    var setclass = hierData.setclass || '';

    // Build leaf-items map based on normalised setclass
    var leafItemsMap = {};
    var normSetclass = normaliseSetclass(setclass);
    if (normSetclass === '0106') {
      // Entity hierarchy — leaves are ccodes
      items.forEach(function (it) {
        var key = it[self.entityField];
        if (!key) return;
        if (!leafItemsMap[key]) leafItemsMap[key] = [];
        leafItemsMap[key].push(it);
      });
    } else if (normSetclass === '0104') {
      // PC hierarchy — leaves are pctrs
      items.forEach(function (it) {
        var key = it[self.profitCenterField];
        if (!key) return;
        if (!leafItemsMap[key]) leafItemsMap[key] = [];
        leafItemsMap[key].push(it);
      });
    } else {
      // CC hierarchy or default — leaves match identityField
      items.forEach(function (it) {
        var key = it[self.identityField];
        if (!key) return;
        leafItemsMap[key] = [it];
      });
    }

    // Build child map from tree data
    var childMap = {};
    var nodes = hierData.nodes || [];
    var leaves = hierData.leaves || [];

    nodes.forEach(function (n) {
      var parent = n.parent_setname || n.parent;
      var child = n.child_setname || n.child;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'node', name: child, seq: n.seq || 0, description: n.description || '' });
    });
    leaves.forEach(function (lf) {
      var parent = lf.setname || lf.parent;
      if (!childMap[parent]) childMap[parent] = [];
      childMap[parent].push({ type: 'leaf', value: lf.value, seq: lf.seq || 0 });
    });

    // Find roots
    var allChildren = {};
    nodes.forEach(function (n) {
      var child = n.child_setname || n.child;
      allChildren[child] = 1;
    });
    var roots = [];
    nodes.forEach(function (n) {
      var parent = n.parent_setname || n.parent;
      if (!allChildren[parent] && roots.indexOf(parent) < 0) roots.push(parent);
    });
    if (!roots.length && nodes.length) {
      roots.push(nodes[0].parent_setname || nodes[0].parent);
    }

    // Count leaves function
    function countLeaves(nodeName) {
      var ch = childMap[nodeName] || [];
      var cnt = 0;
      ch.forEach(function (c) {
        if (c.type === 'leaf') {
          cnt += (leafItemsMap[c.value] || []).length;
        } else {
          cnt += countLeaves(c.name);
        }
      });
      return cnt;
    }

    // Split panel: tree left, detail right
    var html = '<div class="flex gap-3" style="height:calc(100vh - 480px);min-height:200px">';

    // Left panel — tree
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
        (isSelected ? ' font-bold text-blue-700 bg-blue-50' : ' text-gray-800') + '" title="' + escAttr(nodeName) + '">';
      out += esc(nodeName) + ' <span class="text-[10px] text-gray-400">(' + lc + ')</span></span></div>';

      var children = childMap[nodeName] || [];
      children.sort(function (a, b) { return (a.seq || 0) - (b.seq || 0); });

      out += '<div id="' + escAttr(nodeId) + '" class="tree-children' + (isExpanded ? '' : ' hidden') + '">';

      // Render child nodes
      children.forEach(function (c) {
        if (c.type === 'node') {
          out += buildTree(c.name, depth + 1);
        }
      });

      // Render leaves directly under this node with indentation
      children.forEach(function (c) {
        if (c.type === 'leaf') {
          var leafItems = leafItemsMap[c.value] || [];
          if (leafItems.length) {
            leafItems.forEach(function (item) {
              var leafPad = (depth + 1) * 12;
              var isLeafSelected = self._selectedHierNode === ('__leaf__' + c.value);
              out += '<div style="padding-left:' + leafPad + 'px" class="py-0.5">' +
                '<span data-dod-hier-node="__leaf__' + escAttr(c.value) + '" class="text-xs cursor-pointer hover:text-blue-600 px-1 rounded font-medium' +
                (isLeafSelected ? ' text-blue-700 bg-blue-50' : ' text-gray-700') + '">' +
                '📄 ' + esc(item[self.identityField] || c.value) +
                (item.txtsh ? ' — ' + esc(item.txtsh) : (item.name ? ' — ' + esc(item.name) : '')) +
                '</span></div>';
            });
          } else {
            // Leaf without matching item — still show value
            var leafPad2 = (depth + 1) * 12;
            out += '<div style="padding-left:' + leafPad2 + 'px" class="py-0.5">' +
              '<span class="text-xs text-gray-400 px-1">📄 ' + esc(c.value) + ' (no match)</span></div>';
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
      html += '<div class="flex items-center gap-2 px-3 py-2 bg-gray-50 cursor-pointer" data-dod-toggle="' + escAttr(nodeId) + '">';
      html += '<span class="text-[10px] text-gray-400">' + (isExpanded ? '&#9660;' : '&#9654;') + '</span>';
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
      // Re-fetch with large page size + inline hierarchies if needed
      if (self.inlineHierarchies || (self._hierPickerId && !self._hierData && typeof self._hierPickerId === 'number')) {
        self.loadData(function () {
          // Also load tree data if needed and not inline
          if (!self.inlineHierarchies && self._hierPickerId && !self._hierData && typeof self._hierPickerId === 'number') {
            self.loadHierarchyTree(self._hierPickerId, function () { self.render(); });
          } else {
            self.render();
          }
        });
        return;
      }
      self.loadData(function () { self.render(); });
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

        // Always re-fetch data (for hierarchy_id param or inline hierarchies)
        self.loadData(function () {
          // For non-inline mode, also load tree data when in hierarchical view
          if (!self.inlineHierarchies && self._view === 'hierarchy' && self._hierPickerId && typeof self._hierPickerId === 'number') {
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
        if (e.target.tagName === 'INPUT') return; // don't sort on filter click
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

    // Row click
    if (self.onRowClick) {
      container.querySelectorAll('[data-dod-row-id]').forEach(function (tr) {
        tr.addEventListener('click', function () {
          var rowId = this.dataset.dodRowId;
          var items = (self._data && self._data.items) || [];
          var row = items.find(function (it) { return String(it.id) === rowId; });
          if (row) self.onRowClick(row);
        });
      });
    }
  };

  // ── CSV download ────────────────────────────────────────────────────

  DataObjectDisplay.prototype._downloadCSV = function () {
    var items = (this._data && this._data.items) || [];
    if (!items.length) return;
    var cols = this.getTableColumns();
    var self = this;

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
    a.download = this.objectType + '.csv';
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

  // ── Export to global scope ──────────────────────────────────────────

  root.DataObjectDisplay = DataObjectDisplay;

})(window);
