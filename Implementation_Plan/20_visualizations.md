# 20 — Visualisations on Analysis Results (ML + Analytics)

This module specifies the visual surface for analysis results. Every analysis_run
produces a rich set of charts and interactive visuals, available in the cockpit
(§06), the chat assistant (§16) as embeddable images/links, and exportable in the
review packs and run-summary docs.

The goal is to make the volume (~250k centers) tractable at a glance and let the
analyst drill into specifics in two clicks.

## 20.1 Tech choices

- **Frontend**: Recharts (or ECharts) for most chart types; D3 for bespoke
  visualisations (Sankey, hierarchy compare, dendrogram).
- **Server-side rendering** of charts as PNG/SVG when needed for emails / exports
  via `vega-lite` rendered with `vega` in a Node service (or `matplotlib` in
  Python — implementer choice).
- All charts speak a common JSON spec (`vega-lite` preferred) so the same payload
  can power web + email + PDF exports.

## 20.2 Visualisation catalogue

Each visual is a first-class entity with: id, title, description, applicability
(when it makes sense to render), data query, configurable parameters, supported
formats (interactive web / SVG / PNG / CSV-data), and an attached "explainer" doc
chunk for the in-app help drawer (§19).

### Run-level

| ID | Visual | Purpose |
|---|---|---|
| `kpi_tiles` | KPI tile row | Total, KEEP %, RETIRE %, MERGE_MAP %, REDESIGN %, ML coverage, LLM coverage |
| `outcome_donut` | Donut chart | Distribution of `cleansing_outcome` across the run |
| `target_object_bar` | Stacked bar | Distribution of `target_object` per outcome |
| `outcome_by_entity_heatmap` | Heatmap | Entities (rows) × outcome (cols), shaded by count or share |
| `outcome_by_region_map` | Choropleth | If region available, world/country map shaded by outcome share |
| `legacy_to_target_sankey` | Sankey | Legacy outcome → target object flows |
| `inactivity_heatmap` | 2-D heatmap | Months-since-last-posting × balance bucket |
| `balance_bubble` | Bubble | Per-center bubble: x=last-posting-period, y=total balance, size=posting count, color=outcome |
| `naming_coverage_bar` | Stacked bar | Per object type, # auto-named vs collision vs reuse-legacy |
| `ml_confidence_hist` | Histogram | Distribution of `ml_confidence` per outcome class |
| `ml_shap_top_features` | Horizontal bar | Top mean SHAP magnitudes per class (global explainability) |
| `cluster_galaxy` | Scatter (UMAP) | 2-D embedding of all centers, coloured by cluster id; hover→detail |
| `cluster_size_bar` | Bar | Distribution of duplicate cluster sizes |
| `coverage_treemap` | Treemap | Hierarchy nodes sized by # centers in scope, coloured by outcome share |

### Run comparison

| ID | Visual | Purpose |
|---|---|---|
| `outcome_diff_matrix` | Heatmap | from-outcome (run A) × to-outcome (run B) |
| `target_diff_matrix` | Heatmap | target_object diff |
| `changed_centers_table` | Table | Rows changed, with both verdicts and rationale |
| `confidence_delta_hist` | Histogram | Per-center ML confidence delta (B − A) |
| `routine_attribution` | Stacked bar | Which routine flipped most centers between A and B |

### Per-center (drill-in / why-panel embedded)

| ID | Visual | Purpose |
|---|---|---|
| `monthly_balance_line` | Line chart | Monthly TC + GC2 amounts over the data window |
| `posting_count_bar` | Bar | Postings per period |
| `account_class_split` | Donut | B/S, REVENUE, OPEX, OTHER share |
| `feature_radar` | Radar | The center's normalised features vs cluster median |
| `shap_local` | Horizontal bar | Per-prediction SHAP top-5 feature contributions |
| `cluster_neighbours` | Mini-table | Nearest neighbours (cosine similarity) |
| `decision_path_breadcrumb` | Breadcrumb / step diagram | Rule path visualised as a flow |

### Hierarchy visuals

| ID | Visual | Purpose |
|---|---|---|
| `legacy_hierarchy_tree` | Collapsible tree | Cost-center group tree (SETNODE) with leaf counts and outcome-share badges |
| `proposed_hierarchy_tree` | Collapsible tree | New target structure |
| `legacy_vs_proposed_compare` | Side-by-side trees with brushing | Click a node on one side, related nodes light up on the other |
| `hierarchy_dendrogram` | Dendrogram | For duplicate clusters, show how they merge as similarity threshold lowers |

### Housekeeping

| ID | Visual | Purpose |
|---|---|---|
| `housekeeping_flag_donut` | Donut | UNUSED / LOW_VOLUME / NO_OWNER / ANOMALY split |
| `owner_response_funnel` | Funnel | Notified → opened → decided → submitted |
| `closure_trend_line` | Line | Cycle-over-cycle closures and KEEPs |

## 20.3 Interactivity & drill-down rules

- Every chart segment is a hyperlink: clicking opens the cockpit's universe table
  pre-filtered by that segment.
- Crossfilter: selections in one chart filter the others on the same page (small
  shared store).
- Hover tooltips show counts + share + a 1-line description.
- Right-click / "..." menu: "Open in new tab", "Export PNG", "Copy data CSV",
  "Open in chat" (sends a context-rich question to the chat assistant).

## 20.4 Data delivery API

Chart payloads ride a normalised endpoint:

```
GET /api/runs/{id}/visuals/{visual_id}?{params}
→ 200 OK
{
  "vega_lite": { ... },         // vega-lite spec
  "data_url": "/api/runs/{id}/visuals/{visual_id}/data?{params}",
  "summary": { "total": 12345, "filters_applied": {...} }
}
```

`/data` returns either inline data (when small) or paged results. Heavy queries
materialise into Postgres / cache to keep the cockpit fast.

## 20.5 Export & sharing

- **PNG / SVG**: every chart exportable from the UI; same on the API
  (`?format=png|svg|pdf`).
- **CSV**: underlying data downloadable.
- **PowerPoint / Word inserts**: from a run, an analyst can generate a "Run report"
  document (using the docx and pptx skills already configured) that embeds
  selected visuals as PNGs along with auto-narrated commentary
  (§13.5 `prompt.run_summary.v1`).
- **Email digest**: end-of-run notifications to the wave owners include the top 3
  visuals as inline PNG attachments.

## 20.6 Accessibility & quality bar

- All charts include text-equivalent (table view toggle).
- Colour palettes are colour-blind safe (Okabe-Ito or Viridis); never red/green
  alone to indicate state.
- Screen-reader: charts have `aria-label` and `aria-describedby` pointing to a
  textual summary computed server-side.
- Performance: a chart endpoint must respond in < 500ms p95 on cached data.

## 20.7 Configurability

A run owner may choose, per run, which visuals to render (tick-box list backed by
the analytical configuration §05.6 → `visualizations: [...]`). Defaults render the
full set. Custom visuals can be added later by extending the visual catalogue
(plug-in pattern mirroring §04.6 — registered under `cleanup.visual`).

## 20.8 Persistent statistics strip (always visible)

A KPI strip MUST be visible on every cockpit, admin, and wave page. The strip
shows totals **and percentages** for the current scope and contrasts them with
the universe to make reduction visible at a glance. The same payload is available
via API for external dashboards, embeddable in chat answers, and exportable.

### 20.8.1 KPIs to surface

For each metric, render: **In scope**, **Total universe**, **Δ (absolute)**,
**Δ%**, and (where applicable) **Reduction %**.

| KPI | Definition |
|---|---|
| Entities — total | Count of `entity` rows |
| Entities — in current wave | `wave_entity` count for active wave (or scope) |
| Entities — covered by any signed-off wave | DISTINCT entities across signed_off / closed waves |
| Legacy CCs — total active | `legacy_cost_center where is_active=true` |
| Legacy CCs — in current scope | filtered by wave/scope/full-scope-with-exclusion |
| Legacy CCs — proposed KEEP | `center_proposal where cleansing_outcome='KEEP'` |
| Legacy CCs — proposed RETIRE | … = 'RETIRE' |
| Legacy CCs — proposed MERGE_MAP | … = 'MERGE_MAP' |
| Legacy CCs — proposed REDESIGN | … = 'REDESIGN' |
| Target Cost Centers — proposed | `target_cost_center` linked to the run/wave |
| Target Cost Centers — signed off | … `is_active=true and approved_in_wave NOT NULL` |
| Target Profit Centers — proposed | `target_profit_center` linked to the run/wave |
| Target Profit Centers — signed off | … signed off |
| Reduction CC | `1 − target_cc_count / legacy_cc_count_in_scope` |
| Reduction PC | `1 − target_pc_count / legacy_pc_count_in_scope` |
| Hierarchy compliance | `% of legacy CCs in scope with exactly 1 hierarchy node` |
| Sign-off progress | % of review_items in terminal state across active scopes |
| Housekeeping closures (current cycle) | Items with `decision='CLOSE'` |

### 20.8.2 Display rules

- The strip auto-collapses to a single dense line on small screens but always
  remains visible.
- Each KPI tile shows: big number (total in scope), a small grey number (universe
  total), and a coloured delta (`−42% vs universe`). Down-arrow / red when
  shrinking, up-arrow / green when growing.
- Click a tile → drills into the universe table filtered to that subset.
- Hover → tooltip explains the calculation in plain language.
- A toggle switches the percentages between "vs universe", "vs prior wave", "vs
  start of project".

### 20.8.3 Endpoints

```
GET /api/stats/global                       # universe-wide totals
GET /api/stats/wave/{id}                    # wave-scoped
GET /api/stats/run/{run_id}                 # run-scoped (live)
GET /api/stats/housekeeping/{cycle_id}      # housekeeping-scoped
GET /api/stats/compare?a=<scopeA>&b=<scopeB>  # compare two scopes
```

Response shape (representative):

```json
{
  "universe": {
    "entities_total": 547,
    "legacy_cc_total": 217214,
    "legacy_pc_total": 217214,
    "as_of": "2026-04-25T10:00:00Z"
  },
  "scope": {
    "label": "Wave 2026 Q3 APAC",
    "entities": 14,
    "legacy_cc": 18234,
    "legacy_pc": 18234,
    "proposed": {
      "keep":      8120,
      "retire":    9201,
      "merge_map":  710,
      "redesign":   203,
      "target_cc":  5423,
      "target_pc":   612
    }
  },
  "deltas": {
    "cc_reduction_abs":   12811,
    "cc_reduction_pct":   0.7026,
    "pc_reduction_abs":   17622,
    "pc_reduction_pct":   0.9664
  },
  "freshness": {
    "data_snapshot": "uuid",
    "computed_at":   "2026-04-25T10:01:23Z",
    "cache_ttl_s":   60
  }
}
```

### 20.8.4 Performance & caching

- All stats served from materialised views refreshed at the end of every load /
  analysis run.
- Cached in Redis under the run + wave + cycle keys; TTL 60s; busted on lock /
  signoff / load events.
- The strip's frontend hydrates from a single `/api/stats/...` call; no per-tile
  fan-out.

### 20.8.5 Visual variants

A larger **Statistics dashboard** is available at `/stats` (and within each wave
at `/wave/{id}/stats`) with:

- All tiles from §20.8.1 grouped by section.
- Time series of KPIs across all signed-off waves (cumulative reduction).
- Per-entity breakdown table sortable by any KPI.
- Export buttons (CSV / PNG / docx insert).

### 20.8.6 Statistics in emails & exports

- Wave-published, wave-signed-off, and housekeeping-summary emails include a
  tiny stat table (legacy → proposed counts, reduction %).
- The MDG export bundle (§09.4) includes a `stats.csv` summary file alongside the
  CC and PC files for traceability.

## 20.9 Acceptance

- [ ] All run-level visuals render against the sample dataset within
  performance budget.
- [ ] Per-center drill-down opens within 400ms of a row click.
- [ ] Run comparison page renders all diff visuals on cached diffs in < 1s.
- [ ] CSV / PNG / SVG exports work for every visual.
- [ ] Accessibility: every chart has a textual alternative; CI checks for
  `aria-label` presence.
