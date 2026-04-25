# 05 — ML, Analytics, Configuration Versioning & LLM Review Modes

This module covers the **analysis framework** that wraps the decision tree (§04). It
defines:

- The analytics tools catalogue (the toggleable routines exposed in the cockpit).
- The pragmatic classical-ML stack (features, models, training, scoring).
- The **analytical configuration** object: define / save / amend / version.
- **Result versioning** and **version comparison** between analysis runs.
- Three **LLM review modes**: SINGLE, SEQUENTIAL pipeline, and full DEBATE — each
  producing per-center commentary attached to the run.

## 5.1 Analytics tools catalogue (each is a registered routine — §04.6)

| Code | Kind | Purpose |
|---|---|---|
| `rule.posting_activity` | rule | Inactivity check — drives RETIRE |
| `rule.ownership` | rule | Owner present + resolvable |
| `rule.redundancy` | rule | Pairs with `ml.duplicate_cluster` to mark MERGE_MAP |
| `rule.hierarchy_compliance` | rule | Single-node membership check |
| `rule.cross_system_dependency` | rule | Flags BW/GRC/intercompany dependencies |
| `rule.bs_relevance` | rule | Sets PC tentative (Tree B step 0) |
| `rule.has_direct_revenue` | rule | Tree B step 1 |
| `rule.has_operational_costs` | rule | Tree B step 2 |
| `rule.collects_project_costs` | rule | Tree B step 3 |
| `rule.revenue_allocation_vehicle` | rule | Tree B step 4 |
| `rule.cost_allocation_vehicle` | rule | Tree B step 5 |
| `rule.info_only` | rule | Tree B step 6 |
| `aggregate.combine_outcomes` | aggregate | Reduces routine outputs into final outcome + target_object |
| `ml.duplicate_cluster` | ml | Embedding + ANN cluster of similar centers |
| `ml.outcome_classifier` | ml | LightGBM classifier on engineered features → outcome probabilities |
| `ml.target_object_classifier` | ml | LightGBM multinomial → target_object probabilities |
| `ml.naming_purpose` | ml | Sentence-transformer + small head → infers semantic purpose from name |
| `ml.anomaly_detector` | ml | IsolationForest on balance/posting features (housekeeping) |
| `llm.review_single` | llm | One model commentary per center (mode SINGLE) |
| `llm.review_sequential` | llm | Pipeline of models refining the commentary (mode SEQUENTIAL) |
| `llm.review_debate` | llm | Two-model debate + judge (mode DEBATE) |

All routines registered at boot from `cleanup.routine` (§03.2.2).

## 5.2 Feature engineering

Built into `cleanup.feature_set` materialised view, refreshed per data_snapshot.

### Numeric features (per center)

- Balance volume: `bs_amt`, `rev_amt`, `opex_amt`, `other_amt`, totals (TC + GC2).
- Posting cadence: `posting_count_window`, `months_active_in_window`,
  `months_since_last_posting`, `period_count_with_postings`.
- Volatility: stddev of monthly amounts.
- Concentration: share of largest period, share of top 3 currencies.
- Hierarchy: depth, sibling count, leaf-or-node, membership_count.
- Ownership: `has_owner`, owner-tenure-months, owner-active.
- Cross-system: BW/GRC/intercompany flags (boolean).

### Text features

- `txtsh`, `txtmi` cleaned and lowercased.
- Embeddings via `sentence-transformers/all-MiniLM-L6-v2` (384-dim).
- TF-IDF char-ngrams as a fallback for legacy/deterministic clustering.

### Categorical

- `coarea`, `ccode`, `currency`, `category` — one-hot or target-encoded.
- Functional area, region (from `attrs`).

## 5.3 Model catalogue

| Model | Algorithm | Output | Where used |
|---|---|---|---|
| `outcome_classifier` | LightGBM multiclass (4 classes: KEEP/RETIRE/MERGE_MAP/REDESIGN) | per-center class probabilities + SHAP values | `ml.outcome_classifier` routine; surfaces `ml_confidence` on the proposal |
| `target_object_classifier` | LightGBM multiclass (7 target_object values) | per-center probabilities | `ml.target_object_classifier` |
| `duplicate_cluster` | sentence embedding + HNSW ANN + agglomerative clustering | cluster_id + similarity score | `ml.duplicate_cluster`; supports `rule.redundancy` |
| `naming_purpose` | sentence embedding + linear head trained on labelled examples | one of {operational, technical, project, statistical, allocation_vehicle, unknown} | `ml.naming_purpose`; feeds Tree B branches |
| `anomaly_detector` | IsolationForest over post-launch features | anomaly score 0..1 | Housekeeping cycle (§08) |

Models are stored as artifacts under `models/<name>/<version>/`. Loading is lazy and
cached. Each model declares its **expected feature schema** (a JSON list of features +
dtypes); the engine fails fast on mismatch.

## 5.4 Training & re-training

Training data sources:
- The hand-labelled **golden corpus** (200 centers) for v1.
- After the first wave is signed off, the **approved proposals** become labelled data
  (`approved_outcome`, `approved_target_object`) and feed retraining.
- A `training_dataset` table records snapshots used for each model version (lineage).

Training entrypoint: `python -m app.cli ml train <model_name>`. Outputs are version-tagged
and registered in `cleanup.routine` so the cockpit can pick the active version. The
implementer SHOULD wire MLflow if available.

## 5.5 Explainability (mandatory)

Every ML routine MUST contribute to the audit trail:

- **Decision-tree rules**: which rule fired, with what feature values
  (`routine_output.payload`).
- **LightGBM classifiers**: SHAP top-5 features per prediction, included in
  `routine_output.payload.shap`.
- **Duplicate clusters**: cluster_id + nearest neighbours + similarity scores.
- **LLM review**: full prompt hash + model name + raw response stored in
  `routine_output.payload.llm` (PII-scrubbed; full text retained for audit).

The cockpit "why?" panel surfaces this directly.

## 5.6 Analytical configuration object

The configuration is **the recipe** for an analysis run: which routines fire, in what
order, with what parameters; which ML models are active; which LLM review mode is
applied.

### Schema (JSON, versioned with `cleanup.analysis_config`)

```jsonc
{
  "name": "Wave 2026 Q3 APAC – tighter inactivity",
  "code": "WAVE-APAC-V2",
  "version": 2,
  "parent_config_code": "STD-CLEANSING-V2",   // amendments inherit
  "data_window": { "months": 18 },             // for activity / volume features
  "feature_overrides": {
    "account_class.ranges": "use:default"
  },
  "pipeline": [
    { "routine": "rule.posting_activity",
      "enabled": true,
      "params": { "posting_inactivity_threshold": 9, "posting_minimal_threshold": 0 } },

    { "routine": "rule.ownership",          "enabled": true,  "params": {} },
    { "routine": "ml.duplicate_cluster",    "enabled": true,
      "params": { "model_version": "1.3", "similarity_threshold": 0.92 } },
    { "routine": "rule.redundancy",         "enabled": true,  "params": {} },
    { "routine": "rule.hierarchy_compliance","enabled": true, "params": { "strict": false } },
    { "routine": "rule.cross_system_dependency","enabled": true, "params": {} },

    { "routine": "ml.outcome_classifier",   "enabled": true,
      "params": { "model_version": "1.4", "min_confidence": 0.6 } },

    { "routine": "rule.bs_relevance",                 "enabled": true,  "params": {} },
    { "routine": "rule.has_direct_revenue",           "enabled": true,  "params": {} },
    { "routine": "rule.has_operational_costs",        "enabled": true,  "params": {} },
    { "routine": "rule.collects_project_costs",       "enabled": true,  "params": {} },
    { "routine": "rule.revenue_allocation_vehicle",   "enabled": true,  "params": {} },
    { "routine": "rule.cost_allocation_vehicle",      "enabled": true,  "params": {} },
    { "routine": "rule.info_only",                    "enabled": true,  "params": {} },

    { "routine": "ml.target_object_classifier", "enabled": true,
      "params": { "model_version": "1.2" } },

    { "routine": "aggregate.combine_outcomes",  "enabled": true,
      "params": { "ml_override_when_confidence_above": 0.95 } }
  ],
  "llm_review": {
    "mode": "SEQUENTIAL",
    "models": [
      { "provider": "azure",  "model": "gpt-4o",     "role": "drafter" },
      { "provider": "btp",    "model": "gemini-1.5", "role": "critic" }
    ],
    "scope": { "outcomes": ["MERGE_MAP","REDESIGN"], "min_balance_eur": 100000 },
    "max_centers": 5000,
    "prompt_template": "prompt.review.v3"
  },
  "performance": {
    "batch_size": 2000,
    "parallel_workers": 8,
    "ml_score_cache": true
  }
}
```

### Save / amend semantics

- **Save** creates a new `analysis_config` row with a new `code` (or new `version`
  under the same code).
- **Amend** writes a new version of the existing code; the previous version remains
  immutable if it was used by any `analysis_run` (`is_immutable=true`).
- **Fork** (`POST /api/configs/{code}/fork`) creates a new config with
  `parent_config_id` set, so lineage is preserved.
- **Archive** sets `is_active=false`. Configs are never hard-deleted.

### UI (cross-ref §06.3)

The cockpit "Pipeline editor" lists every registered routine with its JSON-Schema-driven
parameter form. Drag-and-drop reordering. Each row has an enabled toggle. The right
panel previews how many centers will be touched by the current config (sampled estimate).

## 5.7 Result versioning

Each run produces a versioned result identified by `analysis_run.id`. The result is
**immutable** once `status='done'`. Re-running with the same config + data_snapshot is
allowed (gives same result modulo LLM stochasticity, which is captured per-call) and
creates a new run row, **not** an in-place mutation.

`version_label` (free-text) lets analysts annotate versions: "v1", "v2 - tighter
inactivity", "v3 - debate review".

`parent_run_id` is set when an analyst clicks "amend & re-run" so lineage is
preserved.

## 5.8 Run comparison (diff)

```
GET /api/runs/{run_a}/diff/{run_b}
```

Computes:

- **Outcome diff matrix**: from-outcome × to-outcome counts.
- **Target-object diff matrix**.
- **Centers changed**: paginated list with both verdicts and rationale.
- **Centers only in A** / **only in B** (when scopes differ).
- **Routine-level deltas**: which routines changed verdict, count by routine.
- **ML score histograms** (run A vs run B).
- **LLM commentary changes** when both runs included LLM review.

Cached in `cleanup.analysis_run_diff` after first computation. Cockpit page §06.6 renders
this diff with a Sankey for outcome flows and tables for changed centers.

## 5.9 LLM review modes (SINGLE / SEQUENTIAL / DEBATE)

All three modes write per-center commentary into `routine_output` and a pass record
into `llm_review_pass`. Cost (tokens, USD) is tracked per pass.

### SINGLE

One model, one prompt, one response per center.

```
for center in scope:
    response = llm_call(model_a, render(prompt_template, center, deterministic_outputs))
    write routine_output(routine_code='llm.review_single', comment=response.text, ...)
```

Cheapest, fastest. Default for housekeeping cycles.

### SEQUENTIAL pipeline

A chain of models where each refines or critiques the prior output. The chain is
configured by `llm_review.models` with `role` markers.

```
draft = llm_call(model_drafter, render(prompt.draft, center, det_outputs))
critique = llm_call(model_critic, render(prompt.critic, center, det_outputs, draft))
final = llm_call(model_finaliser, render(prompt.final, center, det_outputs, draft, critique))
```

Each stage is logged separately in `routine_output` (different `step_index`). The
final stage's `comment` is the canonical rationale on the proposal.

### DEBATE (full multi-agent debate + judge)

Two opposing advocates ("KEEP advocate" vs "RETIRE advocate", or
"PC-target advocate" vs "CC-target advocate") argue, then a judge model issues the
verdict commentary. Number of rounds is configurable (`debate_rounds`, default 2).

```
positions = [
  llm_call(model_advocate_a, render(prompt.advocate_a, ...)),
  llm_call(model_advocate_b, render(prompt.advocate_b, ...)),
]
for r in range(debate_rounds - 1):
    positions[0] = llm_call(model_advocate_a, render(prompt.rebuttal_a, ..., positions))
    positions[1] = llm_call(model_advocate_b, render(prompt.rebuttal_b, ..., positions))
verdict = llm_call(model_judge, render(prompt.judge, ..., positions))
```

DEBATE is intentionally expensive — gate on `min_balance_eur` or specific outcomes
(e.g. only MERGE_MAP or REDESIGN). The pass records all advocate / rebuttal /
judgment messages with `step_index` so the cockpit can render a chat-style transcript.

### Determinism, caching, replay

- Calls cached by SHA-256 of `(prompt, model, params)`. Cache hits return prior text
  without re-billing.
- Temperature is config-controlled (default `0.0` for review modes).
- `llm_review_pass.summary` carries: response distribution, cost summary, top reasons
  (mined post-hoc by clustering rationales).

### Provider abstraction

```python
class LLMProvider(Protocol):
    name: str            # 'azure' or 'btp'
    def complete(self, model: str, messages: list[Message],
                 temperature: float, max_tokens: int,
                 metadata: dict) -> Completion: ...
    def estimate_cost(self, completion: Completion) -> float: ...
```

`AzureOpenAIProvider` and `SapBtpProvider` implement this. The pass uses whichever
provider/model is configured per stage.

## 5.10 Cockpit analytical tools (UI surface)

Independent of the pipeline routines, the cockpit exposes interactive tools (described
fully in §06):

- **Universe explorer** — filterable table of legacy centers with proposed outcomes.
- **Sankey** — legacy → target_object flows.
- **Hierarchy view** — old vs new side-by-side; click to drill.
- **Cluster explorer** — duplicate clusters with merge suggestions.
- **Inactivity heatmap** — months-since-last-posting × balance bucket.
- **Coverage map** — entities/regions × proposal status.
- **Naming preview** — apply naming convention, preview new IDs.
- **Run comparison** — diff two analysis_run versions.
- **LLM transcript viewer** — for SEQUENTIAL/DEBATE passes.
