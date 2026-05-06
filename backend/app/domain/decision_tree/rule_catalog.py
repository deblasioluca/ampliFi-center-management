"""Business-friendly rule catalog metadata.

The decision tree pipeline is built from "routines" (rules, ML models, LLM
passes, aggregates). Each routine has a technical ``code`` and a Python
implementation in ``app.domain.decision_tree.routines``.

This module enriches that with metadata aimed at *business users* — the
people who configure variants of the decision tree without reading Python:

- ``business_label``: short, non-technical name displayed in the UI
- ``description``: one paragraph explaining what the rule checks and why
- ``verdict_meanings``: maps each verdict the rule emits to a plain-language
  explanation (so "RETIRE" appears as "Center wird stillgelegt" in the UI)
- ``params``: enriched param schema with ``min``, ``max``, ``unit``,
  ``help_text``, ``friendly_label`` per parameter
- ``decides``: the cleansing or mapping outcomes this rule can drive — used
  to color-code the rule on the simulation results page

This is the single source of truth: the frontend ``rule-catalog`` endpoint
returns this dict, and config editors render forms from it.

Adding a new rule? Add an entry here at the same time you add the routine
class. Missing entries fall back to the routine's own ``params_schema``,
but business users will see the technical code instead of a friendly name.
"""

from __future__ import annotations

from typing import Any

# ── Catalog entries ──────────────────────────────────────────────────────


CATALOG: dict[str, dict[str, Any]] = {
    # ── Cleansing tree (V1) ──────────────────────────────────────────────
    "rule.posting_activity": {
        "business_label": "Inaktivität erkennen",
        "description": (
            "Markiert Cost Center die seit mehreren Monaten keine oder kaum "
            "Buchungen mehr hatten. Solche Center sind Stilllegungs-Kandidaten "
            "(RETIRE), weil sie keinen aktiven Geschäftszweck mehr abbilden."
        ),
        "decides": ["RETIRE", "KEEP"],
        "verdict_meanings": {
            "RETIRE": "Inaktiv → wird zur Stilllegung vorgeschlagen",
            "KEEP": "Aktiv genug → bleibt erhalten",
            "PASS": "Keine Aussage möglich (z.B. fehlende Daten)",
        },
        "params": {
            "posting_inactivity_threshold": {
                "friendly_label": "Inaktivitäts-Schwelle",
                "type": "integer",
                "default": 12,
                "min": 1,
                "max": 60,
                "unit": "Monate",
                "help_text": (
                    "Ein Cost Center, der seit mehr als dieser Anzahl Monaten "
                    "keine Buchungen hatte, gilt als inaktiv. Empfehlung: 12–24 "
                    "Monate. Niedrigere Werte = strenger (mehr RETIRE-Vorschläge)."
                ),
            },
            "posting_minimal_threshold": {
                "friendly_label": "Minimum-Buchungen",
                "type": "integer",
                "default": 0,
                "min": 0,
                "max": 100,
                "unit": "Buchungen",
                "help_text": (
                    "Anzahl Buchungen im Beobachtungsfenster, ab der ein Center "
                    "noch als aktiv gilt. 0 = jede Buchung zählt; 5 = vereinzelte "
                    "Buchungen werden ignoriert (Center trotzdem inaktiv)."
                ),
            },
        },
    },
    "rule.ownership": {
        "business_label": "Verantwortlichkeit prüfen",
        "description": (
            "Markiert Cost Center ohne gültigen Verantwortlichen (kein 'responsible' "
            "gesetzt oder Person nicht mehr im Unternehmen). Solche Center benötigen "
            "vor Migration eine Klärung — entweder neuer Owner oder Stilllegung."
        ),
        "decides": ["RETIRE", "REDESIGN", "KEEP"],
        "verdict_meanings": {
            "RETIRE": "Kein gültiger Owner → Center kandidiert für Stilllegung",
            "REDESIGN": "Owner unklar → muss neu zugeordnet werden",
            "KEEP": "Gültiger Owner vorhanden",
        },
        "params": {
            "require_active_employee": {
                "friendly_label": "Aktiven Mitarbeiter erforderlich",
                "type": "boolean",
                "default": True,
                "help_text": (
                    "Wenn aktiv, muss der Owner ein aktuell aktiver Mitarbeiter "
                    "sein. Wenn aus, reicht ein historischer Eintrag."
                ),
            },
        },
    },
    "rule.redundancy": {
        "business_label": "Duplikate / Redundanzen",
        "description": (
            "Erkennt Cost Center die inhaltlich Duplikate anderer Center sind "
            "(ähnliche Bezeichnung, gleicher Bereich, ähnliche Buchungsmuster). "
            "Solche Center können zusammengeführt werden (MERGE_MAP)."
        ),
        "decides": ["MERGE_MAP", "KEEP"],
        "verdict_meanings": {
            "MERGE_MAP": "Duplikat → wird in ein anderes Center zusammengeführt",
            "KEEP": "Eindeutig → bleibt erhalten",
        },
        "params": {
            "similarity_threshold": {
                "friendly_label": "Ähnlichkeits-Schwelle",
                "type": "number",
                "default": 0.92,
                "min": 0.5,
                "max": 1.0,
                "step": 0.01,
                "unit": "0..1",
                "help_text": (
                    "Wie ähnlich müssen Bezeichnung + Attribute sein damit zwei "
                    "Center als Duplikat gelten? 0.92 = sehr streng (nur fast "
                    "identische); 0.75 = locker (mehr Treffer aber auch mehr "
                    "False Positives)."
                ),
            },
        },
    },
    "rule.hierarchy_compliance": {
        "business_label": "Hierarchie-Konformität",
        "description": (
            "Prüft ob ein Cost Center an der richtigen Stelle in der Standard-"
            "Hierarchie hängt (z.B. CEMA-Hierarchie). Center die irgendwo lose "
            "hängen oder gar nicht in einer Hierarchie sind, brauchen Redesign."
        ),
        "decides": ["REDESIGN", "KEEP"],
        "verdict_meanings": {
            "REDESIGN": "Nicht in Standard-Hierarchie → muss neu eingehängt werden",
            "KEEP": "Korrekt eingehängt",
        },
        "params": {
            "strict_hierarchy_mode": {
                "friendly_label": "Strenge Hierarchie",
                "type": "boolean",
                "default": False,
                "help_text": (
                    "Wenn aktiv, müssen Center exakt in der Standard-Hierarchie "
                    "verankert sein. Locker (default): tolerante Zuordnung erlaubt."
                ),
            },
        },
    },
    "rule.cross_system_dependency": {
        "business_label": "System-Abhängigkeiten",
        "description": (
            "Erkennt Cost Center die in anderen SAP-Systemen referenziert sind "
            "(z.B. von WBS, Aufträgen, Bestellungen). Solche Center können nicht "
            "ohne Weiteres geändert werden — sie müssen mindestens als Mapping "
            "weiterleben."
        ),
        "decides": ["MERGE_MAP", "KEEP"],
        "verdict_meanings": {
            "MERGE_MAP": "Hat Abhängigkeiten → muss als Mapping erhalten bleiben",
            "KEEP": "Keine externen Abhängigkeiten",
        },
        "params": {},
    },
    # ── Mapping tree (V1) ────────────────────────────────────────────────
    "rule.bs_relevance": {
        "business_label": "Bilanz-Relevanz",
        "description": (
            "Prüft ob das Center bilanzrelevante Buchungen hat (B/S Konten). "
            "Solche Center werden zwingend als Cost Center (CC) im neuen Modell "
            "geführt — Profit Center allein reichen für Bilanz-Reporting nicht."
        ),
        "decides": ["CC", "PC_ONLY"],
        "verdict_meanings": {
            "CC": "Bilanzrelevant → wird CC + PC im neuen Modell",
            "PC_ONLY": "Nur GuV-relevant → reicht als Profit Center",
        },
        "params": {},
    },
    "rule.has_direct_revenue": {
        "business_label": "Direkter Umsatz",
        "description": (
            "Center mit direkten Umsatz-Buchungen werden zwingend als Profit "
            "Center geführt (PC_ONLY oder CC_AND_PC, je nach BS-Relevanz)."
        ),
        "decides": ["PC_ONLY", "CC_AND_PC"],
        "verdict_meanings": {
            "PC_ONLY": "Direkter Umsatz, keine BS → reines PC",
            "CC_AND_PC": "Direkter Umsatz + BS → CC und PC",
        },
        "params": {},
    },
    "rule.collects_project_costs": {
        "business_label": "Projektkosten-Sammler",
        "description": (
            "Center die als Projekt-Kostensammler dienen werden zu WBS-Elementen "
            "konvertiert (entweder echt oder statistisch je nach Verwendung)."
        ),
        "decides": ["WBS_REAL", "WBS_STAT"],
        "verdict_meanings": {
            "WBS_REAL": "Projekt mit echten Kosten → reales WBS-Element",
            "WBS_STAT": "Projekt nur für Reporting → statistisches WBS",
        },
        "params": {},
    },
    "rule.has_operational_costs": {
        "business_label": "Operative Kosten",
        "description": (
            "Center mit operativen Kosten (Gehälter, Mieten, Sachaufwand) sind "
            "klassische Cost Center und werden im neuen Modell entsprechend geführt."
        ),
        "decides": ["CC"],
        "verdict_meanings": {"CC": "Operative Kosten → klassisches Cost Center"},
        "params": {},
    },
    "rule.revenue_allocation_vehicle": {
        "business_label": "Umsatz-Verteilung",
        "description": (
            "Center die Umsätze auf andere Center verteilen (Allokationsfahrzeuge) "
            "können als reine PCs geführt oder ganz ersetzt werden."
        ),
        "decides": ["PC_ONLY", "RETIRE"],
        "verdict_meanings": {
            "PC_ONLY": "Verteilfahrzeug → bleibt als PC",
            "RETIRE": "Veraltetes Verteilfahrzeug → ablösbar",
        },
        "params": {},
    },
    "rule.cost_allocation_vehicle": {
        "business_label": "Kosten-Verteilung",
        "description": "Analog zu Umsatz-Verteilung, aber für Kostenverteilung.",
        "decides": ["CC", "RETIRE"],
        "verdict_meanings": {
            "CC": "Kostenverteiler → bleibt als CC",
            "RETIRE": "Veraltet → ablösbar",
        },
        "params": {},
    },
    "rule.info_only": {
        "business_label": "Nur statistisch",
        "description": (
            "Center die nur für Reporting/Statistik dienen (keine echten Buchungen) "
            "werden zu statistischen WBS oder ablösbar."
        ),
        "decides": ["WBS_STAT", "RETIRE"],
        "verdict_meanings": {
            "WBS_STAT": "Reporting-only → statistisches WBS",
            "RETIRE": "Reporting nicht mehr nötig → ablösbar",
        },
        "params": {},
    },
    "aggregate.combine_outcomes": {
        "business_label": "Ergebnisse zusammenführen",
        "description": (
            "Dieser Schritt fasst die Verdikte aller vorherigen Regeln zusammen "
            "und bestimmt den finalen Outcome (KEEP / RETIRE / MERGE_MAP / "
            "REDESIGN). Sollte am Ende der Pipeline stehen."
        ),
        "decides": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"],
        "verdict_meanings": {
            "KEEP": "Bleibt im neuen Modell erhalten",
            "RETIRE": "Wird stillgelegt",
            "MERGE_MAP": "Wird mit anderem Center zusammengeführt",
            "REDESIGN": "Muss neu konzipiert werden",
        },
        "params": {},
    },
    # ── V2 (CEMA Migration) ──────────────────────────────────────────────
    "v2.retire_flag": {
        "business_label": "RETIRE-Markierung (V2)",
        "description": (
            "V2: Center deren Bezeichnung ein bestimmtes Muster enthält (z.B. "
            "'_RETIRE') werden direkt als zu stilllegen markiert — bypassing "
            "der weiteren Analyse."
        ),
        "decides": ["RETIRE", "PASS"],
        "verdict_meanings": {
            "RETIRE": "Pattern erkannt → stillegen",
            "PASS": "Pattern nicht erkannt → weitere Analyse",
        },
        "params": {
            "retire_pattern": {
                "friendly_label": "Pattern für Stilllegung",
                "type": "string",
                "default": "_RETIRE",
                "help_text": (
                    "Substring der in der Bezeichnung vorkommen muss um als "
                    "Stilllegungs-Kandidat zu gelten. Achtung: Groß-/Kleinschreibung."
                ),
            },
        },
    },
    "v2.balance_migrate": {
        "business_label": "Bilanz-Migration (V2)",
        "description": (
            "V2: Bestimmt ob ein Center wegen seiner Bilanz-Charakteristik in die "
            "neue Welt migriert werden muss (vs. nur PC-Migration)."
        ),
        "decides": ["MIGRATE", "PASS"],
        "verdict_meanings": {
            "MIGRATE": "Bilanz-relevant → muss migriert werden",
            "PASS": "Keine zwingende Bilanz-Migration",
        },
        "params": {},
    },
    "v2.pc_approach": {
        "business_label": "PC-Gruppierungs-Strategie (V2)",
        "description": (
            "V2 — KERN-LOGIK: Bestimmt für jedes Center ob es 1:1 (eigenes PC) "
            "oder 1:n (mehrere CCs teilen sich ein PC) migriert wird. "
            "Gruppierungs-Regeln greifen auf Hierarchie-Level zu (z.B. 'alle "
            "Center unter L3 ROOT/EUROPE/DACH bekommen ein gemeinsames PC')."
        ),
        "decides": ["1:1", "1:n"],
        "verdict_meanings": {
            "1:1": "Eigener PC pro Center (klassisch)",
            "1:n": "Mehrere CCs teilen ein PC (canonical SAP m:1)",
        },
        "params": {
            "approach_rules": {
                "friendly_label": "Gruppierungs-Regeln",
                "type": "array",
                "default": [],
                "help_text": (
                    "Liste von Regeln die bestimmen welche Center zusammengruppiert "
                    "werden. Jede Regel hat 'match' (welche Center treffen?) und "
                    "'approach' (1:1 oder 1:n). Wird in einer dedizierten UI editiert."
                ),
            },
            "default_approach": {
                "friendly_label": "Standard-Strategie",
                "type": "string",
                "default": "1:1",
                "enum": ["1:1", "1:n"],
                "help_text": (
                    "Was passiert mit Centern die keine Regel matcht? "
                    "1:1 ist konservativ (jeder bekommt eigenes PC)."
                ),
            },
        },
    },
    "v2.combine_migration": {
        "business_label": "Migrations-Ergebnis bilden (V2)",
        "description": (
            "V2: Schlussschritt der Pipeline — kombiniert die Outputs der "
            "vorherigen V2-Schritte zu einem finalen Migrations-Plan pro Center."
        ),
        "decides": ["KEEP", "RETIRE", "MIGRATE"],
        "verdict_meanings": {
            "KEEP": "Bleibt unverändert",
            "RETIRE": "Wird stillgelegt",
            "MIGRATE": "Wird ins neue Modell migriert",
        },
        "params": {},
    },
}


# ── Preset variant templates ─────────────────────────────────────────────
# Pre-built decision tree configs for common scenarios. A business user can
# pick one as starting point and then tweak parameters.

PRESETS_V1: dict[str, dict[str, Any]] = {
    "strict": {
        "label": "Streng — viele RETIRE",
        "description": (
            "Aggressive Bereinigung: niedrige Inaktivitäts-Schwelle, strenge "
            "Hierarchie-Prüfung. Eignet sich wenn das Portfolio stark verkleinert "
            "werden soll. Erwarte viele RETIRE-Vorschläge die manuell überprüft "
            "werden müssen."
        ),
        "params": {
            "posting_inactivity_threshold": 6,
            "posting_minimal_threshold": 0,
            "similarity_threshold": 0.85,
            "strict_hierarchy_mode": True,
        },
    },
    "standard": {
        "label": "Standard — empfohlen",
        "description": (
            "Ausgewogene Balance zwischen Bereinigung und Stabilität. Default-"
            "Werte aus der Implementation_Plan §04 Spezifikation. Empfohlen für "
            "den ersten Durchlauf."
        ),
        "params": {
            "posting_inactivity_threshold": 12,
            "posting_minimal_threshold": 0,
            "similarity_threshold": 0.92,
            "strict_hierarchy_mode": False,
        },
    },
    "lenient": {
        "label": "Locker — wenig RETIRE",
        "description": (
            "Konservativ: hohe Inaktivitäts-Schwelle, lockere Duplikat-Erkennung. "
            "Eignet sich wenn maximal vorsichtig migriert werden soll und manuelle "
            "Überprüfung pro RETIRE teuer ist."
        ),
        "params": {
            "posting_inactivity_threshold": 24,
            "posting_minimal_threshold": 5,
            "similarity_threshold": 0.97,
            "strict_hierarchy_mode": False,
        },
    },
}


PRESETS_V2: dict[str, dict[str, Any]] = {
    "all_one_to_one": {
        "label": "Konservativ — 1:1 für alle",
        "description": (
            "Jeder Cost Center bekommt einen eigenen Profit Center (1:1). "
            "Sicher, einfach, aber führt zu vielen PCs. Sinnvoll wenn die "
            "Kosten-Granularität exakt erhalten bleiben muss."
        ),
        "approach_rules": [],
        "default_approach": "1:1",
    },
    "by_level3": {
        "label": "Gruppierung nach L3",
        "description": (
            "Alle Cost Center unter dem gleichen Level-3 Hierarchie-Knoten teilen "
            "sich einen Profit Center (1:n). Reduziert die PC-Zahl drastisch "
            "während die Reporting-Granularität auf L3-Ebene erhalten bleibt."
        ),
        "approach_rules": [
            {"match": {"hier_level": "L3"}, "approach": "1:n"},
        ],
        "default_approach": "1:1",
    },
    "by_country": {
        "label": "Gruppierung nach Land",
        "description": (
            "Cost Center werden nach dem Country-Code (`ccode`) gruppiert. "
            "Sinnvoll für regionale Reporting-Strukturen."
        ),
        "approach_rules": [
            {"match": {"by_field": "ccode"}, "approach": "1:n"},
        ],
        "default_approach": "1:1",
    },
}


# ── Public API ───────────────────────────────────────────────────────────


def get_rule_metadata(code: str) -> dict[str, Any] | None:
    """Return business-friendly metadata for a routine code, or None."""
    return CATALOG.get(code)


def list_rule_catalog(tree: str | None = None) -> list[dict[str, Any]]:
    """Return the full catalog as a list, optionally filtered by tree.

    Each entry includes:
    - code (technical identifier)
    - business_label, description, decides, verdict_meanings, params
    - tree (cleansing | mapping | None for v2/aggregate)
    - kind (rule | aggregate | ml | llm)
    """
    from app.domain.decision_tree.registry import boot_registry, get_registry

    reg = get_registry()
    if not reg.codes():
        boot_registry()

    out: list[dict[str, Any]] = []
    for routine in reg.list():
        meta = CATALOG.get(routine.code, {})
        if tree is not None and routine.tree != tree and meta.get("tree") != tree:
            continue
        entry = {
            "code": routine.code,
            "name": routine.name,
            "kind": routine.kind,
            "tree": routine.tree,
            # Business metadata (with sensible fallbacks)
            "business_label": meta.get("business_label", routine.name),
            "description": meta.get("description", ""),
            "decides": meta.get("decides", []),
            "verdict_meanings": meta.get("verdict_meanings", {}),
            "params": meta.get("params", {}),
            "params_schema": getattr(routine, "params_schema", None),
        }
        out.append(entry)
    return out


def list_presets(engine: str = "v1") -> dict[str, dict[str, Any]]:
    """Return preset templates for the given engine ('v1' or 'v2')."""
    if engine.lower() == "v2":
        return PRESETS_V2
    return PRESETS_V1


def get_preset(engine: str, name: str) -> dict[str, Any] | None:
    return list_presets(engine).get(name)


def build_v1_config_from_preset(preset_name: str) -> dict[str, Any]:
    """Build a full V1 AnalysisConfig.config dict from a preset name."""
    preset = PRESETS_V1.get(preset_name)
    if not preset:
        raise ValueError(f"Unknown V1 preset: {preset_name}")
    p = preset["params"]
    return {
        "pipeline": [
            {
                "routine": "rule.posting_activity",
                "enabled": True,
                "params": {
                    "posting_inactivity_threshold": p["posting_inactivity_threshold"],
                    "posting_minimal_threshold": p["posting_minimal_threshold"],
                },
            },
            {"routine": "rule.ownership", "enabled": True, "params": {}},
            {
                "routine": "rule.redundancy",
                "enabled": True,
                "params": {"similarity_threshold": p["similarity_threshold"]},
            },
            {
                "routine": "rule.hierarchy_compliance",
                "enabled": True,
                "params": {"strict_hierarchy_mode": p["strict_hierarchy_mode"]},
            },
            {"routine": "rule.cross_system_dependency", "enabled": True, "params": {}},
            {"routine": "rule.has_direct_revenue", "enabled": True, "params": {}},
            {"routine": "rule.collects_project_costs", "enabled": True, "params": {}},
            {"routine": "rule.has_operational_costs", "enabled": True, "params": {}},
            {"routine": "rule.revenue_allocation_vehicle", "enabled": True, "params": {}},
            {"routine": "rule.cost_allocation_vehicle", "enabled": True, "params": {}},
            {"routine": "rule.info_only", "enabled": True, "params": {}},
            {"routine": "aggregate.combine_outcomes", "enabled": True, "params": {}},
        ],
        "params": {
            "inactivity_threshold_months": p["posting_inactivity_threshold"],
            "posting_threshold": p["posting_minimal_threshold"],
            "strict_hierarchy_compliance": p["strict_hierarchy_mode"],
        },
        "_preset_origin": preset_name,
    }


def build_v2_config_from_preset(preset_name: str) -> dict[str, Any]:
    """Build a full V2 AnalysisConfig.config dict from a preset name."""
    preset = PRESETS_V2.get(preset_name)
    if not preset:
        raise ValueError(f"Unknown V2 preset: {preset_name}")
    return {
        "version": 2,
        "pipeline": [
            {"routine": "v2.retire_flag", "enabled": True, "params": {"retire_pattern": "_RETIRE"}},
            {"routine": "v2.balance_migrate", "enabled": True, "params": {}},
            {
                "routine": "v2.pc_approach",
                "enabled": True,
                "params": {
                    "approach_rules": preset["approach_rules"],
                    "default_approach": preset["default_approach"],
                },
            },
            {"routine": "v2.combine_migration", "enabled": True, "params": {}},
        ],
        "id_assignment": {
            "pc_prefix": "P",
            "cc_prefix": "C",
            "pc_start": 137,
            "cc_start": 1,
            "id_width": 5,
        },
        "_preset_origin": preset_name,
    }
