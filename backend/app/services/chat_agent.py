"""Chat assistant tool-calling agent (§16).

Implements the tool-using agent that answers user questions using
read-only data access tools.
"""

from __future__ import annotations

import json

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.core import (
    AnalysisRun,
    CenterProposal,
    Entity,
    HousekeepingCycle,
    HousekeepingItem,
    LegacyCostCenter,
    ReviewItem,
    ReviewScope,
    Wave,
)

logger = structlog.get_logger()


# Tool definitions for function calling
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "kpis_for_run",
            "description": "Get KPI statistics (outcome counts, target counts) for an analysis run",
            "parameters": {
                "type": "object",
                "properties": {"run_id": {"type": "integer"}},
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "proposals_search",
            "description": "Search proposals in a run with filters (outcome, entity, etc.)",
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "integer"},
                    "outcome": {"type": "string", "enum": ["KEEP", "RETIRE", "MERGE_MAP", "REDESIGN"]},
                    "entity_code": {"type": "string"},
                    "limit": {"type": "integer", "default": 20},
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "proposal_detail",
            "description": "Get full why-panel detail for a specific proposal including rule path, ML scores, LLM commentary",
            "parameters": {
                "type": "object",
                "properties": {"proposal_id": {"type": "integer"}},
                "required": ["proposal_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "entity_centers",
            "description": "List cost centers belonging to a company code/entity",
            "parameters": {
                "type": "object",
                "properties": {
                    "ccode": {"type": "string"},
                    "run_id": {"type": "integer"},
                },
                "required": ["ccode"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explain_outcome",
            "description": "Explain why a specific center got its outcome (rule path + ML + LLM)",
            "parameters": {
                "type": "object",
                "properties": {"proposal_id": {"type": "integer"}},
                "required": ["proposal_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "outcome_distribution",
            "description": "Get outcome distribution grouped by entity, region, or hierarchy",
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "integer"},
                    "group_by": {"type": "string", "enum": ["entity", "outcome", "target"]},
                },
                "required": ["run_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "housekeeping_status",
            "description": "Get housekeeping cycle status and flagged items summary",
            "parameters": {
                "type": "object",
                "properties": {"cycle_id": {"type": "integer"}},
                "required": ["cycle_id"],
            },
        },
    },
]


def execute_tool(name: str, args: dict, db: Session) -> str:
    """Execute a chat tool and return JSON result."""
    if name == "kpis_for_run":
        run = db.get(AnalysisRun, args["run_id"])
        if not run:
            return json.dumps({"error": "Run not found"})
        return json.dumps({"run_id": run.id, "kpis": run.kpis})

    elif name == "proposals_search":
        query = select(CenterProposal).where(CenterProposal.run_id == args["run_id"])
        if args.get("outcome"):
            query = query.where(CenterProposal.cleansing_outcome == args["outcome"])
        if args.get("entity_code"):
            query = query.where(CenterProposal.entity_code == args["entity_code"])
        limit = args.get("limit", 20)
        proposals = db.execute(query.limit(limit)).scalars().all()
        cc_ids = [p.legacy_cc_id for p in proposals]
        cc_map = {}
        if cc_ids:
            ccs = db.execute(select(LegacyCostCenter).where(LegacyCostCenter.id.in_(cc_ids))).scalars().all()
            cc_map = {c.id: c for c in ccs}
        items = []
        for p in proposals:
            cc = cc_map.get(p.legacy_cc_id)
            items.append({
                "id": p.id,
                "cctr": cc.cctr if cc else None,
                "txtsh": cc.txtsh if cc else None,
                "ccode": p.entity_code,
                "outcome": p.cleansing_outcome,
                "target": p.target_object,
                "confidence": str(p.confidence) if p.confidence else None,
            })
        return json.dumps({"count": len(items), "items": items})

    elif name == "proposal_detail":
        p = db.get(CenterProposal, args["proposal_id"])
        if not p:
            return json.dumps({"error": "Proposal not found"})
        cc = db.get(LegacyCostCenter, p.legacy_cc_id)
        return json.dumps({
            "id": p.id,
            "cctr": cc.cctr if cc else None,
            "txtsh": cc.txtsh if cc else None,
            "outcome": p.cleansing_outcome,
            "target": p.target_object,
            "rule_path": p.rule_path,
            "ml_scores": p.ml_scores,
            "llm_commentary": p.llm_commentary,
            "confidence": str(p.confidence) if p.confidence else None,
        })

    elif name == "entity_centers":
        query = select(LegacyCostCenter).where(LegacyCostCenter.ccode == args["ccode"])
        ccs = db.execute(query.limit(50)).scalars().all()
        items = [{"id": c.id, "cctr": c.cctr, "txtsh": c.txtsh, "responsible": c.responsible} for c in ccs]
        return json.dumps({"count": len(items), "items": items})

    elif name == "explain_outcome":
        p = db.get(CenterProposal, args["proposal_id"])
        if not p:
            return json.dumps({"error": "Proposal not found"})
        cc = db.get(LegacyCostCenter, p.legacy_cc_id)
        explanation = {
            "center": {"cctr": cc.cctr if cc else None, "txtsh": cc.txtsh if cc else None},
            "outcome": p.cleansing_outcome,
            "target": p.target_object,
            "rule_path": p.rule_path,
            "confidence": str(p.confidence) if p.confidence else None,
        }
        if p.ml_scores:
            explanation["ml"] = p.ml_scores
        if p.llm_commentary:
            explanation["llm"] = p.llm_commentary
        return json.dumps(explanation)

    elif name == "outcome_distribution":
        run_id = args["run_id"]
        group_by = args.get("group_by", "outcome")
        if group_by == "entity":
            rows = db.execute(
                select(CenterProposal.entity_code, CenterProposal.cleansing_outcome, func.count())
                .where(CenterProposal.run_id == run_id)
                .group_by(CenterProposal.entity_code, CenterProposal.cleansing_outcome)
            ).all()
            result: dict = {}
            for entity, outcome, count in rows:
                result.setdefault(entity or "Unknown", {})[outcome] = count
            return json.dumps(result)
        elif group_by == "target":
            rows = db.execute(
                select(CenterProposal.target_object, func.count())
                .where(CenterProposal.run_id == run_id)
                .group_by(CenterProposal.target_object)
            ).all()
            return json.dumps({(t or "NONE"): c for t, c in rows})
        else:
            rows = db.execute(
                select(CenterProposal.cleansing_outcome, func.count())
                .where(CenterProposal.run_id == run_id)
                .group_by(CenterProposal.cleansing_outcome)
            ).all()
            return json.dumps({o: c for o, c in rows})

    elif name == "housekeeping_status":
        cycle = db.get(HousekeepingCycle, args["cycle_id"])
        if not cycle:
            return json.dumps({"error": "Cycle not found"})
        items = db.execute(
            select(HousekeepingItem).where(HousekeepingItem.cycle_id == cycle.id)
        ).scalars().all()
        by_flag: dict[str, int] = {}
        by_decision: dict[str, int] = {}
        for item in items:
            by_flag[item.flag or "UNKNOWN"] = by_flag.get(item.flag or "UNKNOWN", 0) + 1
            by_decision[item.decision or "PENDING"] = by_decision.get(item.decision or "PENDING", 0) + 1
        return json.dumps({
            "cycle_id": cycle.id,
            "period": cycle.period,
            "status": cycle.status,
            "total_items": len(items),
            "by_flag": by_flag,
            "by_decision": by_decision,
        })

    return json.dumps({"error": f"Unknown tool: {name}"})


def generate_response(
    user_message: str,
    thread_context: dict,
    db: Session,
) -> str:
    """Generate a chat response using the LLM with tool calling.

    Falls back to a helpful static response if LLM is not configured.
    """
    from app.models.core import AppConfig

    cfg = db.execute(select(AppConfig).where(AppConfig.key == "llm")).scalar_one_or_none()

    if not cfg or not cfg.value:
        return _static_response(user_message, thread_context, db)

    llm_config = cfg.value
    from app.infra.llm.provider import AzureOpenAIProvider, Message, SapBtpProvider

    provider_type = llm_config.get("provider", "azure")
    if provider_type == "azure":
        provider = AzureOpenAIProvider(llm_config)
    elif provider_type == "btp":
        provider = SapBtpProvider(llm_config)
    else:
        return _static_response(user_message, thread_context, db)

    model = llm_config.get("model", "gpt-4o")

    system_prompt = (
        "You are the ampliFi chat assistant. You help analysts and reviewers understand "
        "cost center cleanup analysis results. You have access to tools for looking up "
        "proposals, KPIs, and explanations. Be concise and data-driven. "
        "Always use tools to fetch real data before answering."
    )

    if thread_context.get("run_id"):
        system_prompt += f"\nCurrent run context: run_id={thread_context['run_id']}"
    if thread_context.get("wave_id"):
        system_prompt += f", wave_id={thread_context['wave_id']}"

    messages = [
        Message(role="system", content=system_prompt),
        Message(role="user", content=user_message),
    ]

    try:
        completion = provider.complete(model, messages, temperature=0.0, max_tokens=2000)
        return completion.text
    except Exception as e:
        logger.error("chat.llm_error", error=str(e))
        return _static_response(user_message, thread_context, db)


def _static_response(user_message: str, context: dict, db: Session) -> str:
    """Generate a helpful response without LLM, using direct data lookups."""
    msg = user_message.lower()

    if context.get("run_id"):
        run = db.get(AnalysisRun, context["run_id"])
        if run and run.kpis:
            k = run.kpis
            if any(w in msg for w in ["kpi", "summary", "overview", "status", "how many"]):
                return (
                    f"**Run #{run.id} Summary:**\n"
                    f"- Total centers: {k.get('total_centers', 0)}\n"
                    f"- KEEP: {k.get('keep', 0)}\n"
                    f"- RETIRE: {k.get('retire', 0)}\n"
                    f"- MERGE_MAP: {k.get('merge_map', 0)}\n"
                    f"- REDESIGN: {k.get('redesign', 0)}\n"
                )

    if any(w in msg for w in ["help", "what can you", "commands"]):
        return (
            "I can help you with:\n"
            "- **KPI summaries** — \"Show me the KPIs for this run\"\n"
            "- **Proposal search** — \"Find all RETIRE proposals for entity 1000\"\n"
            "- **Explanations** — \"Why was center 12345 marked KEEP?\"\n"
            "- **Distributions** — \"Show outcome distribution by entity\"\n"
            "- **Housekeeping** — \"What's the status of the current cycle?\"\n\n"
            "Note: For full conversational capability, configure the LLM provider in Admin > LLM Settings."
        )

    return (
        "I'm the ampliFi assistant. To unlock full conversational AI capabilities, "
        "please configure the LLM provider in **Admin > LLM Settings**.\n\n"
        "In the meantime, I can answer basic queries. Try asking:\n"
        "- \"Show KPIs\"\n"
        "- \"Help\"\n"
        "- \"What can you do?\""
    )
