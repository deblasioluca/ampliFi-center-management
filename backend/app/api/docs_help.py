"""In-app documentation & help API (§19).

Serves contextual help content based on help keys and search queries.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

router = APIRouter()

# In-app help topics keyed by helpKey
HELP_TOPICS: dict[str, dict] = {
    "dashboard": {
        "title": "Dashboard",
        "summary": "Your at-a-glance overview of the cleanup project.",
        "content": (
            "## Dashboard\n\n"
            "The dashboard shows key metrics:\n"
            "- **Total entities, cost centers, profit centers, balances** loaded\n"
            "- **Active waves** and their status\n"
            "- **Quick actions** to navigate to common tasks\n\n"
            "### KPI Cards\n"
            "Each card shows the current count. Click to navigate to the detail view."
        ),
        "related": ["cockpit", "data.upload"],
    },
    "cockpit": {
        "title": "Cockpit Overview",
        "summary": "Central hub for managing waves, runs, and analysis results.",
        "content": (
            "## Cockpit\n\n"
            "The cockpit is the analyst's command center:\n\n"
            "### Waves\n"
            "- Create new waves with entity scope\n"
            "- Run analysis on wave data\n"
            "- Compare run versions\n"
            "- Lock proposals and assign review scopes\n\n"
            "### Analysis Runs\n"
            "- Execute the decision tree pipeline\n"
            "- View results with outcome distributions\n"
            "- Drill into individual proposals\n"
            "- Override outcomes with justification\n\n"
            "### Analytics\n"
            "- Outcome distribution charts\n"
            "- Entity heatmaps\n"
            "- Target object analysis\n"
            "- ML confidence histograms"
        ),
        "related": ["cockpit.run", "cockpit.wave", "cockpit.analytics"],
    },
    "cockpit.run": {
        "title": "Run Detail",
        "summary": "Detailed view of an analysis run with proposals and KPIs.",
        "content": (
            "## Run Detail\n\n"
            "Shows all proposals from a completed analysis run.\n\n"
            "### Universe Table\n"
            "- All cost centers with their proposed outcomes\n"
            "- Filter by outcome (KEEP/RETIRE/MERGE_MAP/REDESIGN)\n"
            "- Filter by entity code\n"
            "- Search by center number or name\n\n"
            "### Why Panel\n"
            "Click any proposal to see:\n"
            "- **Rule path**: Which decision tree rules fired\n"
            "- **ML scores**: Probability distribution across outcomes\n"
            "- **LLM commentary**: AI review analysis (if enabled)\n"
            "- **SHAP features**: Top factors influencing the prediction\n\n"
            "### Overrides\n"
            "Analysts can override any proposal with a required justification."
        ),
        "related": ["cockpit", "review"],
    },
    "cockpit.wave": {
        "title": "Wave Management",
        "summary": "Create and manage cleanup waves with entity scoping.",
        "content": (
            "## Wave Management\n\n"
            "### Wave Lifecycle\n"
            "```\n"
            "draft → analysing → proposed → locked → in_review → signed_off → closed\n"
            "```\n\n"
            "### Creating a Wave\n"
            "1. Click 'Create Wave'\n"
            "2. Enter name and description\n"
            "3. Select entities (company codes) in scope\n"
            "4. Run analysis\n\n"
            "### Full-Scope Analysis\n"
            "Check 'Full scope' to analyze all centers not in other waves."
        ),
        "related": ["cockpit", "cockpit.run"],
    },
    "cockpit.analytics": {
        "title": "Analytics Dashboard",
        "summary": "Visual analytics with charts, heatmaps, and Sankey diagrams.",
        "content": (
            "## Analytics Dashboard\n\n"
            "### Available Charts\n"
            "- **Outcome Donut**: KEEP/RETIRE/MERGE/REDESIGN distribution\n"
            "- **Target Stacked Bar**: Target object distribution by entity\n"
            "- **Entity × Outcome Heatmap**: Color-coded matrix\n"
            "- **ML Confidence Histogram**: Distribution of prediction confidence\n"
            "- **Sankey Flow**: Legacy → Target object flows\n"
            "- **Balance vs Activity Bubble**: Posting activity vs balance amounts\n\n"
            "### Filters\n"
            "Select wave and run to filter all charts."
        ),
        "related": ["cockpit.run"],
    },
    "review": {
        "title": "Stakeholder Review",
        "summary": "Review and approve proposals assigned to your scope.",
        "content": (
            "## Reviewer Guide\n\n"
            "### Your Task\n"
            "Review each cost center proposal and decide:\n"
            "- **Approve**: Accept the proposed outcome\n"
            "- **Not Required**: Mark as not needing action\n"
            "- **Add Comment**: Provide feedback\n"
            "- **Request Changes**: Ask the analyst to reconsider\n\n"
            "### Navigation\n"
            "- Use filters to show Pending/Approved/Commented items\n"
            "- Group by entity or outcome for faster review\n"
            "- Search by center number or name\n"
            "- Use Bulk Approve for large scopes\n\n"
            "### Keyboard Shortcuts\n"
            "- `Tab` / `Shift+Tab`: Navigate between items\n"
            "- `Enter`: Expand details"
        ),
        "related": ["cockpit.run"],
    },
    "data.upload": {
        "title": "Data Upload",
        "summary": "Upload cost centers, profit centers, entities, and balances.",
        "content": (
            "## Data Upload\n\n"
            "### Supported Types\n"
            "- **Entities**: Company codes (CCODE, name, city, country)\n"
            "- **Cost Centers**: Legacy cost centers (COAREA, CCTR, CCODE, names)\n"
            "- **Profit Centers**: Profit center master data\n"
            "- **Balances**: Financial balances (COAREA, CCTR, period, amounts)\n"
            "- **Hierarchies**: Cost center hierarchy structures\n\n"
            "### Upload Process\n"
            "1. Select data type\n"
            "2. Download the template CSV\n"
            "3. Fill in your data\n"
            "4. Upload the file\n"
            "5. Validate (checks format, duplicates, referential integrity)\n"
            "6. Load (imports into the database)\n\n"
            "### Tips\n"
            "- For large files (>100K rows), upload is processed asynchronously\n"
            "- Balance uploads should include fiscal_year and period columns"
        ),
        "related": ["admin.sap"],
    },
    "admin.users": {
        "title": "User Administration",
        "summary": "Manage user accounts and roles.",
        "content": (
            "## User Administration\n\n"
            "### Roles\n"
            "- **admin**: Full access to all features\n"
            "- **analyst**: Manage waves, run analysis, assign scopes\n"
            "- **viewer**: Read-only access to dashboards\n"
            "- **reviewer**: External stakeholder with scope-limited access\n\n"
            "### Actions\n"
            "- Create new users with email + password\n"
            "- Edit roles and permissions\n"
            "- Deactivate accounts (cannot deactivate yourself)"
        ),
        "related": ["admin.config"],
    },
    "admin.sap": {
        "title": "SAP Connection Management",
        "summary": "Configure SAP system connections for data extraction and MDG export.",
        "content": (
            "## SAP Connections\n\n"
            "### Connection Types\n"
            "- **OData**: SAP Gateway services for reading master data\n"
            "- **ADT (ABAP Development Tools)**: For custom reports and BAdI calls\n"
            "- **SOAP**: Legacy web services\n\n"
            "### Configuration\n"
            "- Endpoint URL, username, password\n"
            "- Client number and language\n"
            "- SSL verification settings\n\n"
            "### Testing\n"
            "Use 'Test Connection' to verify connectivity before using in waves."
        ),
        "related": ["data.upload"],
    },
    "admin.llm": {
        "title": "LLM Configuration",
        "summary": "Configure AI language model for review passes and chat assistant.",
        "content": (
            "## LLM Settings\n\n"
            "### Providers\n"
            "- **Azure OpenAI**: Enterprise Azure-hosted GPT models\n"
            "- **SAP BTP GenAI Hub**: SAP's AI Foundation models\n\n"
            "### Settings\n"
            "- Provider type (azure / btp)\n"
            "- API endpoint and credentials\n"
            "- Model name and deployment\n"
            "- Temperature (default 0.0 for deterministic review)\n\n"
            "### Review Modes\n"
            "- **SINGLE**: One LLM call per center (cheapest)\n"
            "- **SEQUENTIAL**: Draft → Critic → Finalizer pipeline\n"
            "- **DEBATE**: Advocate A vs B + Judge (most thorough)\n\n"
            "### Cost Controls\n"
            "- Per-call limit (default $1.00)\n"
            "- Daily cap (default $50.00)\n"
            "- Monthly cap (default $500.00)"
        ),
        "related": ["cockpit.run"],
    },
    "housekeeping": {
        "title": "Housekeeping Cycles",
        "summary": "Monthly review of cost center health on the target environment.",
        "content": (
            "## Housekeeping\n\n"
            "### Purpose\n"
            "After cleanup, housekeeping runs monthly to catch:\n"
            "- Inactive centers (no postings in 12 months)\n"
            "- Missing owners\n"
            "- Anomalous patterns\n"
            "- Low-volume usage\n\n"
            "### Lifecycle\n"
            "```\n"
            "scheduled → running → review_open → closed\n"
            "```\n\n"
            "### Owner Sign-off\n"
            "Owners receive digest emails with deep links to review their flagged centers.\n"
            "Each item can be: KEEP, CLOSE, or DEFER."
        ),
        "related": ["cockpit"],
    },
}


@router.get("/help/topics")
def list_help_topics() -> dict:
    """List all available help topics."""
    return {
        "topics": [
            {"key": key, "title": topic["title"], "summary": topic["summary"]}
            for key, topic in HELP_TOPICS.items()
        ]
    }


@router.get("/help/topics/{help_key}")
def get_help_topic(help_key: str) -> dict:
    """Get a specific help topic by key."""
    topic = HELP_TOPICS.get(help_key)
    if not topic:
        return {"error": "Topic not found", "available_keys": list(HELP_TOPICS.keys())}
    return {
        "key": help_key,
        "title": topic["title"],
        "summary": topic["summary"],
        "content": topic["content"],
        "related": [
            {"key": k, "title": HELP_TOPICS[k]["title"]}
            for k in topic.get("related", [])
            if k in HELP_TOPICS
        ],
    }


@router.get("/help/search")
def search_help(q: str = Query(..., min_length=2)) -> dict:
    """Search help topics by keyword."""
    q_lower = q.lower()
    results = []
    for key, topic in HELP_TOPICS.items():
        score = 0
        if q_lower in topic["title"].lower():
            score += 10
        if q_lower in topic["summary"].lower():
            score += 5
        if q_lower in topic["content"].lower():
            score += 2
        if score > 0:
            results.append(
                {
                    "key": key,
                    "title": topic["title"],
                    "summary": topic["summary"],
                    "score": score,
                }
            )
    results.sort(key=lambda x: x["score"], reverse=True)
    return {"query": q, "results": results}
