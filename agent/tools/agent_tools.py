"""
============================================================================
HealthStream v2 — AI Agent Tools (Step 8)
============================================================================
These are the TOOLS the AI agent can call. Each tool queries Redis
for real-time context. This is the local version of Confluent's
Real-Time Context Engine (MCP pattern).

TOOL LIST:
  1. get_patient_context    → Full patient profile with vitals, conditions, meds
  2. get_patients_needing_attention → Patients with concerning trends
  3. get_pipeline_health    → Source status, lag, throughput
  4. get_dlq_analysis       → DLQ error categorization and patterns
  5. get_patient_list       → All known patients
  6. get_source_impact      → What happens if a source goes down
============================================================================
"""

import json
from datetime import datetime

import redis

REDIS_HOST = "localhost"
REDIS_PORT = 6379

_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def get_patient_context(patient_id: str) -> str:
    """Get full real-time context for a specific patient."""
    data = _redis.get(f"patient:{patient_id}")
    if not data:
        return json.dumps({"error": f"No data found for patient {patient_id}"})

    context = json.loads(data)
    summary = {
        "patient_id": context["patient_id"],
        "age": context.get("patient_age", "unknown"),
        "gender": context.get("patient_gender", "unknown"),
        "current_vitals": {},
        "vital_trends": {},
        "conditions": context.get("conditions", []),
        "medications": context.get("medications", []),
        "last_updated": context.get("last_updated", "unknown"),
    }

    for vital_name, vital_data in context.get("vitals", {}).items():
        summary["current_vitals"][vital_name] = {
            "value": vital_data.get("value"),
            "units": vital_data.get("units", ""),
        }
        if "trend" in vital_data:
            summary["vital_trends"][vital_name] = vital_data["trend"]

    return json.dumps(summary, indent=2)


def get_patients_needing_attention() -> str:
    """Find patients with concerning vital sign trends."""
    patient_ids = _redis.smembers("patients:all")
    concerning = []

    for pid in patient_ids:
        data = _redis.get(f"patient:{pid}")
        if not data:
            continue
        context = json.loads(data)
        reasons = []

        for vital_name, vital_data in context.get("vitals", {}).items():
            trend = vital_data.get("trend", {})
            direction = trend.get("direction", "stable")
            pct = abs(trend.get("pct_change", 0))
            value = vital_data.get("value", 0)

            # Flag concerning patterns
            if vital_name == "heart_rate" and value and value > 100:
                reasons.append(f"Elevated heart rate: {value}")
            if vital_name == "heart_rate" and direction == "rising" and pct > 10:
                reasons.append(f"Heart rate rising {pct:.1f}%")
            if vital_name == "oxygen_saturation" and value and value < 94:
                reasons.append(f"Low oxygen: {value}%")
            if vital_name == "oxygen_saturation" and direction == "falling" and pct > 3:
                reasons.append(f"Oxygen falling {pct:.1f}%")
            if vital_name == "systolic_bp" and value and value > 160:
                reasons.append(f"High systolic BP: {value}")
            if vital_name == "temperature" and value and value > 100.4:
                reasons.append(f"Fever: {value}°F")

        if reasons:
            concerning.append({
                "patient_id": pid,
                "age": context.get("patient_age", "?"),
                "gender": context.get("patient_gender", "?"),
                "conditions": [c["name"] for c in context.get("conditions", [])],
                "reasons": reasons,
            })

    # Sort by number of reasons (most concerning first)
    concerning.sort(key=lambda x: len(x["reasons"]), reverse=True)
    return json.dumps({"patients_needing_attention": concerning[:20], "total_flagged": len(concerning)}, indent=2)


def get_pipeline_health() -> str:
    """Get integration pipeline health status."""
    health_data = _redis.get("health:latest")
    source_data = _redis.get("source:health")
    stats_data = _redis.get("stats:global")
    total_patients = _redis.get("stats:total_patients")

    result = {
        "total_patients_in_redis": total_patients or 0,
        "source_health": json.loads(source_data) if source_data else {},
        "pipeline_stats": json.loads(stats_data) if stats_data else {},
        "infrastructure": {},
    }

    if health_data:
        full = json.loads(health_data)
        result["infrastructure"] = {
            "schema_registry": full.get("schema_registry", {}),
            "ksqldb": full.get("ksqldb", {}),
            "connectors": full.get("connectors", {}),
            "overall_status": full.get("overall_status", "unknown"),
        }
        result["topic_message_counts"] = full.get("topics", {})

    return json.dumps(result, indent=2)


def get_dlq_analysis() -> str:
    """Analyze Dead Letter Queue errors — categorize and find patterns."""
    dlq_data = _redis.get("dlq:recent_errors")
    if not dlq_data:
        return json.dumps({"message": "No DLQ errors found. Pipeline is clean."})

    errors = json.loads(dlq_data)
    # Categorize errors
    categories = {}
    sources = {}
    for err in errors:
        reason = err.get("error_reason", "unknown")
        # Extract category from reason (e.g., "OUT_OF_RANGE: ..." → "OUT_OF_RANGE")
        category = reason.split(":")[0] if ":" in reason else reason
        categories[category] = categories.get(category, 0) + 1

        source = err.get("source_system", "unknown")
        sources[source] = sources.get(source, 0) + 1

    return json.dumps({
        "total_dlq_messages": len(errors),
        "error_categories": categories,
        "errors_by_source": sources,
        "recent_errors": errors[-5:],
        "recommendation": (
            "Investigate the most common error category. "
            "If OUT_OF_RANGE, check if source system changed units. "
            "If MISSING_VALUE, check source system for NULL fields."
        ),
    }, indent=2)


def get_patient_list() -> str:
    """List all patients currently tracked in the system."""
    patient_ids = list(_redis.smembers("patients:all"))
    patients = []
    for pid in patient_ids[:50]:  # Limit to 50
        data = _redis.get(f"patient:{pid}")
        if data:
            ctx = json.loads(data)
            patients.append({
                "patient_id": pid,
                "age": ctx.get("patient_age", "?"),
                "gender": ctx.get("patient_gender", "?"),
                "conditions_count": len(ctx.get("conditions", [])),
                "medications_count": len(ctx.get("medications", [])),
                "has_vitals": bool(ctx.get("vitals", {})),
            })

    return json.dumps({
        "total_patients": len(patient_ids),
        "patients": patients,
    }, indent=2)


def get_source_impact(source_name: str) -> str:
    """Analyze what happens if a specific source goes down."""
    source_map = {
        "csv": {"label": "Source A (CSV Lab Export)", "topic": "patient-vitals", "data_type": "vital signs"},
        "postgres": {"label": "Source B (PostgreSQL EHR)", "topic": "patient-conditions", "data_type": "conditions/diagnoses"},
        "mysql": {"label": "Source C (MySQL Pharmacy)", "topic": "patient-medications", "data_type": "medications"},
    }

    source = None
    for key, val in source_map.items():
        if key in source_name.lower():
            source = val
            break

    if not source:
        return json.dumps({"error": f"Unknown source. Use: csv, postgres, or mysql"})

    total_patients = _redis.scard("patients:all")
    patient_ids = list(_redis.smembers("patients:all"))
    affected = 0
    for pid in patient_ids:
        data = _redis.get(f"patient:{pid}")
        if data:
            ctx = json.loads(data)
            if source["data_type"] == "vital signs" and ctx.get("vitals"):
                affected += 1
            elif source["data_type"] == "conditions/diagnoses" and ctx.get("conditions"):
                affected += 1
            elif source["data_type"] == "medications" and ctx.get("medications"):
                affected += 1

    return json.dumps({
        "source": source["label"],
        "topic_affected": source["topic"],
        "data_type_lost": source["data_type"],
        "patients_affected": affected,
        "total_patients": total_patients,
        "impact_percentage": round(affected / max(total_patients, 1) * 100, 1),
        "recovery_plan": {
            "csv": "Restart the Python producer. It will resume from the CSV file.",
            "postgres": "JDBC connector auto-retries. Check connector status. If FAILED, restart via REST API.",
            "mysql": "Debezium resumes from last GTID position. No data loss if binlog retention > downtime.",
        }.get(source_name.lower().split()[0] if " " in source_name else source_name.lower(), "Check connector status"),
        "retention_safety": "Topics have 30-90 day retention. Data is safe in Kafka even if consumer is down.",
    }, indent=2)


# ── Tool definitions for OpenAI function calling ────────────────────────────
TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_patient_context",
            "description": "Get full real-time context for a specific patient including current vitals, trends, conditions, and medications.",
            "parameters": {
                "type": "object",
                "properties": {
                    "patient_id": {
                        "type": "string",
                        "description": "The patient UUID to look up",
                    }
                },
                "required": ["patient_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_patients_needing_attention",
            "description": "Find all patients with concerning vital sign trends that may need clinical attention. Returns patients sorted by urgency.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_pipeline_health",
            "description": "Get the current health status of the entire data integration pipeline including source systems, connectors, Kafka topics, and Redis.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dlq_analysis",
            "description": "Analyze the Dead Letter Queue to find error patterns, categorize failures, and recommend fixes.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_patient_list",
            "description": "List all patients currently tracked in the system with summary info.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_source_impact",
            "description": "Analyze the impact if a specific data source goes down. Shows affected patients and recovery plan.",
            "parameters": {
                "type": "object",
                "properties": {
                    "source_name": {
                        "type": "string",
                        "description": "The source to analyze: 'csv', 'postgres', or 'mysql'",
                    }
                },
                "required": ["source_name"],
            },
        },
    },
]

# Map function names to actual functions
TOOL_FUNCTIONS = {
    "get_patient_context": get_patient_context,
    "get_patients_needing_attention": get_patients_needing_attention,
    "get_pipeline_health": get_pipeline_health,
    "get_dlq_analysis": get_dlq_analysis,
    "get_patient_list": get_patient_list,
    "get_source_impact": get_source_impact,
}
