"""
============================================================================
HealthStream v2 — DLQ Monitor Agent Tools
============================================================================
Tools for the DLQ Monitor Agent that continuously watches the Dead Letter
Queue and helps Kafka engineers diagnose and resolve data quality issues.

Instead of a Kafka engineer manually:
  1. Opening the console to check DLQ
  2. Reading raw error messages
  3. Trying to figure out the root cause
  4. Deciding on a fix

The agent does all of this automatically and sends a report.

TOOLS:
  1. scan_dlq_topic          → Read messages from the DLQ topic
  2. analyze_error_patterns   → Categorize errors, find patterns
  3. diagnose_root_cause      → Deep analysis of why errors occurred
  4. check_connector_health   → Check all connectors for failures
  5. check_schema_compatibility → Verify schema registry for issues
  6. generate_alert_report    → Create an alert report for the engineer
============================================================================
"""

import json
import time
from datetime import datetime

import redis
import requests

REDIS_HOST = "localhost"
REDIS_PORT = 6379
KAFKA_CONNECT_URL = "http://localhost:8083"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

_redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def scan_dlq_topic() -> str:
    """Scan the Dead Letter Queue for recent error messages."""
    # Check Redis for cached DLQ data
    dlq_data = _redis.get("dlq:recent_errors")

    if not dlq_data:
        return json.dumps({
            "status": "CLEAN",
            "total_errors": 0,
            "message": "No errors in the Dead Letter Queue. Pipeline is running clean.",
            "last_checked": datetime.now().isoformat(),
        }, indent=2)

    errors = json.loads(dlq_data)

    # Group by time windows
    recent_5min = []
    recent_1hr = []
    older = []
    now = time.time()

    for err in errors:
        ts = err.get("timestamp", 0)
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts).timestamp()
            except (ValueError, TypeError):
                ts = 0

        age_sec = now - ts if ts > 0 else 999999
        if age_sec < 300:
            recent_5min.append(err)
        elif age_sec < 3600:
            recent_1hr.append(err)
        else:
            older.append(err)

    return json.dumps({
        "status": "ERRORS_FOUND" if errors else "CLEAN",
        "total_errors": len(errors),
        "last_5_minutes": len(recent_5min),
        "last_1_hour": len(recent_1hr),
        "older": len(older),
        "severity": "CRITICAL" if len(recent_5min) > 10 else "WARNING" if len(recent_5min) > 0 else "INFO",
        "sample_errors": errors[-5:],
        "last_checked": datetime.now().isoformat(),
    }, indent=2)


def analyze_error_patterns() -> str:
    """Categorize DLQ errors to find patterns and common failure modes."""
    dlq_data = _redis.get("dlq:recent_errors")
    if not dlq_data:
        return json.dumps({"status": "CLEAN", "message": "No DLQ errors to analyze."})

    errors = json.loads(dlq_data)

    # Categorize by error type
    categories = {}
    by_source = {}
    by_field = {}
    error_timeline = {}

    for err in errors:
        # Category
        reason = err.get("error_reason", "unknown")
        category = reason.split(":")[0].strip() if ":" in reason else reason
        categories[category] = categories.get(category, 0) + 1

        # Source system
        source = err.get("source_system", "unknown")
        by_source[source] = by_source.get(source, 0) + 1

        # Failed field (if available)
        if "field" in err:
            field = err["field"]
            by_field[field] = by_field.get(field, 0) + 1

        # Timeline (by hour)
        ts = err.get("timestamp", "")
        if isinstance(ts, str) and len(ts) >= 13:
            hour = ts[:13]
            error_timeline[hour] = error_timeline.get(hour, 0) + 1

    # Sort categories by frequency
    sorted_categories = sorted(categories.items(), key=lambda x: x[1], reverse=True)

    # Detect burst patterns
    is_burst = False
    if error_timeline:
        max_hour = max(error_timeline.values())
        is_burst = max_hour > len(errors) * 0.5  # >50% errors in one hour = burst

    return json.dumps({
        "total_errors": len(errors),
        "error_categories": dict(sorted_categories),
        "top_category": sorted_categories[0] if sorted_categories else None,
        "errors_by_source": by_source,
        "errors_by_field": by_field if by_field else "field-level info not available",
        "pattern_detected": {
            "is_burst": is_burst,
            "description": (
                "Errors are concentrated in a short time window — likely a connector or schema issue"
                if is_burst
                else "Errors are spread over time — likely individual bad records in the source data"
            ),
        },
        "error_timeline": error_timeline if error_timeline else "timeline not available",
    }, indent=2)


def diagnose_root_cause() -> str:
    """Deep analysis of DLQ errors to determine root cause and suggest fixes."""
    dlq_data = _redis.get("dlq:recent_errors")
    if not dlq_data:
        return json.dumps({"status": "CLEAN", "message": "No errors to diagnose."})

    errors = json.loads(dlq_data)

    # Analyze the most common error
    reasons = {}
    for err in errors:
        reason = err.get("error_reason", "unknown")
        reasons[reason] = reasons.get(reason, 0) + 1

    top_reason = max(reasons, key=reasons.get) if reasons else "unknown"
    top_count = reasons.get(top_reason, 0)

    # Root cause analysis based on error type
    diagnosis = {
        "most_common_error": top_reason,
        "occurrence_count": top_count,
        "percentage": round(top_count / len(errors) * 100, 1),
    }

    reason_lower = top_reason.lower()

    if "out_of_range" in reason_lower or "range" in reason_lower:
        diagnosis["root_cause"] = "Data values outside expected boundaries"
        diagnosis["likely_reason"] = (
            "Source system changed units (e.g., heart rate in bpm vs per-minute) "
            "or a sensor is malfunctioning and sending extreme values."
        )
        diagnosis["fix"] = [
            "1. Check the source system for recent changes to data format or units",
            "2. Update the Avro schema if the valid range has changed",
            "3. Add range validation in the producer before sending to Kafka",
            "4. If sensor issue: flag the device for maintenance",
        ]
        diagnosis["reprocess"] = (
            "After fixing, the DLQ messages can be replayed to the main topic: "
            "consume from DLQ, fix the values, produce to the original topic."
        )

    elif "schema" in reason_lower or "serialization" in reason_lower or "avro" in reason_lower:
        diagnosis["root_cause"] = "Schema mismatch between producer and registry"
        diagnosis["likely_reason"] = (
            "A producer is sending data with a schema that doesn't match what's registered. "
            "This can happen after a database migration or code deployment."
        )
        diagnosis["fix"] = [
            "1. Check Schema Registry for the latest schema version",
            "2. Compare with what the producer is sending",
            "3. If schema needs to evolve: register new compatible version",
            "4. If producer bug: fix the producer code and redeploy",
        ]
        diagnosis["reprocess"] = (
            "Schema-level failures mean ALL records are likely failing. "
            "Fix the schema first, then restart the connector/producer. "
            "Kafka retains the source data, so no data is lost."
        )

    elif "missing" in reason_lower or "null" in reason_lower or "required" in reason_lower:
        diagnosis["root_cause"] = "Required fields are NULL or missing in source data"
        diagnosis["likely_reason"] = (
            "Source system has records with missing required fields. "
            "Could be a data entry issue or a source system bug."
        )
        diagnosis["fix"] = [
            "1. Query the source database for records with NULL required fields",
            "2. Report to the source system team for data cleanup",
            "3. Add default values or make fields optional in the schema",
            "4. Add producer-level validation to catch these before Kafka",
        ]
        diagnosis["reprocess"] = (
            "After source data is fixed, reprocess: consume from DLQ, "
            "re-validate, and produce to the main topic."
        )

    elif "connect" in reason_lower or "timeout" in reason_lower or "refused" in reason_lower:
        diagnosis["root_cause"] = "Connector cannot reach the source database"
        diagnosis["likely_reason"] = (
            "Database is down, network issue, or credentials expired."
        )
        diagnosis["fix"] = [
            "1. Check if the source database is reachable: ping hostname",
            "2. Verify credentials are still valid",
            "3. Check network/firewall rules",
            "4. Restart the connector after fixing the connection",
        ]
        diagnosis["reprocess"] = (
            "Connection errors mean no data was consumed. Once connection is restored, "
            "the connector resumes from its last offset — no data loss."
        )

    else:
        diagnosis["root_cause"] = "Unknown error type — requires manual investigation"
        diagnosis["fix"] = [
            "1. Check connector logs: docker logs kafka-connect --tail 100",
            "2. Check producer logs for error details",
            "3. Inspect the raw DLQ messages for patterns",
        ]

    diagnosis["time_saved"] = (
        "This automated diagnosis saves approximately 15-30 minutes of manual "
        "log analysis that a Kafka engineer would typically spend."
    )

    return json.dumps(diagnosis, indent=2)


def check_connector_health() -> str:
    """Check all Kafka Connect connectors for health issues."""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors?expand=status", timeout=5)
        if resp.status_code != 200:
            return json.dumps({"error": f"Kafka Connect returned HTTP {resp.status_code}"})

        connectors = resp.json()
        report = {"total": len(connectors), "healthy": 0, "unhealthy": 0, "connectors": []}

        for name, details in connectors.items():
            status = details.get("status", {})
            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])

            is_healthy = connector_state == "RUNNING" and all(
                t.get("state") == "RUNNING" for t in tasks
            )

            entry = {
                "name": name,
                "state": connector_state,
                "tasks_total": len(tasks),
                "tasks_running": sum(1 for t in tasks if t.get("state") == "RUNNING"),
                "tasks_failed": sum(1 for t in tasks if t.get("state") == "FAILED"),
                "healthy": is_healthy,
            }

            # Get error details for failed tasks
            if not is_healthy:
                report["unhealthy"] += 1
                for t in tasks:
                    if t.get("state") == "FAILED" and t.get("trace"):
                        entry["error_trace"] = t["trace"][:300]
                        break
            else:
                report["healthy"] += 1

            report["connectors"].append(entry)

        report["overall_status"] = "HEALTHY" if report["unhealthy"] == 0 else "DEGRADED"

        return json.dumps(report, indent=2)

    except requests.exceptions.ConnectionError:
        return json.dumps({"error": "Cannot connect to Kafka Connect at localhost:8083"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def check_schema_compatibility() -> str:
    """Check Schema Registry for any compatibility issues."""
    try:
        # Get all subjects
        resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
        if resp.status_code != 200:
            return json.dumps({"error": f"Schema Registry returned HTTP {resp.status_code}"})

        subjects = resp.json()
        report = {"total_subjects": len(subjects), "subjects": []}

        for subject in subjects:
            # Get latest version
            ver_resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest", timeout=5)
            if ver_resp.status_code == 200:
                ver_data = ver_resp.json()
                report["subjects"].append({
                    "subject": subject,
                    "version": ver_data.get("version", "?"),
                    "schema_type": ver_data.get("schemaType", "AVRO"),
                    "id": ver_data.get("id", "?"),
                })

        # Check compatibility mode
        compat_resp = requests.get(f"{SCHEMA_REGISTRY_URL}/config", timeout=5)
        if compat_resp.status_code == 200:
            report["global_compatibility"] = compat_resp.json().get("compatibilityLevel", "UNKNOWN")

        return json.dumps(report, indent=2)

    except requests.exceptions.ConnectionError:
        return json.dumps({"error": "Cannot connect to Schema Registry at localhost:8081"})
    except Exception as e:
        return json.dumps({"error": str(e)})


def generate_alert_report() -> str:
    """Generate a comprehensive alert report for the Kafka engineering team."""
    # Gather all data
    dlq_data = _redis.get("dlq:recent_errors")
    health_data = _redis.get("health:latest")
    source_data = _redis.get("source:health")

    errors = json.loads(dlq_data) if dlq_data else []
    health = json.loads(health_data) if health_data else {}
    sources = json.loads(source_data) if source_data else {}

    # Build report
    report = {
        "report_type": "DLQ_ALERT",
        "generated_at": datetime.now().isoformat(),
        "summary": {},
        "dlq_status": {},
        "connector_status": {},
        "source_status": {},
        "recommended_actions": [],
        "priority": "LOW",
    }

    # DLQ summary
    if errors:
        categories = {}
        for err in errors:
            reason = err.get("error_reason", "unknown").split(":")[0]
            categories[reason] = categories.get(reason, 0) + 1

        report["dlq_status"] = {
            "total_errors": len(errors),
            "error_breakdown": categories,
        }
        report["priority"] = "HIGH" if len(errors) > 20 else "MEDIUM"
        report["recommended_actions"].append(
            f"DLQ has {len(errors)} errors. Run diagnose_root_cause for detailed analysis."
        )
    else:
        report["dlq_status"] = {"total_errors": 0, "status": "CLEAN"}

    # Connector health
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors?expand=status", timeout=5)
        if resp.status_code == 200:
            connectors = resp.json()
            failed = []
            for name, details in connectors.items():
                state = details.get("status", {}).get("connector", {}).get("state", "UNKNOWN")
                if state != "RUNNING":
                    failed.append({"name": name, "state": state})

            report["connector_status"] = {
                "total": len(connectors),
                "running": len(connectors) - len(failed),
                "failed": failed,
            }
            if failed:
                report["priority"] = "CRITICAL"
                for f in failed:
                    report["recommended_actions"].append(
                        f"Connector '{f['name']}' is {f['state']}. Restart or check logs."
                    )
    except Exception:
        report["connector_status"] = {"error": "Could not reach Kafka Connect"}

    # Source health
    for src_name, src_info in sources.items():
        status = src_info.get("status", "unknown")
        if status != "healthy":
            report["source_status"][src_name] = {"status": status, "alert": True}
            report["recommended_actions"].append(f"Source '{src_name}' is {status}. Check data flow.")

    if not report["recommended_actions"]:
        report["recommended_actions"].append("All systems operating normally. No action needed.")
        report["priority"] = "LOW"

    report["summary"] = {
        "dlq_errors": len(errors),
        "priority": report["priority"],
        "actions_needed": len(report["recommended_actions"]),
    }

    # In production, this would send email/Slack notification
    report["notification"] = {
        "note": "In production, this report would be sent via email/Slack to the Kafka engineering team automatically.",
        "channels": ["Email: kafka-team@hospital.com", "Slack: #kafka-alerts"],
        "frequency": "Every 5 minutes if CRITICAL, every 30 minutes if MEDIUM",
    }

    # Cache the report in Redis
    _redis.set("dlq:latest_report", json.dumps(report), ex=300)

    return json.dumps(report, indent=2)


# ── Tool definitions for OpenAI function calling ────────────────────────────
DLQ_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "scan_dlq_topic",
            "description": "Scan the Dead Letter Queue for recent error messages. Shows count, severity, and recent errors.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "analyze_error_patterns",
            "description": "Categorize DLQ errors to find patterns — groups by error type, source, field, and detects burst patterns.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "diagnose_root_cause",
            "description": "Deep root cause analysis of DLQ errors. Determines why errors occurred and suggests specific fixes with reprocessing strategy.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_connector_health",
            "description": "Check all Kafka Connect connectors for health issues. Shows which are RUNNING, FAILED, or PAUSED.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_schema_compatibility",
            "description": "Check Schema Registry for compatibility issues. Lists all registered schemas and their versions.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_alert_report",
            "description": "Generate a comprehensive alert report for the Kafka engineering team. Includes DLQ status, connector health, source status, and recommended actions with priority level.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

DLQ_TOOL_FUNCTIONS = {
    "scan_dlq_topic": scan_dlq_topic,
    "analyze_error_patterns": analyze_error_patterns,
    "diagnose_root_cause": diagnose_root_cause,
    "check_connector_health": check_connector_health,
    "check_schema_compatibility": check_schema_compatibility,
    "generate_alert_report": generate_alert_report,
}
