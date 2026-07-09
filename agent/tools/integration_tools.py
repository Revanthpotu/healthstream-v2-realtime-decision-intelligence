"""
============================================================================
HealthStream v2 — Kafka Integration Agent Tools
============================================================================
Tools for the Integration Agent that helps Kafka engineers onboard
new data sources. Instead of manually writing connector configs or
producer code, the engineer describes the source and the agent
handles the rest.

TOOLS:
  1. recommend_integration_method  → Analyze source, recommend approach
  2. generate_connector_config     → Build JDBC/Debezium JSON config
  3. generate_producer_code        → Build Python producer for file sources
  4. deploy_connector              → POST config to Kafka Connect REST API
  5. check_connector_status        → Verify connector is RUNNING
  6. list_active_connectors        → Show all deployed connectors
============================================================================
"""

import json
import requests

KAFKA_CONNECT_URL = "http://localhost:8083"


def recommend_integration_method(
    source_type: str,
    database_type: str = "",
    needs_realtime: bool = False,
    needs_delete_detection: bool = False,
) -> str:
    """Analyze source requirements and recommend the best integration method."""

    source_type = source_type.lower().strip()

    # Decision logic
    if source_type in ("csv", "file", "flat file", "excel", "json file", "xml"):
        return json.dumps({
            "recommended_method": "Custom Python Producer",
            "reason": (
                f"Source is a {source_type} file. Kafka Connect does not natively "
                "handle file-based sources with custom transformations. A Python "
                "producer gives full control over parsing, validation, and Avro serialization."
            ),
            "advantages": [
                "Full control over data validation and transformation",
                "Can handle custom file formats and schemas",
                "Bad records routed to Dead Letter Queue without crashing",
                "Can be triggered by file arrival (S3 event, cron job)",
            ],
            "production_deployment": "AWS Lambda triggered by S3 upload event",
            "next_step": "Call generate_producer_code with the file details",
        }, indent=2)

    if source_type in ("database", "db", "postgresql", "postgres", "oracle", "sql server", "mssql"):
        if needs_realtime or needs_delete_detection:
            if database_type.lower() in ("postgresql", "postgres"):
                return json.dumps({
                    "recommended_method": "Debezium CDC (PostgreSQL)",
                    "reason": (
                        "PostgreSQL with real-time requirements. Debezium reads the "
                        "Write-Ahead Log (WAL) for sub-second change capture."
                    ),
                    "advantages": [
                        "Sub-second latency",
                        "Captures INSERT, UPDATE, and DELETE",
                        "No schema changes needed on source database",
                        "Reads WAL — minimal impact on database performance",
                    ],
                    "production_deployment": "Amazon MSK Connect with Debezium plugin",
                    "next_step": "Call generate_connector_config with database details and method='debezium-postgres'",
                }, indent=2)
            else:
                return json.dumps({
                    "recommended_method": "Debezium CDC",
                    "reason": (
                        "Real-time or delete detection required. Debezium reads the "
                        "database transaction log for instant change capture."
                    ),
                    "advantages": [
                        "Sub-second latency",
                        "Captures INSERT, UPDATE, and DELETE",
                        "No schema changes needed on source database",
                    ],
                    "production_deployment": "Amazon MSK Connect with Debezium plugin",
                    "next_step": "Call generate_connector_config with database details and method='debezium'",
                }, indent=2)
        else:
            return json.dumps({
                "recommended_method": "JDBC Source Connector",
                "reason": (
                    "Database source without strict real-time requirements. "
                    "JDBC connector polls at a configurable interval (e.g. every 10 seconds). "
                    "Simpler to set up than CDC."
                ),
                "advantages": [
                    "Zero code — configuration only",
                    "Works with any JDBC-compatible database",
                    "Automatic offset tracking via timestamp column",
                ],
                "limitations": [
                    "Cannot detect DELETE operations",
                    "Requires a timestamp or incrementing column in the table",
                    "Polling interval adds latency (not true real-time)",
                ],
                "production_deployment": "Amazon MSK Connect",
                "next_step": "Call generate_connector_config with database details and method='jdbc'",
            }, indent=2)

    if source_type in ("mysql", "mariadb"):
        if needs_realtime or needs_delete_detection:
            return json.dumps({
                "recommended_method": "Debezium CDC (MySQL)",
                "reason": (
                    "MySQL with real-time or delete detection. Debezium reads the "
                    "MySQL binary log (binlog) for sub-second change capture."
                ),
                "advantages": [
                    "Sub-second latency — reads binlog in real-time",
                    "Captures INSERT, UPDATE, and DELETE",
                    "Initial snapshot loads existing data, then switches to binlog",
                    "No schema changes needed on source database",
                ],
                "prerequisites": [
                    "MySQL binlog must be enabled (log_bin=ON)",
                    "binlog_format must be ROW",
                    "User needs REPLICATION SLAVE and REPLICATION CLIENT privileges",
                ],
                "production_deployment": "Amazon MSK Connect with Debezium MySQL plugin",
                "next_step": "Call generate_connector_config with database details and method='debezium-mysql'",
            }, indent=2)
        else:
            return json.dumps({
                "recommended_method": "JDBC Source Connector",
                "reason": (
                    "MySQL without strict real-time needs. JDBC is simpler. "
                    "If you later need real-time or delete detection, switch to Debezium."
                ),
                "next_step": "Call generate_connector_config with database details and method='jdbc'",
            }, indent=2)

    # API / streaming sources
    if source_type in ("api", "rest api", "webhook", "stream"):
        return json.dumps({
            "recommended_method": "Custom Python Producer",
            "reason": (
                f"{source_type} sources require custom code to handle authentication, "
                "pagination, rate limiting, and data transformation."
            ),
            "next_step": "Call generate_producer_code with the API details",
        }, indent=2)

    return json.dumps({
        "error": f"Unknown source type: '{source_type}'",
        "supported_types": ["csv", "file", "postgresql", "mysql", "oracle", "api", "database"],
        "suggestion": "Please specify the source type more clearly.",
    }, indent=2)


def generate_connector_config(
    method: str,
    connector_name: str,
    hostname: str,
    port: str,
    database_name: str,
    table_name: str,
    username: str,
    password: str,
    topic_name: str = "",
    poll_interval_ms: int = 10000,
    timestamp_column: str = "updated_at",
) -> str:
    """Generate a complete Kafka Connect connector configuration."""

    method = method.lower().strip()
    topic = topic_name or table_name.replace("_", "-")

    if method in ("jdbc", "jdbc-source"):
        # Determine JDBC URL prefix
        db_type = "postgresql"
        if "mysql" in hostname.lower() or "3306" in str(port):
            db_type = "mysql"

        jdbc_url = f"jdbc:{db_type}://{hostname}:{port}/{database_name}"

        config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "connection.url": jdbc_url,
                "connection.user": username,
                "connection.password": password,
                "table.whitelist": table_name,
                "mode": "timestamp",
                "timestamp.column.name": timestamp_column,
                "validate.non.null": False,
                "topic.prefix": "",
                "transforms": "routeToTopic,extractKey",
                "transforms.routeToTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.routeToTopic.regex": table_name,
                "transforms.routeToTopic.replacement": topic,
                "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ValueToKey",
                "transforms.extractKey.fields": "patient_id",
                "poll.interval.ms": poll_interval_ms,
                "batch.max.rows": 100,
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": "http://schema-registry:8081",
                "tasks.max": 1,
            },
        }

        return json.dumps({
            "method": "JDBC Source Connector",
            "config": config,
            "config_json": json.dumps(config, indent=2),
            "deploy_command": f"POST to {KAFKA_CONNECT_URL}/connectors",
            "next_step": f"Call deploy_connector with connector_name='{connector_name}' to deploy this config",
        }, indent=2)

    elif method in ("debezium", "debezium-mysql", "cdc", "cdc-mysql"):
        server_id = str(hash(connector_name) % 900000 + 100000)
        config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.mysql.MySqlConnector",
                "database.hostname": hostname,
                "database.port": str(port),
                "database.user": username,
                "database.password": password,
                "database.server.id": server_id,
                "topic.prefix": database_name.split("_")[0] if "_" in database_name else database_name,
                "database.include.list": database_name,
                "table.include.list": f"{database_name}.{table_name}",
                "schema.history.internal.kafka.bootstrap.servers": "kafka-1:29092",
                "schema.history.internal.kafka.topic": f"_schema-history-{connector_name}",
                "transforms": "routeToTopic,unwrap",
                "transforms.routeToTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.routeToTopic.regex": f"{database_name}\\.{database_name}\\.{table_name}",
                "transforms.routeToTopic.replacement": topic,
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": False,
                "transforms.unwrap.delete.handling.mode": "rewrite",
                "transforms.unwrap.add.fields": "op,source.ts_ms",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": "http://schema-registry:8081",
                "snapshot.mode": "initial",
                "include.schema.changes": False,
                "errors.tolerance": "none",
                "errors.log.enable": True,
                "errors.log.include.messages": True,
                "tasks.max": 1,
            },
        }

        return json.dumps({
            "method": "Debezium CDC (MySQL)",
            "config": config,
            "config_json": json.dumps(config, indent=2),
            "notes": [
                "snapshot.mode=initial: Full table scan first, then binlog streaming",
                "errors.tolerance=none: Fail fast so errors are visible immediately",
                "unwrap transform: Flattens Debezium envelope to simple records",
            ],
            "next_step": f"Call deploy_connector with connector_name='{connector_name}' to deploy this config",
        }, indent=2)

    elif method in ("debezium-postgres", "cdc-postgres"):
        config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": hostname,
                "database.port": str(port),
                "database.user": username,
                "database.password": password,
                "database.dbname": database_name,
                "topic.prefix": database_name.split("_")[0] if "_" in database_name else database_name,
                "table.include.list": f"public.{table_name}",
                "plugin.name": "pgoutput",
                "schema.history.internal.kafka.bootstrap.servers": "kafka-1:29092",
                "schema.history.internal.kafka.topic": f"_schema-history-{connector_name}",
                "transforms": "routeToTopic,unwrap",
                "transforms.routeToTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.routeToTopic.regex": f".*\\.public\\.{table_name}",
                "transforms.routeToTopic.replacement": topic,
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "io.confluent.connect.avro.AvroConverter",
                "value.converter.schema.registry.url": "http://schema-registry:8081",
                "snapshot.mode": "initial",
                "errors.tolerance": "none",
                "errors.log.enable": True,
                "errors.log.include.messages": True,
                "tasks.max": 1,
            },
        }

        return json.dumps({
            "method": "Debezium CDC (PostgreSQL)",
            "config": config,
            "config_json": json.dumps(config, indent=2),
            "next_step": f"Call deploy_connector with connector_name='{connector_name}' to deploy this config",
        }, indent=2)

    return json.dumps({"error": f"Unknown method: {method}. Use 'jdbc', 'debezium-mysql', or 'debezium-postgres'."})


def generate_producer_code(
    source_type: str,
    file_path: str,
    topic_name: str,
    key_field: str = "patient_id",
    description: str = "",
) -> str:
    """Generate Python producer code for file-based sources."""

    code = f'''"""
Auto-generated Kafka Producer for {source_type.upper()} source
Topic: {topic_name}
Description: {description}
"""

import csv
import json
import sys
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "{topic_name}"
FILE_PATH = "{file_path}"

def delivery_report(err, msg):
    if err:
        print(f"  FAILED: {{err}}")
    else:
        print(f"  Delivered to {{msg.topic()}} [{{msg.partition()}}]")

def main():
    producer = Producer({{"bootstrap.servers": KAFKA_BOOTSTRAP}})
    count = 0
    errors = 0

    with open(FILE_PATH, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row.get("{key_field}", "unknown")

            # Validate required fields
            if not key or key == "unknown":
                errors += 1
                # Route to DLQ
                producer.produce(
                    "dead-letter-queue",
                    key=key,
                    value=json.dumps({{"original_record": row, "error": "missing key field"}}),
                    callback=delivery_report,
                )
                continue

            producer.produce(
                TOPIC,
                key=key,
                value=json.dumps(row),
                callback=delivery_report,
            )
            count += 1

            if count % 500 == 0:
                producer.flush()
                print(f"  Produced {{count}} records...")

    producer.flush()
    print(f"Done. Produced {{count}} records, {{errors}} sent to DLQ.")

if __name__ == "__main__":
    main()
'''

    return json.dumps({
        "method": "Custom Python Producer",
        "source_type": source_type,
        "topic": topic_name,
        "code": code,
        "notes": [
            "DLQ routing built-in for bad records",
            "Delivery callbacks for guaranteed delivery",
            "Batch flushing every 500 records for throughput",
            f"Key field: {key_field}",
        ],
        "production_deployment": "Package as AWS Lambda, trigger on S3 upload event",
    }, indent=2)


def deploy_connector(connector_name: str) -> str:
    """Deploy a connector config to Kafka Connect REST API."""
    try:
        # First check if connector already exists
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", timeout=5)
        if resp.status_code == 200:
            status = resp.json()
            state = status.get("connector", {}).get("state", "UNKNOWN")
            if state == "RUNNING":
                return json.dumps({
                    "status": "ALREADY_RUNNING",
                    "connector": connector_name,
                    "message": f"Connector '{connector_name}' is already deployed and RUNNING.",
                    "suggestion": "Use check_connector_status to see detailed status.",
                }, indent=2)
            else:
                # Restart it
                requests.post(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/restart", timeout=5)
                return json.dumps({
                    "status": "RESTARTED",
                    "connector": connector_name,
                    "previous_state": state,
                    "message": f"Connector was in {state} state. Restarted it.",
                }, indent=2)

        # Connector doesn't exist — we need the config
        # Check if we have a saved config in /tmp
        import os
        config_path = f"/tmp/connector_{connector_name}.json"
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = json.load(f)
        else:
            return json.dumps({
                "status": "ERROR",
                "message": (
                    f"No config found for '{connector_name}'. "
                    "Please call generate_connector_config first to create the config, "
                    "then call deploy_connector again."
                ),
            }, indent=2)

        # Deploy
        resp = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            json=config,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        if resp.status_code in (200, 201):
            return json.dumps({
                "status": "DEPLOYED",
                "connector": connector_name,
                "message": f"Connector '{connector_name}' deployed successfully!",
                "next_step": "Call check_connector_status to verify it is RUNNING.",
            }, indent=2)
        else:
            return json.dumps({
                "status": "DEPLOY_FAILED",
                "http_status": resp.status_code,
                "error": resp.text[:500],
            }, indent=2)

    except requests.exceptions.ConnectionError:
        return json.dumps({
            "status": "ERROR",
            "message": "Cannot connect to Kafka Connect at localhost:8083. Is it running?",
        }, indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e)}, indent=2)


def check_connector_status(connector_name: str) -> str:
    """Check the status of a deployed connector."""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status", timeout=5)
        if resp.status_code == 404:
            return json.dumps({
                "status": "NOT_FOUND",
                "message": f"Connector '{connector_name}' is not deployed.",
            }, indent=2)

        status = resp.json()
        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
        tasks = status.get("tasks", [])

        task_summary = []
        for t in tasks:
            task_info = {"id": t["id"], "state": t["state"]}
            if t.get("trace"):
                task_info["error"] = t["trace"][:300]
            task_summary.append(task_info)

        result = {
            "connector": connector_name,
            "state": connector_state,
            "tasks": task_summary,
            "healthy": connector_state == "RUNNING" and all(t["state"] == "RUNNING" for t in tasks),
        }

        if not result["healthy"]:
            result["troubleshooting"] = [
                "Check connector logs: docker logs kafka-connect --tail 50",
                "Common issues: wrong credentials, database unreachable, schema mismatch",
                "Restart: POST /connectors/{name}/restart",
            ]

        return json.dumps(result, indent=2)

    except requests.exceptions.ConnectionError:
        return json.dumps({
            "status": "ERROR",
            "message": "Cannot connect to Kafka Connect. Is it running?",
        }, indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e)}, indent=2)


def list_active_connectors() -> str:
    """List all connectors deployed on Kafka Connect."""
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors?expand=status", timeout=5)
        if resp.status_code != 200:
            return json.dumps({"error": f"HTTP {resp.status_code}"})

        connectors = resp.json()
        result = []
        for name, details in connectors.items():
            status = details.get("status", {})
            result.append({
                "name": name,
                "state": status.get("connector", {}).get("state", "UNKNOWN"),
                "type": status.get("type", "unknown"),
                "tasks": len(status.get("tasks", [])),
                "tasks_running": sum(1 for t in status.get("tasks", []) if t.get("state") == "RUNNING"),
            })

        return json.dumps({
            "total_connectors": len(result),
            "connectors": result,
        }, indent=2)

    except requests.exceptions.ConnectionError:
        return json.dumps({
            "status": "ERROR",
            "message": "Cannot connect to Kafka Connect. Is it running?",
        }, indent=2)
    except Exception as e:
        return json.dumps({"status": "ERROR", "message": str(e)}, indent=2)


# ── Tool definitions for OpenAI function calling ────────────────────────────
INTEGRATION_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "recommend_integration_method",
            "description": (
                "Analyze a data source and recommend the best Kafka integration method. "
                "Call this first when a user wants to connect a new source."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "source_type": {
                        "type": "string",
                        "description": "Type of source: 'csv', 'file', 'mysql', 'postgresql', 'database', 'api'",
                    },
                    "database_type": {
                        "type": "string",
                        "description": "Specific database type if source is a database",
                        "default": "",
                    },
                    "needs_realtime": {
                        "type": "boolean",
                        "description": "Whether real-time (sub-second) capture is needed",
                        "default": False,
                    },
                    "needs_delete_detection": {
                        "type": "boolean",
                        "description": "Whether DELETE operations must be captured",
                        "default": False,
                    },
                },
                "required": ["source_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_connector_config",
            "description": (
                "Generate a complete Kafka Connect connector configuration (JDBC or Debezium). "
                "Returns the full JSON config ready to deploy."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "method": {
                        "type": "string",
                        "description": "Integration method: 'jdbc', 'debezium-mysql', or 'debezium-postgres'",
                    },
                    "connector_name": {
                        "type": "string",
                        "description": "Unique name for this connector (e.g. 'jdbc-source-orders')",
                    },
                    "hostname": {"type": "string", "description": "Database hostname"},
                    "port": {"type": "string", "description": "Database port"},
                    "database_name": {"type": "string", "description": "Database name"},
                    "table_name": {"type": "string", "description": "Table name to capture"},
                    "username": {"type": "string", "description": "Database username"},
                    "password": {"type": "string", "description": "Database password"},
                    "topic_name": {
                        "type": "string",
                        "description": "Kafka topic name (default: derived from table name)",
                        "default": "",
                    },
                    "poll_interval_ms": {
                        "type": "integer",
                        "description": "JDBC poll interval in ms (default: 10000)",
                        "default": 10000,
                    },
                    "timestamp_column": {
                        "type": "string",
                        "description": "Timestamp column for JDBC mode (default: updated_at)",
                        "default": "updated_at",
                    },
                },
                "required": ["method", "connector_name", "hostname", "port", "database_name", "table_name", "username", "password"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_producer_code",
            "description": (
                "Generate Python producer code for file-based sources (CSV, JSON files). "
                "Includes DLQ routing and delivery callbacks."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "source_type": {"type": "string", "description": "File type: 'csv', 'json', 'xml'"},
                    "file_path": {"type": "string", "description": "Path to the source file"},
                    "topic_name": {"type": "string", "description": "Target Kafka topic name"},
                    "key_field": {
                        "type": "string",
                        "description": "Field to use as message key (default: patient_id)",
                        "default": "patient_id",
                    },
                    "description": {"type": "string", "description": "Description of the data", "default": ""},
                },
                "required": ["source_type", "file_path", "topic_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "deploy_connector",
            "description": "Deploy a connector configuration to Kafka Connect REST API. Call generate_connector_config first.",
            "parameters": {
                "type": "object",
                "properties": {
                    "connector_name": {"type": "string", "description": "Name of the connector to deploy"},
                },
                "required": ["connector_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "check_connector_status",
            "description": "Check if a connector is running, failed, or paused. Shows task-level details and errors.",
            "parameters": {
                "type": "object",
                "properties": {
                    "connector_name": {"type": "string", "description": "Name of the connector to check"},
                },
                "required": ["connector_name"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_active_connectors",
            "description": "List all connectors currently deployed on Kafka Connect with their status.",
            "parameters": {"type": "object", "properties": {}},
        },
    },
]

INTEGRATION_TOOL_FUNCTIONS = {
    "recommend_integration_method": recommend_integration_method,
    "generate_connector_config": generate_connector_config,
    "generate_producer_code": generate_producer_code,
    "deploy_connector": deploy_connector,
    "check_connector_status": check_connector_status,
    "list_active_connectors": list_active_connectors,
}
