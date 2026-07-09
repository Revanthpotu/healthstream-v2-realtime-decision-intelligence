"""
============================================================================
HealthStream v2 — Pipeline Health Monitor (Step 7)
============================================================================
Monitors: consumer lag, connector status, source throughput, DLQ growth.
Writes health metrics to Redis and integration-health Kafka topic.
============================================================================
"""

import json
import time
import logging
import subprocess
import requests
from datetime import datetime

import redis

KAFKA_BOOTSTRAP = "localhost:9092"
CONNECT_URL = "http://localhost:8083"
KSQLDB_URL = "http://localhost:8088"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("health_monitor")


class HealthMonitor:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.redis_client.ping()
        logger.info("Connected to Redis")

    def check_connector_status(self):
        """Check Kafka Connect connector health."""
        connectors = {}
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if resp.status_code == 200:
                for name in resp.json():
                    status_resp = requests.get(
                        f"{CONNECT_URL}/connectors/{name}/status", timeout=5
                    )
                    if status_resp.status_code == 200:
                        status = status_resp.json()
                        connectors[name] = {
                            "state": status["connector"]["state"],
                            "worker": status["connector"].get("worker_id", ""),
                            "tasks": [
                                {"id": t["id"], "state": t["state"]}
                                for t in status.get("tasks", [])
                            ],
                            "type": status["type"],
                        }
        except requests.exceptions.ConnectionError:
            connectors["_error"] = "Kafka Connect not reachable"
        except Exception as e:
            connectors["_error"] = str(e)
        return connectors

    def check_schema_registry(self):
        """Check Schema Registry health and registered subjects."""
        try:
            resp = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            if resp.status_code == 200:
                subjects = resp.json()
                return {"status": "healthy", "subjects": subjects, "count": len(subjects)}
        except:
            pass
        return {"status": "unreachable", "subjects": [], "count": 0}

    def check_ksqldb(self):
        """Check ksqlDB health and running queries."""
        try:
            resp = requests.get(f"{KSQLDB_URL}/info", timeout=5)
            if resp.status_code == 200:
                info = resp.json()
                queries_resp = requests.post(
                    f"{KSQLDB_URL}/ksql",
                    json={"ksql": "SHOW QUERIES;"},
                    headers={"Content-Type": "application/vnd.ksql.v1+json"},
                    timeout=5,
                )
                query_count = 0
                if queries_resp.status_code == 200:
                    for item in queries_resp.json():
                        if "queries" in item:
                            query_count = len(item["queries"])
                return {
                    "status": "healthy",
                    "version": info.get("KsqlServerInfo", {}).get("version", ""),
                    "running_queries": query_count,
                }
        except:
            pass
        return {"status": "unreachable", "version": "", "running_queries": 0}

    def get_topic_offsets(self):
        """Get topic message counts via kafka-run-class."""
        topics = {}
        topic_names = [
            "patient-vitals", "patient-conditions", "patient-medications",
            "dead-letter-queue", "integration-health", "risk-alerts",
        ]
        for topic in topic_names:
            try:
                result = subprocess.run(
                    [
                        "docker", "exec", "kafka-1", "kafka-run-class",
                        "kafka.tools.GetOffsetShell",
                        "--broker-list", "kafka-1:29092",
                        "--topic", topic,
                    ],
                    capture_output=True, text=True, timeout=10,
                )
                if result.returncode == 0:
                    total = 0
                    for line in result.stdout.strip().split("\n"):
                        if ":" in line:
                            parts = line.split(":")
                            if len(parts) >= 3:
                                total += int(parts[2])
                    topics[topic] = {"total_messages": total}
            except:
                topics[topic] = {"total_messages": "unknown"}
        return topics

    def check_redis_stats(self):
        """Check Redis data store health."""
        try:
            total_patients = self.redis_client.scard("patients:all")
            source_health = self.redis_client.get("source:health")
            dlq_errors = self.redis_client.get("dlq:recent_errors")

            return {
                "status": "healthy",
                "total_patients": total_patients,
                "source_health": json.loads(source_health) if source_health else {},
                "dlq_error_count": len(json.loads(dlq_errors)) if dlq_errors else 0,
            }
        except:
            return {"status": "unreachable"}

    def check_dlq_alerts(self, health_report):
        """Check DLQ for errors and generate alerts for Kafka engineers.

        In production, this would send email/Slack notifications.
        For the demo, it logs alerts and stores them in Redis.
        """
        dlq_data = self.redis_client.get("dlq:recent_errors")
        dlq_count = 0
        if dlq_data:
            errors = json.loads(dlq_data)
            dlq_count = len(errors)

        # Check connector failures
        failed_connectors = []
        for name, conn in health_report.get("connectors", {}).items():
            if name.startswith("_"):
                continue
            state = conn.get("state", "UNKNOWN")
            if state != "RUNNING":
                failed_connectors.append({"name": name, "state": state})

        # Determine alert level
        alert_level = "OK"
        alerts = []

        if dlq_count > 0:
            alert_level = "WARNING"
            alerts.append(f"DLQ has {dlq_count} error(s). Run DLQ Monitor Agent for root cause analysis.")
            if dlq_count > 20:
                alert_level = "CRITICAL"

        if failed_connectors:
            alert_level = "CRITICAL"
            for fc in failed_connectors:
                alerts.append(f"Connector '{fc['name']}' is {fc['state']}. Restart needed.")

        # Store alert in Redis
        alert_report = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": alert_level,
            "dlq_count": dlq_count,
            "failed_connectors": failed_connectors,
            "alerts": alerts,
            "notification": (
                "In production: email/Slack sent to kafka-team@hospital.com and #kafka-alerts channel"
                if alert_level != "OK" else "No alerts"
            ),
        }
        self.redis_client.set("alerts:latest", json.dumps(alert_report), ex=600)

        # Log alerts
        if alert_level == "CRITICAL":
            logger.warning(f"🚨 CRITICAL ALERT: {' | '.join(alerts)}")
            logger.warning(f"   → In production: Email + Slack notification sent to Kafka engineering team")
        elif alert_level == "WARNING":
            logger.info(f"⚠️  WARNING: {' | '.join(alerts)}")
        else:
            logger.info(f"✅ DLQ: clean | Connectors: all healthy")

        return alert_report

    def run_health_check(self):
        """Run a complete health check and store results."""
        logger.info("Running health check...")

        health_report = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "connectors": self.check_connector_status(),
            "schema_registry": self.check_schema_registry(),
            "ksqldb": self.check_ksqldb(),
            "topics": self.get_topic_offsets(),
            "redis": self.check_redis_stats(),
        }

        # Determine overall status
        statuses = []
        if health_report["schema_registry"]["status"] == "healthy":
            statuses.append("healthy")
        else:
            statuses.append("degraded")

        if health_report["ksqldb"]["status"] == "healthy":
            statuses.append("healthy")
        else:
            statuses.append("degraded")

        if health_report["redis"]["status"] == "healthy":
            statuses.append("healthy")
        else:
            statuses.append("degraded")

        health_report["overall_status"] = (
            "healthy" if all(s == "healthy" for s in statuses) else "degraded"
        )

        # Write to Redis
        self.redis_client.set("health:latest", json.dumps(health_report))
        self.redis_client.set("health:last_check", datetime.utcnow().isoformat())

        # Check DLQ and connector alerts
        self.check_dlq_alerts(health_report)

        return health_report

    def run(self, interval: int = 30):
        """Continuous health monitoring loop."""
        logger.info("=" * 60)
        logger.info("HealthStream v2 — Health Monitor Starting")
        logger.info(f"Check interval: {interval}s")
        logger.info("=" * 60)

        try:
            while True:
                report = self.run_health_check()

                # Log summary
                sr = report["schema_registry"]["status"]
                ks = report["ksqldb"]["status"]
                rd = report["redis"]["status"]
                overall = report["overall_status"]

                logger.info(
                    f"Health: {overall.upper()} | "
                    f"SchemaRegistry={sr} | ksqlDB={ks} | Redis={rd} | "
                    f"Patients={report['redis'].get('total_patients', '?')}"
                )

                for name, conn in report["connectors"].items():
                    if name.startswith("_"):
                        continue
                    state = conn.get("state", "unknown")
                    task_states = [t["state"] for t in conn.get("tasks", [])]
                    logger.info(f"  Connector '{name}': {state} | Tasks: {task_states}")

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Health Monitor stopped")


if __name__ == "__main__":
    monitor = HealthMonitor()
    monitor.run(interval=30)
