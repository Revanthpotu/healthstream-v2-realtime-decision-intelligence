"""
============================================================================
HealthStream v2 — Context Materializer (Step 6)
============================================================================
Consumes from Kafka topics, builds unified patient context,
and writes to Redis for sub-millisecond AI Agent lookups.

Redis Data Model:
  patient:{patient_id}  → JSON with latest vitals, conditions, medications
  source:health         → JSON with integration health metrics
  stats:global          → JSON with global pipeline statistics
============================================================================
"""

import json
import time
import logging
import threading
from datetime import datetime, timedelta
from collections import defaultdict

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import redis

# ============================================================================
# CONFIGURATION
# ============================================================================
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

VITALS_TOPIC = "patient-vitals"
CONDITIONS_TOPIC = "patient-conditions"
MEDICATIONS_TOPIC = "patient-medications"
DLQ_TOPIC = "dead-letter-queue"

# Redis TTL: Patient context expires after 24 hours of no updates.
# In production: only active (admitted) patients stay in cache.
# When a patient is discharged and no new data arrives, their context
# automatically expires. Historical data lives in the system of record.
PATIENT_TTL_SECONDS = 86400  # 24 hours

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("context_materializer")


class ContextMaterializer:
    """
    Consumes from all 3 source topics + DLQ and maintains a real-time
    patient context store in Redis.
    """

    def __init__(self):
        # ── Redis connection ────────────────────────────────────────────
        self.redis_client = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
        self.redis_client.ping()
        logger.info("Connected to Redis")

        # ── Schema Registry ─────────────────────────────────────────────
        sr_conf = {"url": SCHEMA_REGISTRY_URL}
        self.sr_client = SchemaRegistryClient(sr_conf)

        # ── Kafka Consumer for vitals (Avro) ────────────────────────────
        self.vitals_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "context-materializer-vitals",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self.vitals_consumer.subscribe([VITALS_TOPIC])

        # ── Kafka Consumer for conditions + medications (Avro) ──────────
        self.sources_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "context-materializer-sources",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self.sources_consumer.subscribe([CONDITIONS_TOPIC, MEDICATIONS_TOPIC])

        # ── Kafka Consumer for DLQ (JSON) ───────────────────────────────
        self.dlq_consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "context-materializer-dlq",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self.dlq_consumer.subscribe([DLQ_TOPIC])

        # ── Avro deserializer ───────────────────────────────────────────
        self.avro_deserializer = AvroDeserializer(self.sr_client)

        # ── In-memory patient context (buffered before Redis write) ─────
        self.patient_vitals = defaultdict(lambda: {
            "heart_rate": [], "systolic_bp": [], "diastolic_bp": [],
            "oxygen_saturation": [], "respiratory_rate": [], "temperature": [],
        })
        self.stats = {
            "vitals_processed": 0,
            "conditions_processed": 0,
            "medications_processed": 0,
            "dlq_processed": 0,
            "redis_writes": 0,
            "errors": 0,
        }
        self.source_health = {
            "csv_lab_export": {"last_seen": None, "count": 0, "status": "unknown"},
            "jdbc_postgres": {"last_seen": None, "count": 0, "status": "unknown"},
            "debezium_mysql": {"last_seen": None, "count": 0, "status": "unknown"},
        }
        self.dlq_errors = []

    # ── VITAL TYPE MAPPING ──────────────────────────────────────────────
    VITAL_MAP = {
        "8867-4": "heart_rate",
        "8480-6": "systolic_bp",
        "8462-4": "diastolic_bp",
        "2708-6": "oxygen_saturation",
        "9279-1": "respiratory_rate",
        "8310-5": "temperature",
    }

    def process_vital(self, msg):
        """Process a vital sign message and update Redis."""
        try:
            value = self.avro_deserializer(
                msg.value(),
                SerializationContext(VITALS_TOPIC, MessageField.VALUE),
            )
            if not value:
                return

            pid = value["patient_id"]
            vital_key = self.VITAL_MAP.get(value.get("vital_type"), None)
            if not vital_key:
                return

            # Build/update patient context in Redis
            redis_key = f"patient:{pid}"
            existing = self.redis_client.get(redis_key)
            context = json.loads(existing) if existing else {
                "patient_id": pid,
                "patient_age": value.get("patient_age", 0),
                "patient_gender": value.get("patient_gender", ""),
                "vitals": {},
                "vitals_history": {},
                "conditions": [],
                "medications": [],
                "last_updated": None,
            }

            # Update current vital
            context["vitals"][vital_key] = {
                "value": value["value"],
                "units": value.get("units", ""),
                "timestamp": value.get("timestamp", ""),
            }

            # Keep last 10 readings for trend calculation
            history_key = f"{vital_key}_history"
            if history_key not in context.get("vitals_history", {}):
                context.setdefault("vitals_history", {})[history_key] = []
            hist = context["vitals_history"][history_key]
            hist.append({"value": value["value"], "timestamp": value.get("timestamp", "")})
            if len(hist) > 10:
                hist.pop(0)

            # Calculate trend
            if len(hist) >= 2:
                first_val = hist[0]["value"]
                last_val = hist[-1]["value"]
                change = last_val - first_val
                pct_change = (change / first_val * 100) if first_val != 0 else 0
                context["vitals"][vital_key]["trend"] = {
                    "direction": "rising" if change > 0 else "falling" if change < 0 else "stable",
                    "change": round(change, 2),
                    "pct_change": round(pct_change, 2),
                    "readings": len(hist),
                }

            context["last_updated"] = datetime.utcnow().isoformat() + "Z"
            context["patient_age"] = value.get("patient_age", context.get("patient_age", 0))
            context["patient_gender"] = value.get("patient_gender", context.get("patient_gender", ""))

            self.redis_client.set(redis_key, json.dumps(context), ex=PATIENT_TTL_SECONDS)
            self.redis_client.sadd("patients:all", pid)
            self.stats["vitals_processed"] += 1
            self.stats["redis_writes"] += 1

            # Update source health
            self.source_health["csv_lab_export"]["last_seen"] = datetime.utcnow().isoformat()
            self.source_health["csv_lab_export"]["count"] += 1
            self.source_health["csv_lab_export"]["status"] = "healthy"

        except Exception as e:
            logger.error(f"Error processing vital: {e}")
            self.stats["errors"] += 1

    def process_condition(self, msg):
        """Process a condition message from JDBC connector."""
        try:
            value = self.avro_deserializer(
                msg.value(),
                SerializationContext(CONDITIONS_TOPIC, MessageField.VALUE),
            )
            if not value:
                return

            pid = value.get("patient_id", "")
            if not pid:
                return

            redis_key = f"patient:{pid}"
            existing = self.redis_client.get(redis_key)
            context = json.loads(existing) if existing else {
                "patient_id": pid, "patient_age": 0, "patient_gender": "",
                "vitals": {}, "vitals_history": {}, "conditions": [],
                "medications": [], "last_updated": None,
            }

            # Add/update condition
            condition_entry = {
                "name": value.get("condition_name", ""),
                "code": value.get("condition_code", ""),
                "status": value.get("status", ""),
                "severity": value.get("severity", ""),
            }

            # Replace if exists, add if new
            existing_names = [c["name"] for c in context["conditions"]]
            if condition_entry["name"] in existing_names:
                idx = existing_names.index(condition_entry["name"])
                context["conditions"][idx] = condition_entry
            else:
                context["conditions"].append(condition_entry)

            context["last_updated"] = datetime.utcnow().isoformat() + "Z"
            self.redis_client.set(redis_key, json.dumps(context), ex=PATIENT_TTL_SECONDS)
            self.redis_client.sadd("patients:all", pid)
            self.stats["conditions_processed"] += 1
            self.stats["redis_writes"] += 1

            self.source_health["jdbc_postgres"]["last_seen"] = datetime.utcnow().isoformat()
            self.source_health["jdbc_postgres"]["count"] += 1
            self.source_health["jdbc_postgres"]["status"] = "healthy"

        except Exception as e:
            logger.error(f"Error processing condition: {e}")
            self.stats["errors"] += 1

    def process_medication(self, msg):
        """Process a medication message from Debezium CDC."""
        try:
            value = self.avro_deserializer(
                msg.value(),
                SerializationContext(MEDICATIONS_TOPIC, MessageField.VALUE),
            )
            if not value:
                return

            pid = value.get("patient_id", "")
            if not pid:
                return

            redis_key = f"patient:{pid}"
            existing = self.redis_client.get(redis_key)
            context = json.loads(existing) if existing else {
                "patient_id": pid, "patient_age": 0, "patient_gender": "",
                "vitals": {}, "vitals_history": {}, "conditions": [],
                "medications": [], "last_updated": None,
            }

            med_entry = {
                "name": value.get("medication_name", ""),
                "dosage": value.get("dosage", ""),
                "frequency": value.get("frequency", ""),
                "route": value.get("route", ""),
                "status": value.get("status", ""),
            }

            existing_names = [m["name"] for m in context["medications"]]
            if med_entry["name"] in existing_names:
                idx = existing_names.index(med_entry["name"])
                context["medications"][idx] = med_entry
            else:
                context["medications"].append(med_entry)

            context["last_updated"] = datetime.utcnow().isoformat() + "Z"
            self.redis_client.set(redis_key, json.dumps(context), ex=PATIENT_TTL_SECONDS)
            self.redis_client.sadd("patients:all", pid)
            self.stats["medications_processed"] += 1
            self.stats["redis_writes"] += 1

            self.source_health["debezium_mysql"]["last_seen"] = datetime.utcnow().isoformat()
            self.source_health["debezium_mysql"]["count"] += 1
            self.source_health["debezium_mysql"]["status"] = "healthy"

        except Exception as e:
            logger.error(f"Error processing medication: {e}")
            self.stats["errors"] += 1

    def process_dlq(self, msg):
        """Process DLQ messages for the AI agent to analyze."""
        try:
            value = json.loads(msg.value().decode("utf-8"))
            self.dlq_errors.append({
                "error_reason": value.get("error_reason", "unknown"),
                "source_system": value.get("source_system", "unknown"),
                "failed_at": value.get("failed_at", ""),
                "topic_intended": value.get("topic_intended", ""),
            })
            # Keep last 100 DLQ entries
            if len(self.dlq_errors) > 100:
                self.dlq_errors.pop(0)

            self.redis_client.set("dlq:recent_errors", json.dumps(self.dlq_errors))
            self.stats["dlq_processed"] += 1

        except Exception as e:
            logger.error(f"Error processing DLQ: {e}")

    def update_health_metrics(self):
        """Write source health and global stats to Redis."""
        # Check for source staleness (no data in 5 minutes)
        now = datetime.utcnow()
        for source, health in self.source_health.items():
            if health["last_seen"]:
                try:
                    last = datetime.fromisoformat(health["last_seen"])
                    gap_seconds = (now - last).total_seconds()
                    if gap_seconds > 300:
                        health["status"] = "stale"
                        health["gap_seconds"] = gap_seconds
                    else:
                        health["status"] = "healthy"
                        health["gap_seconds"] = gap_seconds
                except:
                    pass

        self.redis_client.set("source:health", json.dumps(self.source_health))
        self.redis_client.set("stats:global", json.dumps(self.stats))

        # Count total patients in Redis
        total_patients = self.redis_client.scard("patients:all")
        self.redis_client.set("stats:total_patients", total_patients)

    def run(self):
        """Main event loop — consume from all topics."""
        logger.info("=" * 60)
        logger.info("HealthStream v2 — Context Materializer Starting")
        logger.info("=" * 60)
        logger.info(f"Kafka: {KAFKA_BOOTSTRAP}")
        logger.info(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
        logger.info(f"Topics: {VITALS_TOPIC}, {CONDITIONS_TOPIC}, {MEDICATIONS_TOPIC}, {DLQ_TOPIC}")
        logger.info("=" * 60)

        last_report = time.time()
        last_health_update = time.time()

        try:
            while True:
                # Poll vitals topic
                msg = self.vitals_consumer.poll(0.1)
                if msg and not msg.error():
                    self.process_vital(msg)
                elif msg and msg.error() and msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Vitals consumer error: {msg.error()}")

                # Poll conditions + medications topics
                msg2 = self.sources_consumer.poll(0.1)
                if msg2 and not msg2.error():
                    topic = msg2.topic()
                    if topic == CONDITIONS_TOPIC:
                        self.process_condition(msg2)
                    elif topic == MEDICATIONS_TOPIC:
                        self.process_medication(msg2)

                # Poll DLQ
                msg3 = self.dlq_consumer.poll(0.1)
                if msg3 and not msg3.error():
                    self.process_dlq(msg3)

                # Update health metrics every 10 seconds
                now = time.time()
                if now - last_health_update >= 10:
                    self.update_health_metrics()
                    last_health_update = now

                # Report progress every 30 seconds
                if now - last_report >= 30:
                    logger.info(
                        f"Vitals: {self.stats['vitals_processed']:,} | "
                        f"Conditions: {self.stats['conditions_processed']} | "
                        f"Medications: {self.stats['medications_processed']} | "
                        f"DLQ: {self.stats['dlq_processed']} | "
                        f"Redis writes: {self.stats['redis_writes']:,}"
                    )
                    last_report = now

        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            self.vitals_consumer.close()
            self.sources_consumer.close()
            self.dlq_consumer.close()
            self.update_health_metrics()
            logger.info("Context Materializer stopped")


if __name__ == "__main__":
    materializer = ContextMaterializer()
    materializer.run()
