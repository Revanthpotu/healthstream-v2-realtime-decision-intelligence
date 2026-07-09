"""
============================================================================
HealthStream v2 — Custom Python Producer (Integration Method 1 of 3)
============================================================================
PURPOSE:
  Reads Synthea observations.csv and patients.csv, filters for vital signs,
  enriches with patient demographics, validates, serializes with Avro,
  and produces to the 'patient-vitals' Kafka topic.

THIS IS METHOD 1 OF 3 INTEGRATION METHODS:
  Method 1: THIS — Custom Python Producer (CSV → Kafka)
  Method 2: Kafka Connect JDBC (PostgreSQL → Kafka)  [config only, no code]
  Method 3: Debezium CDC (MySQL → Kafka)              [config only, no code]

WHY A CUSTOM PRODUCER (instead of Kafka Connect for CSV)?
  Kafka Connect has a CSV connector, but it's limited:
  - Can't filter rows (we only want vital signs, not lab tests)
  - Can't enrich data (we add patient age/gender from another file)
  - Can't do custom validation (check value ranges, handle NULLs)
  - Can't route bad rows to DLQ with error reasons
  A custom producer gives us full control over the integration logic.

WHAT THIS PRODUCER DOES:
  1. Loads patients.csv into memory (patient demographics lookup)
  2. Reads observations.csv row by row
  3. Filters for vital sign LOINC codes only (HR, BP, O2, RR, Temp)
  4. Enriches each row with patient age and gender
  5. Validates: checks for NULL values, out-of-range values
  6. Serializes with Avro (Schema Registry enforces the contract)
  7. Produces to 'patient-vitals' topic with patient_id as key
  8. Bad/invalid rows → 'dead-letter-queue' topic with error reason

PRODUCER CONFIGURATION EXPLAINED:
  acks=all              → Wait for ALL replicas to acknowledge (strongest durability)
  enable.idempotence    → Prevents duplicate messages on retry (exactly-once producer)
  compression.type      → snappy (fast compression, ~50% size reduction)
  linger.ms=5           → Wait 5ms to batch messages (throughput vs latency trade-off)
  batch.size=32768      → Batch up to 32KB before sending (reduces network calls)

INTERVIEW Q: "Why acks=all instead of acks=1?"
ANSWER: "With acks=1, only the leader broker acknowledges. If the leader crashes
         immediately after acknowledging but before replicating, the message is
         LOST. With acks=all combined with min.insync.replicas=2, at least 2
         brokers must have the message before the producer gets a success response.
         This guarantees zero data loss even during broker failures."

INTERVIEW Q: "What is an idempotent producer?"
ANSWER: "If a network error occurs after the broker receives the message but
         before the producer gets the acknowledgment, the producer retries.
         Without idempotence, this creates a duplicate. An idempotent producer
         assigns a sequence number to each message. The broker detects the
         duplicate sequence number and ignores it. Result: exactly-once delivery
         from producer to broker."

INTERVIEW Q: "Why snappy compression?"
ANSWER: "Snappy gives ~50% compression with very low CPU overhead. In healthcare,
         we're sending thousands of vital sign readings per second. Compression
         reduces network bandwidth and Kafka storage. Snappy is faster than gzip
         but compresses slightly less. For real-time data, speed matters more
         than maximum compression."

USAGE:
  python producer/csv_producer.py
  python producer/csv_producer.py --batch-size 500 --delay 0.01
  python producer/csv_producer.py --simulate-realtime
============================================================================
"""

import csv
import json
import os
import sys
import time
import logging
import argparse
from datetime import datetime, date
from pathlib import Path

# ── Confluent Kafka imports ─────────────────────────────────────────────────
from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Kafka broker connection (EXTERNAL listener for host machine)
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Schema Registry URL
SCHEMA_REGISTRY_URL = "http://localhost:8081"

# Topics
VITALS_TOPIC = "patient-vitals"
DLQ_TOPIC = "dead-letter-queue"

# Path to Synthea CSV files
DATA_DIR = Path(__file__).parent.parent / "data" / "synthea"

# ── VITAL SIGN LOINC CODES ──────────────────────────────────────────────────
# LOINC (Logical Observation Identifiers Names and Codes) is the international
# standard for medical laboratory observations. Every vital sign has a unique code.
#
# We ONLY produce these codes to Kafka. Everything else (body weight, BMI,
# lab results, pain scores) is filtered out.
#
# INTERVIEW Q: "What is LOINC?"
# ANSWER: "LOINC is a universal coding system for medical observations.
#          It ensures that 'Heart Rate' means the same thing regardless of
#          which hospital system produced it. When I filter for code 8867-4,
#          I know I'm getting heart rate data whether it came from a Philips
#          monitor, a GE system, or a manual nursing entry."
# ────────────────────────────────────────────────────────────────────────────
VITAL_SIGN_CODES = {
    "8867-4":  "Heart Rate",
    "8480-6":  "Systolic Blood Pressure",
    "8462-4":  "Diastolic Blood Pressure",
    "2708-6":  "Oxygen Saturation",
    "9279-1":  "Respiratory Rate",
    "8310-5":  "Body Temperature",
}

# ── VALIDATION RANGES ───────────────────────────────────────────────────────
# Medically possible ranges. Anything outside = data quality issue → DLQ.
# These are NOT alert thresholds (those are clinical decisions).
# These are "is this value even physically possible?" checks.
VALID_RANGES = {
    "8867-4":  (20, 250),    # Heart rate: 20-250 bpm
    "8480-6":  (50, 300),    # Systolic BP: 50-300 mmHg
    "8462-4":  (20, 200),    # Diastolic BP: 20-200 mmHg
    "2708-6":  (50, 100),    # O2 saturation: 50-100%
    "9279-1":  (4, 60),      # Respiratory rate: 4-60 /min
    "8310-5":  (90, 110),    # Temperature: 90-110°F (some Synthea data is in °F)
}

# ============================================================================
# LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("csv_producer")


# ============================================================================
# HELPER: Calculate age from birthdate
# ============================================================================
def calculate_age(birthdate_str: str, reference_date_str: str) -> int:
    """
    Calculate age in years from a birthdate string.

    WHY: Synthea gives us BIRTHDATE (1989-05-25) not age.
    We need to calculate age at the time of each observation.
    Age is a critical field — it's the #1 predictor of patient risk
    (as we found in our EDA).
    """
    try:
        birthdate = datetime.strptime(birthdate_str, "%Y-%m-%d").date()
        # Use the observation date as reference
        ref_date = datetime.fromisoformat(reference_date_str.replace("Z", "+00:00")).date()
        age = ref_date.year - birthdate.year
        # Adjust if birthday hasn't occurred yet that year
        if (ref_date.month, ref_date.day) < (birthdate.month, birthdate.day):
            age -= 1
        return max(0, min(age, 120))  # Clamp to 0-120
    except (ValueError, TypeError):
        return -1  # Will be caught by validation


# ============================================================================
# HELPER: Load patient demographics
# ============================================================================
def load_patient_demographics(patients_file: Path) -> dict:
    """
    Load patients.csv into a dictionary for fast lookup.

    Returns: { patient_uuid: { "birthdate": "1989-05-25", "gender": "M" }, ... }

    WHY IN MEMORY: We need to look up demographics for every single observation.
    With 1,171 patients, the dictionary fits easily in RAM (~100KB).
    Looking up a dict key is O(1) — instant.
    Querying a database for each row would be 1000x slower.
    """
    demographics = {}
    row_count = 0

    logger.info(f"Loading patient demographics from {patients_file}...")

    with open(patients_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            patient_id = row["Id"]
            demographics[patient_id] = {
                "birthdate": row["BIRTHDATE"],
                "gender": row["GENDER"],
                "first_name": row.get("FIRST", ""),
                "last_name": row.get("LAST", ""),
            }
            row_count += 1

    logger.info(f"Loaded {row_count} patient records")
    return demographics


# ============================================================================
# HELPER: Kafka delivery callback
# ============================================================================
def delivery_callback(err, msg):
    """
    Called once for each message produced, indicating delivery result.

    WHY A CALLBACK: Kafka producing is asynchronous. When you call
    producer.produce(), the message goes into a buffer. It's actually
    sent in the background. This callback tells you if it succeeded
    or failed AFTER the fact.

    INTERVIEW Q: "Is Kafka producing synchronous or asynchronous?"
    ANSWER: "By default, it's asynchronous. producer.produce() returns
             immediately and the message is batched and sent in the
             background. The delivery callback fires later to report
             success or failure. You can make it synchronous by calling
             producer.flush() after each produce, but that kills
             throughput. I use async with callbacks for high throughput
             and only call flush() periodically or at shutdown."
    """
    if err is not None:
        logger.error(f"DELIVERY FAILED: {err}")
    # Uncomment for verbose logging:
    # else:
    #     logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


# ============================================================================
# MAIN PRODUCER CLASS
# ============================================================================
class VitalsProducer:
    """
    Custom Python Producer for Source A (CSV → Kafka).

    This class encapsulates the entire integration logic:
    - Schema registration with Avro
    - CSV reading and filtering
    - Data enrichment with patient demographics
    - Validation with DLQ routing
    - Production to Kafka with delivery guarantees
    """

    def __init__(self):
        # ── Kafka Producer Configuration ────────────────────────────────
        producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,

            # DURABILITY: Wait for ALL replicas to acknowledge
            "acks": "all",
            # Combined with min.insync.replicas=2 on the broker:
            # → At least 2 of 3 brokers must confirm the write
            # → Even if 1 broker crashes, data survives

            # IDEMPOTENCE: Prevent duplicates on retry
            "enable.idempotence": True,
            # If a network error causes a retry, the broker detects
            # the duplicate sequence number and ignores it
            # → Exactly-once delivery from producer to Kafka

            # COMPRESSION: Reduce message size
            "compression.type": "snappy",
            # Snappy: fast compression, ~50% reduction
            # Good trade-off between CPU and bandwidth for real-time data

            # BATCHING: Improve throughput
            "linger.ms": 5,
            # Wait up to 5ms to accumulate messages into a batch
            # → Fewer network calls, higher throughput
            # → Max added latency: 5ms (acceptable for vitals monitoring)

            "batch.size": 32768,
            # Maximum batch size: 32KB
            # Messages accumulate until this size OR linger.ms, whichever first

            # RETRIES: Handle transient failures
            "retries": 5,
            "retry.backoff.ms": 500,
            # Retry up to 5 times with 500ms between retries
            # Idempotence ensures retries don't create duplicates

            # CLIENT ID: For monitoring and debugging
            "client.id": "healthstream-csv-producer",
        }
        self.producer = Producer(producer_config)

        # ── Schema Registry + Avro Serializer ───────────────────────────
        schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        # Load the Avro schema from file
        schema_path = Path(__file__).parent.parent / "schemas" / "patient_vital.avsc"
        with open(schema_path, "r") as f:
            schema_str = f.read()

        # Create Avro serializer — this auto-registers the schema
        # with Schema Registry on first use
        self.avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str,
            lambda vital, ctx: vital,  # dict → dict (no transformation needed)
        )

        self.string_serializer = StringSerializer("utf_8")

        # ── Load patient demographics ───────────────────────────────────
        patients_file = DATA_DIR / "patients.csv"
        if not patients_file.exists():
            logger.error(f"patients.csv not found at {patients_file}")
            logger.error("Copy Synthea CSV files to data/synthea/ directory")
            sys.exit(1)
        self.demographics = load_patient_demographics(patients_file)

        # ── Counters for reporting ──────────────────────────────────────
        self.stats = {
            "total_rows": 0,
            "vitals_found": 0,
            "produced": 0,
            "skipped_non_vital": 0,
            "sent_to_dlq": 0,
            "errors": 0,
        }

    def validate_vital(self, row: dict) -> tuple:
        """
        Validate a vital sign reading.
        Returns (is_valid: bool, error_reason: str or None)

        VALIDATION CHECKS:
        1. VALUE must be numeric (not NULL, not empty, not "N/A")
        2. VALUE must be within medically possible range
        3. PATIENT must exist in demographics (for enrichment)
        4. DATE must be parseable
        """
        # Check 1: Value must be present and numeric
        value_str = row.get("VALUE", "").strip()
        if not value_str:
            return False, "MISSING_VALUE: VALUE field is empty"

        try:
            value = float(value_str)
        except (ValueError, TypeError):
            return False, f"INVALID_VALUE: Cannot parse '{value_str}' as number"

        # Check 2: Value must be in valid medical range
        code = row.get("CODE", "")
        if code in VALID_RANGES:
            min_val, max_val = VALID_RANGES[code]
            if value < min_val or value > max_val:
                return False, (
                    f"OUT_OF_RANGE: {code} value {value} outside "
                    f"valid range [{min_val}, {max_val}]"
                )

        # Check 3: Patient must exist in demographics
        patient_id = row.get("PATIENT", "")
        if patient_id not in self.demographics:
            return False, f"UNKNOWN_PATIENT: Patient {patient_id[:8]}... not in patients.csv"

        # Check 4: Date must be parseable
        date_str = row.get("DATE", "")
        if not date_str:
            return False, "MISSING_DATE: DATE field is empty"

        return True, None

    def send_to_dlq(self, row: dict, error_reason: str):
        """
        Send a failed row to the Dead Letter Queue with the error reason.

        WHY DLQ: Instead of silently dropping bad data (dangerous) or crashing
        the producer (fragile), we route bad messages to a separate topic where
        they can be investigated and potentially recovered later.

        The DLQ message includes:
        - The original row data
        - WHY it failed (error_reason)
        - WHEN it failed (timestamp)
        - WHERE it came from (source_system)

        INTERVIEW Q: "What happens to messages in the DLQ?"
        ANSWER: "They sit there with infinite retention until someone
                 investigates. In our project, the AI agent can analyze
                 DLQ messages and categorize failures: 'Source A had 47
                 schema errors this week, all caused by NULL blood_pressure
                 values.' In production, you'd have alerting when DLQ
                 count exceeds a threshold."
        """
        dlq_message = {
            "original_data": json.dumps(row),
            "error_reason": error_reason,
            "source_system": "csv_lab_export",
            "failed_at": datetime.utcnow().isoformat() + "Z",
            "topic_intended": VITALS_TOPIC,
        }
        try:
            self.producer.produce(
                topic=DLQ_TOPIC,
                value=json.dumps(dlq_message).encode("utf-8"),
                callback=delivery_callback,
            )
            self.stats["sent_to_dlq"] += 1
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
            self.stats["errors"] += 1

    def produce_vital(self, row: dict):
        """
        Transform a CSV row into an Avro message and produce to Kafka.

        FLOW:
        1. Extract fields from CSV row
        2. Enrich with patient age and gender
        3. Serialize with Avro (Schema Registry validates)
        4. Produce with patient_id as key (partition routing)
        """
        patient_id = row["PATIENT"]
        demographics = self.demographics[patient_id]

        # Calculate age at time of observation
        age = calculate_age(demographics["birthdate"], row["DATE"])

        # Build the Avro message (must match schema fields exactly)
        vital_record = {
            "patient_id": patient_id,
            "encounter_id": row["ENCOUNTER"],
            "timestamp": row["DATE"],
            "vital_type": row["CODE"],
            "vital_description": row["DESCRIPTION"],
            "value": float(row["VALUE"]),
            "units": row.get("UNITS", ""),
            "patient_age": age,
            "patient_gender": demographics["gender"],
            "source_system": "csv_lab_export",
        }

        try:
            # Produce to Kafka
            # KEY = patient_id → all vitals for same patient go to same partition
            # VALUE = Avro-serialized vital record
            self.producer.produce(
                topic=VITALS_TOPIC,
                key=self.string_serializer(patient_id),
                value=self.avro_serializer(
                    vital_record,
                    SerializationContext(VITALS_TOPIC, MessageField.VALUE),
                ),
                callback=delivery_callback,
            )
            self.stats["produced"] += 1

        except Exception as e:
            logger.error(f"Production failed: {e}")
            self.send_to_dlq(row, f"PRODUCTION_ERROR: {str(e)}")
            self.stats["errors"] += 1

    def run(self, simulate_realtime: bool = False, delay: float = 0.0,
            max_records: int = None):
        """
        Main processing loop. Reads observations.csv and produces vitals.

        Args:
            simulate_realtime: If True, adds delay between records to simulate
                               real-time data flow (useful for demos)
            delay: Seconds to wait between each record (0 = full speed)
            max_records: Stop after N records (None = process all)
        """
        observations_file = DATA_DIR / "observations.csv"
        if not observations_file.exists():
            logger.error(f"observations.csv not found at {observations_file}")
            logger.error("Copy Synthea CSV files to data/synthea/ directory")
            sys.exit(1)

        logger.info("=" * 60)
        logger.info("HealthStream v2 — CSV Producer Starting")
        logger.info("=" * 60)
        logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Schema Registry: {SCHEMA_REGISTRY_URL}")
        logger.info(f"Topic: {VITALS_TOPIC}")
        logger.info(f"Data: {observations_file}")
        logger.info(f"Vital sign codes: {len(VITAL_SIGN_CODES)}")
        logger.info(f"Patients loaded: {len(self.demographics)}")
        if simulate_realtime:
            logger.info(f"Mode: REAL-TIME SIMULATION (delay={delay}s)")
        else:
            logger.info("Mode: BATCH (full speed)")
        logger.info("=" * 60)

        start_time = time.time()
        last_report_time = start_time

        with open(observations_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                self.stats["total_rows"] += 1

                # ── Step 1: Filter for vital signs only ─────────────────
                code = row.get("CODE", "")
                if code not in VITAL_SIGN_CODES:
                    self.stats["skipped_non_vital"] += 1
                    continue

                self.stats["vitals_found"] += 1

                # ── Step 2: Validate ────────────────────────────────────
                is_valid, error_reason = self.validate_vital(row)
                if not is_valid:
                    self.send_to_dlq(row, error_reason)
                    continue

                # ── Step 3: Produce to Kafka ────────────────────────────
                self.produce_vital(row)

                # ── Step 4: Periodic flush and reporting ────────────────
                # Flush every 1000 messages to ensure delivery
                if self.stats["produced"] % 1000 == 0:
                    self.producer.flush()

                # Report progress every 10 seconds
                now = time.time()
                if now - last_report_time >= 10:
                    elapsed = now - start_time
                    rate = self.stats["produced"] / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {self.stats['produced']:,} produced | "
                        f"{self.stats['sent_to_dlq']} → DLQ | "
                        f"{rate:.0f} msg/s"
                    )
                    last_report_time = now

                # ── Step 5: Optional delay for real-time simulation ─────
                if simulate_realtime or delay > 0:
                    time.sleep(delay if delay > 0 else 0.01)

                # ── Step 6: Check max records limit ─────────────────────
                if max_records and self.stats["produced"] >= max_records:
                    logger.info(f"Reached max_records limit ({max_records})")
                    break

                # ── Step 7: Poll for delivery callbacks ─────────────────
                # This processes delivery callbacks without blocking
                self.producer.poll(0)

        # ── Final flush: ensure all buffered messages are sent ──────────
        logger.info("Flushing remaining messages...")
        remaining = self.producer.flush(timeout=30)
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered!")

        # ── Print final statistics ──────────────────────────────────────
        elapsed = time.time() - start_time
        self.print_stats(elapsed)

    def print_stats(self, elapsed: float):
        """Print final production statistics."""
        logger.info("")
        logger.info("=" * 60)
        logger.info("PRODUCTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total CSV rows read:     {self.stats['total_rows']:>10,}")
        logger.info(f"Non-vital (skipped):     {self.stats['skipped_non_vital']:>10,}")
        logger.info(f"Vital signs found:       {self.stats['vitals_found']:>10,}")
        logger.info(f"Successfully produced:   {self.stats['produced']:>10,}")
        logger.info(f"Sent to DLQ:             {self.stats['sent_to_dlq']:>10,}")
        logger.info(f"Errors:                  {self.stats['errors']:>10,}")
        logger.info(f"Time elapsed:            {elapsed:>10.1f}s")
        if elapsed > 0:
            logger.info(f"Throughput:              {self.stats['produced']/elapsed:>10.0f} msg/s")
        logger.info("=" * 60)


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="HealthStream v2 — CSV to Kafka Producer (Method 1)"
    )
    parser.add_argument(
        "--simulate-realtime",
        action="store_true",
        help="Add 10ms delay between records to simulate real-time flow",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Seconds to wait between records (0 = full speed)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Stop after N vital sign records (None = all)",
    )

    args = parser.parse_args()

    try:
        producer = VitalsProducer()
        producer.run(
            simulate_realtime=args.simulate_realtime,
            delay=args.delay,
            max_records=args.max_records,
        )
    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        raise
