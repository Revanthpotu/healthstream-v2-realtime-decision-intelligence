"""
============================================================================
HealthStream v2 — Risk Score Publisher (Closed Loop, Part 1)
============================================================================
Reads ML readmission risk scores from Snowflake and publishes them to the
Kafka topic 'patient-risk-scores' as JSON.

This is the first half of the closed loop:
  Snowflake (ML scores) --> Kafka topic

A separate consumer then moves them from Kafka into Redis for the dashboard.

We use plain JSON (not Avro) for this side-channel topic to keep it simple
and independent of the Schema Registry used by the main vitals pipeline.

USAGE:
  export SNOWFLAKE_PASSWORD="..."
  python risk_score_publisher.py
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from confluent_kafka import Producer
import snowflake.connector

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
RISK_TOPIC = "patient-risk-scores"

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "SHPBEPY-VN05293")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "POTUREVANTH666")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "HEALTHSTREAM")

if not SNOWFLAKE_PASSWORD:
    print("ERROR: Set SNOWFLAKE_PASSWORD in .env")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("risk_publisher")


def fetch_risk_scores():
    """Pull the latest risk score per patient from Snowflake.
    A patient may have multiple stays; we take their highest-risk stay."""
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT patient_id, risk_level, risk_pct, readmit_probability
        FROM (
            SELECT
                patient_id,
                risk_level,
                risk_pct,
                readmit_probability,
                ROW_NUMBER() OVER (
                    PARTITION BY patient_id
                    ORDER BY readmit_probability DESC
                ) AS rn
            FROM HEALTHSTREAM.ML.PATIENT_RISK_LEVELS
        )
        WHERE rn = 1
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Delivery failed: {err}")


def main():
    logger.info("Connecting to Snowflake to fetch risk scores...")
    rows = fetch_risk_scores()
    logger.info(f"Fetched {len(rows)} patient risk scores")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    published = 0
    for patient_id, risk_level, risk_pct, prob in rows:
        message = {
            "patient_id": patient_id,
            "risk_level": risk_level,
            "risk_pct": float(risk_pct) if risk_pct is not None else None,
            "readmit_probability": float(prob) if prob is not None else None,
            "source": "snowflake_cortex_readmission_model",
        }
        producer.produce(
            RISK_TOPIC,
            key=patient_id,
            value=json.dumps(message),
            callback=delivery_report,
        )
        published += 1
        if published % 200 == 0:
            producer.poll(0)

    producer.flush()
    logger.info(f"Published {published} risk scores to topic '{RISK_TOPIC}'")
    logger.info("Done. The consumer will now move these into Redis.")


if __name__ == "__main__":
    main()
