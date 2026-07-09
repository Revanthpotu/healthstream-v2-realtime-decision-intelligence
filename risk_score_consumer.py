"""
============================================================================
HealthStream v2 — Risk Score Consumer (Closed Loop, Part 2)
============================================================================
Reads patient risk scores from the Kafka topic 'patient-risk-scores' and
writes them into Redis as:
    risk:{patient_id}  ->  JSON { risk_level, risk_pct, ... }

This is the second half of the closed loop:
  Kafka topic --> Redis --> (dashboard reads it)

Runs continuously alongside the other services. It does NOT touch the
existing vitals materializer — it only writes new 'risk:*' keys.

USAGE:
  python risk_score_consumer.py
============================================================================
"""

import os
import sys
import json
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer
import redis

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
RISK_TOPIC = "patient-risk-scores"
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("risk_consumer")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "risk-score-consumer",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([RISK_TOPIC])

    logger.info(f"Consuming '{RISK_TOPIC}' -> writing risk:* keys to Redis "
                f"{REDIS_HOST}:{REDIS_PORT}")
    logger.info("Press Ctrl+C to stop.")

    written = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value())
                pid = data["patient_id"]
                # Store the risk score keyed by patient. 24h TTL so it stays
                # fresh, matching the vitals TTL pattern.
                r.set(f"risk:{pid}", json.dumps({
                    "risk_level": data.get("risk_level"),
                    "risk_pct": data.get("risk_pct"),
                    "readmit_probability": data.get("readmit_probability"),
                    "source": data.get("source"),
                }), ex=86400)
                written += 1
                if written % 100 == 0:
                    logger.info(f"Wrote {written} risk scores to Redis")
            except Exception as e:
                logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info(f"\nStopping. Total risk scores written: {written}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
