#!/bin/bash
# ============================================================================
# HealthStream v2 — Topic Creation Script
# ============================================================================
# Run this AFTER docker-compose up -d and all brokers are healthy.
#
# WHY CREATE TOPICS MANUALLY (instead of auto-create)?
#   Auto-created topics use default settings (3 partitions, 30 day retention).
#   We need SPECIFIC configurations per topic:
#   - patient-vitals needs 6 partitions (high throughput)
#   - dead-letter-queue needs infinite retention (never lose failed messages)
#   - Each topic has a justified partition count and retention period
# ============================================================================

BOOTSTRAP="localhost:9092"

echo "============================================"
echo "Creating HealthStream v2 Kafka Topics"
echo "============================================"

# ── TOPIC 1: patient-vitals ─────────────────────────────────────────────────
# Source: Custom Python Producer (Method 1 — CSV files)
# Partitions: 6 (high volume — vitals come every few seconds per patient)
# Retention: 30 days (clinical lookback window)
# Key: patient_id (ordering guarantee per patient)
echo "Creating patient-vitals..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic patient-vitals \
  --partitions 6 \
  --replication-factor 3 \
  --config retention.ms=2592000000 \
  --config cleanup.policy=delete \
  --if-not-exists
# retention.ms = 30 days × 24h × 60m × 60s × 1000ms = 2,592,000,000

# ── TOPIC 2: patient-conditions ─────────────────────────────────────────────
# Source: Kafka Connect JDBC from PostgreSQL (Method 2)
# Partitions: 3 (lower volume — conditions change rarely)
# Retention: 90 days (chronic conditions are long-term)
echo "Creating patient-conditions..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic patient-conditions \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --if-not-exists
# retention.ms = 90 days

# ── TOPIC 3: patient-medications ────────────────────────────────────────────
# Source: Debezium CDC from MySQL (Method 3)
# Partitions: 3 (moderate volume)
# Retention: 90 days (medication context needed for months)
echo "Creating patient-medications..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic patient-medications \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --if-not-exists

# ── TOPIC 4: patient-context ───────────────────────────────────────────────
# Source: ksqlDB (materialized join of all 3 sources)
# Partitions: 6 (matches patient-vitals for co-partitioning)
# Retention: 30 days
# cleanup.policy=compact: Keeps ONLY the latest value per key
#   WHY COMPACT: We only care about the CURRENT state of each patient.
#   Old context records are useless — compaction removes them, saving space.
echo "Creating patient-context..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic patient-context \
  --partitions 6 \
  --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=3600000 \
  --if-not-exists
# compact = keep only latest per key
# min.compaction.lag.ms = 1 hour (don't compact messages less than 1 hour old)

# ── TOPIC 5: integration-health ────────────────────────────────────────────
# Source: Health monitor service
# Partitions: 3 (one per source)
# Retention: 7 days (short-term operational data)
echo "Creating integration-health..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic integration-health \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --if-not-exists
# retention.ms = 7 days

# ── TOPIC 6: risk-alerts ───────────────────────────────────────────────────
# Source: AI Agent output
# Partitions: 3
# Retention: 90 days (audit trail for compliance)
echo "Creating risk-alerts..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic risk-alerts \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete \
  --if-not-exists

# ── TOPIC 7: dead-letter-queue ─────────────────────────────────────────────
# Source: Failed messages from any stage
# Partitions: 1 (low volume, ordering doesn't matter)
# Retention: INFINITE (-1 means never delete)
#   WHY INFINITE: Failed messages need investigation. You should never
#   lose a failed message before someone has looked at it.
echo "Creating dead-letter-queue..."
docker exec kafka-1 kafka-topics --create \
  --bootstrap-server kafka-1:29092 \
  --topic dead-letter-queue \
  --partitions 1 \
  --replication-factor 3 \
  --config retention.ms=-1 \
  --config cleanup.policy=delete \
  --if-not-exists
# retention.ms = -1 means INFINITE retention

echo ""
echo "============================================"
echo "All topics created! Listing topics:"
echo "============================================"
docker exec kafka-1 kafka-topics --list --bootstrap-server kafka-1:29092

echo ""
echo "============================================"
echo "Topic details:"
echo "============================================"
for TOPIC in patient-vitals patient-conditions patient-medications patient-context integration-health risk-alerts dead-letter-queue; do
  echo ""
  echo "--- $TOPIC ---"
  docker exec kafka-1 kafka-topics --describe --topic $TOPIC --bootstrap-server kafka-1:29092
done
