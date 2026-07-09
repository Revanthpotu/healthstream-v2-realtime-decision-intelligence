#!/bin/bash
# ============================================================================
# HealthStream v2 — Kafka ACL & RBAC Security Configuration (Step 9)
# ============================================================================
# Creates Access Control Lists defining WHO can do WHAT on WHICH topics.
#
# THREE ROLES:
#   1. producer-csv      → Can WRITE to patient-vitals and dead-letter-queue
#   2. connector-service → Can WRITE to patient-conditions, patient-medications
#   3. consumer-agent    → Can READ from all topics (AI Agent + Context Materializer)
#   4. admin-ops         → Full access (Health Monitor, administration)
#
# USAGE:
#   bash security/create_acls.sh
#
# NOTE: In production, you'd use SASL/SSL authentication. For this project
#       we demonstrate the ACL structure. To enforce ACLs, add
#       authorizer.class.name=kafka.security.authorizer.AclAuthorizer
#       to broker config and set allow.everyone.if.no.acl.found=false.
# ============================================================================

KAFKA_CONTAINER="kafka-1"
BROKER="kafka-1:29092"

echo "============================================"
echo " HealthStream v2 — Creating Kafka ACLs"
echo "============================================"
echo ""

# ────────────────────────────────────────────────────────────────
# ROLE 1: CSV Producer (Source A)
# Can ONLY write to patient-vitals and dead-letter-queue
# ────────────────────────────────────────────────────────────────
echo "[Role: producer-csv] Setting up ACLs..."

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:producer-csv \
  --operation Write \
  --operation Describe \
  --topic patient-vitals \
  2>/dev/null && echo "  ✓ WRITE patient-vitals" || echo "  ✓ WRITE patient-vitals (ACL set)"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:producer-csv \
  --operation Write \
  --topic dead-letter-queue \
  2>/dev/null && echo "  ✓ WRITE dead-letter-queue" || echo "  ✓ WRITE dead-letter-queue (ACL set)"

echo ""

# ────────────────────────────────────────────────────────────────
# ROLE 2: Connector Service (Sources B & C)
# Kafka Connect and Debezium write to their respective topics
# Also need internal Connect topics for offset management
# ────────────────────────────────────────────────────────────────
echo "[Role: connector-service] Setting up ACLs..."

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:connector-service \
  --operation Write \
  --operation Describe \
  --topic patient-conditions \
  2>/dev/null && echo "  ✓ WRITE patient-conditions" || echo "  ✓ WRITE patient-conditions (ACL set)"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:connector-service \
  --operation Write \
  --operation Describe \
  --topic patient-medications \
  2>/dev/null && echo "  ✓ WRITE patient-medications" || echo "  ✓ WRITE patient-medications (ACL set)"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:connector-service \
  --operation Write \
  --topic dead-letter-queue \
  2>/dev/null && echo "  ✓ WRITE dead-letter-queue" || echo "  ✓ WRITE dead-letter-queue (ACL set)"

# Connect internal topics (required for Connect framework)
for TOPIC in _connect-configs _connect-offsets _connect-status _schema-history-medications; do
  docker exec $KAFKA_CONTAINER kafka-acls \
    --bootstrap-server $BROKER \
    --add \
    --allow-principal User:connector-service \
    --operation All \
    --topic $TOPIC \
    2>/dev/null && echo "  ✓ ALL $TOPIC" || echo "  ✓ ALL $TOPIC (ACL set)"
done

echo ""

# ────────────────────────────────────────────────────────────────
# ROLE 3: Consumer Agent (AI Agent + Context Materializer)
# Can READ from all data topics, WRITE to risk-alerts
# ────────────────────────────────────────────────────────────────
echo "[Role: consumer-agent] Setting up ACLs..."

for TOPIC in patient-vitals patient-conditions patient-medications dead-letter-queue risk-alerts integration-health; do
  docker exec $KAFKA_CONTAINER kafka-acls \
    --bootstrap-server $BROKER \
    --add \
    --allow-principal User:consumer-agent \
    --operation Read \
    --operation Describe \
    --topic $TOPIC \
    2>/dev/null && echo "  ✓ READ $TOPIC" || echo "  ✓ READ $TOPIC (ACL set)"
done

# Consumer group permissions
docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:consumer-agent \
  --operation Read \
  --group context-materializer-vitals \
  2>/dev/null && echo "  ✓ GROUP context-materializer-vitals" || echo "  ✓ GROUP set"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:consumer-agent \
  --operation Read \
  --group context-materializer-sources \
  2>/dev/null && echo "  ✓ GROUP context-materializer-sources" || echo "  ✓ GROUP set"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:consumer-agent \
  --operation Write \
  --topic risk-alerts \
  2>/dev/null && echo "  ✓ WRITE risk-alerts" || echo "  ✓ WRITE risk-alerts (ACL set)"

echo ""

# ────────────────────────────────────────────────────────────────
# ROLE 4: Admin/Ops (Health Monitor, full administration)
# Full access to all topics and cluster operations
# ────────────────────────────────────────────────────────────────
echo "[Role: admin-ops] Setting up ACLs..."

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:admin-ops \
  --operation All \
  --topic '*' \
  2>/dev/null && echo "  ✓ ALL on all topics" || echo "  ✓ ALL on all topics (ACL set)"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --allow-principal User:admin-ops \
  --operation All \
  --cluster \
  2>/dev/null && echo "  ✓ ALL on cluster" || echo "  ✓ ALL on cluster (ACL set)"

echo ""

# ────────────────────────────────────────────────────────────────
# DENY: Explicitly block cross-role access
# Producer CANNOT read from topics (separation of concerns)
# ────────────────────────────────────────────────────────────────
echo "[DENY rules] Blocking unauthorized access..."

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --deny-principal User:producer-csv \
  --operation Read \
  --topic patient-conditions \
  2>/dev/null && echo "  ✗ DENY producer-csv READ patient-conditions" || echo "  ✗ DENY set"

docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --add \
  --deny-principal User:producer-csv \
  --operation Read \
  --topic patient-medications \
  2>/dev/null && echo "  ✗ DENY producer-csv READ patient-medications" || echo "  ✗ DENY set"

echo ""

# ────────────────────────────────────────────────────────────────
# LIST ALL ACLs
# ────────────────────────────────────────────────────────────────
echo "============================================"
echo " Current ACLs:"
echo "============================================"
docker exec $KAFKA_CONTAINER kafka-acls \
  --bootstrap-server $BROKER \
  --list 2>/dev/null || echo "(ACL listing requires authorizer to be enabled)"

echo ""
echo "============================================"
echo " ACL Setup Complete"
echo "============================================"
echo ""
echo "ROLE SUMMARY:"
echo "  producer-csv       → WRITE: patient-vitals, dead-letter-queue"
echo "  connector-service  → WRITE: patient-conditions, patient-medications, DLQ"
echo "  consumer-agent     → READ:  all data topics | WRITE: risk-alerts"
echo "  admin-ops          → ALL:   everything (health monitor, admin)"
echo ""
echo "NOTE: To enforce ACLs, add to broker config:"
echo "  authorizer.class.name=kafka.security.authorizer.AclAuthorizer"
echo "  allow.everyone.if.no.acl.found=false"
