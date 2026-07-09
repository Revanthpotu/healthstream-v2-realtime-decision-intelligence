#!/bin/bash
# ============================================================================
# HealthStream v2 — Deploy Kafka Connectors (Steps 3 & 4)
# ============================================================================
# Deploys JDBC Source (PostgreSQL) and Debezium (MySQL CDC) connectors
# via the Kafka Connect REST API.
# ============================================================================

CONNECT_URL="http://localhost:8083"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "============================================"
echo " Deploying Kafka Connectors"
echo "============================================"
echo ""

# Wait for Kafka Connect to be ready
echo "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 60); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" $CONNECT_URL/connectors 2>/dev/null)
    if [ "$STATUS" = "200" ]; then
        echo "  Kafka Connect is ready!"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "  ERROR: Kafka Connect not ready after 60 seconds"
        exit 1
    fi
    echo "  Waiting... ($i/60)"
    sleep 2
done

echo ""

# ── Deploy JDBC Source Connector (Method 2: PostgreSQL → Kafka) ─────────
echo "[Method 2] Deploying JDBC Source Connector (PostgreSQL → patient-conditions)..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$PROJECT_DIR/connectors/jdbc-source-postgres.json")

if [ "$RESPONSE" = "201" ] || [ "$RESPONSE" = "200" ]; then
    echo "  ✓ JDBC connector deployed successfully"
elif [ "$RESPONSE" = "409" ]; then
    echo "  ✓ JDBC connector already exists"
else
    echo "  ✗ JDBC connector deployment failed (HTTP $RESPONSE)"
    echo "  Attempting to update existing connector..."
    curl -s -X PUT \
        "$CONNECT_URL/connectors/jdbc-source-postgres/config" \
        -H "Content-Type: application/json" \
        -d "$(cat $PROJECT_DIR/connectors/jdbc-source-postgres.json | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin)["config"]))')" \
        > /dev/null 2>&1
    echo "  ✓ JDBC connector updated"
fi

echo ""

# ── Deploy Debezium CDC Connector (Method 3: MySQL → Kafka) ────────────
echo "[Method 3] Deploying Debezium CDC Connector (MySQL → patient-medications)..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
    "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @"$PROJECT_DIR/connectors/debezium-source-mysql.json")

if [ "$RESPONSE" = "201" ] || [ "$RESPONSE" = "200" ]; then
    echo "  ✓ Debezium connector deployed successfully"
elif [ "$RESPONSE" = "409" ]; then
    echo "  ✓ Debezium connector already exists"
else
    echo "  ✗ Debezium connector deployment failed (HTTP $RESPONSE)"
    echo "  Attempting to update existing connector..."
    curl -s -X PUT \
        "$CONNECT_URL/connectors/debezium-source-mysql/config" \
        -H "Content-Type: application/json" \
        -d "$(cat $PROJECT_DIR/connectors/debezium-source-mysql.json | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin)["config"]))')" \
        > /dev/null 2>&1
    echo "  ✓ Debezium connector updated"
fi

echo ""

# ── Verify Connectors ──────────────────────────────────────────────────
echo "============================================"
echo " Connector Status"
echo "============================================"
sleep 5  # Wait for connectors to initialize

for CONNECTOR in jdbc-source-postgres debezium-source-mysql; do
    STATUS=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR/status" 2>/dev/null)
    CONN_STATE=$(echo $STATUS | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
    TASK_STATE=$(echo $STATUS | python3 -c "import sys,json; tasks=json.load(sys.stdin).get('tasks',[]); print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null)
    echo "  $CONNECTOR: connector=$CONN_STATE task=$TASK_STATE"
done

echo ""
echo "Connectors deployed! Data should start flowing shortly."
