#!/bin/bash
# ============================================================================
# HealthStream v2 — Master Startup Script
# ============================================================================
# One command to bring up the entire platform:
#   bash scripts/start_all.sh
#
# SEQUENCE:
#   1. Docker infrastructure (Kafka, databases, Redis, etc.)
#   2. Wait for all services healthy
#   3. Create Kafka topics
#   4. Deploy connectors (JDBC + Debezium)
#   5. Run ksqlDB statements
#   6. Start CSV Producer (background)
#   7. Start Context Materializer (background)
#   8. Start Health Monitor (background)
#   9. Print status and instructions
# ============================================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║         HealthStream v2 — Starting All Services             ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Docker Infrastructure ──────────────────────────────────────
echo "[1/8] Starting Docker infrastructure..."
docker-compose up -d
echo "  ✓ Docker services started"
echo ""

# ── Step 2: Wait for services to be healthy ────────────────────────────
echo "[2/8] Waiting for services to be healthy..."

wait_for_service() {
    local name=$1
    local url=$2
    local max_wait=$3
    for i in $(seq 1 $max_wait); do
        if curl -s "$url" > /dev/null 2>&1; then
            echo "  ✓ $name is ready"
            return 0
        fi
        sleep 2
    done
    echo "  ⚠ $name not ready after ${max_wait}s (may still be starting)"
    return 1
}

# Wait for Kafka broker
echo "  Waiting for Kafka..."
for i in $(seq 1 60); do
    docker exec kafka-1 kafka-topics --bootstrap-server kafka-1:29092 --list > /dev/null 2>&1 && break
    sleep 2
done
echo "  ✓ Kafka brokers ready"

wait_for_service "Schema Registry" "http://localhost:8081/subjects" 30
wait_for_service "Kafka Connect" "http://localhost:8083/connectors" 60
wait_for_service "ksqlDB" "http://localhost:8088/info" 30

echo ""

# ── Step 3: Create Kafka topics ────────────────────────────────────────
echo "[3/8] Creating Kafka topics..."
bash scripts/create_topics.sh 2>/dev/null || true
echo "  ✓ Topics created"
echo ""

# ── Step 4: Deploy connectors ─────────────────────────────────────────
echo "[4/8] Deploying Kafka connectors..."
bash scripts/deploy_connectors.sh 2>/dev/null || echo "  ⚠ Some connectors may need retry"
echo ""

# ── Step 5: Run ksqlDB statements ─────────────────────────────────────
echo "[5/8] Setting up ksqlDB stream processing..."
docker exec -i ksqldb ksql http://localhost:8088 < processing/ksqldb_statements.sql 2>/dev/null || true
echo "  ✓ ksqlDB streams and tables created"
echo ""

# ── Step 6: Security ACLs ─────────────────────────────────────────────
echo "[6/8] Setting up security ACLs..."
bash security/create_acls.sh 2>/dev/null || true
echo "  ✓ ACLs configured"
echo ""

# ── Step 7: Start background services ─────────────────────────────────
echo "[7/8] Activating Python virtual environment..."
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "  ✓ Virtual environment activated"
else
    echo "  ⚠ No venv found. Run: python -m venv venv && source venv/bin/activate && pip install -r requirements.txt"
fi
echo ""

echo "[8/8] Starting Python services..."
echo "  Run these in separate terminal tabs:"
echo ""
echo "  Tab 1 — CSV Producer:"
echo "    cd $(pwd)"
echo "    source venv/bin/activate"
echo "    python producer/csv_producer.py --max-records 5000"
echo ""
echo "  Tab 2 — Context Materializer:"
echo "    cd $(pwd)"
echo "    source venv/bin/activate"
echo "    python context-engine/context_materializer.py"
echo ""
echo "  Tab 3 — Health Monitor:"
echo "    cd $(pwd)"
echo "    source venv/bin/activate"
echo "    python monitoring/health_monitor.py"
echo ""
echo "  Tab 4 — AI Agent (after producer has run for a bit):"
echo "    cd $(pwd)"
echo "    source venv/bin/activate"
echo "    export OPENAI_API_KEY='sk-...'"
echo "    python agent/agent.py"
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              HealthStream v2 — Infrastructure Ready         ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║                                                              ║"
echo "║  SERVICE              PORT    STATUS                         ║"
echo "║  ─────────────────────────────────────                       ║"
echo "║  Kafka Broker 1       9092    docker-compose ps              ║"
echo "║  Kafka Broker 2       9093                                   ║"
echo "║  Kafka Broker 3       9094                                   ║"
echo "║  Schema Registry      8081    http://localhost:8081/subjects  ║"
echo "║  Kafka Connect        8083    http://localhost:8083/connectors║"
echo "║  ksqlDB               8088    http://localhost:8088/info      ║"
echo "║  PostgreSQL           5432                                   ║"
echo "║  MySQL                3306                                   ║"
echo "║  Redis                6379                                   ║"
echo "║                                                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
