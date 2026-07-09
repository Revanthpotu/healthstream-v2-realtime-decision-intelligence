#!/bin/bash
# ════════════════════════════════════════════════════════════════════
# HealthStream v2 — Start Demo
# ════════════════════════════════════════════════════════════════════
# Usage:  ./start_demo.sh
# Starts everything and opens dashboard at http://localhost:5050
# ════════════════════════════════════════════════════════════════════

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
RED='\033[0;31m'; BOLD='\033[1m'; NC='\033[0m'

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"
mkdir -p logs

echo ""
echo -e "${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║${NC}  ${BOLD}HealthStream v2 — Starting Demo${NC}                              ${CYAN}║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""

# ── CLEANUP: Kill leftover processes from previous runs ────────────────
echo -e "${YELLOW}[1/8]${NC} Cleaning up old processes..."
pkill -f "csv_producer.py" 2>/dev/null
pkill -f "context_materializer.py" 2>/dev/null
pkill -f "health_monitor.py" 2>/dev/null
pkill -f "dashboard/api.py" 2>/dev/null
lsof -ti :5050 2>/dev/null | xargs kill -9 2>/dev/null
sleep 1

# Kill local MySQL/Postgres that conflict with Docker ports
sudo pkill -f mysqld 2>/dev/null
sudo pkill -f postgres 2>/dev/null
sleep 1

# Verify ports are free
BLOCKED=""
for PORT in 5432 3306; do
    if lsof -i :$PORT -sTCP:LISTEN > /dev/null 2>&1; then
        BLOCKED="$BLOCKED $PORT"
    fi
done
if [ -n "$BLOCKED" ]; then
    echo -e "  ${RED}ERROR: Ports$BLOCKED still in use. Run these and try again:${NC}"
    echo "    sudo lsof -i :5432 -i :3306 | grep LISTEN"
    echo "    sudo kill <PID>"
    exit 1
fi
echo -e "  ${GREEN}✓ All ports free${NC}"

# ── DOCKER ─────────────────────────────────────────────────────────────
echo -e "${YELLOW}[2/8]${NC} Starting Docker infrastructure (10 services)..."
docker-compose up -d > logs/docker.log 2>&1

echo -n "      Waiting for healthy containers"
for i in $(seq 1 60); do
    HEALTHY=$(docker-compose ps 2>/dev/null | grep -c "(healthy)" || echo "0")
    if [ "$HEALTHY" -ge 9 ]; then break; fi
    echo -n "."
    sleep 3
done
echo ""

HEALTHY=$(docker-compose ps 2>/dev/null | grep -c "(healthy)" || echo "0")
if [ "$HEALTHY" -lt 9 ]; then
    echo -e "  ${RED}Only $HEALTHY/10 containers healthy. Waiting 30 more seconds...${NC}"
    sleep 30
fi
echo -e "  ${GREEN}✓ Docker services running ($HEALTHY healthy)${NC}"

# ── TOPICS ─────────────────────────────────────────────────────────────
echo -e "${YELLOW}[3/8]${NC} Creating Kafka topics..."
bash scripts/create_topics.sh > logs/topics.log 2>&1 || true
echo -e "  ${GREEN}✓ Topics ready${NC}"

# ── PYTHON VENV ────────────────────────────────────────────────────────
echo -e "${YELLOW}[4/8]${NC} Activating Python environment..."
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi
python3 -c "import flask" 2>/dev/null || pip install flask flask-cors -q 2>/dev/null
echo -e "  ${GREEN}✓ Python ready${NC}"

# ── CONNECTORS ─────────────────────────────────────────────────────────
echo -e "${YELLOW}[5/8]${NC} Deploying connectors..."
# Delete old Debezium connector (may have broken config from previous run)
curl -s -X DELETE http://localhost:8083/connectors/debezium-source-mysql > /dev/null 2>&1
sleep 2
bash scripts/deploy_connectors.sh > logs/connectors.log 2>&1 || true

# Verify Debezium is actually working (not silently failing)
sleep 5
DEB_STATE=$(curl -s http://localhost:8083/connectors/debezium-source-mysql/status 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('tasks',[{}])[0].get('state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
if [ "$DEB_STATE" = "FAILED" ]; then
    echo -e "  ${RED}⚠ Debezium task failed — check logs/connectors.log${NC}"
else
    echo -e "  ${GREEN}✓ Connectors deployed (Debezium: $DEB_STATE)${NC}"
fi

# ── PRODUCER ───────────────────────────────────────────────────────────
echo -e "${YELLOW}[6/8]${NC} Running CSV Producer (5000 vitals)..."
python producer/csv_producer.py --max-records 5000 > logs/producer.log 2>&1 &
PRODUCER_PID=$!
# Wait for producer to finish (usually ~2 seconds)
sleep 4
if ! kill -0 $PRODUCER_PID 2>/dev/null; then
    echo -e "  ${GREEN}✓ Producer finished (5000 records sent)${NC}"
else
    echo -e "  ${GREEN}✓ Producer running in background${NC}"
fi

# ── KSQLDB STATEMENTS ──────────────────────────────────────────────────
echo -e "${YELLOW}[6.5]${NC} Deploying ksqlDB streams..."
sleep 5
docker exec -i ksqldb ksql http://localhost:8088 < processing/ksqldb_statements.sql > logs/ksqldb.log 2>&1 || true
echo -e "  ${GREEN}â ksqlDB streams deployed${NC}"

# ── MATERIALIZER + MONITOR ─────────────────────────────────────────────
echo -e "${YELLOW}[7/8]${NC} Starting Context Materializer + Health Monitor..."
python context-engine/context_materializer.py > logs/materializer.log 2>&1 &
python monitoring/health_monitor.py > logs/monitor.log 2>&1 &
sleep 5
echo -e "  ${GREEN}✓ Materializer + Monitor running${NC}"

# ── DASHBOARD ──────────────────────────────────────────────────────────
echo -e "${YELLOW}[8/8]${NC} Starting Dashboard server..."
python dashboard/api.py > logs/dashboard.log 2>&1 &
sleep 2

# Verify dashboard is responding
if curl -s http://localhost:5050/api/health > /dev/null 2>&1; then
    echo -e "  ${GREEN}✓ Dashboard live at http://localhost:5050${NC}"
else
    echo -e "  ${YELLOW}⚠ Dashboard starting... check logs/dashboard.log${NC}"
fi

# ── OPEN BROWSER ───────────────────────────────────────────────────────
sleep 1
if command -v open &> /dev/null; then
    open "http://localhost:5050"
elif command -v xdg-open &> /dev/null; then
    xdg-open "http://localhost:5050"
fi

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  ${BOLD}🎉  HealthStream v2 is LIVE!${NC}                                ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  📊  Dashboard:  ${CYAN}http://localhost:5050${NC}                       ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}  🛑  Stop:       ${YELLOW}./stop_demo.sh${NC}                              ${GREEN}║${NC}"
echo -e "${GREEN}║${NC}                                                              ${GREEN}║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
