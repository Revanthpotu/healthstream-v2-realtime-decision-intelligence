#!/bin/bash
# ════════════════════════════════════════════════════════════════════
# HealthStream v2 — Stop Demo
# ════════════════════════════════════════════════════════════════════

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

echo ""
echo -e "${YELLOW}Stopping HealthStream v2...${NC}"
echo ""

# ── Kill all Python services ───────────────────────────────────────────
pkill -f "csv_producer.py" 2>/dev/null && echo "  ✓ Producer stopped" || echo "  · Producer not running"
pkill -f "context_materializer.py" 2>/dev/null && echo "  ✓ Materializer stopped" || echo "  · Materializer not running"
pkill -f "health_monitor.py" 2>/dev/null && echo "  ✓ Monitor stopped" || echo "  · Monitor not running"
pkill -f "dashboard/api.py" 2>/dev/null && echo "  ✓ Dashboard stopped" || echo "  · Dashboard not running"

# ── Kill anything still on port 5050 ──────────────────────────────────
lsof -ti :5050 2>/dev/null | xargs kill -9 2>/dev/null

sleep 1

# ── Stop Docker ────────────────────────────────────────────────────────
echo ""
cd "$(dirname "$0")"
docker-compose down --remove-orphans 2>/dev/null
echo -e "  ${GREEN}✓ Docker containers stopped${NC}"

echo ""
echo -e "${GREEN}All services stopped.${NC} Run ${YELLOW}./start_demo.sh${NC} to restart."
echo ""
