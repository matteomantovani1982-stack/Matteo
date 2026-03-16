#!/bin/bash
set -e

CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND="$SCRIPT_DIR/careeros"
FRONTEND="$SCRIPT_DIR/cvlab-frontend"
COMPOSE="docker compose -f $BACKEND/docker/docker-compose.yml"

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  CVLab — Local Setup${NC}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# ── 1. Check Docker ───────────────────────────────────────
echo -e "${YELLOW}[1/5] Checking Docker...${NC}"
if ! docker info > /dev/null 2>&1; then
  echo -e "${RED}✗ Docker is not running. Open Docker Desktop and try again.${NC}"
  exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

# ── 2. Check folders ──────────────────────────────────────
if [ ! -d "$BACKEND" ]; then
  echo -e "${RED}✗ Cannot find 'careeros' folder.${NC}"
  exit 1
fi

# ── 3. Set up .env ────────────────────────────────────────
echo -e "${YELLOW}[2/5] Checking environment...${NC}"

if [ ! -f "$BACKEND/.env" ]; then
  cp "$BACKEND/.env.example" "$BACKEND/.env"
fi

if grep -q "PASTE_YOUR_ANTHROPIC_KEY_HERE" "$BACKEND/.env"; then
  echo ""
  echo -e "${RED}  ACTION REQUIRED — Add your API keys to the .env file${NC}"
  echo ""
  echo "  Opening the file now..."
  echo "  Replace these lines with your real keys:"
  echo "    ANTHROPIC_API_KEY=PASTE_YOUR_ANTHROPIC_KEY_HERE"
  echo "    SERPER_API_KEY=PASTE_YOUR_SERPER_KEY_HERE"
  echo ""
  echo "  Save, then run this script again."
  open -e "$BACKEND/.env" 2>/dev/null || open "$BACKEND/.env"
  exit 1
fi
echo -e "${GREEN}✓ API keys found${NC}"

# ── 4. Clean slate — stop old containers + wipe volumes ───
echo -e "${YELLOW}[3/5] Resetting Docker state (ensures clean DB)...${NC}"
$COMPOSE down -v --remove-orphans 2>/dev/null || true
echo -e "${GREEN}✓ Clean slate ready${NC}"

# ── 5. Build + start ──────────────────────────────────────
echo ""
echo -e "${YELLOW}[4/5] Building and starting backend...${NC}"
echo "  First time: ~5 min. Subsequent runs: ~20 sec."
echo ""
cd "$BACKEND"
$COMPOSE up --build -d

# ── 6. Wait for API ───────────────────────────────────────
echo ""
echo -e "${YELLOW}  Waiting for API to be ready...${NC}"
MAX_WAIT=180
ELAPSED=0
printf "  "
while [ $ELAPSED -lt $MAX_WAIT ]; do
  if curl -sf http://localhost:8000/health > /dev/null 2>&1; then
    echo ""
    echo -e "${GREEN}✓ Backend is up${NC}"
    break
  fi
  printf "."
  sleep 3
  ELAPSED=$((ELAPSED + 3))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
  echo ""
  echo -e "${RED}✗ Backend didn't start in time. Last logs:${NC}"
  $COMPOSE logs --tail=40 api
  exit 1
fi

# ── 7. Frontend ───────────────────────────────────────────
echo ""
echo -e "${YELLOW}[5/5] Starting frontend...${NC}"
cd "$FRONTEND"

if ! command -v node > /dev/null 2>&1; then
  echo -e "${RED}✗ Node.js not found.${NC}"
  echo "  Install from: https://nodejs.org  (click LTS)"
  echo "  Then run this script again."
  exit 1
fi

if [ ! -d "node_modules" ]; then
  echo "  Installing packages (~1 min)..."
  npm install
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  ✓ CVLab is running!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  👉  Open in browser → ${CYAN}http://localhost:3000${NC}"
echo ""
echo "  API docs:     http://localhost:8000/docs"
echo "  Task monitor: http://localhost:5555  (admin / careeros)"
echo ""
echo -e "${YELLOW}  Ctrl+C to stop${NC}"
echo ""

npm run dev
