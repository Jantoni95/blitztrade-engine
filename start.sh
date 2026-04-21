#!/bin/bash
# Momentum Screener — Start Script (Mac/Linux)
# Starts the TWS API bridge + opens screener in browser
# Prerequisites: TWS or IB Gateway must be running and logged in

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVE_PORT=8888
TWS_PORT=""  # empty = auto-detect (tries 7497, 7496)
COGNITO_POOL=""
COGNITO_CLIENT=""

# Parse optional args
while [[ $# -gt 0 ]]; do
  case $1 in
    --tws-port) TWS_PORT="$2"; shift 2;;
    --port) SERVE_PORT="$2"; shift 2;;
    --live) TWS_PORT=7496; shift;;
    --cognito-pool) COGNITO_POOL="$2"; shift 2;;
    --cognito-client) COGNITO_CLIENT="$2"; shift 2;;
    *) shift;;
  esac
done

# ── Check Python dependencies ────────────────────────────────
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
  pip3 install -q -r "$SCRIPT_DIR/requirements.txt"
else
  python3 -c "import ib_insync, aiohttp, yfinance" 2>/dev/null
  if [ $? -ne 0 ]; then
    echo "📦 Installing Python dependencies..."
    pip3 install ib_insync aiohttp yfinance
  fi
fi

# ── Kill old processes on our port ───────────────────────────
lsof -ti:$SERVE_PORT | xargs kill 2>/dev/null
sleep 1

# ── Check if TWS/Gateway is reachable ────────────────────────
if [ -n "$TWS_PORT" ]; then
  echo "Checking for TWS/IB Gateway on port $TWS_PORT..."
  if ! nc -z 127.0.0.1 $TWS_PORT 2>/dev/null; then
    echo ""
    echo "  TWS/Gateway not found on port $TWS_PORT — starting anyway."
    echo "  The bridge will keep retrying until TWS is connected."
    echo ""
  fi
else
  echo "TWS port: auto-detect (will try 7497, 7496)"
fi

# ── Launch native window ─────────────────────────────────────
echo "⚡ Starting BlitzTrade..."
cd "$SCRIPT_DIR"

if [ -n "$TWS_PORT" ]; then
  export TWS_PORT=$TWS_PORT
fi
export SERVE_PORT=$SERVE_PORT
if [ -n "$COGNITO_POOL" ]; then
  export COGNITO_USER_POOL_ID="$COGNITO_POOL"
  export COGNITO_CLIENT_ID="$COGNITO_CLIENT"
fi

# Use launcher.py (native pywebview window) if pywebview is installed,
# otherwise fall back to serve.py + browser
if python3 -c "import webview" 2>/dev/null; then
  LOG_FILE="$SCRIPT_DIR/serve.log"
  python3 "$SCRIPT_DIR/launcher.py" > "$LOG_FILE" 2>&1
else
  echo "  pywebview not found — falling back to browser mode"
  echo "  Install it:  pip3 install pywebview"
  echo ""

  LOG_FILE="$SCRIPT_DIR/serve.log"
  nohup bash watchdog.sh --port $SERVE_PORT ${TWS_PORT:+--tws-port $TWS_PORT} > "$LOG_FILE" 2>&1 &
  SERVE_PID=$!
  disown $SERVE_PID
  sleep 2

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "  Screener: http://localhost:$SERVE_PORT"
  echo "  TWS API:  ${TWS_PORT:-auto-detect}"
  echo "  Log:      $LOG_FILE"
  echo "  PID:      $SERVE_PID"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo ""
  echo "  To stop:  kill $SERVE_PID"
  echo ""

  if [[ "$OSTYPE" == "darwin"* ]]; then
    open "http://localhost:$SERVE_PORT"
  else
    xdg-open "http://localhost:$SERVE_PORT" 2>/dev/null
  fi
fi
