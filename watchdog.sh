#!/bin/bash
# Lightweight watchdog for serve.py — auto-restarts on crash
# Usage: ./watchdog.sh [same args as start.sh]

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVE_PORT=8888
TWS_PORT=7497  # 7496=live, 7497=paper (default)
MAX_RESTARTS=20
RESTART_DELAY=3
COOLDOWN=60        # reset restart counter after this many seconds of uptime

while [[ $# -gt 0 ]]; do
  case $1 in
    --tws-port) TWS_PORT="$2"; shift 2;;
    --port) SERVE_PORT="$2"; shift 2;;
    --live) TWS_PORT=7496; shift;;
    *) shift;;
  esac
done

restarts=0

cleanup() {
  echo ""
  echo "⏹  Watchdog stopped"
  kill "$PID" 2>/dev/null
  exit 0
}
trap cleanup SIGINT SIGTERM

while true; do
  if [ "$restarts" -ge "$MAX_RESTARTS" ]; then
    echo "🛑 Max restarts ($MAX_RESTARTS) reached — giving up"
    exit 1
  fi

  echo "🚀 Starting serve.py (attempt $((restarts + 1)))..."
  START_TIME=$(date +%s)

  cd "$SCRIPT_DIR"
  python3 serve.py "$SERVE_PORT" --tws-port "$TWS_PORT" &
  PID=$!
  wait "$PID"
  EXIT_CODE=$?
  END_TIME=$(date +%s)
  UPTIME=$((END_TIME - START_TIME))

  # Exit cleanly on SIGINT/SIGTERM (user stopped it)
  if [ "$EXIT_CODE" -eq 0 ] || [ "$EXIT_CODE" -eq 130 ] || [ "$EXIT_CODE" -eq 143 ]; then
    echo "✅ serve.py exited cleanly (code $EXIT_CODE)"
    break
  fi

  # Reset counter if it ran long enough
  if [ "$UPTIME" -ge "$COOLDOWN" ]; then
    restarts=0
  fi

  restarts=$((restarts + 1))
  echo "💥 serve.py crashed (exit code $EXIT_CODE, uptime ${UPTIME}s) — restarting in ${RESTART_DELAY}s... [$restarts/$MAX_RESTARTS]"
  sleep "$RESTART_DELAY"
done
