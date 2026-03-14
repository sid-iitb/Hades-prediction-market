#!/usr/bin/env bash
# Grep console/log file for last_90s RUN_ERROR and oracle_ws Fatal WS error.
# Usage: ./tools/grep_last_90s_errors.sh [path_to_log_file]
# Default: last_90s_verify.log in repo root

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="${1:-$REPO_ROOT/last_90s_verify.log}"

if [[ ! -f "$LOG" ]]; then
  echo "Log file not found: $LOG"
  echo "Usage: $0 [path_to_log_file]"
  echo "Run the bot with: python3 -m bot.main --config config/config.yaml 2>&1 | tee last_90s_verify.log"
  exit 1
fi

echo "=== Checking: $LOG ==="
echo ""
echo "--- [last_90s] RUN_ERROR (Kalshi REST / parallel batch) ---"
grep -n "\[last_90s\] RUN_ERROR" "$LOG" || true
echo ""
echo "--- [oracle_ws] Fatal WS error (Kraken/Coinbase WebSocket) ---"
grep -n "\[oracle_ws\] Fatal WS error" "$LOG" || true
echo ""
echo "Done. If nothing printed above, no matching errors in this log."
