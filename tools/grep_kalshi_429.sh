#!/usr/bin/env bash
# Search bot logs for Kalshi HTTP 429 (Too Many Requests) and related API errors.
# Usage: ./tools/grep_kalshi_429.sh [log_file]
# Default: logs/bot.log (relative to repo root)

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="${1:-$REPO_ROOT/logs/bot.log}"

if [[ ! -f "$LOG" ]]; then
  echo "Log file not found: $LOG"
  exit 1
fi

echo "=== 429 Too Many Requests (Kalshi API) in $LOG ==="
grep -n "429 Client Error\|Too Many Requests" "$LOG" || echo "(none)"
echo ""
echo "=== Other Kalshi API errors (401, 502, 504) - last 30 lines ==="
grep -n "Client Error\|Server Error\|Gateway Time-out\|Unauthorized" "$LOG" | tail -30 || echo "(none)"
