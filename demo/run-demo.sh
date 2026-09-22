#!/usr/bin/env bash
# One-shot local demo: reset the local data dir, ingest every synthetic ticket,
# process them, and show where each one went. No network, no AWS.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${ICR_DATA_DIR:-.icr}"
ICR="./bin/icr"
[ -x "$ICR" ] || go build -o bin/icr ./cmd/cli

echo "== Rivergate ICR demo (mock classifier, synthetic tickets) =="
"$ICR" -data "$DATA" reset -yes >/dev/null 2>&1 || true

echo
echo "-- 1. Ingest 10 tickets (email, chat, web form) and process them"
"$ICR" -data "$DATA" ingest -process demo/tickets

echo
echo "-- 2. Where did everything go?"
"$ICR" -data "$DATA" status

echo
echo "-- 3. Automatic actions (stubbed Slack / tickets / KB), each with the reason it fired"
"$ICR" -data "$DATA" outbox

echo
echo "-- 4. The human review queue"
"$ICR" -data "$DATA" status -review

cat <<'EOF'

Next:
  make dashboard                      # review queue UI at http://127.0.0.1:8080
  ./bin/icr status <ID>               # one ticket's full audit trail
  ./bin/icr review approve <ID> -reviewer you -note "looks right"
  ./bin/icr mcp                       # the same pipeline as MCP tools (read-only)
EOF
