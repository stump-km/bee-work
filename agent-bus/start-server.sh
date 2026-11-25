#!/bin/bash
# Start the agent-bus MCP server (bus core).
#
# The bearer token is read at runtime from bus-token.txt in this
# directory. That file is deployment state — create it locally with a
# long random value and NEVER commit it (see .gitignore).
set -euo pipefail
cd "$(dirname "$0")"
export MCP_BUS_TOKEN="$(tr -d '[:space:]' < ./bus-token.txt)"
exec ./agent-bus --http :8484 --db ./bus.db --lease-sec 900
