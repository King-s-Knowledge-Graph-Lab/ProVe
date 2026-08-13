#!/bin/bash
# @repo: dashboard
# @description: Regenerates info.json from prove-api usage stats and publishes
# the dashboard to Plotly Cloud. Designed to be run hourly via cron — see
# prove-dashboard/scripts/README.md for the crontab line and PLOTLY_API_KEY setup.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DASHBOARD_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
API_DIR="$(cd "$DASHBOARD_DIR/../prove-api" && pwd)"

log() {
    printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$1"
}

# Crontab runs with a bare environment. If a static PLOTLY_API_KEY is set
# (Pro plans only), it's picked up straight from the environment or a
# gitignored .env file next to this script's dashboard dir.
if [ -f "$DASHBOARD_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DASHBOARD_DIR/.env"
    set +a
fi

# Free plan: no static API key available. Auth instead relies on a cached
# OAuth refresh token from a one-time interactive `plotly user login`
# (see README.md). The CLI auto-refreshes it on every publish, so once that
# login has happened on this machine, cron never needs a browser again.
if [ -z "${PLOTLY_API_KEY:-}" ] && [ ! -f "$HOME/.plotly-cloud" ]; then
    log "ERROR: not authenticated with Plotly Cloud. Run 'plotly user login --no-browser' once on this machine (see README.md), or set PLOTLY_API_KEY if you're on a Pro plan."
    exit 1
fi

# Load prove-api's own .env for any overrides it defines (same pattern as
# the dashboard .env above).
if [ -f "$API_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$API_DIR/.env"
    set +a
fi

# info.py runs as a bare host process here — never inside the Docker
# container prove-api's Flask app normally runs in production — so
# config.yaml's "mongodb://host.docker.internal:27017/" won't resolve.
# Default to localhost; prove-api/.env can override MONGO_CONNECTION_STRING
# if this host's Mongo lives somewhere else.
export MONGO_CONNECTION_STRING="${MONGO_CONNECTION_STRING:-mongodb://localhost:27017/}"

log "Generating info.json from MongoDB usage records..."
(cd "$DASHBOARD_DIR" && "$DASHBOARD_DIR/.venv/bin/plotly" app publish --poll-timeout 300)
(cd "$API_DIR" && "MONGO_CONNECTION_STRING=mongodb://localhost:27017/ $API_DIR/.venv/bin/python" info.py)

log "Copying info.json into dashboard..."
cp "$API_DIR/info.json" "$DASHBOARD_DIR/info.json"

log "Publishing dashboard to Plotly Cloud..."
(cd "$DASHBOARD_DIR" && "$DASHBOARD_DIR/.venv/bin/plotly" app publish --poll-timeout 300)

log "Done."
