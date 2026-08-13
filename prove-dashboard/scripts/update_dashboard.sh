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

# Crontab runs with a bare environment, so PLOTLY_API_KEY normally won't be
# set unless it's exported by the crontab entry itself. Fall back to a
# gitignored .env file next to this script's dashboard dir if present.
if [ -f "$DASHBOARD_DIR/.env" ]; then
    set -a
    # shellcheck disable=SC1090
    source "$DASHBOARD_DIR/.env"
    set +a
fi

if [ -z "${PLOTLY_API_KEY:-}" ]; then
    log "ERROR: PLOTLY_API_KEY is not set (env var or $DASHBOARD_DIR/.env). Cannot publish non-interactively."
    exit 1
fi

log "Generating info.json from MongoDB usage records..."
(cd "$API_DIR" && "$API_DIR/.venv/bin/python" info.py)

log "Copying info.json into dashboard..."
cp "$API_DIR/info.json" "$DASHBOARD_DIR/info.json"

log "Publishing dashboard to Plotly Cloud..."
(cd "$DASHBOARD_DIR" && "$DASHBOARD_DIR/.venv/bin/plotly" app publish --poll-timeout 300)

log "Done."
