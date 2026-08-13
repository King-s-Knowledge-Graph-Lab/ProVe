# Dashboard automation

`update_dashboard.sh` regenerates the usage stats and republishes the
dashboard to Plotly Cloud in one shot:

1. Runs `prove-api/info.py` (via prove-api's own venv, so it has `prove_shared`
   / MongoDB access) to produce a fresh `prove-api/info.json`.
2. Copies that file over `prove-dashboard/info.json`, which `dashboard.py`
   reads at startup.
3. Runs `plotly app publish` (via prove-dashboard's venv) against the
   existing app defined in `plotly-cloud.toml` — no browser, no manual
   upload.

## One-time setup

The Plotly Cloud CLI needs credentials cached on the machine that will run
cron before it can publish unattended.

### Free plan (default)

Static API keys (`PLOTLY_API_KEY`) are a Pro-plan feature, so free-plan
publishing has to go through the normal device-code login instead — but
only **once**. The CLI caches a refresh token to `~/.plotly-cloud` and
auto-refreshes it on every subsequent `plotly app publish` call, so cron
never has to open a browser after this step.

On the server (or wherever cron will run), as the same user cron will run
as:

```
./prove-dashboard/.venv/bin/plotly user login --no-browser
```

`--no-browser` prints a verification URL + device code instead of trying to
launch a browser on the headless box — open that URL on your phone/laptop
and approve it there. Once it says "Successfully logged in", `~/.plotly-cloud`
holds the cached credentials the cron job will reuse.

### Pro plan (optional, if you upgrade)

If you're on a Pro plan or higher, you can skip the login step above and use
a static API key instead: generate one from your Plotly Cloud account
settings, then put it in a gitignored `.env` file at `prove-dashboard/.env`:

```
PLOTLY_API_KEY=your-key-here
```

`update_dashboard.sh` prefers `PLOTLY_API_KEY` when present and falls back
to the cached OAuth login otherwise.

## Registering the hourly cron job

```
0 * * * * /path/to/ProVe/prove-dashboard/scripts/update_dashboard.sh >> /var/log/prove-dashboard.log 2>&1
```

Run `crontab -e` and add that line (adjust the repo path). Check
`/var/log/prove-dashboard.log` afterwards to confirm the first few runs
succeed — the script exits non-zero (and logs why) if `info.py` fails,
`PLOTLY_API_KEY` is missing, or the publish step fails.

## Manual run

```
./update_dashboard.sh
```
