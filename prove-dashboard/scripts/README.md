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

The publish step authenticates with a static API key instead of the
interactive OAuth login, so it can run unattended from cron.

1. Generate an API key from your Plotly Cloud account settings.
2. Put it in a gitignored `.env` file at `prove-dashboard/.env`:

   ```
   PLOTLY_API_KEY=your-key-here
   ```

   (Cron runs with a near-empty environment, so exporting it only in your
   interactive shell won't reach the script — the `.env` file is what makes
   it available.)

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
