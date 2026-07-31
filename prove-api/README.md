# prove-api

Flask REST API for ProVe — result retrieval, queue management, and the internal
endpoints `prove-processing` calls (`getKey`, `getNextQueue`). Depends on
`prove-shared` as a local path dependency.

## Docker

**Build context must be the repo root**, not this directory — the Dockerfile copies
in the sibling `prove-shared` package, which `pyproject.toml` depends on via a
`[tool.uv.sources]` path source.

```bash
docker build -f prove-api/Dockerfile -t prove-api .

docker run -d --name prove-api \
  --env-file prove-api/.env \
  -p 127.0.0.1:8000:8000 \
  -v "C:\path\to\reference_checked.db:/data/reference_checked.db" \
  prove-api
```

Serves via `gunicorn` on port 8000. Binding to `127.0.0.1:8000` (rather than
`0.0.0.0:8000`) keeps the container reachable only from the host machine — point
Apache's `ProxyPass` at `http://127.0.0.1:8000/` rather than exposing the container
to the network directly.

### Environment (`.env`)

Runtime config comes from environment variables, not a committed secrets file — see
`settings.py`. Copy `.env.example` to `.env` and fill in real values; `.env` is
gitignored and must never be committed. `--env-file prove-api/.env` above loads it
into the container.

### SQLite result DB

`config.yaml`'s `database.result_db_for_API` is a fixed in-container path
(`/data/reference_checked.db`) — only the host-side source of the bind mount changes
per machine, same pattern as the model volume in `prove-processing`.

### Mongo connectivity

`config.yaml`'s `database.mongo.connection_string` points at `host.docker.internal`
so the container can reach MongoDB running on the host machine. If Mongo runs
somewhere else (its own container, a remote host), update that value accordingly.

### Talking to `prove-processing`

`prove-processing`'s `main_service` calls this API's `getKey`/`getNextQueue`
endpoints. If it's running in its own container, point it at
`http://host.docker.internal:8000/api/internal/` via `PROVE_ENDPOINT`, with a
`PROVE_API_KEY` matching whatever this container is using (see
`prove-processing/README.md`).
