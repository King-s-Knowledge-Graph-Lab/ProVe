# prove-processing

Background worker services for ProVe — the NLP verification pipeline (claim parsing,
HTML fetching, evidence selection, textual entailment) plus a lightweight random-QID
generator. Depends on `prove-shared` as a local path dependency.

## Services

- **`ProVe_main_service.py`** — consumes queue items via the API, runs the full
  pipeline (loads the BERT/T5 models), writes results to MongoDB.
- **`ProVe_heuristic_service.py`** — generates random QIDs and enqueues them. Runs
  with `model=False`, so it never loads the ML models.

## Docker

Each service has its own Dockerfile. **Build context must be the repo root**, not
this directory — both Dockerfiles copy in the sibling `prove-shared` package,
which `pyproject.toml` depends on via a `[tool.uv.sources]` path source.

### main_service

```bash
docker build -f prove-processing/Dockerfile.main_service -t prove-processing-main .

docker run -d --name prove-processing-main \
  --gpus all \
  -v "C:\path\to\models:/home/ubuntu/RQV/base" \
  prove-processing-main
```

The model volume mount is required — `utils/textual_entailment_module.py`,
`utils/sentence_retrieval_module.py`, and `utils/verbalisation_module.py` all hardcode
model paths under `/home/ubuntu/RQV/base/...`. That path is fixed inside the
container; only the host-side source of the bind mount changes per machine. Get the
model folder from the SharePoint link in the root README if you don't have it locally.

To reach a `prove-api` instance for `getKey`/`getNextQueue` (instead of the default
`http://localhost/api/internal/`, which inside a container means "this container"),
set `PROVE_ENDPOINT` and `PROVE_API_KEY` — the latter must match the value `prove-api`
is using, since it's the shared signing key for `AsyncAuth`:

```bash
docker run -d --name prove-processing-main \
  --gpus all \
  -v "C:\path\to\models:/home/ubuntu/RQV/base" \
  -e PROVE_ENDPOINT="http://host.docker.internal:8000/api/internal/" \
  -e PROVE_API_KEY="<same value prove-api uses>" \
  prove-processing-main
```

#### GPU

`--gpus all` is required for the BERT/T5 models to run on GPU instead of CPU — the
Dockerfile itself needs no changes, since the standard `torch` wheel already bundles
CUDA support (confirmed: `torch.cuda.is_available()` is `True` inside the container
with `--gpus all`, `False` without it). Requirements on the host: an NVIDIA GPU, a
recent driver, and Docker Desktop's `nvidia` container runtime (`docker info` should
list `nvidia` under `Runtimes:`). Sanity-check GPU visibility inside the image
without running the full service:

```bash
docker run --rm --gpus all --entrypoint python prove-processing-main \
  -c "import torch; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"
```

`heuristic_service` never loads models (`model=False`), so `--gpus all` has no effect
there — omit it.

### heuristic_service

No model volume needed — this one never loads models.

```bash
docker build -f prove-processing/Dockerfile.heuristic_service -t prove-processing-heuristic .

docker run -d --name prove-processing-heuristic prove-processing-heuristic
```

Note: `HeuristicBasedService.run()`'s loop has no rate limiting — it generates and
enqueues QIDs continuously with no sleep between iterations. Expect a running
container to write to the `random_queue` Mongo collection quickly and repeatedly.

### Mongo connectivity

`config.yaml`'s `database.mongo.connection_string` points at `host.docker.internal`
so containers can reach MongoDB running on the host machine. If Mongo runs somewhere
else (its own container, a remote host), update that value accordingly.
