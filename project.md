# ProVe — Repository Split Plan

This document records the proposed split of the current monorepo into three separate repositories. Every source file has been annotated with `# @repo: <name>` at its top.

## Target structure

```
prove-api/          ← HTTP layer: Flask app, queue serving, result aggregation
prove-processing/   ← Background workers: pipeline, ML models, scheduled tasks
prove-shared/       ← pip-installable package: DB models, auth, logging, config
```

`prove-api` and `prove-processing` will each list `prove-shared` as a dependency in their `pyproject.toml`.

---

## `prove-shared` — shared package

> Data models, database access, authentication, logging, and config.  
> No Flask, no torch/transformers — keeps the API's dependency tree light.

| File | Description |
|---|---|
| [utils/mongo_handler.py](utils/mongo_handler.py) | MongoDB abstraction — all collections; API reads, processing writes |
| [utils/objects.py](utils/objects.py) | `Status`, `HtmlContent`, `Entailment` dataclasses (MongoDB serialisation) |
| [utils/auth.py](utils/auth.py) | RSA encryption/decryption for API key validation |
| [utils/logger.py](utils/logger.py) | Centralised `TimedRotatingFileHandler` logging config |
| [utils/queue_manager.py](utils/queue_manager.py) | HTTP client for authenticated queue calls to the API |
| [utils/wikidata_utils.py](utils/wikidata_utils.py) | `CachedWikidataAPI` — reduces repeated Wikidata entity lookups |
| [utils/file_utils.py](utils/file_utils.py) | S3/HTTP file download and local model cache (adapted from AllenNLP) |
| [config.yaml](config.yaml) | Pipeline configuration (DB paths, batch sizes, thresholds, versions) |
| `utils/local_secrets.py` | Runtime secrets — **gitignored**, must be present on each deployment host |

---

## `prove-api` — API repository

> Flask application, queue management, result aggregation, dashboard, and all static/template assets.

### Python source

| File | Description |
|---|---|
| [api/app.py](api/app.py) | Flask routes — all HTTP endpoints (`/api/items/*`, `/api/task/*`, `/api/internal/*`, `/page/*`) |
| [api/custom_decorators.py](api/custom_decorators.py) | `@log_request`, `@api_required` decorators; `StatsDBHandler` for usage tracking |
| [api/queue_manager.py](api/queue_manager.py) | Serves next queue item to processing workers; consistency checks and reset-on-failure |
| [api/utils_api.py](api/utils_api.py) | Rotating file logger and IP geolocation helper |
| [api/wsgi.py](api/wsgi.py) | WSGI entry point for production Apache/Gunicorn deployment |
| [api/db/website.py](api/db/website.py) | SQLAlchemy `NewsletterSubscriber` model |
| [api/hackathon/api_code.py](api/hackathon/api_code.py) | Standalone hackathon prototype Flask app — consider removing or archiving |
| [functions.py](functions.py) | Business logic for the API — aggregates results, summaries, history, and queue stats from MongoDB |
| [dashboard.py](dashboard.py) | Plotly/Dash usage statistics dashboard (geographic, monthly, KPIs) |
| [info.py](info.py) | Collects and aggregates API usage statistics for the dashboard |
| [test_functions.py](test_functions.py) | Test utilities for API result retrieval and status checking |

### Assets and docs

| File | Description |
|---|---|
| [api/static/style.css](api/static/style.css) | Main stylesheet |
| [api/templates/prove.html](api/templates/prove.html) | Main web interface template |
| [api/templates/hackathon.html](api/templates/hackathon.html) | Hackathon prototype template |
| [index.html](index.html) | Root-level Swagger UI page |
| [api/index.html](api/index.html) | API-level Swagger UI page |
| [swagger.json](swagger.json) | OpenAPI 3.0 spec for the full ProVe API |
| [api/docs/](api/docs/) | Per-endpoint Swagger YAML specs (items, task, worklist, page, process_reference) |
| [api/page/](api/page/) | Additional page-level Swagger YAML specs |

### DevOps

| File | Description |
|---|---|
| [scripts/restart.sh](scripts/restart.sh) | Deploys API files to Apache vhost folder and restarts services |

---

## `prove-processing` — processing repository

> Background workers, the full verification pipeline, and all ML/NLP models.  
> Heavy dependencies (`torch`, `transformers`, `pytorch_lightning`, `selenium`).

### Workers and orchestration

| File | Description |
|---|---|
| [ProVe_main_service.py](ProVe_main_service.py) | Main background worker — consumes queue via API, runs pipeline, writes results to MongoDB |
| [ProVe_heuristic_service.py](ProVe_heuristic_service.py) | Alternative worker that generates random QIDs via heuristics |
| [ProVe_main_process.py](ProVe_main_process.py) | Pipeline orchestration: parse → fetch HTML → extract evidence → check entailment |
| [background_processing.py](background_processing.py) | Scheduled tasks — fetches top-viewed Wikipedia items and pagepile lists to enqueue QIDs |

### Pipeline stages

| File | Description |
|---|---|
| [wikidata_parser.py](wikidata_parser.py) | Extracts claims, property/object labels, and reference URLs from a Wikidata QID |
| [refs_html_collection.py](refs_html_collection.py) | Fetches HTML from reference URLs (requests + Selenium fallback), batched with status tracking |
| [refs_html_to_evidences.py](refs_html_to_evidences.py) | Converts HTML into candidate evidence sentences; ranks them via `SentenceRetrievalModule` + `VerbModule` |
| [claim_entailment.py](claim_entailment.py) | Verifies entailment between claims and evidence using `TextualEntailmentModule` (BERT-FEVER) |

### ML models

| File | Description |
|---|---|
| [utils/textual_entailment_module.py](utils/textual_entailment_module.py) | BERT-FEVER fine-tuned model — classifies SUPPORTS / REFUTES / NOT ENOUGH INFO |
| [utils/sentence_retrieval_module.py](utils/sentence_retrieval_module.py) | BERT-based sentence relevance scorer — used by `EvidenceSelector` |
| [utils/sentence_retrieval_model.py](utils/sentence_retrieval_model.py) | Neural network architecture (`BertForSequenceEncoder` wrapper) |
| [utils/verbalisation_module.py](utils/verbalisation_module.py) | T5-based model that converts Wikidata graph statements into natural language |
| [utils/bert_model.py](utils/bert_model.py) | BERT encoder backbone for the sentence retrieval model |
| [utils/finetune.py](utils/finetune.py) | PyTorch Lightning `Graph2TextModule` — T5 fine-tuning; also loaded at inference time |
| [utils/callbacks.py](utils/callbacks.py) | PyTorch Lightning training callbacks (checkpointing, early stopping) |
| [utils/lightning_base.py](utils/lightning_base.py) | Lightning base classes for seq2seq models — parent of `Graph2TextModule` |
| [utils/graph2text.py](utils/graph2text.py) | Graph-to-text conversion placeholder |
| [utils/utils_graph2text.py](utils/utils_graph2text.py) | Text normalisation helpers for the graph-to-text pipeline |
| [utils/utils_verbalisation_module.py](utils/utils_verbalisation_module.py) | BLEU scoring, dataset reading, and graph linearisation helpers |

### Data files

| File | Description |
|---|---|
| [utils/pagepileList.txt](utils/pagepileList.txt) | Static list of Wikidata QIDs used for scheduled batch processing |
| [properties_to_remove.json](properties_to_remove.json) | Wikidata property IDs filtered out during claim extraction in `wikidata_parser.py` |

### DevOps

| File | Description |
|---|---|
| [scripts/dr_backup.sh](scripts/dr_backup.sh) | Daily disaster-recovery MongoDB dump to RDS |
| [scripts/historical_backup.sh](scripts/historical_backup.sh) | Weekly dated MongoDB dump to RDS |

---

## Files that stay at the root (or are split)

| File | Notes |
|---|---|
| [pyproject.toml](pyproject.toml) | Current monorepo manifest — needs to be split into three separate `pyproject.toml` files, one per repo |
| [requirements.txt](requirements.txt) | Legacy pip freeze — superseded by `pyproject.toml`; each repo should derive its own |
| [README.md](README.md) | Update to describe the three-repo structure and point to each |
| [.gitignore](.gitignore) | Copy to each repo; ensure `local_secrets.py` is always ignored |
| [LICENSE](LICENSE) | Copy to each repo |
| [front/common.js](front/common.js) | Frontend JS (result colour map and UI helpers) — belongs in `prove-api` alongside the HTML templates |

---

## Runtime coupling (not an import — intentional)

`ProVe_main_service` (processing) calls `GET /api/internal/getNextQueue` over HTTP at runtime to fetch the next task. This is the only cross-repo runtime dependency and is by design: the API owns the queue state.

```
prove-processing  --HTTP GET /api/internal/getNextQueue-->  prove-api
prove-processing  --writes results-->  MongoDB  <--reads--  prove-api
```

---

## Suggested `pyproject.toml` dependency split

**`prove-shared`**
```
pymongo, pandas, pyyaml, cryptography, requests, boto3
```

**`prove-api`**
```
prove-shared, flask, flask-cors, flasgger, flask-sqlalchemy,
dash, dash-bootstrap-components, plotly, schedule
```

**`prove-processing`**
```
prove-shared, torch, transformers==4.46.3, pytorch_lightning==2.4.0,
qwikidata, nltk, bs4, selenium, html2text, rouge_score, sacrebleu,
sparqlwrapper, schedule
```
