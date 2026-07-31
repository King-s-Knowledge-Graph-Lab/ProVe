# ProVe (Provenance Verification for Wikidata claims)


## Overview

ProVe is a system designed to automatically verify claims and references in Wikidata. It extracts claims from Wikidata entities, fetches the referenced URLs, processes the HTML content, and uses NLP models to determine whether the claims are supported by the referenced content.

It:
1. extracts claims and references from a Wikidata item,
2. fetches reference content from external URLs,
3. selects evidence sentences,
4. runs textual entailment,
5. stores and serves results through API and background services.

## Current Repository Structure

The codebase is now organized into three top-level folders inside this workspace:

- prove-api: HTTP/API layer, dashboard, templates, docs, queue endpoint
- prove-processing: background workers, pipeline orchestration, ML/NLP models
- prove-shared: pip-installable shared package (`DataStore` DB abstraction with a MongoDB backend, models, auth, utilities)

Root-level files still include global project metadata such as pyproject.toml, README.md, LICENSE, and project planning docs.

## Architecture Summary

### 1) Data Collection and Processing

- WikidataParser extracts claims and reference URLs from QIDs.
- HTMLFetcher downloads referenced pages (requests/selenium fallback).
- HTMLSentenceProcessor turns HTML into candidate evidence sentences.

### 2) Evidence Selection and Verification

- EvidenceSelector ranks candidate evidence against claims.
- ClaimEntailmentChecker classifies SUPPORTS / REFUTES / NOT ENOUGH INFO.

### 3) NLP Models

- TextualEntailmentModule (BERT-FEVER style entailment)
- SentenceRetrievalModule (sentence relevance scoring)
- VerbModule (graph statement verbalization)

### 4) Storage

- Access goes through the `DataStore` abstraction (`prove_shared.database.interface.DataStore`), implemented today by `MongoDBHandler` (`prove_shared.database.mongo`), with a PostgreSQL backend in progress
- MongoDB (current backend): html content, entailment outputs, parser stats, queue/status
- SQLite: historical/aggregated data used by API logic in legacy paths

## Shared Package (prove-shared)

The shared package is installable and used by API and processing code.

### Local install

From root:

```bash
uv sync
# or
pip install .
```

Root pyproject.toml includes a local path dependency to install prove_shared from prove-shared.

### Direct shared install

```bash
cd prove-shared
pip install -e .
```

### Import style

```python
from prove_shared import MongoDBHandler, AsyncAuth, Status
from prove_shared.database.mongo import requestItemProcessing
```

### Package layout

```text
prove-shared/
  pyproject.toml
  config.yaml
  src/
    prove_shared/
      __init__.py
      auth.py
      file_utils.py
      logger.py
      objects.py
      queue_manager.py
      wikidata_utils.py
      database/
        __init__.py
        interface.py     # DataStore (ABC) — the contract
        mongo.py         # MongoDBHandler implementation
        postgres.py      # PostgreSQLHandler stub
        orchestrator.py  # get_database() + DatabaseOrchestrator
```

## Setup Instructions

## 1) Python environment

Use Python 3.10.16 as declared in project metadata.

## 2) Install dependencies

Install from root:

```bash
pip install .
```

## 3) Download model assets

The base model assets are still required for processing pipelines.

Download:

https://emckclac-my.sharepoint.com/:u:/g/personal/stty3154_kcl_ac_uk/IQDeSEYuxxRDSp-zJovVXvbRAVmhmXRw97g7D0eLmJIKyUs?e=Iq446V

Place the base folder at the expected location used by model paths in processing modules.

## 4) Runtime secrets

Environment-specific secrets files are required and should remain gitignored.

Key examples:

- prove-shared/src/prove_shared/local_secrets.py
- prove-api/api/local_secrets.py (if used by API modules)
- prove-processing/utils/local_secrets.py (legacy paths still referenced by some processing code)

## 5) Configuration

Shared runtime settings are in:

- prove-shared/config.yaml

Includes DB paths, batch sizes, thresholds, and algorithm version.

## How to Run

## Processing a single entity

```python
from ProVe_main_process import initialize_models, process_entity

models = initialize_models()
qid = "Q44"
html_df, entailment_results, parser_stats = process_entity(qid, models)
```

## Start processing service

```bash
cd prove-processing
python ProVe_main_service.py
```

## Start API service

```bash
cd prove-api
python api/app.py
```

## Background processing

The scheduler can process:

- top viewed Wikidata items,
- pagepile list items,
- heuristic/random QID queues.

## Data Flow

1. API or scheduler enqueues a QID.
2. Processing worker fetches queue task.
3. Parser extracts claims + reference URLs.
4. HTML collector fetches and stores page content metadata.
5. Evidence selector ranks candidate sentences.
6. Entailment model classifies claim-evidence relationship.
7. Results are written to MongoDB and served by API.

## Notes on Ongoing Split

This repository currently contains all three components in one workspace folder, but structure and imports are being aligned for independent repository operation:

- prove-api
- prove-processing
- prove-shared

Project planning details are documented in project.md.

## Legacy Information Preserved from Previous README

The original README emphasized:

- parser/fetcher/evidence/entailment pipeline,
- MongoDB + SQLite storage model,
- service entry points,
- configuration in config.yaml,
- required external model folder.

All of these remain applicable, now mapped to the split folder layout above.
