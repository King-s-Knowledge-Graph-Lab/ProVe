# prove-shared

Shared Python package for ProVe.

This package contains common utilities used by both API and processing services:
- MongoDB handlers and shared data objects
- Auth helpers for queue/API communication
- Logging setup
- Wikidata and file helper utilities

## Install

From the `prove-shared` folder:

```bash
pip install .
```

For editable development install:

```bash
pip install -e .
```

## Import examples

```python
from prove_shared import MongoDBHandler, Status, HtmlContent, Entailment, AsyncAuth

# direct module imports still work
from prove_shared.mongo_handler import requestItemProcessing
```

## Package layout

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
      mongo_handler.py
      objects.py
      queue_manager.py
      wikidata_utils.py
```

## Runtime secret file

Some modules import `prove_shared.local_secrets` at runtime.

Create a file at:

- `src/prove_shared/local_secrets.py` (for local development), or
- `prove_shared/local_secrets.py` in the installed environment

This file is intentionally environment-specific and should stay gitignored.

## Build

To build wheel/sdist:

```bash
python -m build
```

(Install build first if needed: `pip install build`)
