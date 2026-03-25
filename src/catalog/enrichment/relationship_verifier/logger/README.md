# logger

Simple file logger for relationship verification.

## What it does

- Creates one log file per run.
- Writes logs to `logger/logs/`.
- Uses `CB_VERIFIER_LOG_LEVEL` for log level (default: `INFO`).

## Usage

```python
from catalog.enrichment.relationship_verifier.logger import get_verifier_logger

logger = get_verifier_logger()
logger.info("starting verification")
```
