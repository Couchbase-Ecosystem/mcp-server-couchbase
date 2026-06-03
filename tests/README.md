# Tests

The suite is split into three tiers by how much infrastructure each tier
needs. Each tier lives in its own directory so a glance at the layout tells
you what a file depends on, and so CI can opt into / out of each tier
independently.

```
tests/
├── _test_env.py         # shared env helpers (cluster creds, default bucket)
├── conftest.py          # top-level fixtures + helpers used by integration tests
├── unit/                # pure Python, no Couchbase, no LLM
├── integration/         # needs a live Couchbase cluster
└── accuracy/            # AI-in-the-loop — needs Couchbase + an OpenAI key
```

## Tiers

| Tier | Directory | Marker | Live cluster? | LLM cost? |
| --- | --- | --- | --- | --- |
| Unit | `tests/unit/` | — | No | No |
| Integration | `tests/integration/` | `integration` | Yes | No |
| Accuracy | `tests/accuracy/` | `accuracy` | Yes | Yes |

- **Unit** — call functions in `cb_mcp.*` directly, with fakes / `SimpleNamespace`.
  Fast, deterministic, runnable anywhere.
- **Integration** — spawn the real MCP server over stdio (`create_mcp_session`)
  and talk to a running Couchbase cluster. Requires `CB_CONNECTION_STRING`,
  `CB_USERNAME`, `CB_PASSWORD`, plus `CB_MCP_TEST_BUCKET` for tests that
  need a bucket. Missing env vars cause `pytest.skip(...)` rather than a
  failure.
- **Accuracy** — drive an OpenAI tool-calling agent against the live MCP
  server and score the resulting tool calls. See [Accuracy tier
  details](#accuracy-tier-details) below.

## Running

Install dev dependencies (pulls in pytest + the accuracy SDK's `openai`):

```bash
uv sync --extra dev
# or: pip install -e ".[dev]"
```

Common commands:

```bash
# everything
pytest

# fast pass — unit only (no Couchbase, no API cost)
pytest tests/unit

# integration only (needs Couchbase env vars)
pytest tests/integration               # or: pytest -m integration

# accuracy only (needs Couchbase + OPENAI_API_KEY)
pytest tests/accuracy -v               # or: pytest -m accuracy

# CI fast-path: skip the LLM tier
pytest -m "not accuracy"
```

Env vars used across the tiers:

```bash
# Couchbase (integration + accuracy)
export CB_CONNECTION_STRING="couchbases://..."
export CB_USERNAME="..."
export CB_PASSWORD="..."
export CB_MCP_TEST_BUCKET="travel-sample"
export CB_MCP_TEST_SCOPE="_default"
export CB_MCP_TEST_COLLECTION="_default"

# OpenAI (accuracy only)
export OPENAI_API_KEY="sk-..."
# Optional accuracy overrides:
# export CB_ACCURACY_OPENAI_MODEL="gpt-4o-mini"     # default
# export CB_ACCURACY_OPENAI_BASE_URL="https://..."  # Azure / proxy
# export CB_ACCURACY_RUN_ID="ci-2026-05-22"
# export CB_ACCURACY_RESULTS_DIR="/tmp/acc"
```

## Adding a test

- Pure logic, no I/O? → `tests/unit/`.
- Needs the running MCP server or a Couchbase round-trip? → `tests/integration/`.
- Verifies that an LLM picks the right tool / extracts the right params?
  → `tests/accuracy/`. See the [recipe below](#adding-an-accuracy-case).

If you're tempted to drop a unit test into `integration/` because it's
"close enough", don't — keeping the unit tier free of cluster
dependencies is what lets `pytest tests/unit` stay fast and runnable on
any laptop.

## Shared helpers

- [`_test_env.py`](_test_env.py) — env builders (`_build_env`,
  `require_test_bucket`, `get_test_scope`, `get_test_collection`).
  Imported by both the integration and accuracy conftests.
- [`conftest.py`](conftest.py) — re-exports the helpers and adds
  integration-only utilities (`create_mcp_session`, `extract_payload`,
  `ensure_list`, the `EXPECTED_TOOLS` / `TOOLS_BY_CATEGORY` /
  `TOOL_REQUIRED_PARAMS` tables).
- [`accuracy/conftest.py`](accuracy/conftest.py) — accuracy-only
  fixtures (`accuracy_client`, `openai_agent`, `result_storage`, etc.).

---

# Accuracy tier details

These tests check semantic correctness: given a natural-language prompt,
does an LLM pick the right MCP tool and extract the correct parameters?

The flow per test:

```
Test → AccuracyTestingClient → MCP Server → Couchbase
         ↑                ↓
       records         OpenAI tool-calling agent
       LLM tool calls
```

## What's currently covered

56 cases across two axes — **parameter extraction** (per-tool-family) and
**tool selection / intent recognition** (one cross-family file):

### Parameter extraction (33 cases)

These tests use explicit prompts and verify both the right tool *and* the
right parameters were extracted.

| File | Family | Cases |
| --- | --- | --- |
| `accuracy/test_kv_accuracy.py` | KV (get/insert/upsert/replace/delete + multi-step + negative selection) | 7 |
| `accuracy/test_server_accuracy.py` | Server / cluster (buckets, scopes, collections, health, connection, config) | 9 |
| `accuracy/test_query_accuracy.py` | SQL++ query (schema, run, explain) | 4 |
| `accuracy/test_index_accuracy.py` | Indexes (list with filters, advisor recommendations) | 4 |
| `accuracy/test_performance_accuracy.py` | Query performance analysis (all 7 tools, default + explicit limits) | 9 |

### Tool selection (23 cases)

[`accuracy/test_tool_selection.py`](accuracy/test_tool_selection.py) tests
*intent recognition*: given a conversational, real-user-style prompt, does
the LLM pick the right tool from the pool of 24? Parameter values are
intentionally not checked (the expected `parameters` is just
`Matcher.any_value()`) so this signal is decoupled from parameter
extraction.

Example: prompt is _"Tell me which SQL++ queries on my cluster are taking
forever to run — I want to know the biggest time hogs."_ and we assert
the LLM called `get_longest_running_queries`.

Each test file defines its cases inside `_build_cases(...)` and runs them
through the shared `run_accuracy_case` driver in
[`accuracy/sdk/runner.py`](accuracy/sdk/runner.py).

## Scoring

`accuracy/sdk/scorer.py` implements the 0 / 0.75 / 1.0 rubric:

- **1.0** — exact expected tool calls with exact parameters.
- **0.75** — right tools called but with extras (extra calls / extra params).
- **0** — a required expected tool call was missing, or a matched call had
  incorrect parameters.

Tests fail when the score drops below 0.75.

## Flexible parameter matching

`accuracy/sdk/matcher.py` provides matchers for the inherent
non-determinism of LLM output. Examples (used directly inside
`parameters`):

```python
from sdk import Matcher

ExpectedToolCall(
    tool_name="upsert_document_by_id",
    parameters={
        "bucket_name": "travel-sample",
        "scope_name": "inventory",
        "collection_name": "airline",
        "document_id": "airline_42",
        "document_content": Matcher.any_value(),  # body is LLM-derived
    },
)
```

Available matchers: `any_value`, `empty_object_or_undefined`, `undefined`,
`null`, `boolean`, `number`, `string`, `case_insensitive_string`, `any_of`,
`not_`, and the default `value` (literal match with recursion).

## Results

Each run writes a JSON file under `tests/accuracy/results/<run_id>.json`
containing the prompt, expected tool calls, accuracy score, captured tool
calls, full message transcript, and token usage per prompt.

## Adding an accuracy case

1. Pick the tool-family file (or create a new
   `accuracy/test_<family>_accuracy.py`).
2. Append an `AccuracyCase` inside that file's `_build_cases(...)`:
   ```python
   AccuracyCase(
       test_id="my_new_case",
       prompt="...",
       expected_tools=[ExpectedToolCall(...)],
       seed=...,       # optional async fn taking AccuracyTestingClient
       cleanup=...,    # optional async fn taking AccuracyTestingClient
   )
   ```
3. Use `client.call_tool_silent(name, args)` inside seed/cleanup hooks so
   they don't pollute the LLM tool-call log.
4. Add the new `test_id` to the file's `*_CASE_IDS` list.

## Accuracy SDK reference

- [`accuracy/sdk/runner.py`](accuracy/sdk/runner.py) — `run_accuracy_case`
  drives one case end-to-end (open MCP session, seed, list tools, run
  agent, cleanup, score, persist).
- [`accuracy/sdk/client.py`](accuracy/sdk/client.py) —
  `AccuracyTestingClient` (MCP ↔ OpenAI bridge, tool-call recording,
  mock support, `call_tool_silent`).
- [`accuracy/sdk/agent.py`](accuracy/sdk/agent.py) — `OpenAIAgent`
  (tool-call loop).
- [`accuracy/sdk/scorer.py`](accuracy/sdk/scorer.py) — 0 / 0.75 / 1.0
  scoring.
- [`accuracy/sdk/matcher.py`](accuracy/sdk/matcher.py) — flexible
  parameter matchers.
- [`accuracy/sdk/result_storage.py`](accuracy/sdk/result_storage.py) —
  disk JSON storage.
