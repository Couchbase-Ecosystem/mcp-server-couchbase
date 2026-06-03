# Accuracy (AI-in-the-loop) Tests

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
| `test_kv_accuracy.py` | KV (get/insert/upsert/replace/delete + multi-step + negative selection) | 7 |
| `test_server_accuracy.py` | Server / cluster (buckets, scopes, collections, health, connection, config) | 9 |
| `test_query_accuracy.py` | SQL++ query (schema, run, explain) | 4 |
| `test_index_accuracy.py` | Indexes (list with filters, advisor recommendations) | 4 |
| `test_performance_accuracy.py` | Query performance analysis (all 7 tools, default + explicit limits) | 9 |

### Tool selection (23 cases)

[`test_tool_selection.py`](test_tool_selection.py) tests *intent
recognition*: given a conversational, real-user-style prompt, does the
LLM pick the right tool from the pool of 24? Parameter values are
intentionally not checked (the expected `parameters` is just
`Matcher.any_value()`) so this signal is decoupled from parameter
extraction.

Example: prompt is _"Tell me which SQL++ queries on my cluster are
taking forever to run — I want to know the biggest time hogs."_ and we
assert the LLM called `get_longest_running_queries`.

Each test file defines its cases inside `_build_cases(...)` and runs them
through the shared `run_accuracy_case` driver in
[`sdk/runner.py`](sdk/runner.py).

## Scoring

`sdk/scorer.py` implements the 0 / 0.75 / 1.0 rubric:

- **1.0** — exact expected tool calls with exact parameters.
- **0.75** — right tools called but with extras (extra calls / extra params).
- **0** — a required expected tool call was missing, or a matched call had
  incorrect parameters.

Tests fail when the score drops below 0.75.

## Flexible parameter matching

`sdk/matcher.py` provides matchers for the inherent non-determinism of LLM
output. Examples (used directly inside `parameters`):

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

## Running

Install dev dependencies (the accuracy tests' `openai` client lives in the
`dev` extra alongside pytest):

```bash
uv sync --extra dev
# or: pip install -e ".[dev]"
```

Set the required environment:

```bash
# Couchbase (same as integration tests)
export CB_CONNECTION_STRING="couchbases://..."
export CB_USERNAME="..."
export CB_PASSWORD="..."
export CB_MCP_TEST_BUCKET="travel-sample"
export CB_MCP_TEST_SCOPE="_default"
export CB_MCP_TEST_COLLECTION="_default"

# OpenAI
export OPENAI_API_KEY="sk-..."
# Optional overrides:
# export CB_ACCURACY_OPENAI_MODEL="gpt-4o-mini"   # default
# export CB_ACCURACY_OPENAI_BASE_URL="https://..."  # Azure / proxy
# export CB_ACCURACY_RUN_ID="ci-2026-05-22"
# export CB_ACCURACY_RESULTS_DIR="/tmp/acc"
```

Run only the accuracy tests:

```bash
pytest tests/accuracy -m accuracy -v
```

Run everything except accuracy (the default for CI integration runs):

```bash
pytest -m "not accuracy"
```

## Results

Each run writes a JSON file under `tests/accuracy/results/<run_id>.json`
containing the prompt, expected tool calls, accuracy score, captured tool
calls, full message transcript, and token usage per prompt.

## Adding a case

1. Pick the tool-family file (or create a new `test_<family>_accuracy.py`).
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

## SDK reference

- [`sdk/runner.py`](sdk/runner.py) — `run_accuracy_case` drives one case
  end-to-end (open MCP session, seed, list tools, run agent, cleanup,
  score, persist).
- [`sdk/client.py`](sdk/client.py) — `AccuracyTestingClient` (MCP ↔ OpenAI
  bridge, tool-call recording, mock support, `call_tool_silent`).
- [`sdk/agent.py`](sdk/agent.py) — `OpenAIAgent` (tool-call loop).
- [`sdk/scorer.py`](sdk/scorer.py) — 0 / 0.75 / 1.0 scoring.
- [`sdk/matcher.py`](sdk/matcher.py) — flexible parameter matchers.
- [`sdk/result_storage.py`](sdk/result_storage.py) — disk JSON storage.
