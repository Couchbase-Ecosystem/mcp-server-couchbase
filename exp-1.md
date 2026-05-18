# Experiment 1

## Plain English

This experiment makes SQL++ query generation more reliable without redesigning the whole system.
Instead of trusting the first model output, we now run a simple 2-step post-processing flow:
1) extract and validate a query, then 2) fix/finalize it and validate again.

It also keeps Bedrock-compatible tool binding behavior so query generation does not fail because of unsupported parallel tool-call flags.

## More detail

### What changed

- Added an explicit **2-step query post-processing pipeline** in `src/query_postprocessor.py`:
  - **Step 1 (Extract + Validate):** extract best-effort SQL++ and run Lark validation.
  - **Step 2 (Fix/Finalize + Revalidate):** revise query using user intent, validation feedback, and optional function context; then validate final output.
- The postprocessor now returns structured metadata for both steps (plus backward-compatible fields).
- Existing handler integration remains mostly the same (`MultiAgentHandler` still calls `post_process_query_response(...)`).
- Bedrock-safe tool binding remains in place in query-generation agents to avoid `parallelToolCalls` errors.

### Why this differs from the starting branches

Compared to the original base branches, Experiment 1 adds a reliability layer after generation rather than relying on one-pass output.
This is intentionally minimal-change: most logic is centralized in one file (`src/query_postprocessor.py`) instead of introducing a large orchestrator rewrite.

### What this experiment does not change

- It does **not** introduce a full planner/composer multi-node graph rewrite.
- It does **not** move ownership of query orchestration out of iQ-FastAPI.
- It does **not** change MCP into the primary query planner; MCP still acts as caller/context bridge.
