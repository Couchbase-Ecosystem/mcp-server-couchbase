## Experiment 2

### Objective
Add a two-agent SQL++ flow:
1. Agent 1 generates a draft query and draft query description.
2. Agent 2 receives both and returns a final valid, optimized SQL++ query.

### Agent 2 tool contract
Agent 2 can use exactly these tools:
1. `validate_sqlpp_query` (lark-based SQL++ validation)
2. `search_couchbase` (Couchbase docs/vector search)
3. `validate_sqlpp_functions` (function-usage validation via docs retrieval per function)

### Expected behavior
- Agent 1 output is treated as draft.
- Agent 2 validates/optimizes the draft and returns final SQL++.
- The final response returned to the user should contain the optimized validated query.
