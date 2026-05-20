## Experiment 3

### Objective
Add a two-agent SQL++ flow:
1. Agent 1 generates a draft query and draft query description.
2. Agent 2 receives both and returns a final valid, optimized SQL++ query.
3. Agent 2 also receives SQL++ DQL EBNF grammar context.

### Agent 2 tool contract
Agent 2 can use exactly these tools:
1. `validate_sqlpp_query` (lark-based SQL++ validation)
2. `search_couchbase` (Couchbase docs/vector search)
3. `validate_sqlpp_functions` (function-usage validation via docs retrieval per function)

### Agent 2 grammar context
- Include DQL EBNF from:
  `https://github.com/couchbaselabs/docs-devex/blob/e2bc0cf6381c67fbad1825b2229d35bbaea9fb80/modules/n1ql/partials/grammar/dql.ebnf`

### Expected behavior
- Agent 1 output is treated as draft.
- Agent 2 validates/optimizes the draft using tools + EBNF context and returns final SQL++.
- The final response returned to the user should contain the optimized validated query.
