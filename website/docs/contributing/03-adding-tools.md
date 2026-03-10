---
sidebar_position: 3
title: Adding New Tools
---

# Adding New Tools

Guide for adding new MCP tools to the Couchbase MCP Server.

## Steps

1. **Create the tool function** in the appropriate module under `src/tools/`:
   - `server.py` — Server and cluster management tools
   - `kv.py` — Key-value document operations
   - `query.py` — SQL++ query and performance analysis tools
   - `index.py` — Index management tools

2. **Export the tool** in `src/tools/__init__.py`:
   - Import the function
   - Add it to `READ_ONLY_TOOLS` (if it only reads data) or `KV_WRITE_TOOLS` (if it modifies data)
   - Add it to `__all__`

3. **Test the tool** with an MCP client.

## Example

Here's the pattern used by existing tools:

```python
import logging
from typing import Any

from mcp.server.fastmcp import Context

from utils.constants import MCP_SERVER_NAME
from utils.context import get_cluster_connection

logger = logging.getLogger(f"{MCP_SERVER_NAME}.tools.your_module")


def your_new_tool(
    ctx: Context, bucket_name: str, some_param: str
) -> dict[str, Any]:
    """Description of what this tool does.
    This docstring is exposed to the LLM as the tool description.
    """
    cluster = get_cluster_connection(ctx)
    # ... your implementation
    return {"result": "data"}
```

## Key Patterns

- **Context**: Always accept `ctx: Context` as the first parameter. Use `get_cluster_connection(ctx)` to get the Couchbase cluster object.
- **Lazy connection**: The cluster connection is established on the first tool call, not at server startup.
- **Logging**: Use the hierarchical logger pattern: `logging.getLogger(f"{MCP_SERVER_NAME}.tools.module_name")`
- **Error handling**: Either raise exceptions (they're returned to the LLM) or return error dictionaries — follow the pattern of similar tools.
- **Read-only awareness**: If your tool modifies data, add it to `KV_WRITE_TOOLS` so it's excluded in read-only mode.

## Checklist

- [ ] Function created in appropriate `src/tools/*.py` module
- [ ] Exported in `src/tools/__init__.py`
- [ ] Added to the correct tool list (`READ_ONLY_TOOLS` or `KV_WRITE_TOOLS`)
- [ ] Added to `__all__`
- [ ] Descriptive docstring (this is what the LLM sees)
- [ ] Tested with an MCP client
