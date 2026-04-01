---
sidebar_label: Overview
slug: /
---

# Couchbase MCP Server

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/) [![PyPI version](https://badge.fury.io/py/couchbase-mcp-server.svg)](https://pypi.org/project/couchbase-mcp-server/)

Couchbase MCP Server is a [Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server implementation that lets LLMs directly interact with data stored in Couchbase clusters through a rich set of tools.

It exposes capabilities like cluster health checks, schema discovery, document key-value operations, SQL++ querying, and query performance analysis in a controlled way that is safe by default via read-only modes and fine-grained tool disabling. The server supports multiple transport modes, including stdio and Streamable HTTP, so it can be run locally or shared across teams.

## Architecture

![Couchbase MCP Server Architecture](/img/architecture.png)

## Tools

The server exposes several tools across multiple categories. See the [Tools](./tools/01-cluster-health.md) page for full details.

| Category | Tools |
| -------- | ----- |
| **Cluster Setup & Health** | `get_server_configuration_status`, `test_cluster_connection`, `get_cluster_health_and_services` |
| **Data Model & Schema Discovery** | `get_buckets_in_cluster`, `get_scopes_in_bucket`, `get_collections_in_scope`, `get_scopes_and_collections_in_bucket`, `get_schema_for_collection` |
| **Document KV Operations** | `get_document_by_id`, `upsert_document_by_id`, `insert_document_by_id`, `replace_document_by_id`, `delete_document_by_id` |
| **Query and Indexing** | `run_sql_plus_plus_query`, `explain_sql_plus_plus_query`, `list_indexes`, `get_index_advisor_recommendations` |
| **Query Performance Analysis** | `get_longest_running_queries`, `get_most_frequent_queries`, `get_queries_not_selective`, `get_queries_not_using_covering_index`, `get_queries_using_primary_index`,`get_queries_with_largest_response_sizes`, `get_queries_with_large_result_count` |

## Tutorials

<div class="tutorial-cards">
  <a class="tutorial-card" href="https://developer.couchbase.com/tutorial-ai-agent-using-langchain-and-couchbase-mcp-server/" target="_blank" rel="noopener noreferrer">
    <strong>AI Agent with LangChain</strong><br/>
    <span>Build an AI agent using LangChain and the Couchbase MCP Server.</span>
  </a>
  <a class="tutorial-card" href="https://developer.couchbase.com/tutorial-ai-agent-using-openai-agents-sdk-and-couchbase-mcp-server/" target="_blank" rel="noopener noreferrer">
    <strong>AI Agent with OpenAI Agents SDK</strong><br/>
    <span>Build an AI agent using the OpenAI Agents SDK and the Couchbase MCP Server.</span>
  </a>
</div>

## Releases

The latest release is available on [PyPI](https://pypi.org/project/couchbase-mcp-server/) and [Docker Hub](https://hub.docker.com/r/couchbaseecosystem/mcp-server-couchbase). See the [Release Notes](./product-notes/01-release-notes.md) for version history and details.

The latest version (v0.7.0) introduces support for explaining queries and elicitation for tool calls.

## Support Policy

We truly appreciate your interest in this project!
This project is **Couchbase community-maintained**, which means it's **not officially supported** by our support team. However, our engineers are actively monitoring and maintaining this repo and will try to resolve issues on a best-effort basis.

Our support portal is unable to assist with requests related to this project, so we kindly ask that all inquiries stay within GitHub.

- **Bug reports**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues)
- **Feature requests**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues) with the "enhancement" label
- **Questions**: [Open a GitHub issue](https://github.com/Couchbase-Ecosystem/mcp-server-couchbase/issues)

Your collaboration helps us all move forward together - thank you! Pull requests and contributions from the community are welcome - see [Contributing](./product-notes/02-contributing.md).

## Learn More

### Video Walkthrough

<iframe
  width="100%"
  style={{aspectRatio: "16/9"}}
  src="https://www.youtube.com/embed/sU40zTRjWcc"
  title="Introducing the Couchbase MCP Server for AI Agents"
  frameBorder="0"
  allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
  allowFullScreen
/>
