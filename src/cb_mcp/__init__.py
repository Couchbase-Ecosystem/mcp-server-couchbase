"""Couchbase MCP Server Tools and Utilities package.

Reusable tool functions, utilities and core contracts shared by the
MCP server (``mcp_server.py``) and any other MCP server that
embeds the same tools (e.g. the managed Capella runtime). Each host
supplies its own ``ClusterProvider`` implementation; the standalone
host's lives in the top-level ``providers`` package alongside
``mcp_server.py``.
"""
