from mcp.server.fastmcp import FastMCP, Context
import logging

# Global object for import
MCP_SERVER_NAME = "couchbase"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(MCP_SERVER_NAME)

_mcp  : FastMCP = None


def set_mcp(instance):
    global _mcp
    _mcp = instance

def get_mcp():
    return _mcp