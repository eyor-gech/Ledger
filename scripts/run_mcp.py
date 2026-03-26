"""
Run local MCP server for Apex Ledger demo
Connect your Svelte UI to http://localhost:8000/sse
"""

from __future__ import annotations
import os
import sys

# Ensure project root is on path
#sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ledger.mcp.mcp_server import create_mcp_server

def main() -> None:
    """Start MCP server in SSE mode."""
    db_url = os.getenv("DATABASE_URL")
    server = create_mcp_server(db_url=db_url)
    server.run(host="localhost", port=8000, sse=True)

if __name__ == "__main__":
    main()