import os
import sys
from fastmcp import FastMCP

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Your MCP server instance
mcp = FastMCP("apex-ledger-mcp")

@mcp.tool()
def get_ledger_balance(account_id: str) -> str:
    """Retrieve the balance for a specific ledger account."""
    return f"Balance for {account_id} is $1,250.00"

def main(transport: str = "stdio"):
    """
    Run the MCP server.
    Note: FastMCP.run only accepts the 'transport' argument.
    Transports: "stdio" (default) or "sse" (HTTP).
    """
    print(f"Starting MCP server using {transport} transport...")
    
    # The fix: Remove host/port kwargs as they aren't supported by .run()
    mcp.run(transport=transport)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run Apex Ledger MCP")
    # Change default to 'stdio' for better compatibility with AI clients
    parser.add_argument("--transport", default="stdio", choices=["stdio", "sse"], 
                        help="Transport type (stdio or sse)")
    
    args = parser.parse_args()
    main(args.transport)