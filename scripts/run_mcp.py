from __future__ import annotations

import os

from ledger.mcp.server import create_mcp_server


def main() -> None:
    db_url = os.getenv("DATABASE_URL")
    server = create_mcp_server(db_url=db_url)
    server.run()


if __name__ == "__main__":
    main()

