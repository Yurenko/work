#!/usr/bin/env python3
"""Single entrypoint: start dashboard + bot control API."""

from app.core.logging import setup_logging
from app.dashboard.server import run

if __name__ == "__main__":
    setup_logging()
    run()
