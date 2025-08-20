"""Main entry point for Avanza CLI."""

from pathlib import Path
import logging


def _setup_runtime():
    """Set up logging and ensure data directory exists."""
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s %(levelname)s %(message)s'
    )
    Path("./data").mkdir(parents=True, exist_ok=True)


if __name__ == "__main__":
    _setup_runtime()
    from avanza_cli.cli import app
    app()
