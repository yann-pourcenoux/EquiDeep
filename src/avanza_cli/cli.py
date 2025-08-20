"""Command-line interface for Avanza CLI."""

import argparse
import logging


def run(args: argparse.Namespace) -> int:
    """Run the avanza-cli pipeline (skeleton implementation)."""
    logging.info("Starting avanza-cli run (skeleton)")
    # Placeholder; real pipeline will be implemented in later tasks
    logging.info("Completed avanza-cli run (skeleton)")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Build the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="avanza-cli", 
        description="Avanza CLI"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_run = subparsers.add_parser("run", help="Run the pipeline (skeleton)")
    # Flags will be added in later tasks; keep minimal now
    p_run.set_defaults(func=run)

    return parser


def app() -> int:
    """Main application entry point."""
    parser = _build_parser()
    args = parser.parse_args()
    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 2
    return int(func(args) or 0)
