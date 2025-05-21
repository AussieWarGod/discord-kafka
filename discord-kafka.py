#!/usr/bin/env python3
"""
Discord-Kafka - A library extending discord.py to integrate with Kafka for microservice communication.

This is the main entry point for the library, providing an easy-to-use interface
for integrating Discord bots with Kafka for microservice communication.
"""
import argparse
import logging
import sys
import importlib

from discord_kafka import __version__


def main():
    """
    Command-line interface for discord-kafka.

    This allows users to run example bots or get information about the library.
    """
    parser = argparse.ArgumentParser(
        description="Discord-Kafka - A library extending discord.py to integrate with Kafka"
    )

    parser.add_argument(
        "--version", action="store_true", help="Show the version of discord-kafka"
    )

    parser.add_argument(
        "--run-example",
        choices=["basic_bot", "microservices"],
        help="Run one of the example bots",
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(
        level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    if args.version:
        print(f"discord-kafka version {__version__}")
        return

    if args.run_example:
        try:
            example_module = importlib.import_module(
                f"discord_kafka.examples.{args.run_example}"
            )
            if hasattr(example_module, "main"):
                import asyncio

                asyncio.run(example_module.main())
            else:
                print(
                    f"Error: Example {args.run_example} doesn't have a main() function"
                )
                sys.exit(1)
        except ImportError as e:
            print(f"Error importing example {args.run_example}: {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"Error running example {args.run_example}: {str(e)}")
            sys.exit(1)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
