#!/usr/bin/env python3
"""
Runner script for discord-kafka examples.

This script provides an easy way to run the examples included with discord-kafka.
It sets up the environment and handles command-line arguments.

Usage:
    python run_example.py basic_bot
    python run_example.py microservices
"""
import os
import sys
import argparse
import asyncio
import logging
import importlib
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("discord_kafka_runner")


def get_examples() -> Dict[str, Dict[str, Any]]:
    """
    Get a dictionary of available examples.

    Returns:
        Dictionary mapping example names to their information
    """
    return {
        "basic_bot": {
            "module": "discord_kafka.examples.basic_bot",
            "description": "Basic bot that publishes events to Kafka and consumes them",
            "env_vars": ["DISCORD_TOKEN", "KAFKA_BOOTSTRAP_SERVERS"],
        },
        "microservices": {
            "module": "discord_kafka.examples.microservices",
            "description": "Microservice architecture with three specialized bots",
            "env_vars": [
                "DISCORD_MOD_TOKEN",
                "DISCORD_LOG_TOKEN",
                "DISCORD_WELCOME_TOKEN",
                "KAFKA_BOOTSTRAP_SERVERS",
            ],
        },
    }


def check_environment(env_vars: list) -> bool:
    """
    Check if all required environment variables are set.

    Args:
        env_vars: List of environment variable names to check

    Returns:
        True if all required environment variables are set, False otherwise
    """
    missing = []
    for var in env_vars:
        if var not in os.environ:
            missing.append(var)

    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}")
        logger.error("Please set these variables before running the example")
        return False

    return True


def setup_kafka() -> bool:
    """
    Ensure that Kafka is properly set up for the examples.

    Returns:
        True if Kafka setup is successful, False otherwise
    """
    # For a real setup, this would check Kafka connection and create topics
    # For simplicity, we'll just check if KAFKA_BOOTSTRAP_SERVERS is set
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not kafka_servers:
        logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable is not set")
        logger.error(
            "Please set it to your Kafka bootstrap servers, e.g., localhost:9092"
        )
        return False

    logger.info(f"Using Kafka bootstrap servers: {kafka_servers}")
    return True


async def run_example(example_name: str) -> None:
    """
    Run an example by name.

    Args:
        example_name: Name of the example to run
    """
    examples = get_examples()

    if example_name not in examples:
        logger.error(f"Unknown example: {example_name}")
        logger.error(f"Available examples: {', '.join(examples.keys())}")
        sys.exit(1)

    example = examples[example_name]

    # Check environment variables
    if not check_environment(example["env_vars"]):
        sys.exit(1)

    # Check Kafka setup
    if not setup_kafka():
        sys.exit(1)

    # Import the example module
    try:
        module = importlib.import_module(example["module"])
    except ImportError as e:
        logger.error(f"Failed to import example module: {str(e)}")
        sys.exit(1)

    # Run the example
    if hasattr(module, "main"):
        try:
            logger.info(f"Running example: {example_name}")
            await module.main()
        except Exception as e:
            logger.exception(f"Error running example: {str(e)}")
            sys.exit(1)
    else:
        logger.error(f"Example {example_name} doesn't have a main() function")
        sys.exit(1)


def main() -> None:
    """
    Main entry point for the runner script.
    """
    examples = get_examples()

    parser = argparse.ArgumentParser(description="Run discord-kafka examples")
    parser.add_argument("example", choices=list(examples.keys()), help="Example to run")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Print example description
    logger.info(f"Example: {args.example}")
    logger.info(f"Description: {examples[args.example]['description']}")

    # Run the example
    try:
        asyncio.run(run_example(args.example))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
