"""
Example of a basic Discord bot using discord-kafka integration.

This example demonstrates how to create a basic Discord bot that publishes
events to Kafka and consumes them from Kafka.

Usage:
    python basic_bot.py

Environment Variables:
    DISCORD_TOKEN - Discord bot token
    KAFKA_BOOTSTRAP_SERVERS - Comma-separated list of Kafka broker addresses
"""

import os
import asyncio
import logging
from typing import Dict, Any, Optional

import discord
from discord.ext import commands

from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
from discord_kafka.handlers.event_handlers import MessageHandler, ReactionHandler
from discord_kafka.utils.communication import BotCommunicator, register_command_handler


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("discord_bot")


def get_env_variable(name: str, default: Optional[str] = None) -> str:
    """
    Get an environment variable or raise an exception if it's not set.

    Args:
        name: Name of the environment variable
        default: Default value to return if variable is not set

    Returns:
        Value of the environment variable

    Raises:
        RuntimeError: If the variable is not set and no default is provided
    """
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable {name} is not set")
    return value


# Handle command from another bot via Kafka
def handle_ping_command(
    args: Dict[str, Any],
    correlation_id: str,
    sender_id: str,
    communicator: BotCommunicator,
) -> None:
    """
    Handle a ping command received from Kafka.

    Args:
        args: Command arguments
        correlation_id: ID to correlate the response
        sender_id: ID of the bot that sent the command
        communicator: BotCommunicator instance for sending responses
    """
    logger.info(f"Received ping command from {sender_id}")

    # Send a pong response
    communicator.send_response(
        correlation_id=correlation_id,
        response_data={"message": "Pong!", "timestamp": args.get("timestamp")},
        success=True,
    )


async def main() -> None:
    """
    Main function to start the bot.
    """
    # Get environment variables
    discord_token = get_env_variable("DISCORD_TOKEN")
    kafka_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Configure intents
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True

    # Configure Kafka
    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_servers,
        client_id="discord-basic-bot",
        topics={
            "message": "discord_messages",
            "reaction": "discord_reactions",
            "member": "discord_members",
            "guild": "discord_guilds",
            "command": "discord_commands",
        },
        group_id="basic-bot-group",
    )

    # Create the bot
    bot = KafkaBot(
        command_prefix="!",
        kafka_config=kafka_config,
        enable_consumer=True,  # Enable Kafka consumer
        intents=intents,
    )

    # Register event handlers
    message_handler = MessageHandler(bot, bot.producer)
    reaction_handler = ReactionHandler(bot, bot.producer)

    # Create a bot communicator for inter-bot messaging
    bot_communicator = BotCommunicator(
        bot.producer, f"basic-bot-{bot.user.id if bot.user else 'unknown'}"
    )

    # Register command handlers for bot communication
    register_command_handler(bot_communicator, "ping", handle_ping_command)

    # Register bot communicator with the Kafka consumer
    bot.register_kafka_handler("bot_message", bot_communicator.handle_message)

    # Start the Kafka consumer
    bot.start_consumer()

    # Define Discord command
    @bot.command(name="ping")
    async def ping_command(ctx):
        """Simple ping command to test bot responsiveness."""
        await ctx.send("Pong!")

        # Also broadcast this as an event to other bots
        bot_communicator.broadcast_event(
            "ping",
            {
                "guild_id": ctx.guild.id if ctx.guild else None,
                "channel_id": ctx.channel.id,
            },
        )

    @bot.command(name="sendbot")
    async def send_bot_command(ctx, bot_name: str, command: str, *args):
        """Send a command to another bot via Kafka."""
        command_args = " ".join(args)
        bot_communicator.send_command(
            target_bot=bot_name,
            command_name=command,
            command_args={
                "args": command_args,
                "timestamp": ctx.message.created_at.isoformat(),
            },
            reply_to="discord_responses",
        )
        await ctx.send(f"Command '{command}' sent to bot '{bot_name}'")

    # Event for when the bot is ready
    @bot.event
    async def on_ready():
        logger.info(f"Bot is ready! Logged in as {bot.user.name} ({bot.user.id})")
        logger.info(f"Connected to Kafka at {kafka_servers}")

    try:
        # Start the bot
        await bot.start(discord_token)
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
    finally:
        # Close the bot properly
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.exception(f"Fatal error: {str(e)}")
        exit(1)
