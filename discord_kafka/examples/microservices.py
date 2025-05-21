"""
Example of a microservice architecture using discord-kafka.

This example demonstrates how to create multiple Discord bots that work together
as microservices, communicating with each other through Kafka.

- Moderation Bot: Handles moderation commands and publishes moderation events
- Logging Bot: Consumes events and logs them
- Welcome Bot: Handles member join events and sends welcome messages

Usage:
    python microservices.py

Environment Variables:
    DISCORD_MOD_TOKEN - Discord token for moderation bot
    DISCORD_LOG_TOKEN - Discord token for logging bot
    DISCORD_WELCOME_TOKEN - Discord token for welcome bot
    KAFKA_BOOTSTRAP_SERVERS - Comma-separated list of Kafka broker addresses
"""

import os
import asyncio
import logging
import signal
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

import discord
from discord.ext import commands, tasks

from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
from discord_kafka.handlers.event_handlers import MessageHandler, MemberHandler
from discord_kafka.utils.communication import (
    BotCommunicator,
    register_command_handler,
    register_event_handler,
)
from discord_kafka.utils.discord_helpers import find_channel


# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("discord_microservices")


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


class ModerationBot(KafkaBot):
    """
    Bot responsible for moderation actions.

    This bot handles commands for muting, warning, and banning users,
    and publishes these actions as events for other bots to consume.
    """

    def __init__(self, kafka_config: KafkaConfig, **options):
        super().__init__(command_prefix="!mod ", kafka_config=kafka_config, **options)

        # Store user warnings
        self.warnings: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}

        # Create communicator for inter-bot messaging
        self.communicator = BotCommunicator(self.producer, "moderation-bot")

        # Register command handlers
        register_command_handler(
            self.communicator, "check_user", self._handle_check_user
        )

    async def setup_hook(self) -> None:
        """Set up the bot when it's ready."""
        # Start background tasks
        self.check_expired_mutes.start()

    def _handle_check_user(
        self,
        args: Dict[str, Any],
        correlation_id: str,
        sender_id: str,
        communicator: BotCommunicator,
    ) -> None:
        """
        Handle a check_user command from another bot.

        Args:
            args: Command arguments (must contain user_id)
            correlation_id: ID to correlate the response
            sender_id: ID of the bot that sent the command
            communicator: BotCommunicator instance for sending responses
        """
        user_id = args.get("user_id")
        if not user_id:
            communicator.send_response(
                correlation_id=correlation_id,
                response_data=None,
                success=False,
                error_message="Missing user_id parameter",
            )
            return

        # Get warnings for this user across all guilds
        user_warnings = {}
        for guild_id, guild_warnings in self.warnings.items():
            if int(user_id) in guild_warnings:
                user_warnings[guild_id] = guild_warnings[int(user_id)]

        communicator.send_response(
            correlation_id=correlation_id,
            response_data={"user_id": user_id, "warnings": user_warnings},
            success=True,
        )

    @tasks.loop(minutes=5.0)
    async def check_expired_mutes(self):
        """
        Check for expired mutes and unmute users.
        """
        try:
            logger.info("Checking for expired mutes...")

            # This would normally check a database
            # For this example, we'll just log that we're checking

            # Broadcast that we checked mutes
            self.communicator.broadcast_event(
                "mutes_checked", {"timestamp": datetime.utcnow().isoformat()}
            )

        except Exception as e:
            logger.error(f"Error checking mutes: {str(e)}")

    @check_expired_mutes.before_loop
    async def before_check_mutes(self):
        """Wait until the bot is ready before starting the task."""
        await self.wait_until_ready()

    @commands.command(name="warn")
    @commands.has_permissions(kick_members=True)
    async def warn_command(self, ctx, member: discord.Member, *, reason: str):
        """Warn a user and record the warning."""
        if ctx.guild.id not in self.warnings:
            self.warnings[ctx.guild.id] = {}

        if member.id not in self.warnings[ctx.guild.id]:
            self.warnings[ctx.guild.id][member.id] = []

        # Add the warning
        warning = {
            "reason": reason,
            "timestamp": datetime.utcnow().isoformat(),
            "moderator_id": ctx.author.id,
            "warning_id": len(self.warnings[ctx.guild.id][member.id]) + 1,
        }
        self.warnings[ctx.guild.id][member.id].append(warning)

        # Send confirmation
        await ctx.send(f"⚠️ **{member}** has been warned for: {reason}")

        # Try to DM the user
        try:
            await member.send(
                f"⚠️ You have been warned in **{ctx.guild.name}** for: {reason}"
            )
        except discord.HTTPException:
            await ctx.send("Could not DM user about the warning.")

        # Broadcast the warning event
        self.communicator.broadcast_event(
            "user_warned",
            {
                "guild_id": ctx.guild.id,
                "user_id": member.id,
                "moderator_id": ctx.author.id,
                "reason": reason,
                "warning_count": len(self.warnings[ctx.guild.id][member.id]),
            },
        )

    @commands.command(name="mute")
    @commands.has_permissions(manage_roles=True)
    async def mute_command(
        self,
        ctx,
        member: discord.Member,
        duration: Optional[str] = "1h",
        *,
        reason: Optional[str] = "No reason provided",
    ):
        """Mute a user for a specified duration."""
        # This would normally involve setting a role
        # For this example, we'll just pretend to mute the user

        # Parse duration (e.g., "1h", "30m", "1d")
        duration_seconds = 3600  # Default: 1 hour
        if duration.endswith("m"):
            duration_seconds = int(duration[:-1]) * 60
        elif duration.endswith("h"):
            duration_seconds = int(duration[:-1]) * 3600
        elif duration.endswith("d"):
            duration_seconds = int(duration[:-1]) * 86400

        expires_at = datetime.utcnow() + timedelta(seconds=duration_seconds)

        await ctx.send(f"🔇 **{member}** has been muted for {duration} for: {reason}")

        # Broadcast the mute event
        self.communicator.broadcast_event(
            "user_muted",
            {
                "guild_id": ctx.guild.id,
                "user_id": member.id,
                "moderator_id": ctx.author.id,
                "reason": reason,
                "duration": duration,
                "expires_at": expires_at.isoformat(),
            },
        )

    @commands.command(name="ban")
    @commands.has_permissions(ban_members=True)
    async def ban_command(
        self,
        ctx,
        member: discord.Member,
        *,
        reason: Optional[str] = "No reason provided",
    ):
        """Ban a user from the server."""
        await ctx.send(f"🔨 **{member}** has been banned for: {reason}")

        # Broadcast the ban event
        self.communicator.broadcast_event(
            "user_banned",
            {
                "guild_id": ctx.guild.id,
                "user_id": member.id,
                "moderator_id": ctx.author.id,
                "reason": reason,
            },
        )

        # Actually ban the user
        await member.ban(reason=reason)


class LoggingBot(KafkaBot):
    """
    Bot responsible for logging Discord events.

    This bot consumes events from Kafka and logs them to a designated channel.
    """

    def __init__(self, kafka_config: KafkaConfig, **options):
        super().__init__(
            command_prefix="!log ",
            kafka_config=kafka_config,
            enable_consumer=True,  # Enable Kafka consumer
            **options,
        )

        # Store log channel IDs per guild
        self.log_channels: Dict[int, int] = {}

        # Create communicator for inter-bot messaging
        self.communicator = BotCommunicator(self.producer, "logging-bot")

        # Register for various events we want to log
        # Register handlers for our Kafka consumer
        self.register_kafka_handler("bot_message", self.communicator.handle_message)
        self.register_kafka_handler("message", self._handle_message_event)
        self.register_kafka_handler("message_delete", self._handle_message_delete_event)

        # Register handlers for bot events
        register_event_handler(
            self.communicator, "user_warned", self._handle_user_warned_event
        )
        register_event_handler(
            self.communicator, "user_muted", self._handle_user_muted_event
        )
        register_event_handler(
            self.communicator, "user_banned", self._handle_user_banned_event
        )

    async def setup_hook(self) -> None:
        """Set up the bot when it's ready."""
        # Start the Kafka consumer
        self.start_consumer()

    async def _log_to_channel(
        self, guild_id: int, message: str, embed: Optional[discord.Embed] = None
    ) -> None:
        """
        Log a message to the designated log channel for a guild.

        Args:
            guild_id: ID of the guild to log to
            message: Message to log
            embed: Optional embed to include
        """
        if guild_id in self.log_channels:
            channel_id = self.log_channels[guild_id]
            channel = self.get_channel(channel_id)

            if channel:
                try:
                    if embed:
                        await channel.send(message, embed=embed)
                    else:
                        await channel.send(message)
                except discord.HTTPException as e:
                    logger.error(f"Failed to log to channel {channel_id}: {str(e)}")

    def _handle_message_event(
        self, event_type: str, event_data: Dict[str, Any]
    ) -> None:
        """
        Handle message events from Kafka.

        Args:
            event_type: Type of event (e.g., 'message')
            event_data: Event data
        """
        # This would normally do something more useful with the message
        logger.info(f"Received message event: {event_type}")

    def _handle_message_delete_event(
        self, event_type: str, event_data: Dict[str, Any]
    ) -> None:
        """
        Handle message deletion events from Kafka.

        Args:
            event_type: Type of event (e.g., 'message_delete')
            event_data: Event data
        """
        # Extract message details from event_data
        logger.info(f"Message deleted: {event_data.get('id', 'unknown')}")

    def _handle_user_warned_event(
        self,
        event_data: Dict[str, Any],
        correlation_id: str,
        sender_id: str,
        communicator: BotCommunicator,
    ) -> None:
        """
        Handle user warning events.

        Args:
            event_data: Event data
            correlation_id: Event correlation ID
            sender_id: ID of the bot that sent the event
            communicator: BotCommunicator instance
        """
        guild_id = event_data.get("guild_id")
        user_id = event_data.get("user_id")
        moderator_id = event_data.get("moderator_id")
        reason = event_data.get("reason")
        warning_count = event_data.get("warning_count", 1)

        # Create an embed for the log
        embed = discord.Embed(
            title="User Warned",
            description=f"<@{user_id}> has been warned by <@{moderator_id}>",
            color=discord.Color.yellow(),
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Warning Count", value=str(warning_count), inline=True)
        embed.set_footer(text=f"User ID: {user_id}")

        # Log the warning
        asyncio.create_task(
            self._log_to_channel(guild_id, f"⚠️ User <@{user_id}> warned", embed)
        )

    def _handle_user_muted_event(
        self,
        event_data: Dict[str, Any],
        correlation_id: str,
        sender_id: str,
        communicator: BotCommunicator,
    ) -> None:
        """
        Handle user mute events.

        Args:
            event_data: Event data
            correlation_id: Event correlation ID
            sender_id: ID of the bot that sent the event
            communicator: BotCommunicator instance
        """
        guild_id = event_data.get("guild_id")
        user_id = event_data.get("user_id")
        moderator_id = event_data.get("moderator_id")
        reason = event_data.get("reason")
        duration = event_data.get("duration")
        expires_at = event_data.get("expires_at")

        # Create an embed for the log
        embed = discord.Embed(
            title="User Muted",
            description=f"<@{user_id}> has been muted by <@{moderator_id}>",
            color=discord.Color.orange(),
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.add_field(name="Duration", value=duration, inline=True)
        embed.add_field(name="Expires", value=expires_at, inline=True)
        embed.set_footer(text=f"User ID: {user_id}")

        # Log the mute
        asyncio.create_task(
            self._log_to_channel(
                guild_id, f"🔇 User <@{user_id}> muted for {duration}", embed
            )
        )

    def _handle_user_banned_event(
        self,
        event_data: Dict[str, Any],
        correlation_id: str,
        sender_id: str,
        communicator: BotCommunicator,
    ) -> None:
        """
        Handle user ban events.

        Args:
            event_data: Event data
            correlation_id: Event correlation ID
            sender_id: ID of the bot that sent the event
            communicator: BotCommunicator instance
        """
        guild_id = event_data.get("guild_id")
        user_id = event_data.get("user_id")
        moderator_id = event_data.get("moderator_id")
        reason = event_data.get("reason")

        # Create an embed for the log
        embed = discord.Embed(
            title="User Banned",
            description=f"<@{user_id}> has been banned by <@{moderator_id}>",
            color=discord.Color.red(),
        )
        embed.add_field(name="Reason", value=reason, inline=False)
        embed.set_footer(text=f"User ID: {user_id}")

        # Log the ban
        asyncio.create_task(
            self._log_to_channel(guild_id, f"🔨 User <@{user_id}> banned", embed)
        )

    @commands.command(name="setchannel")
    @commands.has_permissions(administrator=True)
    async def set_log_channel(self, ctx, channel: discord.TextChannel = None):
        """Set the logging channel for this guild."""
        if not channel:
            channel = ctx.channel

        self.log_channels[ctx.guild.id] = channel.id
        await ctx.send(f"Log channel set to {channel.mention}")


class WelcomeBot(KafkaBot):
    """
    Bot responsible for welcoming new members.

    This bot listens for member join events and sends welcome messages.
    It also checks with the moderation bot to see if the user has previous warnings.
    """

    def __init__(self, kafka_config: KafkaConfig, **options):
        super().__init__(
            command_prefix="!welcome ",
            kafka_config=kafka_config,
            enable_consumer=True,  # Enable Kafka consumer
            **options,
        )

        # Store welcome channel IDs per guild
        self.welcome_channels: Dict[int, int] = {}
        self.welcome_messages: Dict[int, str] = {}

        # Create communicator for inter-bot messaging
        self.communicator = BotCommunicator(self.producer, "welcome-bot")

        # Register handlers for our Kafka consumer
        self.register_kafka_handler("bot_message", self.communicator.handle_message)

        # Add member handler to track joins
        self.member_handler = MemberHandler(self, self.producer)

    async def setup_hook(self) -> None:
        """Set up the bot when it's ready."""
        # Start the Kafka consumer
        self.start_consumer()

    @commands.command(name="setwelcome")
    @commands.has_permissions(administrator=True)
    async def set_welcome(
        self, ctx, channel: discord.TextChannel = None, *, message: str = None
    ):
        """Set the welcome channel and message for this guild."""
        if not channel:
            channel = ctx.channel

        self.welcome_channels[ctx.guild.id] = channel.id

        if message:
            self.welcome_messages[ctx.guild.id] = message
        else:
            self.welcome_messages[ctx.guild.id] = (
                "Welcome to the server, {user_mention}!"
            )

        await ctx.send(
            f"Welcome channel set to {channel.mention} with message: {self.welcome_messages[ctx.guild.id]}"
        )

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        """Handle member join events."""
        guild_id = member.guild.id

        if guild_id in self.welcome_channels and guild_id in self.welcome_messages:
            channel_id = self.welcome_channels[guild_id]
            channel = self.get_channel(channel_id)

            if channel:
                # Format the welcome message
                welcome_msg = self.welcome_messages[guild_id].format(
                    user_mention=member.mention,
                    user_name=member.name,
                    guild_name=member.guild.name,
                    member_count=member.guild.member_count,
                )

                try:
                    # Check if the user has previous warnings
                    correlation_id = self.communicator.send_command(
                        target_bot="moderation-bot",
                        command_name="check_user",
                        command_args={"user_id": member.id},
                        reply_to="discord_responses",
                    )

                    # In a real bot, you would wait for and process the response
                    # Here we'll just send the welcome message
                    await channel.send(welcome_msg)

                except Exception as e:
                    logger.error(f"Error sending welcome message: {str(e)}")
                    # Send simple welcome message as fallback
                    await channel.send(f"Welcome to the server, {member.mention}!")


async def run_moderation_bot():
    """Run the moderation bot."""
    # Get environment variables
    discord_token = get_env_variable("DISCORD_MOD_TOKEN")
    kafka_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Configure intents
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True

    # Configure Kafka
    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_servers,
        client_id="discord-mod-bot",
        group_id="mod-bot-group",
    )

    # Create and start the bot
    bot = ModerationBot(kafka_config=kafka_config, intents=intents)
    await bot.start(discord_token)


async def run_logging_bot():
    """Run the logging bot."""
    # Get environment variables
    discord_token = get_env_variable("DISCORD_LOG_TOKEN")
    kafka_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Configure intents
    intents = discord.Intents.default()
    intents.message_content = True

    # Configure Kafka
    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_servers,
        client_id="discord-log-bot",
        group_id="log-bot-group",
    )

    # Create and start the bot
    bot = LoggingBot(kafka_config=kafka_config, intents=intents)
    await bot.start(discord_token)


async def run_welcome_bot():
    """Run the welcome bot."""
    # Get environment variables
    discord_token = get_env_variable("DISCORD_WELCOME_TOKEN")
    kafka_servers = get_env_variable("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    # Configure intents
    intents = discord.Intents.default()
    intents.message_content = True
    intents.members = True

    # Configure Kafka
    kafka_config = KafkaConfig(
        bootstrap_servers=kafka_servers,
        client_id="discord-welcome-bot",
        group_id="welcome-bot-group",
    )

    # Create and start the bot
    bot = WelcomeBot(kafka_config=kafka_config, intents=intents)
    await bot.start(discord_token)


async def main():
    """Run all bots in separate tasks."""
    try:
        # Create tasks for each bot
        mod_task = asyncio.create_task(run_moderation_bot())
        log_task = asyncio.create_task(run_logging_bot())
        welcome_task = asyncio.create_task(run_welcome_bot())

        # Wait for all tasks to complete (they shouldn't unless there's an error)
        await asyncio.gather(mod_task, log_task, welcome_task)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down bots...")
    except Exception as e:
        logger.exception(f"Error running bots: {str(e)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.exception(f"Fatal error: {str(e)}")
        exit(1)
