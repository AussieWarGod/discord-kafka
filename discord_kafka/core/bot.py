"""
Core bot module extending discord.py's commands.Bot with Kafka integration.

This module provides the KafkaBot class that extends discord.py's Bot with
functionality to publish Discord events to Kafka topics.
"""

import asyncio
import logging
import inspect
from typing import Any, Dict, List, Optional, Union, Callable, Type

import discord
from discord.ext import commands

from discord_kafka.core.config import KafkaConfig
from discord_kafka.core.producer import KafkaProducer
from discord_kafka.core.consumer import KafkaConsumer
from discord_kafka.utils.serializers import EventSerializer


logger = logging.getLogger(__name__)


class KafkaBot(commands.Bot):
    """
    Discord bot with Kafka integration.

    This class extends discord.py's Bot class to automatically publish Discord events to
    Kafka topics, and to provide utilities for inter-bot communication through Kafka.

    Attributes:
        kafka_config (KafkaConfig): Configuration for Kafka connection
        producer (KafkaProducer): Kafka producer for sending events
        consumer (Optional[KafkaConsumer]): Optional Kafka consumer for receiving events
        publish_events (bool): Whether to publish Discord events to Kafka
        tracked_events (List[str]): List of Discord event names to track and publish
    """

    def __init__(
        self,
        command_prefix,
        kafka_config: KafkaConfig,
        enable_consumer: bool = False,
        serializer: Optional[EventSerializer] = None,
        publish_events: bool = True,
        tracked_events: Optional[List[str]] = None,
        **options,
    ):
        """
        Initialize a bot with Kafka integration.

        Args:
            command_prefix: Prefix for commands (passed to commands.Bot)
            kafka_config: Configuration for Kafka connection and topics
            enable_consumer: Whether to enable the Kafka consumer
            serializer: Optional custom serializer for Discord objects
            publish_events: Whether to publish Discord events to Kafka
            tracked_events: Optional list of event names to track (defaults to a standard set)
            **options: Additional options passed to commands.Bot
        """
        super().__init__(command_prefix=command_prefix, **options)

        # Initialize Kafka components
        self.kafka_config = kafka_config
        self.serializer = serializer or EventSerializer()
        self.producer = KafkaProducer(kafka_config, self.serializer)
        self.consumer = None
        if enable_consumer:
            self.consumer = KafkaConsumer(kafka_config, self.serializer)

        self.publish_events = publish_events

        # Default events to track if not specified
        self.tracked_events = tracked_events or [
            "on_message",
            "on_message_delete",
            "on_message_edit",
            "on_reaction_add",
            "on_reaction_remove",
            "on_member_join",
            "on_member_remove",
            "on_member_update",
            "on_guild_join",
            "on_guild_remove",
            "on_guild_update",
            "on_guild_role_create",
            "on_guild_role_delete",
            "on_guild_role_update",
            "on_voice_state_update",
        ]

        if self.publish_events:
            self._setup_event_publishing()

        logger.info("Initialized KafkaBot with Kafka integration")

    def _setup_event_publishing(self) -> None:
        """
        Set up listeners for Discord events to publish them to Kafka.
        """
        # Get all event methods from discord.Client
        for event_name, _ in inspect.getmembers(
            discord.Client, predicate=inspect.isfunction
        ):
            if event_name in self.tracked_events or event_name == "on_raw_event":
                # Create a wrapper that will publish the event to Kafka
                self._create_event_publisher(event_name)

        logger.info(f"Set up event publishing for events: {self.tracked_events}")

    def _create_event_publisher(self, event_name: str) -> None:
        """
        Create and register a wrapper method that publishes the event to Kafka.

        Args:
            event_name: Name of the Discord event (e.g., 'on_message')
        """
        # Get the original event method if it exists
        original_handler = getattr(self, event_name, None)

        # Convert the event name to a Kafka topic identifier (strip 'on_' prefix)
        event_type = event_name[3:] if event_name.startswith("on_") else event_name

        async def event_wrapper(*args, **kwargs):
            """
            Wrapper for Discord event that publishes to Kafka before calling original handler.
            """
            try:
                # The first argument is typically the event data (e.g., the message)
                if args:
                    # Publish event to Kafka
                    event_data = args[0]
                    # Create headers with metadata
                    headers = {
                        "event_type": event_type,
                        "bot_id": self.user.id if self.user else "unknown",
                    }

                    # Generate a key for the message if possible
                    key = None
                    if hasattr(event_data, "id"):
                        key = str(event_data.id)
                    elif len(args) > 1 and hasattr(args[1], "id"):
                        key = str(args[1].id)

                    # Try to get guild id for partitioning if available
                    guild_id = None
                    if hasattr(event_data, "guild") and event_data.guild:
                        guild_id = event_data.guild.id
                        headers["guild_id"] = str(guild_id)
                    elif hasattr(event_data, "guild_id") and event_data.guild_id:
                        guild_id = event_data.guild_id
                        headers["guild_id"] = str(guild_id)

                    # Publish asynchronously to Kafka
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,
                        lambda: self.producer.produce(
                            event_type, event_data, key=key, headers=headers
                        ),
                    )

                    logger.debug(f"Published '{event_type}' event to Kafka")

            except Exception as e:
                logger.error(f"Error publishing event to Kafka: {str(e)}")

            # Call the original handler if it exists
            if original_handler:
                return await original_handler(*args, **kwargs)

        # Register the wrapper as the event handler
        self.event(event_wrapper)

    def start_consumer(self) -> None:
        """
        Start the Kafka consumer if it's enabled.

        Raises:
            RuntimeError: If the consumer is not enabled
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer is not enabled for this bot")

        self.consumer.start()
        logger.info("Started Kafka consumer")

    def stop_consumer(self) -> None:
        """
        Stop the Kafka consumer if it's running.
        """
        if self.consumer:
            self.consumer.stop()
            logger.info("Stopped Kafka consumer")

    def register_kafka_handler(self, event_type: str, handler: Callable) -> None:
        """
        Register a handler for Kafka events.

        Args:
            event_type: Type of event to handle (e.g., 'message', 'reaction')
            handler: Function to call when an event of this type is received

        Raises:
            RuntimeError: If the consumer is not enabled
        """
        if not self.consumer:
            raise RuntimeError("Kafka consumer is not enabled for this bot")

        self.consumer.register_handler(event_type, handler)

    async def close(self) -> None:
        """
        Close the bot, including Kafka connections.
        """
        # Stop the Kafka consumer if it's running
        self.stop_consumer()

        # Ensure all messages are delivered before shutting down
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: self.producer.flush())

        # Call the parent close method
        await super().close()
