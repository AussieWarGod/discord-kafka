"""
Base event handler classes for Discord events.

This module provides base classes for handling Discord events and publishing
them to Kafka, as well as consuming events from Kafka and triggering actions.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, Callable, List, Union

import discord
from discord.ext import commands

from discord_kafka.core.producer import KafkaProducer
from discord_kafka.core.consumer import KafkaConsumer
from discord_kafka.utils.serializers import EventSerializer


logger = logging.getLogger(__name__)


class BaseEventHandler:
    """
    Base class for handling Discord events.

    This class provides common functionality for event handlers,
    such as event filtering and Kafka integration.

    Attributes:
        bot: The Discord bot instance
        producer: KafkaProducer for sending events
        serializer: EventSerializer for serializing events
    """

    def __init__(
        self,
        bot: commands.Bot,
        producer: Optional[KafkaProducer] = None,
        serializer: Optional[EventSerializer] = None,
    ):
        """
        Initialize a new event handler.

        Args:
            bot: The Discord bot instance
            producer: Optional KafkaProducer for sending events
            serializer: Optional EventSerializer for serializing events
        """
        self.bot = bot
        self.producer = producer
        self.serializer = serializer or EventSerializer()

        # Register this handler with the bot
        self._register_handlers()

    def _register_handlers(self) -> None:
        """
        Register event handlers with the bot.

        This method should be implemented by subclasses to register
        the appropriate event handlers for the specific event types.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    async def _process_event(self, event_type: str, *args, **kwargs) -> None:
        """
        Process a Discord event.

        This method is called when an event is triggered. It should be
        implemented by subclasses to handle the specific event types.

        Args:
            event_type: The type of event being processed
            *args: Positional arguments from the event
            **kwargs: Keyword arguments from the event
        """
        raise NotImplementedError("Subclasses must implement this method.")

    async def _publish_to_kafka(
        self, event_type: str, event_data: Any, **headers
    ) -> None:
        """
        Publish an event to Kafka.

        Args:
            event_type: The type of event being published
            event_data: The event data to publish
            **headers: Additional headers to include in the Kafka message
        """
        if not self.producer:
            return

        # Generate a message key if possible
        key = None
        if hasattr(event_data, "id"):
            key = str(event_data.id)

        try:
            # We need to run producer.produce in a thread since it's blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.producer.produce(
                    event_type, event_data, key=key, headers=headers or None
                ),
            )
        except Exception as e:
            logger.error(f"Failed to publish event to Kafka: {str(e)}")


class MessageHandler(BaseEventHandler):
    """
    Handler for Discord message events.

    This class handles message creation, editing, and deletion events,
    with filtering options based on authors, channels, and content.

    Attributes:
        ignored_authors: Set of user IDs to ignore messages from
        allowed_channels: Set of channel IDs to allow messages from (None = all)
        ignored_channels: Set of channel IDs to ignore messages from
        content_filters: List of functions that filter messages based on content
    """

    def __init__(
        self,
        bot: commands.Bot,
        producer: Optional[KafkaProducer] = None,
        serializer: Optional[EventSerializer] = None,
        ignored_authors: Optional[List[int]] = None,
        allowed_channels: Optional[List[int]] = None,
        ignored_channels: Optional[List[int]] = None,
        content_filters: Optional[List[Callable[[str], bool]]] = None,
    ):
        """
        Initialize a new message handler.

        Args:
            bot: The Discord bot instance
            producer: Optional KafkaProducer for sending events
            serializer: Optional EventSerializer for serializing events
            ignored_authors: Optional list of user IDs to ignore messages from
            allowed_channels: Optional list of channel IDs to allow messages from (None = all)
            ignored_channels: Optional list of channel IDs to ignore messages from
            content_filters: Optional list of functions that filter messages based on content
        """
        self.ignored_authors = set(ignored_authors or [])
        self.allowed_channels = set(allowed_channels or [])
        self.ignored_channels = set(ignored_channels or [])
        self.content_filters = content_filters or []

        super().__init__(bot, producer, serializer)

    def _register_handlers(self) -> None:
        """
        Register message event handlers with the bot.
        """
        self.bot.add_listener(self._on_message, "on_message")
        self.bot.add_listener(self._on_message_edit, "on_message_edit")
        self.bot.add_listener(self._on_message_delete, "on_message_delete")

    def _should_process_message(self, message: discord.Message) -> bool:
        """
        Determine if a message should be processed based on filters.

        Args:
            message: The Discord message to check

        Returns:
            True if the message should be processed, False otherwise
        """
        # Skip messages from ignored authors
        if message.author.id in self.ignored_authors:
            return False

        # Skip messages from bots or webhooks if configured
        if message.author.bot:
            return False

        # Skip messages from ignored channels
        if message.channel.id in self.ignored_channels:
            return False

        # Only process messages from allowed channels if specified
        if self.allowed_channels and message.channel.id not in self.allowed_channels:
            return False

        # Apply content filters
        for filter_func in self.content_filters:
            if not filter_func(message.content):
                return False

        return True

    async def _on_message(self, message: discord.Message) -> None:
        """
        Handle the on_message event.

        Args:
            message: The Discord message that was created
        """
        if not self._should_process_message(message):
            return

        await self._process_event("message", message)

    async def _on_message_edit(
        self, before: discord.Message, after: discord.Message
    ) -> None:
        """
        Handle the on_message_edit event.

        Args:
            before: The Discord message before editing
            after: The Discord message after editing
        """
        if not self._should_process_message(after):
            return

        await self._process_event("message_edit", before, after)

    async def _on_message_delete(self, message: discord.Message) -> None:
        """
        Handle the on_message_delete event.

        Args:
            message: The Discord message that was deleted
        """
        if not self._should_process_message(message):
            return

        await self._process_event("message_delete", message)

    async def _process_event(self, event_type: str, *args, **kwargs) -> None:
        """
        Process a message event.

        Args:
            event_type: The type of event (message, message_edit, or message_delete)
            *args: The message object(s) associated with the event
        """
        if event_type == "message":
            message = args[0]
            await self._publish_to_kafka(
                "message",
                message,
                guild_id=str(message.guild.id) if message.guild else None,
                channel_id=str(message.channel.id),
            )
        elif event_type == "message_edit":
            before, after = args
            await self._publish_to_kafka(
                "message_edit",
                {"before": before, "after": after},
                guild_id=str(after.guild.id) if after.guild else None,
                channel_id=str(after.channel.id),
            )
        elif event_type == "message_delete":
            message = args[0]
            await self._publish_to_kafka(
                "message_delete",
                message,
                guild_id=str(message.guild.id) if message.guild else None,
                channel_id=str(message.channel.id),
            )


class ReactionHandler(BaseEventHandler):
    """
    Handler for Discord reaction events.

    This class handles reaction addition and removal events,
    with filtering options based on users, channels, and emojis.

    Attributes:
        ignored_users: Set of user IDs to ignore reactions from
        allowed_channels: Set of channel IDs to allow reactions from (None = all)
        ignored_channels: Set of channel IDs to ignore reactions from
        allowed_emojis: Set of emoji names/IDs to allow (None = all)
    """

    def __init__(
        self,
        bot: commands.Bot,
        producer: Optional[KafkaProducer] = None,
        serializer: Optional[EventSerializer] = None,
        ignored_users: Optional[List[int]] = None,
        allowed_channels: Optional[List[int]] = None,
        ignored_channels: Optional[List[int]] = None,
        allowed_emojis: Optional[List[Union[str, int]]] = None,
    ):
        """
        Initialize a new reaction handler.

        Args:
            bot: The Discord bot instance
            producer: Optional KafkaProducer for sending events
            serializer: Optional EventSerializer for serializing events
            ignored_users: Optional list of user IDs to ignore reactions from
            allowed_channels: Optional list of channel IDs to allow reactions from (None = all)
            ignored_channels: Optional list of channel IDs to ignore reactions from
            allowed_emojis: Optional list of emoji names/IDs to allow (None = all)
        """
        self.ignored_users = set(ignored_users or [])
        self.allowed_channels = set(allowed_channels or [])
        self.ignored_channels = set(ignored_channels or [])
        self.allowed_emojis = set(allowed_emojis or [])

        super().__init__(bot, producer, serializer)

    def _register_handlers(self) -> None:
        """
        Register reaction event handlers with the bot.
        """
        self.bot.add_listener(self._on_reaction_add, "on_reaction_add")
        self.bot.add_listener(self._on_reaction_remove, "on_reaction_remove")
        self.bot.add_listener(self._on_raw_reaction_add, "on_raw_reaction_add")
        self.bot.add_listener(self._on_raw_reaction_remove, "on_raw_reaction_remove")

    def _should_process_reaction(
        self,
        user_id: int,
        channel_id: int,
        emoji: Union[discord.Emoji, discord.PartialEmoji, str],
    ) -> bool:
        """
        Determine if a reaction should be processed based on filters.

        Args:
            user_id: The ID of the user who added/removed the reaction
            channel_id: The ID of the channel containing the message
            emoji: The emoji that was added/removed

        Returns:
            True if the reaction should be processed, False otherwise
        """
        # Skip reactions from ignored users
        if user_id in self.ignored_users:
            return False

        # Skip reactions from ignored channels
        if channel_id in self.ignored_channels:
            return False

        # Only process reactions from allowed channels if specified
        if self.allowed_channels and channel_id not in self.allowed_channels:
            return False

        # Only process allowed emojis if specified
        if self.allowed_emojis:
            emoji_id = emoji.id if hasattr(emoji, "id") else emoji
            emoji_name = emoji.name if hasattr(emoji, "name") else emoji
            if (
                str(emoji_id) not in self.allowed_emojis
                and emoji_name not in self.allowed_emojis
            ):
                return False

        return True

    async def _on_reaction_add(
        self, reaction: discord.Reaction, user: Union[discord.User, discord.Member]
    ) -> None:
        """
        Handle the on_reaction_add event.

        Args:
            reaction: The reaction that was added
            user: The user who added the reaction
        """
        if not self._should_process_reaction(
            user.id, reaction.message.channel.id, reaction.emoji
        ):
            return

        await self._process_event("reaction_add", reaction, user)

    async def _on_reaction_remove(
        self, reaction: discord.Reaction, user: Union[discord.User, discord.Member]
    ) -> None:
        """
        Handle the on_reaction_remove event.

        Args:
            reaction: The reaction that was removed
            user: The user who removed the reaction
        """
        if not self._should_process_reaction(
            user.id, reaction.message.channel.id, reaction.emoji
        ):
            return

        await self._process_event("reaction_remove", reaction, user)

    async def _on_raw_reaction_add(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        """
        Handle the on_raw_reaction_add event.

        This is triggered for all reaction adds, even for messages not in cache.

        Args:
            payload: The reaction payload
        """
        if not self._should_process_reaction(
            payload.user_id, payload.channel_id, payload.emoji
        ):
            return

        await self._process_event("raw_reaction_add", payload)

    async def _on_raw_reaction_remove(
        self, payload: discord.RawReactionActionEvent
    ) -> None:
        """
        Handle the on_raw_reaction_remove event.

        This is triggered for all reaction removes, even for messages not in cache.

        Args:
            payload: The reaction payload
        """
        if not self._should_process_reaction(
            payload.user_id, payload.channel_id, payload.emoji
        ):
            return

        await self._process_event("raw_reaction_remove", payload)

    async def _process_event(self, event_type: str, *args, **kwargs) -> None:
        """
        Process a reaction event.

        Args:
            event_type: The type of event (reaction_add, reaction_remove, etc.)
            *args: The reaction object(s) associated with the event
        """
        if event_type in ("reaction_add", "reaction_remove"):
            reaction, user = args
            message = reaction.message

            await self._publish_to_kafka(
                event_type,
                {
                    "reaction": reaction,
                    "user": user,
                    "message_id": message.id,
                    "emoji": reaction.emoji,
                },
                guild_id=str(message.guild.id) if message.guild else None,
                channel_id=str(message.channel.id),
                user_id=str(user.id),
            )
        elif event_type in ("raw_reaction_add", "raw_reaction_remove"):
            payload = args[0]

            await self._publish_to_kafka(
                event_type,
                payload,
                guild_id=str(payload.guild_id) if payload.guild_id else None,
                channel_id=str(payload.channel_id),
                user_id=str(payload.user_id),
            )


class MemberHandler(BaseEventHandler):
    """
    Handler for Discord member events.

    This class handles member join, leave, and update events,
    with filtering options based on guilds and user IDs.

    Attributes:
        allowed_guilds: Set of guild IDs to monitor members in (None = all)
        ignored_guilds: Set of guild IDs to ignore member events from
        ignored_users: Set of user IDs to ignore member events for
    """

    def __init__(
        self,
        bot: commands.Bot,
        producer: Optional[KafkaProducer] = None,
        serializer: Optional[EventSerializer] = None,
        allowed_guilds: Optional[List[int]] = None,
        ignored_guilds: Optional[List[int]] = None,
        ignored_users: Optional[List[int]] = None,
    ):
        """
        Initialize a new member handler.

        Args:
            bot: The Discord bot instance
            producer: Optional KafkaProducer for sending events
            serializer: Optional EventSerializer for serializing events
            allowed_guilds: Optional list of guild IDs to monitor members in (None = all)
            ignored_guilds: Optional list of guild IDs to ignore member events from
            ignored_users: Optional list of user IDs to ignore member events for
        """
        self.allowed_guilds = set(allowed_guilds or [])
        self.ignored_guilds = set(ignored_guilds or [])
        self.ignored_users = set(ignored_users or [])

        super().__init__(bot, producer, serializer)

    def _register_handlers(self) -> None:
        """
        Register member event handlers with the bot.
        """
        self.bot.add_listener(self._on_member_join, "on_member_join")
        self.bot.add_listener(self._on_member_remove, "on_member_remove")
        self.bot.add_listener(self._on_member_update, "on_member_update")

    def _should_process_member(self, member: discord.Member) -> bool:
        """
        Determine if a member event should be processed based on filters.

        Args:
            member: The Discord member to check

        Returns:
            True if the member should be processed, False otherwise
        """
        # Skip events from ignored users
        if member.id in self.ignored_users:
            return False

        # Skip events from ignored guilds
        if member.guild.id in self.ignored_guilds:
            return False

        # Only process events from allowed guilds if specified
        if self.allowed_guilds and member.guild.id not in self.allowed_guilds:
            return False

        return True

    async def _on_member_join(self, member: discord.Member) -> None:
        """
        Handle the on_member_join event.

        Args:
            member: The member who joined the guild
        """
        if not self._should_process_member(member):
            return

        await self._process_event("member_join", member)

    async def _on_member_remove(self, member: discord.Member) -> None:
        """
        Handle the on_member_remove event.

        Args:
            member: The member who left the guild
        """
        if not self._should_process_member(member):
            return

        await self._process_event("member_remove", member)

    async def _on_member_update(
        self, before: discord.Member, after: discord.Member
    ) -> None:
        """
        Handle the on_member_update event.

        Args:
            before: The member before the update
            after: The member after the update
        """
        if not self._should_process_member(after):
            return

        await self._process_event("member_update", before, after)

    async def _process_event(self, event_type: str, *args, **kwargs) -> None:
        """
        Process a member event.

        Args:
            event_type: The type of event (member_join, member_remove, member_update)
            *args: The member object(s) associated with the event
        """
        if event_type == "member_join":
            member = args[0]
            await self._publish_to_kafka(
                "member_join",
                member,
                guild_id=str(member.guild.id),
                user_id=str(member.id),
            )
        elif event_type == "member_remove":
            member = args[0]
            await self._publish_to_kafka(
                "member_remove",
                member,
                guild_id=str(member.guild.id),
                user_id=str(member.id),
            )
        elif event_type == "member_update":
            before, after = args
            await self._publish_to_kafka(
                "member_update",
                {"before": before, "after": after},
                guild_id=str(after.guild.id),
                user_id=str(after.id),
            )
