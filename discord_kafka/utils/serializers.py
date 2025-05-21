"""
Serialization utilities for Discord events and objects.

This module provides functionality to serialize and deserialize Discord objects
for transmission via Kafka.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional, Union, Type

import discord


logger = logging.getLogger(__name__)


class EventSerializer:
    """
    Serializes and deserializes Discord objects for Kafka transmission.

    This class handles converting Discord objects to JSON-serializable dictionaries
    and vice versa. Since Discord objects often contain complex relationships and
    circular references, special handling is needed.

    Attributes:
        custom_serializers (Dict[Type, Callable]): Type-specific serialization functions
    """

    def __init__(self):
        """
        Initialize the serializer with default serialization handlers.
        """
        # Define custom serialization handlers for different Discord types
        self.custom_serializers = {
            discord.Message: self._serialize_message,
            discord.Member: self._serialize_member,
            discord.User: self._serialize_user,
            discord.Guild: self._serialize_guild,
            discord.TextChannel: self._serialize_text_channel,
            discord.VoiceChannel: self._serialize_voice_channel,
            discord.CategoryChannel: self._serialize_category_channel,
            discord.Role: self._serialize_role,
            discord.Reaction: self._serialize_reaction,
            discord.Emoji: self._serialize_emoji,
        }
        logger.debug("Initialized EventSerializer with default serializers")

    def serialize(self, event_type: str, obj: Any) -> bytes:
        """
        Serialize a Discord object to a JSON string.

        Args:
            event_type: Type of event being serialized
            obj: Discord object to serialize

        Returns:
            JSON-encoded bytes representation of the object
        """
        try:
            # Convert object to a serializable dictionary
            serialized_dict = self._serialize_object(obj)

            # Add metadata
            serialized_dict["_kafka_meta"] = {
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "serializer_version": "0.1.0",
            }

            # Convert to JSON bytes
            return json.dumps(serialized_dict).encode("utf-8")
        except Exception as e:
            logger.error(f"Error serializing {event_type} event: {str(e)}")
            # Return a minimal representation if serialization fails
            return json.dumps(
                {
                    "error": f"Serialization failed: {str(e)}",
                    "_kafka_meta": {
                        "event_type": event_type,
                        "timestamp": datetime.utcnow().isoformat(),
                        "error": True,
                    },
                }
            ).encode("utf-8")

    def deserialize(self, event_type: str, data: bytes) -> Dict[str, Any]:
        """
        Deserialize JSON data back into a dictionary representation.

        Currently only deserializes to a dictionary, not back to Discord objects,
        as the consumer would typically only need the data for processing.

        Args:
            event_type: Type of event being deserialized
            data: JSON bytes data to deserialize

        Returns:
            Dictionary representation of the object
        """
        try:
            return json.loads(data.decode("utf-8"))
        except Exception as e:
            logger.error(f"Error deserializing {event_type} event: {str(e)}")
            return {
                "error": f"Deserialization failed: {str(e)}",
                "_kafka_meta": {"event_type": event_type, "error": True},
            }

    def _serialize_object(self, obj: Any) -> Union[Dict[str, Any], Any]:
        """
        Convert an object to a serializable representation.

        Args:
            obj: Object to serialize

        Returns:
            JSON-serializable representation of the object
        """
        # Handle None
        if obj is None:
            return None

        # Handle basic types that are already serializable
        if isinstance(obj, (str, int, float, bool)):
            return obj

        # Handle lists by recursively serializing each item
        if isinstance(obj, list):
            return [self._serialize_object(item) for item in obj]

        # Handle dictionaries by recursively serializing each value
        if isinstance(obj, dict):
            return {k: self._serialize_object(v) for k, v in obj.items()}

        # Handle datetime objects
        if isinstance(obj, datetime):
            return obj.isoformat()

        # Use custom serializer if available for this type
        for cls, serializer in self.custom_serializers.items():
            if isinstance(obj, cls):
                return serializer(obj)

        # For unrecognized types, try to convert to a dictionary
        try:
            # Try to get object attributes as a dict
            obj_dict = {}

            # For Discord objects that have an id, always include it
            if hasattr(obj, "id"):
                obj_dict["id"] = obj.id

            # For Discord objects that have a name, always include it
            if hasattr(obj, "name"):
                obj_dict["name"] = obj.name

            # Try to extract common attributes that might be useful
            for attr in dir(obj):
                # Skip private attributes, callables, and known problematic attributes
                if (
                    attr.startswith("_")
                    or callable(getattr(obj, attr))
                    or attr in ["_state", "guild", "channel", "author"]
                ):
                    continue

                try:
                    value = getattr(obj, attr)
                    # Recursively serialize the value
                    obj_dict[attr] = self._serialize_object(value)
                except (AttributeError, TypeError):
                    # Skip attributes that can't be accessed or serialized
                    pass

            # If we found any attributes to serialize, return the dict
            if obj_dict:
                return obj_dict

            # Last resort: try string representation
            return str(obj)
        except Exception as e:
            logger.warning(f"Could not serialize object of type {type(obj)}: {str(e)}")
            return f"<Unserializable {type(obj).__name__}>"

    def _serialize_message(self, message: discord.Message) -> Dict[str, Any]:
        """
        Serialize a Discord message.

        Args:
            message: Discord Message object

        Returns:
            Dictionary representation of the message
        """
        return {
            "id": message.id,
            "content": message.content,
            "author": self._serialize_object(message.author),
            "channel_id": message.channel.id,
            "guild_id": message.guild.id if message.guild else None,
            "created_at": message.created_at.isoformat(),
            "edited_at": message.edited_at.isoformat() if message.edited_at else None,
            "embeds": [self._serialize_object(embed) for embed in message.embeds],
            "attachments": [self._serialize_object(a) for a in message.attachments],
            "mentions": [self._serialize_object(m) for m in message.mentions],
            "mention_roles": [self._serialize_object(r) for r in message.role_mentions],
        }

    def _serialize_user(self, user: discord.User) -> Dict[str, Any]:
        """
        Serialize a Discord user.

        Args:
            user: Discord User object

        Returns:
            Dictionary representation of the user
        """
        return {
            "id": user.id,
            "name": user.name,
            "discriminator": user.discriminator,
            "display_name": user.display_name,
            "bot": user.bot,
            "avatar_url": str(user.avatar.url) if user.avatar else None,
        }

    def _serialize_member(self, member: discord.Member) -> Dict[str, Any]:
        """
        Serialize a Discord member.

        Args:
            member: Discord Member object

        Returns:
            Dictionary representation of the member
        """
        user_data = self._serialize_user(member)
        return {
            **user_data,
            "guild_id": member.guild.id,
            "roles": [role.id for role in member.roles],
            "joined_at": member.joined_at.isoformat() if member.joined_at else None,
            "nick": member.nick,
        }

    def _serialize_guild(self, guild: discord.Guild) -> Dict[str, Any]:
        """
        Serialize a Discord guild.

        Args:
            guild: Discord Guild object

        Returns:
            Dictionary representation of the guild
        """
        return {
            "id": guild.id,
            "name": guild.name,
            "icon_url": str(guild.icon.url) if guild.icon else None,
            "member_count": guild.member_count,
            "owner_id": guild.owner_id,
            "created_at": guild.created_at.isoformat(),
            # Avoid serializing all members, channels, roles to prevent large payloads
        }

    def _serialize_text_channel(self, channel: discord.TextChannel) -> Dict[str, Any]:
        """
        Serialize a Discord text channel.

        Args:
            channel: Discord TextChannel object

        Returns:
            Dictionary representation of the channel
        """
        return {
            "id": channel.id,
            "name": channel.name,
            "guild_id": channel.guild.id,
            "position": channel.position,
            "category_id": channel.category_id,
            "topic": channel.topic,
            "type": "text",
        }

    def _serialize_voice_channel(self, channel: discord.VoiceChannel) -> Dict[str, Any]:
        """
        Serialize a Discord voice channel.

        Args:
            channel: Discord VoiceChannel object

        Returns:
            Dictionary representation of the channel
        """
        return {
            "id": channel.id,
            "name": channel.name,
            "guild_id": channel.guild.id,
            "position": channel.position,
            "category_id": channel.category_id,
            "user_limit": channel.user_limit,
            "type": "voice",
        }

    def _serialize_category_channel(
        self, channel: discord.CategoryChannel
    ) -> Dict[str, Any]:
        """
        Serialize a Discord category channel.

        Args:
            channel: Discord CategoryChannel object

        Returns:
            Dictionary representation of the channel
        """
        return {
            "id": channel.id,
            "name": channel.name,
            "guild_id": channel.guild.id,
            "position": channel.position,
            "type": "category",
        }

    def _serialize_role(self, role: discord.Role) -> Dict[str, Any]:
        """
        Serialize a Discord role.

        Args:
            role: Discord Role object

        Returns:
            Dictionary representation of the role
        """
        return {
            "id": role.id,
            "name": role.name,
            "guild_id": role.guild.id,
            "color": role.color.value,
            "position": role.position,
            "permissions": role.permissions.value,
            "mentionable": role.mentionable,
            "hoist": role.hoist,
        }

    def _serialize_reaction(self, reaction: discord.Reaction) -> Dict[str, Any]:
        """
        Serialize a Discord reaction.

        Args:
            reaction: Discord Reaction object

        Returns:
            Dictionary representation of the reaction
        """
        return {
            "emoji": self._serialize_emoji(reaction.emoji),
            "count": reaction.count,
            "message_id": reaction.message.id,
            "channel_id": reaction.message.channel.id,
            "guild_id": reaction.message.guild.id if reaction.message.guild else None,
        }

    def _serialize_emoji(
        self, emoji: Union[discord.Emoji, discord.PartialEmoji, str]
    ) -> Dict[str, Any]:
        """
        Serialize a Discord emoji.

        Args:
            emoji: Discord Emoji, PartialEmoji, or str object

        Returns:
            Dictionary representation of the emoji
        """
        if isinstance(emoji, str):
            return {"name": emoji, "id": None, "type": "unicode"}

        return {
            "id": emoji.id,
            "name": emoji.name,
            "animated": emoji.animated if hasattr(emoji, "animated") else False,
            "type": "custom",
            "url": str(emoji.url) if hasattr(emoji, "url") else None,
        }
