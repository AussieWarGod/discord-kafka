"""
Utility functions for working with Discord objects.

This module provides helper functions for common operations on Discord objects
that may be useful across multiple bot instances.
"""

import logging
from typing import Any, Dict, List, Optional, Union, Tuple

import discord
from discord.ext import commands


logger = logging.getLogger(__name__)


async def find_member(
    guild: discord.Guild, user_id_or_name: Union[int, str]
) -> Optional[discord.Member]:
    """
    Find a member in a guild by ID or name.

    Args:
        guild: The Discord guild to search in
        user_id_or_name: User ID or name/nickname to search for

    Returns:
        The member if found, None otherwise
    """
    # Try to get by ID first
    if isinstance(user_id_or_name, int) or user_id_or_name.isdigit():
        member = guild.get_member(int(user_id_or_name))
        if member:
            return member

    # Try to find by name
    for member in guild.members:
        if user_id_or_name.lower() == member.name.lower() or (
            member.nick and user_id_or_name.lower() == member.nick.lower()
        ):
            return member

    # Try to fetch from API as a last resort
    try:
        member = await guild.fetch_member(int(user_id_or_name))
        return member
    except (discord.NotFound, discord.HTTPException, ValueError):
        return None


async def find_channel(
    guild: discord.Guild, channel_id_or_name: Union[int, str]
) -> Optional[discord.abc.GuildChannel]:
    """
    Find a channel in a guild by ID or name.

    Args:
        guild: The Discord guild to search in
        channel_id_or_name: Channel ID or name to search for

    Returns:
        The channel if found, None otherwise
    """
    # Try to get by ID first
    if isinstance(channel_id_or_name, int) or channel_id_or_name.isdigit():
        channel = guild.get_channel(int(channel_id_or_name))
        if channel:
            return channel

    # Try to find by name
    for channel in guild.channels:
        if channel_id_or_name.lower() == channel.name.lower():
            return channel

    # Try to fetch from API as a last resort
    try:
        channel = await guild.fetch_channel(int(channel_id_or_name))
        return channel
    except (discord.NotFound, discord.HTTPException, ValueError):
        return None


async def find_role(
    guild: discord.Guild, role_id_or_name: Union[int, str]
) -> Optional[discord.Role]:
    """
    Find a role in a guild by ID or name.

    Args:
        guild: The Discord guild to search in
        role_id_or_name: Role ID or name to search for

    Returns:
        The role if found, None otherwise
    """
    # Try to get by ID first
    if isinstance(role_id_or_name, int) or role_id_or_name.isdigit():
        role = guild.get_role(int(role_id_or_name))
        if role:
            return role

    # Try to find by name
    for role in guild.roles:
        if role_id_or_name.lower() == role.name.lower():
            return role

    return None


async def batch_delete_messages(
    channel: discord.TextChannel, message_ids: List[int], reason: Optional[str] = None
) -> Tuple[int, int]:
    """
    Delete multiple messages in a channel in batches.

    Discord's bulk delete API can only delete messages that are less than 14 days old
    and can only delete 100 messages at a time.

    Args:
        channel: The Discord text channel to delete messages from
        message_ids: List of message IDs to delete
        reason: Optional reason for the audit log

    Returns:
        Tuple of (number of deleted messages, number of failed deletions)
    """
    if not message_ids:
        return 0, 0

    success_count = 0
    fail_count = 0

    # Discord bulk delete has a max of 100 messages
    chunks = [message_ids[i: i + 100] for i in range(0, len(message_ids), 100)]

    for chunk in chunks:
        try:
            # Try bulk delete first
            await channel.delete_messages(
                [discord.Object(id=msg_id) for msg_id in chunk], reason=reason
            )
            success_count += len(chunk)
        except discord.HTTPException as e:
            # If bulk delete fails (e.g., messages older than 14 days), try individual deletion
            logger.warning(
                f"Bulk delete failed, falling back to individual deletion: {str(e)}"
            )
            for msg_id in chunk:
                try:
                    await channel.get_partial_message(msg_id).delete()
                    success_count += 1
                except discord.HTTPException:
                    fail_count += 1

    return success_count, fail_count


async def add_roles_with_reason(
    member: discord.Member, roles: List[discord.Role], reason: Optional[str] = None
) -> Tuple[List[discord.Role], List[Tuple[discord.Role, str]]]:
    """
    Add multiple roles to a member with error handling.

    Args:
        member: The Discord member to add roles to
        roles: List of roles to add
        reason: Optional reason for the audit log

    Returns:
        Tuple of (list of successfully added roles, list of (role, error) tuples)
    """
    success = []
    failures = []

    for role in roles:
        try:
            if role not in member.roles:
                await member.add_roles(role, reason=reason)
                success.append(role)
        except discord.HTTPException as e:
            failures.append((role, str(e)))

    return success, failures


async def create_text_channel_with_permissions(
    guild: discord.Guild,
    name: str,
    category: Optional[discord.CategoryChannel] = None,
    overwrites: Optional[
        Dict[Union[discord.Role, discord.Member], discord.PermissionOverwrite]
    ] = None,
    position: Optional[int] = None,
    topic: Optional[str] = None,
    slowmode_delay: Optional[int] = None,
    nsfw: bool = False,
    reason: Optional[str] = None,
) -> discord.TextChannel:
    """
    Create a new text channel with permissions.

    Args:
        guild: The Discord guild to create the channel in
        name: Name of the channel
        category: Optional category to put the channel in
        overwrites: Optional permission overwrites
        position: Optional position in the channel list
        topic: Optional channel topic
        slowmode_delay: Optional slowmode delay in seconds
        nsfw: Whether the channel should be marked as NSFW
        reason: Optional reason for the audit log

    Returns:
        The newly created text channel
    """
    if not overwrites:
        # Default overwrites: everyone can read but not send messages
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(
                read_messages=True, send_messages=False
            ),
            guild.me: discord.PermissionOverwrite(
                read_messages=True,
                send_messages=True,
                manage_messages=True,
                manage_channels=True,
            ),
        }

    try:
        channel = await guild.create_text_channel(
            name=name,
            overwrites=overwrites,
            category=category,
            position=position,
            topic=topic,
            slowmode_delay=slowmode_delay,
            nsfw=nsfw,
            reason=reason,
        )
        return channel
    except discord.HTTPException as e:
        logger.error(f"Failed to create channel '{name}': {str(e)}")
        raise


def get_clean_prefix(ctx: commands.Context) -> str:
    """
    Get the clean command prefix for a context.

    This handles cases where the prefix might be a mention or have extra whitespace.

    Args:
        ctx: The command context

    Returns:
        A clean version of the prefix
    """
    # If using a mention as prefix, return the bot name
    if (
        ctx.prefix == f"<@{ctx.bot.user.id}> "
        or ctx.prefix == f"<@!{ctx.bot.user.id}> "
    ):
        return f"@{ctx.bot.user.name} "

    return ctx.prefix


def format_timedelta(seconds: int) -> str:
    """
    Format a time delta in seconds to a human-readable string.

    Args:
        seconds: Number of seconds

    Returns:
        Formatted string like "2d 5h 3m 20s"
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if seconds > 0 or not parts:
        parts.append(f"{seconds}s")

    return " ".join(parts)


def extract_user_id_from_mention(mention: str) -> Optional[int]:
    """
    Extract a user ID from a Discord mention string.

    Args:
        mention: Discord mention string (e.g., "<@123456789>")

    Returns:
        User ID as integer if valid, None otherwise
    """
    if not mention:
        return None

    # Regular user mention
    if mention.startswith("<@") and mention.endswith(">"):
        mention = mention[2:-1]
        if mention.startswith("!"):
            mention = mention[1:]
        if mention.isdigit():
            return int(mention)

    return None
