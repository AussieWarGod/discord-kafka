"""
Utility functions for inter-bot communication.

This module provides helper functions and classes to facilitate communication
between different bot instances using Kafka.
"""

import json
import logging
import uuid
from typing import Any, Dict, Optional, List, Callable, Union
from datetime import datetime

from discord_kafka.core.producer import KafkaProducer


logger = logging.getLogger(__name__)


class BotMessage:
    """
    Represents a message exchanged between bots.

    This class provides a standard format for messages exchanged between bot
    instances, including metadata like correlation IDs to track request-response patterns.

    Attributes:
        type (str): Type of message (e.g., 'command', 'event', 'response')
        data (Any): Payload data for the message
        sender (str): Identifier of the sending bot
        correlation_id (str): Unique ID to correlate requests and responses
        timestamp (datetime): Time the message was created
        reply_to (Optional[str]): Topic to send responses to, if any
        ttl (int): Time-to-live in seconds, after which the message is considered stale
    """

    def __init__(
        self,
        message_type: str,
        data: Any,
        sender: str,
        correlation_id: Optional[str] = None,
        reply_to: Optional[str] = None,
        ttl: int = 60,
    ):
        """
        Initialize a new bot message.

        Args:
            message_type: Type of message (e.g., 'command', 'event', 'response')
            data: Payload data for the message
            sender: Identifier of the sending bot
            correlation_id: Unique ID to correlate requests and responses (generated if None)
            reply_to: Topic to send responses to, if any
            ttl: Time-to-live in seconds, after which the message is considered stale
        """
        self.type = message_type
        self.data = data
        self.sender = sender
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.timestamp = datetime.utcnow()
        self.reply_to = reply_to
        self.ttl = ttl

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the message to a dictionary for serialization.

        Returns:
            Dictionary representation of the message
        """
        return {
            "type": self.type,
            "data": self.data,
            "metadata": {
                "sender": self.sender,
                "correlation_id": self.correlation_id,
                "timestamp": self.timestamp.isoformat(),
                "reply_to": self.reply_to,
                "ttl": self.ttl,
            },
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BotMessage":
        """
        Create a BotMessage from a dictionary.

        Args:
            data: Dictionary representation of the message

        Returns:
            A new BotMessage instance
        """
        metadata = data.get("metadata", {})
        return cls(
            message_type=data["type"],
            data=data["data"],
            sender=metadata.get("sender", "unknown"),
            correlation_id=metadata.get("correlation_id"),
            reply_to=metadata.get("reply_to"),
            ttl=metadata.get("ttl", 60),
        )

    def is_expired(self) -> bool:
        """
        Check if the message has expired based on its TTL.

        Returns:
            True if the message has expired, False otherwise
        """
        elapsed = (datetime.utcnow() - self.timestamp).total_seconds()
        return elapsed > self.ttl


class BotCommunicator:
    """
    Facilitates communication between bot instances using a Kafka topic.

    This class provides methods to send and listen for messages between different
    bot instances, supporting request-response patterns and event broadcasts.

    Attributes:
        producer (KafkaProducer): Kafka producer for sending messages
        bot_id (str): Identifier for this bot instance
        control_topic (str): Kafka topic used for bot communication
        message_handlers (Dict[str, List[Callable]]): Handlers for different message types
    """

    def __init__(
        self,
        producer: KafkaProducer,
        bot_id: str,
        control_topic: str = "discord_bot_control",
    ):
        """
        Initialize a communicator for inter-bot messaging.

        Args:
            producer: KafkaProducer for sending messages
            bot_id: Unique identifier for this bot instance
            control_topic: Kafka topic used for bot communication
        """
        self.producer = producer
        self.bot_id = bot_id
        self.control_topic = control_topic
        self.message_handlers: Dict[str, List[Callable]] = {}
        logger.info(f"Initialized BotCommunicator for bot {bot_id}")

    def send_message(
        self,
        message_type: str,
        data: Any,
        reply_to: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        """
        Send a message to other bots via the control topic.

        Args:
            message_type: Type of message (e.g., 'command', 'event', 'response')
            data: Payload data for the message
            reply_to: Topic to send responses to, if any
            correlation_id: ID to correlate with a previous message (generated if None)

        Returns:
            The correlation ID of the sent message
        """
        message = BotMessage(
            message_type=message_type,
            data=data,
            sender=self.bot_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
        )

        # Serialize the message
        serialized = json.dumps(message.to_dict()).encode("utf-8")

        # Create headers
        headers = {
            "message_type": message_type,
            "sender": self.bot_id,
            "correlation_id": message.correlation_id,
        }

        # Send to the control topic
        self.producer.produce(
            event_type="bot_message",
            value=serialized,
            key=message.correlation_id,
            headers=headers,
        )

        logger.debug(
            f"Sent bot message of type {message_type} with correlation ID {message.correlation_id}"
        )
        return message.correlation_id

    def broadcast_event(self, event_name: str, event_data: Any) -> str:
        """
        Broadcast an event to all bots.

        Args:
            event_name: Name of the event
            event_data: Data associated with the event

        Returns:
            The correlation ID of the broadcast message
        """
        return self.send_message(
            message_type="event",
            data={"event_name": event_name, "event_data": event_data},
        )

    def send_command(
        self,
        target_bot: Optional[str],
        command_name: str,
        command_args: Dict[str, Any],
        reply_to: Optional[str] = None,
    ) -> str:
        """
        Send a command to another bot or broadcast to all bots.

        Args:
            target_bot: Target bot ID, or None to broadcast to all bots
            command_name: Name of the command to execute
            command_args: Arguments for the command
            reply_to: Topic for sending back responses

        Returns:
            The correlation ID of the command message
        """
        return self.send_message(
            message_type="command",
            data={"target": target_bot, "command": command_name, "args": command_args},
            reply_to=reply_to,
        )

    def send_response(
        self,
        correlation_id: str,
        response_data: Any,
        success: bool = True,
        error_message: Optional[str] = None,
    ) -> str:
        """
        Send a response to a previous message.

        Args:
            correlation_id: Correlation ID of the message being responded to
            response_data: Response payload data
            success: Whether the operation was successful
            error_message: Error message if the operation failed

        Returns:
            The correlation ID of the response message (same as input)
        """
        return self.send_message(
            message_type="response",
            data={"success": success, "data": response_data, "error": error_message},
            correlation_id=correlation_id,
        )

    def register_handler(self, message_type: str, handler: Callable) -> None:
        """
        Register a handler for a specific message type.

        Args:
            message_type: Type of message to handle
            handler: Function to call when a message of this type is received
        """
        if message_type not in self.message_handlers:
            self.message_handlers[message_type] = []

        self.message_handlers[message_type].append(handler)
        logger.debug(f"Registered handler for message type '{message_type}'")

    def handle_message(self, event_type: str, event_data: Dict[str, Any]) -> None:
        """
        Process a received bot message.

        This method should be registered as a handler for 'bot_message' events
        with the KafkaConsumer.

        Args:
            event_type: The event type from Kafka (should be 'bot_message')
            event_data: The serialized bot message data
        """
        try:
            # Deserialize the message
            bot_message = BotMessage.from_dict(json.loads(event_data))

            # Check if the message has expired
            if bot_message.is_expired():
                logger.debug(
                    f"Ignoring expired message with correlation ID {bot_message.correlation_id}"
                )
                return

            # Skip messages sent by this bot
            if bot_message.sender == self.bot_id:
                return

            # Handle commands where this bot is the target or target is None (broadcast)
            if bot_message.type == "command" and (
                "target" not in bot_message.data
                or not bot_message.data["target"]
                or bot_message.data["target"] == self.bot_id
            ):

                if "command" in self.message_handlers:
                    for handler in self.message_handlers["command"]:
                        handler(bot_message)

            # Handle all other message types
            if bot_message.type in self.message_handlers:
                for handler in self.message_handlers[bot_message.type]:
                    handler(bot_message)

            # Handle 'all' message type
            if "all" in self.message_handlers:
                for handler in self.message_handlers["all"]:
                    handler(bot_message)

        except Exception as e:
            logger.error(f"Error handling bot message: {str(e)}")


# Helper functions for common operations


def create_bot_communicator(producer: KafkaProducer, bot_id: str) -> BotCommunicator:
    """
    Create and configure a BotCommunicator instance.

    Args:
        producer: KafkaProducer instance
        bot_id: Unique identifier for the bot

    Returns:
        Configured BotCommunicator instance
    """
    return BotCommunicator(producer, bot_id)


def register_command_handler(
    communicator: BotCommunicator, command_name: str, handler: Callable
) -> None:
    """
    Register a handler for a specific command.

    Args:
        communicator: BotCommunicator instance
        command_name: Name of the command to handle
        handler: Function to call when the command is received
    """

    def command_wrapper(message: BotMessage):
        if message.type == "command" and message.data.get("command") == command_name:
            handler(
                message.data.get("args", {}),
                message.correlation_id,
                message.sender,
                communicator,
            )

    communicator.register_handler("command", command_wrapper)


def register_event_handler(
    communicator: BotCommunicator, event_name: str, handler: Callable
) -> None:
    """
    Register a handler for a specific event.

    Args:
        communicator: BotCommunicator instance
        event_name: Name of the event to handle
        handler: Function to call when the event is received
    """

    def event_wrapper(message: BotMessage):
        if message.type == "event" and message.data.get("event_name") == event_name:
            handler(
                message.data.get("event_data", {}),
                message.correlation_id,
                message.sender,
                communicator,
            )

    communicator.register_handler("event", event_wrapper)
