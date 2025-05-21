"""
Configuration module for Kafka integration with discord.py.

This module provides configuration classes for connecting to Kafka and
managing topics for different types of Discord events.
"""

from typing import Dict, Optional, Any


class KafkaConfig:
    """
    Configuration for Kafka connection and topics.

    This class holds configuration options for connecting to a Kafka cluster and
    mapping Discord events to Kafka topics.

    Attributes:
        bootstrap_servers (str): Comma-separated list of Kafka broker addresses
        client_id (str): Identifier for this client in the Kafka cluster
        topics (Dict[str, str]): Mapping of event types to topic names
        producer_config (Dict[str, Any]): Additional configuration for the Kafka producer
        consumer_config (Dict[str, Any]): Additional configuration for the Kafka consumer
        default_topic (str): Default topic to use for events not explicitly mapped
        group_id (str): Consumer group ID for Kafka consumers
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        client_id: str = "discord-kafka",
        topics: Optional[Dict[str, str]] = None,
        producer_config: Optional[Dict[str, Any]] = None,
        consumer_config: Optional[Dict[str, Any]] = None,
        default_topic: str = "discord_events",
        group_id: str = "discord_bot_group",
    ) -> None:
        """
        Initialize KafkaConfig with connection settings and topic mappings.

        Args:
            bootstrap_servers: Comma-separated list of Kafka broker addresses
            client_id: Identifier for this client in the Kafka cluster
            topics: Mapping of event types to topic names
            producer_config: Additional configuration for the Kafka producer
            consumer_config: Additional configuration for the Kafka consumer
            default_topic: Default topic to use for events not explicitly mapped
            group_id: Consumer group ID for Kafka consumers
        """
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.topics = topics or {
            "message": "discord_messages",
            "reaction": "discord_reactions",
            "member": "discord_members",
            "guild": "discord_guilds",
        }
        self.producer_config = producer_config or {}
        self.consumer_config = consumer_config or {}
        self.default_topic = default_topic
        self.group_id = group_id

    def get_topic_for_event(self, event_type: str) -> str:
        """
        Get the appropriate Kafka topic name for a given Discord event type.

        Args:
            event_type: The type of Discord event (e.g., 'message', 'reaction')

        Returns:
            The corresponding topic name, or the default topic if not found
        """
        return self.topics.get(event_type, self.default_topic)

    def get_producer_config(self) -> Dict[str, Any]:
        """
        Get the configuration dictionary for the Kafka producer.

        Returns:
            A dictionary with all producer configuration options
        """
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.client_id}-producer",
        }
        config.update(self.producer_config)
        return config

    def get_consumer_config(self) -> Dict[str, Any]:
        """
        Get the configuration dictionary for the Kafka consumer.

        Returns:
            A dictionary with all consumer configuration options
        """
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": f"{self.client_id}-consumer",
            "group.id": self.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
        config.update(self.consumer_config)
        return config
