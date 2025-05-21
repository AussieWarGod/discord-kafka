"""
Kafka producer module for sending Discord events to Kafka topics.

This module provides functionality to serialize and send Discord events
to configured Kafka topics.
"""

import logging
from typing import Any, Dict, Optional, Callable

from confluent_kafka import Producer

from discord_kafka.core.config import KafkaConfig
from discord_kafka.utils.serializers import EventSerializer


logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Manages the production of Discord events to Kafka topics.

    This class wraps the confluent_kafka.Producer to provide a simple interface
    for publishing Discord events to Kafka topics.

    Attributes:
        config (KafkaConfig): Configuration for Kafka connection and topics
        producer (Producer): Underlying confluent_kafka.Producer instance
        serializer (EventSerializer): Serializer for Discord objects
    """

    def __init__(
        self, config: KafkaConfig, serializer: Optional[EventSerializer] = None
    ) -> None:
        """
        Initialize a KafkaProducer with the given configuration.

        Args:
            config: KafkaConfig instance with connection details and topic mappings
            serializer: Optional custom serializer for Discord objects
        """
        self.config = config
        self.producer = Producer(config.get_producer_config())
        self.serializer = serializer or EventSerializer()
        logger.info(
            f"Initialized Kafka producer with bootstrap servers: {config.bootstrap_servers}"
        )

    def produce(
        self,
        event_type: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        callback: Optional[Callable] = None,
    ) -> None:
        """
        Produce a message to the appropriate Kafka topic for the given event type.

        Args:
            event_type: Type of Discord event (e.g., 'message', 'reaction')
            value: The event data to be serialized and sent
            key: Optional message key for Kafka partitioning
            headers: Optional headers to include with the message
            callback: Optional callback function for delivery reports
        """
        topic = self.config.get_topic_for_event(event_type)
        serialized_value = self.serializer.serialize(event_type, value)

        # Convert headers to the format expected by confluent-kafka if provided
        kafka_headers = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

        # Define a default callback if none provided
        def default_callback(err, msg):
            if err:
                logger.error(f"Failed to deliver message: {err}")
            else:
                logger.debug(
                    f"Successfully delivered message to {msg.topic()}:{msg.partition()}:{msg.offset()}"
                )

        try:
            self.producer.produce(
                topic=topic,
                value=serialized_value,
                key=key.encode("utf-8") if key else None,
                headers=kafka_headers or None,
                on_delivery=callback or default_callback,
            )
            # Trigger any available delivery report callbacks from previous produce calls
            self.producer.poll(0)

            logger.debug(f"Produced event '{event_type}' to topic '{topic}'")
        except Exception as e:
            logger.error(f"Error producing message to Kafka: {str(e)}")
            raise

    def flush(self, timeout: float = 10.0) -> int:
        """
        Wait for all messages in the producer queue to be delivered.

        Args:
            timeout: Maximum time to block in seconds

        Returns:
            Number of messages still in the queue after timeout
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush timeout")
        return remaining
