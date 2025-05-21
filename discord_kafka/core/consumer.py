"""
Kafka consumer module for processing Discord events from Kafka topics.

This module provides functionality to consume and process Discord events
from configured Kafka topics.
"""

import json
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Union, Callable, Set

from confluent_kafka import Consumer, KafkaError, KafkaException

from discord_kafka.core.config import KafkaConfig
from discord_kafka.utils.serializers import EventSerializer


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Manages the consumption of Discord events from Kafka topics.

    This class wraps the confluent_kafka.Consumer to provide a simple interface
    for consuming Discord events from Kafka topics.

    Attributes:
        config (KafkaConfig): Configuration for Kafka connection and topics
        consumer (Consumer): Underlying confluent_kafka.Consumer instance
        serializer (EventSerializer): Deserializer for Discord objects
        event_handlers (Dict[str, List[Callable]]): Event type to handler function mappings
        is_running (bool): Flag indicating if the consumer is currently running
    """

    def __init__(
        self,
        config: KafkaConfig,
        serializer: Optional[EventSerializer] = None,
        topics: Optional[List[str]] = None,
    ) -> None:
        """
        Initialize a KafkaConsumer with the given configuration.

        Args:
            config: KafkaConfig instance with connection details and topic mappings
            serializer: Optional custom serializer for Discord objects
            topics: Optional list of topic names to subscribe to (defaults to all topics defined in config)
        """
        self.config = config
        self.consumer = Consumer(config.get_consumer_config())
        self.serializer = serializer or EventSerializer()
        self.event_handlers: Dict[str, List[Callable]] = {}
        self.is_running = False
        self._consumer_thread: Optional[threading.Thread] = None

        # Subscribe to topics
        if topics:
            self.subscribe(topics)
        else:
            # Subscribe to all topics defined in the config
            self.subscribe(list(set(config.topics.values())))

        logger.info(
            f"Initialized Kafka consumer with bootstrap servers: {config.bootstrap_servers}"
        )

    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribe to Kafka topics.

        Args:
            topics: List of topic names to subscribe to
        """
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to topics: {topics}")

    def register_handler(self, event_type: str, handler: Callable) -> None:
        """
        Register a handler function for a specific event type.

        Args:
            event_type: Type of event to handle (e.g., 'message', 'reaction')
            handler: Function to call when an event of this type is received
        """
        if event_type not in self.event_handlers:
            self.event_handlers[event_type] = []
        self.event_handlers[event_type].append(handler)
        logger.debug(f"Registered handler for event type '{event_type}'")

    def start(self, polling_interval: float = 0.1) -> None:
        """
        Start consuming messages in a background thread.

        Args:
            polling_interval: Time to wait between polls in seconds
        """
        if self.is_running:
            logger.warning("Consumer is already running")
            return

        self.is_running = True
        self._consumer_thread = threading.Thread(
            target=self._consume_loop, args=(polling_interval,), daemon=True
        )
        self._consumer_thread.start()
        logger.info("Started Kafka consumer thread")

    def stop(self) -> None:
        """
        Stop the consumer thread.
        """
        if not self.is_running:
            logger.warning("Consumer is not running")
            return

        self.is_running = False
        if self._consumer_thread:
            self._consumer_thread.join(timeout=5.0)
            if self._consumer_thread.is_alive():
                logger.warning("Consumer thread did not terminate gracefully")

        self.consumer.close()
        logger.info("Stopped Kafka consumer")

    def _consume_loop(self, polling_interval: float) -> None:
        """
        Main loop for consuming messages.

        Args:
            polling_interval: Time to wait between polls in seconds
        """
        try:
            while self.is_running:
                msg = self.consumer.poll(timeout=polling_interval)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        logger.debug(f"Reached end of {msg.topic()}/{msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                else:
                    try:
                        self._handle_message(msg)
                    except Exception as e:
                        logger.exception(f"Error handling message: {str(e)}")
        except KafkaException as e:
            logger.error(f"Kafka exception in consumer thread: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error in consumer thread: {str(e)}")
        finally:
            if self.is_running:
                # If we get here while still running, the loop terminated unexpectedly
                self.is_running = False
                logger.error("Consumer loop terminated unexpectedly")

    def _handle_message(self, msg) -> None:
        """
        Process a single message from Kafka.

        Args:
            msg: Message from the Kafka consumer
        """
        # Get the event type from the message headers if available
        event_type = None
        if msg.headers():
            for key, value in msg.headers():
                if key == "event_type":
                    event_type = value.decode("utf-8")
                    break

        # If no event type in headers, try to extract from the topic name
        if not event_type:
            topic_reverse_mapping = {v: k for k, v in self.config.topics.items()}
            event_type = topic_reverse_mapping.get(msg.topic(), "unknown")

        try:
            # Deserialize the message value
            event_data = self.serializer.deserialize(event_type, msg.value())

            # Call the appropriate handlers
            if event_type in self.event_handlers:
                for handler in self.event_handlers[event_type]:
                    handler(event_type, event_data)

            # Call handlers for 'all' events
            if "all" in self.event_handlers:
                for handler in self.event_handlers["all"]:
                    handler(event_type, event_data)

            logger.debug(
                f"Handled message from topic {msg.topic()}, event type {event_type}"
            )

        except Exception as e:
            logger.error(f"Error processing message from {msg.topic()}: {str(e)}")
            # Continue processing other messages
