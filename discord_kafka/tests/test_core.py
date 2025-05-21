"""
Unit tests for the discord-kafka library.

This module contains tests for the core functionality of the discord-kafka library,
focusing on the Kafka integration with discord.py.
"""

import unittest
from unittest import mock
import json
import asyncio
from datetime import datetime

import discord
from confluent_kafka import Producer, Consumer, KafkaError

from discord_kafka.core.config import KafkaConfig
from discord_kafka.core.producer import KafkaProducer
from discord_kafka.core.consumer import KafkaConsumer
from discord_kafka.core.bot import KafkaBot
from discord_kafka.utils.serializers import EventSerializer


class TestKafkaConfig(unittest.TestCase):
    """Tests for the KafkaConfig class."""

    def test_default_config(self):
        """Test that default configurations are set correctly."""
        config = KafkaConfig()
        self.assertEqual(config.bootstrap_servers, "localhost:9092")
        self.assertEqual(config.client_id, "discord-kafka")
        self.assertEqual(config.default_topic, "discord_events")
        self.assertEqual(config.group_id, "discord_bot_group")

    def test_topic_mapping(self):
        """Test that topic mappings work correctly."""
        config = KafkaConfig(
            topics={"message": "custom_messages", "reaction": "custom_reactions"}
        )

        self.assertEqual(config.get_topic_for_event("message"), "custom_messages")
        self.assertEqual(config.get_topic_for_event("reaction"), "custom_reactions")
        self.assertEqual(config.get_topic_for_event("unknown"), "discord_events")

    def test_producer_config(self):
        """Test that producer config is generated correctly."""
        config = KafkaConfig(
            bootstrap_servers="test:9092",
            client_id="test-client",
            producer_config={"compression.type": "lz4"},
        )

        producer_config = config.get_producer_config()
        self.assertEqual(producer_config["bootstrap.servers"], "test:9092")
        self.assertEqual(producer_config["client.id"], "test-client-producer")
        self.assertEqual(producer_config["compression.type"], "lz4")

    def test_consumer_config(self):
        """Test that consumer config is generated correctly."""
        config = KafkaConfig(
            bootstrap_servers="test:9092",
            client_id="test-client",
            group_id="test-group",
            consumer_config={"auto.offset.reset": "latest"},
        )

        consumer_config = config.get_consumer_config()
        self.assertEqual(consumer_config["bootstrap.servers"], "test:9092")
        self.assertEqual(consumer_config["client.id"], "test-client-consumer")
        self.assertEqual(consumer_config["group.id"], "test-group")
        self.assertEqual(consumer_config["auto.offset.reset"], "latest")


class TestEventSerializer(unittest.TestCase):
    """Tests for the EventSerializer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.serializer = EventSerializer()

    def test_serialize_simple_types(self):
        """Test serialization of simple Python types."""
        # Test string
        result = self.serializer._serialize_object("test")
        self.assertEqual(result, "test")

        # Test integer
        result = self.serializer._serialize_object(42)
        self.assertEqual(result, 42)

        # Test list
        result = self.serializer._serialize_object([1, 2, 3])
        self.assertEqual(result, [1, 2, 3])

        # Test dict
        result = self.serializer._serialize_object({"key": "value"})
        self.assertEqual(result, {"key": "value"})

    def test_serialize_datetime(self):
        """Test serialization of datetime objects."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        result = self.serializer._serialize_object(dt)
        self.assertEqual(result, "2023-01-01T12:00:00")

    def test_serialize_discord_user_mock(self):
        """Test serialization of a mocked Discord User object."""
        mock_user = mock.Mock(spec=discord.User)
        mock_user.id = 123456789
        mock_user.name = "TestUser"
        mock_user.discriminator = "1234"
        mock_user.display_name = "Test User"
        mock_user.bot = False
        mock_user.avatar = mock.Mock()
        mock_user.avatar.url = "https://example.com/avatar.png"

        result = self.serializer._serialize_user(mock_user)

        self.assertEqual(result["id"], 123456789)
        self.assertEqual(result["name"], "TestUser")
        self.assertEqual(result["discriminator"], "1234")
        self.assertEqual(result["display_name"], "Test User")
        self.assertEqual(result["bot"], False)
        self.assertEqual(result["avatar_url"], "https://example.com/avatar.png")

    def test_deserialize(self):
        """Test deserialization of JSON data."""
        # Create test data
        test_data = {
            "id": 123456789,
            "content": "Test message",
            "_kafka_meta": {
                "event_type": "message",
                "timestamp": "2023-01-01T12:00:00",
            },
        }
        json_data = json.dumps(test_data).encode("utf-8")

        # Deserialize
        result = self.serializer.deserialize("message", json_data)

        # Check result
        self.assertEqual(result["id"], 123456789)
        self.assertEqual(result["content"], "Test message")
        self.assertEqual(result["_kafka_meta"]["event_type"], "message")


@mock.patch("discord_kafka.core.producer.Producer")
class TestKafkaProducer(unittest.TestCase):
    """Tests for the KafkaProducer class."""

    def setUp(self):
        """Set up test fixtures."""
        self.config = KafkaConfig(
            bootstrap_servers="test:9092", client_id="test-client"
        )
        self.serializer = mock.Mock(spec=EventSerializer)

        # Set up serializer mock to return a simple JSON string
        self.serializer.serialize.return_value = b'{"test": "data"}'

    def test_init(self, mock_producer_class):
        """Test initialization of KafkaProducer."""
        producer = KafkaProducer(self.config, self.serializer)

        # Check that the Producer was initialized with the correct config
        mock_producer_class.assert_called_once_with(self.config.get_producer_config())

        # Check that the serializer was set
        self.assertEqual(producer.serializer, self.serializer)

    def test_produce(self, mock_producer_class):
        """Test producing a message to Kafka."""
        # Create a mock producer instance
        mock_producer_instance = mock_producer_class.return_value

        # Create a producer
        producer = KafkaProducer(self.config, self.serializer)

        # Produce a message
        test_value = "test_value"
        test_key = "test_key"
        test_headers = {"header1": "value1"}

        producer.produce("message", test_value, key=test_key, headers=test_headers)

        # Check that the serializer was called
        self.serializer.serialize.assert_called_once_with("message", test_value)

        # Check that produce was called on the underlying producer
        mock_producer_instance.produce.assert_called_once()
        call_args = mock_producer_instance.produce.call_args[1]

        self.assertEqual(call_args["topic"], "discord_messages")
        self.assertEqual(call_args["value"], b'{"test": "data"}')
        self.assertEqual(call_args["key"], b"test_key")

        # Check that poll was called
        mock_producer_instance.poll.assert_called_once_with(0)


if __name__ == "__main__":
    unittest.main()
