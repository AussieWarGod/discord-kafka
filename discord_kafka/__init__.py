"""
Discord-Kafka - A library extending discord.py to integrate with Kafka for microservice communication.

This library provides tools to publish Discord events to Kafka topics,
and utilities to help with inter-bot communication in a microservice architecture.
"""

__version__ = "0.1.0"

from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
from discord_kafka.core.producer import KafkaProducer
from discord_kafka.core.consumer import KafkaConsumer
from discord_kafka.utils.serializers import EventSerializer

__all__ = [
    "KafkaBot",
    "KafkaConfig",
    "KafkaProducer",
    "KafkaConsumer",
    "EventSerializer",
]
