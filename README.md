# Discord-Kafka

A Python library that extends discord.py to integrate with Apache Kafka, enabling microservice communication patterns for Discord bots. This library facilitates building scalable, resilient, and maintainable Discord bot systems through a microservice architecture.

## Features

- Extends discord.py to automatically publish Discord events to Kafka topics
- Provides consumers for easily processing events from Kafka topics
- Includes helper functions for inter-bot communication
- Event handlers that simplify processing Discord events
- Serialization utilities for Discord objects
- Support for building microservice architectures
- Easy-to-use API designed for seamless integration

## Why Use Discord-Kafka?

Traditional Discord bots suffer from several limitations:

1. **Single Point of Failure**: If a bot crashes, all functionality is lost until it restarts
2. **Scaling Challenges**: As bots grow in complexity, they become harder to maintain
3. **Limited Fault Isolation**: Issues in one feature can affect the entire bot
4. **Deployment Complexity**: Updates require restarting the entire bot

Discord-Kafka solves these problems by enabling a microservice architecture:

1. **Resilience**: If one bot service fails, others continue operating
2. **Modularity**: Each bot service can be developed, deployed, and maintained independently
3. **Scalability**: Services can be scaled horizontally based on load
4. **Flexibility**: Different teams can work on different services using their preferred languages/frameworks
5. **Event-Driven**: All Discord events are published to Kafka for any service to consume

## Installation

```bash
# Clone the repository
git clone https://github.com/AussieWarGod/discord-kafka.git
cd discord-kafka

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
pip install -e .
```

## Quick Start

### Basic Usage

```python
from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
import discord
import os

# Configure Kafka connection
kafka_config = KafkaConfig(
    bootstrap_servers='localhost:9092',
    topics={
        'message': 'discord_messages',
        'reaction': 'discord_reactions',
        'member': 'discord_members'
    }
)

# Configure intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

# Create a bot with Kafka integration
bot = KafkaBot(
    command_prefix='!',
    kafka_config=kafka_config,
    intents=intents,
    enable_consumer=True  # Enable consuming messages from Kafka
)

# Regular discord.py functionality works as usual
@bot.command()
async def ping(ctx):
    await ctx.send('Pong!')

# Run the bot
bot.run(os.environ.get('DISCORD_TOKEN'))
```

### Using Event Handlers

```python
from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
from discord_kafka.handlers.event_handlers import MessageHandler, ReactionHandler
import discord
import os

# Configure Kafka
kafka_config = KafkaConfig(bootstrap_servers='localhost:9092')
bot = KafkaBot(command_prefix='!', kafka_config=kafka_config)

# Register event handlers
message_handler = MessageHandler(
    bot,
    bot.producer,
    # Optional: filter messages
    ignored_authors=[123456789],
    allowed_channels=[987654321]
)

reaction_handler = ReactionHandler(bot, bot.producer)

# Run the bot
bot.run(os.environ.get('DISCORD_TOKEN'))
```

### Inter-Bot Communication

```python
from discord_kafka.core.bot import KafkaBot
from discord_kafka.core.config import KafkaConfig
from discord_kafka.utils.communication import BotCommunicator, register_command_handler
import discord
import os

# Configure Kafka
kafka_config = KafkaConfig(bootstrap_servers='localhost:9092')
bot = KafkaBot(command_prefix='!', kafka_config=kafka_config, enable_consumer=True)

# Create a communicator for inter-bot messaging
communicator = BotCommunicator(bot.producer, "my-bot")

# Register the communicator with the Kafka consumer
bot.register_kafka_handler('bot_message', communicator.handle_message)

# Handle command from another bot
def handle_ping_command(args, correlation_id, sender_id, communicator):
    print(f"Received ping from {sender_id} with args: {args}")
    
    # Send a response
    communicator.send_response(
        correlation_id=correlation_id,
        response_data={"message": "Pong!"},
        success=True
    )

# Register the command handler
register_command_handler(communicator, "ping", handle_ping_command)

# Discord command that sends a command to another bot
@bot.command()
async def pingbot(ctx, bot_name: str):
    """Send a ping command to another bot."""
    correlation_id = communicator.send_command(
        target_bot=bot_name,
        command_name="ping",
        command_args={"timestamp": ctx.message.created_at.isoformat()},
        reply_to="responses"
    )
    await ctx.send(f"Ping sent to {bot_name} (ID: {correlation_id})")

# Start the Kafka consumer
bot.start_consumer()

# Run the bot
bot.run(os.environ.get('DISCORD_TOKEN'))
```

## Microservice Architecture Example

Check out `/discord_kafka/examples/microservices.py` for a complete example of a microservice architecture with three specialized bots:

1. **Moderation Bot**: Handles moderation commands and publishes moderation events
2. **Logging Bot**: Consumes events from all bots and logs them to a channel
3. **Welcome Bot**: Handles member join events and sends welcome messages

## Documentation

The library is extensively documented with inline comments and docstrings. For examples, see the `/discord_kafka/examples` directory.

### Core Components

- `KafkaBot`: Main bot class that extends discord.py's Bot with Kafka integration
- `KafkaConfig`: Configuration for Kafka connection and topic mappings
- `KafkaProducer`: Handles publishing Discord events to Kafka
- `KafkaConsumer`: Handles consuming events from Kafka

### Event Handlers

- `MessageHandler`: Handles Discord message events
- `ReactionHandler`: Handles Discord reaction events
- `MemberHandler`: Handles Discord member join/leave events

### Utilities

- `BotCommunicator`: Facilitates communication between bot instances
- `EventSerializer`: Handles serialization of Discord objects
- Various helper functions for Discord operations

## Requirements

- Python 3.8+
- Discord.py 2.0+
- Confluent-Kafka 2.0+
- A running Kafka cluster

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Commit your changes: `git commit -am 'Add feature'`
4. Push to the branch: `git push origin feature-name`
5. Submit a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.