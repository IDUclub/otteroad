# 🦉 idu_kafka_client

[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI version](https://badge.fury.io/py/idu-kafka-client.svg)](https://pypi.org/project/idu-kafka-client/)
[![CI](https://github.com/Jesusya-26/idu_kafka_client/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/idu_kafka_client/actions)
[![codecov](https://codecov.io/gh/Jesusya-26/idu_kafka_client/branch/main/graph/badge.svg)](https://codecov.io/gh/your-org/idu_kafka_client)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

> Kafka framework for FastAPI microservices in the IDU (Institute of Design and Urban Studies) project.

---

## ✨ Overview

`idu_kafka_client` is a Kafka framework designed for FastAPI-based microservices. It simplifies integration with Apache Kafka and supports:

- ✅ Unified consumer & producer APIs
- ✅ AVRO + Schema Registry support via Pydantic
- ✅ Pluggable settings from `.env`, `.yaml`, or custom config
- ✅ Structured event handlers with lifecycle hooks
- ✅ Flexible handler registry and extensible processing pipeline
- ✅ Designed for FastAPI services but works standalone

---

## 📦 Installation

```bash
pip install idu-kafka-client
```

Or via poetry:

```bash
poetry add idu-kafka-client
```

---

## ⚙️ Configuration

Kafka settings are defined via two classes:

- `KafkaConsumerSettings`
- `KafkaProducerSettings`

They can be created from any source:

```python
from idu_kafka_client import KafkaConsumerSettings, KafkaProducerSettings

consumer_settings = KafkaConsumerSettings.from_env()
producer_settings = KafkaProducerSettings.from_yaml("config/kafka.yaml")

# define pydantic model/dataclass/dict/etc.
config = {"bootstrap.servers": "localhost: 9092"}
settings = KafkaProducerSettings.from_custom_config(config)
```

---

## 📡 Event Models (AVRO + Schema Registry)

Use `AvroEventModel` as the base for your event schemas. These are strict, typed messages validated via Pydantic.

```python
from typing import ClassVar
from idu_kafka_client.avro import AvroEventModel

class UserCreatedEvent(AvroEventModel):
    topic: ClassVar[str] = "users.created"
    schema_subject: ClassVar[str] = "users.created-value"

    user_id: str
    name: str
```

---

## 🧠 Handlers

Handlers process typed events. Extend `BaseMessageHandler` and implement core logic in `handle()`. Optional hooks: `pre_process`, `post_process`, `on_startup`, `on_shutdown`, `handle_error`.

```python
from idu_kafka_client.consumer import BaseMessageHandler
from idu_kafka_client.models import UserCreatedEvent

class UserCreatedHandler(BaseMessageHandler[UserCreatedEvent]):
    async def handle(self, event, ctx):
        print(f"User created: {event.user_id}")
```

---

## 🔄 Consumer

`KafkaConsumerService` manages lifecycle and worker threads; `KafkaConsumerWorker` pulls messages, resolves handlers and runs processing logic.

```python
from idu_kafka_client import KafkaConsumerService

service = KafkaConsumerService(consumer_settings)
service.register_handler(UserCreatedHandler())
service.add_worker(topics=["users.events"]).start()
```

Under the hood, the pipeline is:

```text
receive message -> validate -> pre_process -> handle -> post_process
```

If an error occurs, custom error handling or DQL logic can be added.

---

## 🚀 Producer

Use `KafkaProducerClient` to send strongly typed Avro events:

```python
from idu_kafka_client import KafkaProducerClient
from idu_kafka_client.models import UserCreatedEvent

async def send_event():
    async with KafkaProducerClient(producer_settings) as producer:
        event = UserCreatedEvent(user_id="123", name="Alice")
        await producer.send(event)
```

---

### 🧩 FastAPI Integration

For a simple integration example with FastAPI, see:

📄 [`Example`](examples/app/)
