# 🦦 otteroad

[![code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI version](https://badge.fury.io/py/otteroad.svg)](https://pypi.org/project/otteroad/)
[![CI](https://github.com/Jesusya-26/otteroad/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/otteroad/actions)
[![codecov](https://codecov.io/gh/Jesusya-26/otteroad/branch/main/graph/badge.svg)](https://codecov.io/gh/your-org/otteroad)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE)

![logo](https://drive.google.com/uc?export=view&id=1DuMT0nrzqfRr3q5kUeki_6q6eI1gs-5m)

> Kafka framework for FastAPI microservices in the IDU ([Institute of Design and Urban Studies](https://idu.itmo.ru/en)).
>
> _** The name was inspired by this [text](https://habr.com/ru/articles/563582/)._
---

## ✨ Overview

`otteroad` is a Kafka framework designed for FastAPI-based microservices. It simplifies integration with Apache Kafka and supports:

- ✅ Unified consumer & producer APIs
- ✅ AVRO + Schema Registry support via Pydantic
- ✅ Pluggable settings from `.env`, `.yaml`, or custom config
- ✅ Structured event handlers with lifecycle hooks
- ✅ Flexible handler registry and extensible processing pipeline
- ✅ Designed for FastAPI services but works standalone

---

## 📦 Installation

```bash
pip install otteroad
```

Or via poetry:

```bash
poetry add otteroad
```

---

## ⚙️ Configuration

Kafka settings are defined via two classes:

- `KafkaConsumerSettings`
- `KafkaProducerSettings`

They can be created from any source:

```python
from otteroad import KafkaConsumerSettings, KafkaProducerSettings

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
from pydantic import Field
from otteroad.avro import AvroEventModel


class TerritoryCreated(AvroEventModel):
    """Model for message indicates that a territory has been created."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "territories"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    territory_id: int = Field(..., description="new territory identifier")
```

---

## 🧠 Handlers

Handlers process typed events. Extend `BaseMessageHandler` and implement core logic in `handle()`. Optional hooks: `pre_process`, `post_process`, `on_startup`, `on_shutdown`, `handle_error`.

> ℹ️ **Note for IDU services:** It is strongly recommended to use only models from the `models/` directory to ensure schema consistency and maintainability across services.

```python
from otteroad.consumer import BaseMessageHandler
from otteroad.models import TerritoryCreated  # please, use only models from the models/ directory

class TerritoryCreatedHandler(BaseMessageHandler[TerritoryCreated]):
    async def handle(self, event, ctx):
        print(f"Territory created: {event.territory_id}")
        
    async def on_startup(self): ...
    
    async def on_shutdown(self): ...
```

---

## 🔄 Consumer

`KafkaConsumerService` manages lifecycle and worker threads; `KafkaConsumerWorker` pulls messages, resolves handlers and runs processing logic.

```python
from otteroad import KafkaConsumerService

service = KafkaConsumerService(consumer_settings)
service.register_handler(TerritoryCreatedHandler())
service.add_worker(topics=["urban.events"]).start()
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
from otteroad import KafkaProducerClient
from otteroad.models import TerritoryCreated

async def send_event():
    async with KafkaProducerClient(producer_settings) as producer:
        event = TerritoryCreated(territory_id=1)
        await producer.send(event)
```

---

### 🧩 FastAPI Integration

For a simple integration example with FastAPI, see:

📄 [`Example`](examples/app/)
