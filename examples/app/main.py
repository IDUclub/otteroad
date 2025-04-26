from contextlib import asynccontextmanager

from fastapi import FastAPI

from handlers import UserCreatedHandler
from idu_kafka_client import (
    KafkaConsumerService,
    KafkaConsumerSettings,
    KafkaProducerClient,
    KafkaProducerSettings,
)

# Initialize FastAPI app
app = FastAPI()

# Global Kafka settings
consumer_settings = KafkaConsumerSettings.from_env()
producer_settings = KafkaProducerSettings.from_env()

# Initialize consumer service and producer client
consumer_service = KafkaConsumerService(consumer_settings)
producer: KafkaProducerClient = KafkaProducerClient(producer_settings)


@asynccontextmanager
async def lifespan(application: FastAPI):
    # start
    consumer_service.register_handler(UserCreatedHandler())
    await consumer_service.add_worker(topics=["users.events"]).start()
    await producer.start()

    yield

    # stop
    await consumer_service.stop()
    await producer.close()
