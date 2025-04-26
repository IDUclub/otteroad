from idu_kafka_client.consumer import BaseMessageHandler

from models import UserCreatedEvent


class UserCreatedHandler(BaseMessageHandler[UserCreatedEvent]):
    async def handle(self, event, ctx):
        print(f"[Kafka] User created: {event.user_id}")
