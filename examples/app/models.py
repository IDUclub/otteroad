from typing import ClassVar

from idu_kafka_client.avro import AvroEventModel


class UserCreatedEvent(AvroEventModel):
    topic: ClassVar[str] = "users.events"
    schema_subject: ClassVar[str] = "users.created"

    user_id: str
    name: str
