from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class SocGroupsUpdated(AvroEventModel):
    """Model for message indicates that a social group has been updated."""

    topic: ClassVar[str] = "urban.events"
    namespace: ClassVar[str] = "soc_groups"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    soc_group_id: int = Field(..., description="updated social group identifier")
