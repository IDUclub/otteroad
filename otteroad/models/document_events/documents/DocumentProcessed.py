from typing import ClassVar

from pydantic import Field

from otteroad.avro import AvroEventModel


class DocumentProcessed(AvroEventModel):
    """Model for message indicates that a document has been fully processed
    and stored in the vector database (IDU_DVD)."""

    topic: ClassVar[str] = "document.events"
    namespace: ClassVar[str] = "documents"
    schema_version: ClassVar[int] = 1
    schema_compatibility: ClassVar[str] = "BACKWARD"

    document_name: str = Field(
        ...,
        description="unique document name (registry key), enough to fetch all "
        "fragments and versions of the document from the DVD API",
    )
