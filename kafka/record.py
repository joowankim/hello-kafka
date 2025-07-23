from pydantic import BaseModel, Field


class RecordMetadata(BaseModel):
    topic: str
    partition: int
    offset: int
    timestamp: int


class ProducerRecord(BaseModel):
    topic: str
    value: bytes
    key: bytes | None = None
    partition: int | None = None
    timestamp: int | None = None
    headers: dict[str, bytes] = Field(default_factory=dict)


class ConsumerRecord(BaseModel):
    topic: str
    value: bytes
    key: bytes | None
    partition: int
    timestamp: int
    headers: dict[str, bytes]
    offset: int
