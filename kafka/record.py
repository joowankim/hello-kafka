import base64
import json
from typing import Self

from pydantic import BaseModel, Field

from kafka import message


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

    @classmethod
    def from_message(cls, msg: message.Message) -> Self:
        if msg.headers.api_key != message.MessageType.PRODUCE:
            raise ValueError("Message is not of type PRODUCE")
        params = json.loads(msg.payload.decode("utf-8"))
        if (value := params.get("value")) is None:
            raise ValueError("ProducerRecord must have a 'value' field")
        params["value"] = base64.b64decode(value)
        if (key := params.get("key")) is not None:
            params["key"] = base64.b64decode(key)
        return cls.model_validate(params)


class ConsumerRecord(BaseModel):
    topic: str
    value: bytes
    key: bytes | None
    partition: int
    timestamp: int
    headers: dict[str, bytes]
    offset: int
