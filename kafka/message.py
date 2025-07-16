import enum
import re
from typing import Self, ClassVar

from pydantic import BaseModel

from kafka import constants
from kafka.error import SerializationError


class MessageType(enum.IntEnum):
    CREATE_TOPICS = 0
    PRODUCE = 1
    FETCH = 2
    OFFSET_COMMIT = 3


class MessageHeaders(BaseModel):
    correlation_id: int
    api_key: MessageType


class Message(BaseModel):
    headers: MessageHeaders
    payload: bytes

    message_pattern: ClassVar[re.Pattern] = re.compile(
        rf"^(?P<correlation_id>\d{{{constants.CORRELATION_ID_WIDTH}}})"
        rf"(?P<api_key>\d{{{constants.API_KEY_WIDTH}}})"
        rf"(?P<payload_length>\d{{{constants.PAYLOAD_LENGTH_WIDTH}}})"
        r"(?P<payload>.*)$"
    )

    @property
    def serialized(self) -> bytes:
        return (
            (f"{self.headers.correlation_id:0{constants.CORRELATION_ID_WIDTH}d}"
            f"{self.headers.api_key:0{constants.API_KEY_WIDTH}d}"
            f"{len(self.payload):0{constants.PAYLOAD_LENGTH_WIDTH}d}").encode("utf-8")
            + self.payload
        )

    @classmethod
    def deserialize(cls, serialized: bytes) -> Self:
        decoded = serialized.decode("utf-8")
        match = cls.message_pattern.match(decoded)
        if not match:
            raise SerializationError("Invalid serialized message format")

        correlation_id = int(match.group("correlation_id"))
        api_key = MessageType(int(match.group("api_key")))
        payload_length = int(match.group("payload_length"))
        payload = match.group("payload")

        if len(payload) != payload_length:
            raise SerializationError("Payload length does not match")

        return cls(
            headers=MessageHeaders(
                correlation_id=correlation_id,
                api_key=api_key,
            ),
            payload=payload.encode("utf-8"),
        )
