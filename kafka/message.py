import enum

from pydantic import BaseModel


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

    @property
    def serialized(self) -> bytes:
        return (
            f"{self.headers.correlation_id:04d}{self.headers.api_key:02d}{len(self.payload):04d}".encode("utf-8")
            + self.payload
        )
