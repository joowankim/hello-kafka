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
    payload_length: int


class Message(BaseModel):
    headers: MessageHeaders
    payload: bytes
