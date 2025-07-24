from typing import Self

import pydantic

from kafka import message


class Fetch(pydantic.BaseModel):
    topic: str
    partition: int
    offset: int
    max_bytes: int

    @property
    def partition_dirname(self) -> str:
        return f"{self.topic}-{self.partition}"

    @classmethod
    def from_message(cls, msg: message.Message) -> Self:
        if msg.headers.api_key != message.MessageType.FETCH:
            raise ValueError("Message is not of type FETCH")
        return cls.model_validate_json(msg.payload.decode("utf-8"))
