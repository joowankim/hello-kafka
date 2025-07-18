import base64
from pathlib import Path
from typing import Self

from pydantic import BaseModel

from kafka import constants
from kafka.error import InvalidOffsetError


class Record(BaseModel):
    topic: str
    partition: int
    value: bytes
    key: bytes | None
    timestamp: int
    headers: dict[str, bytes]
    offset: int | None

    @property
    def partition_dirname(self) -> Path:
        return Path(f"{self.topic}-{self.partition}")

    @property
    def bin(self) -> bytes:
        if self.offset is None:
            raise InvalidOffsetError(
                "Offset must be set before converting to binary format"
            )
        encoded_value = base64.b64encode(self.value)
        encoded_key = self.key and base64.b64encode(self.key)
        encoded_headers = {k: base64.b64encode(v) for k, v in self.headers.items()}
        encoded_self = self.model_copy(
            deep=True,
            update={
                "value": encoded_value,
                "key": encoded_key,
                "headers": encoded_headers,
            },
        )
        data = encoded_self.model_dump_json(exclude={"topic", "partition"})
        return f"{len(data):0{constants.PAYLOAD_LENGTH_WIDTH}d}{data}".encode("utf-8")

    def record_at(self, offset: int) -> Self:
        if self.offset is not None:
            raise InvalidOffsetError(
                "Offset must be set before converting to binary format"
            )
        return self.model_copy(deep=True, update={"offset": offset})
