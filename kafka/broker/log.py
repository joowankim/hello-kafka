from pathlib import Path
from typing import Self

from pydantic import BaseModel

from kafka import constants
from kafka.broker import command
from kafka.error import InvalidOffsetError


class Record(BaseModel):
    topic: str
    partition: int
    value: str
    key: str | None
    timestamp: int
    headers: dict[str, str]
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
        data = self.model_dump_json(exclude={"topic", "partition"})
        return f"{len(data):0{constants.PAYLOAD_LENGTH_WIDTH}d}{data}".encode("utf-8")

    @property
    def size(self) -> int:
        return len(self.bin[constants.PAYLOAD_LENGTH_WIDTH :])

    @classmethod
    def from_produce_command(cls, cmd: command.Produce) -> list[Self]:
        return [
            cls(
                topic=cmd.topic,
                partition=cmd.partition,
                value=record.value,
                key=record.key,
                timestamp=record.timestamp,
                headers=record.headers,
                offset=None,
            )
            for record in cmd.records
        ]

    def record_at(self, offset: int) -> Self:
        if self.offset is not None:
            raise InvalidOffsetError(
                "Offset is already set, cannot create a new record at a different offset"
            )
        return self.model_copy(deep=True, update={"offset": offset})
