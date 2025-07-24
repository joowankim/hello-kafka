from pathlib import Path
from typing import Self

import pydantic
from pydantic import Field

from kafka import constants
from kafka.broker import command
from kafka.error import InvalidOffsetError


class Record(pydantic.BaseModel):
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


class Segment(pydantic.BaseModel):
    base_offset: int

    @property
    def log(self) -> str:
        return f"{self.base_offset:0{constants.LOG_FILENAME_LENGTH}d}.log"

    @property
    def index(self) -> str:
        return f"{self.base_offset:0{constants.LOG_FILENAME_LENGTH}d}.index"


class Partition(pydantic.BaseModel):
    topic: str
    num: int
    segments: list[Segment] = Field(min_length=1)
    leo: int

    @property
    def name(self) -> str:
        return f"{self.topic}-{self.num}"
