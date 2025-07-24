import abc
import re
from pathlib import Path
from typing import Self, ClassVar

import pydantic

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


class Segment(abc.ABC, pydantic.BaseModel):
    log_path: Path
    index_path: Path
    size_limit: int

    dirname_pattern: ClassVar[re.Pattern] = re.compile(
        r"^(?P<topic>.+)-(?P<partition>\d+)$"
    )

    @property
    def topic(self) -> str:
        dirname = self.log_path.parent.stem
        match = self.dirname_pattern.match(dirname)
        return match.group("topic")

    @property
    def partition(self) -> int:
        dirname = self.log_path.parent.stem
        match = self.dirname_pattern.match(dirname)
        return int(match.group("partition"))

    @abc.abstractmethod
    def append(self, record: Record) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def read(self, start_offset: int, max_bytes: int) -> list[Record]:
        raise NotImplementedError
