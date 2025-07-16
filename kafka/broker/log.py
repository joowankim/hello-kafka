from pathlib import Path

from pydantic import BaseModel


class Segment(BaseModel):
    topic: str
    partition: int
    value: bytes

    @property
    def partition_dirname(self) -> Path:
        return Path(f"{self.topic}-{self.partition}")
