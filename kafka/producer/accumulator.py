import asyncio

from kafka import record
from kafka.producer import command


class RecordAccumulator:
    def __init__(self):
        self.records: dict[
            tuple[str, int],
            tuple[command.RecordContents, asyncio.Future[record.RecordMetadata]],
        ] = {}

    def add(
        self,
        topic: str,
        partition: int,
        contents: command.RecordContents,
        future: asyncio.Future[record.RecordMetadata],
    ) -> None:
        key = (topic, partition)
        if key in self.records:
            raise ValueError(
                f"Record for topic {topic} and partition {partition} already exists."
            )
        self.records[key] = (contents, future)
