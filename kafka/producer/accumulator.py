import asyncio

from kafka import record, broker


class RecordAccumulator:
    def __init__(self):
        self.records: dict[
            tuple[str, int],
            list[tuple[broker.RecordContents, asyncio.Future[record.RecordMetadata]]],
        ] = {}

    def add(
        self,
        rec: record.ProducerRecord,
        future: asyncio.Future[record.RecordMetadata],
    ) -> None:
        key = (rec.topic, rec.partition)
        if key not in self.records:
            self.records[key] = []
        self.records[key].extend(
            [
                (
                    broker.RecordContents(
                        value=rec.value,
                        key=rec.key,
                        timestamp=rec.timestamp,
                        headers=rec.headers,
                    ),
                    future,
                )
            ]
        )
