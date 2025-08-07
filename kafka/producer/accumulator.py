import asyncio

from kafka import record, broker


class RecordAccumulator:
    def __init__(self):
        self.records: dict[
            tuple[str, int],
            list[tuple[broker.RecordContents, asyncio.Future[record.RecordMetadata]]],
        ] = {}

    def ready_batches(
        self, size: int
    ) -> list[tuple[broker.Produce, list[asyncio.Future[record.RecordMetadata]]]]:
        produces = [
            (
                broker.Produce(
                    topic=topic,
                    partition=partition,
                    records=[record_contents for record_contents, _ in records],
                ),
                [future for _, future in records],
            )
            for (topic, partition), records in self.records.items()
        ]
        return [
            (produce, future)
            for produce, future in produces
            if len(produce.serialized) >= size
        ]

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
