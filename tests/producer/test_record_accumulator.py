import asyncio

import pytest

from kafka.producer.accumulator import RecordAccumulator
from kafka.producer.command import RecordContents
from kafka.record import RecordMetadata


@pytest.fixture
def accumulator() -> RecordAccumulator:
    return RecordAccumulator()


@pytest.fixture
def topic() -> str:
    return "test-topic"


@pytest.fixture
def partition() -> int:
    return 0


@pytest.fixture
def contents(base_record_contents: RecordContents) -> RecordContents:
    return base_record_contents.model_copy(update=dict(value=b"test-value", key=None))


@pytest.fixture
def future() -> asyncio.Future[RecordMetadata]:
    return asyncio.Future()


def test_add_new_record(
    accumulator: RecordAccumulator,
    topic: str,
    partition: int,
    contents: RecordContents,
    future: asyncio.Future[RecordMetadata],
) -> None:
    accumulator.add(topic, partition, contents, future)

    assert (topic, partition) in accumulator.records
    assert accumulator.records[(topic, partition)] == (contents, future)


def test_add_record_to_existing_topic_partition_raises_error(
    accumulator: RecordAccumulator,
    topic: str,
    partition: int,
    contents: RecordContents,
    future: asyncio.Future[RecordMetadata],
) -> None:
    accumulator.add(topic, partition, contents, future)

    with pytest.raises(ValueError):
        accumulator.add(topic, partition, contents, future)
