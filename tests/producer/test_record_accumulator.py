import asyncio
from typing import Any
from unittest import mock

import pytest

from kafka.producer.accumulator import RecordAccumulator
from kafka.broker.command import RecordContents, Produce
from kafka.record import ProducerRecord


@pytest.fixture
def record_accumulator(request: pytest.FixtureRequest) -> RecordAccumulator:
    records: dict[tuple[str, int], list[tuple[RecordContents, asyncio.Future]]] = (
        request.param
    )
    accumulator = RecordAccumulator()
    accumulator.records |= records
    return accumulator


@pytest.fixture
def producer_record(
    base_producer_record: ProducerRecord, request: pytest.FixtureRequest
) -> ProducerRecord:
    record: dict[str, Any] = request.param
    return base_producer_record.model_copy(update=record)


@pytest.fixture
def contents(base_record_contents: RecordContents) -> RecordContents:
    return base_record_contents.model_copy(update=dict(value="test-value", key=None))


@pytest.mark.parametrize(
    "record_accumulator, producer_record, expected",
    [
        (
            {},
            dict(
                topic="test-topic",
                partition=0,
                key=None,
                value="test-value",
                timestamp=None,
                headers={},
            ),
            {
                ("test-topic", 0): [
                    (
                        RecordContents(
                            value="test-value", key=None, timestamp=None, headers={}
                        ),
                        mock.ANY,
                    ),
                ],
            },
        ),
        (
            {
                ("test-topic", 0): [
                    (
                        RecordContents(
                            value="test-value", key=None, timestamp=None, headers={}
                        ),
                        mock.ANY,
                    ),
                ],
            },
            dict(
                topic="test-topic",
                partition=0,
                key=None,
                value="test-value",
                timestamp=None,
                headers={},
            ),
            {
                ("test-topic", 0): [
                    (
                        RecordContents(
                            value="test-value", key=None, timestamp=None, headers={}
                        ),
                        mock.ANY,
                    ),
                    (
                        RecordContents(
                            value="test-value", key=None, timestamp=None, headers={}
                        ),
                        mock.ANY,
                    ),
                ],
            },
        ),
    ],
    indirect=["record_accumulator", "producer_record"],
)
def test_add(
    record_accumulator: RecordAccumulator,
    producer_record: ProducerRecord,
    expected: dict[tuple[str, int], list[tuple[RecordContents, asyncio.Future]]],
) -> None:
    future = asyncio.Future()

    record_accumulator.add(rec=producer_record, future=future)

    assert record_accumulator.records == expected


@pytest.fixture
def expected_produces(
    base_produce: Produce,
    base_record_contents: RecordContents,
    request: pytest.FixtureRequest,
) -> list[Produce]:
    produces: list[dict[str, Any]] = request.param
    return [
        base_produce.model_copy(
            update=dict(
                topic=produce["topic"],
                partition=produce["partition"],
                records=produce["records"],
            )
        )
        for produce in produces
    ]


@pytest.mark.parametrize(
    "record_accumulator, size, expected_produces",
    [
        ({}, 100, []),
        (
            {
                ("test-topic", 0): [
                    (
                        RecordContents(
                            value="short", key=None, timestamp=None, headers={}
                        ),
                        asyncio.Future(),
                    ),
                ],
            },
            1000,
            [],
        ),
        (
            {
                ("test-topic", 0): [
                    (
                        RecordContents(
                            value="this is a long message that should meet the size requirement",
                            key=None,
                            timestamp=None,
                            headers={},
                        ),
                        asyncio.Future(),
                    ),
                    (
                        RecordContents(
                            value="short", key=None, timestamp=None, headers={}
                        ),
                        asyncio.Future(),
                    ),
                ],
            },
            100,
            [
                dict(
                    topic="test-topic",
                    partition=0,
                    records=[
                        RecordContents(
                            value="this is a long message that should meet the size requirement",
                            key=None,
                            timestamp=None,
                            headers={},
                        ),
                        RecordContents(
                            value="short", key=None, timestamp=None, headers={}
                        ),
                    ],
                )
            ],
        ),
    ],
    indirect=["record_accumulator", "expected_produces"],
)
def test_ready_batches(
    record_accumulator: RecordAccumulator,
    size: int,
    expected_produces: list[Produce],
) -> None:
    produces = record_accumulator.ready_batches(size=size)
    assert [produce for produce, _ in produces] == expected_produces
