import asyncio
from typing import Any
from unittest import mock

import pytest

from kafka.producer.accumulator import RecordAccumulator
from kafka.broker.command import RecordContents
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
