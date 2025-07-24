from pathlib import Path
from typing import Any

import pytest

from kafka.broker.command import Produce, RecordContents
from kafka.broker.log import Record
from kafka.error import InvalidOffsetError


@pytest.fixture
def log_record(base_log_record: Record, request: pytest.FixtureRequest) -> Record:
    topic_name, partition_num, value, key, timestamp, headers, offset = request.param
    return base_log_record.model_copy(
        update=dict(
            topic=topic_name,
            partition=partition_num,
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            offset=offset,
        )
    )


@pytest.mark.parametrize(
    "log_record, expected",
    [
        (("test-topic", 0, b"hello", None, 1752735958, {}, 0), Path("test-topic-0")),
        (
            ("another-topic", 2, b"hello", None, 1752735958, {}, 0),
            Path("another-topic-2"),
        ),
    ],
    indirect=["log_record"],
)
def test_partition_dirname(log_record: Record, expected: Path):
    assert log_record.partition_dirname == expected


@pytest.mark.parametrize(
    "log_record, expected",
    [
        (
            ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, 0),
            (
                b"0086{"
                b'"value":"dGVzdC12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735958,'
                b'"headers":{},'
                b'"offset":0'
                b"}"
            ),
        ),
        (
            ("another-topic", 1, b"YW5vdGhlci12YWx1ZQ==", None, 1752735959, {}, 3),
            (
                b"0090"
                b"{"
                b'"value":"YW5vdGhlci12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735959,'
                b'"headers":{},'
                b'"offset":3'
                b"}"
            ),
        ),
    ],
    indirect=["log_record"],
)
def test_bin(log_record: Record, expected: bytes):
    assert log_record.bin == expected


@pytest.fixture
def recorded(base_log_record: Record, request: pytest.FixtureRequest) -> Record:
    record_params: dict[str, Any] = request.param
    return base_log_record.model_copy(update=record_params)


@pytest.mark.parametrize(
    "log_record, offset, recorded",
    [
        (
            ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, None),
            1,
            dict(
                topic="test-topic",
                partition=0,
                value="dGVzdC12YWx1ZQ==",
                key=None,
                timestamp=1752735958,
                headers={},
                offset=1,
            ),
        ),
    ],
    indirect=["log_record", "recorded"],
)
def test_record_at(log_record: Record, offset: int, recorded: Record):
    result = log_record.record_at(offset)

    assert result == recorded


@pytest.mark.parametrize(
    "log_record",
    [
        ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, 0),
        ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, 1),
    ],
    indirect=True,
)
def test_record_at_with_already_recorded(log_record: Record):
    offset = 1

    with pytest.raises(InvalidOffsetError):
        log_record.record_at(offset)


@pytest.mark.parametrize(
    "log_record",
    [
        ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, None),
        ("another-topic", 1, "YW5vdGhlci12YWx1ZQ==", None, 1752735959, {}, None),
    ],
    indirect=True,
)
def test_convert_to_binary_without_offset(log_record: Record):
    with pytest.raises(InvalidOffsetError):
        log_record.bin


@pytest.fixture
def produce_command(
    base_record_contents: RecordContents,
    base_produce: Produce,
    request: pytest.FixtureRequest,
) -> Produce:
    topic, partition, records = request.param
    return base_produce.model_copy(
        update=dict(
            topic=topic,
            partition=partition,
            records=[
                base_record_contents.model_copy(update=record_params)
                for record_params in records
            ],
        )
    )


@pytest.fixture
def expected_records(
    base_log_record: Record, request: pytest.FixtureRequest
) -> list[Record]:
    records: list[dict[str, Any]] = request.param
    return [
        base_log_record.model_copy(update=record_params) for record_params in records
    ]


@pytest.mark.parametrize(
    "produce_command, expected_records",
    [
        (
            (
                "test-topic",
                0,
                [
                    {
                        "value": "aGVsbG8=",
                        "key": None,
                        "timestamp": 1752735958,
                        "headers": {},
                    },
                    {
                        "value": "aGVsbG8=",
                        "key": None,
                        "timestamp": 1752735958,
                        "headers": {},
                    },
                ],
            ),
            [
                dict(
                    topic="test-topic",
                    partition=0,
                    value="aGVsbG8=",
                    key=None,
                    timestamp=1752735958,
                    headers={},
                    offset=None,
                ),
                dict(
                    topic="test-topic",
                    partition=0,
                    value="aGVsbG8=",
                    key=None,
                    timestamp=1752735958,
                    headers={},
                    offset=None,
                ),
            ],
        ),
    ],
    indirect=["produce_command", "expected_records"],
)
def test_from_produce_command(produce_command: Produce, expected_records: list[Record]):
    records = Record.from_produce_command(produce_command)

    assert records == expected_records


@pytest.mark.parametrize(
    "log_record, position, expected",
    [
        (
            ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}, 0),
            0,
            b"0000000000000000",
        ),
        (
            ("another-topic", 1, "YW5vdGhlci12YWx1ZQ==", None, 1752735959, {}, 3),
            100,
            b"0000000300000100",
        ),
    ],
    indirect=["log_record"],
)
def test_index_entry(log_record: Record, position: int, expected: bytes):
    entry = log_record.index_entry(position)

    assert entry == expected
