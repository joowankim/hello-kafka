from pathlib import Path

import pytest

from kafka.broker.log import Record


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
            ("test-topic", 0, b"test-value", None, 1752735958, {}, 0),
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
            ("another-topic", 1, b"another-value", None, 1752735959, {}, 3),
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


@pytest.mark.parametrize(
    "log_record, expected",
    [
        (("test-topic", 0, b"test-value", None, 1752735958, {}, 0), True),
        (("another-topic", 1, b"another-value", None, 1752735959, {}, 3), True),
        (("test-topic", 2, b"yet-another-value", None, 1752735960, {}, None), False),
    ],
    indirect=["log_record"],
)
def test_is_recorded(log_record: Record, expected: bool):
    assert log_record.is_recorded is expected
