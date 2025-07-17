from pathlib import Path

import pytest

from kafka.broker.log import Segment


@pytest.fixture
def log_segment(base_log_segment: Segment, request: pytest.FixtureRequest) -> Segment:
    topic_name, partition_num, value, key, timestamp, headers = request.param
    return base_log_segment.model_copy(
        update=dict(
            topic=topic_name,
            partition=partition_num,
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
        )
    )


@pytest.mark.parametrize(
    "log_segment, expected",
    [
        (("test-topic", 0, b"hello", None, 1752735958, {}), Path("test-topic-0")),
        (("another-topic", 2, b"hello", None, 1752735958, {}), Path("another-topic-2")),
    ],
    indirect=["log_segment"],
)
def test_partition_dirname(log_segment: Segment, expected: Path):
    assert log_segment.partition_dirname == expected


@pytest.mark.parametrize(
    "log_segment, expected",
    [
        (
            ("test-topic", 0, b"test-value", None, 1752735958, {}),
            (
                b"0110{"
                b'"topic":"test-topic",'
                b'"partition":0,'
                b'"value":"dGVzdC12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735958,'
                b'"headers":{}'
                b"}"
            ),
        ),
        (
            ("another-topic", 1, b"another-value", None, 1752735959, {}),
            (
                b"0117"
                b'{"topic":"another-topic",'
                b'"partition":1,'
                b'"value":"YW5vdGhlci12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735959,'
                b'"headers":{}'
                b"}"
            ),
        ),
    ],
    indirect=["log_segment"],
)
def test_binary_record(log_segment: Segment, expected: bytes):
    assert log_segment.binary_record == expected
