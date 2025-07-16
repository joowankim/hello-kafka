from pathlib import Path

import pytest

from kafka.broker.log import Segment


@pytest.fixture
def log_segment(base_log_segment: Segment, request: pytest.FixtureRequest) -> Segment:
    topic_name, partition_num = request.param
    return base_log_segment.model_copy(
        update=dict(
            topic=topic_name,
            partition=partition_num,
            value=b"test-value",
        )
    )


@pytest.mark.parametrize(
    "log_segment, expected",
    [
        (("test-topic", 0), Path("test-topic-0")),
        (("test-topic", 1), Path("test-topic-1")),
        (("another-topic", 2), Path("another-topic-2")),
        (("test-topic", 3), Path("test-topic-3")),
    ],
    indirect=["log_segment"],
)
def test_partition_dirname(log_segment: Segment, expected: Path):
    assert log_segment.partition_dirname == expected
