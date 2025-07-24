import pytest

from kafka.broker.log import Record, Partition, Segment


@pytest.fixture
def base_log_record() -> Record:
    return Record(
        topic="test-topic",
        partition=0,
        value="test-value",
        key=None,
        timestamp=1752735958,
        headers={},
        offset=0,
    )


@pytest.fixture
def base_segment() -> Segment:
    return Segment(base_offset=0)


@pytest.fixture
def base_partition(base_segment: Segment) -> Partition:
    return Partition(
        topic="test-topic",
        num=0,
        segments=[base_segment.model_copy()],
        leo=0,
        segment_size_limit=1000,
    )
