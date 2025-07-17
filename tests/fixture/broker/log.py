import pytest

from kafka.broker.log import Segment


@pytest.fixture
def base_log_segment() -> Segment:
    return Segment(
        topic="test-topic",
        partition=0,
        value=b"test-value",
        key=None,
        timestamp=1752735958,
        headers={},
    )
