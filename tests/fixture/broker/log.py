import pytest

from kafka.broker.log import Record


@pytest.fixture
def base_log_record() -> Record:
    return Record(
        topic="test-topic",
        partition=0,
        value=b"test-value",
        key=None,
        timestamp=1752735958,
        headers={},
        offset=0,
    )
