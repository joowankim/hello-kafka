import pytest

from kafka.producer.command import RecordContents


@pytest.fixture
def base_producer_record_contents() -> RecordContents:
    return RecordContents(
        key=b"test-key",
        value=b"test-value",
        timestamp=1234567890,
        headers={"header1": b"value1", "header2": b"value2"},
    )
