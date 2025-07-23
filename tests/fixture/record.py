import pytest

from kafka.record import ProducerRecord


@pytest.fixture
def base_producer_record() -> ProducerRecord:
    return ProducerRecord(
        topic="test-topic",
        value=b"test-value",
        key=None,
        partition=None,
        timestamp=None,
        headers={},
    )
