import pytest

from kafka.record import ProducerRecord


@pytest.fixture
def base_producer_record() -> ProducerRecord:
    return ProducerRecord(
        topic="test-topic",
        partition=0,
        key="test-key",
        value="test-value",
        headers={"header1": "value1", "header2": "value2"},
    )
