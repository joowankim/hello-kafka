import pytest

from kafka.broker import ProduceResponse


@pytest.fixture
def base_produce_response() -> ProduceResponse:
    return ProduceResponse(
        topic="test-topic",
        partition=0,
        base_offset=0,
        error_code=0,
        error_message=None,
    )
