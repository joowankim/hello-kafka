from typing import Any
from unittest import mock


import pytest

from kafka.broker import ProduceResponse


@pytest.fixture
def expected(
    base_produce_response: ProduceResponse, request: pytest.FixtureRequest
) -> ProduceResponse:
    response_param: dict[str, Any] = request.param
    return base_produce_response.model_copy(update=response_param)


@pytest.mark.parametrize(
    "topic, partition, base_offset, expected",
    [
        (
            "test_topic",
            0,
            100,
            dict(
                topic="test_topic",
                partition=0,
                error_code=0,
                base_offset=100,
                timestamp=1754556963,
            ),
        ),
        (
            "another_topic",
            1,
            200,
            dict(
                topic="another_topic",
                partition=1,
                error_code=0,
                base_offset=200,
                timestamp=1754556963,
            ),
        ),
    ],
    indirect=["expected"],
)
def test_success(
    topic: str, partition: int, base_offset: int, expected: ProduceResponse
):
    with mock.patch("time.time", return_value=1754556963):
        resp = ProduceResponse.success(
            topic=topic, partition=partition, base_offset=base_offset
        )

        assert resp == expected


@pytest.mark.parametrize(
    "topic, partition, error_code, error_message, expected",
    [
        (
            "test_topic",
            0,
            1,
            "Error occurred",
            dict(
                topic="test_topic",
                partition=0,
                base_offset=-1,
                timestamp=1754556963,
                error_code=1,
                error_message="Error occurred",
            ),
        ),
        (
            "another_topic",
            1,
            2,
            "Another error",
            dict(
                topic="another_topic",
                partition=1,
                base_offset=-1,
                timestamp=1754556963,
                error_code=2,
                error_message="Another error",
            ),
        ),
    ],
    indirect=["expected"],
)
def test_failure(
    topic: str,
    partition: int,
    error_code: int,
    error_message: str,
    expected: ProduceResponse,
):
    with mock.patch("time.time", return_value=1754556963):
        resp = ProduceResponse.failure(
            topic=topic,
            partition=partition,
            error_code=error_code,
            error_message=error_message,
        )

        assert resp == expected


@pytest.mark.parametrize(
    "payload, expected",
    [
        (
            b'{"topic":"test_topic","partition":0,"base_offset":100,"timestamp":1754556963,"error_code":0,"error_message":null}',
            dict(
                topic="test_topic",
                partition=0,
                base_offset=100,
                timestamp=1754556963,
                error_code=0,
                error_message=None,
            ),
        ),
        (
            b'{"topic":"another_topic","partition":1,"base_offset":200,"timestamp":1754556963,"error_code":0,"error_message":null}',
            dict(
                topic="another_topic",
                partition=1,
                base_offset=200,
                timestamp=1754556963,
                error_code=0,
                error_message=None,
            ),
        ),
    ],
    indirect=["expected"],
)
def test_deserialize(payload: bytes, expected: ProduceResponse):
    response = ProduceResponse.deserialize(payload)

    assert response == expected
