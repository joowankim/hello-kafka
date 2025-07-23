from typing import Any
from unittest import mock

import pydantic
import pytest

from kafka.broker.command import Produce
from kafka.message import MessageHeaders, Message, MessageType


@pytest.fixture
def message(
    base_message_headers: MessageHeaders,
    base_message: Message,
    request: pytest.FixtureRequest,
) -> Message:
    headers, payload = request.param
    return base_message.model_copy(
        update=dict(
            headers=base_message_headers.model_copy(update=headers),
            payload=payload,
        )
    )


@pytest.fixture
def expected(base_produce: Produce, request: pytest.FixtureRequest) -> Produce:
    record_params: dict[str, Any] = request.param
    return base_produce.model_copy(update=record_params)


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.PRODUCE},
                b'{"topic":"topic01","value":"dmFsdWU=","partition":0,"key":null,"timestamp":null,"headers":{}}',
            ),
            dict(
                topic="topic01",
                value=b"value",
                partition=0,
                key=None,
                timestamp=1753230940,
                headers={},
            ),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.PRODUCE},
                b'{"topic": "topic02","value":"dmFsdWUy","key":"dXNlcjAx","partition":1,"timestamp":null,"headers":{}}',
            ),
            dict(
                topic="topic02",
                value=b"value2",
                partition=1,
                key=b"user01",
                timestamp=1753230940,
                headers={},
            ),
        ),
    ],
    indirect=["message", "expected"],
)
def test_from_message(message: Message, expected: Produce):
    with mock.patch("time.time", return_value=1753230940):
        cmd = Produce.from_message(message)

        assert cmd == expected


@pytest.mark.parametrize(
    "message, error_type",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.CREATE_TOPICS},
                b'{"topic":"topic01","value":"dmFsdWU=","partition":0}',
            ),
            ValueError,
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.PRODUCE},
                b'{"topic": "topic02"}',
            ),
            ValueError,
        ),
        (
            (
                {"correlation_id": 3, "api_key": MessageType.PRODUCE},
                b'{"topic": "topic03", "value": "dmFsdWU=", "partition": 2}',
            ),
            pydantic.ValidationError,
        ),
    ],
    indirect=["message"],
)
def test_from_message_with_invalid_message(
    message: Message, error_type: type[Exception]
):
    with pytest.raises(error_type):
        Produce.from_message(message)
