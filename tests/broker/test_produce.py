from typing import Any
from unittest import mock

import pydantic
import pytest

from kafka.broker.command import Produce, RecordContents
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
def expected(
    base_record_contents: RecordContents,
    base_produce: Produce,
    request: pytest.FixtureRequest,
) -> Produce:
    record_params: dict[str, Any] = request.param
    return base_produce.model_copy(
        update=dict(
            topic=record_params["topic"],
            partition=record_params["partition"],
            records=[
                base_record_contents.model_copy(update=record)
                for record in record_params["records"]
            ],
        )
    )


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.PRODUCE},
                b'{"topic":"topic01","partition":0,"records":[{"value":"dmFsdWU=","key":null,"timestamp":null,"headers":{}}]}',
            ),
            dict(
                topic="topic01",
                partition=0,
                records=[
                    dict(
                        value=b"value",
                        key=None,
                        timestamp=1753230940,
                        headers={},
                    ),
                ],
            ),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.PRODUCE},
                b'{"topic": "topic02","partition":1,"records":[{"value":"dmFsdWUy","key":"dXNlcjAx","timestamp":null,"headers":{}}]}',
            ),
            dict(
                topic="topic02",
                partition=1,
                records=[
                    dict(
                        value=b"value2",
                        key=b"user01",
                        timestamp=1753230940,
                        headers={},
                    ),
                ],
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
                b'{"topic":"topic01","partition":0,"records":[{"value":"dmFsdWU="}]}',
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
                b'{"topic": "topic03", "partition": 2, "records": []}',
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
