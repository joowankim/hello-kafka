from typing import Any

import pytest

from kafka.message import Message, MessageHeaders, MessageType
from kafka.record import ProducerRecord


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
    base_producer_record: ProducerRecord, request: pytest.FixtureRequest
) -> ProducerRecord:
    record_params: dict[str, Any] = request.param
    return base_producer_record.model_copy(update=record_params)


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.PRODUCE},
                b'{"topic":"topic01","value":"dmFsdWU="}',
            ),
            dict(
                topic="topic01",
                value=b"value",
                key=None,
                partition=None,
                timestamp=None,
                headers={},
            ),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.PRODUCE},
                b'{"topic": "topic02","value":"dmFsdWUy","key":"dXNlcjAx"}',
            ),
            dict(
                topic="topic02",
                value=b"value2",
                key=b"user01",
                partition=None,
                timestamp=None,
                headers={},
            ),
        ),
    ],
    indirect=["message", "expected"],
)
def test_from_message(message: Message, expected: ProducerRecord):
    record = ProducerRecord.from_message(message)

    assert record == expected


@pytest.mark.parametrize(
    "message",
    [
        (
            {"correlation_id": 3, "api_key": MessageType.CREATE_TOPICS},
            b'{"topic": "topic03", "value": "value3"}',
        ),
    ],
    indirect=["message"],
)
def test_from_message_with_invalid_message_type(message: Message):
    with pytest.raises(ValueError, match="Message is not of type PRODUCE"):
        ProducerRecord.from_message(message)
