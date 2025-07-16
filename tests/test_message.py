import pytest

from kafka.message import Message, MessageHeaders


@pytest.fixture
def message(
    base_message: Message,
    base_message_headers: MessageHeaders,
    request: pytest.FixtureRequest,
) -> Message:
    correlation_id, api_key, record = request.param
    return base_message.model_copy(
        update=dict(
            headers=base_message_headers.model_copy(
                update=dict(
                    correlation_id=correlation_id,
                    api_key=api_key,
                )
            ),
            payload=record,
        )
    )


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (1, 0, b"{'topic': 'topic-1', 'value': 'value'}"),
            b"0001000038{'topic': 'topic-1', 'value': 'value'}",
        ),
        (
            (2, 1, b"{'topic': 'topic-2', 'value': 'value2'}"),
            b"0002010039{'topic': 'topic-2', 'value': 'value2'}",
        ),
        (
            (3, 2, b"{'topic': 'topic-3', 'value': 'value3'}"),
            b"0003020039{'topic': 'topic-3', 'value': 'value3'}",
        ),
    ],
    indirect=["message"],
)
def test_serialized(message: Message, expected: bytes):
    assert message.serialized == expected
