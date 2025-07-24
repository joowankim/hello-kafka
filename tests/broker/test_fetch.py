import pytest

from kafka.broker.query import Fetch
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
def expected(base_fetch: Fetch, request: pytest.FixtureRequest) -> Fetch:
    fetch_params: dict[str, int] = request.param
    return base_fetch.model_copy(update=fetch_params)


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.FETCH},
                b'{"topic":"topic01","partition":0,"offset":100,"max_bytes":1048576}',
            ),
            dict(topic="topic01", partition=0, offset=100, max_bytes=1048576),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.FETCH},
                b'{"topic":"topic02","partition":1,"offset":200,"max_bytes":2097152}',
            ),
            dict(topic="topic02", partition=1, offset=200, max_bytes=2097152),
        ),
    ],
    indirect=["message", "expected"],
)
def test_from_message(message: Message, expected: Fetch):
    cmd = Fetch.from_message(message)

    assert cmd == expected


@pytest.mark.parametrize(
    "message",
    [
        (
            {"correlation_id": 3, "api_key": MessageType.CREATE_TOPICS},
            b'{"topic":"topic03","partition":2,"offset":300,"max_bytes":3145728}',
        ),
    ],
    indirect=["message"],
)
def test_from_message_with_invalid_message(message: Message):
    with pytest.raises(ValueError):
        Fetch.from_message(message)
