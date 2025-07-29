import pytest

from kafka.broker.router import Router
from kafka.error import UnknownMessageTypeError
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
def router() -> Router:
    return Router()


@pytest.fixture
def expected(
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


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.FETCH},
                b"{}",
            ),
            (
                {"correlation_id": 1, "api_key": MessageType.FETCH},
                b"{'status': 'success'}",
            ),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.CREATE_TOPICS},
                b"{}",
            ),
            (
                {"correlation_id": 2, "api_key": MessageType.CREATE_TOPICS},
                b"{'status': 'failure'}",
            ),
        ),
    ],
    indirect=["message", "expected"],
)
def test_router_register_and_route(router: Router, message: Message, expected: Message):
    def handler_a(req: Message) -> Message:
        return req.model_copy(update={"payload": b"{'status': 'success'}"})

    def handler_b(req: Message) -> Message:
        return req.model_copy(update={"payload": b"{'status': 'failure'}"})

    router.register(MessageType.FETCH)(handler_a)
    router.register(MessageType.CREATE_TOPICS)(handler_b)

    response = router.route(message)

    assert response == expected


@pytest.mark.parametrize(
    "message",
    [
        (
            {"correlation_id": 3, "api_key": MessageType.LIST_TOPICS},
            b"{}",
        ),
    ],
    indirect=["message"],
)
def test_route_with_unknown_message_type(router: Router, message: Message):
    with pytest.raises(UnknownMessageTypeError):
        router.route(message)
