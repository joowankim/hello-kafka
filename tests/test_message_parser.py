import asyncio
from collections.abc import Callable
from typing import Any

import pytest

from kafka.parser import MessageParser
from kafka.message import Message, MessageHeaders, MessageType


@pytest.fixture
def message_parser(
    fake_stream_reader_factory: Callable[[bytes], asyncio.StreamReader],
    request: pytest.FixtureRequest,
) -> MessageParser:
    data: bytes = request.param
    fake_stream_reader = fake_stream_reader_factory(data)
    return MessageParser(reader=fake_stream_reader)


@pytest.fixture
def expected(
    base_message_headers: MessageHeaders,
    base_message: Message,
    request: pytest.FixtureRequest,
) -> list[Message]:
    message_params: list[tuple[dict[str, Any], bytes]] = request.param
    return [
        base_message.model_copy(
            update=dict(
                headers=base_message_headers.model_copy(update=headers),
                payload=payload,
            )
        )
        for headers, payload in message_params
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message_parser, expected",
    [
        (
            b"0001000035{'topic':'topic-1','value':'value'}"
            b"0002000035{'topic':'topic-1','value':'value'}",
            [
                (
                    dict(correlation_id=1, api_key=MessageType.CREATE_TOPICS),
                    b"{'topic':'topic-1','value':'value'}",
                ),
                (
                    dict(correlation_id=2, api_key=MessageType.CREATE_TOPICS),
                    b"{'topic':'topic-1','value':'value'}",
                ),
            ],
        ),
    ],
    indirect=["message_parser", "expected"],
)
async def test_aiter(message_parser: MessageParser, expected: list[Message]):
    parsed_messages = [msg async for msg in message_parser]

    assert parsed_messages == expected
