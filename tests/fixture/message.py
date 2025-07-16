import json

import pytest

from kafka.message import Message, MessageHeaders, MessageType


@pytest.fixture
def base_message_headers() -> MessageHeaders:
    return MessageHeaders(
        correlation_id=1,
        api_key=MessageType.CREATE_TOPICS,
    )


@pytest.fixture
def base_message(base_message_headers: MessageHeaders) -> Message:
    return Message(
        headers=base_message_headers.model_copy(),
        payload=json.dumps(
            dict(topic="topic-1", value="value")
        ).encode("utf-8"),
    )
