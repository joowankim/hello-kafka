from typing import Any

import pytest

from kafka.broker.command import OffsetCommit, TopicOffset
from kafka.message import Message, MessageHeaders, MessageType


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
    base_topic_offset: TopicOffset,
    base_offset_commit: OffsetCommit,
    request: pytest.FixtureRequest,
) -> OffsetCommit:
    offset_commit_params: dict[str, Any] = request.param
    return base_offset_commit.model_copy(
        update=dict(
            group_id=offset_commit_params["group_id"],
            topics=[
                base_topic_offset.model_copy(update=topic)
                for topic in offset_commit_params["topics"]
            ],
        )
    )


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": MessageType.OFFSET_COMMIT},
                b'{"group_id":"test-group","topics":[{"topic":"test-topic","partition":0,"offset":100}]}',
            ),
            dict(
                group_id="test-group",
                topics=[dict(topic="test-topic", partition=0, offset=100)],
            ),
        ),
        (
            (
                {"correlation_id": 2, "api_key": MessageType.OFFSET_COMMIT},
                b'{"group_id":"another-group","topics":[{"topic":"test-topic","partition":0,"offset":100},{"topic":"another-topic","partition":1,"offset":200}]}',
            ),
            dict(
                group_id="another-group",
                topics=[
                    dict(topic="test-topic", partition=0, offset=100),
                    dict(topic="another-topic", partition=1, offset=200),
                ],
            ),
        ),
    ],
    indirect=["message", "expected"],
)
def test_from_message(message: Message, expected: OffsetCommit):
    cmd = OffsetCommit.from_message(message)

    assert cmd == expected
