import json
from typing import Any

import pydantic
import pytest

from kafka.broker.command import CreateTopics, CreateTopic
from kafka.message import Message, MessageHeaders


@pytest.fixture
def message(
    base_message: Message,
    base_message_headers: MessageHeaders,
    request: pytest.FixtureRequest,
) -> Message:
    headers, payload = request.param
    return base_message.model_copy(
        update=dict(
            headers=base_message_headers.model_copy(update=headers),
            payload=payload.encode("utf-8"),
        )
    )


@pytest.fixture
def expected(
    base_create_topic: CreateTopic,
    base_create_topics: CreateTopics,
    request: pytest.FixtureRequest,
) -> CreateTopics:
    topics: list[dict[str, Any]] = request.param
    return base_create_topics.model_copy(
        update=dict(topics=[base_create_topic.model_copy(update=t) for t in topics])
    )


@pytest.mark.parametrize(
    "message, expected",
    [
        (
            (
                {"correlation_id": 1, "api_key": 0},
                json.dumps({"topics": [{"name": "topic01", "num_partitions": 3}]}),
            ),
            [{"name": "topic01", "num_partitions": 3}],
        ),
        (
            (
                {"correlation_id": 123, "api_key": 0},
                json.dumps({"topics": [{"name": "topic02", "num_partitions": 5}]}),
            ),
            [{"name": "topic02", "num_partitions": 5}],
        ),
        (
            (
                {"correlation_id": 456, "api_key": 0},
                json.dumps(
                    {
                        "topics": [
                            {"name": "topic03", "num_partitions": 2},
                            {"name": "topic04", "num_partitions": 2},
                        ]
                    }
                ),
            ),
            [
                {"name": "topic03", "num_partitions": 2},
                {"name": "topic04", "num_partitions": 2},
            ],
        ),
    ],
    indirect=["message", "expected"],
)
def test_from_message(message: Message, expected: CreateTopics):
    create_topics = CreateTopics.from_message(message)

    assert create_topics == expected


# 1. api key error
# 2. duplicated topics error
@pytest.mark.parametrize(
    "message, error_type",
    [
        (
            (
                {"correlation_id": 1, "api_key": 1},
                json.dumps({"topics": [{"name": "topic01", "num_partitions": 3}]}),
            ),
            ValueError,
        ),
        (
            (
                {"correlation_id": 123, "api_key": 0},
                json.dumps(
                    {
                        "topics": [
                            {"name": "topic02", "num_partitions": 5},
                            {"name": "topic02", "num_partitions": 1},
                        ]
                    }
                ),
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
        CreateTopics.from_message(message)
