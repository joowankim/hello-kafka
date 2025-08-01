from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from kafka.admin.request import NewTopicList, NewTopic


@pytest.fixture
def new_topics(
    base_new_topic: NewTopic, request: pytest.FixtureRequest
) -> NewTopicList:
    topic_params: list[dict[str, Any]] = request.param
    return NewTopicList(
        [base_new_topic.model_copy(update=topic) for topic in topic_params]
    )


@pytest.mark.parametrize(
    "new_topics",
    [
        [dict(name="topic-1", num_partitions=3, replication_factor=2)],
        [
            dict(name="topic-1", num_partitions=3, replication_factor=2),
            dict(name="topic-2", num_partitions=5, replication_factor=3),
        ],
    ],
    indirect=["new_topics"],
)
def test_validation(new_topics: NewTopicList):
    validated = TypeAdapter(NewTopicList).validate_python(new_topics)

    assert validated == new_topics


@pytest.mark.parametrize(
    "new_topics",
    [
        [
            dict(name="topic-1", num_partitions=0, replication_factor=2),
            dict(name="topic-1", num_partitions=3, replication_factor=0),
        ],
    ],
    indirect=["new_topics"],
)
def test_validation_with_duplicated_topics(new_topics: NewTopicList):
    with pytest.raises(
        ValidationError, match="New topics must not have duplicated names"
    ):
        TypeAdapter(NewTopicList).validate_python(new_topics)


@pytest.mark.parametrize(
    "new_topics, expected",
    [
        (
            [dict(name="topic-1", num_partitions=3, replication_factor=1)],
            b'{"topics": [{"name": "topic-1", "num_partitions": 3}]}',
        ),
        (
            [
                dict(name="topic-1", num_partitions=3, replication_factor=2),
                dict(name="topic-2", num_partitions=5, replication_factor=3),
            ],
            b'{"topics": [{"name": "topic-1", "num_partitions": 3}, {"name": "topic-2", "num_partitions": 5}]}',
        ),
    ],
    indirect=["new_topics"],
)
def test_payload(new_topics: NewTopicList, expected: bytes):
    payload = new_topics.payload

    assert payload == expected
