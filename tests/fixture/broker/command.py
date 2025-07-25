import pytest

from kafka.broker.command import (
    CreateTopics,
    CreateTopic,
    Produce,
    RecordContents,
    TopicOffset,
    OffsetCommit,
)


@pytest.fixture
def base_create_topic() -> CreateTopic:
    return CreateTopic(
        name="topic-1",
        num_partitions=3,
    )


@pytest.fixture
def base_create_topics(base_create_topic: CreateTopic) -> CreateTopics:
    return CreateTopics(topics=[base_create_topic.model_copy()])


@pytest.fixture
def base_record_contents() -> RecordContents:
    return RecordContents(
        value="test-value",
        key=None,
        timestamp=None,
        headers={},
    )


@pytest.fixture
def base_produce(base_record_contents: RecordContents) -> Produce:
    return Produce(
        topic="test-topic",
        partition=0,
        records=[base_record_contents.model_copy()],
    )


@pytest.fixture
def base_topic_offset() -> TopicOffset:
    return TopicOffset(
        topic="test-topic",
        partition=0,
        offset=100,
    )


@pytest.fixture
def base_offset_commit(base_topic_offset: TopicOffset) -> OffsetCommit:
    return OffsetCommit(
        group_id="test-group",
        topics=[base_topic_offset.model_copy()],
    )
