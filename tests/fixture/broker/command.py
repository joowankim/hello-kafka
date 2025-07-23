import pytest

from kafka.broker.command import CreateTopics, CreateTopic, Produce


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
def base_produce() -> Produce:
    return Produce(
        topic="test-topic",
        value=b"test-value",
        partition=0,
        key=None,
        timestamp=None,
        headers={},
    )
