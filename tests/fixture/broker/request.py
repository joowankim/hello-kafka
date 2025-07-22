import pytest

from kafka.broker.request import CreateTopics, CreateTopic


@pytest.fixture
def base_create_topic() -> CreateTopic:
    return CreateTopic(
        name="topic-1",
        num_partitions=3,
    )


@pytest.fixture
def base_create_topics(base_create_topic: CreateTopic) -> CreateTopics:
    return CreateTopics(topics=[base_create_topic.model_copy()])
