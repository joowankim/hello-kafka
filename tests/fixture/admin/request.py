import pytest

from kafka.admin.request import NewTopic


@pytest.fixture
def base_new_topic() -> NewTopic:
    return NewTopic(name="topic-1", num_partitions=3, replication_factor=2)
