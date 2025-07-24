from typing import Any

import pytest

from kafka.broker.log import Partition, Segment


@pytest.fixture
def partition(
    base_segment: Segment, base_partition: Partition, request: pytest.FixtureRequest
) -> Partition:
    partition_params: dict[str, Any] = request.param
    return base_partition.model_copy(
        update=dict(
            topic=partition_params["topic"],
            num=partition_params["num"],
            segments=[
                base_segment.model_copy(update=segment_data)
                for segment_data in partition_params["segments"]
            ],
            leo=partition_params["leo"],
        )
    )


@pytest.mark.parametrize(
    "partition, expected",
    [
        (
            dict(
                topic="test-topic",
                num=0,
                segments=[dict(base_offset=0)],
                leo=0,
            ),
            "test-topic-0",
        ),
        (
            dict(
                topic="another-topic",
                num=1,
                segments=[dict(base_offset=100)],
                leo=50,
            ),
            "another-topic-1",
        ),
        (
            dict(
                topic="topic-with-multiple-segments",
                num=2,
                segments=[dict(base_offset=200), dict(base_offset=300)],
                leo=250,
            ),
            "topic-with-multiple-segments-2",
        ),
    ],
    indirect=["partition"],
)
def test_name(partition: Partition, expected: str):
    assert partition.name == expected


@pytest.mark.parametrize(
    "partition, expected",
    [
        (
            dict(
                topic="test-topic",
                num=0,
                segments=[dict(base_offset=0)],
                leo=0,
            ),
            Segment(base_offset=0),
        ),
        (
            dict(
                topic="another-topic",
                num=1,
                segments=[dict(base_offset=100)],
                leo=150,
            ),
            Segment(base_offset=100),
        ),
        (
            dict(
                topic="topic-with-multiple-segments",
                num=2,
                segments=[dict(base_offset=200), dict(base_offset=300)],
                leo=350,
            ),
            Segment(base_offset=300),
        ),
    ],
    indirect=["partition"],
)
def test_active_segment(partition: Partition, expected: Segment):
    assert partition.active_segment == expected
