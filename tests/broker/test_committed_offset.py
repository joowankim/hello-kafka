from typing import Any

import pytest

from kafka.broker.command import OffsetCommit, TopicOffset
from kafka.broker.log import CommittedOffset


@pytest.fixture
def offset_commit(
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


@pytest.fixture
def expected(
    base_committed_offset: CommittedOffset, request: pytest.FixtureRequest
) -> list[CommittedOffset]:
    committed_offsets_params: list[dict[str, Any]] = request.param
    return [
        base_committed_offset.model_copy(update=offset)
        for offset in committed_offsets_params
    ]


@pytest.mark.parametrize(
    "offset_commit, expected",
    [
        (
            {
                "group_id": "test-group",
                "topics": [{"topic": "test-topic", "partition": 0, "offset": 100}],
            },
            [
                {
                    "group_id": "test-group",
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 100,
                }
            ],
        ),
        (
            {
                "group_id": "another-group",
                "topics": [
                    {"topic": "test-topic", "partition": 0, "offset": 100},
                    {"topic": "another-topic", "partition": 1, "offset": 200},
                ],
            },
            [
                {
                    "group_id": "another-group",
                    "topic": "test-topic",
                    "partition": 0,
                    "offset": 100,
                },
                {
                    "group_id": "another-group",
                    "topic": "another-topic",
                    "partition": 1,
                    "offset": 200,
                },
            ],
        ),
    ],
    indirect=["offset_commit", "expected"],
)
def test_from_offset_commit_command(
    offset_commit: OffsetCommit, expected: list[CommittedOffset]
):
    committed_offsets = CommittedOffset.from_offset_commit_command(offset_commit)

    assert committed_offsets == expected
