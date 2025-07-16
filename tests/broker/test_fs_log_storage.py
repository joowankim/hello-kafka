from pathlib import Path

import pytest

from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError


@pytest.fixture
def fs_log_storage(tmp_path: Path) -> FSLogStorage:
    root_path = tmp_path
    return FSLogStorage(root_path=root_path)


@pytest.mark.parametrize(
    "topic_name, num_partitions, expected",
    [
        (
            "test-topic",
            1,
            [
                Path("test-topic-0"),
                Path("test-topic-0/00000000000000000000.log"),
                Path("test-topic-0/00000000000000000000.index"),
            ],
        ),
        (
            "test-topic",
            2,
            [
                Path("test-topic-0"),
                Path("test-topic-0/00000000000000000000.log"),
                Path("test-topic-0/00000000000000000000.index"),
                Path("test-topic-1"),
                Path("test-topic-1/00000000000000000000.log"),
                Path("test-topic-1/00000000000000000000.index"),
            ],
        ),
        (
            "another-topic",
            1,
            [
                Path("another-topic-0"),
                Path("another-topic-0/00000000000000000000.log"),
                Path("another-topic-0/00000000000000000000.index"),
            ],
        ),
    ],
)
def test_init_topic(
    fs_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    tmp_path: Path,
    expected: list[Path],
):
    fs_log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)

    for filename in expected:
        file_path = tmp_path / filename
        assert file_path.exists()


@pytest.mark.parametrize("num_partitions", [0, -1, -2])
def test_init_topic_with_invalid_num_partitions(
    fs_log_storage: FSLogStorage, num_partitions: int
):
    with pytest.raises(
        InvalidAdminCommandError, match="Number of partitions must be greater than 0"
    ):
        fs_log_storage.init_topic(
            topic_name="test-topic", num_partitions=num_partitions
        )


@pytest.fixture
def initiated_log_storage(
    tmp_path: Path, request: pytest.FixtureRequest
) -> FSLogStorage:
    topic_name, num_partitions = request.param
    log_storage = FSLogStorage(root_path=tmp_path)
    log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)
    return log_storage


@pytest.mark.parametrize(
    "initiated_log_storage, topic_name, num_partitions, expected",
    [
        (
            ("test-topic", 3),
            "test-topic",
            1,
            [
                Path("test-topic-0"),
                Path("test-topic-0/00000000000000000000.log"),
                Path("test-topic-0/00000000000000000000.index"),
                Path("test-topic-1"),
                Path("test-topic-1/00000000000000000000.log"),
                Path("test-topic-1/00000000000000000000.index"),
                Path("test-topic-2"),
                Path("test-topic-2/00000000000000000000.log"),
                Path("test-topic-2/00000000000000000000.index"),
                Path("test-topic-3"),
                Path("test-topic-3/00000000000000000000.log"),
                Path("test-topic-3/00000000000000000000.index"),
            ],
        ),
        (
            ("test-topic", 2),
            "test-topic",
            2,
            [
                Path("test-topic-0"),
                Path("test-topic-0/00000000000000000000.log"),
                Path("test-topic-0/00000000000000000000.index"),
                Path("test-topic-1"),
                Path("test-topic-1/00000000000000000000.log"),
                Path("test-topic-1/00000000000000000000.index"),
                Path("test-topic-2"),
                Path("test-topic-2/00000000000000000000.log"),
                Path("test-topic-2/00000000000000000000.index"),
                Path("test-topic-3"),
                Path("test-topic-3/00000000000000000000.log"),
                Path("test-topic-3/00000000000000000000.index"),
            ],
        ),
        (
            ("test-topic", 1),
            "another-topic",
            3,
            [
                Path("test-topic-0"),
                Path("test-topic-0/00000000000000000000.log"),
                Path("test-topic-0/00000000000000000000.index"),
                Path("another-topic-0"),
                Path("another-topic-0/00000000000000000000.log"),
                Path("another-topic-0/00000000000000000000.index"),
                Path("another-topic-1"),
                Path("another-topic-1/00000000000000000000.log"),
                Path("another-topic-1/00000000000000000000.index"),
                Path("another-topic-2"),
                Path("another-topic-2/00000000000000000000.log"),
                Path("another-topic-2/00000000000000000000.index"),
            ],
        ),
    ],
    indirect=["initiated_log_storage"],
)
def test_append_partitions(
    initiated_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    tmp_path: Path,
    expected: list[Path],
):
    initiated_log_storage.append_partitions(
        topic_name=topic_name, num_partitions=num_partitions
    )

    for filename in expected:
        file_path = tmp_path / filename
        assert file_path.exists()
