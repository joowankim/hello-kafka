from pathlib import Path

import pytest

from kafka import constants
from kafka.broker.log import Segment
from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


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


def test_init_partition(fs_log_storage: FSLogStorage, tmp_path: Path):
    partition_path = tmp_path / "test-topic-0"
    fs_log_storage.init_partition(topic_name="test-topic", partition_num=0)

    log_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
    index_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.index"

    assert log_file_path.exists()
    assert index_file_path.exists()


@pytest.fixture
def log_segment(base_log_segment: Segment, request: pytest.FixtureRequest) -> Segment:
    topic_name, partition_num, value, key, timestamp, headers = request.param
    return base_log_segment.model_copy(
        update=dict(
            topic=topic_name,
            partition=partition_num,
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
        )
    )


@pytest.mark.parametrize(
    "initiated_log_storage, log_segment, expected",
    [
        (
            ("test-topic", 1),
            ("test-topic", 0, b"test-value", None, 1752735958, {}),
            (
                b"0110"
                b"{"
                b'"topic":"test-topic",'
                b'"partition":0,'
                b'"value":"dGVzdC12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735958,'
                b'"headers":{}'
                b"}"
            ),
        ),
        (
            ("another-topic", 1),
            ("another-topic", 0, b"another-value", None, 1752735959, {}),
            (
                b"0117"
                b"{"
                b'"topic":"another-topic",'
                b'"partition":0,'
                b'"value":"YW5vdGhlci12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735959,'
                b'"headers":{}'
                b"}"
            ),
        ),
        (
            ("test-topic", 3),
            ("test-topic", 1, b"additional-data", None, 1752735960, {}),
            (
                b"0114"
                b'{"topic":"test-topic",'
                b'"partition":1,'
                b'"value":"YWRkaXRpb25hbC1kYXRh",'
                b'"key":null,'
                b'"timestamp":1752735960,'
                b'"headers":{}'
                b"}"
            ),
        ),
    ],
    indirect=["initiated_log_storage", "log_segment"],
)
def test_append_log(
    initiated_log_storage: FSLogStorage,
    log_segment: Segment,
    tmp_path: Path,
    expected: bytes,
):
    initiated_log_storage.append_log(log_segment)

    log_file_path = (
        tmp_path
        / log_segment.partition_dirname
        / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
    )
    with open(log_file_path, "rb") as log_file:
        assert log_file.read() == expected


@pytest.mark.parametrize(
    "initiated_log_storage, log_segment",
    [
        (
            ("test-topic", 1),
            ("test-topic", 1, b"test-value", None, 1752735960, {}),
        ),
    ],
    indirect=["initiated_log_storage", "log_segment"],
)
def test_append_log_without_initiated_partition(
    initiated_log_storage: FSLogStorage, log_segment: Segment, tmp_path: Path
):
    with pytest.raises(
        PartitionNotFoundError, match="Partition test-topic-1 does not exist"
    ):
        initiated_log_storage.append_log(log_segment)
