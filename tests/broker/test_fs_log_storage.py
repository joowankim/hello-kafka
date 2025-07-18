import shutil
from pathlib import Path

import pytest

from kafka import constants
from kafka.broker.log import Record
from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


@pytest.fixture
def root_path(
    resource_dir: Path, tmp_path: Path, request: pytest.FixtureRequest
) -> Path:
    root_dirname: str = request.param
    root_path = resource_dir / "roots" / root_dirname
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return tmp_path


@pytest.mark.parametrize(
    "root_path, expected",
    [
        ("root-empty", {}),
        ("root-limit_1GB", {("topic01", 0): 1, ("topic01", 1): 0}),
        ("root-limit_100B", {("topic01", 0): 1, ("topic01", 1): 0}),
    ],
    indirect=["root_path"],
)
def test_load_from_root(root_path: Path, expected: dict[tuple[str, int], int]):
    log_file_size_limit = constants.LOG_FILE_SIZE_LIMIT
    log_storage = FSLogStorage.load_from_root(
        root_path=root_path, log_file_size_limit=log_file_size_limit
    )

    assert log_storage.leo_map == expected


@pytest.fixture
def fs_log_storage(tmp_path: Path) -> FSLogStorage:
    root_path = tmp_path
    return FSLogStorage(
        root_path=root_path,
        log_file_size_limit=constants.LOG_FILE_SIZE_LIMIT,
        leo_map={},
    )


@pytest.mark.parametrize(
    "topic_name, num_partitions, expected",
    [
        (
            "test-topic",
            1,
            (
                {("test-topic", 0): 0},
                [
                    Path("test-topic-0"),
                    Path("test-topic-0/00000000000000000000.log"),
                    Path("test-topic-0/00000000000000000000.index"),
                ],
            ),
        ),
        (
            "test-topic",
            2,
            (
                {("test-topic", 0): 0, ("test-topic", 1): 0},
                [
                    Path("test-topic-0"),
                    Path("test-topic-0/00000000000000000000.log"),
                    Path("test-topic-0/00000000000000000000.index"),
                    Path("test-topic-1"),
                    Path("test-topic-1/00000000000000000000.log"),
                    Path("test-topic-1/00000000000000000000.index"),
                ],
            ),
        ),
        (
            "another-topic",
            1,
            (
                {("another-topic", 0): 0},
                [
                    Path("another-topic-0"),
                    Path("another-topic-0/00000000000000000000.log"),
                    Path("another-topic-0/00000000000000000000.index"),
                ],
            ),
        ),
    ],
)
def test_init_topic(
    fs_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    tmp_path: Path,
    expected: tuple[dict[tuple[str, int], int], list[Path]],
):
    fs_log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)

    leo_map, paths = expected
    assert fs_log_storage.leo_map == leo_map
    for filename in paths:
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
    log_storage = FSLogStorage(
        root_path=tmp_path,
        log_file_size_limit=constants.LOG_FILE_SIZE_LIMIT,
        leo_map={},
    )
    log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)
    return log_storage


@pytest.mark.parametrize(
    "initiated_log_storage, topic_name, num_partitions, expected",
    [
        (
            ("test-topic", 3),
            "test-topic",
            1,
            (
                {
                    ("test-topic", 0): 0,
                    ("test-topic", 1): 0,
                    ("test-topic", 2): 0,
                    ("test-topic", 3): 0,
                },
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
        ),
        (
            ("test-topic", 2),
            "test-topic",
            2,
            (
                {
                    ("test-topic", 0): 0,
                    ("test-topic", 1): 0,
                    ("test-topic", 2): 0,
                    ("test-topic", 3): 0,
                },
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
        ),
        (
            ("test-topic", 1),
            "another-topic",
            3,
            (
                {
                    ("test-topic", 0): 0,
                    ("another-topic", 0): 0,
                    ("another-topic", 1): 0,
                    ("another-topic", 2): 0,
                },
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
        ),
    ],
    indirect=["initiated_log_storage"],
)
def test_append_partitions(
    initiated_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    tmp_path: Path,
    expected: tuple[dict[tuple[str, int], int], list[Path]],
):
    initiated_log_storage.append_partitions(
        topic_name=topic_name, num_partitions=num_partitions
    )

    leo_map, paths = expected
    assert initiated_log_storage.leo_map == leo_map
    for filename in paths:
        file_path = tmp_path / filename
        assert file_path.exists()


def test_init_partition(fs_log_storage: FSLogStorage, tmp_path: Path):
    partition_path = tmp_path / "test-topic-0"
    fs_log_storage.init_partition(topic_name="test-topic", partition_num=0)

    log_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
    index_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.index"

    assert log_file_path.exists()
    assert index_file_path.exists()
    assert fs_log_storage.leo_map == {("test-topic", 0): 0}


@pytest.fixture
def log_record(base_log_record: Record, request: pytest.FixtureRequest) -> Record:
    topic_name, partition_num, value, key, timestamp, headers = request.param
    return base_log_record.model_copy(
        update=dict(
            topic=topic_name,
            partition=partition_num,
            value=value,
            key=key,
            timestamp=timestamp,
            headers=headers,
            offset=None,
        )
    )


@pytest.mark.parametrize(
    "initiated_log_storage, log_record, expected",
    [
        (
            ("test-topic", 1),
            ("test-topic", 0, b"test-value", None, 1752735958, {}),
            (
                {("test-topic", 0): 1},
                b"0086"
                b"{"
                b'"value":"dGVzdC12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735958,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
            ),
        ),
        (
            ("another-topic", 1),
            ("another-topic", 0, b"another-value", None, 1752735959, {}),
            (
                {("another-topic", 0): 1},
                b"0090"
                b"{"
                b'"value":"YW5vdGhlci12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735959,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
            ),
        ),
        (
            ("test-topic", 3),
            ("test-topic", 1, b"additional-data", None, 1752735960, {}),
            (
                {
                    ("test-topic", 0): 0,
                    ("test-topic", 1): 1,
                    ("test-topic", 2): 0,
                },
                b"0090"
                b"{"
                b'"value":"YWRkaXRpb25hbC1kYXRh",'
                b'"key":null,'
                b'"timestamp":1752735960,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
            ),
        ),
    ],
    indirect=["initiated_log_storage", "log_record"],
)
def test_append_log(
    initiated_log_storage: FSLogStorage,
    log_record: Record,
    tmp_path: Path,
    expected: tuple[dict[tuple[str, int], int], bytes],
):
    initiated_log_storage.append_log(log_record)

    leo_map, record = expected
    assert initiated_log_storage.leo_map == leo_map
    log_file_path = (
        tmp_path
        / log_record.partition_dirname
        / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
    )
    with open(log_file_path, "rb") as log_file:
        assert log_file.read() == record


@pytest.mark.parametrize(
    "initiated_log_storage, log_record",
    [
        (
            ("test-topic", 1),
            ("test-topic", 1, b"test-value", None, 1752735960, {}),
        ),
    ],
    indirect=["initiated_log_storage", "log_record"],
)
def test_append_log_without_initiated_partition(
    initiated_log_storage: FSLogStorage, log_record: Record, tmp_path: Path
):
    with pytest.raises(
        PartitionNotFoundError, match="Partition test-topic-1 does not exist"
    ):
        initiated_log_storage.append_log(log_record)


@pytest.fixture
def logged_log_storage(
    resource_dir: Path, tmp_path: Path, request: pytest.FixtureRequest
) -> FSLogStorage:
    root_dirname, log_file_size_limit = request.param
    root_path = resource_dir / "roots" / root_dirname
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return FSLogStorage.load_from_root(
        root_path=tmp_path, log_file_size_limit=log_file_size_limit
    )


@pytest.mark.parametrize(
    "logged_log_storage, log_record, expected",
    [
        (
            ("root-limit_1GB", 1024**3),
            ("topic01", 0, b"second-log", None, 1752735962, {}),
            (
                {
                    ("topic01", 0): 2,
                    ("topic01", 1): 0,
                },
                [
                    (
                        0,
                        b"0086"
                        b"{"
                        b'"value":"aW5pdGlhbC1sb2c=",'
                        b'"key":null,'
                        b'"timestamp":1752735961,'
                        b'"headers":{},'
                        b'"offset":0'
                        b"}"
                        b"0086"
                        b"{"
                        b'"value":"c2Vjb25kLWxvZw==",'
                        b'"key":null,'
                        b'"timestamp":1752735962,'
                        b'"headers":{},'
                        b'"offset":1'
                        b"}",
                    ),
                ],
            ),
        ),
        (
            ("root-limit_100B", 100),
            ("topic01", 0, b"second-log", None, 1752735962, {}),
            (
                {
                    ("topic01", 0): 2,
                    ("topic01", 1): 0,
                },
                [
                    (
                        0,
                        b"0086"
                        b"{"
                        b'"value":"aW5pdGlhbC1sb2c=",'
                        b'"key":null,'
                        b'"timestamp":1752735961,'
                        b'"headers":{},'
                        b'"offset":0'
                        b"}",
                    ),
                    (
                        1,
                        b"0086"
                        b"{"
                        b'"value":"c2Vjb25kLWxvZw==",'
                        b'"key":null,'
                        b'"timestamp":1752735962,'
                        b'"headers":{},'
                        b'"offset":1'
                        b"}",
                    ),
                ],
            ),
        ),
    ],
    indirect=["logged_log_storage", "log_record"],
)
def test_append_log_to_already_logged_partition(
    logged_log_storage: FSLogStorage,
    log_record: Record,
    tmp_path: Path,
    expected: tuple[dict[tuple[str, int], int], list[tuple[int, bytes]]],
):
    logged_log_storage.append_log(log_record)

    leo_map, segment = expected
    assert logged_log_storage.leo_map == leo_map
    for segment_id, expected_log in segment:
        log_file_path = (
            tmp_path
            / log_record.partition_dirname
            / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.log"
        )
        with open(log_file_path, "rb") as log_file:
            assert log_file.read() == expected_log
