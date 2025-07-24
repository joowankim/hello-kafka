from typing import Any
import shutil
from pathlib import Path

import pytest

from kafka import constants
from kafka.broker.log import Record, Partition, Segment
from kafka.broker.query import Fetch
from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


@pytest.fixture
def root_path(
    resource_dir: Path, tmp_path: Path, request: pytest.FixtureRequest
) -> Path:
    root_dirname: str = request.param
    root_path = resource_dir / "roots" / root_dirname
    root_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return tmp_path


@pytest.fixture
def expected_partitions(
    base_segment: Segment,
    base_partition: Partition,
    request: pytest.FixtureRequest,
) -> dict[tuple[str, int], Partition]:
    partitions_params: dict[tuple[str, int], dict[str, Any]] = request.param
    return {
        (topic, num): base_partition.model_copy(
            update=dict(
                topic=partition_data["topic"],
                num=partition_data["num"],
                segments=[
                    base_segment.model_copy(update=segment_data)
                    for segment_data in partition_data["segments"]
                ],
                leo=partition_data["leo"],
            )
        )
        for (topic, num), partition_data in partitions_params.items()
    }


@pytest.mark.parametrize(
    "root_path, expected_partitions",
    [
        ("root-empty", {}),
        (
            "root-limit_1GB",
            {
                ("topic01", 0): dict(
                    topic="topic01",
                    num=0,
                    segments=[dict(base_offset=0)],
                    leo=1,
                ),
                ("topic01", 1): dict(
                    topic="topic01",
                    num=1,
                    segments=[dict(base_offset=0)],
                    leo=0,
                ),
            },
        ),
        (
            "root-limit_100B",
            {
                ("topic01", 0): dict(
                    topic="topic01",
                    num=0,
                    segments=[
                        dict(base_offset=0),
                        dict(base_offset=1),
                    ],
                    leo=2,
                ),
                ("topic01", 1): dict(
                    topic="topic01",
                    num=1,
                    segments=[dict(base_offset=0)],
                    leo=0,
                ),
            },
        ),
    ],
    indirect=["root_path", "expected_partitions"],
)
def test_load_from_root(
    root_path: Path, expected_partitions: dict[tuple[str, int], Partition]
):
    log_file_size_limit = constants.LOG_FILE_SIZE_LIMIT
    log_storage = FSLogStorage.load_from_root(
        root_path=root_path, log_file_size_limit=log_file_size_limit
    )

    assert log_storage.partitions == expected_partitions


@pytest.fixture
def fs_log_storage(tmp_path: Path) -> FSLogStorage:
    root_path = tmp_path
    return FSLogStorage(
        root_path=root_path,
        log_file_size_limit=constants.LOG_FILE_SIZE_LIMIT,
        partitions={},
    )


@pytest.mark.parametrize(
    "topic_name, num_partitions, expected_partitions",
    [
        (
            "test-topic",
            1,
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                )
            },
        ),
        (
            "test-topic",
            2,
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 1): dict(
                    topic="test-topic", num=1, segments=[dict(base_offset=0)], leo=0
                ),
            },
        ),
        (
            "another-topic",
            1,
            {
                ("another-topic", 0): dict(
                    topic="another-topic", num=0, segments=[dict(base_offset=0)], leo=0
                )
            },
        ),
    ],
    indirect=["expected_partitions"],
)
def test_init_topic(
    fs_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    expected_partitions: dict[tuple[str, int], Partition],
):
    fs_log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)

    assert fs_log_storage.partitions == expected_partitions


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
        partitions={},
    )
    log_storage.init_topic(topic_name=topic_name, num_partitions=num_partitions)
    return log_storage


@pytest.mark.parametrize(
    "initiated_log_storage, topic_name, num_partitions, expected_partitions",
    [
        (
            ("test-topic", 3),
            "test-topic",
            1,
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 1): dict(
                    topic="test-topic", num=1, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 2): dict(
                    topic="test-topic", num=2, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 3): dict(
                    topic="test-topic", num=3, segments=[dict(base_offset=0)], leo=0
                ),
            },
        ),
        (
            ("test-topic", 2),
            "test-topic",
            2,
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 1): dict(
                    topic="test-topic", num=1, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 2): dict(
                    topic="test-topic", num=2, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 3): dict(
                    topic="test-topic", num=3, segments=[dict(base_offset=0)], leo=0
                ),
            },
        ),
        (
            ("test-topic", 1),
            "another-topic",
            3,
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("another-topic", 0): dict(
                    topic="another-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("another-topic", 1): dict(
                    topic="another-topic", num=1, segments=[dict(base_offset=0)], leo=0
                ),
                ("another-topic", 2): dict(
                    topic="another-topic", num=2, segments=[dict(base_offset=0)], leo=0
                ),
            },
        ),
    ],
    indirect=["initiated_log_storage", "expected_partitions"],
)
def test_append_partitions(
    initiated_log_storage: FSLogStorage,
    topic_name: str,
    num_partitions: int,
    tmp_path: Path,
    expected_partitions: dict[tuple[str, int], Partition],
):
    initiated_log_storage.append_partitions(
        topic_name=topic_name, num_partitions=num_partitions
    )

    assert initiated_log_storage.partitions == expected_partitions


def test_init_partition(fs_log_storage: FSLogStorage, tmp_path: Path):
    partition_path = tmp_path / "test-topic-0"
    fs_log_storage.init_partition(topic_name="test-topic", partition_num=0)

    log_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
    index_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.index"

    assert log_file_path.exists()
    assert index_file_path.exists()
    assert fs_log_storage.partitions == {
        ("test-topic", 0): Partition(
            topic="test-topic", num=0, segments=[Segment(base_offset=0)], leo=0
        )
    }


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
    "initiated_log_storage, log_record, expected_partitions, expected_records",
    [
        (
            ("test-topic", 1),
            ("test-topic", 0, "dGVzdC12YWx1ZQ==", None, 1752735958, {}),
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=1
                )
            },
            [
                b"0086"
                b"{"
                b'"value":"dGVzdC12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735958,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
            ],
        ),
        (
            ("another-topic", 1),
            ("another-topic", 0, "YW5vdGhlci12YWx1ZQ==", None, 1752735959, {}),
            {
                ("another-topic", 0): dict(
                    topic="another-topic", num=0, segments=[dict(base_offset=0)], leo=1
                ),
            },
            [
                b"0090"
                b"{"
                b'"value":"YW5vdGhlci12YWx1ZQ==",'
                b'"key":null,'
                b'"timestamp":1752735959,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
            ],
        ),
        (
            ("test-topic", 3),
            ("test-topic", 1, "YWRkaXRpb25hbC1kYXRh", None, 1752735960, {}),
            {
                ("test-topic", 0): dict(
                    topic="test-topic", num=0, segments=[dict(base_offset=0)], leo=0
                ),
                ("test-topic", 1): dict(
                    topic="test-topic", num=1, segments=[dict(base_offset=0)], leo=1
                ),
                ("test-topic", 2): dict(
                    topic="test-topic", num=2, segments=[dict(base_offset=0)], leo=0
                ),
            },
            [
                b"",
                b"0090"
                b"{"
                b'"value":"YWRkaXRpb25hbC1kYXRh",'
                b'"key":null,'
                b'"timestamp":1752735960,'
                b'"headers":{},'
                b'"offset":0'
                b"}",
                b"",
            ],
        ),
    ],
    indirect=["initiated_log_storage", "log_record", "expected_partitions"],
)
def test_append_log(
    initiated_log_storage: FSLogStorage,
    log_record: Record,
    tmp_path: Path,
    expected_partitions: dict[tuple[str, int], Partition],
    expected_records: list[bytes],
):
    initiated_log_storage.append_log(log_record)

    assert initiated_log_storage.partitions == expected_partitions
    for partition, record in zip(expected_partitions.values(), expected_records):
        log_file_path = tmp_path / partition.name / partition.active_segment.log
        with open(log_file_path, "rb") as log_file:
            assert log_file.read() == record


@pytest.mark.parametrize(
    "initiated_log_storage, log_record",
    [
        (
            ("test-topic", 1),
            ("test-topic", 1, "dGVzdC12YWx1ZQ==", None, 1752735960, {}),
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
    root_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return FSLogStorage.load_from_root(
        root_path=tmp_path, log_file_size_limit=log_file_size_limit
    )


@pytest.mark.parametrize(
    "logged_log_storage, log_record, expected_partitions, expected_records",
    [
        (
            ("root-limit_1GB", 1024**3),
            ("topic01", 0, "c2Vjb25kLWxvZw==", None, 1752735962, {}),
            {
                ("topic01", 0): dict(
                    topic="topic01", num=0, segments=[dict(base_offset=0)], leo=2
                ),
                ("topic01", 1): dict(
                    topic="topic01", num=1, segments=[dict(base_offset=0)], leo=0
                ),
            },
            [
                (
                    ("topic01", 0),
                    [
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
                    ],
                ),
            ],
        ),
        (
            ("root-limit_100B", 100),
            ("topic01", 1, "c2Vjb25kLWxvZw==", None, 1752735962, {}),
            {
                ("topic01", 0): dict(
                    topic="topic01",
                    num=0,
                    segments=[dict(base_offset=0), dict(base_offset=1)],
                    leo=2,
                ),
                ("topic01", 1): dict(
                    topic="topic01", num=1, segments=[dict(base_offset=0)], leo=1
                ),
            },
            [
                (
                    ("topic01", 0),
                    [
                        b"0086"
                        b"{"
                        b'"value":"aW5pdGlhbC1sb2c=",'
                        b'"key":null,'
                        b'"timestamp":1752735961,'
                        b'"headers":{},'
                        b'"offset":0'
                        b"}",
                        b"0086"
                        b"{"
                        b'"value":"aW5pdGlhbC1sb2c=",'
                        b'"key":null,'
                        b'"timestamp":1752735961,'
                        b'"headers":{},'
                        b'"offset":1'
                        b"}",
                    ],
                ),
                (
                    ("topic01", 1),
                    [
                        b"0086"
                        b"{"
                        b'"value":"c2Vjb25kLWxvZw==",'
                        b'"key":null,'
                        b'"timestamp":1752735962,'
                        b'"headers":{},'
                        b'"offset":0'
                        b"}",
                    ],
                ),
            ],
        ),
    ],
    indirect=["logged_log_storage", "log_record", "expected_partitions"],
)
def test_append_log_to_already_logged_partition(
    logged_log_storage: FSLogStorage,
    log_record: Record,
    tmp_path: Path,
    expected_partitions: dict[tuple[str, int], Partition],
    expected_records: list[tuple[tuple[str, int], list[bytes]]],
):
    logged_log_storage.append_log(log_record)

    assert logged_log_storage.partitions == expected_partitions
    for partition_key, records in expected_records:
        partition = expected_partitions[partition_key]
        for segment, record in zip(partition.segments, records):
            log_file_path = tmp_path / partition.name / segment.log
            with open(log_file_path, "rb") as log_file:
                assert log_file.read() == record


@pytest.mark.parametrize(
    "logged_log_storage, qry, expected",
    [
        (
            ("root-limit_1GB", 1024**3),
            Fetch(topic="topic01", partition=0, offset=0, max_bytes=100),
            [
                Record(
                    topic="topic01",
                    partition=0,
                    value="aW5pdGlhbC1sb2c=",
                    key=None,
                    timestamp=1752735961,
                    headers={},
                    offset=0,
                ),
            ],
        ),
        (
            ("root-limit_1GB", 1024**3),
            Fetch(topic="topic01", partition=0, offset=1, max_bytes=100),
            [],
        ),
        (
            ("root-limit_100B", 100),
            Fetch(topic="topic01", partition=0, offset=0, max_bytes=100),
            [
                Record(
                    topic="topic01",
                    partition=0,
                    value="aW5pdGlhbC1sb2c=",
                    key=None,
                    timestamp=1752735961,
                    headers={},
                    offset=0,
                ),
            ],
        ),
        (
            ("root-limit_100B", 100),
            Fetch(topic="topic01", partition=0, offset=0, max_bytes=10000),
            [
                Record(
                    topic="topic01",
                    partition=0,
                    value="aW5pdGlhbC1sb2c=",
                    key=None,
                    timestamp=1752735961,
                    headers={},
                    offset=0,
                ),
                Record(
                    topic="topic01",
                    partition=0,
                    value="aW5pdGlhbC1sb2c=",
                    key=None,
                    timestamp=1752735961,
                    headers={},
                    offset=1,
                ),
            ],
        ),
    ],
    indirect=["logged_log_storage"],
)
def test_list_logs(
    logged_log_storage: FSLogStorage, qry: Fetch, expected: list[Record]
):
    records = logged_log_storage.list_logs(qry)

    assert records == expected
