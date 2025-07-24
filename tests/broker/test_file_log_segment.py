import shutil
from pathlib import Path
from typing import Any

import pytest

from kafka import constants
from kafka.broker.adapter.segment import FileLogSegment
from kafka.broker.log import Record
from kafka.error import ExceedSegmentSizeError


@pytest.fixture
def file_log_segment(
    resource_dir: Path,
    tmp_path: Path,
    base_file_log_segment: FileLogSegment,
    request: pytest.FixtureRequest,
) -> FileLogSegment:
    root_dirname, partition_dirname, segment_id, size_limit = request.param
    root_path = resource_dir / "roots" / root_dirname
    root_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return base_file_log_segment.model_copy(
        update=dict(
            log_path=tmp_path
            / partition_dirname
            / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.log",
            index_path=tmp_path
            / partition_dirname
            / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.index",
            size_limit=size_limit,
        )
    )


@pytest.fixture
def record(base_log_record: Record, request: pytest.FixtureRequest) -> Record:
    record_param: dict[str, Any] = request.param
    return base_log_record.model_copy(update=record_param)


@pytest.fixture
def expected(base_log_record: Record, request: pytest.FixtureRequest) -> list[Record]:
    records: list[dict[str, Any]] = request.param
    return [base_log_record.model_copy(update=record_param) for record_param in records]


@pytest.mark.parametrize(
    "file_log_segment, record, expected",
    [
        (
            ("root-limit_1GB", "topic01-0", 0, 1024**3),
            dict(
                topic="topic01",
                partition=0,
                value="c2Vjb25kLWxvZw==",
                key=None,
                timestamp=1752735962,
                headers={},
                offset=1,
            ),
            [
                dict(
                    topic="topic01",
                    partition=0,
                    value="aW5pdGlhbC1sb2c=",
                    key=None,
                    timestamp=1752735961,
                    headers={},
                    offset=0,
                ),
                dict(
                    topic="topic01",
                    partition=0,
                    value="c2Vjb25kLWxvZw==",
                    key=None,
                    timestamp=1752735962,
                    headers={},
                    offset=1,
                ),
            ],
        ),
    ],
    indirect=["file_log_segment", "record", "expected"],
)
def test_append_and_read(
    file_log_segment: FileLogSegment, record: Record, expected: list[Record]
):
    file_log_segment.append(record)

    records = file_log_segment.read(start_offset=0, max_bytes=1024)

    assert records == expected


@pytest.mark.parametrize(
    "file_log_segment, record",
    [
        (
            ("root-limit_100B", "topic01-0", 0, 100),
            dict(
                topic="topic01",
                partition=0,
                value="c2Vjb25kLWxvZw==",
                key=None,
                timestamp=1752735962,
                headers={},
                offset=3,
            ),
        ),
    ],
    indirect=["file_log_segment", "record"],
)
def test_append_with_over_size_record(file_log_segment: FileLogSegment, record: Record):
    with pytest.raises(ExceedSegmentSizeError):
        file_log_segment.append(record)


@pytest.mark.parametrize(
    "file_log_segment, start_offset, max_bytes, expected",
    [
        (
            ("root-limit_100B", "topic01-0", 0, 100),
            1,
            1024,
            [],
        ),
        (
            ("root-limit_100B", "topic01-0", 0, 100),
            0,
            50,
            [],
        ),
        (
            ("root-limit_100B", "topic01-0", 0, 100),
            0,
            100,
            [
                dict(
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
    ],
    indirect=["file_log_segment", "expected"],
)
def test_read(
    file_log_segment: FileLogSegment,
    start_offset: int,
    max_bytes: int,
    expected: list[Record],
):
    records = file_log_segment.read(start_offset=start_offset, max_bytes=max_bytes)

    assert records == expected
