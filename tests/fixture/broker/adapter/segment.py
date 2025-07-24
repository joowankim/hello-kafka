import pytest

from kafka.broker.adapter.segment import FileLogSegment


@pytest.fixture
def base_file_log_segment() -> FileLogSegment:
    return FileLogSegment(
        log_path="test_log.log",
        index_path="test_index.idx",
        size_limit=1024,
    )
