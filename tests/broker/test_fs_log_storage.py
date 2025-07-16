from pathlib import Path

import pytest

from kafka.broker.storage import FSLogStorage


@pytest.fixture
def fs_log_storage(tmp_path: Path) -> FSLogStorage:
    root_path = tmp_path
    return FSLogStorage(root_path=root_path)


def test_init_topic(fs_log_storage: FSLogStorage, tmp_path: Path):
    fs_log_storage.init_topic(topic="test-topic", partition=0)

    for filename in [f"{0:020d}.log", f"{0:020d}.index"]:
        file_path = tmp_path / f"test-topic-0/{filename}"
        assert file_path.exists()
