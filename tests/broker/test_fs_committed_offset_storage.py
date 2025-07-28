import shutil
from pathlib import Path

import pytest

from kafka.broker.log import CommittedOffset
from kafka.broker.storage import FSCommittedOffsetStorage


@pytest.fixture
def root_path(
    resource_dir: Path, tmp_path: Path, request: pytest.FixtureRequest
) -> Path:
    root_dirname: str = request.param
    root_path = resource_dir / "roots" / root_dirname
    root_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return tmp_path


@pytest.mark.parametrize(
    "root_path, expected",
    [
        ("root-empty", {}),
        ("root-limit_1GB", {("default-group", "topic01", 0): 0}),
        ("root-limit_100B", {("default-group", "topic01", 0): 1}),
    ],
    indirect=["root_path"],
)
def test_from_root_path(root_path: Path, expected: dict[tuple[str, str, int], int]):
    storage = FSCommittedOffsetStorage.load_from_root(root_path)

    assert storage.cache == expected


@pytest.fixture
def fs_committed_offset_storage(
    resource_dir: Path, tmp_path: Path, request: pytest.FixtureRequest
) -> FSCommittedOffsetStorage:
    root_dirname: str = request.param
    root_path = resource_dir / "roots" / root_dirname
    root_path.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root_path, tmp_path, dirs_exist_ok=True)
    return FSCommittedOffsetStorage.load_from_root(tmp_path)


@pytest.fixture
def committed_offset(
    base_committed_offset: CommittedOffset, request: pytest.FixtureRequest
) -> CommittedOffset:
    offset_params: dict[str, str | int] = request.param
    return base_committed_offset.model_copy(update=offset_params)


@pytest.mark.parametrize(
    "fs_committed_offset_storage, committed_offset, expected",
    [
        (
            "root-empty",
            dict(group_id="default-group", topic="topic01", partition=0, offset=0),
            {("default-group", "topic01", 0): 0},
        ),
        (
            "root-limit_1GB",
            dict(group_id="default-group", topic="topic01", partition=0, offset=1),
            {("default-group", "topic01", 0): 1},
        ),
        (
            "root-limit_100B",
            dict(group_id="default-group", topic="topic01", partition=0, offset=2),
            {("default-group", "topic01", 0): 2},
        ),
    ],
    indirect=["fs_committed_offset_storage", "committed_offset"],
)
def test_update(
    fs_committed_offset_storage: FSCommittedOffsetStorage,
    committed_offset: CommittedOffset,
    expected: dict[tuple[str, str, int], int],
):
    fs_committed_offset_storage.update(committed_offset)

    assert fs_committed_offset_storage.cache == expected
