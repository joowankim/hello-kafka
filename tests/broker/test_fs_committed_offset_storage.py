import shutil
from pathlib import Path

import pytest

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
