from pathlib import Path

import pytest

pytest_plugins = ["tests.fixture"]


@pytest.fixture
def resource_dir() -> Path:
    return Path(__file__).parent / "resource"
