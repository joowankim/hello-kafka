import pytest

from kafka.broker.query import Fetch


@pytest.fixture
def base_fetch() -> Fetch:
    return Fetch(
        topic="topic01",
        partition=0,
        offset=100,
        max_bytes=1048576,
    )
