import asyncio
from collections.abc import Callable
from pathlib import Path
from unittest import mock

import pytest

pytest_plugins = ["tests.fixture"]


@pytest.fixture
def resource_dir() -> Path:
    return Path(__file__).parent / "resource"


@pytest.fixture
def fake_stream_reader_factory() -> Callable[[bytes], asyncio.StreamReader]:
    def _factory(data: bytes) -> asyncio.StreamReader:
        buf = data

        async def _read(size: int) -> bytes:
            nonlocal buf
            data, buf = buf[:size], buf[size:]
            return data

        reader = mock.Mock(spec=asyncio.StreamReader)
        reader.read = _read
        return reader

    return _factory
