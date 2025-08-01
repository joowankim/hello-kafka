import asyncio
from collections.abc import Callable
from unittest import mock

import pytest

from kafka.client.dispatcher import ResponseDispatcher
from kafka.connection import BrokerConnection
from kafka.error import InvalidCorrelationIdError


@pytest.fixture
def dispatcher(
    fake_stream_reader_factory: Callable[[bytes], asyncio.StreamReader],
    request: pytest.FixtureRequest,
) -> ResponseDispatcher:
    data: bytes = request.param
    fake_stream_reader = fake_stream_reader_factory(data)
    conn = BrokerConnection("localhost", 9092)
    conn._writer = mock.Mock(spec=asyncio.StreamWriter)
    conn._reader = fake_stream_reader
    return ResponseDispatcher(conn=conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatcher",
    [b'0001000016{"topic":"test"}'],
    indirect=True,
)
async def test_dispatch_success(dispatcher: ResponseDispatcher):
    correlation_id = 1
    future = asyncio.Future()
    dispatcher._pending_requests[correlation_id] = future

    await dispatcher.dispatch()

    assert future.done()
    assert future.result() == b'{"topic":"test"}'
    assert correlation_id not in dispatcher._pending_requests


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dispatcher",
    [b'0001000016{"topic":"test"}'],
    indirect=True,
)
async def test_dispatch_invalid_correlation_id(dispatcher: ResponseDispatcher):
    with pytest.raises(InvalidCorrelationIdError):
        await dispatcher.dispatch()
