import asyncio
from collections.abc import Callable
from unittest import mock

import pytest

from kafka.connection import BrokerConnection, BrokerConnectionError
from kafka.dispatcher import ProduceDispatcher
from kafka.error import InvalidCorrelationIdError
from kafka.record import RecordMetadata


@pytest.fixture
def produce_dispatcher(
    fake_stream_reader_factory: Callable[[bytes], asyncio.StreamReader],
    request: pytest.FixtureRequest,
) -> ProduceDispatcher:
    data: bytes = request.param
    fake_stream_reader = fake_stream_reader_factory(data)
    conn = BrokerConnection("localhost", 9092)
    conn._writer = mock.Mock(spec=asyncio.StreamWriter)
    conn._reader = fake_stream_reader
    return ProduceDispatcher(conn=conn)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "produce_dispatcher",
    [
        b'0001000107{"topic":"test","partition":0,"base_offset":123,"timestamp":1754556963,"error_code":0,"error_message":null}'
    ],
    indirect=True,
)
async def test_dispatch_success(produce_dispatcher: ProduceDispatcher) -> None:
    correlation_id = 1
    future1 = asyncio.Future()
    future2 = asyncio.Future()
    produce_dispatcher._pending_requests[correlation_id] = [future1, future2]

    await produce_dispatcher.dispatch()

    assert future1.done()
    assert future2.done()
    assert future1.result() == RecordMetadata(
        topic="test", partition=0, offset=123, timestamp=1754556963
    )
    assert future2.result() == RecordMetadata(
        topic="test", partition=0, offset=124, timestamp=1754556963
    )
    assert correlation_id not in produce_dispatcher._pending_requests


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "produce_dispatcher",
    [b'0001000016{"topic":"test"}'],  # dummy payload
    indirect=True,
)
async def test_dispatch_invalid_correlation_id(
    produce_dispatcher: ProduceDispatcher,
) -> None:
    with pytest.raises(InvalidCorrelationIdError):
        await produce_dispatcher.dispatch()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "produce_dispatcher",
    [b""],
    indirect=True,
)
async def test_dispatch_connection_error(produce_dispatcher: ProduceDispatcher) -> None:
    with pytest.raises(BrokerConnectionError):
        await produce_dispatcher.dispatch()


@pytest.mark.parametrize(
    "produce_dispatcher",
    [b""],
    indirect=True,
)
def test_link_success(produce_dispatcher: ProduceDispatcher) -> None:
    correlation_id = 1
    futures = [asyncio.Future(), asyncio.Future()]

    produce_dispatcher.link(correlation_id=correlation_id, futures=futures)

    assert correlation_id in produce_dispatcher._pending_requests
    assert produce_dispatcher._pending_requests[correlation_id] is futures


@pytest.mark.parametrize(
    "produce_dispatcher",
    [b""],
    indirect=True,
)
def test_link_duplicate_correlation_id(
    produce_dispatcher: ProduceDispatcher,
) -> None:
    correlation_id = 1
    futures = [asyncio.Future()]
    produce_dispatcher.link(correlation_id=correlation_id, futures=futures)

    with pytest.raises(InvalidCorrelationIdError):
        produce_dispatcher.link(correlation_id=correlation_id, futures=futures)
