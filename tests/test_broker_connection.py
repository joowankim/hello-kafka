import asyncio
from asyncio import StreamReader, StreamWriter
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from kafka.connection import BrokerConnection
from kafka.error import BrokerConnectionError


@pytest_asyncio.fixture
async def broker() -> AsyncGenerator[tuple[str, int], None]:
    async def handle_echo(reader: StreamReader, writer: StreamWriter) -> None:
        while True:
            data = await reader.read(100)
            if not data:
                break
            writer.write(data)
            await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_echo, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    yield host, port
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_connect_and_close(broker: tuple[str, int]) -> None:
    host, port = broker
    async with BrokerConnection(host, port) as conn:
        assert conn.is_connected is True
    assert conn.is_connected is False


@pytest.mark.asyncio
async def test_close_unconnected() -> None:
    conn = BrokerConnection("localhost", 9092)
    with pytest.raises(BrokerConnectionError):
        await conn.close()


@pytest.mark.asyncio
async def test_send_and_receive(broker: tuple[str, int]) -> None:
    host, port = broker
    async with BrokerConnection(host, port) as conn:
        data = b"test data"
        await conn.send(data)
        resp = await conn.recv(len(data))
    assert resp == b"test data"


@pytest.mark.asyncio
async def test_send_unconnected() -> None:
    conn = BrokerConnection("localhost", 9092)
    with pytest.raises(BrokerConnectionError):
        await conn.send(b"test")
