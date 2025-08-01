import asyncio
import io
from types import TracebackType
from typing import IO, Self

from kafka.error import BrokerConnectionError


class BrokerConnection:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._buf: IO[bytes] = io.BytesIO()

    @property
    def is_connected(self) -> bool:
        return self._reader is not None and self._writer is not None

    async def connect(self) -> None:
        reader, writer = await asyncio.open_connection(self.host, self.port)
        self._reader = reader
        self._writer = writer

    async def close(self) -> None:
        if not self.is_connected:
            raise BrokerConnectionError("Connection not established")
        self._writer.close()
        await self._writer.wait_closed()
        self._reader = None
        self._writer = None

    async def send(self, data: bytes) -> None:
        if not self.is_connected:
            raise BrokerConnectionError("Connection not established")
        self._writer.write(data)
        await self._writer.drain()

    async def read(self, n: int) -> bytes:
        if not self.is_connected:
            raise BrokerConnectionError("Connection not established")
        return await self._reader.read(n)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[Exception] | None,
        exc_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()
