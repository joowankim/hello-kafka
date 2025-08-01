import asyncio
from collections.abc import AsyncIterator

from kafka import message, constants


class MessageParser:
    def __init__(self, reader: asyncio.StreamReader):
        self.reader = reader

    async def __aiter__(self) -> AsyncIterator[message.Message]:
        while True:
            try:
                msg = await self.parse()
                if msg is None:
                    break
                yield msg
            except (asyncio.IncompleteReadError, StopIteration):
                break

    async def parse(self) -> message.Message | None:
        headers_data = await self.reader.read(constants.HEADER_WIDTH)
        if not headers_data:
            return None
        headers = headers_data.decode("utf-8")
        payload_length = int(headers[-constants.PAYLOAD_LENGTH_WIDTH :])
        payload_data = await self.reader.read(payload_length)
        return message.Message.deserialize(headers_data + payload_data)
