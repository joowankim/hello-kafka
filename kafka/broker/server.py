from pathlib import Path
from kafka import constants, message
from kafka.broker.router import Router
from kafka.broker import handler

import asyncio
import functools

from kafka.broker import parser, storage


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    log_storage = storage.FSLogStorage.load_from_root(
        Path("tmp"), constants.LOG_FILE_SIZE_LIMIT
    )
    committed_offset_storage = storage.FSCommittedOffsetStorage.load_from_root(
        Path("tmp")
    )
    router = Router()
    router.register(
        message.MessageType.CREATE_TOPICS,
        functools.partial(handler.create_topics, log_storage=log_storage),
    )
    router.register(
        message.MessageType.PRODUCE,
        functools.partial(handler.produce, log_storage=log_storage),
    )
    router.register(
        message.MessageType.FETCH,
        functools.partial(handler.fetch, log_storage=log_storage),
    )
    router.register(
        message.MessageType.OFFSET_COMMIT,
        functools.partial(
            handler.offset_commit, committed_offset_storage=committed_offset_storage
        ),
    )
    router.register(
        message.MessageType.LIST_TOPICS,
        functools.partial(handler.list_topics, log_storage=log_storage),
    )
    message_parser = parser.MessageParser(reader)
    try:
        async for msg in message_parser:
            resp = router.route(msg)
            writer.write(resp.serialized)
            await writer.drain()
    except asyncio.CancelledError:
        pass
    finally:
        print("Closing connection")
        writer.close()
        await writer.wait_closed()


async def run_broker():
    server = await asyncio.start_server(handle_client, "localhost", 8000)

    async with server:
        await server.serve_forever()
