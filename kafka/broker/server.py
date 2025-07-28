import asyncio
from pathlib import Path

from kafka import constants, message
from kafka.broker import storage, parser, handler


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    log_storage = storage.FSLogStorage.load_from_root(
        Path("tmp"), constants.LOG_FILE_SIZE_LIMIT
    )
    committed_offset_storage = storage.FSCommittedOffsetStorage.load_from_root(
        Path("tmp")
    )
    message_parser = parser.MessageParser(reader)
    command_handler = handler.CommandHandler(log_storage, committed_offset_storage)
    query_handler = handler.QueryHandler(log_storage)
    handlers = {
        message.MessageType.CREATE_TOPICS: command_handler,
        message.MessageType.PRODUCE: command_handler,
        message.MessageType.OFFSET_COMMIT: command_handler,
        message.MessageType.FETCH: query_handler,
    }
    try:
        async for msg in message_parser:
            resp = handlers[msg.headers.api_key].handle(msg)
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
