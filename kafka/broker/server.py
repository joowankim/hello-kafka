import asyncio
from pathlib import Path

from kafka import constants
from kafka.broker import storage, parser, handler


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    log_storage = storage.FSLogStorage.load_from_root(
        Path("tmp"), constants.LOG_FILE_SIZE_LIMIT
    )
    message_parser = parser.MessageParser(reader)
    request_handler = handler.RequestHandler(log_storage)
    try:
        async for msg in message_parser:
            resp = request_handler.handle(msg)
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
