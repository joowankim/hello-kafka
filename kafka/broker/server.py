import asyncio

from kafka.broker import parser
from kafka.broker.handler import router


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
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
