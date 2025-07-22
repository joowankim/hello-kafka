import asyncio

from kafka import constants, message
from kafka.broker import request


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    try:
        while True:
            header_data = await reader.read(constants.HEADER_WIDTH)
            if not header_data:
                break
            header = header_data.decode()
            print(f"Header: {header}")
            payload_data = await reader.read(
                int(header[: -constants.PAYLOAD_LENGTH_WIDTH])
            )
            msg = message.Message.deserialize(header_data + payload_data)
            request_payload = request.CreateTopics.from_message(msg)  # noqa: F841
            writer.write(payload_data)
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
