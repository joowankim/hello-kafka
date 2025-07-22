import asyncio
from pathlib import Path

from kafka import constants, message
from kafka.broker import request, storage


async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    log_storage = storage.FSLogStorage.load_from_root(
        Path("tmp"), constants.LOG_FILE_SIZE_LIMIT
    )
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
            req = request.CreateTopics.from_message(msg)
            for topic in req.topics:
                log_storage.init_topic(
                    topic_name=topic.name, num_partitions=topic.num_partitions
                )
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
