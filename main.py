import asyncio

from kafka.admin.client import AdminClient


async def main():
    client = AdminClient(
        broker_host="localhost", broker_port=8000, correlation_id_factory=lambda: 1
    )
    bg_task = client.run_loop()

    while not client.is_connected:
        await asyncio.sleep(0.1)
    print("Broker connected.")

    try:
        future = await client.list_topics()
        topics = await future
        print(topics)
    finally:
        bg_task.cancel()
        try:
            await bg_task
        except asyncio.CancelledError:
            pass
        print("Broker connection closed.")


if __name__ == "__main__":
    asyncio.run(main())
