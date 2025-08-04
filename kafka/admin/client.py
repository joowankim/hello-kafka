import asyncio
from collections.abc import Callable

from kafka import connection, message
from kafka.admin import request, dispatcher


class AdminClient:
    def __init__(
        self,
        broker_host: str,
        broker_port: int,
        correlation_id_factory: Callable[[], int],
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.correlation_id_factory = correlation_id_factory
        self._conn: connection.BrokerConnection | None = None
        self._dispatcher: dispatcher.ResponseDispatcher | None = None

    @property
    def is_connected(self) -> bool:
        return (
            self._conn is not None
            and self._conn.is_connected
            and self._dispatcher is not None
        )

    async def loop(self) -> None:
        async with connection.BrokerConnection(
            self.broker_host, self.broker_port
        ) as conn:
            self._conn = conn
            self._dispatcher = dispatcher.ResponseDispatcher(conn)
            while True:
                await self._dispatcher.dispatch()

    def run_loop(self) -> asyncio.Task:
        return asyncio.create_task(self.loop())

    async def create_topics(
        self, new_topics: request.NewTopicList
    ) -> asyncio.Future[bytes]:
        if not self.is_connected:
            raise connection.BrokerConnectionError("Not connected to broker")
        new_correlation_id = self.correlation_id_factory()
        future = asyncio.Future()
        msg = message.Message.create_topics(
            correlation_id=new_correlation_id, payload=new_topics.payload
        )
        self._dispatcher.link(correlation_id=new_correlation_id, future=future)
        await self._conn.send(msg.serialized)

        return future

    async def list_topics(self) -> asyncio.Future[bytes]:
        if not self.is_connected:
            raise connection.BrokerConnectionError("Not connected to broker")
        new_correlation_id = self.correlation_id_factory()
        future = asyncio.Future()
        msg = message.Message.list_topics(new_correlation_id)
        self._dispatcher.link(correlation_id=new_correlation_id, future=future)
        await self._conn.send(msg.serialized)

        return future
