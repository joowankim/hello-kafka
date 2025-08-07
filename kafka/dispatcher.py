import asyncio

from kafka import connection, parser, broker, record
from kafka.error import InvalidCorrelationIdError


class ResponseDispatcher:
    def __init__(self, conn: connection.BrokerConnection):
        self.conn = conn
        self._pending_requests: dict[int, asyncio.Future[bytes]] = {}

    async def dispatch(self) -> None:
        response_parser = parser.MessageParser(self.conn)
        resp = await response_parser.parse()
        if resp is None:
            raise connection.BrokerConnectionError(
                "Connection closed or no data received"
            )
        correlation_id = resp.headers.correlation_id
        if correlation_id not in self._pending_requests:
            raise InvalidCorrelationIdError(
                f"Received response with unknown correlation ID: {correlation_id}"
            )
        future = self._pending_requests.pop(correlation_id)
        future.set_result(resp.payload)

    def link(self, correlation_id: int, future: asyncio.Future[bytes]) -> None:
        if correlation_id in self._pending_requests:
            raise InvalidCorrelationIdError("already linked correlation id")
        self._pending_requests[correlation_id] = future


class ProduceDispatcher(ResponseDispatcher):
    def __init__(self, conn: connection.BrokerConnection):
        super().__init__(conn)
        self._pending_requests: dict[
            int, list[asyncio.Future[record.RecordMetadata]]
        ] = {}

    async def dispatch(self) -> None:
        response_parser = parser.MessageParser(self.conn)
        resp = await response_parser.parse()
        if resp is None:
            raise connection.BrokerConnectionError(
                "Connection closed or no data received"
            )
        correlation_id = resp.headers.correlation_id
        if correlation_id not in self._pending_requests:
            raise InvalidCorrelationIdError(
                f"Received response with unknown correlation ID: {correlation_id}"
            )
        futures = self._pending_requests.pop(correlation_id)
        response = broker.ProduceResponse.deserialize(resp.payload)
        for idx, future in enumerate(futures):
            future.set_result(
                record.RecordMetadata(
                    topic=response.topic,
                    partition=response.partition,
                    offset=response.base_offset + idx,
                    timestamp=response.timestamp,
                )
            )

    def link(
        self, correlation_id: int, futures: list[asyncio.Future[record.RecordMetadata]]
    ) -> None:
        if correlation_id in self._pending_requests:
            raise InvalidCorrelationIdError("already linked correlation id")
        self._pending_requests[correlation_id] = futures
