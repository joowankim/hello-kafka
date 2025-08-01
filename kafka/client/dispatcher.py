import asyncio

from kafka import connection, parser
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
