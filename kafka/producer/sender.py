from collections.abc import Callable

from kafka import connection, dispatcher, constants, message
from kafka.producer import accumulator


class RequestSender:
    def __init__(
        self,
        conn: connection.BrokerConnection,
        correlation_id_factory: Callable[[], int],
        response_dispatcher: dispatcher.ProduceDispatcher,
        record_accumulator: accumulator.RecordAccumulator,
        message_size_limit: int,
    ):
        self.conn = conn
        self.correlation_id_factory = correlation_id_factory
        self.dispatcher = response_dispatcher
        self.accumulator = record_accumulator
        self.message_size_limit = message_size_limit

    async def send(self) -> None:
        payload_size_limit = self.message_size_limit - constants.HEADER_WIDTH
        produces = self.accumulator.ready_batches(payload_size_limit)
        for produce, futures in produces:
            correlation_id = self.correlation_id_factory()
            msg = message.Message.produce(
                correlation_id=correlation_id, payload=produce.serialized
            )
            await self.conn.send(msg.serialized)
            self.dispatcher.link(correlation_id, futures)
