from collections.abc import Callable

from kafka import message
from kafka.error import UnknownMessageTypeError


class Router:
    def __init__(self):
        self._handlers: dict[
            message.MessageType, Callable[[message.Message], message.Message]
        ] = {}

    def register(
        self,
        msg_type: message.MessageType,
        handler: Callable[[message.Message], message.Message],
    ) -> None:
        self._handlers[msg_type] = handler

    def route(self, req: message.Message) -> message.Message:
        if (handler := self._handlers.get(req.headers.api_key)) is None:
            raise UnknownMessageTypeError(
                f"No handler registered for API key: {req.headers.api_key}"
            )
        return handler(req)
