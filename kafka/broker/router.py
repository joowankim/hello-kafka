import functools
from collections.abc import Callable

from kafka import message
from kafka.error import UnknownMessageTypeError


class Router:
    def __init__(self):
        self._handlers: dict[
            message.MessageType, Callable[[message.Message], message.Message]
        ] = {}

    def register(
        self, msg_type: message.MessageType
    ) -> Callable[
        [Callable[[message.Message], message.Message]],
        Callable[[message.Message], message.Message],
    ]:
        def decorator(
            handler: Callable[[message.Message], message.Message],
        ) -> Callable[[message.Message], message.Message]:
            self._handlers[msg_type] = handler

            @functools.wraps(handler)
            def wrapper(req: message.Message) -> message.Message:
                return handler(req)

            return wrapper

        return decorator

    def route(self, req: message.Message) -> message.Message:
        if (handler := self._handlers.get(req.headers.api_key)) is None:
            raise UnknownMessageTypeError(
                f"No handler registered for API key: {req.headers.api_key}"
            )
        return handler(req)
