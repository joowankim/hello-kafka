import json
from collections.abc import Callable

from kafka import message
from kafka.broker import command, log
from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class RequestHandler:
    def __init__(self, log_storage: FSLogStorage):
        self.log_storage = log_storage

    def handle(self, req: message.Message) -> message.Message:
        handlers: dict[
            message.MessageType, Callable[[message.Message], message.Message]
        ] = {
            message.MessageType.CREATE_TOPICS: self._handle_create_topics,
            message.MessageType.PRODUCE: self._handle_produce,
        }
        return handlers[req.headers.api_key](req)

    def _handle_create_topics(self, req: message.Message) -> message.Message:
        cmd = command.CreateTopics.from_message(req)
        results = []
        for topic in cmd.topics:
            try:
                self.log_storage.init_topic(
                    topic_name=topic.name, num_partitions=topic.num_partitions
                )
                result = {
                    "name": topic.name,
                    "error_code": 0,
                    "error_message": None,
                }
            except InvalidAdminCommandError as exc:
                result = {
                    "name": topic.name,
                    "error_code": 10,
                    "error_message": str(exc),
                }
            except PartitionNotFoundError as exc:
                result = {
                    "name": topic.name,
                    "error_code": 11,
                    "error_message": str(exc),
                }
            except Exception as exc:
                result = {
                    "name": topic.name,
                    "error_code": -1,
                    "error_message": str(exc),
                }
            finally:
                results.append(result)
        return message.Message(
            headers=req.headers,
            payload=json.dumps({"topics": results}).encode("utf-8"),
        )

    def _handle_produce(self, req: message.Message) -> message.Message:
        cmd = command.Produce.from_message(req)
        records = log.Record.from_produce_command(cmd)
        result = {
            "base_offset": self.log_storage.leo_map[(cmd.topic, cmd.partition)],
            "error_message": None,
        }
        try:
            for record in records:
                self.log_storage.append_log(record)
            result |= {
                "topic": cmd.topic,
                "partition": cmd.partition,
                "error_code": 0,
            }
        except PartitionNotFoundError as exc:
            result = {
                "topic": cmd.topic,
                "partition": cmd.partition,
                "error_code": 11,
                "base_offset": -1,
                "error_message": str(exc),
            }
        except Exception as exc:
            result = {
                "topic": cmd.topic,
                "partition": cmd.partition,
                "error_code": -1,
                "base_offset": -1,
                "error_message": str(exc),
            }
        return message.Message(
            headers=req.headers,
            payload=json.dumps(result).encode("utf-8"),
        )
