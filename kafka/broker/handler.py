import json

from kafka import message
from kafka.broker import command
from kafka.broker.storage import FSLogStorage
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class RequestHandler:
    def __init__(self, log_storage: FSLogStorage):
        self.log_storage = log_storage

    def handle(self, req: message.Message) -> message.Message:
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
