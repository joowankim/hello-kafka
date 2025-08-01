import json
import time
from typing import Self

import pydantic
from pydantic import Field

from kafka import message


class CreateTopic(pydantic.BaseModel):
    name: str
    num_partitions: int = Field(ge=1)


class CreateTopics(pydantic.BaseModel):
    topics: list[CreateTopic] = Field(min_length=1)

    @pydantic.model_validator(mode="after")
    def should_not_have_duplicated_topic_names(self) -> Self:
        topic_names = {topic.name for topic in self.topics}
        if len(topic_names) != len(self.topics):
            raise ValueError("Duplicate topic names found in CreateTopics request")
        return self

    @classmethod
    def from_message(cls, msg: message.Message) -> Self:
        if msg.headers.api_key != message.MessageType.CREATE_TOPICS:
            raise ValueError("Message is not of type CREATE_TOPICS")
        return cls.model_validate_json(msg.payload.decode("utf-8"))


class RecordContents(pydantic.BaseModel):
    value: str
    key: str | None
    timestamp: int | None
    headers: dict[str, str]


class Produce(pydantic.BaseModel):
    topic: str
    partition: int
    records: list[RecordContents] = Field(min_length=1)

    @classmethod
    def from_message(cls, msg: message.Message) -> Self:
        if msg.headers.api_key != message.MessageType.PRODUCE:
            raise ValueError("Message is not of type PRODUCE")
        params = json.loads(msg.payload.decode("utf-8"))
        if params.get("records") is None:
            raise ValueError("Produce command must have a 'records' field")
        for idx, record in enumerate(params["records"]):
            if record.get("timestamp") is None:
                record["timestamp"] = int(time.time())
            params["records"][idx] = record
        return cls.model_validate(params)


class TopicOffset(pydantic.BaseModel):
    topic: str
    partition: int
    offset: int


class OffsetCommit(pydantic.BaseModel):
    group_id: str
    topics: list[TopicOffset] = Field(min_length=1)

    @classmethod
    def from_message(cls, msg: message.Message) -> Self:
        if msg.headers.api_key != message.MessageType.OFFSET_COMMIT:
            raise ValueError("Message is not of type OFFSET_COMMIT")
        return cls.model_validate_json(msg.payload.decode("utf-8"))
