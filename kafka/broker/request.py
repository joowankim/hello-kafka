from typing import Self

import pydantic
from pydantic import Field

from kafka import message


class CreateTopic(pydantic.BaseModel):
    name: str
    num_partitions: int


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
        return cls.model_validate_json(msg.payload)
