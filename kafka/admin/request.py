import json
from typing import Self, Any

import pydantic
from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class NewTopic(pydantic.BaseModel):
    name: str
    num_partitions: int = pydantic.Field(ge=1)
    replication_factor: int = pydantic.Field(ge=1)


class NewTopicList(list[NewTopic]):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return core_schema.no_info_after_validator_function(
            cls.should_not_have_duplicated_topics,
            handler(list[NewTopic]),
        )

    @classmethod
    def should_not_have_duplicated_topics(cls, v: Self) -> Self:
        seen = {topic.name for topic in v}
        if len(seen) != len(v):
            raise ValueError("New topics must not have duplicated names")
        return v

    @property
    def payload(self) -> bytes:
        content = {
            "topics": [
                topic.model_dump(exclude={"replication_factor"}) for topic in self
            ]
        }
        return json.dumps(content).encode("utf-8")
