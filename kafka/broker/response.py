from typing import Self

import pydantic


class ProduceResponse(pydantic.BaseModel):
    topic: str
    partition: int
    base_offset: int
    error_code: int
    error_message: str | None = None

    @classmethod
    def deserialize(cls, data: bytes) -> Self:
        return cls.model_validate_json(data.decode("utf-8"))

    @classmethod
    def success(cls, topic: str, partition: int, base_offset: int) -> Self:
        return cls(
            topic=topic,
            partition=partition,
            base_offset=base_offset,
            error_code=0,
            error_message=None,
        )

    @classmethod
    def failure(
        cls,
        topic: str,
        partition: int,
        error_code: int,
        error_message: str | None = None,
    ) -> Self:
        return cls(
            topic=topic,
            partition=partition,
            base_offset=-1,
            error_code=error_code,
            error_message=error_message,
        )
