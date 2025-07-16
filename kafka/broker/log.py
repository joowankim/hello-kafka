from pydantic import BaseModel


class Segment(BaseModel):
    id: str
    topic: str
    partition: int
    value: bytes
