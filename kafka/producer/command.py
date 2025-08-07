import pydantic


class RecordContents(pydantic.BaseModel):
    value: str
    key: str | None = None
    timestamp: int | None = None
    headers: dict[str, str] = pydantic.Field(default_factory=dict)
