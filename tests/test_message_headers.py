import pytest

from kafka.message import MessageHeaders, MessageType


@pytest.mark.parametrize(
    "correlation_id, expected",
    [
        (1, MessageHeaders(correlation_id=1, api_key=MessageType.CREATE_TOPICS)),
        (2, MessageHeaders(correlation_id=2, api_key=MessageType.CREATE_TOPICS)),
        (3, MessageHeaders(correlation_id=3, api_key=MessageType.CREATE_TOPICS)),
    ],
)
def test_create_topics(correlation_id: int, expected: MessageHeaders):
    headers = MessageHeaders.create_topics(correlation_id)

    assert headers == expected


@pytest.mark.parametrize(
    "correlation_id, expected",
    [
        (1, MessageHeaders(correlation_id=1, api_key=MessageType.LIST_TOPICS)),
        (2, MessageHeaders(correlation_id=2, api_key=MessageType.LIST_TOPICS)),
        (3, MessageHeaders(correlation_id=3, api_key=MessageType.LIST_TOPICS)),
    ],
)
def test_list_topics(correlation_id: int, expected: MessageHeaders):
    headers = MessageHeaders.list_topics(correlation_id)

    assert headers == expected
