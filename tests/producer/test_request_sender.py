import asyncio
from unittest import mock

import pytest

from kafka.connection import BrokerConnection
from kafka.dispatcher import ResponseDispatcher
from kafka.producer.accumulator import RecordAccumulator
from kafka.producer.sender import RequestSender


@pytest.fixture
def mock_conn() -> mock.Mock:
    conn = mock.Mock(spec=BrokerConnection)
    conn.send = mock.AsyncMock()
    return conn


@pytest.fixture
def mock_correlation_id_factory() -> mock.Mock:
    return mock.Mock()


@pytest.fixture
def mock_response_dispatcher() -> mock.Mock:
    return mock.Mock(spec=ResponseDispatcher)


@pytest.fixture
def mock_record_accumulator() -> mock.Mock:
    return mock.Mock(spec=RecordAccumulator)


@pytest.fixture
def message_size_limit() -> int:
    return 1024


@pytest.fixture
def request_sender(
    mock_conn: mock.Mock,
    mock_correlation_id_factory: mock.Mock,
    mock_response_dispatcher: mock.Mock,
    mock_record_accumulator: mock.Mock,
    message_size_limit: int,
) -> RequestSender:
    return RequestSender(
        conn=mock_conn,
        correlation_id_factory=mock_correlation_id_factory,
        response_dispatcher=mock_response_dispatcher,
        record_accumulator=mock_record_accumulator,
        message_size_limit=message_size_limit,
    )


@pytest.mark.asyncio
async def test_send_with_no_batches(
    request_sender: RequestSender,
    mock_record_accumulator: mock.Mock,
    mock_conn: mock.Mock,
    mock_response_dispatcher: mock.Mock,
    mock_correlation_id_factory: mock.Mock,
    message_size_limit: int,
):
    """전송할 레코드 배치가 없는 경우"""
    mock_record_accumulator.ready_batches.return_value = []

    with mock.patch("kafka.message.Message") as MessageMock:
        await request_sender.send()

        expected_payload_size_limit = message_size_limit - 10
        mock_record_accumulator.ready_batches.assert_called_once_with(
            expected_payload_size_limit
        )

        assert mock_conn.send.call_count == 0
        assert mock_response_dispatcher.link.call_count == 0
        assert mock_correlation_id_factory.call_count == 0
        assert MessageMock.produce.call_count == 0


@pytest.mark.asyncio
async def test_send_with_single_batch(
    request_sender: RequestSender,
    mock_record_accumulator: mock.Mock,
    mock_conn: mock.Mock,
    mock_response_dispatcher: mock.Mock,
    mock_correlation_id_factory: mock.Mock,
    message_size_limit: int,
):
    """전송할 레코드 배치가 하나인 경우"""
    future1 = asyncio.Future()
    future2 = asyncio.Future()
    produce = mock.Mock(serialized=b"produce-payload-1")
    correlation_id = 100

    mock_record_accumulator.ready_batches.return_value = [(produce, [future1, future2])]
    mock_correlation_id_factory.return_value = correlation_id

    with mock.patch("kafka.message.Message") as MessageMock:
        mock_msg = mock.Mock(serialized=b"serialized-message-100")
        MessageMock.produce.return_value = mock_msg

        await request_sender.send()

        expected_payload_size_limit = message_size_limit - 10
        mock_record_accumulator.ready_batches.assert_called_once_with(
            expected_payload_size_limit
        )

        MessageMock.produce.assert_called_once_with(
            correlation_id=correlation_id, payload=produce.serialized
        )
        mock_conn.send.assert_called_once_with(mock_msg.serialized)
        mock_response_dispatcher.link.assert_called_once_with(
            correlation_id, [future1, future2]
        )


@pytest.mark.asyncio
async def test_send_with_multiple_batches(
    request_sender: RequestSender,
    mock_record_accumulator: mock.Mock,
    mock_conn: mock.Mock,
    mock_response_dispatcher: mock.Mock,
    mock_correlation_id_factory: mock.Mock,
    message_size_limit: int,
):
    """전송할 레코드 배치가 여러 개인 경우"""
    future1 = asyncio.Future()
    future2 = asyncio.Future()
    future3 = asyncio.Future()

    produce1 = mock.Mock(serialized=b"produce-payload-1")
    produce2 = mock.Mock(serialized=b"produce-payload-2")

    mock_record_accumulator.ready_batches.return_value = [
        (produce1, [future1]),
        (produce2, [future2, future3]),
    ]
    mock_correlation_id_factory.side_effect = [100, 200]

    with mock.patch("kafka.message.Message") as MessageMock:
        mock_msg1 = mock.Mock(serialized=b"serialized-message-100")
        mock_msg2 = mock.Mock(serialized=b"serialized-message-200")
        MessageMock.produce.side_effect = [mock_msg1, mock_msg2]

        await request_sender.send()

        expected_payload_size_limit = message_size_limit - 10
        mock_record_accumulator.ready_batches.assert_called_once_with(
            expected_payload_size_limit
        )

        assert MessageMock.produce.call_args_list == [
            mock.call(correlation_id=100, payload=produce1.serialized),
            mock.call(correlation_id=200, payload=produce2.serialized),
        ]
        assert mock_conn.send.call_args_list == [
            mock.call(mock_msg1.serialized),
            mock.call(mock_msg2.serialized),
        ]
        assert mock_response_dispatcher.link.call_args_list == [
            mock.call(100, [future1]),
            mock.call(200, [future2, future3]),
        ]
