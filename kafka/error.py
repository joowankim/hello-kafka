class KafkaError(Exception):
    def __init__(self, message: str, cause: Exception = None):
        super().__init__(message)
        self.cause = cause


class RetriableError(KafkaError):
    """재시도 가능한 예외"""

    pass


class NonRetriableError(KafkaError):
    """재시도 불가능한 예외"""

    pass


class SerializationError(NonRetriableError):
    """직렬화 또는 역직렬화 중 발생하는 예외"""

    pass


class InvalidAdminCommandError(NonRetriableError):
    """잘못된 관리자 명령어에 대한 예외"""

    pass


class PartitionNotFoundError(NonRetriableError):
    """파티션을 찾을 수 없는 경우 발생하는 예외"""

    pass


class InvalidOffsetError(NonRetriableError):
    """잘못된 오프셋에 대한 예외"""

    pass


class ExceedSegmentSizeError(NonRetriableError):
    """세그먼트 크기를 초과하는 경우 발생하는 예외"""

    pass


class UnknownMessageTypeError(NonRetriableError):
    """알 수 없는 메시지 타입에 대한 예외"""

    pass


class BrokerConnectionError(NonRetriableError):
    """연결 관련 예외"""

    pass
