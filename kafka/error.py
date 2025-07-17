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


class SerializationError(KafkaError):
    """직렬화 또는 역직렬화 중 발생하는 예외"""

    pass


class InvalidAdminCommandError(KafkaError):
    """잘못된 관리자 명령어에 대한 예외"""

    pass


class PartitionNotFoundError(KafkaError):
    """파티션을 찾을 수 없는 경우 발생하는 예외"""

    pass
