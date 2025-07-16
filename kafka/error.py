class KafkaException(Exception):
    def __init__(self, message: str, cause: Exception = None):
        super().__init__(message)
        self.cause = cause

class RetriableException(KafkaException):
    """재시도 가능한 예외"""
    pass

class NonRetriableException(KafkaException):
    """재시도 불가능한 예외"""
    pass
