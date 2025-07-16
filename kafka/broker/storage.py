from pathlib import Path

from kafka import constants
from kafka.error import InvalidAdminCommandError


class FSLogStorage:
    def __init__(self, root_path: Path):
        self.root_path = root_path
        if not root_path.exists():
            root_path.mkdir(parents=True, exist_ok=True)

    def init_topic(self, topic_name: str, num_partitions: int) -> None:
        if num_partitions <= 0:
            raise InvalidAdminCommandError("Number of partitions must be greater than 0")

        partition_paths = [self.root_path / f"{topic_name}-{partition}" for partition in range(num_partitions)]
        for partition_path in partition_paths:
            partition_path.mkdir(parents=True, exist_ok=True)
            log_file_path = partition_path / f"{0:0{constants.LOG_SEGMENT_FILENAME_LENGTH}d}.log"
            log_file_path.touch()
            index_file_path = partition_path / f"{0:0{constants.LOG_SEGMENT_FILENAME_LENGTH}d}.index"
            index_file_path.touch()
