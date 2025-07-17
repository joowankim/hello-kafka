from pathlib import Path

from kafka import constants
from kafka.broker import log
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class FSLogStorage:
    def __init__(self, root_path: Path):
        self.root_path = root_path
        if not root_path.exists():
            root_path.mkdir(parents=True, exist_ok=True)

    def init_partition(self, topic_name: str, partition_num: int) -> None:
        partition_path = self.root_path / f"{topic_name}-{partition_num}"
        if not partition_path.exists():
            partition_path.mkdir(parents=True, exist_ok=True)
        log_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
        log_file_path.touch()
        index_file_path = (
            partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.index"
        )
        index_file_path.touch()

    def init_topic(self, topic_name: str, num_partitions: int) -> None:
        if num_partitions <= 0:
            raise InvalidAdminCommandError(
                "Number of partitions must be greater than 0"
            )

        for partition_num in range(num_partitions):
            self.init_partition(topic_name=topic_name, partition_num=partition_num)

    def append_partitions(self, topic_name: str, num_partitions: int) -> None:
        if num_partitions <= 0:
            raise InvalidAdminCommandError(
                "Number of partitions must be greater than 0"
            )

        existing_partitions = len(list(self.root_path.glob(f"{topic_name}-*")))
        for partition_num in range(
            existing_partitions, existing_partitions + num_partitions
        ):
            self.init_partition(topic_name=topic_name, partition_num=partition_num)

    def append_log(self, segment: log.Segment) -> None:
        partition_path = self.root_path / segment.partition_dirname
        if not partition_path.exists():
            raise PartitionNotFoundError("Partition test-topic-1 does not exist")
        log_file_path = partition_path / f"{0:0{constants.LOG_FILENAME_LENGTH}d}.log"
        with log_file_path.open("ab") as log_file:
            log_file.write(segment.value)
