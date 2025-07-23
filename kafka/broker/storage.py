import re
from pathlib import Path
from typing import Self, ClassVar

from kafka import constants
from kafka.broker import log
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class FSLogStorage:
    record_pattern: ClassVar[re.Pattern] = re.compile(
        r"(?P<size>\d{4})(?P<payload>{.*?})"
    )

    def __init__(
        self,
        root_path: Path,
        log_file_size_limit: int,
        leo_map: dict[tuple[str, int], int],
    ):
        self.root_path = root_path
        self.log_file_size_limit = log_file_size_limit
        self.leo_map = leo_map
        if not root_path.exists():
            root_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load_from_root(cls, root_path: Path, log_file_size_limit: int) -> Self:
        leo_map = {}
        for partition_path in root_path.glob("*-*"):
            topic_name, partition_num = partition_path.name.split("-")
            log_end_segment_id = max(int(p.stem) for p in partition_path.glob("*.log"))
            with (
                partition_path
                / f"{log_end_segment_id:0{constants.LOG_FILENAME_LENGTH}d}.log"
            ).open("rb") as log_file:
                records = []
                while payload_size_str := log_file.read(constants.PAYLOAD_LENGTH_WIDTH):
                    payload_size = int(payload_size_str)
                    record = log_file.read(payload_size).decode("utf-8")
                    records.append(record)
                log_end_offset = log_end_segment_id + len(records)
            leo_map[(topic_name, int(partition_num))] = log_end_offset
        return cls(
            root_path=root_path,
            log_file_size_limit=log_file_size_limit,
            leo_map=leo_map,
        )

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
        self.leo_map[(topic_name, partition_num)] = 0

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

    def append_log(self, record: log.Record) -> None:
        partition_path = self.root_path / record.partition_dirname
        if not partition_path.exists():
            raise PartitionNotFoundError("Partition test-topic-1 does not exist")
        segment_id = max(int(p.stem) for p in partition_path.glob("*.log"))
        log_path = (
            partition_path / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.log"
        )
        index_path = (
            partition_path / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.index"
        )
        new_record = record.record_at(self.leo_map[(record.topic, record.partition)])
        new_record_binary = new_record.bin
        current_log_file_size = log_path.stat().st_size + len(new_record_binary)
        if current_log_file_size > self.log_file_size_limit:
            segment_id += 1
            log_path = (
                partition_path / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.log"
            )
            index_path = (
                partition_path / f"{segment_id:0{constants.LOG_FILENAME_LENGTH}d}.index"
            )
            log_path.touch()
            index_path.touch()
        with log_path.open("ab") as log_file:
            log_file.write(new_record_binary)
        with index_path.open("ab") as index_file:
            index_file.write(
                f"{new_record.offset:0{constants.LOG_RECORD_OFFSET_WIDTH}d}"
                f"{current_log_file_size:0{constants.LOG_RECORD_POSITION_WIDTH}d}".encode(
                    "utf-8"
                )
            )
        self.leo_map[(record.topic, record.partition)] += 1
