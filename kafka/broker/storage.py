import base64
import re
from pathlib import Path
from typing import Self, ClassVar

from kafka import constants
from kafka.broker import log
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class FSLogStorage:
    record_pattern: ClassVar[re.Pattern] = re.compile(
        r"(?P<size>\d{4})(?P<payload>{.*})"
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
                logs = log_file.read()
                decoded = base64.b64decode(logs).decode("utf-8")
                record_lengths = list(
                    int(record["size"])
                    for record in cls.record_pattern.finditer(decoded)
                )
                log_end_offset = log_end_segment_id + sum(record_lengths)
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
            # todo: log file의 크기가 충분히 커지면 다음 로그파일을 생성해서 기록하는 기능 추가
            log_file.write(segment.binary_record)
