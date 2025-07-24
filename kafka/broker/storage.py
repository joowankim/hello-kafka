import re
from pathlib import Path
from typing import Self, ClassVar

from kafka import constants
from kafka.broker import log, query
from kafka.error import InvalidAdminCommandError, PartitionNotFoundError


class FSLogStorage:
    record_pattern: ClassVar[re.Pattern] = re.compile(
        r"(?P<size>\d{4})(?P<payload>{.*?})"
    )

    def __init__(
        self,
        root_path: Path,
        log_file_size_limit: int,
        partitions: dict[tuple[str, int], log.Partition],
    ):
        self.root_path = root_path
        self.log_file_size_limit = log_file_size_limit
        self.partitions = partitions
        if not root_path.exists():
            root_path.mkdir(parents=True, exist_ok=True)

    @classmethod
    def load_from_root(cls, root_path: Path, log_file_size_limit: int) -> Self:
        partitions = []
        for partition_path in root_path.glob("*-*"):
            topic_name, partition_num = partition_path.name.split("-")
            base_offsets = sorted(int(p.stem) for p in partition_path.glob("*.log"))
            if not base_offsets:
                continue
            segments = [log.Segment(base_offset=offset) for offset in base_offsets]
            with (partition_path / segments[-1].log).open("rb") as log_file:
                records = []
                while payload_size_str := log_file.read(constants.PAYLOAD_LENGTH_WIDTH):
                    payload_size = int(payload_size_str)
                    payload_data = log_file.read(payload_size).decode("utf-8")
                    records.append(payload_data)
                log_end_offset = segments[-1].base_offset + len(records)
            partitions.append(
                log.Partition(
                    topic=topic_name,
                    num=int(partition_num),
                    segments=segments,
                    leo=log_end_offset,
                )
            )
        return cls(
            root_path=root_path,
            log_file_size_limit=log_file_size_limit,
            partitions={(p.topic, p.num): p for p in partitions},
        )

    def init_partition(self, topic_name: str, partition_num: int) -> None:
        partition_path = self.root_path / f"{topic_name}-{partition_num}"
        if not partition_path.exists():
            partition_path.mkdir(parents=True, exist_ok=True)
        new_segment = log.Segment(base_offset=0)
        log_file_path = partition_path / new_segment.log
        log_file_path.touch()
        index_file_path = partition_path / new_segment.index
        index_file_path.touch()
        self.partitions[(topic_name, partition_num)] = log.Partition(
            topic=topic_name,
            num=partition_num,
            segments=[new_segment],
            leo=0,
        )

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
        if (partition := self.partitions.get((record.topic, record.partition))) is None:
            raise PartitionNotFoundError(
                f"Partition {record.partition_name} does not exist"
            )
        partition_path = self.root_path / partition.name
        log_path = partition_path / partition.active_segment.log
        index_path = partition_path / partition.active_segment.index
        new_record = record.record_at(partition.leo)
        new_record_binary = new_record.bin
        current_log_file_size = log_path.stat().st_size + len(new_record_binary)
        if current_log_file_size > self.log_file_size_limit:
            partition = partition.roll()
            log_path = partition_path / partition.active_segment.log
            index_path = partition_path / partition.active_segment.index
            log_path.touch()
            index_path.touch()
        with log_path.open("ab") as log_file:
            position = log_file.tell()
            log_file.write(new_record_binary)
        with index_path.open("ab") as index_file:
            index_file.write(new_record.index_entry(position))
        self.partitions[(partition.topic, partition.num)] = partition.commit_record()

    def list_logs(self, qry: query.Fetch) -> list[log.Record]:
        if (partition := self.partitions.get((qry.topic, qry.partition))) is None:
            raise PartitionNotFoundError(
                f"Partition {qry.topic}-{qry.partition} does not exist"
            )
        partition_path = self.root_path / partition.name
        over_start_offset = [
            s for s in partition.segments if s.base_offset > qry.offset
        ]
        read_targets = partition.segments[-(1 + len(over_start_offset)) :]
        total_record_size = 0
        result = []
        for segment in read_targets:
            log_path = partition_path / segment.log
            index_path = partition_path / segment.index
            with index_path.open("rb") as index_file:
                with log_path.open("rb") as log_file:
                    while total_record_size < qry.max_bytes:
                        index_entry = index_file.read(
                            constants.LOG_RECORD_OFFSET_WIDTH
                            + constants.LOG_RECORD_POSITION_WIDTH
                        )
                        if not index_entry:
                            break
                        offset, pos = (
                            int(index_entry[: constants.LOG_RECORD_OFFSET_WIDTH]),
                            int(index_entry[constants.LOG_RECORD_OFFSET_WIDTH :]),
                        )
                        if offset < qry.offset:
                            continue
                        log_file.seek(pos)
                        record_size_str = log_file.read(constants.PAYLOAD_LENGTH_WIDTH)
                        if not record_size_str:
                            break
                        record_size = int(record_size_str)
                        record = log.Record.from_log(
                            topic=partition.topic,
                            partition=partition.num,
                            record_data=log_file.read(record_size),
                        )
                        if total_record_size + record.size > qry.max_bytes:
                            break
                        result.append(record)
                        total_record_size += record.size

        return result
