import json

from kafka.broker import log
from kafka import constants
from kafka.error import ExceedSegmentSizeError


class FileLogSegment(log.Segment):
    def append(self, record: log.Record) -> None:
        binary_data = record.bin

        if self.log_path.stat().st_size + len(binary_data) > self.size_limit:
            raise ExceedSegmentSizeError("Log file size exceeds the maximum limit")

        with self.log_path.open("ab") as log_file:
            pos = log_file.tell()
            log_file.write(binary_data)

        with self.index_path.open("ab") as index_file:
            index_entry = (
                f"{record.offset:0{constants.LOG_RECORD_OFFSET_WIDTH}d}"
                f"{pos:0{constants.LOG_RECORD_POSITION_WIDTH}d}"
            ).encode("utf-8")
            index_file.write(index_entry)

    def read(self, start_offset: int, max_bytes: int) -> list[log.Record]:
        records = []
        total_size = 0
        with self.index_path.open("rb") as index_file:
            while True:
                entry_length = (
                    constants.LOG_RECORD_OFFSET_WIDTH
                    + constants.LOG_RECORD_POSITION_WIDTH
                )
                entry = index_file.read(entry_length)
                if not entry:
                    break
                offset = int(entry[: constants.LOG_RECORD_OFFSET_WIDTH])
                pos = int(entry[constants.LOG_RECORD_OFFSET_WIDTH :])
                if offset >= start_offset:
                    with self.log_path.open("rb") as log_file:
                        log_file.seek(pos)
                        size = int(
                            log_file.read(constants.PAYLOAD_LENGTH_WIDTH).decode(
                                "utf-8"
                            )
                        )
                        data = log_file.read(size)
                        record = log.Record.model_validate(
                            json.loads(data)
                            | {"topic": self.topic, "partition": self.partition}
                        )
                        total_size += record.size
                        if total_size > max_bytes:
                            break
                        records.append(record)
        return records
