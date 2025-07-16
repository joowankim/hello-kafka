from pathlib import Path

from kafka import constants


class FSLogStorage:
    def __init__(self, root_path: Path):
        self.root_path = root_path
        if not root_path.exists():
            root_path.mkdir(parents=True, exist_ok=True)

    def init_topic(self, topic: str, partition: int) -> None:
        topic_path = self.root_path / f"{topic}-{partition}"
        if topic_path.exists():
            return
        topic_path.mkdir(exist_ok=True)
        log_file_path = topic_path / f"{0:0{constants.LOG_SEGMENT_FILENAME_LENGTH}d}.log"
        log_file_path.touch()
        index_file_path = topic_path / f"{0:0{constants.LOG_SEGMENT_FILENAME_LENGTH}d}.index"
        index_file_path.touch()
