import json

from kafka import message
from kafka.broker import command, log, query, storage
from kafka.error import (
    InvalidAdminCommandError,
    PartitionNotFoundError,
    InvalidOffsetError,
    ExceedSegmentSizeError,
)


def create_topics(
    req: message.Message, log_storage: storage.FSLogStorage
) -> message.Message:
    cmd = command.CreateTopics.from_message(req)
    results = []
    for topic in cmd.topics:
        try:
            log_storage.init_topic(
                topic_name=topic.name, num_partitions=topic.num_partitions
            )
            result = {
                "name": topic.name,
                "error_code": 0,
                "error_message": None,
            }
        except InvalidAdminCommandError as exc:
            result = {
                "name": topic.name,
                "error_code": 10,
                "error_message": str(exc),
            }
        except PartitionNotFoundError as exc:
            result = {
                "name": topic.name,
                "error_code": 11,
                "error_message": str(exc),
            }
        except Exception as exc:
            result = {
                "name": topic.name,
                "error_code": -1,
                "error_message": str(exc),
            }
        finally:
            results.append(result)
    return message.Message(
        headers=req.headers,
        payload=json.dumps({"topics": results}).encode("utf-8"),
    )


def produce(req: message.Message, log_storage: storage.FSLogStorage) -> message.Message:
    cmd = command.Produce.from_message(req)
    records = log.Record.from_produce_command(cmd)
    result = {
        "base_offset": log_storage.partitions[(cmd.topic, cmd.partition)].leo,
        "error_message": None,
    }
    try:
        for record in records:
            log_storage.append_log(record)
        result |= {
            "topic": cmd.topic,
            "partition": cmd.partition,
            "error_code": 0,
        }
    except PartitionNotFoundError as exc:
        result = {
            "topic": cmd.topic,
            "partition": cmd.partition,
            "error_code": 11,
            "base_offset": -1,
            "error_message": str(exc),
        }
    except Exception as exc:
        result = {
            "topic": cmd.topic,
            "partition": cmd.partition,
            "error_code": -1,
            "base_offset": -1,
            "error_message": str(exc),
        }
    return message.Message(
        headers=req.headers,
        payload=json.dumps(result).encode("utf-8"),
    )


def offset_commit(
    req: message.Message, committed_offset_storage: storage.FSCommittedOffsetStorage
) -> message.Message:
    cmd = command.OffsetCommit.from_message(req)
    committed_offsets = log.CommittedOffset.from_offset_commit_command(cmd)
    results = []
    for committed_offset in committed_offsets:
        try:
            committed_offset_storage.update(committed_offset)
            committed_offset_storage.commit()
            result = {
                "topic": committed_offset.topic,
                "partition": committed_offset.partition,
                "error_code": 0,
                "error_message": None,
            }
        except PartitionNotFoundError as exc:
            result = {
                "topic": committed_offset.topic,
                "partition": committed_offset.partition,
                "error_code": 21,
                "error_message": str(exc),
            }
        except Exception as exc:
            result = {
                "topic": committed_offset.topic,
                "partition": committed_offset.partition,
                "error_code": -1,
                "error_message": str(exc),
            }
        finally:
            results.append(result)
    return message.Message(
        headers=req.headers,
        payload=json.dumps({"topics": results}).encode("utf-8"),
    )


def fetch(req: message.Message, log_storage: storage.FSLogStorage) -> message.Message:
    qry = query.Fetch.from_message(req)
    result = {
        "topic": qry.topic,
        "partition": qry.partition,
        "error_code": 0,
        "error_message": None,
        "records": [],
    }
    try:
        records = log_storage.list_logs(qry)
        result["records"] = [
            r.model_dump(exclude={"topic", "partition"}) for r in records
        ]
    except PartitionNotFoundError as exc:
        result = {
            "topic": qry.topic,
            "partition": qry.partition,
            "error_code": 21,
            "error_message": str(exc),
            "records": [],
        }
    except (InvalidOffsetError, ExceedSegmentSizeError) as exc:
        result = {
            "topic": qry.topic,
            "partition": qry.partition,
            "error_code": 20,
            "error_message": str(exc),
            "records": [],
        }
    except Exception as exc:
        result = {
            "topic": qry.topic,
            "partition": qry.partition,
            "error_code": -1,
            "error_message": str(exc),
            "records": [],
        }
    return message.Message(
        headers=req.headers,
        payload=json.dumps(result).encode("utf-8"),
    )


def list_topics(
    req: message.Message, log_storage: storage.FSLogStorage
) -> message.Message:
    topics = log_storage.list_topics()
    result = {
        "topics": topics,
        "error_code": 0,
        "error_message": None,
    }
    return message.Message(
        headers=req.headers,
        payload=json.dumps(result).encode("utf-8"),
    )
