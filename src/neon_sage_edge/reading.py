"""Reading a Kafka cluster without joining consumer groups or committing offsets.

MirrorReader takes its Kafka clients, decoder and clock as arguments, so tests can
pass fakes; MirrorReader.connect builds the real ones.
"""
from __future__ import annotations

import logging
import time
import uuid
from typing import Callable, Dict, List, Mapping, Optional, Tuple

import pandas as pd
from confluent_kafka import OFFSET_END, ConsumerGroupTopicPartitions, KafkaError, KafkaException, TopicPartition

from .config import KafkaSettings
from .decoding import MessageDecoder
from .frames import message_to_row, rows_to_frame

log = logging.getLogger(__name__)

WATERMARK_COLUMNS = ["partition", "low", "high", "messages"]
TOPIC_COLUMNS = ["topic", "partitions", "messages"]
GROUP_COLUMNS = ["group", "state", "topic", "partition", "committed", "end", "lag"]


def is_internal_topic(topic: str) -> bool:
    """Kafka's own topics (__consumer_offsets, ...) and MirrorMaker's bookkeeping topics
    (heartbeats, and names ending in .internal such as mm2-offsets.primary.internal)."""
    return topic.startswith("_") or topic.endswith(".internal") or topic == "heartbeats"


def reader_consumer_config(client_config: Mapping[str, object]) -> Dict[str, object]:
    """Consumer settings for reading by direct partition assignment: no group membership
    and no commits. librdkafka requires a group.id, but nothing is ever stored under it."""
    return {
        **client_config,
        "group.id": f"neon-sage-edge-{uuid.uuid4().hex[:8]}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
    }


def _is_end_of_partition(msg) -> bool:
    error = msg.error()
    return error is not None and error.code() == KafkaError._PARTITION_EOF


class MirrorReader:
    """Read-only access to a cluster's topics, messages and consumer groups."""

    def __init__(
        self,
        admin,
        consumer,
        decoder: Optional[MessageDecoder] = None,
        timeout: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._admin = admin
        self._consumer = consumer
        self._decoder = decoder or MessageDecoder()
        self._timeout = timeout
        self._clock = clock

    @classmethod
    def connect(cls, settings: KafkaSettings, decoder: Optional[MessageDecoder] = None) -> "MirrorReader":
        """A reader with real Kafka clients for `settings`."""
        from confluent_kafka import Consumer
        from confluent_kafka.admin import AdminClient

        config = settings.client_config()
        return cls(AdminClient(config), Consumer(reader_consumer_config(config)), decoder)

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> "MirrorReader":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- Topics and offsets -------------------------------------------------------------

    def partitions(self, topic: str) -> List[int]:
        """The topic's partition numbers. Raises ValueError for an unknown topic."""
        meta = self._admin.list_topics(topic, timeout=self._timeout).topics.get(topic)
        if meta is None or meta.error is not None:
            reason = f" ({meta.error})" if meta is not None else ""
            raise ValueError(f"Topic {topic!r} not found{reason}.")
        return sorted(meta.partitions)

    def offsets(self, topic: str, partitions: Optional[List[int]] = None) -> Dict[int, Tuple[int, int]]:
        """(oldest retained offset, next offset to be written) for each partition."""
        partitions = self.partitions(topic) if partitions is None else partitions
        return {
            p: self._consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=self._timeout, cached=False)
            for p in partitions
        }

    def watermarks(self, topic: str) -> pd.DataFrame:
        """Oldest offset (low), next offset (high) and messages (high - low) per partition.
        "messages" can overcount slightly on transactional or compacted topics."""
        rows = [{"partition": p, "low": low, "high": high, "messages": high - low}
                for p, (low, high) in self.offsets(topic).items()]
        return pd.DataFrame(rows, columns=WATERMARK_COLUMNS)

    def topics(self, include_internal: bool = False) -> pd.DataFrame:
        """Every topic with its partition count and retained messages."""
        meta = self._admin.list_topics(timeout=self._timeout)
        names = sorted(name for name, t in meta.topics.items()
                       if t.error is None and (include_internal or not is_internal_topic(name)))
        rows = []
        for name in names:
            partitions = sorted(meta.topics[name].partitions)
            retained = sum(high - low for low, high in self.offsets(name, partitions).values())
            rows.append({"topic": name, "partitions": len(partitions), "messages": retained})
        return pd.DataFrame(rows, columns=TOPIC_COLUMNS)

    # --- Messages -----------------------------------------------------------------------

    def read_range(
        self,
        topic: str,
        start: Mapping[int, int],
        end: Mapping[int, int],
        max_messages: Optional[int] = None,
        timeout_s: float = 30.0,
    ) -> pd.DataFrame:
        """Read each partition p from offset start[p] up to, not including, end[p]."""
        todo = {p: s for p, s in start.items() if s < end[p]}
        rows: List[dict] = []
        if not todo:
            return rows_to_frame(rows)
        self._consumer.assign([TopicPartition(topic, p, s) for p, s in todo.items()])
        try:
            remaining = self._collect(todo, end, rows, max_messages, timeout_s)
        finally:
            self._consumer.unassign()
        if remaining:
            log.warning("Stopped after %ss; partitions %s weren't read to the end.", timeout_s, sorted(remaining))
        return rows_to_frame(rows)

    def _collect(self, todo, end, rows, max_messages, timeout_s) -> set:
        """Fill `rows` from the assigned partitions; return the partitions not finished."""
        remaining, deadline = set(todo), self._clock() + timeout_s
        while remaining and self._clock() < deadline:
            for msg in self._consumer.consume(num_messages=500, timeout=1.0):
                if _is_end_of_partition(msg):
                    remaining.discard(msg.partition())
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                p, offset = msg.partition(), msg.offset()
                if p not in remaining or offset < todo[p]:
                    continue
                if offset >= end[p]:
                    remaining.discard(p)
                    continue
                rows.append(message_to_row(msg, self._decoder))
                if offset >= end[p] - 1:
                    remaining.discard(p)
                if max_messages and len(rows) >= max_messages:
                    log.warning("Stopped at %d messages (the max_messages limit); "
                                "pass a larger max_messages to read the rest.", max_messages)
                    return set()
        return remaining

    def peek(self, topic: str, n: int = 20) -> pd.DataFrame:
        """The latest n messages across all partitions."""
        offsets = self.offsets(topic)
        start = {p: max(low, high - n) for p, (low, high) in offsets.items()}
        end = {p: high for p, (_, high) in offsets.items()}
        return self.read_range(topic, start, end).tail(n).reset_index(drop=True)

    def read_all(self, topic: str, max_messages: Optional[int] = 10_000) -> pd.DataFrame:
        """Everything retained in the topic, up to max_messages."""
        offsets = self.offsets(topic)
        return self.read_range(topic, {p: low for p, (low, _) in offsets.items()},
                               {p: high for p, (_, high) in offsets.items()}, max_messages=max_messages)

    def read_since(
        self,
        topic: str,
        minutes: float = 15,
        max_messages: Optional[int] = 10_000,
        now_ms: Optional[int] = None,
    ) -> pd.DataFrame:
        """Messages whose Kafka timestamp falls in the last `minutes` minutes."""
        now_ms = int(time.time() * 1000) if now_ms is None else now_ms
        since_ms = now_ms - int(minutes * 60_000)
        end = {p: high for p, (_, high) in self.offsets(topic).items()}
        found = self._consumer.offsets_for_times([TopicPartition(topic, p, since_ms) for p in end],
                                                 timeout=self._timeout)
        # An offset of -1 means the partition has nothing that recent.
        start = {tp.partition: tp.offset if tp.offset >= 0 else end[tp.partition] for tp in found}
        return self.read_range(topic, start, end, max_messages=max_messages)

    def watch(
        self,
        topic: str,
        seconds: float = 30,
        on_message: Optional[Callable[[dict], None]] = None,
    ) -> pd.DataFrame:
        """Collect messages that arrive during the next `seconds` seconds, calling
        on_message(row) for each as it arrives."""
        self._consumer.assign([TopicPartition(topic, p, OFFSET_END) for p in self.partitions(topic)])
        rows: List[dict] = []
        deadline = self._clock() + seconds
        try:
            while self._clock() < deadline:
                for msg in self._consumer.consume(num_messages=100, timeout=0.5):
                    if _is_end_of_partition(msg):
                        continue  # caught up; nothing new yet
                    if msg.error():
                        raise KafkaException(msg.error())
                    row = message_to_row(msg, self._decoder)
                    rows.append(row)
                    if on_message is not None:
                        on_message(row)
        finally:
            self._consumer.unassign()
        return rows_to_frame(rows)

    # --- Consumer groups --------------------------------------------------------------------

    def consumer_groups(self) -> pd.DataFrame:
        """Every consumer group's committed offsets, with the partition end and the lag."""
        listing = self._admin.list_consumer_groups(request_timeout=self._timeout).result()
        rows = []
        for group in sorted(listing.valid, key=lambda g: g.group_id):
            request = [ConsumerGroupTopicPartitions(group.group_id)]
            result = self._admin.list_consumer_group_offsets(request, request_timeout=self._timeout)
            for tp in result[group.group_id].result().topic_partitions or []:
                if tp.offset < 0:
                    continue
                _, high = self.offsets(tp.topic, [tp.partition])[tp.partition]
                rows.append({"group": group.group_id, "state": str(group.state).split(".")[-1],
                             "topic": tp.topic, "partition": tp.partition, "committed": tp.offset,
                             "end": high, "lag": max(high - tp.offset, 0)})
        return pd.DataFrame(rows, columns=GROUP_COLUMNS)
