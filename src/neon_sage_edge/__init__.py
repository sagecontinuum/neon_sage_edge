"""Read-only tools for the sage-kafka mirror: topics, messages, schema registry
decoding and consumer groups.

Typical use::

    from neon_sage_edge import KafkaSettings, RegistrySettings, MessageDecoder, MirrorReader, make_registry_client

    registry = RegistrySettings.from_env()
    decoder = MessageDecoder.from_settings(registry, make_registry_client(registry))
    with MirrorReader.connect(KafkaSettings.from_env(), decoder) as reader:
        latest = reader.peek("my-topic", n=20)
"""
from .config import FORMATS, KafkaSettings, RegistrySettings
from .decoding import MessageDecoder, decode_text, looks_like_registry_record
from .dns import clear_host_overrides, host_overrides, override_host
from .logs import show_notes
from .frames import (
    epoch_unit,
    expand_json,
    format_row,
    message_to_row,
    per_minute_counts,
    rows_to_frame,
    time_range,
    to_utc_datetimes,
)
from .reading import MirrorReader, is_internal_topic, reader_consumer_config
from .registry import make_deserializer, make_registry_client, registry_subjects

__version__ = "0.1.0"

__all__ = [
    "FORMATS",
    "KafkaSettings",
    "RegistrySettings",
    "MessageDecoder",
    "decode_text",
    "looks_like_registry_record",
    "override_host",
    "host_overrides",
    "clear_host_overrides",
    "epoch_unit",
    "expand_json",
    "format_row",
    "message_to_row",
    "per_minute_counts",
    "rows_to_frame",
    "time_range",
    "to_utc_datetimes",
    "MirrorReader",
    "is_internal_topic",
    "reader_consumer_config",
    "show_notes",
    "make_deserializer",
    "make_registry_client",
    "registry_subjects",
]
