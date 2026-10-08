"""Turning raw Kafka keys and values into Python values."""
from __future__ import annotations

import logging
from typing import Optional, Set, Tuple, Union

from confluent_kafka.serialization import MessageField, SerializationContext

from .config import RegistrySettings
from .registry import Deserializer, make_deserializer

log = logging.getLogger(__name__)

KEY = MessageField.KEY  # "key"
VALUE = MessageField.VALUE  # "value"


def decode_text(raw: Optional[bytes]) -> Union[str, bytes, None]:
    """UTF-8 data as text; anything else stays bytes; None stays None."""
    if raw is None:
        return None
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw


def looks_like_registry_record(raw: bytes) -> bool:
    """True for the schema registry's wire format: a zero byte, a 4-byte schema ID, then data."""
    return len(raw) > 5 and raw[0] == 0


class MessageDecoder:
    """Decodes message keys and values, each with its own deserializer.

    It never raises for a record it can't handle: the record is shown as text
    (or raw bytes), and the reason is logged once per topic and field.
    """

    def __init__(
        self,
        value_deserializer: Optional[Deserializer] = None,
        key_deserializer: Optional[Deserializer] = None,
        value_format: str = "string",
        key_format: str = "string",
    ) -> None:
        self._deserializers = {VALUE: value_deserializer, KEY: key_deserializer}
        self._formats = {VALUE: value_format, KEY: key_format}
        self._reported: Set[Tuple[str, str]] = set()

    @classmethod
    def from_settings(cls, settings: RegistrySettings, client=None) -> "MessageDecoder":
        """A decoder using the registry's deserializers for the formats in `settings`."""
        return cls(
            value_deserializer=make_deserializer(settings.value_format, client),
            key_deserializer=make_deserializer(settings.key_format, client),
            value_format=settings.value_format,
            key_format=settings.key_format,
        )

    def decode(self, raw: Optional[bytes], topic: str, field: str) -> object:
        """Decode one key (`field="key"`) or value (`field="value"`) from `topic`."""
        if raw is None:
            return None
        deserializer = self._deserializers[field]
        if deserializer is None:
            return self._without_deserializer(raw, topic, field)
        try:
            return deserializer(raw, SerializationContext(topic, field))
        except Exception as error:  # any deserializer failure means "not this format"
            self._report(topic, field, f"couldn't deserialize as {self._formats[field]} ({error}); showing them undecoded")
            return raw if looks_like_registry_record(raw) else decode_text(raw)

    def _without_deserializer(self, raw: bytes, topic: str, field: str) -> object:
        if looks_like_registry_record(raw):
            self._report(topic, field, "these look like schema registry records; set a registry and a format to deserialize them")
            return raw
        return decode_text(raw)

    def _report(self, topic: str, field: str, problem: str) -> None:
        if (topic, field) in self._reported:
            return
        self._reported.add((topic, field))
        log.warning("%s (%ss): %s.", topic, field, problem)
