"""Tables of messages, and helpers that analyse them.

Everything here is a pure function of its inputs: no Kafka connection is needed,
which keeps it easy to test with plain Python objects.
"""
from __future__ import annotations

import json
import logging
from typing import Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple

import pandas as pd

from .decoding import KEY, VALUE, MessageDecoder, decode_text

log = logging.getLogger(__name__)

MESSAGE_COLUMNS = ["partition", "offset", "timestamp", "key", "value", "headers"]
RANGE_COLUMNS = ["messages", "min", "max", "span"]


class MessageLike(Protocol):
    """The parts of confluent_kafka.Message this module uses."""

    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def timestamp(self) -> Tuple[int, int]: ...
    def key(self) -> Optional[bytes]: ...
    def value(self) -> Optional[bytes]: ...
    def headers(self) -> Optional[List[Tuple[str, bytes]]]: ...


# --- Messages to tables ---------------------------------------------------------------

def message_to_row(msg: MessageLike, decoder: MessageDecoder) -> dict:
    """One message as a dict with MESSAGE_COLUMNS; the timestamp in epoch milliseconds."""
    _, timestamp_ms = msg.timestamp()
    return {
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": timestamp_ms if timestamp_ms > 0 else None,
        "key": decoder.decode(msg.key(), msg.topic(), KEY),
        "value": decoder.decode(msg.value(), msg.topic(), VALUE),
        "headers": {name: decode_text(data) for name, data in (msg.headers() or [])} or None,
    }


def rows_to_frame(rows: Sequence[Mapping]) -> pd.DataFrame:
    """Rows from message_to_row as a table sorted by time, with UTC timestamps."""
    frame = pd.DataFrame(list(rows), columns=MESSAGE_COLUMNS)
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="ms", utc=True)
    return frame.sort_values(["timestamp", "partition", "offset"], ignore_index=True)


def format_row(row: Mapping, width: int = 80) -> str:
    """A one-line summary of a message row, e.g. for printing messages as they arrive."""
    value = str(row["value"])[:width]
    return f"partition {row['partition']}  offset {row['offset']}  key={row['key']!r}  value={value!r}"


# --- Values to columns ----------------------------------------------------------------

def _as_record(value: object) -> dict:
    """A dict for deserialized records and JSON-object text; an empty dict otherwise."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def expand_json(frame: pd.DataFrame, column: str = "value") -> pd.DataFrame:
    """Replace `column` with one column per field of its records (nested fields as
    parent.child). Fields named like an existing column keep a "<column>." prefix,
    so no two columns share a name."""
    fields = pd.json_normalize([_as_record(v) for v in frame[column]])
    others = set(frame.columns) - {column}
    fields = fields.rename(columns=lambda name: f"{column}.{name}" if name in others else name)
    fields.index = frame.index
    return pd.concat([frame.drop(columns=[column]), fields], axis=1)


# --- Time fields --------------------------------------------------------------------------

def epoch_unit(magnitude: float) -> str:
    """The unit of an epoch number judged by its size: "s", "ms", "us" or "ns"."""
    if magnitude > 1e17:
        return "ns"
    if magnitude > 1e14:
        return "us"
    if magnitude > 1e11:
        return "ms"
    return "s"


def to_utc_datetimes(values: Iterable) -> pd.Series:
    """Datetimes, epoch numbers (any unit, see epoch_unit) or date strings as UTC
    timestamps. Missing values are dropped; unparseable strings become NaT."""
    series = pd.Series(list(values) if not isinstance(values, pd.Series) else values).dropna()
    if series.empty:
        return pd.Series(dtype="datetime64[ns, UTC]")
    if pd.api.types.is_datetime64_any_dtype(series):
        return pd.to_datetime(series, utc=True)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_datetime(series, unit=epoch_unit(series.abs().max()), utc=True)
    return pd.to_datetime(series, utc=True, errors="coerce", format="mixed")


def _range_row(times: pd.Series) -> dict:
    return {"messages": int(times.notna().sum()), "min": times.min(), "max": times.max(),
            "span": times.max() - times.min()}


def time_range(frame: pd.DataFrame, field: str = "readout_time") -> pd.DataFrame:
    """Earliest and latest value of a time field inside the message values (found at
    any nesting level), with the messages' Kafka timestamps as a second row.
    Empty when there are no messages or no such field (the reason is logged)."""
    empty = pd.DataFrame(columns=RANGE_COLUMNS)
    if frame.empty:
        log.warning("No messages to look at.")
        return empty
    flat = expand_json(frame)
    columns = [c for c in flat.columns if c == field or c.endswith("." + field)]
    if not columns:
        log.warning("No %r field in these messages. Are the values deserialized?", field)
        return empty
    rows = {name: _range_row(to_utc_datetimes(flat[name])) for name in columns}
    rows["timestamp (Kafka)"] = _range_row(frame["timestamp"].dropna())
    return pd.DataFrame.from_dict(rows, orient="index", columns=RANGE_COLUMNS).rename_axis("field")


def per_minute_counts(frame: pd.DataFrame) -> pd.DataFrame:
    """How many messages arrived in each minute, by Kafka timestamp."""
    return frame.set_index("timestamp").resample("1min").size().to_frame("messages")
