"""The schema registry client, and the deserializers built on it.

confluent_kafka's registry modules are optional dependencies (the `registry`
extra), so they are imported only when a registry is actually used.
"""
from __future__ import annotations

from typing import Callable, Optional

import pandas as pd

from .config import RegistrySettings
from .dns import override_host

#: Anything called like confluent_kafka's deserializers: (data, SerializationContext) -> value.
Deserializer = Callable[[bytes, object], object]

SUBJECT_COLUMNS = ["subject", "version", "schema_id", "type"]


def make_registry_client(settings: RegistrySettings):
    """A SchemaRegistryClient for `settings`, or None when no registry is configured.
    Applies the settings' IP override for the registry's hostname, if any."""
    if not settings.enabled:
        return None
    if settings.ip:
        override_host(settings.host, settings.ip)
    from confluent_kafka.schema_registry import SchemaRegistryClient

    return SchemaRegistryClient(settings.client_config())


def make_deserializer(fmt: str, client) -> Optional[Deserializer]:
    """The registry's deserializer for "avro" or "json"; None for plain "string" text.
    Each deserializer fetches the schema a record was written with, so different
    schema versions in one topic all decode."""
    if fmt == "string":
        return None
    if client is None:
        raise ValueError(f"The {fmt!r} format needs a schema registry client.")
    if fmt == "avro":
        from confluent_kafka.schema_registry.avro import AvroDeserializer

        return AvroDeserializer(client)
    if fmt == "json":
        from confluent_kafka.schema_registry.json_schema import JSONDeserializer

        return JSONDeserializer(None, schema_registry_client=client)
    raise ValueError(f"Unknown format {fmt!r}.")


def registry_subjects(client) -> pd.DataFrame:
    """Every subject in the registry with its latest version, schema ID and schema type.
    An empty table when `client` is None."""
    if client is None:
        return pd.DataFrame(columns=SUBJECT_COLUMNS)
    rows = []
    for subject in sorted(client.get_subjects()):
        latest = client.get_latest_version(subject)
        rows.append({
            "subject": subject,
            "version": latest.version,
            "schema_id": latest.schema_id,
            "type": latest.schema.schema_type or "AVRO",
        })
    return pd.DataFrame(rows, columns=SUBJECT_COLUMNS)
