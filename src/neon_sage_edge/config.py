"""Connection settings for the mirror and the optional schema registry.

Both settings classes are immutable and validate themselves on creation, so an
invalid combination fails immediately instead of at the first read.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Callable, Dict, Mapping, Optional
from urllib.parse import urlparse

#: Ways to deserialize message keys and values.
FORMATS = ("avro", "json", "string")
#: Formats that need a schema registry.
REGISTRY_FORMATS = ("avro", "json")


@dataclass(frozen=True)
class KafkaSettings:
    """How to connect to the Kafka cluster being read."""

    bootstrap_servers: str = "localhost:9094"
    security_protocol: str = "SASL_PLAINTEXT"
    username: str = "notebook"
    password: Optional[str] = None
    sasl_mechanism: str = "SCRAM-SHA-512"
    client_id: str = "neon-sage-edge"

    @property
    def uses_sasl(self) -> bool:
        return self.security_protocol.upper().startswith("SASL")

    def client_config(self) -> Dict[str, object]:
        """librdkafka settings shared by every client this package creates."""
        config: Dict[str, object] = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "client.id": self.client_id,
            "log_level": 3,  # keep librdkafka's informational logs quiet
            "allow.auto.create.topics": False,  # a mistyped topic name must never create a topic
        }
        if self.uses_sasl:
            if not self.password:
                raise ValueError(f"{self.security_protocol} needs a password (MIRROR_PASSWORD).")
            config.update({
                "sasl.mechanism": self.sasl_mechanism,
                "sasl.username": self.username,
                "sasl.password": self.password,
            })
        return config

    def describe(self) -> str:
        user = f", user {self.username}" if self.uses_sasl else ""
        return f"{self.bootstrap_servers} ({self.security_protocol}{user})"

    @classmethod
    def from_env(
        cls,
        env: Optional[Mapping[str, str]] = None,
        prompt: Optional[Callable[[str], str]] = None,
    ) -> "KafkaSettings":
        """Settings from MIRROR_BOOTSTRAP, MIRROR_SECURITY_PROTOCOL, MIRROR_USERNAME and
        MIRROR_PASSWORD. If SASL needs a password that isn't set, `prompt` (for example
        getpass.getpass) is asked for it."""
        env = os.environ if env is None else env
        settings = cls(
            bootstrap_servers=env.get("MIRROR_BOOTSTRAP", cls.bootstrap_servers),
            security_protocol=env.get("MIRROR_SECURITY_PROTOCOL", cls.security_protocol),
            username=env.get("MIRROR_USERNAME", cls.username),
            password=env.get("MIRROR_PASSWORD") or None,
        )
        if settings.uses_sasl and not settings.password and prompt is not None:
            settings = replace(settings, password=prompt(f"Password for Kafka user '{settings.username}': "))
        return settings


@dataclass(frozen=True)
class RegistrySettings:
    """The optional schema registry, and which deserializer to use for keys and values."""

    url: str = ""
    #: IP address to use for the registry's hostname when this machine can't resolve it.
    ip: str = ""
    value_format: str = "string"
    key_format: str = "string"

    def __post_init__(self) -> None:
        for name in ("value_format", "key_format"):
            fmt = getattr(self, name)
            if fmt not in FORMATS:
                raise ValueError(f"{name} is {fmt!r}; use one of: {', '.join(FORMATS)}.")
            if fmt in REGISTRY_FORMATS and not self.url:
                raise ValueError(f"{name} is {fmt!r}, which needs a schema registry URL.")

    @property
    def enabled(self) -> bool:
        return bool(self.url)

    @property
    def host(self) -> str:
        return urlparse(self.url).hostname or ""

    def client_config(self) -> Dict[str, str]:
        """Settings for confluent_kafka's SchemaRegistryClient."""
        return {"url": self.url}

    def describe(self) -> str:
        if not self.enabled:
            return f"not used (values {self.value_format}, keys {self.key_format})"
        via = f" (reached at {self.ip})" if self.ip else ""
        return f"{self.url}{via}; values {self.value_format}, keys {self.key_format}"

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "RegistrySettings":
        """Settings from SCHEMA_REGISTRY_URL, SCHEMA_REGISTRY_IP, VALUE_FORMAT and KEY_FORMAT.
        Values default to "avro" when a registry URL is set, otherwise "string"."""
        env = os.environ if env is None else env
        url = env.get("SCHEMA_REGISTRY_URL", "")
        return cls(
            url=url,
            ip=env.get("SCHEMA_REGISTRY_IP", ""),
            value_format=env.get("VALUE_FORMAT", "avro" if url else "string"),
            key_format=env.get("KEY_FORMAT", "string"),
        )
