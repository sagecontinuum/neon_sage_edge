"""Resolve chosen hostnames to fixed IP addresses inside this Python process,
the way ``curl --resolve`` does for one command.

Only the address lookup changes. Requests still carry the hostname, so a proxy
that routes by name (such as Traefik) and HTTPS certificate checks keep working.
librdkafka's Kafka connections do their own lookups and are not affected.
"""
from __future__ import annotations

import socket
from typing import Dict

_overrides: Dict[str, str] = {}
_system_getaddrinfo = socket.getaddrinfo


def _getaddrinfo(host, *args, **kwargs):
    return _system_getaddrinfo(_overrides.get(host, host), *args, **kwargs)


def override_host(hostname: str, ip: str) -> None:
    """Look `hostname` up as `ip` for the rest of this process (or until cleared)."""
    if not hostname or not ip:
        raise ValueError("override_host needs both a hostname and an IP address.")
    _overrides[hostname] = ip
    socket.getaddrinfo = _getaddrinfo


def host_overrides() -> Dict[str, str]:
    """The overrides currently in effect (a copy)."""
    return dict(_overrides)


def clear_host_overrides() -> None:
    """Remove every override and restore normal lookups."""
    _overrides.clear()
    socket.getaddrinfo = _system_getaddrinfo
