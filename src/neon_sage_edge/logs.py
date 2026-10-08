"""Showing this package's notes (records that couldn't be deserialized, reads that hit
their limit) without turning on logging for every other library."""
from __future__ import annotations

import logging
from typing import IO, Optional

_MARK = "_neon_sage_edge_handler"


def show_notes(level: int = logging.INFO, stream: Optional[IO[str]] = None) -> logging.Handler:
    """Print the package's log messages as plain lines (to stderr unless `stream` is given).
    Calling it again replaces the earlier handler instead of printing everything twice."""
    logger = logging.getLogger("neon_sage_edge")
    for handler in [h for h in logger.handlers if getattr(h, _MARK, False)]:
        logger.removeHandler(handler)
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    setattr(handler, _MARK, True)
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return handler
