from __future__ import annotations

import asyncio
from typing import Awaitable, TypeVar


# One deadline for the whole request head. A per-read timeout is not enough:
# a client that dribbles a byte at a time would reset it forever and hold the
# connection open, which is all a slowloris needs.
REQUEST_HEAD_TIMEOUT_SECONDS = 15.0

# Upper bound on a request body we are willing to buffer. Both listeners are
# JSON/form endpoints, so anything larger is a memory-exhaustion attempt.
MAX_REQUEST_BODY_BYTES = 64 * 1024

REQUEST_BODY_TIMEOUT_SECONDS = 15.0


_T = TypeVar("_T")


class RequestBodyTooLarge(ValueError):
    pass


async def with_head_deadline(awaitable: Awaitable[_T], *, timeout: float = REQUEST_HEAD_TIMEOUT_SECONDS) -> _T:
    """Run request-head parsing under a single overall deadline."""
    return await asyncio.wait_for(awaitable, timeout=timeout)


def clamp_content_length(raw: object, *, max_bytes: int = MAX_REQUEST_BODY_BYTES) -> int:
    """Parse a Content-Length header into a length we are willing to buffer."""
    try:
        length = int(str(raw or "0").strip() or "0")
    except ValueError:
        return 0
    return max(0, min(length, max_bytes))


def request_content_length(raw: object, *, max_bytes: int = MAX_REQUEST_BODY_BYTES) -> int:
    """Parse Content-Length, rejecting malformed, negative, or oversized bodies."""
    try:
        length = int(str(raw or "0").strip() or "0")
    except ValueError as exc:
        raise ValueError("invalid Content-Length") from exc
    if length < 0:
        raise ValueError("invalid Content-Length")
    if length > max_bytes:
        raise RequestBodyTooLarge(f"request body exceeds {max_bytes} bytes")
    return length


async def read_body(
    reader: asyncio.StreamReader,
    content_length: int,
    *,
    max_bytes: int = MAX_REQUEST_BODY_BYTES,
    timeout: float = REQUEST_BODY_TIMEOUT_SECONDS,
) -> bytes:
    """Read at most `max_bytes` of body under a deadline."""
    length = max(0, min(int(content_length), max_bytes))
    if length <= 0:
        return b""
    return await asyncio.wait_for(reader.readexactly(length), timeout=timeout)
