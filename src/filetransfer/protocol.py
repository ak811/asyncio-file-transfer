"""Wire protocol: length-prefixed JSON control frames, followed by raw data where needed.

Every control message is one *frame*: a 4-byte big-endian unsigned length ``N`` followed by
``N`` bytes of UTF-8 JSON encoding an object. File contents are never wrapped in frames; they
follow the frame that announces their exact length, so the receiver always knows where the
data ends. This fixes the core problem of treating a TCP byte stream as if it had message
boundaries.

Requests (client to server) carry ``"v"`` (protocol version) and ``"op"``:

* ``list``  ``{"path"}`` -> ``{"ok", "entries", "truncated"}``
* ``stat``  ``{"path"}`` -> ``{"ok", "type", "size", "mtime"}``
* ``get``   ``{"path", "offset"}`` -> ``{"ok", "size", "offset", "length"}``, then ``length``
  raw bytes, then ``{"ok", "sha256"}`` with the SHA-256 of the *entire* file
* ``put``   ``{"path", "size", "sha256", "overwrite"}`` -> ``{"ok"}`` (ready); the client then
  sends ``size`` raw bytes; -> ``{"ok", "sha256"}`` once stored

Failures are ``{"ok": false, "error": <code>, "message": <text>}``; see
:class:`filetransfer.errors.ErrorCode`.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

from filetransfer.errors import ProtocolError

VERSION = 1

#: Largest request frame the server accepts. Requests are small; this bounds memory per client.
MAX_REQUEST_FRAME = 64 * 1024

#: Largest response frame a client accepts (directory listings can be large).
MAX_RESPONSE_FRAME = 8 * 1024 * 1024

_LENGTH = struct.Struct(">I")


def encode_frame(message: dict[str, Any]) -> bytes:
    """Serializes ``message`` as one frame."""
    body = json.dumps(message, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _LENGTH.pack(len(body)) + body


async def write_frame(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
    """Writes one frame and waits until the transport buffer has room (backpressure)."""
    writer.write(encode_frame(message))
    await writer.drain()


async def read_frame(
    reader: asyncio.StreamReader, max_size: int, timeout: float | None = None
) -> dict[str, Any] | None:
    """Reads one frame.

    Returns ``None`` if the peer closed the connection cleanly *before* the frame started.

    Raises:
        ProtocolError: the frame is truncated, oversized, or not a JSON object.
        TimeoutError: no complete frame arrived within ``timeout`` seconds.
    """
    return await asyncio.wait_for(_read_frame(reader, max_size), timeout)


async def _read_frame(reader: asyncio.StreamReader, max_size: int) -> dict[str, Any] | None:
    try:
        header = await reader.readexactly(_LENGTH.size)
    except asyncio.IncompleteReadError as e:
        if not e.partial:
            return None
        raise ProtocolError("connection closed inside a frame header") from e
    (length,) = _LENGTH.unpack(header)
    if length == 0 or length > max_size:
        raise ProtocolError(f"frame length {length} outside [1, {max_size}]")
    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as e:
        raise ProtocolError("connection closed inside a frame body") from e
    try:
        message = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ProtocolError(f"frame is not valid UTF-8 JSON: {e}") from e
    if not isinstance(message, dict):
        raise ProtocolError("frame must contain a JSON object")
    return message


def ok(**fields: Any) -> dict[str, Any]:
    """Builds a success response."""
    return {"ok": True, **fields}


def error(code: str, message: str) -> dict[str, Any]:
    """Builds an error response."""
    return {"ok": False, "error": code, "message": message}
