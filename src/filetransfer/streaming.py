"""Chunked, hashed streaming between files and sockets.

File reads and writes, and SHA-256 updates, run in worker threads via ``asyncio.to_thread`` so
that disk I/O never blocks the event loop. CPython's ``hashlib`` releases the GIL while hashing
large buffers, so concurrent transfers hash in parallel on multiple cores.
"""

from __future__ import annotations

import asyncio
import hashlib
from typing import BinaryIO, Callable

from filetransfer.errors import ProtocolError

CHUNK_SIZE = 1024 * 1024

ProgressCallback = Callable[[int], None]


def _read_and_hash(file: BinaryIO, size: int, hasher: "hashlib._Hash | None") -> bytes:
    data = file.read(size)
    if hasher is not None:
        hasher.update(data)
    return data


def _write_and_hash(file: BinaryIO, data: bytes, hasher: "hashlib._Hash") -> None:
    file.write(data)
    hasher.update(data)


async def hash_prefix(file: BinaryIO, length: int, hasher: "hashlib._Hash") -> None:
    """Feeds the next ``length`` bytes of ``file`` into ``hasher`` without sending them."""
    remaining = length
    while remaining > 0:
        data = await asyncio.to_thread(_read_and_hash, file, min(CHUNK_SIZE, remaining), hasher)
        if not data:
            raise EOFError("file ended before the expected length")
        remaining -= len(data)


async def send_file(
    file: BinaryIO,
    length: int,
    writer: asyncio.StreamWriter,
    hasher: "hashlib._Hash | None",
    progress: ProgressCallback | None = None,
) -> None:
    """Sends exactly ``length`` bytes from ``file`` with backpressure, hashing them if ``hasher`` is given."""
    remaining = length
    while remaining > 0:
        data = await asyncio.to_thread(_read_and_hash, file, min(CHUNK_SIZE, remaining), hasher)
        if not data:
            raise EOFError("file shrank during transfer")
        writer.write(data)
        await writer.drain()
        remaining -= len(data)
        if progress:
            progress(len(data))


async def receive_file(
    reader: asyncio.StreamReader,
    length: int,
    file: BinaryIO,
    hasher: "hashlib._Hash",
    timeout: float | None,
    progress: ProgressCallback | None = None,
) -> None:
    """Receives exactly ``length`` bytes into ``file``, hashing them.

    Raises:
        ProtocolError: the connection closed before ``length`` bytes arrived.
        TimeoutError: a chunk of up to ``CHUNK_SIZE`` bytes did not arrive within ``timeout`` seconds.
    """
    remaining = length
    while remaining > 0:
        # Waiting for a full chunk keeps thread hand-offs, and disk writes, large.
        try:
            data = await asyncio.wait_for(reader.readexactly(min(CHUNK_SIZE, remaining)), timeout)
        except asyncio.IncompleteReadError as e:
            raise ProtocolError(
                f"connection closed with {remaining - len(e.partial)} bytes still expected") from e
        await asyncio.to_thread(_write_and_hash, file, data, hasher)
        remaining -= len(data)
        if progress:
            progress(len(data))


def new_hasher() -> "hashlib._Hash":
    return hashlib.sha256()
