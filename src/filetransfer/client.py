"""Asynchronous client for :mod:`filetransfer.server`."""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from filetransfer import protocol, streaming
from filetransfer.errors import ErrorCode, IntegrityError, ProtocolError, RemoteError
from filetransfer.streaming import ProgressCallback

#: Suffix of a partially downloaded file, kept so an interrupted download can resume.
PARTIAL_SUFFIX = ".part"


@dataclass(frozen=True)
class TransferResult:
    """Outcome of a completed transfer.

    Attributes:
        size: total file size in bytes.
        transferred: bytes sent over the network in this transfer.
        resumed_from: offset the transfer resumed from (0 for a full transfer).
        sha256: verified SHA-256 of the whole file.
        seconds: wall-clock duration.
    """

    size: int
    transferred: int
    resumed_from: int
    sha256: str
    seconds: float

    @property
    def throughput(self) -> float:
        """Transferred bytes per second."""
        return self.transferred / self.seconds if self.seconds > 0 else float("inf")


class FileTransferClient:
    """A connection to a file server. Requests on one client run one at a time.

    Usage::

        async with FileTransferClient("127.0.0.1", 9000) as client:
            await client.get("reports/q3.pdf", Path("q3.pdf"))
    """

    def __init__(self, host: str, port: int, timeout: float = 60.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), self.timeout)

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(Exception):
                await self._writer.wait_closed()
            self._reader = self._writer = None

    async def __aenter__(self) -> "FileTransferClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- operations -----------------------------------------------------------------------------

    async def list(self, path: str = "") -> list[dict[str, Any]]:
        """Returns the entries of a remote directory: ``name``, ``type``, ``size``, ``mtime``."""
        async with self._lock:
            response = await self._request({"op": "list", "path": path})
            return response["entries"]

    async def stat(self, path: str) -> dict[str, Any]:
        """Returns ``type``, ``size``, and ``mtime`` of a remote path."""
        async with self._lock:
            response = await self._request({"op": "stat", "path": path})
            return {k: response[k] for k in ("type", "size", "mtime")}

    async def get(self, remote: str, local: Path, resume: bool = True,
                  progress: ProgressCallback | None = None) -> TransferResult:
        """Downloads ``remote`` to ``local`` and verifies its SHA-256.

        Data is written to ``local`` + ``.part`` and renamed into place only after verification.
        If the transfer is interrupted, the partial file remains, and a later call with
        ``resume=True`` requests only the missing bytes. If a resumed download fails
        verification (for example, because the partial file was corrupted or the remote file
        changed), it is restarted once from the beginning.

        Raises:
            RemoteError: the server rejected the request.
            IntegrityError: the downloaded file does not match the server's checksum.
        """
        local = Path(local)
        partial = local.with_name(local.name + PARTIAL_SUFFIX)
        async with self._lock:
            offset = partial.stat().st_size if resume and partial.exists() else 0
            try:
                return await self._get(remote, local, partial, offset, progress)
            except IntegrityError:
                if offset == 0:
                    raise
            except RemoteError as e:
                # The remote file became shorter than the partial download; start over.
                if offset == 0 or e.code != ErrorCode.BAD_REQUEST:
                    raise
            return await self._get(remote, local, partial, 0, progress)

    async def put(self, local: Path, remote: str, overwrite: bool = False,
                  progress: ProgressCallback | None = None) -> TransferResult:
        """Uploads ``local`` to ``remote``; the server verifies the SHA-256 before storing it.

        Raises:
            RemoteError: the server rejected the upload, including on checksum mismatch.
        """
        local = Path(local)
        async with self._lock:
            start = time.perf_counter()
            with open(local, "rb") as file:
                size = os.fstat(file.fileno()).st_size
                digest = streaming.new_hasher()
                await streaming.hash_prefix(file, size, digest)
                expected = digest.hexdigest()
                file.seek(0)
                await self._request({"op": "put", "path": remote, "size": size,
                                     "sha256": expected, "overwrite": overwrite})
                await streaming.send_file(file, size, self._stream()[1], None, progress)
            response = await self._response()
            if response.get("sha256") != expected:
                raise IntegrityError("server reported a different checksum")
            return TransferResult(size, size, 0, expected, time.perf_counter() - start)

    # -- internals ------------------------------------------------------------------------------

    async def _get(self, remote: str, local: Path, partial: Path, offset: int,
                   progress: ProgressCallback | None) -> TransferResult:
        start = time.perf_counter()
        header = await self._request({"op": "get", "path": remote, "offset": offset})
        size, length = header["size"], header["length"]
        if header.get("offset") != offset or length != size - offset or length < 0:
            raise ProtocolError(f"inconsistent download header: {header}")
        hasher = streaming.new_hasher()
        reader, _ = self._stream()
        with open(partial, "r+b" if offset else "wb") as file:
            if offset:
                await streaming.hash_prefix(file, offset, hasher)
                file.seek(offset)
                file.truncate()
            await streaming.receive_file(reader, length, file, hasher, self.timeout, progress)
        trailer = await self._response()
        actual = hasher.hexdigest()
        if trailer.get("sha256") != actual:
            partial.unlink(missing_ok=True)
            raise IntegrityError(f"{remote}: downloaded data hashes to {actual}, "
                                 f"server reports {trailer.get('sha256')}")
        os.replace(partial, local)
        return TransferResult(size, length, offset, actual, time.perf_counter() - start)

    def _stream(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self._reader is None or self._writer is None:
            raise RuntimeError("client is not connected")
        return self._reader, self._writer

    async def _request(self, message: dict[str, Any]) -> dict[str, Any]:
        _, writer = self._stream()
        await protocol.write_frame(writer, {"v": protocol.VERSION, **message})
        return await self._response()

    async def _response(self) -> dict[str, Any]:
        reader, _ = self._stream()
        response = await protocol.read_frame(reader, protocol.MAX_RESPONSE_FRAME, self.timeout)
        if response is None:
            raise ProtocolError("server closed the connection")
        if not response.get("ok"):
            raise RemoteError(str(response.get("error")), str(response.get("message")))
        return response
