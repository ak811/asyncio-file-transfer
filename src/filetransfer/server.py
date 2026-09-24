"""Asynchronous file server.

One event loop serves every connection. Each connection handles any number of requests in
sequence; file I/O and hashing run in worker threads, so a slow disk or a large checksum never
stalls other clients. A connection limit, request-size limit, and idle timeouts bound the
resources any one client can hold.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import stat as stat_module
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from filetransfer import protocol, sandbox, streaming
from filetransfer.errors import ErrorCode, ProtocolError, RequestError

log = logging.getLogger("filetransfer.server")

#: Upper bound on entries returned by one ``list`` request.
MAX_LIST_ENTRIES = 10_000


@dataclass(frozen=True)
class ServerConfig:
    """Server settings.

    Attributes:
        root: directory to serve; every request is confined to it.
        host: interface to bind.
        port: TCP port; 0 picks a free port.
        max_connections: concurrent connections; further clients receive ``busy``.
        read_only: reject uploads.
        max_upload_bytes: largest accepted upload.
        idle_timeout: seconds to wait for the next request on an open connection.
        io_timeout: seconds to wait for each chunk of an upload.
    """

    root: Path
    host: str = "127.0.0.1"
    port: int = 9000
    max_connections: int = 256
    read_only: bool = False
    max_upload_bytes: int = 64 * 1024**3
    idle_timeout: float = 300.0
    io_timeout: float = 60.0


@dataclass
class ServerStats:
    """Counters since the server started."""

    connections_total: int = 0
    connections_active: int = 0
    connections_rejected: int = 0
    requests: int = 0
    bytes_sent: int = 0
    bytes_received: int = 0
    errors: dict[str, int] = field(default_factory=dict)


class FileServer:
    """Serves files under ``config.root`` over the protocol in :mod:`filetransfer.protocol`.

    Usage::

        server = FileServer(ServerConfig(root=Path("files")))
        await server.start()
        await server.serve_forever()
    """

    def __init__(self, config: ServerConfig) -> None:
        root = config.root.resolve()
        if not root.is_dir():
            raise NotADirectoryError(f"root is not a directory: {config.root}")
        if config.max_connections < 1:
            raise ValueError("max_connections must be at least 1")
        self.config = config
        self.root = root
        self.stats = ServerStats()
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        """The bound port (useful when the configured port is 0)."""
        if self._server is None:
            raise RuntimeError("server is not started")
        return self._server.sockets[0].getsockname()[1]

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._on_connect, self.config.host, self.config.port)
        log.info("serving %s on %s:%d", self.root, self.config.host, self.port)

    async def serve_forever(self) -> None:
        if self._server is None:
            await self.start()
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def close(self) -> None:
        """Stops accepting connections and closes the ones in progress."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for task in list(self._connections):
            task.cancel()
        if self._connections:
            await asyncio.gather(*self._connections, return_exceptions=True)

    async def __aenter__(self) -> "FileServer":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    # -- connection handling ------------------------------------------------------------------

    async def _on_connect(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._connections.add(task)
        peer = writer.get_extra_info("peername")
        try:
            if self.stats.connections_active >= self.config.max_connections:
                self.stats.connections_rejected += 1
                with contextlib.suppress(ConnectionError):
                    await protocol.write_frame(
                        writer, protocol.error(ErrorCode.BUSY, "server is at its connection limit"))
                return
            self.stats.connections_total += 1
            self.stats.connections_active += 1
            try:
                await self._serve_connection(reader, writer, peer)
            finally:
                self.stats.connections_active -= 1
        finally:
            self._connections.discard(task)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _serve_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                peer: Any) -> None:
        log.debug("connection from %s", peer)
        while True:
            try:
                request = await protocol.read_frame(
                    reader, protocol.MAX_REQUEST_FRAME, self.config.idle_timeout)
            except ProtocolError as e:
                log.warning("protocol error from %s: %s", peer, e)
                with contextlib.suppress(ConnectionError):
                    await protocol.write_frame(writer, protocol.error(ErrorCode.BAD_REQUEST, str(e)))
                return
            except (TimeoutError, asyncio.TimeoutError):
                log.debug("idle timeout for %s", peer)
                return
            except ConnectionError:
                return
            if request is None:
                return
            self.stats.requests += 1
            try:
                await self._dispatch(request, reader, writer)
            except RequestError as e:
                self.stats.errors[e.code] = self.stats.errors.get(e.code, 0) + 1
                log.info("%s: %s %s -> %s", peer, request.get("op"), request.get("path"), e.code)
                try:
                    await protocol.write_frame(writer, protocol.error(e.code, e.message))
                except ConnectionError:
                    return
            except (ProtocolError, ConnectionError, EOFError, TimeoutError, asyncio.TimeoutError) as e:
                # The byte stream is no longer in a known state; the connection must close.
                log.warning("%s: aborted %s: %s", peer, request.get("op"), e)
                return
            except OSError as e:
                log.exception("%s: I/O error", peer)
                with contextlib.suppress(ConnectionError):
                    await protocol.write_frame(writer, protocol.error(ErrorCode.INTERNAL, e.strerror or str(e)))
                return

    async def _dispatch(self, request: dict[str, Any], reader: asyncio.StreamReader,
                        writer: asyncio.StreamWriter) -> None:
        if request.get("v") != protocol.VERSION:
            raise RequestError(ErrorCode.UNSUPPORTED_VERSION,
                               f"protocol version {request.get('v')!r} is not supported")
        op = request.get("op")
        if op == "list":
            await self._list(request, writer)
        elif op == "stat":
            await self._stat(request, writer)
        elif op == "get":
            await self._get(request, writer)
        elif op == "put":
            await self._put(request, reader, writer)
        else:
            raise RequestError(ErrorCode.BAD_REQUEST, f"unknown operation {op!r}")

    # -- operations -----------------------------------------------------------------------------

    async def _list(self, request: dict[str, Any], writer: asyncio.StreamWriter) -> None:
        path = sandbox.resolve(self.root, request.get("path", ""))
        entries, truncated = await asyncio.to_thread(self._scan, path)
        await protocol.write_frame(writer, protocol.ok(entries=entries, truncated=truncated))

    def _scan(self, path: Path) -> tuple[list[dict[str, Any]], bool]:
        if not path.exists():
            raise RequestError(ErrorCode.NOT_FOUND, f"{sandbox.display(self.root, path)} does not exist")
        if not path.is_dir():
            raise RequestError(ErrorCode.NOT_A_DIRECTORY, f"{sandbox.display(self.root, path)} is not a directory")
        entries: list[dict[str, Any]] = []
        with os.scandir(path) as it:
            for entry in sorted(it, key=lambda e: e.name):
                if entry.name.startswith(sandbox.UPLOAD_PREFIX):
                    continue
                if len(entries) >= MAX_LIST_ENTRIES:
                    return entries, True
                try:
                    info = entry.stat()
                except OSError:
                    continue  # e.g. a dangling symlink
                kind = "dir" if stat_module.S_ISDIR(info.st_mode) else "file"
                entries.append({"name": entry.name, "type": kind,
                                "size": info.st_size if kind == "file" else None,
                                "mtime": int(info.st_mtime)})
        return entries, False

    async def _stat(self, request: dict[str, Any], writer: asyncio.StreamWriter) -> None:
        path = sandbox.resolve(self.root, request.get("path", ""))
        try:
            info = await asyncio.to_thread(path.stat)
        except FileNotFoundError:
            raise RequestError(ErrorCode.NOT_FOUND, f"{sandbox.display(self.root, path)} does not exist") from None
        kind = "dir" if stat_module.S_ISDIR(info.st_mode) else "file"
        await protocol.write_frame(writer, protocol.ok(
            type=kind, size=info.st_size if kind == "file" else None, mtime=int(info.st_mtime)))

    async def _get(self, request: dict[str, Any], writer: asyncio.StreamWriter) -> None:
        path = sandbox.resolve(self.root, request.get("path"))
        offset = request.get("offset", 0)
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise RequestError(ErrorCode.BAD_REQUEST, "offset must be a non-negative integer")
        if path.is_dir():
            raise RequestError(ErrorCode.IS_DIRECTORY, f"{sandbox.display(self.root, path)} is a directory")
        try:
            file = await asyncio.to_thread(open, path, "rb")
        except FileNotFoundError:
            raise RequestError(ErrorCode.NOT_FOUND, f"{sandbox.display(self.root, path)} does not exist") from None
        try:
            # The size is fixed when the transfer starts; if the file later shrinks,
            # send_file fails and the connection is closed rather than sending a bad stream.
            size = os.fstat(file.fileno()).st_size
            if offset > size:
                raise RequestError(ErrorCode.BAD_REQUEST, f"offset {offset} is beyond the file size {size}")
            length = size - offset
            await protocol.write_frame(writer, protocol.ok(size=size, offset=offset, length=length))
            hasher = streaming.new_hasher()
            # The checksum covers the whole file, so a resumed download is verified end to end.
            await streaming.hash_prefix(file, offset, hasher)
            await streaming.send_file(file, length, writer, hasher)
            self.stats.bytes_sent += length
            await protocol.write_frame(writer, protocol.ok(sha256=hasher.hexdigest()))
        finally:
            await asyncio.to_thread(file.close)

    async def _put(self, request: dict[str, Any], reader: asyncio.StreamReader,
                   writer: asyncio.StreamWriter) -> None:
        if self.config.read_only:
            raise RequestError(ErrorCode.FORBIDDEN, "server is read-only")
        path = sandbox.resolve(self.root, request.get("path"))
        size = request.get("size")
        expected = request.get("sha256")
        overwrite = request.get("overwrite", False)
        if path == self.root:
            raise RequestError(ErrorCode.BAD_REQUEST, "a file name is required")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise RequestError(ErrorCode.BAD_REQUEST, "size must be a non-negative integer")
        if not _is_sha256(expected):
            raise RequestError(ErrorCode.BAD_REQUEST, "sha256 must be 64 lowercase hex digits")
        if not isinstance(overwrite, bool):
            raise RequestError(ErrorCode.BAD_REQUEST, "overwrite must be a boolean")
        if size > self.config.max_upload_bytes:
            raise RequestError(ErrorCode.TOO_LARGE,
                               f"{size} bytes exceeds the upload limit of {self.config.max_upload_bytes}")
        if path.is_dir():
            raise RequestError(ErrorCode.IS_DIRECTORY, f"{sandbox.display(self.root, path)} is a directory")
        if path.exists() and not overwrite:
            raise RequestError(ErrorCode.EXISTS, f"{sandbox.display(self.root, path)} already exists")
        try:
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        except (FileExistsError, NotADirectoryError):
            raise RequestError(ErrorCode.NOT_A_DIRECTORY, "a parent of the target path is a file") from None

        # Receive into a hidden temporary file in the target directory, then rename it into
        # place atomically, so readers never see a partial or corrupt upload.
        temp = path.parent / f"{sandbox.UPLOAD_PREFIX}{uuid.uuid4().hex}.part"
        file = await asyncio.to_thread(open, temp, "xb")
        try:
            await protocol.write_frame(writer, protocol.ok())
            hasher = streaming.new_hasher()
            try:
                await streaming.receive_file(reader, size, file, hasher, self.config.io_timeout)
            finally:
                await asyncio.to_thread(file.close)
            self.stats.bytes_received += size
            actual = hasher.hexdigest()
            if actual != expected:
                raise RequestError(ErrorCode.CHECKSUM_MISMATCH,
                                   f"received data hashes to {actual}, expected {expected}")
            await asyncio.to_thread(_commit, temp, path, overwrite)
            await protocol.write_frame(writer, protocol.ok(sha256=actual))
        finally:
            with contextlib.suppress(FileNotFoundError):
                await asyncio.to_thread(temp.unlink)


def _commit(temp: Path, path: Path, overwrite: bool) -> None:
    """Moves a completed upload into place atomically.

    Without ``overwrite``, a hard link creates the target only if it does not already exist, as a
    single atomic step, so two clients uploading the same new name cannot overwrite each other.
    """
    if overwrite:
        os.replace(temp, path)
        return
    try:
        os.link(temp, path)
    except FileExistsError:
        raise RequestError(ErrorCode.EXISTS, f"{path.name} was created by another upload") from None
    except OSError:
        # File systems without hard links: fall back to a check followed by a rename.
        if path.exists():
            raise RequestError(ErrorCode.EXISTS, f"{path.name} was created by another upload") from None
        os.replace(temp, path)


def _is_sha256(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)
