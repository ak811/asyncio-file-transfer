"""Shared fixtures: a temporary served directory, a running server, and a client."""

from __future__ import annotations

import asyncio
import hashlib
import os
import random
import tempfile
import unittest
from pathlib import Path

from filetransfer import protocol
from filetransfer.client import FileTransferClient
from filetransfer.server import FileServer, ServerConfig


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ServerTestCase(unittest.IsolatedAsyncioTestCase):
    """Starts a server on a free port with ``self.root`` served and ``self.local`` for downloads."""

    config_overrides: dict = {}

    async def asyncSetUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="filetransfer-test-")
        base = Path(self._tmp.name)
        self.root = base / "served"
        self.local = base / "local"
        self.root.mkdir()
        self.local.mkdir()
        self.server = FileServer(ServerConfig(root=self.root, port=0, **self.config_overrides))
        await self.server.start()
        self.client = await self.connect()

    async def asyncTearDown(self) -> None:
        await self.client.close()
        await self.server.close()
        self._tmp.cleanup()

    async def connect(self) -> FileTransferClient:
        client = FileTransferClient("127.0.0.1", self.server.port, timeout=10)
        await client.connect()
        return client

    def make_file(self, relative: str, data: bytes) -> Path:
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return path

    async def raw_connection(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.open_connection("127.0.0.1", self.server.port)

    async def raw_request(self, message: dict) -> dict | None:
        reader, writer = await self.raw_connection()
        try:
            await protocol.write_frame(writer, message)
            return await protocol.read_frame(reader, protocol.MAX_RESPONSE_FRAME, 5)
        finally:
            writer.close()


def random_bytes(size: int, seed: int = 0) -> bytes:
    """Deterministic pseudo-random bytes."""
    return random.Random(seed).randbytes(size)


def listdir(path: Path) -> list[str]:
    return sorted(os.listdir(path))
