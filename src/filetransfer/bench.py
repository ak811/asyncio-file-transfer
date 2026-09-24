"""Throughput benchmark: many clients transferring concurrently over loopback."""

from __future__ import annotations

import asyncio
import os
import statistics
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from filetransfer.client import FileTransferClient
from filetransfer.server import FileServer, ServerConfig


@dataclass(frozen=True)
class BenchResult:
    """One benchmark configuration.

    Attributes:
        operation: ``download`` or ``upload``.
        clients: number of concurrent clients.
        file_bytes: size of the file each client transfers.
        seconds: median wall-clock time for all clients to finish.
    """

    operation: str
    clients: int
    file_bytes: int
    seconds: float

    @property
    def aggregate_bytes_per_second(self) -> float:
        return self.clients * self.file_bytes / self.seconds


def _write_random_file(path: Path, size: int) -> None:
    with open(path, "wb") as file:
        remaining = size
        while remaining:
            block = os.urandom(min(remaining, 4 * 1024 * 1024))
            file.write(block)
            remaining -= len(block)


async def run(client_counts: list[int], file_bytes: int, repeat: int) -> list[BenchResult]:
    """Runs downloads and uploads of a ``file_bytes`` file for each client count.

    Every transfer is fully verified with SHA-256, so the numbers include checksum cost.
    """
    results: list[BenchResult] = []
    with tempfile.TemporaryDirectory(prefix="filetransfer-bench-") as tmp:
        root = Path(tmp, "served")
        local = Path(tmp, "local")
        root.mkdir()
        local.mkdir()
        source = root / "payload.bin"
        await asyncio.to_thread(_write_random_file, source, file_bytes)
        upload_source = local / "upload.bin"
        await asyncio.to_thread(_write_random_file, upload_source, file_bytes)

        config = ServerConfig(root=root, port=0, max_connections=max(client_counts) + 8)
        async with FileServer(config) as server:
            for clients in client_counts:
                for operation in ("download", "upload"):
                    timings = []
                    for run_index in range(repeat):
                        timings.append(await _round(server.port, operation, clients, local,
                                                    upload_source, root, run_index))
                    results.append(BenchResult(operation, clients, file_bytes, statistics.median(timings)))
    return results


async def _round(port: int, operation: str, clients: int, local: Path, upload_source: Path,
                 root: Path, run_index: int) -> float:
    connections = [FileTransferClient("127.0.0.1", port) for _ in range(clients)]
    await asyncio.gather(*(c.connect() for c in connections))
    try:
        start = time.perf_counter()
        if operation == "download":
            await asyncio.gather(*(c.get("payload.bin", local / f"down-{i}.bin", resume=False)
                                   for i, c in enumerate(connections)))
        else:
            await asyncio.gather(*(c.put(upload_source, f"up/{run_index}-{i}.bin")
                                   for i, c in enumerate(connections)))
        elapsed = time.perf_counter() - start
    finally:
        await asyncio.gather(*(c.close() for c in connections))
    # Remove the round's output so disk usage stays bounded.
    for path in list(local.glob("down-*.bin")) + list((root / "up").glob("*.bin")):
        path.unlink()
    return elapsed
