"""Command-line interface: ``filetransfer serve | ls | get | put | bench``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from filetransfer import bench
from filetransfer.client import PARTIAL_SUFFIX, FileTransferClient
from filetransfer.errors import IntegrityError, ProtocolError, RemoteError
from filetransfer.progress import ProgressBar, human_bytes
from filetransfer.server import FileServer, ServerConfig

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="filetransfer",
        description="Concurrent, integrity-checked file transfer over TCP.")
    commands = parser.add_subparsers(dest="command", required=True)

    def connection(sub: argparse.ArgumentParser) -> None:
        sub.add_argument("--host", default="127.0.0.1", help="server address (default: 127.0.0.1)")
        sub.add_argument("--port", type=int, default=9000, help="server port (default: 9000)")
        sub.add_argument("--timeout", type=float, default=60.0, help="network timeout in seconds (default: 60)")

    serve = commands.add_parser("serve", help="serve a directory")
    serve.add_argument("--root", type=Path, required=True, help="directory to serve")
    serve.add_argument("--host", default="127.0.0.1", help="interface to bind (default: 127.0.0.1)")
    serve.add_argument("--port", type=int, default=9000, help="port to bind (default: 9000)")
    serve.add_argument("--max-connections", type=int, default=256, help="concurrent connection limit (default: 256)")
    serve.add_argument("--read-only", action="store_true", help="reject uploads")
    serve.add_argument("--max-upload-mb", type=float, default=64 * 1024, help="largest upload in MiB (default: 65536)")
    serve.add_argument("--idle-timeout", type=float, default=300.0, help="seconds before idle connections close (default: 300)")
    serve.add_argument("-v", "--verbose", action="store_true", help="log every request")

    ls = commands.add_parser("ls", help="list a remote directory")
    ls.add_argument("path", nargs="?", default="", help="remote directory (default: root)")
    connection(ls)

    get = commands.add_parser("get", help="download a file")
    get.add_argument("remote", help="remote file path")
    get.add_argument("local", nargs="?", type=Path, help="local destination (default: remote file name)")
    get.add_argument("--no-resume", action="store_true", help="ignore any partial download and start over")
    connection(get)

    put = commands.add_parser("put", help="upload a file")
    put.add_argument("local", type=Path, help="local file")
    put.add_argument("remote", help="remote destination path")
    put.add_argument("--overwrite", action="store_true", help="replace an existing remote file")
    connection(put)

    bench_cmd = commands.add_parser("bench", help="measure throughput with concurrent clients on loopback")
    bench_cmd.add_argument("--clients", default="1,4,16", help="comma-separated client counts (default: 1,4,16)")
    bench_cmd.add_argument("--size-mb", type=int, default=32, help="file size per transfer in MiB (default: 32)")
    bench_cmd.add_argument("--repeat", type=int, default=3, help="runs per configuration; median reported (default: 3)")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return EXIT_OK if e.code == 0 else EXIT_USAGE
    handlers = {"serve": _serve, "ls": _ls, "get": _get, "put": _put, "bench": _bench}
    try:
        return asyncio.run(handlers[args.command](args))
    except KeyboardInterrupt:
        return EXIT_ERROR
    except RemoteError as e:
        print(f"error: server refused: {e.message} ({e.code})", file=sys.stderr)
    except IntegrityError as e:
        print(f"error: integrity check failed: {e}", file=sys.stderr)
    except ProtocolError as e:
        print(f"error: protocol error: {e}", file=sys.stderr)
    except (TimeoutError, asyncio.TimeoutError):
        print("error: timed out", file=sys.stderr)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE
    except OSError as e:
        print(f"error: {e.strerror or e}{': ' + str(e.filename) if e.filename else ''}", file=sys.stderr)
    return EXIT_ERROR


async def _serve(args: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING,
                        format="%(asctime)s %(levelname)s %(message)s")
    config = ServerConfig(root=args.root, host=args.host, port=args.port,
                          max_connections=args.max_connections, read_only=args.read_only,
                          max_upload_bytes=int(args.max_upload_mb * 1024 * 1024),
                          idle_timeout=args.idle_timeout)
    server = FileServer(config)
    await server.start()
    print(f"Serving {server.root} on {args.host}:{server.port}"
          f"{' (read-only)' if args.read_only else ''}. Press Ctrl+C to stop.", file=sys.stderr)
    try:
        await server.serve_forever()
    finally:
        await server.close()
    return EXIT_OK


async def _ls(args: argparse.Namespace) -> int:
    async with FileTransferClient(args.host, args.port, args.timeout) as client:
        entries = await client.list(args.path)
    for entry in entries:
        modified = datetime.fromtimestamp(entry["mtime"], timezone.utc).strftime("%Y-%m-%d %H:%M")
        size = "<dir>" if entry["type"] == "dir" else human_bytes(entry["size"])
        print(f"{size:>12}  {modified}  {entry['name']}{'/' if entry['type'] == 'dir' else ''}")
    return EXIT_OK


async def _get(args: argparse.Namespace) -> int:
    local: Path = args.local or Path(PurePosixPath(args.remote).name)
    if local.is_dir():
        local = local / PurePosixPath(args.remote).name
    partial = local.with_name(local.name + PARTIAL_SUFFIX)
    initial = partial.stat().st_size if partial.exists() and not args.no_resume else 0
    async with FileTransferClient(args.host, args.port, args.timeout) as client:
        info = await client.stat(args.remote)
        bar = ProgressBar(info["size"] or 0, local.name, initial=initial)
        result = await client.get(args.remote, local, resume=not args.no_resume, progress=bar)
        bar.finish()
    resumed = f", resumed at {human_bytes(result.resumed_from)}" if result.resumed_from else ""
    print(f"{local}: {human_bytes(result.size)} verified (sha256 {result.sha256[:16]}...), "
          f"{human_bytes(result.throughput)}/s{resumed}")
    return EXIT_OK


async def _put(args: argparse.Namespace) -> int:
    size = args.local.stat().st_size
    async with FileTransferClient(args.host, args.port, args.timeout) as client:
        bar = ProgressBar(size, args.local.name)
        result = await client.put(args.local, args.remote, overwrite=args.overwrite, progress=bar)
        bar.finish()
    print(f"{args.remote}: {human_bytes(result.size)} stored and verified "
          f"(sha256 {result.sha256[:16]}...), {human_bytes(result.throughput)}/s")
    return EXIT_OK


async def _bench(args: argparse.Namespace) -> int:
    try:
        counts = [int(c) for c in args.clients.split(",") if c.strip()]
    except ValueError:
        raise ValueError(f"--clients expects comma-separated integers: {args.clients}") from None
    if not counts or min(counts) < 1 or args.size_mb < 1 or args.repeat < 1:
        raise ValueError("client counts, --size-mb, and --repeat must be positive")
    size = args.size_mb * 1024 * 1024
    print(f"Loopback benchmark: {human_bytes(size)} per transfer, SHA-256 verified, "
          f"median of {args.repeat} run(s)")
    print(f"CPUs: {os.cpu_count()}; Python {platform.python_version()}\n")
    print(f"  {'Operation':<10} {'Clients':>7} {'Time (s)':>9} {'Aggregate':>14} {'Per client':>14}")
    for r in await bench.run(counts, size, args.repeat):
        per_client = r.file_bytes / r.seconds
        print(f"  {r.operation:<10} {r.clients:>7} {r.seconds:>9.3f} "
              f"{human_bytes(r.aggregate_bytes_per_second) + '/s':>14} {human_bytes(per_client) + '/s':>14}")
    return EXIT_OK
