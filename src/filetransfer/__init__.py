"""Concurrent, integrity-checked file transfer over TCP with asyncio."""

from filetransfer.client import FileTransferClient, TransferResult
from filetransfer.errors import IntegrityError, ProtocolError, RemoteError
from filetransfer.server import FileServer, ServerConfig

__all__ = [
    "FileServer",
    "FileTransferClient",
    "IntegrityError",
    "ProtocolError",
    "RemoteError",
    "ServerConfig",
    "TransferResult",
]

__version__ = "1.0.0"
