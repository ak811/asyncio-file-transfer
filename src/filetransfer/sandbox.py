"""Confines client-supplied paths to the server's root directory."""

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath

from filetransfer.errors import ErrorCode, RequestError

#: Prefix of in-progress upload files. They are hidden from listings and cannot be addressed.
UPLOAD_PREFIX = ".upload-"


def resolve(root: Path, requested: object) -> Path:
    """Maps a client path to an absolute path inside ``root``.

    ``root`` must already be resolved. The requested path must be a relative path string. After
    resolving ``..`` components and symbolic links, it must still lie inside ``root``; this
    blocks directory traversal (``../../etc/passwd``), absolute paths, and symlinks that point
    outside the served tree.

    Raises:
        RequestError: with ``bad_request`` or ``forbidden``.
    """
    if not isinstance(requested, str):
        raise RequestError(ErrorCode.BAD_REQUEST, "path must be a string")
    if "\x00" in requested:
        raise RequestError(ErrorCode.FORBIDDEN, "path contains a NUL byte")
    if PurePosixPath(requested).is_absolute() or PureWindowsPath(requested).drive or \
            PureWindowsPath(requested).is_absolute():
        raise RequestError(ErrorCode.FORBIDDEN, "absolute paths are not allowed")
    candidate = (root / requested).resolve()
    if candidate != root and not candidate.is_relative_to(root):
        raise RequestError(ErrorCode.FORBIDDEN, "path escapes the served directory")
    if any(part.startswith(UPLOAD_PREFIX) for part in candidate.relative_to(root).parts):
        raise RequestError(ErrorCode.FORBIDDEN, "in-progress uploads cannot be accessed")
    return candidate


def display(root: Path, path: Path) -> str:
    """Returns ``path`` relative to ``root``, with forward slashes, for messages and logs."""
    relative = path.relative_to(root).as_posix()
    return relative if relative != "." else "/"
