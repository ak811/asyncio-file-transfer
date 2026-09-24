"""Exception types and protocol error codes."""

from __future__ import annotations


class ErrorCode:
    """Error codes carried in failed responses. Values are stable wire identifiers."""

    BAD_REQUEST = "bad_request"
    UNSUPPORTED_VERSION = "unsupported_version"
    NOT_FOUND = "not_found"
    FORBIDDEN = "forbidden"
    IS_DIRECTORY = "is_directory"
    NOT_A_DIRECTORY = "not_a_directory"
    EXISTS = "exists"
    TOO_LARGE = "too_large"
    CHECKSUM_MISMATCH = "checksum_mismatch"
    BUSY = "busy"
    INTERNAL = "internal"


class ProtocolError(Exception):
    """The peer sent bytes that violate the protocol, or closed the connection mid-message."""


class RequestError(Exception):
    """A request the server rejects; reported to the client as an error response."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class RemoteError(Exception):
    """An error response received from the server."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class IntegrityError(Exception):
    """Transferred data does not match its SHA-256 checksum."""
