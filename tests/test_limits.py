import asyncio
import struct

from filetransfer import protocol
from filetransfer.errors import ErrorCode, RemoteError

from tests.helpers import ServerTestCase


class ReadOnlyTest(ServerTestCase):
    config_overrides = {"read_only": True}

    async def test_rejects_uploads(self):
        source = self.local / "s.txt"
        source.write_bytes(b"x")
        with self.assertRaises(RemoteError) as ctx:
            await self.client.put(source, "s.txt")
        self.assertEqual(ctx.exception.code, ErrorCode.FORBIDDEN)


class UploadLimitTest(ServerTestCase):
    config_overrides = {"max_upload_bytes": 100}

    async def test_rejects_oversized_uploads_before_receiving_data(self):
        source = self.local / "big.bin"
        source.write_bytes(b"x" * 101)
        with self.assertRaises(RemoteError) as ctx:
            await self.client.put(source, "big.bin")
        self.assertEqual(ctx.exception.code, ErrorCode.TOO_LARGE)
        source.write_bytes(b"x" * 100)
        await self.client.put(source, "big.bin")


class ConnectionLimitTest(ServerTestCase):
    config_overrides = {"max_connections": 2}

    async def test_turns_away_clients_beyond_the_limit(self):
        second = await self.connect()
        await second.list("")  # make sure the server has registered it
        response = await self.raw_request({"v": 1, "op": "list", "path": ""})
        self.assertEqual(response["error"], ErrorCode.BUSY)
        self.assertEqual(self.server.stats.connections_rejected, 1)
        await second.close()


class IdleTimeoutTest(ServerTestCase):
    config_overrides = {"idle_timeout": 0.2}

    async def test_closes_idle_connections(self):
        reader, writer = await self.raw_connection()
        self.assertEqual(await asyncio.wait_for(reader.read(), 5), b"")  # EOF from the server
        writer.close()


class RobustnessTest(ServerTestCase):

    async def test_malformed_frames_close_only_that_connection(self):
        for garbage in [struct.pack(">I", 10**9), struct.pack(">I", 5) + b"notjs", b"\x00\x00"]:
            reader, writer = await self.raw_connection()
            writer.write(garbage)
            if garbage == b"\x00\x00":
                writer.write_eof()
            await writer.drain()
            await asyncio.wait_for(reader.read(), 5)  # the server closes the connection
            writer.close()
        self.assertEqual(await self.client.list(""), [])

    async def test_rejects_unknown_versions_operations_and_fields(self):
        cases = [
            ({"v": 2, "op": "list"}, ErrorCode.UNSUPPORTED_VERSION),
            ({"op": "list"}, ErrorCode.UNSUPPORTED_VERSION),
            ({"v": 1, "op": "delete", "path": "x"}, ErrorCode.BAD_REQUEST),
            ({"v": 1, "op": "get", "path": 5}, ErrorCode.BAD_REQUEST),
            ({"v": 1, "op": "get", "path": "x", "offset": -1}, ErrorCode.BAD_REQUEST),
            ({"v": 1, "op": "get", "path": "x", "offset": True}, ErrorCode.BAD_REQUEST),
            ({"v": 1, "op": "put", "path": "x", "size": 1, "sha256": "zz", "overwrite": False},
             ErrorCode.BAD_REQUEST),
            ({"v": 1, "op": "put", "path": "x", "size": -1, "sha256": "0" * 64}, ErrorCode.BAD_REQUEST),
        ]
        for request, code in cases:
            response = await self.raw_request(request)
            self.assertEqual(response["error"], code, request)

    async def test_offset_beyond_the_end_is_rejected(self):
        self.make_file("f.txt", b"abc")
        response = await self.raw_request({"v": 1, "op": "get", "path": "f.txt", "offset": 4})
        self.assertEqual(response["error"], ErrorCode.BAD_REQUEST)

    async def test_statistics_are_tracked(self):
        self.make_file("f.bin", b"x" * 1000)
        await self.client.get("f.bin", self.local / "f.bin")
        source = self.local / "u.bin"
        source.write_bytes(b"y" * 500)
        await self.client.put(source, "u.bin")
        with self.assertRaises(RemoteError):
            await self.client.get("missing", self.local / "m")
        stats = self.server.stats
        self.assertEqual((stats.bytes_sent, stats.bytes_received), (1000, 500))
        self.assertEqual(stats.errors, {ErrorCode.NOT_FOUND: 1})
