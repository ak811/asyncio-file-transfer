import asyncio
import os

from filetransfer import protocol
from filetransfer.client import PARTIAL_SUFFIX
from filetransfer.errors import ErrorCode, RemoteError

from tests.helpers import ServerTestCase, listdir, random_bytes, sha256

MIB = 1024 * 1024


class BrowseTest(ServerTestCase):

    async def test_list_and_stat(self):
        self.make_file("b.txt", b"hello")
        self.make_file("a/nested.bin", b"x" * 10)
        entries = await self.client.list("")
        self.assertEqual([(e["name"], e["type"], e["size"]) for e in entries],
                         [("a", "dir", None), ("b.txt", "file", 5)])
        self.assertEqual((await self.client.stat("a/nested.bin"))["size"], 10)
        self.assertEqual((await self.client.stat("a"))["type"], "dir")

    async def test_listing_errors(self):
        self.make_file("f.txt", b"x")
        for path, code in [("missing", ErrorCode.NOT_FOUND), ("f.txt", ErrorCode.NOT_A_DIRECTORY),
                           ("../", ErrorCode.FORBIDDEN)]:
            with self.assertRaises(RemoteError) as ctx:
                await self.client.list(path)
            self.assertEqual(ctx.exception.code, code)


class DownloadTest(ServerTestCase):

    async def test_downloads_files_of_many_sizes_with_verification(self):
        for size in [0, 1, 1000, MIB, 3 * MIB + 17]:
            data = random_bytes(size, size)
            self.make_file(f"f{size}.bin", data)
            target = self.local / f"f{size}.bin"
            result = await self.client.get(f"f{size}.bin", target)
            self.assertEqual(target.read_bytes(), data)
            self.assertEqual(result.sha256, sha256(data))
            self.assertEqual((result.size, result.transferred, result.resumed_from), (size, size, 0))
        self.assertFalse(any(name.endswith(PARTIAL_SUFFIX) for name in listdir(self.local)))

    async def test_resumes_from_a_partial_download(self):
        data = random_bytes(3 * MIB, 1)
        self.make_file("big.bin", data)
        target = self.local / "big.bin"
        (self.local / ("big.bin" + PARTIAL_SUFFIX)).write_bytes(data[: MIB + 123])

        result = await self.client.get("big.bin", target)

        self.assertEqual(target.read_bytes(), data)
        self.assertEqual(result.resumed_from, MIB + 123)
        self.assertEqual(result.transferred, len(data) - (MIB + 123))
        self.assertEqual(result.sha256, sha256(data))

    async def test_restarts_when_the_partial_download_is_corrupt(self):
        data = random_bytes(2 * MIB, 2)
        self.make_file("big.bin", data)
        corrupt = bytearray(data[:MIB])
        corrupt[500] ^= 0xFF
        (self.local / ("big.bin" + PARTIAL_SUFFIX)).write_bytes(bytes(corrupt))

        result = await self.client.get("big.bin", self.local / "big.bin")

        self.assertEqual((self.local / "big.bin").read_bytes(), data)
        self.assertEqual(result.resumed_from, 0)

    async def test_restarts_when_the_partial_download_is_longer_than_the_file(self):
        self.make_file("small.bin", b"abc")
        (self.local / ("small.bin" + PARTIAL_SUFFIX)).write_bytes(b"abcdef")
        result = await self.client.get("small.bin", self.local / "small.bin")
        self.assertEqual((self.local / "small.bin").read_bytes(), b"abc")
        self.assertEqual(result.resumed_from, 0)

    async def test_no_resume_ignores_a_partial_download(self):
        self.make_file("f.bin", b"0123456789")
        (self.local / ("f.bin" + PARTIAL_SUFFIX)).write_bytes(b"XXXXX")
        result = await self.client.get("f.bin", self.local / "f.bin", resume=False)
        self.assertEqual((self.local / "f.bin").read_bytes(), b"0123456789")
        self.assertEqual(result.transferred, 10)

    async def test_many_requests_share_one_connection(self):
        for i in range(20):
            self.make_file(f"{i}.txt", str(i).encode())
        for i in range(20):
            await self.client.get(f"{i}.txt", self.local / f"{i}.txt")
            self.assertEqual((self.local / f"{i}.txt").read_text(), str(i))
        self.assertEqual(self.server.stats.connections_total, 1)

    async def test_download_errors_leave_the_connection_usable(self):
        self.make_file("ok.txt", b"ok")
        (self.root / "dir").mkdir()
        cases = [("missing.txt", ErrorCode.NOT_FOUND), ("dir", ErrorCode.IS_DIRECTORY),
                 ("../../etc/passwd", ErrorCode.FORBIDDEN), ("/etc/passwd", ErrorCode.FORBIDDEN)]
        for path, code in cases:
            with self.assertRaises(RemoteError) as ctx:
                await self.client.get(path, self.local / "out")
            self.assertEqual(ctx.exception.code, code, path)
        await self.client.get("ok.txt", self.local / "ok.txt")
        self.assertEqual((self.local / "ok.txt").read_bytes(), b"ok")
        self.assertFalse((self.local / "out").exists())


class UploadTest(ServerTestCase):

    async def test_uploads_are_verified_and_stored(self):
        for size in [0, 5, 2 * MIB + 3]:
            data = random_bytes(size, size + 7)
            source = self.local / f"src{size}"
            source.write_bytes(data)
            result = await self.client.put(source, f"in/{size}.bin")
            self.assertEqual((self.root / "in" / f"{size}.bin").read_bytes(), data)
            self.assertEqual(result.sha256, sha256(data))
        self.assertEqual(listdir(self.root / "in"), ["0.bin", "2097155.bin", "5.bin"])

    async def test_overwrite_must_be_explicit(self):
        self.make_file("f.txt", b"original")
        source = self.local / "new.txt"
        source.write_bytes(b"replacement")
        with self.assertRaises(RemoteError) as ctx:
            await self.client.put(source, "f.txt")
        self.assertEqual(ctx.exception.code, ErrorCode.EXISTS)
        self.assertEqual((self.root / "f.txt").read_bytes(), b"original")
        await self.client.put(source, "f.txt", overwrite=True)
        self.assertEqual((self.root / "f.txt").read_bytes(), b"replacement")

    async def test_rejects_uploads_with_a_wrong_checksum(self):
        reader, writer = await self.raw_connection()
        await protocol.write_frame(writer, {"v": 1, "op": "put", "path": "x.bin", "size": 4,
                                            "sha256": "0" * 64, "overwrite": False})
        self.assertTrue((await protocol.read_frame(reader, 1 << 20, 5))["ok"])
        writer.write(b"data")
        await writer.drain()
        response = await protocol.read_frame(reader, 1 << 20, 5)
        writer.close()
        self.assertEqual(response["error"], ErrorCode.CHECKSUM_MISMATCH)
        self.assertEqual(listdir(self.root), [])

    async def test_an_interrupted_upload_leaves_no_trace(self):
        reader, writer = await self.raw_connection()
        await protocol.write_frame(writer, {"v": 1, "op": "put", "path": "x.bin", "size": 10 * MIB,
                                            "sha256": "0" * 64, "overwrite": False})
        await protocol.read_frame(reader, 1 << 20, 5)
        writer.write(b"x" * MIB)
        await writer.drain()
        writer.close()
        for _ in range(100):  # wait for the server to notice the closed connection
            if self.server.stats.connections_active == 1:
                break
            await asyncio.sleep(0.02)
        self.assertEqual(listdir(self.root), [])
        self.assertEqual(await self.client.list(""), [])  # the server is still healthy

    async def test_upload_validation(self):
        self.make_file("file.txt", b"x")
        source = self.local / "s.txt"
        source.write_bytes(b"data")
        for remote, code in [("", ErrorCode.BAD_REQUEST), ("../escape.txt", ErrorCode.FORBIDDEN),
                             ("file.txt/child.txt", ErrorCode.NOT_A_DIRECTORY)]:
            with self.assertRaises(RemoteError) as ctx:
                await self.client.put(source, remote)
            self.assertEqual(ctx.exception.code, code, remote)
        self.assertFalse((self.root.parent / "escape.txt").exists())


class ConcurrencyTest(ServerTestCase):

    async def test_many_clients_download_concurrently(self):
        data = random_bytes(MIB, 3)
        self.make_file("shared.bin", data)
        clients = [await self.connect() for _ in range(40)]
        try:
            results = await asyncio.gather(*(
                c.get("shared.bin", self.local / f"copy{i}.bin") for i, c in enumerate(clients)))
        finally:
            await asyncio.gather(*(c.close() for c in clients))
        self.assertTrue(all(r.sha256 == sha256(data) for r in results))
        self.assertTrue(all((self.local / f"copy{i}.bin").read_bytes() == data for i in range(40)))

    async def test_concurrent_uploads_to_one_new_name_have_exactly_one_winner(self):
        sources = []
        for i in range(10):
            path = self.local / f"v{i}"
            path.write_bytes(random_bytes(200_000, i))
            sources.append(path)
        clients = [await self.connect() for _ in range(10)]
        try:
            outcomes = await asyncio.gather(*(c.put(s, "race.bin") for c, s in zip(clients, sources)),
                                            return_exceptions=True)
        finally:
            await asyncio.gather(*(c.close() for c in clients))
        winners = [i for i, o in enumerate(outcomes) if not isinstance(o, Exception)]
        losers = [o for o in outcomes if isinstance(o, RemoteError)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 9)
        self.assertTrue(all(e.code == ErrorCode.EXISTS for e in losers))
        self.assertEqual((self.root / "race.bin").read_bytes(), sources[winners[0]].read_bytes())
        self.assertEqual(listdir(self.root), ["race.bin"])
