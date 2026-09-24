import asyncio
import json
import struct
import unittest

from filetransfer import protocol
from filetransfer.errors import ProtocolError


def reader_with(data: bytes, eof: bool = True) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    if eof:
        reader.feed_eof()
    return reader


class FrameTest(unittest.IsolatedAsyncioTestCase):

    async def test_round_trip_including_unicode(self):
        message = {"op": "get", "path": "résumé/файл.txt", "offset": 7}
        frame = protocol.encode_frame(message)
        self.assertEqual(struct.unpack(">I", frame[:4])[0], len(frame) - 4)
        self.assertEqual(await protocol.read_frame(reader_with(frame), 1024), message)

    async def test_clean_eof_before_a_frame_returns_none(self):
        self.assertIsNone(await protocol.read_frame(reader_with(b""), 1024))

    async def test_truncated_frames_are_protocol_errors(self):
        frame = protocol.encode_frame({"a": 1})
        for cut in (2, 5, len(frame) - 1):
            with self.assertRaises(ProtocolError):
                await protocol.read_frame(reader_with(frame[:cut]), 1024)

    async def test_rejects_oversized_empty_and_non_object_frames(self):
        with self.assertRaises(ProtocolError):
            await protocol.read_frame(reader_with(struct.pack(">I", 2000) + b"x" * 2000), 1024)
        with self.assertRaises(ProtocolError):
            await protocol.read_frame(reader_with(struct.pack(">I", 0)), 1024)
        body = json.dumps([1, 2]).encode()
        with self.assertRaises(ProtocolError):
            await protocol.read_frame(reader_with(struct.pack(">I", len(body)) + body), 1024)
        with self.assertRaises(ProtocolError):
            await protocol.read_frame(reader_with(struct.pack(">I", 3) + b"\xff\xfe\xfd"), 1024)

    async def test_times_out_waiting_for_a_frame(self):
        with self.assertRaises((TimeoutError, asyncio.TimeoutError)):
            await protocol.read_frame(reader_with(b"", eof=False), 1024, timeout=0.05)

    async def test_reads_consecutive_frames_from_one_stream(self):
        data = protocol.encode_frame({"n": 1}) + protocol.encode_frame({"n": 2})
        reader = reader_with(data)
        self.assertEqual((await protocol.read_frame(reader, 1024))["n"], 1)
        self.assertEqual((await protocol.read_frame(reader, 1024))["n"], 2)
        self.assertIsNone(await protocol.read_frame(reader, 1024))
