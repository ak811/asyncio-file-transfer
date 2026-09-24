import asyncio
import contextlib
import io
import tempfile
import threading
import unittest
from pathlib import Path

from filetransfer.cli import EXIT_ERROR, EXIT_OK, EXIT_USAGE, main
from filetransfer.server import FileServer, ServerConfig


class CliTest(unittest.TestCase):
    """Runs the real CLI against a server on a background event loop."""

    @classmethod
    def setUpClass(cls):
        cls._tmp = tempfile.TemporaryDirectory()
        cls.root = Path(cls._tmp.name, "served")
        cls.root.mkdir()
        cls.loop = asyncio.new_event_loop()
        cls.server = FileServer(ServerConfig(root=cls.root, port=0))
        ready = threading.Event()

        def run():
            asyncio.set_event_loop(cls.loop)
            cls.loop.run_until_complete(cls.server.start())
            ready.set()
            cls.loop.run_forever()

        cls.thread = threading.Thread(target=run, daemon=True)
        cls.thread.start()
        ready.wait(10)
        cls.port = str(cls.server.port)

    @classmethod
    def tearDownClass(cls):
        asyncio.run_coroutine_threadsafe(cls.server.close(), cls.loop).result(10)
        cls.loop.call_soon_threadsafe(cls.loop.stop)
        cls.thread.join(10)
        cls.loop.close()
        cls._tmp.cleanup()

    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            status = main(list(args))
        return status, out.getvalue(), err.getvalue()

    def test_put_ls_get_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp, "report.txt")
            source.write_text("quarterly numbers\n")
            status, out, err = self.run_cli("put", str(source), "docs/report.txt", "--port", self.port)
            self.assertEqual(status, EXIT_OK, err)
            self.assertIn("stored and verified", out)

            status, out, _ = self.run_cli("ls", "docs", "--port", self.port)
            self.assertEqual(status, EXIT_OK)
            self.assertIn("report.txt", out)

            target = Path(tmp, "downloaded.txt")
            status, out, err = self.run_cli("get", "docs/report.txt", str(target), "--port", self.port)
            self.assertEqual(status, EXIT_OK, err)
            self.assertEqual(target.read_text(), "quarterly numbers\n")

            status, _, err = self.run_cli("put", str(source), "docs/report.txt", "--port", self.port)
            self.assertEqual(status, EXIT_ERROR)
            self.assertIn("exists", err)

    def test_errors_and_usage(self):
        status, _, err = self.run_cli("get", "missing.txt", "--port", self.port)
        self.assertEqual(status, EXIT_ERROR)
        self.assertIn("not_found", err)
        self.assertEqual(self.run_cli("frobnicate")[0], EXIT_USAGE)
        self.assertEqual(self.run_cli("bench", "--clients", "x")[0], EXIT_USAGE)
        status, _, err = self.run_cli("ls", "--port", "1")
        self.assertEqual(status, EXIT_ERROR)

    def test_bench_runs(self):
        status, out, err = self.run_cli("bench", "--clients", "1,2", "--size-mb", "1", "--repeat", "1")
        self.assertEqual(status, EXIT_OK, err)
        self.assertIn("download", out)
        self.assertIn("upload", out)
