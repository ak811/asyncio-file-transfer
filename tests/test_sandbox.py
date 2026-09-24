import os
import tempfile
import unittest
from pathlib import Path

from filetransfer import sandbox
from filetransfer.errors import ErrorCode, RequestError


class SandboxTest(unittest.TestCase):

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name).resolve()
        self.root = self.base / "root"
        (self.root / "docs").mkdir(parents=True)
        (self.base / "secret.txt").write_text("secret")

    def tearDown(self):
        self._tmp.cleanup()

    def assert_rejected(self, requested, code=ErrorCode.FORBIDDEN):
        with self.assertRaises(RequestError) as ctx:
            sandbox.resolve(self.root, requested)
        self.assertEqual(ctx.exception.code, code, requested)

    def test_accepts_paths_inside_the_root(self):
        self.assertEqual(sandbox.resolve(self.root, ""), self.root)
        self.assertEqual(sandbox.resolve(self.root, "docs/a.txt"), self.root / "docs" / "a.txt")
        self.assertEqual(sandbox.resolve(self.root, "docs/../b.txt"), self.root / "b.txt")
        self.assertEqual(sandbox.resolve(self.root, "./docs"), self.root / "docs")

    def test_rejects_traversal_and_absolute_paths(self):
        for requested in ["..", "../secret.txt", "docs/../../secret.txt", "docs/../../../etc/passwd",
                          "/etc/passwd", str(self.base / "secret.txt"), "C:\\Windows\\win.ini", "C:secret"]:
            self.assert_rejected(requested)

    def test_rejects_nul_bytes_and_non_strings(self):
        self.assert_rejected("docs\x00.txt")
        self.assert_rejected(None, ErrorCode.BAD_REQUEST)
        self.assert_rejected(42, ErrorCode.BAD_REQUEST)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unsupported")
    def test_rejects_symlinks_that_escape_the_root(self):
        (self.root / "escape").symlink_to(self.base / "secret.txt")
        (self.root / "inside").symlink_to(self.root / "docs")
        self.assert_rejected("escape")
        self.assertEqual(sandbox.resolve(self.root, "inside"), self.root / "docs")

    def test_hides_in_progress_uploads(self):
        self.assert_rejected(sandbox.UPLOAD_PREFIX + "abc.part")
        self.assert_rejected("docs/" + sandbox.UPLOAD_PREFIX + "abc.part")
