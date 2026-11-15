import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import archive
import filetransferd


class ArchiveKindTests(unittest.TestCase):
    def test_detects_supported_names(self):
        self.assertEqual(archive.archive_kind("a.ZIP"), ("zip", []))
        self.assertEqual(archive.archive_kind("a.tar.gz"), ("tar", ["-z"]))
        self.assertEqual(archive.archive_kind("a.tar.zst"), ("tar", ["--zstd"]))
        self.assertEqual(archive.archive_kind("a.tar"), ("tar", []))
        self.assertIsNone(archive.archive_kind("a.rar"))


class TrackerTests(unittest.TestCase):
    def test_tar_create_counts_finished_files(self):
        with tempfile.TemporaryDirectory() as d:
            os.mkdir(os.path.join(d, "x"))
            for name, size in (("a", 100), ("b", 250)):
                with open(os.path.join(d, "x", name), "wb") as fh:
                    fh.write(b"0" * size)
            t = archive.Tracker("compress", "tar", d)
            for line in ("x/", "x/a", "x/b"):
                t.feed_line(line + "\n")
            self.assertEqual(t.bytes_done(), 100)  # b is still being read
            self.assertEqual(archive.tree_size([os.path.join(d, "x")]), 350)

    def test_zip_lines(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "f"), "wb") as fh:
                fh.write(b"0" * 10)
            t = archive.Tracker("compress", "zip", d)
            self.assertEqual(t.feed_line("  adding: f (deflated 40%)\n"), "f")
            self.assertEqual(t.feed_line("garbage"), None)

    def test_unzip_uses_central_directory_sizes(self):
        t = archive.Tracker("extract", "zip", None, {"a": 5, "b": 7})
        t.feed_line("  inflating: a\n")
        t.feed_line(" extracting: b\n")
        self.assertEqual(t.bytes_done(), 5)


class ValidateArchiveTests(unittest.TestCase):
    def setUp(self):
        self.daemon = filetransferd.TransferDaemon.__new__(filetransferd.TransferDaemon)
        self.daemon.jobs = {}
        self.daemon.queue_order = []
        self.daemon.running_ids = set()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.d = self.tmp.name
        open(os.path.join(self.d, "a"), "w").close()

    def test_compress_ok(self):
        err = self.daemon._validate_enqueue("compress", [os.path.join(self.d, "a")], os.path.join(self.d, "a.tar.gz"))
        self.assertIsNone(err)

    def test_compress_refuses_existing_and_bad_extension(self):
        src = [os.path.join(self.d, "a")]
        self.assertIsNotNone(self.daemon._validate_enqueue("compress", src, os.path.join(self.d, "a.rar")))
        existing = os.path.join(self.d, "e.zip")
        open(existing, "w").close()
        self.assertIn("already exists", self.daemon._validate_enqueue("compress", src, existing))

    def test_compress_needs_one_parent_and_absolute_paths(self):
        self.assertIsNotNone(self.daemon._validate_enqueue("compress", ["rel"], os.path.join(self.d, "x.zip")))
        self.assertIn("same folder", self.daemon._validate_enqueue(
            "compress", [os.path.join(self.d, "a"), "/etc/hostname"], os.path.join(self.d, "x.zip")))

    def test_extract_rules(self):
        arc = os.path.join(self.d, "z.tar.gz")
        open(arc, "w").close()
        self.assertIsNone(self.daemon._validate_enqueue("extract", [arc], os.path.join(self.d, "z")))
        self.assertIn("exactly one", self.daemon._validate_enqueue("extract", [arc, arc], os.path.join(self.d, "z")))
        self.assertIn("unsupported", self.daemon._validate_enqueue(
            "extract", [os.path.join(self.d, "a")], os.path.join(self.d, "z")))
        self.assertIn("not a folder", self.daemon._validate_enqueue("extract", [arc], os.path.join(self.d, "a")))


if __name__ == "__main__":
    unittest.main()
