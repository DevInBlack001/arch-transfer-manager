#!/usr/bin/env python3
"""Unit tests for remote-spec detection and remote-to-remote rejection.

Run directly: python3 daemon/tests/test_remote_spec.py
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ft_common  # noqa: E402
import filetransferd  # noqa: E402


class IsRemoteSpecTests(unittest.TestCase):
    def test_local_paths_are_not_remote(self):
        for path in ["/home/user/file.txt", "./relative/path", "~/downloads",
                     "relative/no/colon", "./weird:name", "plainname"]:
            self.assertFalse(ft_common.is_remote_spec(path), path)

    def test_remote_specs_are_detected(self):
        for path in ["nas.local:/mnt/data", "user@nas.local:/mnt/data",
                     "armiya@192.168.1.50:/srv/backup", "host:relative/path"]:
            self.assertTrue(ft_common.is_remote_spec(path), path)

    def test_rsync_daemon_protocol_is_excluded(self):
        self.assertFalse(ft_common.is_remote_spec("rsync://host/path"))


class ValidateEnqueueRemoteTests(unittest.TestCase):
    def setUp(self):
        self.daemon = filetransferd.TransferDaemon.__new__(filetransferd.TransferDaemon)
        self.daemon.jobs = {}
        self.daemon.queue_order = []
        self.daemon.running_ids = set()

    def test_remote_to_remote_is_rejected(self):
        error = self.daemon._validate_enqueue(
            "copy", ["user@a.local:/src"], "user@b.local:/dst")
        self.assertIsNotNone(error)
        self.assertIn("remote-to-remote", error)

    def test_remote_source_local_dest_is_allowed_shape(self):
        error = self.daemon._validate_enqueue(
            "copy", ["user@a.local:/src"], "/tmp")
        self.assertIsNone(error)


if __name__ == "__main__":
    unittest.main()
