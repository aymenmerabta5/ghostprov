import base64
import gzip
import hashlib
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import types
import unittest


class DaemonProtocolTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.previous_workspace = os.environ.get("WORKSPACE_DIR")
        cls.previous_recent_max = os.environ.get("GP_V63_RECENT_MAX")
        cls.previous_recent_ttl = os.environ.get("GP_V63_RECENT_TTL")
        os.environ["WORKSPACE_DIR"] = cls.temp.name
        os.environ["GP_V63_RECENT_MAX"] = "32"
        os.environ["GP_V63_RECENT_TTL"] = "10"
        if os.name == "nt" and "fcntl" not in sys.modules:
            fcntl = types.ModuleType("fcntl")
            fcntl.LOCK_EX = 1
            fcntl.LOCK_NB = 2
            fcntl.flock = lambda *_args, **_kwargs: None
            sys.modules["fcntl"] = fcntl
        source = pathlib.Path(__file__).with_name("gp_daemon.py")
        spec = importlib.util.spec_from_file_location("gp_daemon_under_test", source)
        cls.daemon = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.daemon)
        cls.daemon.ensure_slot_files()
        cls.gp_dir = pathlib.Path(cls.temp.name) / "gp"

    @classmethod
    def tearDownClass(cls):
        for key, value in (
            ("WORKSPACE_DIR", cls.previous_workspace),
            ("GP_V63_RECENT_MAX", cls.previous_recent_max),
            ("GP_V63_RECENT_TTL", cls.previous_recent_ttl),
        ):
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        cls.temp.cleanup()

    def publish_and_assemble(self, request_id, encoded):
        original = json.dumps(
            {"model": "gpt-5.6-sol", "input": "hello 🚀" * 2000},
            ensure_ascii=False,
        ).encode("utf-8")
        wire = base64.b64encode(gzip.compress(original)) if encoded else original
        parts = [wire[index:index + 700] for index in range(0, len(wire), 700)]
        manifest = {
            "v": 6.3,
            "id": request_id,
            "parts": len(parts),
            "size": len(original),
            "sha12": hashlib.sha256(original).hexdigest()[:12],
            **({"enc": "gz+b64"} if encoded else {}),
        }
        for index, payload in enumerate(parts[1:], 1):
            (self.gp_dir / f"job_s1.json.{request_id}.{index}").write_bytes(payload)
        part_zero = self.daemon.format_v63_part0(manifest, parts[0])
        (self.gp_dir / f"job_s1.json.{request_id}.0").write_bytes(part_zero)
        parsed, first_payload = self.daemon.parse_v63_part0(
            part_zero, expected_id=request_id
        )
        self.assertEqual(first_payload, parts[0])
        self.assertEqual(
            self.daemon.assemble_segmented_job(1, parsed).encode("utf-8"),
            original,
        )

    def test_v63_raw_and_gzip_round_trip(self):
        self.publish_and_assemble("roundtrip-raw", encoded=False)
        self.publish_and_assemble("roundtrip-gzip", encoded=True)

    def test_newest_commit_wins_deterministically(self):
        events = [
            {"slot": 1, "request_id": "older", "order": (100, "older")},
            {"slot": 1, "request_id": "newer-a", "order": (200, "newer-a")},
            {"slot": 1, "request_id": "newer-b", "order": (200, "newer-b")},
            {"slot": 2, "request_id": "other", "order": (50, "other")},
        ]
        newest, losers = self.daemon.select_newest_v63_events(events)
        self.assertEqual(newest[1]["request_id"], "newer-b")
        self.assertEqual(newest[2]["request_id"], "other")
        self.assertEqual(
            {event["request_id"] for event in losers},
            {"older", "newer-a"},
        )

    def test_cleanup_and_seen_metadata_remain_bounded(self):
        for index in range(3):
            (self.gp_dir / f"job_s1.json.cleanup.{index}").write_text("x")
        removed = self.daemon.cleanup_v63_parts(1, "cleanup")
        self.assertEqual(len(removed), 3)
        self.assertEqual(list(self.gp_dir.glob("job_s1.json.cleanup.*")), [])

        seen = {}
        for index in range(1000):
            self.daemon.mark_v63_seen(seen, (1, f"req-{index}"), now=float(index))
        self.daemon.prune_v63_metadata({}, seen, now=1000.0)
        self.assertLessEqual(len(seen), 32)

    def test_malformed_failure_waits_for_active_worker(self):
        calls = []
        original_writer = self.daemon.write_slot_failed
        self.daemon.write_slot_failed = lambda *args, **kwargs: calls.append(
            (args, kwargs)
        )
        try:
            path = self.gp_dir / "job_s1.json.bad.0"
            path.write_bytes(b"GP63 broken")
            event = {
                "slot": 1,
                "key": (1, "bad"),
                "request_id": "bad",
                "ack": "a" * 12,
                "code": "bad_v63_commit",
                "message": "bad",
                "transport": {"v": 6.3},
            }

            class Worker:
                def __init__(self, alive):
                    self.alive = alive

                def is_alive(self):
                    return self.alive

            self.assertFalse(
                self.daemon.publish_v63_failure_if_idle(event, Worker(True), {}, {})
            )
            self.assertEqual(calls, [])
            self.assertTrue(path.exists())
            self.assertTrue(
                self.daemon.publish_v63_failure_if_idle(event, Worker(False), {}, {})
            )
            self.assertEqual(len(calls), 1)
            self.assertFalse(path.exists())
        finally:
            self.daemon.write_slot_failed = original_writer


if __name__ == "__main__":
    unittest.main()
