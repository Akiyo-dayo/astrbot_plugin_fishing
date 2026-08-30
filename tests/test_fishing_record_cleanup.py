import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _install_astrbot_stub():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    api.logger = _Logger()
    astrbot.api = api
    sys.modules.setdefault("astrbot", astrbot)
    sys.modules.setdefault("astrbot.api", api)


class FishingRecordCleanupTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.log_module = importlib.import_module("core.repositories.sqlite_log_repo")
        cls.models_module = importlib.import_module("core.domain.models")

    def setUp(self):
        self.temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.temp_dir.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE fishing_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    fish_id INTEGER NOT NULL,
                    weight INTEGER NOT NULL,
                    value INTEGER NOT NULL,
                    rod_instance_id INTEGER,
                    accessory_instance_id INTEGER,
                    bait_id INTEGER,
                    timestamp DATETIME,
                    location_id INTEGER,
                    is_king_size INTEGER DEFAULT 0
                );
                CREATE INDEX idx_fishing_records_timestamp
                    ON fishing_records(timestamp);
                CREATE INDEX idx_fishing_records_user_time
                    ON fishing_records(user_id, timestamp);
                CREATE TABLE user_fish_stats (
                    user_id TEXT NOT NULL,
                    fish_id INTEGER NOT NULL,
                    first_caught_at DATETIME,
                    last_caught_at DATETIME,
                    max_weight INTEGER,
                    min_weight INTEGER,
                    total_caught INTEGER,
                    total_weight INTEGER,
                    PRIMARY KEY (user_id, fish_id)
                );
                """
            )
        self.repo = self.log_module.SqliteLogRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self.temp_dir.cleanup()

    def _insert_record(self, user_id: str, timestamp: datetime) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO fishing_records (
                    user_id, fish_id, weight, value, timestamp, is_king_size
                ) VALUES (?, 1, 1, 1, ?, 0)
                """,
                (user_id, timestamp),
            )

    def test_cleanup_deletes_only_one_oldest_batch(self):
        now = datetime.now(timezone(timedelta(hours=8)))
        for user_id, age_days in (
            ("oldest", 90),
            ("older", 60),
            ("old", 40),
            ("recent", 10),
        ):
            self._insert_record(user_id, now - timedelta(days=age_days))

        self.assertEqual(
            self.repo.cleanup_old_fishing_records(days=30, batch_size=2), 2
        )
        with sqlite3.connect(self.db_path) as conn:
            remaining = {
                row[0]
                for row in conn.execute(
                    "SELECT user_id FROM fishing_records ORDER BY record_id"
                )
            }
        self.assertEqual(remaining, {"old", "recent"})

        self.assertEqual(
            self.repo.cleanup_old_fishing_records(days=30, batch_size=2), 1
        )
        self.assertEqual(
            self.repo.cleanup_old_fishing_records(days=30, batch_size=2), 0
        )

    def test_adding_record_no_longer_runs_global_retention(self):
        now = datetime.now(timezone(timedelta(hours=8)))
        self._insert_record("inactive-user", now - timedelta(days=90))
        record = self.models_module.FishingRecord(
            record_id=0,
            user_id="active-user",
            fish_id=1,
            weight=10,
            value=20,
            timestamp=now,
        )

        self.assertTrue(self.repo.add_fishing_record(record))

        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM fishing_records WHERE user_id = ?",
                    ("inactive-user",),
                ).fetchone()[0],
                1,
            )

    def test_cleanup_rejects_unbounded_arguments(self):
        with self.assertRaises(ValueError):
            self.repo.cleanup_old_fishing_records(days=0, batch_size=100)
        with self.assertRaises(ValueError):
            self.repo.cleanup_old_fishing_records(days=30, batch_size=0)


if __name__ == "__main__":
    unittest.main()
