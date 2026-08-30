import importlib
import sqlite3
import sys
import types
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


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


class ManagerBackedLogRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.models = importlib.import_module("core.domain.models")
        cls.repo_module = importlib.import_module("core.repositories.sqlite_log_repo")

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        self._create_database()
        self.repo = self.repo_module.SqliteLogRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _create_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE users (
                    user_id TEXT PRIMARY KEY,
                    nickname TEXT
                );
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
                CREATE TABLE user_fish_stats (
                    user_id TEXT NOT NULL,
                    fish_id INTEGER NOT NULL,
                    first_caught_at DATETIME,
                    last_caught_at DATETIME,
                    max_weight INTEGER NOT NULL,
                    min_weight INTEGER NOT NULL,
                    total_caught INTEGER NOT NULL,
                    total_weight INTEGER NOT NULL,
                    PRIMARY KEY (user_id, fish_id)
                );
                CREATE TABLE gacha_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    gacha_pool_id INTEGER NOT NULL,
                    item_type TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    item_name TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    rarity INTEGER NOT NULL,
                    timestamp DATETIME
                );
                CREATE TABLE wipe_bomb_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    contribution_amount INTEGER NOT NULL,
                    reward_multiplier REAL NOT NULL,
                    reward_amount INTEGER NOT NULL,
                    timestamp DATETIME
                );
                CREATE TABLE check_ins (
                    user_id TEXT NOT NULL,
                    check_in_date DATE NOT NULL,
                    PRIMARY KEY (user_id, check_in_date)
                );
                CREATE TABLE taxes (
                    tax_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    tax_amount INTEGER NOT NULL,
                    tax_rate REAL NOT NULL,
                    original_amount INTEGER NOT NULL,
                    balance_after INTEGER NOT NULL,
                    tax_type TEXT NOT NULL,
                    timestamp DATETIME
                );
                INSERT INTO users VALUES ('u1', 'user');
                """
            )

    def _fishing_record(self):
        return self.models.FishingRecord(
            record_id=None,
            user_id="u1",
            fish_id=1,
            weight=20,
            value=30,
            timestamp=datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc),
            is_king_size=True,
        )

    def test_all_log_writes_use_replayed_transactions(self):
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        gacha_record = self.models.GachaRecord(
            record_id=None,
            user_id="u1",
            gacha_pool_id=1,
            item_type="fish",
            item_id=1,
            item_name="fish",
            timestamp=now,
        )
        wipe_log = self.models.WipeBombLog(
            log_id=None,
            user_id="u1",
            contribution_amount=10,
            reward_multiplier=1.5,
            reward_amount=15,
            timestamp=now,
        )
        tax_record = self.models.TaxRecord(
            tax_id=None,
            user_id="u1",
            tax_amount=3,
            tax_rate=0.1,
            original_amount=30,
            balance_after=27,
            timestamp=now,
            tax_type="daily",
        )

        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.assertTrue(self.repo.add_fishing_record(self._fishing_record()))
            self.repo.add_gacha_record(gacha_record)
            self.repo.add_wipe_bomb_log(wipe_log)
            self.repo.add_check_in("u1", date(2026, 8, 29))
            self.repo.add_log("u1", "test", "message")
            self.repo.add_tax_record(tax_record)
            self.assertEqual(
                self.repo.cleanup_old_fishing_records(days=30, batch_size=10),
                0,
            )

        self.assertEqual(run_transaction.call_count, 7)
        self.assertEqual(len(self.repo.get_fishing_records("u1", 10)), 1)
        self.assertEqual(self.repo.get_user_fish_stat("u1", 1).total_caught, 1)
        self.assertEqual(len(self.repo.get_gacha_records("u1", 10)), 1)
        self.assertEqual(len(self.repo.get_wipe_bomb_logs("u1", 10)), 2)
        self.assertTrue(self.repo.has_checked_in("u1", date(2026, 8, 29)))
        self.assertEqual(len(self.repo.get_tax_records("u1", 10)), 1)
        self.assertEqual(
            self.repo._conn_mgr.detect_types, sqlite3.PARSE_DECLTYPES
        )

    def test_fishing_record_failure_rolls_back_aggregate_and_record(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_fish_stats
                BEFORE INSERT ON user_fish_stats
                BEGIN
                    SELECT RAISE(ABORT, 'forced stats failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.add_fishing_record(self._fishing_record())

        self.assertFalse(self.repo._conn_mgr._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM fishing_records").fetchone()[0],
                0,
            )
            conn.execute(
                "INSERT INTO wipe_bomb_log VALUES (NULL, 'other', 0, 0, 0, ?)",
                (datetime.now(),),
            )

    def test_tax_cleanup_remains_batched_inside_insert_transaction(self):
        old_time = datetime.now(timezone.utc) - timedelta(days=120)
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO taxes (
                    user_id, tax_amount, tax_rate, original_amount,
                    balance_after, tax_type, timestamp
                ) VALUES ('u1', 1, 0.1, 10, 9, 'old', ?)
                """,
                ((old_time,), (old_time,), (old_time,)),
            )

        self.repo.set_tax_record_retention(90, cleanup_batch_size=2)
        self.repo.add_tax_record(
            self.models.TaxRecord(
                tax_id=None,
                user_id="u1",
                tax_amount=2,
                tax_rate=0.1,
                original_amount=20,
                balance_after=18,
                timestamp=datetime.now(timezone.utc),
                tax_type="new",
            )
        )

        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM taxes WHERE tax_type = 'old'"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM taxes WHERE tax_type = 'new'"
                ).fetchone()[0],
                1,
            )

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_tax_records("u1")
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
