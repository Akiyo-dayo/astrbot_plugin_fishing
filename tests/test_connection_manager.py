import importlib
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


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


class ConnectionManagerConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.module = importlib.import_module("core.database.connection_manager")

    def setUp(self):
        self.temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.temp_dir.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE values_table (value INTEGER)")
        self.managers = []

    def tearDown(self):
        for manager in self.managers:
            manager.close_connection()
        self.temp_dir.cleanup()

    def _manager(self, **kwargs):
        manager = self.module.DatabaseConnectionManager(str(self.db_path), **kwargs)
        self.managers.append(manager)
        return manager

    def test_detect_types_can_be_disabled_for_legacy_timestamps(self):
        legacy_timestamp = "2026-08-29T12:34:56.123456+08:00"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("CREATE TABLE legacy_events (happened_at TIMESTAMP)")
            conn.execute(
                "INSERT INTO legacy_events(happened_at) VALUES (?)",
                (legacy_timestamp,),
            )

        manager = self._manager(detect_types=0)
        with manager.get_connection() as conn:
            value = conn.execute(
                "SELECT happened_at FROM legacy_events"
            ).fetchone()[0]

        self.assertEqual(value, legacy_timestamp)
        self.assertIsInstance(value, str)

    def test_row_factory_and_connection_pragmas_are_configurable(self):
        manager = self._manager(
            row_factory=None,
            foreign_keys=False,
            synchronous=None,
        )
        with manager.get_connection() as conn:
            row = conn.execute("SELECT 7").fetchone()
            foreign_keys = conn.execute("PRAGMA foreign_keys").fetchone()[0]

        self.assertEqual(row, (7,))
        self.assertEqual(foreign_keys, 0)

    def test_transaction_retry_replays_after_the_write_lock_is_released(self):
        manager = self._manager(
            timeout=0,
            max_retries=2,
            retry_delay=0,
            retry_timeout=1,
        )
        blocker = sqlite3.connect(self.db_path)
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("INSERT INTO values_table VALUES (1)")
        real_sleep = self.module.time.sleep
        sleep_calls = 0

        def release_then_continue(_delay):
            nonlocal sleep_calls
            sleep_calls += 1
            blocker.rollback()
            real_sleep(0.01)

        try:
            with patch.object(self.module.time, "sleep", release_then_continue):
                manager.run_in_transaction(
                    lambda cursor: cursor.execute(
                        "INSERT INTO values_table VALUES (?)", (2,)
                    )
                )
        finally:
            blocker.close()

        self.assertEqual(sleep_calls, 1)
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute("SELECT value FROM values_table").fetchall(), [(2,)]
            )

    def test_total_retry_budget_prevents_an_over_budget_sleep(self):
        manager = self._manager(
            timeout=0,
            max_retries=10,
            retry_delay=1,
            retry_timeout=0.05,
        )
        blocker = sqlite3.connect(self.db_path)
        blocker.execute("BEGIN IMMEDIATE")
        blocker.execute("INSERT INTO values_table VALUES (1)")

        try:
            with patch.object(self.module.time, "sleep") as mocked_sleep:
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    manager.execute_with_retry(
                        "INSERT INTO values_table VALUES (?)", (2,)
                    )
            mocked_sleep.assert_not_called()
        finally:
            blocker.rollback()
            blocker.close()

    def test_busy_timeout_uses_remaining_budget_and_is_restored(self):
        manager = self._manager(timeout=5, retry_timeout=1)
        seen_timeouts = []

        def operation(cursor):
            seen_timeouts.append(cursor.execute("PRAGMA busy_timeout").fetchone()[0])

        manager.run_in_transaction(operation)
        with manager.get_connection() as conn:
            restored_timeout = conn.execute("PRAGMA busy_timeout").fetchone()[0]

        self.assertEqual(len(seen_timeouts), 1)
        self.assertGreater(seen_timeouts[0], 0)
        self.assertLessEqual(seen_timeouts[0], 1000)
        self.assertEqual(restored_timeout, 5000)

    def test_invalid_connection_options_are_rejected(self):
        invalid_options = (
            {"timeout": -1},
            {"max_retries": -1},
            {"retry_delay": -1},
            {"retry_timeout": 0},
            {"synchronous": "INVALID"},
        )
        for options in invalid_options:
            with self.subTest(options=options):
                with self.assertRaises(ValueError):
                    self.module.DatabaseConnectionManager(
                        str(self.db_path), **options
                    )


if __name__ == "__main__":
    unittest.main()
