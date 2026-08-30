import dataclasses
import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime
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


class ManagerBackedUserRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.models = importlib.import_module("core.domain.models")
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_user_repo"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        self._create_database()
        self.repo = self.repo_module.SqliteUserRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _create_database(self):
        datetime_fields = {
            "created_at",
            "bait_start_time",
            "last_fishing_time",
            "last_wipe_bomb_time",
            "last_steal_time",
            "last_electric_fish_time",
            "last_login_time",
            "last_stolen_at",
            "last_wof_play_time",
            "wof_last_action_time",
            "last_sicbo_time",
        }
        text_fields = {
            "user_id",
            "nickname",
            "wipe_bomb_forecast",
            "last_wof_date",
            "last_wipe_bomb_date",
        }
        real_fields = {
            "max_wipe_bomb_multiplier",
            "min_wipe_bomb_multiplier",
        }
        columns = []
        for field in dataclasses.fields(self.models.User):
            if field.name == "user_id":
                declaration = "TEXT PRIMARY KEY"
            elif field.name in datetime_fields:
                declaration = "DATETIME"
            elif field.name in text_fields:
                declaration = "TEXT"
            elif field.name in real_fields:
                declaration = "REAL"
            else:
                declaration = "INTEGER"
            columns.append(f'"{field.name}" {declaration}')

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"CREATE TABLE users ({', '.join(columns)})")

    def _user(self, user_id="u1", coins=100):
        return self.models.User(
            user_id=user_id,
            created_at=datetime(2026, 8, 29, 12, 0, 0),
            nickname=user_id,
            coins=coins,
            max_coins=coins,
        )

    def test_all_repository_writes_use_replayed_transactions(self):
        user = self._user()
        missing_user = self._user("u2", coins=30)

        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.repo.add(user)
            user.coins = 120
            self.repo.update(user)
            self.assertTrue(self.repo.toggle_auto_fishing("u1"))
            self.assertTrue(self.repo.set_auto_fishing_enabled("u1", False))
            self.assertTrue(
                self.repo.record_failed_fishing(
                    "u1", 5, datetime(2026, 8, 29, 12, 1, 0)
                )
            )
            self.assertTrue(
                self.repo.update_bait_state(
                    "u1", 7, datetime(2026, 8, 29, 12, 2, 0)
                )
            )
            self.assertTrue(self.repo.set_fishing_zone("u1", 3))
            self.assertEqual(self.repo.deduct_coins_up_to("u1", 20), (20, 95))
            self.repo.update(missing_user)
            self.assertTrue(self.repo.delete_user("u2"))

        self.assertEqual(run_transaction.call_count, 10)
        stored = self.repo.get_by_id("u1")
        self.assertEqual(stored.coins, 95)
        self.assertEqual(stored.max_coins, 120)
        self.assertFalse(stored.auto_fishing_enabled)
        self.assertEqual(stored.current_bait_id, 7)
        self.assertEqual(stored.fishing_zone_id, 3)
        self.assertFalse(self.repo.check_exists("u2"))

    def test_write_failure_rolls_back_and_releases_lock(self):
        self.repo.add(self._user())
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_coin_update
                BEFORE UPDATE OF coins ON users
                BEGIN
                    SELECT RAISE(ABORT, 'forced user update failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.record_failed_fishing("u1", 5, datetime.now())

        self.assertFalse(self.repo._conn_mgr._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as other_conn:
            other_conn.execute(
                "INSERT INTO users (user_id, created_at, nickname) VALUES (?, ?, ?)",
                ("other", datetime.now(), "other"),
            )

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_users_count()
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
