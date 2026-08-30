import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime, timedelta
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


class ManagerBackedSimpleRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.achievement_module = importlib.import_module(
            "core.repositories.sqlite_achievement_repo"
        )
        cls.buff_module = importlib.import_module(
            "core.repositories.sqlite_user_buff_repo"
        )
        cls.models = importlib.import_module("core.domain.models")

    def setUp(self):
        self.temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.temp_dir.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE users (user_id TEXT PRIMARY KEY);
                INSERT INTO users(user_id) VALUES ('u1');

                CREATE TABLE user_achievement_progress (
                    user_id TEXT NOT NULL,
                    achievement_id INTEGER NOT NULL,
                    current_progress INTEGER DEFAULT 0,
                    completed_at TIMESTAMP,
                    claimed_at TIMESTAMP,
                    PRIMARY KEY (user_id, achievement_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
                CREATE TABLE user_titles (
                    user_id TEXT NOT NULL,
                    title_id INTEGER NOT NULL,
                    unlocked_at TIMESTAMP,
                    PRIMARY KEY (user_id, title_id),
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
                CREATE TABLE user_buffs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    buff_type TEXT NOT NULL,
                    payload TEXT,
                    started_at DATETIME NOT NULL,
                    expires_at DATETIME,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );
                """
            )
        self.repos = []

    def tearDown(self):
        for repo in self.repos:
            repo.close_connection()
        self.temp_dir.cleanup()

    def _achievement_repo(self):
        repo = self.achievement_module.SqliteAchievementRepository(
            str(self.db_path)
        )
        self.repos.append(repo)
        return repo

    def _buff_repo(self):
        repo = self.buff_module.SqliteUserBuffRepository(str(self.db_path))
        self.repos.append(repo)
        return repo

    def test_achievement_writes_use_replayed_transactions(self):
        repo = self._achievement_repo()
        first_completed_at = datetime(2026, 8, 20, 12, 0, 0)
        later_completed_at = datetime(2026, 8, 21, 12, 0, 0)

        with patch.object(
            repo._conn_mgr,
            "run_in_transaction",
            wraps=repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            repo.update_user_progress("u1", 7, 10, first_completed_at)
            repo.update_user_progress("u1", 7, 20, later_completed_at)
            repo.grant_title_to_user("u1", 3)
            repo.revoke_title_from_user("u1", 3)

        self.assertEqual(run_transaction.call_count, 4)
        progress = repo.get_user_progress("u1")[7]
        self.assertEqual(progress["progress"], 20)
        self.assertEqual(progress["completed_at"], first_completed_at)
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM user_titles").fetchone()[0], 0)

    def test_buff_writes_use_replayed_transactions(self):
        repo = self._buff_repo()
        now = self.buff_module.get_now().replace(microsecond=0)
        buff = self.models.UserBuff(
            id=0,
            user_id="u1",
            buff_type="test",
            payload="v1",
            started_at=now,
            expires_at=now + timedelta(days=1),
        )

        with patch.object(
            repo._conn_mgr,
            "run_in_transaction",
            wraps=repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            repo.add(buff)
            stored = repo.get_active_by_user_and_type("u1", "test")
            self.assertIsNotNone(stored)
            stored.payload = "v2"
            repo.update(stored)
            repo.delete(stored.id)

        self.assertEqual(run_transaction.call_count, 3)
        self.assertIsNone(repo.get_active_by_user_and_type("u1", "test"))

    def test_buff_cleanup_is_transactional_and_connection_can_close(self):
        repo = self._buff_repo()
        now = self.buff_module.get_now().replace(microsecond=0)
        repo.add(
            self.models.UserBuff(
                id=0,
                user_id="u1",
                buff_type="expired",
                payload=None,
                started_at=now - timedelta(days=2),
                expires_at=now - timedelta(days=1),
            )
        )

        repo.delete_expired()
        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM user_buffs").fetchone()[0], 0)

        repo.get_all_active_by_user("u1")
        self.assertTrue(hasattr(repo._conn_mgr._local, "connection"))
        repo.close_connection()
        self.assertFalse(hasattr(repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
