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


class ManagerBackedGachaRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.module = importlib.import_module("core.repositories.sqlite_gacha_repo")

    def setUp(self):
        self.temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.temp_dir.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            conn.executescript(
                """
                CREATE TABLE gacha_pools (
                    gacha_pool_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    description TEXT,
                    cost_coins INTEGER DEFAULT 0,
                    cost_premium_currency INTEGER DEFAULT 0,
                    is_limited_time INTEGER DEFAULT 0,
                    open_until TEXT
                );
                CREATE TABLE gacha_pool_items (
                    gacha_pool_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    gacha_pool_id INTEGER NOT NULL,
                    item_type TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    quantity INTEGER DEFAULT 1,
                    weight INTEGER NOT NULL,
                    FOREIGN KEY (gacha_pool_id) REFERENCES gacha_pools(gacha_pool_id)
                        ON DELETE CASCADE
                );
                """
            )
        self.repo = self.module.SqliteGachaRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self.temp_dir.cleanup()

    def _pool_data(self, name="测试池"):
        return {
            "name": name,
            "description": "desc",
            "cost_coins": 10,
            "cost_premium_currency": 0,
            "is_limited_time": False,
            "open_until": None,
        }

    def test_crud_and_copy_writes_use_replayed_transactions(self):
        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.repo.add_pool_template(self._pool_data())
            pool = self.repo.get_all_pools()[0]
            self.repo.add_item_to_pool(
                pool.gacha_pool_id,
                {"item_full_id": "fish-7", "quantity": 2, "weight": 30},
            )
            item = self.repo.get_pool_items(pool.gacha_pool_id)[0]
            self.repo.update_pool_item(
                item.gacha_pool_item_id, {"quantity": 3, "weight": 40}
            )
            copied_pool_id = self.repo.copy_pool_template(pool.gacha_pool_id)
            self.repo.update_pool_template(
                copied_pool_id, self._pool_data(name="复制池改名")
            )

        self.assertEqual(run_transaction.call_count, 5)
        copied_pool = self.repo.get_pool_by_id(copied_pool_id)
        self.assertEqual(copied_pool.name, "复制池改名")
        self.assertEqual(len(copied_pool.items), 1)
        self.assertEqual(copied_pool.items[0].quantity, 3)
        self.assertEqual(copied_pool.items[0].weight, 40)

        self.repo.delete_pool_template(pool.gacha_pool_id)
        self.assertIsNone(self.repo.get_pool_by_id(pool.gacha_pool_id))
        self.assertEqual(len(self.repo.get_pool_items(copied_pool_id)), 1)

    def test_copy_rolls_back_new_pool_when_item_copy_fails(self):
        self.repo.add_pool_template(self._pool_data())
        pool_id = self.repo.get_all_pools()[0].gacha_pool_id
        self.repo.add_item_to_pool(
            pool_id,
            {"item_full_id": "fish-7", "quantity": 1, "weight": 10},
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_copied_item
                BEFORE INSERT ON gacha_pool_items
                WHEN NEW.gacha_pool_id != 1
                BEGIN
                    SELECT RAISE(ABORT, 'forced item copy failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.copy_pool_template(pool_id)

        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM gacha_pools").fetchone()[0], 1)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM gacha_pool_items").fetchone()[0],
                1,
            )

    def test_close_releases_thread_local_connection(self):
        self.repo.get_all_pools()
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
