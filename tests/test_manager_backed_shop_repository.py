import importlib
import sqlite3
import sys
import types
import unittest
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


class ManagerBackedShopRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_shop_repo"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;
                CREATE TABLE users (user_id TEXT PRIMARY KEY);
                CREATE TABLE shops (
                    shop_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    shop_type TEXT NOT NULL DEFAULT 'normal',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    start_time DATETIME,
                    end_time DATETIME,
                    daily_start_time TIME,
                    daily_end_time TIME,
                    sort_order INTEGER NOT NULL DEFAULT 100,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME
                );
                CREATE TABLE shop_items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    shop_id INTEGER NOT NULL REFERENCES shops(shop_id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    description TEXT,
                    category TEXT NOT NULL DEFAULT 'general',
                    stock_total INTEGER,
                    stock_sold INTEGER NOT NULL DEFAULT 0,
                    per_user_limit INTEGER,
                    per_user_daily_limit INTEGER,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    start_time DATETIME,
                    end_time DATETIME,
                    sort_order INTEGER NOT NULL DEFAULT 100,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME
                );
                CREATE TABLE shop_item_costs (
                    cost_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES shop_items(item_id) ON DELETE CASCADE,
                    cost_type TEXT NOT NULL,
                    cost_amount INTEGER NOT NULL,
                    cost_item_id INTEGER,
                    cost_relation TEXT DEFAULT 'and',
                    group_id INTEGER,
                    quality_level INTEGER DEFAULT 0
                );
                CREATE TABLE shop_item_rewards (
                    reward_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL REFERENCES shop_items(item_id) ON DELETE CASCADE,
                    reward_type TEXT NOT NULL,
                    reward_item_id INTEGER,
                    reward_quantity INTEGER NOT NULL DEFAULT 1,
                    reward_refine_level INTEGER,
                    quality_level INTEGER DEFAULT 0
                );
                CREATE TABLE shop_purchase_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    item_id INTEGER NOT NULL REFERENCES shop_items(item_id) ON DELETE CASCADE,
                    quantity INTEGER NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                INSERT INTO users VALUES ('u1');
                """
            )
        self.repo = self.repo_module.SqliteShopRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def test_all_shop_writes_use_replayed_transactions(self):
        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            shop = self.repo.create_shop(
                {
                    "name": "Main",
                    "description": "shop",
                    "daily_start_time": "00:00",
                    "daily_end_time": "23:59",
                }
            )
            self.repo.update_shop(shop["shop_id"], {"name": "Renamed"})
            item = self.repo.create_shop_item(
                shop["shop_id"],
                {
                    "name": "Offer",
                    "stock_total": 10,
                    "per_user_limit": 5,
                },
            )
            self.repo.update_shop_item(item["item_id"], {"category": "fish"})
            self.repo.increase_item_sold(item["item_id"], 2)

            self.repo.add_item_cost(
                item["item_id"],
                {"cost_type": "coins", "cost_amount": 10},
            )
            cost_id = self.repo.get_item_costs(item["item_id"])[0]["cost_id"]
            self.repo.update_item_cost(cost_id, {"cost_amount": 12})
            self.repo.delete_item_cost(cost_id)

            self.repo.add_item_reward(
                item["item_id"],
                {"reward_type": "fish", "reward_item_id": 1},
            )
            reward_id = self.repo.get_item_rewards(item["item_id"])[0]["reward_id"]
            self.repo.update_item_reward(reward_id, {"reward_quantity": 2})
            self.repo.delete_item_reward(reward_id)

            self.repo.add_purchase_record("u1", item["item_id"], 2)
            self.repo.delete_shop_item(item["item_id"])
            self.repo.delete_shop(shop["shop_id"])

        self.assertEqual(run_transaction.call_count, 14)
        self.assertEqual(self.repo.get_all_shops(), [])
        self.assertEqual(self.repo.get_user_purchased_count("u1", item["item_id"]), 0)

    def test_read_and_normalization_contract_is_preserved(self):
        shop = self.repo.create_shop(
            {
                "name": "Main",
                "is_active": True,
                "daily_start_time": "00:00:00",
                "daily_end_time": "23:59:00",
            }
        )
        item = self.repo.create_shop_item(
            shop["shop_id"], {"name": "Offer", "category": "fish"}
        )

        self.assertIs(shop["is_active"], True)
        self.assertEqual(shop["daily_start_time"], "00:00")
        self.assertEqual(len(self.repo.get_active_shops()), 1)
        self.assertEqual(self.repo.get_offer_by_id(item["item_id"])["name"], "Offer")
        self.assertEqual(len(self.repo.get_active_offers("fish")), 1)

    def test_foreign_key_failure_rolls_back_and_releases_lock(self):
        shop = self.repo.create_shop({"name": "Main"})
        item = self.repo.create_shop_item(shop["shop_id"], {"name": "Offer"})

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.add_purchase_record("missing-user", item["item_id"], 1)

        self.assertFalse(self.repo._conn_mgr._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as other_conn:
            other_conn.execute("INSERT INTO users VALUES ('other')")

    def test_empty_updates_do_not_open_write_transactions(self):
        shop = self.repo.create_shop({"name": "Main"})
        item = self.repo.create_shop_item(shop["shop_id"], {"name": "Offer"})

        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.repo.update_shop(shop["shop_id"], {})
            self.repo.update_shop_item(item["item_id"], {})
            self.repo.update_item_cost(1, {})
            self.repo.update_item_reward(1, {})

        run_transaction.assert_not_called()

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_all_shops()
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
