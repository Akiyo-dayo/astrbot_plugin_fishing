import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime, timedelta
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


class ManagerBackedMarketRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.models = importlib.import_module("core.domain.models")
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_market_repo"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE users (
                    user_id TEXT PRIMARY KEY,
                    nickname TEXT
                );
                CREATE TABLE rods (
                    rod_id INTEGER PRIMARY KEY, name TEXT, description TEXT
                );
                CREATE TABLE accessories (
                    accessory_id INTEGER PRIMARY KEY, name TEXT, description TEXT
                );
                CREATE TABLE items (
                    item_id INTEGER PRIMARY KEY, name TEXT, description TEXT
                );
                CREATE TABLE fish (
                    fish_id INTEGER PRIMARY KEY, name TEXT, description TEXT
                );
                CREATE TABLE commodities (
                    commodity_id TEXT PRIMARY KEY, name TEXT, description TEXT
                );
                CREATE TABLE market (
                    market_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    item_type TEXT NOT NULL,
                    item_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    listed_at TEXT NOT NULL,
                    expires_at TEXT,
                    refine_level INTEGER DEFAULT 1,
                    seller_nickname TEXT,
                    item_name TEXT,
                    item_description TEXT,
                    item_instance_id INTEGER,
                    is_anonymous INTEGER DEFAULT 0,
                    quality_level INTEGER DEFAULT 0
                );
                INSERT INTO users VALUES ('seller', 'Seller');
                INSERT INTO fish VALUES (1, 'Fish', 'desc');
                """
            )
        self.repo = self.repo_module.SqliteMarketRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _listing(self):
        now = datetime(2026, 8, 29, 12, 0, 0)
        return self.models.MarketListing(
            market_id=None,
            user_id="seller",
            seller_nickname="Seller",
            item_type="fish",
            item_id=1,
            item_name="Fish",
            item_description="desc",
            quantity=2,
            price=10,
            listed_at=now,
            item_instance_id=None,
            refine_level=1,
            quality_level=1,
            expires_at=now + timedelta(days=1),
            is_anonymous=False,
        )

    def test_market_writes_use_replayed_transactions(self):
        listing = self._listing()

        with patch.object(
            self.repo.db_manager,
            "run_in_transaction",
            wraps=self.repo.db_manager.run_in_transaction,
        ) as run_transaction:
            self.repo.add_listing(listing)
            stored, total = self.repo.get_all_listings()
            listing.market_id = stored[0].market_id
            listing.price = 15
            listing.refine_level = 2
            self.repo.update_listing(listing)
            updated = self.repo.get_listing_by_id(listing.market_id)
            self.repo.remove_listing(listing.market_id)

        self.assertEqual(run_transaction.call_count, 3)
        self.assertEqual(total, 1)
        self.assertEqual(updated.price, 15)
        self.assertEqual(updated.refine_level, 2)
        self.assertEqual(updated.quality_level, 1)
        self.assertIsInstance(updated.listed_at, datetime)
        self.assertEqual(self.repo.get_all_listings()[1], 0)

    def test_failed_insert_rolls_back_and_releases_lock(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_listing
                BEFORE INSERT ON market
                BEGIN
                    SELECT RAISE(ABORT, 'forced listing failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.add_listing(self._listing())

        self.assertFalse(self.repo.db_manager._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as other_conn:
            other_conn.execute("INSERT INTO users VALUES ('other', 'Other')")

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_all_listings()
        self.assertTrue(hasattr(self.repo.db_manager._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo.db_manager._local, "connection"))


if __name__ == "__main__":
    unittest.main()
