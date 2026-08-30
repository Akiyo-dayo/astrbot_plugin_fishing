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


class ManagerBackedExchangeRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.models = importlib.import_module("core.domain.models")
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_exchange_repo"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE commodities (
                    commodity_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT NOT NULL
                );
                CREATE TABLE exchange_prices (
                    price_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    time TEXT NOT NULL,
                    commodity_id TEXT NOT NULL,
                    price INTEGER NOT NULL,
                    update_type TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE user_commodities (
                    instance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    commodity_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    purchase_price INTEGER NOT NULL,
                    purchased_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                INSERT INTO commodities VALUES ('roe', 'Fish Roe', 'desc');
                """
            )
        self.repo = self.repo_module.SqliteExchangeRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _holding(self, *, expires_at=None):
        now = datetime.now()
        return self.models.UserCommodity(
            instance_id=None,
            user_id="u1",
            commodity_id="roe",
            quantity=3,
            purchase_price=10,
            purchased_at=now,
            expires_at=expires_at or now + timedelta(days=1),
        )

    def test_all_exchange_writes_use_replayed_transactions(self):
        price = self.models.Exchange(
            date="2026-08-29",
            time="12:00:00",
            commodity_id="roe",
            price=12,
            update_type="manual",
            created_at="2026-08-29T12:00:00",
        )
        holding = self._holding()
        expired = self._holding(expires_at=datetime.now() - timedelta(days=1))

        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.repo.add_exchange_price(price)
            self.assertEqual(len(self.repo.get_prices_for_date("2026-08-29")), 1)
            self.repo.delete_prices_for_date("2026-08-29")
            self.repo.add_user_commodity(holding)
            self.repo.add_user_commodity(expired)
            self.repo.update_user_commodity_quantity(holding.instance_id, 5)
            self.assertEqual(self.repo.clear_expired_commodities("u1"), 1)
            self.repo.delete_user_commodity(holding.instance_id)

        self.assertEqual(run_transaction.call_count, 7)
        self.assertIsInstance(expired.instance_id, int)
        self.assertEqual(self.repo.get_prices_for_date("2026-08-29"), [])
        self.assertEqual(self.repo.get_user_commodities("u1"), [])
        self.assertEqual(self.repo.get_all_commodities()[0].commodity_id, "roe")
        self.assertEqual(self.repo._conn_mgr.detect_types, 0)
        self.assertIsNone(self.repo._conn_mgr.row_factory)

    def test_failed_insert_does_not_mutate_domain_object_or_hold_lock(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_holding
                BEFORE INSERT ON user_commodities
                BEGIN
                    SELECT RAISE(ABORT, 'forced holding failure');
                END
                """
            )
        holding = self._holding()

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.add_user_commodity(holding)

        self.assertIsNone(holding.instance_id)
        self.assertFalse(self.repo._conn_mgr._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as other_conn:
            other_conn.execute(
                "INSERT INTO exchange_prices VALUES (NULL, ?, ?, ?, ?, ?, ?)",
                (
                    "2026-08-29",
                    "13:00:00",
                    "roe",
                    13,
                    "manual",
                    "2026-08-29T13:00:00",
                ),
            )

    def test_expired_cleanup_rolls_back_count_delete_transaction(self):
        expired = self.repo.add_user_commodity(
            self._holding(expires_at=datetime.now() - timedelta(days=1))
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_holding_delete
                BEFORE DELETE ON user_commodities
                BEGIN
                    SELECT RAISE(ABORT, 'forced cleanup failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.clear_expired_commodities("u1")

        self.assertIsNotNone(
            self.repo.get_user_commodity_by_instance_id(expired.instance_id)
        )

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_all_commodities()
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
