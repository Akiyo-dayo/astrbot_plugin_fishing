import contextlib
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


@contextlib.contextmanager
def _sqlite(db_path):
    """提交并关闭的连接助手。

    sqlite3.Connection 的 with 只负责提交/回滚事务，不会关闭连接。测试里直接
    用 `with sqlite3.connect(...)` 会把文件句柄一直挂着，Windows 上删临时目录
    时报 PermissionError。
    """
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


class _ClosesConnectionsMixin:
    """仓储持有线程本地长连接，测试结束前必须显式关闭。

    不关的话，TemporaryDirectory 在 Windows 上会因为 fish.db 仍被占用而删除
    失败，断言明明通过、用例却报 PermissionError。
    """

    def setUp(self):
        super().setUp()
        self._tracked_closables = []

    def tearDown(self):
        for closable in self._tracked_closables:
            try:
                closable.close_connection()
            except Exception:
                pass
        self._tracked_closables.clear()
        super().tearDown()

    def _track(self, closable):
        self._tracked_closables.append(closable)
        return closable

    @contextlib.contextmanager
    def temp_workspace(self):
        """临时目录 + 退出前关闭其中打开的所有仓储连接。

        tearDown 里关来不及：那时 TemporaryDirectory 已经在删目录了。
        """
        with TemporaryDirectory() as temp_dir:
            try:
                yield temp_dir
            finally:
                for closable in self._tracked_closables:
                    try:
                        closable.close_connection()
                    except Exception:
                        pass
                self._tracked_closables.clear()


class SqliteAtomicOperationTests(_ClosesConnectionsMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.inventory_module = importlib.import_module(
            "core.repositories.sqlite_inventory_repo"
        )
        cls.user_module = importlib.import_module("core.repositories.sqlite_user_repo")

    def _create_database(self, db_path: Path):
        with _sqlite(db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                CREATE TABLE users (
                    user_id TEXT PRIMARY KEY, coins INTEGER NOT NULL DEFAULT 0,
                    max_coins INTEGER NOT NULL DEFAULT 0,
                    auto_fishing_enabled INTEGER NOT NULL DEFAULT 0,
                    total_fishing_count INTEGER NOT NULL DEFAULT 0,
                    total_weight_caught INTEGER NOT NULL DEFAULT 0,
                    total_coins_earned INTEGER NOT NULL DEFAULT 0,
                    last_fishing_time DATETIME,
                    equipped_rod_instance_id INTEGER
                );
                CREATE TABLE fish (
                    fish_id INTEGER PRIMARY KEY, name TEXT, rarity INTEGER,
                    base_value INTEGER NOT NULL
                );
                CREATE TABLE user_fish_inventory (
                    user_id TEXT, fish_id INTEGER, quality_level INTEGER, quantity INTEGER,
                    PRIMARY KEY (user_id, fish_id, quality_level)
                );
                CREATE TABLE user_aquarium (
                    user_id TEXT, fish_id INTEGER, quality_level INTEGER, quantity INTEGER,
                    added_at DATETIME,
                    PRIMARY KEY (user_id, fish_id, quality_level)
                );
                CREATE TABLE user_rods (
                    rod_instance_id INTEGER PRIMARY KEY, user_id TEXT, rod_id INTEGER,
                    current_durability INTEGER, is_equipped INTEGER, is_locked INTEGER
                );
                CREATE TABLE user_accessories (
                    accessory_instance_id INTEGER PRIMARY KEY, user_id TEXT,
                    accessory_id INTEGER, is_equipped INTEGER, is_locked INTEGER
                );
                CREATE TABLE fishing_zones (
                    id INTEGER PRIMARY KEY, rare_fish_caught_today INTEGER
                );
                CREATE TABLE fishing_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, fish_id INTEGER,
                    weight INTEGER, value INTEGER, rod_instance_id INTEGER,
                    accessory_instance_id INTEGER, bait_id INTEGER,
                    timestamp DATETIME, is_king_size INTEGER
                );
                CREATE TABLE user_fish_stats (
                    user_id TEXT, fish_id INTEGER, first_caught_at DATETIME,
                    last_caught_at DATETIME, max_weight INTEGER, min_weight INTEGER,
                    total_caught INTEGER, total_weight INTEGER,
                    PRIMARY KEY (user_id, fish_id)
                );
                INSERT INTO users (user_id, coins, max_coins) VALUES ('u1', 100, 100);
                INSERT INTO fish VALUES (1, 'fish', 4, 10);
                INSERT INTO fishing_zones VALUES (1, 0);
                """
            )

    def test_fish_sale_updates_inventory_and_coins_atomically(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with _sqlite(db_path) as conn:
                conn.execute("INSERT INTO user_fish_inventory VALUES ('u1', 1, 1, 3)")

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            result = repo.sell_fish_atomic("u1")

            self.assertEqual(result["total_value"], 60)
            with _sqlite(db_path) as conn:
                self.assertEqual(conn.execute("SELECT coins FROM users").fetchone()[0], 160)
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM user_fish_inventory").fetchone()[0],
                    0,
                )

    def test_fish_sale_rolls_back_inventory_when_credit_fails(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with _sqlite(db_path) as conn:
                conn.execute("INSERT INTO user_fish_inventory VALUES ('u1', 1, 0, 2)")
                conn.execute(
                    "CREATE TRIGGER reject_credit BEFORE UPDATE ON users "
                    "BEGIN SELECT RAISE(ABORT, 'forced credit failure'); END"
                )

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            with self.assertRaises(sqlite3.IntegrityError):
                repo.sell_fish_atomic("u1")

            with _sqlite(db_path) as conn:
                self.assertEqual(conn.execute("SELECT coins FROM users").fetchone()[0], 100)
                self.assertEqual(
                    conn.execute("SELECT quantity FROM user_fish_inventory").fetchone()[0],
                    2,
                )

    def test_smart_deduction_rolls_back_pond_when_aquarium_is_insufficient(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with _sqlite(db_path) as conn:
                conn.execute("INSERT INTO user_fish_inventory VALUES ('u1', 1, 0, 2)")
                conn.execute(
                    "INSERT INTO user_aquarium "
                    "(user_id, fish_id, quality_level, quantity) "
                    "VALUES ('u1', 1, 0, 1)"
                )

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            with self.assertRaises(ValueError):
                repo.deduct_fish_smart("u1", 1, 4)

            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute("SELECT quantity FROM user_fish_inventory").fetchone()[0],
                    2,
                )
                self.assertEqual(
                    conn.execute("SELECT quantity FROM user_aquarium").fetchone()[0],
                    1,
                )

    def test_aquarium_writes_commit_as_complete_operations(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))

            repo.add_fish_to_aquarium("u1", 1, 3, quality_level=1)
            repo.remove_fish_from_aquarium("u1", 1, 1, quality_level=1)

            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT quantity FROM user_aquarium "
                        "WHERE user_id = 'u1' AND fish_id = 1 AND quality_level = 1"
                    ).fetchone()[0],
                    2,
                )

            repo.clear_aquarium_inventory("u1", rarity=4)
            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM user_aquarium").fetchone()[0],
                    0,
                )

    def test_smart_deduction_commits_pond_and_aquarium_together(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with _sqlite(db_path) as conn:
                conn.execute("INSERT INTO user_fish_inventory VALUES ('u1', 1, 0, 2)")
                conn.execute(
                    "INSERT INTO user_aquarium "
                    "(user_id, fish_id, quality_level, quantity) "
                    "VALUES ('u1', 1, 0, 3)"
                )

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            repo.deduct_fish_smart("u1", 1, 4)

            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute("SELECT COUNT(*) FROM user_fish_inventory").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    conn.execute("SELECT quantity FROM user_aquarium").fetchone()[0],
                    1,
                )

    def test_fishing_settlement_rolls_back_everything_when_log_insert_fails(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with _sqlite(db_path) as conn:
                conn.execute("INSERT INTO user_rods VALUES (7, 'u1', 1, 5, 1, 0)")
                conn.execute(
                    "CREATE TRIGGER reject_log BEFORE INSERT ON fishing_records "
                    "BEGIN SELECT RAISE(ABORT, 'forced log failure'); END"
                )

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            with self.assertRaises(sqlite3.IntegrityError):
                repo.settle_fishing_catch(
                    user_id="u1", fish_id=1, total_catches=1, quality_level=0,
                    weight=20, base_value=10, earned_value=10, fishing_cost=10,
                    fish_pond_capacity=10, timestamp=__import__("datetime").datetime.now(),
                    zone_id=1, is_rare=True, rod_instance_id=7,
                    rod_durability=4, rod_broken=False,
                    accessory_instance_id=None, bait_id=None,
                )

            with _sqlite(db_path) as conn:
                self.assertEqual(conn.execute("SELECT coins FROM users").fetchone()[0], 100)
                self.assertEqual(conn.execute("SELECT current_durability FROM user_rods").fetchone()[0], 5)
                self.assertEqual(conn.execute("SELECT rare_fish_caught_today FROM fishing_zones").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM user_fish_inventory").fetchone()[0], 0)
                self.assertEqual(conn.execute("SELECT total_fishing_count FROM users").fetchone()[0], 0)

    def test_fishing_settlement_does_not_delete_other_users_old_records(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            old_timestamp = datetime.now() - timedelta(days=90)
            with _sqlite(db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO fishing_records (
                        user_id, fish_id, weight, value, timestamp, is_king_size
                    ) VALUES (?, 1, 1, 1, ?, 0)
                    """,
                    ("inactive-user", old_timestamp),
                )

            repo = self._track(self.inventory_module.SqliteInventoryRepository(str(db_path)))
            self.assertTrue(
                repo.settle_fishing_catch(
                    user_id="u1",
                    fish_id=1,
                    total_catches=1,
                    quality_level=0,
                    weight=20,
                    base_value=10,
                    earned_value=10,
                    fishing_cost=10,
                    fish_pond_capacity=10,
                    timestamp=datetime.now(),
                    zone_id=1,
                    is_rare=False,
                    rod_instance_id=None,
                    rod_durability=None,
                    rod_broken=False,
                    accessory_instance_id=None,
                    bait_id=None,
                )
            )

            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM fishing_records WHERE user_id = ?",
                        ("inactive-user",),
                    ).fetchone()[0],
                    1,
                )

    def test_auto_fishing_toggle_does_not_overwrite_coins(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self._track(self.user_module.SqliteUserRepository(str(db_path)))

            self.assertTrue(repo.toggle_auto_fishing("u1"))
            with _sqlite(db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT auto_fishing_enabled, coins FROM users WHERE user_id = 'u1'"
                    ).fetchone(),
                    (1, 100),
                )

    def test_coin_deduction_is_atomic_and_capped_at_balance(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self._track(self.user_module.SqliteUserRepository(str(db_path)))

            deducted, balance = repo.deduct_coins_up_to("u1", 130)

            self.assertEqual((deducted, balance), (100, 0))
            with _sqlite(db_path) as conn:
                self.assertEqual(conn.execute("SELECT coins FROM users").fetchone()[0], 0)


class ConnectionRetryTests(_ClosesConnectionsMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.manager_module = importlib.import_module("core.database.connection_manager")

    def test_execute_with_retry_replays_locked_statement(self):
        with self.temp_workspace() as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            with _sqlite(db_path) as conn:
                conn.execute("CREATE TABLE values_table (value INTEGER)")

            manager = self._track(self.manager_module.DatabaseConnectionManager(
                str(db_path), timeout=0, max_retries=2, retry_delay=0
            ))
            blocker = sqlite3.connect(db_path)
            blocker.execute("BEGIN IMMEDIATE")
            blocker.execute("INSERT INTO values_table VALUES (1)")

            real_sleep = self.manager_module.time.sleep
            calls = 0

            def release_then_continue(_delay):
                nonlocal calls
                calls += 1
                blocker.rollback()
                real_sleep(0.01)

            with patch.object(self.manager_module.time, "sleep", release_then_continue):
                manager.execute_with_retry("INSERT INTO values_table VALUES (?)", (2,))

            blocker.close()

            self.assertEqual(calls, 1)
            with _sqlite(db_path) as conn:
                self.assertEqual(conn.execute("SELECT value FROM values_table").fetchall(), [(2,)])


if __name__ == "__main__":
    unittest.main()
