import importlib.util
import sqlite3
import unittest
from pathlib import Path


MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "core"
    / "database"
    / "migrations"
    / "049_add_performance_indexes.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location("migration_049", MIGRATION_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载迁移: {MIGRATION_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PerformanceIndexMigrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.migration = _load_migration()

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.executescript(
            """
            CREATE TABLE users (
                user_id TEXT PRIMARY KEY,
                auto_fishing_enabled INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE fishing_records (
                record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                timestamp DATETIME
            );
            """
        )

    def tearDown(self):
        self.conn.close()

    def _index_names(self, table_name: str):
        return {
            row[1]
            for row in self.conn.execute(f"PRAGMA index_list({table_name})").fetchall()
        }

    def test_up_creates_both_indexes_and_is_idempotent(self):
        cursor = self.conn.cursor()
        self.migration.up(cursor)
        self.migration.up(cursor)

        self.assertIn(
            "idx_fishing_records_timestamp", self._index_names("fishing_records")
        )
        self.assertIn("idx_users_auto_fishing", self._index_names("users"))

        auto_index_sql = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE name = 'idx_users_auto_fishing'"
        ).fetchone()[0]
        self.assertIn("WHERE auto_fishing_enabled = 1", auto_index_sql)

    def test_indexes_are_used_by_the_target_queries(self):
        self.conn.executemany(
            "INSERT INTO users(user_id, auto_fishing_enabled) VALUES (?, ?)",
            ((f"u{i}", 1 if i < 5 else 0) for i in range(100)),
        )
        self.conn.executemany(
            "INSERT INTO fishing_records(user_id, timestamp) VALUES (?, ?)",
            (("u0", f"2026-01-{day:02d} 00:00:00") for day in range(1, 29)),
        )
        self.migration.up(self.conn.cursor())
        self.conn.execute("ANALYZE")

        auto_plan = " ".join(
            row[3]
            for row in self.conn.execute(
                "EXPLAIN QUERY PLAN "
                "SELECT user_id FROM users WHERE auto_fishing_enabled = 1"
            )
        )
        cleanup_plan = " ".join(
            row[3]
            for row in self.conn.execute(
                "EXPLAIN QUERY PLAN "
                "DELETE FROM fishing_records WHERE timestamp < ?",
                ("2026-01-15 00:00:00",),
            )
        )

        self.assertIn("idx_users_auto_fishing", auto_plan)
        self.assertIn("idx_fishing_records_timestamp", cleanup_plan)

    def test_down_removes_only_migration_indexes(self):
        cursor = self.conn.cursor()
        cursor.execute("CREATE INDEX keep_users_pk_helper ON users(user_id)")
        self.migration.up(cursor)
        self.migration.down(cursor)

        self.assertNotIn(
            "idx_fishing_records_timestamp", self._index_names("fishing_records")
        )
        self.assertNotIn("idx_users_auto_fishing", self._index_names("users"))
        self.assertIn("keep_users_pk_helper", self._index_names("users"))


if __name__ == "__main__":
    unittest.main()
