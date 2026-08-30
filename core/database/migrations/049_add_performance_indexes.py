"""迁移049：补充高频轮询与历史清理所需索引。"""

import sqlite3


def up(cursor: sqlite3.Cursor) -> None:
    """为钓鱼记录按时间清理和自动钓鱼轮询建立索引。"""
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fishing_records_timestamp
        ON fishing_records(timestamp)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_users_auto_fishing
        ON users(auto_fishing_enabled)
        WHERE auto_fishing_enabled = 1
        """
    )


def down(cursor: sqlite3.Cursor) -> None:
    """移除本迁移新增的索引。"""
    cursor.execute("DROP INDEX IF EXISTS idx_users_auto_fishing")
    cursor.execute("DROP INDEX IF EXISTS idx_fishing_records_timestamp")
