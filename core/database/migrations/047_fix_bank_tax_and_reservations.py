"""
迁移047：修复银行预约锁定与税收欠税表
"""

import sqlite3


def _column_exists(cursor: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table_name})")
    return any(row[1] == column_name for row in cursor.fetchall())


def up(cursor: sqlite3.Cursor):
    """添加银行锁定余额与欠税表。"""
    if not _column_exists(cursor, "bank_accounts", "locked_balance"):
        cursor.execute("""
            ALTER TABLE bank_accounts
            ADD COLUMN locked_balance INTEGER NOT NULL DEFAULT 0
        """)

    cursor.execute("""
        UPDATE bank_accounts
        SET locked_balance = 0
        WHERE locked_balance IS NULL OR locked_balance < 0
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tax_debts (
            user_id TEXT PRIMARY KEY,
            debt_amount INTEGER NOT NULL DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_tax_debts_amount
        ON tax_debts(debt_amount)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_taxes_timestamp
        ON taxes(timestamp)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_taxes_user_timestamp
        ON taxes(user_id, timestamp)
    """)


def down(cursor: sqlite3.Cursor):
    """SQLite 不安全移除列，仅回滚新表和索引。"""
    cursor.execute("DROP INDEX IF EXISTS idx_taxes_user_timestamp")
    cursor.execute("DROP INDEX IF EXISTS idx_taxes_timestamp")
    cursor.execute("DROP INDEX IF EXISTS idx_tax_debts_amount")
    cursor.execute("DROP TABLE IF EXISTS tax_debts")
