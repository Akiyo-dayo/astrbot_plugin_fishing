"""
迁移046：添加银行定期存款系统
"""

import sqlite3


def up(cursor: sqlite3.Cursor):
    """创建银行定期存款表"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bank_fixed_deposits (
            deposit_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            principal INTEGER NOT NULL,
            term_days INTEGER NOT NULL,
            interest_rate REAL NOT NULL,
            expected_interest INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'completed', 'cancelled')),
            started_at DATETIME NOT NULL,
            matures_at DATETIME NOT NULL,
            completed_at DATETIME,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bank_fixed_deposits_user_status
        ON bank_fixed_deposits(user_id, status)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bank_fixed_deposits_matures_at
        ON bank_fixed_deposits(status, matures_at)
    """)


def down(cursor: sqlite3.Cursor):
    """回滚银行定期存款表"""
    cursor.execute("DROP INDEX IF EXISTS idx_bank_fixed_deposits_matures_at")
    cursor.execute("DROP INDEX IF EXISTS idx_bank_fixed_deposits_user_status")
    cursor.execute("DROP TABLE IF EXISTS bank_fixed_deposits")
