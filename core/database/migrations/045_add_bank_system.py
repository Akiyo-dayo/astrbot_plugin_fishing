"""
迁移045：添加银行系统
"""

import sqlite3


def up(cursor: sqlite3.Cursor):
    """创建银行账户与大额取款预约表"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bank_accounts (
            user_id TEXT PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0,
            today_withdrawn INTEGER NOT NULL DEFAULT 0,
            last_withdraw_reset_date TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bank_withdraw_reservations (
            reservation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            amount INTEGER NOT NULL,
            fee_amount INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'completed', 'cancelled', 'expired')),
            ready_at DATETIME NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bank_reservations_user_status
        ON bank_withdraw_reservations(user_id, status)
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_bank_reservations_ready_at
        ON bank_withdraw_reservations(status, ready_at)
    """)


def down(cursor: sqlite3.Cursor):
    """回滚银行系统表"""
    cursor.execute("DROP INDEX IF EXISTS idx_bank_reservations_ready_at")
    cursor.execute("DROP INDEX IF EXISTS idx_bank_reservations_user_status")
    cursor.execute("DROP TABLE IF EXISTS bank_withdraw_reservations")
    cursor.execute("DROP TABLE IF EXISTS bank_accounts")
