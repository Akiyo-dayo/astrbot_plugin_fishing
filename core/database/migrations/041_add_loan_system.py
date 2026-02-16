"""
迁移脚本：添加借贷系统
"""

import sqlite3


def up(cursor: sqlite3.Cursor):
    """应用迁移"""
    
    # 创建借条表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS loans (
            loan_id INTEGER PRIMARY KEY AUTOINCREMENT,
            lender_id TEXT NOT NULL,
            borrower_id TEXT NOT NULL,
            principal INTEGER NOT NULL,
            interest_rate REAL NOT NULL DEFAULT 0.05,
            borrowed_at TIMESTAMP NOT NULL,
            due_amount INTEGER NOT NULL,
            repaid_amount INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            due_date TIMESTAMP,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL,
            FOREIGN KEY (lender_id) REFERENCES users(user_id),
            FOREIGN KEY (borrower_id) REFERENCES users(user_id)
        )
    """)
    
    # 创建索引以提高查询性能
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_loans_lender 
        ON loans(lender_id, status)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_loans_borrower 
        ON loans(borrower_id, status)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_loans_status 
        ON loans(status)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_loans_overdue 
        ON loans(lender_id, status, due_date)
    """)


def down(cursor: sqlite3.Cursor):
    """回滚迁移"""
    cursor.execute("DROP INDEX IF EXISTS idx_loans_overdue")
    cursor.execute("DROP INDEX IF EXISTS idx_loans_status")
    cursor.execute("DROP INDEX IF EXISTS idx_loans_borrower")
    cursor.execute("DROP INDEX IF EXISTS idx_loans_lender")
    cursor.execute("DROP TABLE IF EXISTS loans")
