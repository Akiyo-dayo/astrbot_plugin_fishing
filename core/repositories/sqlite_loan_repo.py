"""
借贷系统数据仓储层
"""

import sqlite3
import threading
from datetime import datetime
from typing import Optional, List

from astrbot.api import logger

from ..domain.loan_models import Loan


class SqliteLoanRepository:
    """借贷数据仓储的SQLite实现"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._local = threading.local()

    def _get_connection(self) -> sqlite3.Connection:
        conn = getattr(self._local, "connection", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA foreign_keys = ON;")
            self._local.connection = conn
        return conn

    def _row_to_loan(self, row: sqlite3.Row) -> Optional[Loan]:
        """将数据库行转换为Loan对象"""
        if not row:
            return None
        
        row_keys = row.keys()
        
        return Loan(
            loan_id=row["loan_id"],
            lender_id=row["lender_id"],
            borrower_id=row["borrower_id"],
            principal=row["principal"],
            interest_rate=row["interest_rate"],
            borrowed_at=self._parse_datetime(row["borrowed_at"]),
            due_amount=row["due_amount"],
            repaid_amount=row["repaid_amount"],
            status=row["status"],
            due_date=self._parse_datetime(row["due_date"]) if "due_date" in row_keys and row["due_date"] else None,
            created_at=self._parse_datetime(row["created_at"]),
            updated_at=self._parse_datetime(row["updated_at"])
        )

    def _parse_datetime(self, dt_val):
        """解析日期时间"""
        if isinstance(dt_val, datetime):
            return dt_val
        if isinstance(dt_val, str):
            try:
                return datetime.fromisoformat(dt_val.replace("Z", "+00:00"))
            except ValueError:
                try:
                    return datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S.%f")
                except ValueError:
                    try:
                        return datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        return None
        return None

    def create_loan(self, loan: Loan) -> int:
        """创建借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        now = datetime.now()
        cursor.execute("""
            INSERT INTO loans (
                lender_id, borrower_id, principal, interest_rate,
                borrowed_at, due_amount, repaid_amount, status,
                due_date, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            loan.lender_id, loan.borrower_id, loan.principal, loan.interest_rate,
            loan.borrowed_at or now, loan.due_amount, loan.repaid_amount, loan.status,
            loan.due_date, now, now
        ))
        conn.commit()
        return cursor.lastrowid

    def get_loan_by_id(self, loan_id: int) -> Optional[Loan]:
        """根据ID获取借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
        row = cursor.fetchone()
        return self._row_to_loan(row)

    def get_active_loans_between_users(self, lender_id: str, borrower_id: str) -> List[Loan]:
        """获取两个用户之间的进行中借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM loans
            WHERE lender_id = ? AND borrower_id = ? AND status = 'active'
            ORDER BY borrowed_at DESC
        """, (lender_id, borrower_id))
        return [self._row_to_loan(row) for row in cursor.fetchall()]

    def get_loans_by_lender(self, lender_id: str, status: Optional[str] = None) -> List[Loan]:
        """获取某人作为放贷人的所有借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if status:
            cursor.execute("""
                SELECT * FROM loans
                WHERE lender_id = ? AND status = ?
                ORDER BY borrowed_at DESC
            """, (lender_id, status))
        else:
            cursor.execute("""
                SELECT * FROM loans
                WHERE lender_id = ?
                ORDER BY borrowed_at DESC
            """, (lender_id,))
        
        return [self._row_to_loan(row) for row in cursor.fetchall()]

    def get_loans_by_borrower(self, borrower_id: str, status: Optional[str] = None) -> List[Loan]:
        """获取某人作为借款人的所有借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        if status:
            cursor.execute("""
                SELECT * FROM loans
                WHERE borrower_id = ? AND status = ?
                ORDER BY borrowed_at DESC
            """, (borrower_id, status))
        else:
            cursor.execute("""
                SELECT * FROM loans
                WHERE borrower_id = ?
                ORDER BY borrowed_at DESC
            """, (borrower_id,))
        
        return [self._row_to_loan(row) for row in cursor.fetchall()]

    def update_loan_repayment(self, loan_id: int, repaid_amount: int, status: str) -> bool:
        """更新还款金额和状态"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE loans
            SET repaid_amount = ?, status = ?, updated_at = ?
            WHERE loan_id = ?
        """, (repaid_amount, status, datetime.now(), loan_id))
        
        conn.commit()
        return cursor.rowcount > 0

    def get_all_active_loans(self) -> List[Loan]:
        """获取所有进行中的借条"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM loans
            WHERE status = 'active'
            ORDER BY borrowed_at DESC
        """)
        return [self._row_to_loan(row) for row in cursor.fetchall()]
    
    def get_overdue_loans(self) -> List[Loan]:
        """获取所有逾期的系统借款"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM loans
            WHERE lender_id = 'SYSTEM' 
            AND status = 'active'
            AND due_date IS NOT NULL
            AND due_date < ?
        """, (datetime.now(),))
        return [self._row_to_loan(row) for row in cursor.fetchall()]
    
    def get_active_system_loan(self, borrower_id: str) -> Optional[Loan]:
        """获取用户当前进行中的系统借款（一次只能有一笔）"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM loans
            WHERE lender_id = 'SYSTEM' AND borrower_id = ? AND status = 'active'
            ORDER BY borrowed_at DESC
            LIMIT 1
        """, (borrower_id,))
        row = cursor.fetchone()
        return self._row_to_loan(row)
    
    def has_overdue_system_loan(self, borrower_id: str) -> bool:
        """检查用户是否有逾期的系统借款"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM loans
            WHERE lender_id = 'SYSTEM' 
            AND borrower_id = ?
            AND status IN ('active', 'overdue')
            AND due_date IS NOT NULL
            AND due_date < ?
        """, (borrower_id, datetime.now()))
        count = cursor.fetchone()[0]
        return count > 0
