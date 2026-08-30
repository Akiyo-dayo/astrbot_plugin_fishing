"""
借贷系统数据仓储层
"""

import sqlite3
from datetime import datetime
from typing import Optional, List

from astrbot.api import logger

from ..database.connection_manager import DatabaseConnectionManager
from ..domain.loan_models import Loan


class SqliteLoanRepository:
    """借贷数据仓储的SQLite实现"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        # 不使用 detect_types：loans 表列类型为 TIMESTAMP，
        # Python 内置 convert_timestamp 解析器极度脆弱，
        # 任何非 "YYYY-MM-DD HH:MM:SS" 格式都会崩溃。
        # 改为在 _row_to_loan → _parse_datetime 中手动解析。
        self._conn_mgr = DatabaseConnectionManager(db_path, detect_types=0)

    def close_connection(self) -> None:
        """Close the connection owned by the current thread."""
        self._conn_mgr.close_connection()

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
        """安全解析日期时间，兼容所有常见格式"""
        if dt_val is None:
            return None
        if isinstance(dt_val, datetime):
            return dt_val
        if isinstance(dt_val, (str, bytes)):
            val = dt_val.decode('utf-8') if isinstance(dt_val, bytes) else dt_val
            val = val.strip()
            if not val:
                return None
            # 统一分隔符：ISO 8601 的 T → 空格
            val = val.replace('T', ' ')
            for fmt in (
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
            ):
                try:
                    return datetime.strptime(val, fmt)
                except ValueError:
                    continue
            # 最后尝试 fromisoformat（兼容带时区的格式）
            try:
                return datetime.fromisoformat(dt_val if isinstance(dt_val, str) else val)
            except (ValueError, TypeError):
                pass
            logger.warning(f"无法解析日期时间值: {dt_val!r}")
        return None

    def create_loan(self, loan: Loan) -> int:
        """创建借条"""
        now = datetime.now()

        def _op(cursor: sqlite3.Cursor) -> int:
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
            return cursor.lastrowid

        return self._conn_mgr.run_in_transaction(_op)

    def get_loan_by_id(self, loan_id: int) -> Optional[Loan]:
        """根据ID获取借条"""
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM loans WHERE loan_id = ?", (loan_id,))
            row = cursor.fetchone()
            return self._row_to_loan(row)

    def get_active_loans_between_users(self, lender_id: str, borrower_id: str) -> List[Loan]:
        """获取两个用户之间的进行中借条"""
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM loans
                WHERE lender_id = ? AND borrower_id = ? AND status = 'active'
                ORDER BY borrowed_at DESC
            """, (lender_id, borrower_id))
            return [self._row_to_loan(row) for row in cursor.fetchall()]

    def get_loans_by_lender(self, lender_id: str, status: Optional[str] = None) -> List[Loan]:
        """获取某人作为放贷人的所有借条"""
        with self._conn_mgr.get_connection() as conn:
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
        with self._conn_mgr.get_connection() as conn:
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
        updated_at = datetime.now()

        def _op(cursor: sqlite3.Cursor) -> bool:
            cursor.execute("""
                UPDATE loans
                SET repaid_amount = ?, status = ?, updated_at = ?
                WHERE loan_id = ?
            """, (repaid_amount, status, updated_at, loan_id))
            return cursor.rowcount > 0

        return self._conn_mgr.run_in_transaction(_op)

    def get_all_active_loans(self) -> List[Loan]:
        """获取所有进行中的借条"""
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM loans
                WHERE status = 'active'
                ORDER BY borrowed_at DESC
            """)
            return [self._row_to_loan(row) for row in cursor.fetchall()]
    
    def get_overdue_loans(self) -> List[Loan]:
        """获取所有逾期的系统借款"""
        with self._conn_mgr.get_connection() as conn:
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
        with self._conn_mgr.get_connection() as conn:
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
        with self._conn_mgr.get_connection() as conn:
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
