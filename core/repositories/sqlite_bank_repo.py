import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from astrbot.api import logger

from ..domain.bank_models import BankAccount, BankFixedDeposit, BankWithdrawReservation
from ..domain.models import User


class SqliteBankRepository:
    """银行系统 SQLite 仓储。涉及钱包与银行余额的操作在单事务中完成。"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=30,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA synchronous = NORMAL;")
        return conn

    def _row_to_account(self, row: sqlite3.Row) -> Optional[BankAccount]:
        if not row:
            return None
        data = dict(row)
        data.setdefault("locked_balance", 0)
        return BankAccount(**data)

    def _available_balance(self, account: Optional[BankAccount]) -> int:
        if not account:
            return 0
        return max((account.balance or 0) - (account.locked_balance or 0), 0)

    def _row_to_user(self, row: sqlite3.Row) -> Optional[User]:
        if not row:
            return None
        allowed_keys = set(User.__dataclass_fields__.keys())
        data = {key: value for key, value in dict(row).items() if key in allowed_keys}
        for key in (
            "created_at",
            "last_login_time",
            "last_fishing_time",
            "last_wipe_bomb_time",
            "last_steal_time",
            "last_electric_fish_time",
            "last_stolen_at",
            "bait_start_time",
            "last_wof_play_time",
            "wof_last_action_time",
            "last_sicbo_time",
        ):
            if isinstance(data.get(key), str):
                try:
                    data[key] = datetime.fromisoformat(data[key])
                except ValueError:
                    pass
        return User(**data)

    def _row_to_reservation(self, row: sqlite3.Row) -> Optional[BankWithdrawReservation]:
        if not row:
            return None
        data = dict(row)
        for key in ("ready_at", "created_at", "updated_at"):
            if isinstance(data.get(key), str):
                try:
                    data[key] = datetime.fromisoformat(data[key])
                except ValueError:
                    pass
        return BankWithdrawReservation(**data)

    def _row_to_fixed_deposit(self, row: sqlite3.Row) -> Optional[BankFixedDeposit]:
        if not row:
            return None
        allowed_keys = {
            "deposit_id",
            "user_id",
            "principal",
            "term_days",
            "interest_rate",
            "expected_interest",
            "status",
            "started_at",
            "matures_at",
            "completed_at",
            "created_at",
            "updated_at",
        }
        data = {key: value for key, value in dict(row).items() if key in allowed_keys}
        for key in ("started_at", "matures_at", "completed_at", "created_at", "updated_at"):
            if isinstance(data.get(key), str):
                try:
                    data[key] = datetime.fromisoformat(data[key])
                except ValueError:
                    pass
        return BankFixedDeposit(**data)

    def ensure_account(self, user_id: str) -> BankAccount:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._ensure_account(cursor, user_id)
            conn.commit()
            return self._get_account_with_cursor(cursor, user_id)

    def get_account(self, user_id: str) -> Optional[BankAccount]:
        with self._connect() as conn:
            cursor = conn.cursor()
            return self._get_account_with_cursor(cursor, user_id)

    def _ensure_account(self, cursor: sqlite3.Cursor, user_id: str) -> None:
        now = datetime.now()
        cursor.execute("""
            INSERT OR IGNORE INTO bank_accounts
                (user_id, balance, locked_balance, today_withdrawn, last_withdraw_reset_date, created_at, updated_at)
            VALUES (?, 0, 0, 0, NULL, ?, ?)
        """, (user_id, now, now))

    def _collect_tax_debt_from_amount(
        self, cursor: sqlite3.Cursor, user_id: str, amount: int
    ) -> Tuple[int, int, int]:
        amount = max(amount, 0)
        cursor.execute("SELECT debt_amount FROM tax_debts WHERE user_id = ?", (user_id,))
        row = cursor.fetchone()
        debt_amount = row["debt_amount"] if row else 0
        paid_amount = min(amount, max(debt_amount, 0))
        if paid_amount <= 0:
            return amount, 0, debt_amount

        debt_after = debt_amount - paid_amount
        now = datetime.now()
        if debt_after > 0:
            cursor.execute("""
                UPDATE tax_debts
                SET debt_amount = ?, updated_at = ?
                WHERE user_id = ?
            """, (debt_after, now, user_id))
        else:
            cursor.execute("DELETE FROM tax_debts WHERE user_id = ?", (user_id,))
        return amount - paid_amount, paid_amount, debt_after

    def add_tax_debt(self, user_id: str, debt_amount: int) -> int:
        debt_amount = max(int(debt_amount), 0)
        if debt_amount <= 0:
            return 0
        with self._connect() as conn:
            cursor = conn.cursor()
            now = datetime.now()
            cursor.execute("""
                INSERT INTO tax_debts (user_id, debt_amount, created_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    debt_amount = tax_debts.debt_amount + excluded.debt_amount,
                    updated_at = excluded.updated_at
            """, (user_id, debt_amount, now, now))
            conn.commit()
            cursor.execute("SELECT debt_amount FROM tax_debts WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return row["debt_amount"] if row else 0

    def _get_account_with_cursor(self, cursor: sqlite3.Cursor, user_id: str) -> Optional[BankAccount]:
        cursor.execute("SELECT * FROM bank_accounts WHERE user_id = ?", (user_id,))
        return self._row_to_account(cursor.fetchone())

    def _get_pending_reservation_with_cursor(
        self, cursor: sqlite3.Cursor, user_id: str
    ) -> Optional[BankWithdrawReservation]:
        cursor.execute("""
            SELECT * FROM bank_withdraw_reservations
            WHERE user_id = ? AND status = 'pending'
            ORDER BY created_at DESC, reservation_id DESC
            LIMIT 1
        """, (user_id,))
        return self._row_to_reservation(cursor.fetchone())

    def get_pending_reservation(self, user_id: str) -> Optional[BankWithdrawReservation]:
        with self._connect() as conn:
            cursor = conn.cursor()
            return self._get_pending_reservation_with_cursor(cursor, user_id)

    def reset_daily_withdrawal_if_needed(
        self, user_id: str, reset_date: str
    ) -> BankAccount:
        with self._connect() as conn:
            cursor = conn.cursor()
            self._ensure_account(cursor, user_id)
            cursor.execute("""
                UPDATE bank_accounts
                SET today_withdrawn = 0,
                    last_withdraw_reset_date = ?,
                    updated_at = ?
                WHERE user_id = ?
                  AND (last_withdraw_reset_date IS NULL OR last_withdraw_reset_date != ?)
            """, (reset_date, datetime.now(), user_id, reset_date))
            conn.commit()
            return self._get_account_with_cursor(cursor, user_id)

    def deposit(self, user_id: str, amount: int) -> Tuple[bool, str, Optional[BankAccount], int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if not row:
                    conn.rollback()
                    return False, "用户不存在，请先注册", None, 0
                if row["coins"] < amount:
                    conn.rollback()
                    return False, "钱包余额不足", None, row["coins"]

                self._ensure_account(cursor, user_id)
                cursor.execute("UPDATE users SET coins = coins - ? WHERE user_id = ?", (amount, user_id))
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance + ?, updated_at = ?
                    WHERE user_id = ?
                """, (amount, datetime.now(), user_id))
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                wallet_after = cursor.fetchone()["coins"]
                account = self._get_account_with_cursor(cursor, user_id)
                conn.commit()
                return True, "ok", account, wallet_after
            except Exception as e:
                conn.rollback()
                logger.error(f"银行存款失败: {e}")
                raise

    def withdraw(
        self,
        user_id: str,
        amount: int,
        fee_amount: int,
        reset_date: str,
    ) -> Tuple[bool, str, Optional[BankAccount], int, int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if not row:
                    conn.rollback()
                    return False, "用户不存在，请先注册", None, 0, 0

                self._ensure_account(cursor, user_id)
                self._reset_daily_withdrawal_with_cursor(cursor, user_id, reset_date)
                account = self._get_account_with_cursor(cursor, user_id)
                if self._available_balance(account) < amount:
                    conn.rollback()
                    return False, "银行可用余额不足", account, row["coins"], 0

                net_amount = amount - fee_amount
                if net_amount < 0:
                    conn.rollback()
                    return False, "手续费不能超过取款金额", account, row["coins"], 0
                net_amount, debt_paid, _ = self._collect_tax_debt_from_amount(cursor, user_id, net_amount)

                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance - ?,
                        today_withdrawn = today_withdrawn + ?,
                        updated_at = ?
                    WHERE user_id = ?
                """, (amount, amount, datetime.now(), user_id))
                cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (net_amount, user_id))
                cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (user_id,))
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                wallet_after = cursor.fetchone()["coins"]
                account = self._get_account_with_cursor(cursor, user_id)
                conn.commit()
                return True, "ok", account, wallet_after, debt_paid
            except Exception as e:
                conn.rollback()
                logger.error(f"银行取款失败: {e}")
                raise

    def create_reservation(
        self,
        user_id: str,
        amount: int,
        fee_amount: int,
        ready_at: datetime,
        max_pending: int,
    ) -> Tuple[bool, str, Optional[BankWithdrawReservation]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
                if not cursor.fetchone():
                    conn.rollback()
                    return False, "用户不存在，请先注册", None
                self._ensure_account(cursor, user_id)
                account = self._get_account_with_cursor(cursor, user_id)
                if self._available_balance(account) < amount:
                    conn.rollback()
                    return False, "银行可用余额不足", None

                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM bank_withdraw_reservations
                    WHERE user_id = ? AND status = 'pending'
                """, (user_id,))
                if cursor.fetchone()["cnt"] >= max_pending:
                    conn.rollback()
                    return False, "已有待确认的大额取款预约", None

                now = datetime.now()
                cursor.execute("""
                    UPDATE bank_accounts
                    SET locked_balance = locked_balance + ?, updated_at = ?
                    WHERE user_id = ?
                """, (amount, now, user_id))
                cursor.execute("""
                    INSERT INTO bank_withdraw_reservations
                        (user_id, amount, fee_amount, status, ready_at, created_at, updated_at)
                    VALUES (?, ?, ?, 'pending', ?, ?, ?)
                """, (user_id, amount, fee_amount, ready_at, now, now))
                reservation_id = cursor.lastrowid
                cursor.execute(
                    "SELECT * FROM bank_withdraw_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                )
                reservation = self._row_to_reservation(cursor.fetchone())
                conn.commit()
                return True, "ok", reservation
            except Exception as e:
                conn.rollback()
                logger.error(f"创建银行取款预约失败: {e}")
                raise

    def complete_pending_reservation(
        self,
        user_id: str,
        reset_date: str,
    ) -> Tuple[bool, str, Optional[BankWithdrawReservation], Optional[BankAccount], int, int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                user_row = cursor.fetchone()
                if not user_row:
                    conn.rollback()
                    return False, "用户不存在，请先注册", None, None, 0, 0

                reservation = self._get_pending_reservation_with_cursor(cursor, user_id)
                if not reservation:
                    conn.rollback()
                    return False, "没有待确认的大额取款预约", None, None, user_row["coins"], 0

                now = datetime.now()
                if reservation.ready_at > now:
                    conn.rollback()
                    return False, "预约尚未到可取时间", reservation, None, user_row["coins"], 0

                self._reset_daily_withdrawal_with_cursor(cursor, user_id, reset_date)
                account = self._get_account_with_cursor(cursor, user_id)
                if not account or account.balance < reservation.amount:
                    conn.rollback()
                    return False, "银行余额不足，无法完成预约取款", reservation, account, user_row["coins"], 0

                net_amount = reservation.amount - reservation.fee_amount
                net_amount, debt_paid, _ = self._collect_tax_debt_from_amount(cursor, user_id, net_amount)
                locked_deduction = min(account.locked_balance or 0, reservation.amount)
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance - ?,
                        locked_balance = MAX(locked_balance - ?, 0),
                        today_withdrawn = today_withdrawn + ?,
                        updated_at = ?
                    WHERE user_id = ?
                """, (reservation.amount, locked_deduction, reservation.amount, now, user_id))
                cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (net_amount, user_id))
                cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (user_id,))
                cursor.execute("""
                    UPDATE bank_withdraw_reservations
                    SET status = 'completed', updated_at = ?
                    WHERE reservation_id = ?
                """, (now, reservation.reservation_id))
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                wallet_after = cursor.fetchone()["coins"]
                account = self._get_account_with_cursor(cursor, user_id)
                cursor.execute(
                    "SELECT * FROM bank_withdraw_reservations WHERE reservation_id = ?",
                    (reservation.reservation_id,),
                )
                reservation = self._row_to_reservation(cursor.fetchone())
                conn.commit()
                return True, "ok", reservation, account, wallet_after, debt_paid
            except Exception as e:
                conn.rollback()
                logger.error(f"确认银行取款预约失败: {e}")
                raise

    def cancel_pending_reservation(self, user_id: str) -> Tuple[bool, str, Optional[BankWithdrawReservation]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
                if not cursor.fetchone():
                    conn.rollback()
                    return False, "用户不存在，请先注册", None
                reservation = self._get_pending_reservation_with_cursor(cursor, user_id)
                if not reservation:
                    conn.rollback()
                    return False, "没有待取消的大额取款预约", None
                now = datetime.now()
                cursor.execute("""
                    UPDATE bank_accounts
                    SET locked_balance = MAX(locked_balance - ?, 0), updated_at = ?
                    WHERE user_id = ?
                """, (reservation.amount, now, user_id))
                cursor.execute("""
                    UPDATE bank_withdraw_reservations
                    SET status = 'cancelled', updated_at = ?
                    WHERE reservation_id = ?
                """, (now, reservation.reservation_id))
                cursor.execute(
                    "SELECT * FROM bank_withdraw_reservations WHERE reservation_id = ?",
                    (reservation.reservation_id,),
                )
                reservation = self._row_to_reservation(cursor.fetchone())
                conn.commit()
                return True, "ok", reservation
            except Exception as e:
                conn.rollback()
                logger.error(f"取消银行取款预约失败: {e}")
                raise

    def get_fixed_deposits(self, user_id: str, limit: int = 10) -> List[BankFixedDeposit]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM bank_fixed_deposits
                WHERE user_id = ?
                ORDER BY
                    CASE status WHEN 'active' THEN 0 WHEN 'completed' THEN 1 ELSE 2 END,
                    matures_at ASC,
                    deposit_id DESC
                LIMIT ?
            """, (user_id, limit))
            return [self._row_to_fixed_deposit(row) for row in cursor.fetchall()]

    def get_active_fixed_deposit_count(self, user_id: str) -> int:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) AS cnt FROM bank_fixed_deposits
                WHERE user_id = ? AND status = 'active'
            """, (user_id,))
            return cursor.fetchone()["cnt"]

    def get_admin_summary_for_users(self, user_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        if not user_ids:
            return {}

        summaries = {
            user_id: {
                "account_balance": 0,
                "locked_balance": 0,
                "available_balance": 0,
                "today_withdrawn": 0,
                "pending_reservation_amount": 0,
                "active_fixed_count": 0,
                "active_fixed_principal": 0,
                "active_expected_interest": 0,
                "next_maturity": None,
                "completed_fixed_count": 0,
                "cancelled_fixed_count": 0,
                "total_fixed_count": 0,
            }
            for user_id in user_ids
        }
        placeholders = ",".join(["?"] * len(user_ids))

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT user_id, balance, locked_balance, today_withdrawn
                FROM bank_accounts
                WHERE user_id IN ({placeholders})
            """, user_ids)
            for row in cursor.fetchall():
                summary = summaries[row["user_id"]]
                summary["account_balance"] = row["balance"] or 0
                summary["locked_balance"] = row["locked_balance"] or 0
                summary["available_balance"] = max(summary["account_balance"] - summary["locked_balance"], 0)
                summary["today_withdrawn"] = row["today_withdrawn"] or 0

            cursor.execute(f"""
                SELECT user_id, COALESCE(SUM(amount), 0) AS pending_amount
                FROM bank_withdraw_reservations
                WHERE user_id IN ({placeholders}) AND status = 'pending'
                GROUP BY user_id
            """, user_ids)
            for row in cursor.fetchall():
                summaries[row["user_id"]]["pending_reservation_amount"] = row["pending_amount"] or 0

            cursor.execute(f"""
                SELECT
                    user_id,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_fixed_count,
                    SUM(CASE WHEN status = 'active' THEN principal ELSE 0 END) AS active_fixed_principal,
                    SUM(CASE WHEN status = 'active' THEN expected_interest ELSE 0 END) AS active_expected_interest,
                    MIN(CASE WHEN status = 'active' THEN matures_at ELSE NULL END) AS next_maturity,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_fixed_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_fixed_count,
                    COUNT(*) AS total_fixed_count
                FROM bank_fixed_deposits
                WHERE user_id IN ({placeholders})
                GROUP BY user_id
            """, user_ids)
            for row in cursor.fetchall():
                summary = summaries[row["user_id"]]
                for key in (
                    "active_fixed_count",
                    "active_fixed_principal",
                    "active_expected_interest",
                    "completed_fixed_count",
                    "cancelled_fixed_count",
                    "total_fixed_count",
                ):
                    summary[key] = row[key] or 0
                summary["next_maturity"] = row["next_maturity"]

        return summaries

    def get_admin_totals(self) -> Dict[str, int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    COALESCE(SUM(balance), 0) AS total_account_balance,
                    COALESCE(SUM(locked_balance), 0) AS total_locked_balance,
                    COUNT(*) AS account_count
                FROM bank_accounts
            """)
            account_row = cursor.fetchone()
            cursor.execute("""
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'active' THEN principal ELSE 0 END), 0) AS active_fixed_principal,
                    COALESCE(SUM(CASE WHEN status = 'active' THEN expected_interest ELSE 0 END), 0) AS active_expected_interest,
                    SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_fixed_count,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_fixed_count,
                    SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_fixed_count,
                    COUNT(*) AS total_fixed_count
                FROM bank_fixed_deposits
            """)
            fixed_row = cursor.fetchone()
            return {
                "total_account_balance": account_row["total_account_balance"] or 0,
                "total_locked_balance": account_row["total_locked_balance"] or 0,
                "total_available_balance": max(
                    (account_row["total_account_balance"] or 0) - (account_row["total_locked_balance"] or 0),
                    0,
                ),
                "account_count": account_row["account_count"] or 0,
                "active_fixed_principal": fixed_row["active_fixed_principal"] or 0,
                "active_expected_interest": fixed_row["active_expected_interest"] or 0,
                "active_fixed_count": fixed_row["active_fixed_count"] or 0,
                "completed_fixed_count": fixed_row["completed_fixed_count"] or 0,
                "cancelled_fixed_count": fixed_row["cancelled_fixed_count"] or 0,
                "total_fixed_count": fixed_row["total_fixed_count"] or 0,
            }

    def get_daily_tax_subjects(self, threshold: int, asset_scope: str) -> List[Dict[str, Any]]:
        include_fixed = asset_scope == "wallet_bank_fixed"
        fixed_expr = "COALESCE(fd.active_fixed_principal, 0)" if include_fixed else "0"
        assessed_expr = f"(COALESCE(u.coins, 0) + COALESCE(a.balance, 0) + {fixed_expr})"

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT
                    u.*,
                    COALESCE(a.balance, 0) AS bank_balance,
                    COALESCE(fd.active_fixed_principal, 0) AS active_fixed_principal,
                    {assessed_expr} AS assessed_assets
                FROM users u
                LEFT JOIN bank_accounts a ON a.user_id = u.user_id
                LEFT JOIN (
                    SELECT user_id, SUM(principal) AS active_fixed_principal
                    FROM bank_fixed_deposits
                    WHERE status = 'active'
                    GROUP BY user_id
                ) fd ON fd.user_id = u.user_id
                WHERE {assessed_expr} >= ?
            """, (threshold,))
            subjects = []
            for row in cursor.fetchall():
                subjects.append({
                    "user": self._row_to_user(row),
                    "wallet_balance": row["coins"] or 0,
                    "bank_balance": row["bank_balance"] or 0,
                    "active_fixed_principal": row["active_fixed_principal"] or 0,
                    "assessed_assets": row["assessed_assets"] or 0,
                })
            return subjects

    def collect_daily_tax(self, user_id: str, tax_amount: int, deduct_scope: str = "wallet") -> Tuple[int, int, int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                self._ensure_account(cursor, user_id)
                cursor.execute("""
                    SELECT u.coins, a.balance, a.locked_balance
                    FROM users u
                    LEFT JOIN bank_accounts a ON a.user_id = u.user_id
                    WHERE u.user_id = ?
                """, (user_id,))
                row = cursor.fetchone()
                if not row:
                    conn.rollback()
                    return 0, 0, 0

                wallet_balance = row["coins"] or 0
                bank_balance = row["balance"] or 0
                locked_balance = row["locked_balance"] or 0
                bank_available = max(bank_balance - locked_balance, 0)
                requested_tax = max(tax_amount, 0)
                if deduct_scope == "bank":
                    actual_tax = min(requested_tax, bank_available)
                    wallet_deduction = 0
                    bank_deduction = actual_tax
                elif deduct_scope == "wallet_bank":
                    actual_tax = min(requested_tax, wallet_balance + bank_available)
                    wallet_deduction = min(wallet_balance, actual_tax)
                    bank_deduction = actual_tax - wallet_deduction
                else:
                    actual_tax = min(requested_tax, wallet_balance)
                    wallet_deduction = actual_tax
                    bank_deduction = 0
                debt_added = requested_tax - actual_tax

                now = datetime.now()
                if wallet_deduction > 0:
                    cursor.execute(
                        "UPDATE users SET coins = coins - ? WHERE user_id = ?",
                        (wallet_deduction, user_id),
                    )
                if bank_deduction > 0:
                    cursor.execute("""
                        UPDATE bank_accounts
                        SET balance = balance - ?, updated_at = ?
                        WHERE user_id = ?
                    """, (bank_deduction, now, user_id))
                if debt_added > 0:
                    cursor.execute("""
                        INSERT INTO tax_debts (user_id, debt_amount, created_at, updated_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(user_id) DO UPDATE SET
                            debt_amount = tax_debts.debt_amount + excluded.debt_amount,
                            updated_at = excluded.updated_at
                    """, (user_id, debt_added, now, now))

                balance_after = wallet_balance + bank_available - actual_tax
                conn.commit()
                return actual_tax, balance_after, debt_added
            except Exception as e:
                conn.rollback()
                logger.error(f"每日资产税扣款失败: {e}")
                raise

    def get_fixed_deposits_for_admin(self, search: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        params: List[Any] = []
        where = ""
        if search:
            where = "WHERE d.user_id LIKE ? OR COALESCE(u.nickname, '') LIKE ?"
            keyword = f"%{search}%"
            params.extend([keyword, keyword])
        params.append(limit)

        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT
                    d.*,
                    COALESCE(u.nickname, '') AS nickname,
                    COALESCE(u.coins, 0) AS wallet_balance,
                    COALESCE(a.balance, 0) AS account_balance,
                    COALESCE(a.locked_balance, 0) AS locked_balance
                FROM bank_fixed_deposits d
                LEFT JOIN users u ON u.user_id = d.user_id
                LEFT JOIN bank_accounts a ON a.user_id = d.user_id
                {where}
                ORDER BY
                    CASE d.status WHEN 'active' THEN 0 WHEN 'completed' THEN 1 ELSE 2 END,
                    d.matures_at ASC,
                    d.deposit_id DESC
                LIMIT ?
            """, params)
            deposits = []
            for row in cursor.fetchall():
                data = dict(row)
                deposit = self._row_to_fixed_deposit(row)
                deposits.append({
                    "deposit": deposit,
                    "nickname": data.get("nickname"),
                    "wallet_balance": data.get("wallet_balance", 0),
                    "account_balance": data.get("account_balance", 0),
                    "locked_balance": data.get("locked_balance", 0),
                })
            return deposits

    def create_fixed_deposit(
        self,
        user_id: str,
        principal: int,
        term_days: int,
        interest_rate: float,
        expected_interest: int,
        matures_at: datetime,
        max_active: int,
    ) -> Tuple[bool, str, Optional[BankFixedDeposit], Optional[BankAccount]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                self._ensure_account(cursor, user_id)
                account = self._get_account_with_cursor(cursor, user_id)
                if self._available_balance(account) < principal:
                    conn.rollback()
                    return False, "银行活期可用余额不足", None, account

                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM bank_fixed_deposits
                    WHERE user_id = ? AND status = 'active'
                """, (user_id,))
                if cursor.fetchone()["cnt"] >= max_active:
                    conn.rollback()
                    return False, "进行中的定期存款数量已达上限", None, account

                now = datetime.now()
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance - ?, updated_at = ?
                    WHERE user_id = ?
                """, (principal, now, user_id))
                cursor.execute("""
                    INSERT INTO bank_fixed_deposits (
                        user_id, principal, term_days, interest_rate, expected_interest,
                        status, started_at, matures_at, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, ?)
                """, (
                    user_id, principal, term_days, interest_rate, expected_interest,
                    now, matures_at, now, now,
                ))
                deposit_id = cursor.lastrowid
                cursor.execute("SELECT * FROM bank_fixed_deposits WHERE deposit_id = ?", (deposit_id,))
                deposit = self._row_to_fixed_deposit(cursor.fetchone())
                account = self._get_account_with_cursor(cursor, user_id)
                conn.commit()
                return True, "ok", deposit, account
            except Exception as e:
                conn.rollback()
                logger.error(f"创建银行定期存款失败: {e}")
                raise

    def complete_fixed_deposit(
        self, user_id: str, deposit_id: int
    ) -> Tuple[bool, str, Optional[BankFixedDeposit], Optional[BankAccount], int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                self._ensure_account(cursor, user_id)
                cursor.execute("""
                    SELECT * FROM bank_fixed_deposits
                    WHERE deposit_id = ? AND user_id = ? AND status = 'active'
                """, (deposit_id, user_id))
                deposit = self._row_to_fixed_deposit(cursor.fetchone())
                if not deposit:
                    conn.rollback()
                    return False, "未找到可领取的定期存款", None, None, 0

                now = datetime.now()
                if deposit.matures_at > now:
                    conn.rollback()
                    return False, "定期存款尚未到期", deposit, None, 0

                payout = deposit.principal + deposit.expected_interest
                payout, debt_paid, _ = self._collect_tax_debt_from_amount(cursor, user_id, payout)
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance + ?, updated_at = ?
                    WHERE user_id = ?
                """, (payout, now, user_id))
                cursor.execute("""
                    UPDATE bank_fixed_deposits
                    SET status = 'completed', completed_at = ?, updated_at = ?
                    WHERE deposit_id = ?
                """, (now, now, deposit_id))
                cursor.execute("SELECT * FROM bank_fixed_deposits WHERE deposit_id = ?", (deposit_id,))
                deposit = self._row_to_fixed_deposit(cursor.fetchone())
                account = self._get_account_with_cursor(cursor, user_id)
                conn.commit()
                return True, "ok", deposit, account, debt_paid
            except Exception as e:
                conn.rollback()
                logger.error(f"领取银行定期存款失败: {e}")
                raise

    def cancel_fixed_deposit(
        self, user_id: str, deposit_id: int, penalty_amount: int
    ) -> Tuple[bool, str, Optional[BankFixedDeposit], Optional[BankAccount], int, int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                self._ensure_account(cursor, user_id)
                cursor.execute("""
                    SELECT * FROM bank_fixed_deposits
                    WHERE deposit_id = ? AND user_id = ? AND status = 'active'
                """, (deposit_id, user_id))
                deposit = self._row_to_fixed_deposit(cursor.fetchone())
                if not deposit:
                    conn.rollback()
                    return False, "未找到可提前取出的定期存款", None, None, 0, 0

                penalty_amount = max(0, min(penalty_amount, deposit.principal))
                payout = deposit.principal - penalty_amount
                payout, debt_paid, _ = self._collect_tax_debt_from_amount(cursor, user_id, payout)
                now = datetime.now()
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance + ?, updated_at = ?
                    WHERE user_id = ?
                """, (payout, now, user_id))
                cursor.execute("""
                    UPDATE bank_fixed_deposits
                    SET status = 'cancelled', completed_at = ?, updated_at = ?
                    WHERE deposit_id = ?
                """, (now, now, deposit_id))
                cursor.execute("SELECT * FROM bank_fixed_deposits WHERE deposit_id = ?", (deposit_id,))
                deposit = self._row_to_fixed_deposit(cursor.fetchone())
                account = self._get_account_with_cursor(cursor, user_id)
                conn.commit()
                return True, "ok", deposit, account, penalty_amount, debt_paid
            except Exception as e:
                conn.rollback()
                logger.error(f"提前取出银行定期存款失败: {e}")
                raise

    def _reset_daily_withdrawal_with_cursor(
        self, cursor: sqlite3.Cursor, user_id: str, reset_date: str
    ) -> None:
        cursor.execute("""
            UPDATE bank_accounts
            SET today_withdrawn = 0,
                last_withdraw_reset_date = ?,
                updated_at = ?
            WHERE user_id = ?
              AND (last_withdraw_reset_date IS NULL OR last_withdraw_reset_date != ?)
        """, (reset_date, datetime.now(), user_id, reset_date))

    def get_tax_debt_summary(self, user_id: Optional[str] = None) -> Dict[str, int]:
        conditions = ["debt_amount > 0"]
        params: List[Any] = []
        if user_id:
            conditions.append("user_id = ?")
            params.append(user_id)
        where_sql = "WHERE " + " AND ".join(conditions)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT
                    COALESCE(SUM(debt_amount), 0) AS total_debt,
                    COUNT(*) AS debt_user_count
                FROM tax_debts
                {where_sql}
            """, params)
            row = cursor.fetchone()
            return {
                "total_debt": row["total_debt"] or 0,
                "debt_user_count": row["debt_user_count"] or 0,
            }

    def get_tax_debts_for_admin(self, user_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        conditions = ["d.debt_amount > 0"]
        params: List[Any] = []
        if user_id:
            conditions.append("d.user_id = ?")
            params.append(user_id)
        where_sql = "WHERE " + " AND ".join(conditions)
        params.append(limit)
        with self._connect() as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
                SELECT
                    d.*,
                    COALESCE(u.nickname, '') AS nickname,
                    COALESCE(u.coins, 0) AS wallet_balance,
                    COALESCE(a.balance, 0) AS account_balance,
                    COALESCE(a.locked_balance, 0) AS locked_balance
                FROM tax_debts d
                LEFT JOIN users u ON u.user_id = d.user_id
                LEFT JOIN bank_accounts a ON a.user_id = d.user_id
                {where_sql}
                ORDER BY d.debt_amount DESC, d.updated_at DESC
                LIMIT ?
            """, params)
            return [dict(row) for row in cursor.fetchall()]
