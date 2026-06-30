import sqlite3
from datetime import datetime
from typing import Optional, Tuple

from astrbot.api import logger

from ..domain.bank_models import BankAccount, BankWithdrawReservation


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
        return BankAccount(**dict(row))

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
                (user_id, balance, today_withdrawn, last_withdraw_reset_date, created_at, updated_at)
            VALUES (?, 0, 0, NULL, ?, ?)
        """, (user_id, now, now))

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
    ) -> Tuple[bool, str, Optional[BankAccount], int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                row = cursor.fetchone()
                if not row:
                    conn.rollback()
                    return False, "用户不存在，请先注册", None, 0

                self._ensure_account(cursor, user_id)
                self._reset_daily_withdrawal_with_cursor(cursor, user_id, reset_date)
                account = self._get_account_with_cursor(cursor, user_id)
                if not account or account.balance < amount:
                    conn.rollback()
                    return False, "银行余额不足", account, row["coins"]

                net_amount = amount - fee_amount
                if net_amount < 0:
                    conn.rollback()
                    return False, "手续费不能超过取款金额", account, row["coins"]

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
                return True, "ok", account, wallet_after
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
                self._ensure_account(cursor, user_id)
                account = self._get_account_with_cursor(cursor, user_id)
                if not account or account.balance < amount:
                    conn.rollback()
                    return False, "银行余额不足", None

                cursor.execute("""
                    SELECT COUNT(*) AS cnt FROM bank_withdraw_reservations
                    WHERE user_id = ? AND status = 'pending'
                """, (user_id,))
                if cursor.fetchone()["cnt"] >= max_pending:
                    conn.rollback()
                    return False, "已有待确认的大额取款预约", None

                now = datetime.now()
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
    ) -> Tuple[bool, str, Optional[BankWithdrawReservation], Optional[BankAccount], int]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
                user_row = cursor.fetchone()
                if not user_row:
                    conn.rollback()
                    return False, "用户不存在，请先注册", None, None, 0

                reservation = self._get_pending_reservation_with_cursor(cursor, user_id)
                if not reservation:
                    conn.rollback()
                    return False, "没有待确认的大额取款预约", None, None, user_row["coins"]

                now = datetime.now()
                if reservation.ready_at > now:
                    conn.rollback()
                    return False, "预约尚未到可取时间", reservation, None, user_row["coins"]

                self._reset_daily_withdrawal_with_cursor(cursor, user_id, reset_date)
                account = self._get_account_with_cursor(cursor, user_id)
                if not account or account.balance < reservation.amount:
                    conn.rollback()
                    return False, "银行余额不足，无法完成预约取款", reservation, account, user_row["coins"]

                net_amount = reservation.amount - reservation.fee_amount
                cursor.execute("""
                    UPDATE bank_accounts
                    SET balance = balance - ?,
                        today_withdrawn = today_withdrawn + ?,
                        updated_at = ?
                    WHERE user_id = ?
                """, (reservation.amount, reservation.amount, now, user_id))
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
                return True, "ok", reservation, account, wallet_after
            except Exception as e:
                conn.rollback()
                logger.error(f"确认银行取款预约失败: {e}")
                raise

    def cancel_pending_reservation(self, user_id: str) -> Tuple[bool, str, Optional[BankWithdrawReservation]]:
        with self._connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE")
                reservation = self._get_pending_reservation_with_cursor(cursor, user_id)
                if not reservation:
                    conn.rollback()
                    return False, "没有待取消的大额取款预约", None
                cursor.execute("""
                    UPDATE bank_withdraw_reservations
                    SET status = 'cancelled', updated_at = ?
                    WHERE reservation_id = ?
                """, (datetime.now(), reservation.reservation_id))
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
