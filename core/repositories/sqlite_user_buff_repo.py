import sqlite3
from typing import List, Optional
from datetime import datetime

from ..database.connection_manager import DatabaseConnectionManager
from ..domain.models import UserBuff
from .abstract_repository import AbstractUserBuffRepository
from ..utils import get_now

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


class SqliteUserBuffRepository(AbstractUserBuffRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn_mgr = DatabaseConnectionManager(
            db_path, detect_types=sqlite3.PARSE_DECLTYPES
        )

    def close_connection(self) -> None:
        self._conn_mgr.close_connection()

    def _to_domain(self, row: sqlite3.Row) -> UserBuff:
        # 处理 started_at 字段
        started_at = row['started_at']
        if isinstance(started_at, str):
            started_at = datetime.strptime(started_at, DATETIME_FORMAT)
        
        # 处理 expires_at 字段
        expires_at = row['expires_at']
        if expires_at is not None:
            if isinstance(expires_at, str):
                expires_at = datetime.strptime(expires_at, DATETIME_FORMAT)
        
        return UserBuff(
            id=row['id'],
            user_id=row['user_id'],
            buff_type=row['buff_type'],
            payload=row['payload'],
            started_at=started_at,
            expires_at=expires_at,
        )

    def add(self, buff: UserBuff):
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute(
                """
                INSERT INTO user_buffs (user_id, buff_type, payload, started_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    buff.user_id,
                    buff.buff_type,
                    buff.payload,
                    buff.started_at.strftime(DATETIME_FORMAT),
                    buff.expires_at.strftime(DATETIME_FORMAT) if buff.expires_at else None,
                ),
            )

        self._conn_mgr.run_in_transaction(_op)

    def get_active_by_user_and_type(
        self, user_id: str, buff_type: str
    ) -> Optional[UserBuff]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, user_id, buff_type, payload, started_at, expires_at
                FROM user_buffs
                WHERE user_id = ? AND buff_type = ? AND (expires_at IS NULL OR expires_at > ?)
                ORDER BY expires_at DESC
                LIMIT 1
                """,
                (user_id, buff_type, get_now().strftime(DATETIME_FORMAT)),
            )
            row = cursor.fetchone()
            return self._to_domain(row) if row else None

    def update(self, buff: UserBuff):
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute(
                """
                UPDATE user_buffs
                SET payload = ?, expires_at = ?
                WHERE id = ?
                """,
                (
                    buff.payload,
                    buff.expires_at.strftime(DATETIME_FORMAT) if buff.expires_at else None,
                    buff.id,
                ),
            )

        self._conn_mgr.run_in_transaction(_op)

    def get_all_active_by_user(self, user_id: str) -> List[UserBuff]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, user_id, buff_type, payload, started_at, expires_at
                FROM user_buffs
                WHERE user_id = ? AND (expires_at IS NULL OR expires_at > ?)
                """,
                (user_id, get_now().strftime(DATETIME_FORMAT)),
            )
            rows = cursor.fetchall()
            return [self._to_domain(row) for row in rows]

    def delete_expired(self):
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute(
                "DELETE FROM user_buffs WHERE expires_at IS NOT NULL AND expires_at <= ?",
                (get_now().strftime(DATETIME_FORMAT),),
            )

        self._conn_mgr.run_in_transaction(_op)

    def delete(self, buff_id: int):
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute("DELETE FROM user_buffs WHERE id = ?", (buff_id,))

        self._conn_mgr.run_in_transaction(_op)
