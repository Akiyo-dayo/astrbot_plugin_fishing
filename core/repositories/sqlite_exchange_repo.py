import sqlite3
from datetime import datetime, timedelta
from typing import List, Optional

from ..database.connection_manager import DatabaseConnectionManager
from ..domain.models import Commodity, Exchange, UserCommodity
from .abstract_repository import AbstractExchangeRepository


class SqliteExchangeRepository(AbstractExchangeRepository):
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn_mgr = DatabaseConnectionManager(
            db_path, detect_types=0, row_factory=None
        )

    def close_connection(self) -> None:
        self._conn_mgr.close_connection()

    def get_all_commodities(self) -> List[Commodity]:
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT commodity_id, name, description FROM commodities")
            rows = c.fetchall()
            return [Commodity(*row) for row in rows]

    def get_commodity_by_id(self, commodity_id: str) -> Optional[Commodity]:
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT commodity_id, name, description FROM commodities WHERE commodity_id=?", (commodity_id,))
            row = c.fetchone()
            return Commodity(*row) if row else None

    def get_prices_for_date(self, date: str) -> List[Exchange]:
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("SELECT date, time, commodity_id, price, update_type, created_at FROM exchange_prices WHERE date=? ORDER BY time", (date,))
            rows = c.fetchall()
            return [Exchange(*row) for row in rows]

    def add_exchange_price(self, price: Exchange) -> None:
        def _op(cursor: sqlite3.Cursor) -> None:
            # 插入新的价格记录，支持每日多次更新
            cursor.execute("INSERT INTO exchange_prices (date, time, commodity_id, price, update_type, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                           (price.date, price.time, price.commodity_id, price.price, price.update_type, price.created_at))

        self._conn_mgr.run_in_transaction(_op)
    
    def delete_prices_for_date(self, date: str) -> None:
        """删除指定日期的所有价格"""
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute("DELETE FROM exchange_prices WHERE date=?", (date,))

        self._conn_mgr.run_in_transaction(_op)

    def get_user_commodities(self, user_id: str) -> List[UserCommodity]:
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT instance_id, user_id, commodity_id, quantity, purchase_price, purchased_at, expires_at
                FROM user_commodities WHERE user_id=?
            """, (user_id,))
            rows = c.fetchall()
        
        commodities = []
        for row in rows:
            try:
                # 安全解析日期
                purchased_at = datetime.fromisoformat(row[5]) if row[5] else datetime.now()
                expires_at = datetime.fromisoformat(row[6]) if row[6] else datetime.now() + timedelta(days=1)
                
                commodity = UserCommodity(
                    instance_id=row[0], user_id=row[1], commodity_id=row[2], quantity=row[3],
                    purchase_price=row[4], purchased_at=purchased_at, expires_at=expires_at
                )
                commodities.append(commodity)
            except Exception as e:
                from astrbot.api import logger
                logger.error(f"解析用户商品数据失败: {e}, 行数据: {row}")
                continue
                
        return commodities

    def add_user_commodity(self, user_commodity: UserCommodity) -> UserCommodity:
        def _op(cursor: sqlite3.Cursor) -> int:
            cursor.execute("""
                INSERT INTO user_commodities (user_id, commodity_id, quantity, purchase_price, purchased_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                user_commodity.user_id, user_commodity.commodity_id, user_commodity.quantity,
                user_commodity.purchase_price, user_commodity.purchased_at.isoformat(),
                user_commodity.expires_at.isoformat()
            ))
            return cursor.lastrowid

        user_commodity.instance_id = self._conn_mgr.run_in_transaction(_op)
        return user_commodity

    def update_user_commodity_quantity(self, instance_id: int, new_quantity: int) -> None:
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute("UPDATE user_commodities SET quantity=? WHERE instance_id=?", (new_quantity, instance_id))

        self._conn_mgr.run_in_transaction(_op)

    def delete_user_commodity(self, instance_id: int) -> None:
        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute("DELETE FROM user_commodities WHERE instance_id=?", (instance_id,))

        self._conn_mgr.run_in_transaction(_op)

    def get_user_commodity_by_instance_id(self, instance_id: int) -> Optional[UserCommodity]:
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT instance_id, user_id, commodity_id, quantity, purchase_price, purchased_at, expires_at
                FROM user_commodities WHERE instance_id=?
            """, (instance_id,))
            row = c.fetchone()
            if row:
                return UserCommodity(
                    instance_id=row[0], user_id=row[1], commodity_id=row[2], quantity=row[3],
                    purchase_price=row[4], purchased_at=datetime.fromisoformat(row[5]),
                    expires_at=datetime.fromisoformat(row[6])
                )
            return None

    def get_all_user_commodities(self) -> List[UserCommodity]:
        """获取所有用户的大宗商品持仓"""
        with self._conn_mgr.get_connection() as conn:
            c = conn.cursor()
            c.execute("""
                SELECT instance_id, user_id, commodity_id, quantity, purchase_price, purchased_at, expires_at
                FROM user_commodities
            """)
            rows = c.fetchall()
            return [
                UserCommodity(
                    instance_id=row[0], user_id=row[1], commodity_id=row[2], quantity=row[3],
                    purchase_price=row[4], purchased_at=datetime.fromisoformat(row[5]),
                    expires_at=datetime.fromisoformat(row[6])
                )
                for row in rows
            ]

    def clear_expired_commodities(self, user_id: str) -> int:
        """清理用户库存中的腐败商品，返回清理的数量"""
        now = datetime.now()

        def _op(cursor: sqlite3.Cursor) -> int:
            # 先查询要删除的商品数量
            cursor.execute("""
                SELECT COUNT(*) FROM user_commodities
                WHERE user_id = ? AND expires_at <= ?
            """, (user_id, now))
            count = cursor.fetchone()[0]

            # 删除腐败商品
            cursor.execute("""
                DELETE FROM user_commodities
                WHERE user_id = ? AND expires_at <= ?
            """, (user_id, now))
            return count

        return self._conn_mgr.run_in_transaction(_op)
