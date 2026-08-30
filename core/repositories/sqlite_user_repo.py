import dataclasses
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any

from astrbot.api import logger

from ..database.connection_manager import DatabaseConnectionManager
from ..domain.models import User, TaxRecord
from .abstract_repository import AbstractUserRepository

class SqliteUserRepository(AbstractUserRepository):
    """用户数据仓储的SQLite实现"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn_mgr = DatabaseConnectionManager(db_path)

    def _get_connection(self) -> sqlite3.Connection:
        """兼容仍需跨表事务的服务层，连接所有权统一归 manager。"""
        return self._conn_mgr._get_connection()

    def close_connection(self) -> None:
        """关闭当前线程的连接，退出后台任务时释放线程资源。"""
        self._conn_mgr.close_connection()

    def _row_to_user(self, row: sqlite3.Row) -> Optional[User]:
        """
        [已修正] 将数据库行安全地转换为 User 对象。
        现在可以正确读取所有新旧字段。
        """
        if not row:
            return None
            
        def parse_datetime(dt_val):
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
        
        # 使用 .keys() 检查字段是否存在，确保向后兼容性
        row_keys = row.keys()
        
        return User(
            user_id=row["user_id"],
            nickname=row["nickname"],
            coins=row["coins"],
            premium_currency=row["premium_currency"],
            total_fishing_count=row["total_fishing_count"],
            total_weight_caught=row["total_weight_caught"],
            total_coins_earned=row["total_coins_earned"],
            max_coins=row["max_coins"] if "max_coins" in row_keys else 0,
            consecutive_login_days=row["consecutive_login_days"],
            fish_pond_capacity=row["fish_pond_capacity"],
            aquarium_capacity=row["aquarium_capacity"] if "aquarium_capacity" in row_keys else 50,
            created_at=parse_datetime(row["created_at"]),
            equipped_rod_instance_id=row["equipped_rod_instance_id"],
            equipped_accessory_instance_id=row["equipped_accessory_instance_id"],
            current_title_id=row["current_title_id"],
            current_bait_id=row["current_bait_id"],
            bait_start_time=parse_datetime(row["bait_start_time"]),

            max_wipe_bomb_multiplier=row["max_wipe_bomb_multiplier"] if "max_wipe_bomb_multiplier" in row_keys else 0.0,
            min_wipe_bomb_multiplier=row["min_wipe_bomb_multiplier"] if "min_wipe_bomb_multiplier" in row_keys else None,

            auto_fishing_enabled=bool(row["auto_fishing_enabled"]),
            last_fishing_time=parse_datetime(row["last_fishing_time"]),
            last_wipe_bomb_time=parse_datetime(row["last_wipe_bomb_time"]),
            last_steal_time=parse_datetime(row["last_steal_time"]),
            last_electric_fish_time=parse_datetime(row["last_electric_fish_time"]) if "last_electric_fish_time" in row_keys else None,
            last_login_time=parse_datetime(row["last_login_time"]),
            last_stolen_at=parse_datetime(row["last_stolen_at"]),
            wipe_bomb_forecast=row["wipe_bomb_forecast"],
            fishing_zone_id=row["fishing_zone_id"],

            # ==================== 在这里添加新字段 ====================
            wipe_bomb_attempts_today=row["wipe_bomb_attempts_today"] if "wipe_bomb_attempts_today" in row_keys else 0,
            last_wipe_bomb_date=row["last_wipe_bomb_date"] if "last_wipe_bomb_date" in row_keys else None,
            # =========================================================

            # --- [关键修复] 添加所有命运之轮字段的读取 ---
            in_wheel_of_fate=bool(row["in_wheel_of_fate"]) if "in_wheel_of_fate" in row_keys else False,
            wof_current_level=row["wof_current_level"] if "wof_current_level" in row_keys else 0,
            wof_current_prize=row["wof_current_prize"] if "wof_current_prize" in row_keys else 0,
            wof_entry_fee=row["wof_entry_fee"] if "wof_entry_fee" in row_keys else 0,
            last_wof_play_time=parse_datetime(row["last_wof_play_time"]) if "last_wof_play_time" in row_keys else None,
            wof_last_action_time=parse_datetime(row["wof_last_action_time"]) if "wof_last_action_time" in row_keys else None,
            wof_plays_today=row["wof_plays_today"] if "wof_plays_today" in row_keys else 0,
            last_wof_date=row["last_wof_date"] if "last_wof_date" in row_keys else None,
            # --- [新功能] 添加骰宝冷却字段的读取 ---
            last_sicbo_time=parse_datetime(row["last_sicbo_time"]) if "last_sicbo_time" in row_keys else None,
            
            # --- [新功能] 添加交易所账户状态字段的读取 ---
            exchange_account_status=bool(row["exchange_account_status"]) if "exchange_account_status" in row_keys else False,
            exchange_capacity_level=row["exchange_capacity_level"] if "exchange_capacity_level" in row_keys else 0,
        )

    def get_by_id(self, user_id: str) -> Optional[User]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            return self._row_to_user(row)

    def check_exists(self, user_id: str) -> bool:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            return cursor.fetchone() is not None

    def add(self, user: User) -> None:
        # 使用与 update 相同的动态方法，确保 add 也是完整的
        fields = [f.name for f in dataclasses.fields(User)]
        columns_clause = ", ".join(fields)
        placeholders_clause = ", ".join(["?"] * len(fields))
        values = [getattr(user, field) for field in fields]

        sql = f"INSERT INTO users ({columns_clause}) VALUES ({placeholders_clause})"

        def _op(cursor: sqlite3.Cursor) -> None:
            cursor.execute(sql, tuple(values))

        self._conn_mgr.run_in_transaction(_op)

    def update(self, user: User) -> None:
        """
        [已修正] 更新一个现有的用户记录。
        这个版本是动态的，可以处理 User 模型中的所有字段，未来扩展也无需修改。
        自动更新 max_coins 如果当前金币数超过历史最高。
        """
        # 自动更新历史最高金币数
        if user.coins > user.max_coins:
            user.max_coins = user.coins
        
        fields = [f.name for f in dataclasses.fields(User) if f.name != 'user_id']
        set_clause = ", ".join([f"{field} = ?" for field in fields])
        values = [getattr(user, field) for field in fields]
        values.append(user.user_id)
        
        sql = f"UPDATE users SET {set_clause} WHERE user_id = ?"
        insert_fields = [f.name for f in dataclasses.fields(User)]
        insert_columns = ", ".join(insert_fields)
        insert_placeholders = ", ".join(["?"] * len(insert_fields))
        insert_values = tuple(getattr(user, field) for field in insert_fields)
        insert_sql = (
            f"INSERT INTO users ({insert_columns}) VALUES ({insert_placeholders})"
        )

        try:
            def _op(cursor: sqlite3.Cursor) -> bool:
                cursor.execute(sql, tuple(values))
                if cursor.rowcount == 0:
                    cursor.execute(insert_sql, insert_values)
                    return True
                return False

            inserted = self._conn_mgr.run_in_transaction(_op)
            if inserted:
                logger.warning(f"尝试更新不存在的用户 {user.user_id}，将转为添加操作。")
        except sqlite3.Error as e:
            logger.error(f"更新用户 {user.user_id} 数据时发生数据库错误: {e}")
            raise

    def toggle_auto_fishing(self, user_id: str) -> Optional[bool]:
        """原子切换自动钓鱼状态并返回新状态。"""
        def _op(cursor: sqlite3.Cursor) -> Optional[bool]:
            cursor.execute(
                """
                UPDATE users
                SET auto_fishing_enabled = CASE auto_fishing_enabled WHEN 0 THEN 1 ELSE 0 END
                WHERE user_id = ?
                RETURNING auto_fishing_enabled
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            return bool(row[0]) if row else None

        return self._conn_mgr.run_in_transaction(_op)

    def set_auto_fishing_enabled(self, user_id: str, enabled: bool) -> bool:
        """原子设置自动钓鱼状态。"""
        def _op(cursor: sqlite3.Cursor) -> bool:
            cursor.execute(
                "UPDATE users SET auto_fishing_enabled = ? WHERE user_id = ?",
                (1 if enabled else 0, user_id),
            )
            return cursor.rowcount == 1

        return self._conn_mgr.run_in_transaction(_op)

    def record_failed_fishing(self, user_id: str, cost: int, timestamp: datetime) -> bool:
        """仅更新失败钓鱼所需字段，避免整行旧数据回写。"""
        def _op(cursor: sqlite3.Cursor) -> bool:
            cursor.execute(
                """
                UPDATE users
                SET coins = coins - ?, last_fishing_time = ?
                WHERE user_id = ? AND coins >= ?
                """,
                (cost, timestamp, user_id, cost),
            )
            return cursor.rowcount == 1

        return self._conn_mgr.run_in_transaction(_op)

    def update_bait_state(
        self, user_id: str, bait_id: Optional[int], bait_start_time: Optional[datetime]
    ) -> bool:
        """只保存当前鱼饵状态。"""
        def _op(cursor: sqlite3.Cursor) -> bool:
            cursor.execute(
                "UPDATE users SET current_bait_id = ?, bait_start_time = ? WHERE user_id = ?",
                (bait_id, bait_start_time, user_id),
            )
            return cursor.rowcount == 1

        return self._conn_mgr.run_in_transaction(_op)

    def set_fishing_zone(self, user_id: str, zone_id: int) -> bool:
        """只更新用户所在钓鱼区域。"""
        def _op(cursor: sqlite3.Cursor) -> bool:
            cursor.execute(
                "UPDATE users SET fishing_zone_id = ? WHERE user_id = ?",
                (zone_id, user_id),
            )
            return cursor.rowcount == 1

        return self._conn_mgr.run_in_transaction(_op)

    def deduct_coins_up_to(self, user_id: str, amount: int) -> tuple[int, int]:
        """在一个事务内最多扣除指定金币，返回实际扣除额和余额。"""
        amount = max(int(amount), 0)

        def _op(cursor: sqlite3.Cursor) -> tuple[int, int]:
            cursor.execute("SELECT coins FROM users WHERE user_id = ?", (user_id,))
            row = cursor.fetchone()
            if row is None:
                return 0, 0
            balance = int(row[0])
            deducted = min(amount, balance)
            new_balance = balance - deducted
            cursor.execute(
                "UPDATE users SET coins = ? WHERE user_id = ?",
                (new_balance, user_id),
            )
            return deducted, new_balance

        return self._conn_mgr.run_in_transaction(_op)

    def get_all_user_ids(self, auto_fishing_only: bool = False) -> List[str]:
        query = "SELECT user_id FROM users"
        if auto_fishing_only:
            query += " WHERE auto_fishing_enabled = 1"
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [row["user_id"] for row in cursor.fetchall()]

    def _get_top_users_base_query(self, order_by_column: str, limit: int) -> List[User]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            if order_by_column not in ["total_fishing_count", "coins", "total_weight_caught", "max_coins"]:
                raise ValueError("Invalid order by column")
            
            query = f"SELECT * FROM users WHERE user_id != 'SYSTEM' ORDER BY {order_by_column} DESC LIMIT ?"
            cursor.execute(query, (limit,))
            return [self._row_to_user(row) for row in cursor.fetchall()]

    def get_top_users_by_fish_count(self, limit: int) -> List[User]:
        return self._get_top_users_base_query("total_fishing_count", limit)

    def get_top_users_by_coins(self, limit: int) -> List[User]:
        return self._get_top_users_base_query("coins", limit)

    def get_top_users_by_max_coins(self, limit: int) -> List[User]:
        """获取历史最高金币排行榜"""
        return self._get_top_users_base_query("max_coins", limit)

    def get_top_users_by_weight(self, limit: int) -> List[User]:
        return self._get_top_users_base_query("total_weight_caught", limit)

    def get_high_value_users(self, threshold: int) -> List[User]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE coins >= ?", (threshold,))
            return [self._row_to_user(row) for row in cursor.fetchall()]
    
    # 其他辅助方法保持不变...
    def get_all_users(self, limit: int = 100, offset: int = 0) -> List[User]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ? OFFSET ?", (limit, offset))
            return [self._row_to_user(row) for row in cursor.fetchall()]

    def get_users_count(self) -> int:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0]

    def search_users(self, keyword: str, limit: int = 50, offset: int = 0) -> List[User]:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM users 
                WHERE user_id LIKE ? OR nickname LIKE ? 
                ORDER BY created_at DESC 
                LIMIT ? OFFSET ?
            """, (f"%{keyword}%", f"%{keyword}%", limit, offset))
            return [self._row_to_user(row) for row in cursor.fetchall()]

    def get_search_users_count(self, keyword: str) -> int:
        with self._conn_mgr.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM users WHERE user_id LIKE ? OR nickname LIKE ?",
                (f"%{keyword}%", f"%{keyword}%")
            )
            return cursor.fetchone()[0]

    def delete_user(self, user_id: str) -> bool:
        try:
            def _op(cursor: sqlite3.Cursor) -> bool:
                cursor.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
                return cursor.rowcount > 0

            return self._conn_mgr.run_in_transaction(_op)
        except sqlite3.Error:
            return False
