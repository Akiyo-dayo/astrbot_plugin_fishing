from __future__ import annotations

import sqlite3
from typing import Any, Mapping, Sequence

from ..database.connection_manager import DatabaseConnectionManager
from ..domain.models import AquariumUpgrade
from .abstract_repository import AbstractAquariumConfigRepository


class _SqliteCapacityUpgradeRepository:
    """容量升级表的公共实现；表名和关联字段只允许由内部固定配置提供。"""

    def __init__(
        self,
        db_path: str,
        *,
        upgrade_table: str,
        user_capacity_column: str,
        inventory_table: str,
        system_label: str,
    ):
        self.db_path = db_path
        self._upgrade_table = upgrade_table
        self._user_capacity_column = user_capacity_column
        self._inventory_table = inventory_table
        self._system_label = system_label
        self._conn_mgr = DatabaseConnectionManager(db_path)

    def close_connection(self) -> None:
        self._conn_mgr.close_connection()

    @staticmethod
    def _row_to_upgrade(row: sqlite3.Row | None) -> AquariumUpgrade | None:
        return None if row is None else AquariumUpgrade(**row)

    @staticmethod
    def _level_for_capacity(
        capacity: int, upgrades: Sequence[Mapping[str, int]]
    ) -> int:
        if not upgrades:
            return 1
        current_level = int(upgrades[0]["level"])
        for upgrade in upgrades:
            if capacity < int(upgrade["capacity"]):
                break
            current_level = int(upgrade["level"])
        return current_level

    def get_all(self) -> list[AquariumUpgrade]:
        with self._conn_mgr.get_connection() as conn:
            rows = conn.execute(
                f"""
                SELECT upgrade_id, level, capacity, cost_coins, cost_premium,
                       description, created_at
                FROM {self._upgrade_table}
                ORDER BY level
                """
            ).fetchall()
            return [self._row_to_upgrade(row) for row in rows]

    def get_by_level(self, level: int) -> AquariumUpgrade | None:
        with self._conn_mgr.get_connection() as conn:
            row = conn.execute(
                f"""
                SELECT upgrade_id, level, capacity, cost_coins, cost_premium,
                       description, created_at
                FROM {self._upgrade_table}
                WHERE level = ?
                """,
                (level,),
            ).fetchone()
            return self._row_to_upgrade(row)

    def replace_all(self, upgrades: Sequence[Mapping[str, Any]]) -> int:
        """原子替换档位，并让已有玩家保持原等级对应的新容量。"""
        new_by_level = {int(row["level"]): row for row in upgrades}
        highest_new_level = max(new_by_level)

        def _op(cursor: sqlite3.Cursor) -> int:
            old_rows = cursor.execute(
                f"SELECT level, capacity FROM {self._upgrade_table} ORDER BY level"
            ).fetchall()
            old_upgrades = [
                {"level": int(row["level"]), "capacity": int(row["capacity"])}
                for row in old_rows
            ]

            user_rows = cursor.execute(
                f"""
                SELECT u.user_id, u.{self._user_capacity_column} AS capacity,
                       COALESCE(SUM(i.quantity), 0) AS inventory_count
                FROM users AS u
                LEFT JOIN {self._inventory_table} AS i ON i.user_id = u.user_id
                GROUP BY u.user_id, u.{self._user_capacity_column}
                """
            ).fetchall()

            capacity_updates: list[tuple[int, str]] = []
            for user_row in user_rows:
                current_capacity = int(user_row["capacity"] or 0)
                current_level = self._level_for_capacity(
                    current_capacity, old_upgrades
                )
                if current_level > highest_new_level:
                    raise ValueError(
                        f"不能删除等级 {current_level}（{self._system_label}）："
                        "仍有玩家正在使用该等级"
                    )

                target_capacity = int(new_by_level[current_level]["capacity"])
                inventory_count = int(user_row["inventory_count"] or 0)
                if inventory_count > target_capacity:
                    raise ValueError(
                        f"{self._system_label}等级 {current_level} 的新容量 "
                        f"{target_capacity} 小于玩家现有{self._system_label}数量 "
                        f"{inventory_count}"
                    )
                if current_capacity != target_capacity:
                    capacity_updates.append(
                        (target_capacity, str(user_row["user_id"]))
                    )

            cursor.execute(
                f"DELETE FROM {self._upgrade_table} WHERE level > ?",
                (highest_new_level,),
            )
            for row in upgrades:
                cursor.execute(
                    f"""
                    INSERT INTO {self._upgrade_table} (
                        level, capacity, cost_coins, cost_premium, description
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(level) DO UPDATE SET
                        capacity = excluded.capacity,
                        cost_coins = excluded.cost_coins,
                        cost_premium = excluded.cost_premium,
                        description = excluded.description
                    """,
                    (
                        int(row["level"]),
                        int(row["capacity"]),
                        int(row["cost_coins"]),
                        int(row["cost_premium"]),
                        str(row.get("description") or ""),
                    ),
                )

            if capacity_updates:
                cursor.executemany(
                    f"""
                    UPDATE users SET {self._user_capacity_column} = ?
                    WHERE user_id = ?
                    """,
                    capacity_updates,
                )
            return len(capacity_updates)

        return self._conn_mgr.run_in_transaction(_op)


class SqliteAquariumConfigRepository(
    _SqliteCapacityUpgradeRepository, AbstractAquariumConfigRepository
):
    """水族箱升级档位的独立 SQLite 仓储。"""

    def __init__(self, db_path: str):
        super().__init__(
            db_path,
            upgrade_table="aquarium_upgrades",
            user_capacity_column="aquarium_capacity",
            inventory_table="user_aquarium",
            system_label="水族箱",
        )


class SqliteFishPondConfigRepository(
    _SqliteCapacityUpgradeRepository, AbstractAquariumConfigRepository
):
    """鱼塘升级档位的独立 SQLite 仓储。"""

    def __init__(self, db_path: str):
        super().__init__(
            db_path,
            upgrade_table="fish_pond_upgrades",
            user_capacity_column="fish_pond_capacity",
            inventory_table="user_fish_inventory",
            system_label="鱼塘",
        )
