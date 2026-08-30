import json
import sqlite3
from pathlib import Path


DEFAULT_UPGRADES_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "aquarium_upgrades.json"
)


def _load_default_upgrades():
    """迁移只负责建表；初始档位来自独立、可审阅的版本化配置。"""
    with DEFAULT_UPGRADES_PATH.open("r", encoding="utf-8") as file:
        rows = json.load(file)
    return [
        (
            int(row["level"]),
            int(row["capacity"]),
            int(row["cost_coins"]),
            int(row.get("cost_premium", 0)),
            str(row.get("description") or ""),
        )
        for row in rows
    ]


def up(cursor: sqlite3.Cursor):
    """
    添加水族箱系统：
    - 用户水族箱表：存储用户水族箱中的鱼
    - 用户表添加水族箱容量字段
    - 水族箱升级配置表
    """
    
    # 1. 在用户表中添加水族箱容量字段
    cursor.execute("""
        ALTER TABLE users ADD COLUMN aquarium_capacity INTEGER DEFAULT 50
    """)
    
    # 2. 创建用户水族箱表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_aquarium (
            user_id TEXT NOT NULL,
            fish_id INTEGER NOT NULL,
            quantity INTEGER DEFAULT 0 CHECK (quantity >= 0),
            added_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, fish_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
            FOREIGN KEY (fish_id) REFERENCES fish(fish_id) ON DELETE CASCADE
        )
    """)
    
    # 3. 创建水族箱升级配置表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS aquarium_upgrades (
            upgrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            level INTEGER NOT NULL UNIQUE,
            capacity INTEGER NOT NULL,
            cost_coins INTEGER NOT NULL,
            cost_premium INTEGER DEFAULT 0,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # 4. 插入默认的水族箱升级配置
    upgrades = _load_default_upgrades()
    
    cursor.executemany("""
        INSERT OR IGNORE INTO aquarium_upgrades (level, capacity, cost_coins, cost_premium, description)
        VALUES (?, ?, ?, ?, ?)
    """, upgrades)
    
    # 5. 创建索引
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_aquarium_user ON user_aquarium(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_aquarium_fish ON user_aquarium(fish_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_aquarium_upgrades_level ON aquarium_upgrades(level)")


def down(cursor: sqlite3.Cursor):
    """回滚水族箱系统"""
    cursor.execute("DROP TABLE IF EXISTS user_aquarium")
    cursor.execute("DROP TABLE IF EXISTS aquarium_upgrades")
    
    # 注意：SQLite不支持直接删除列，所以这里不处理users表的aquarium_capacity字段
    # 如果需要完全回滚，需要重建users表
