"""
迁移044：为鱼饵增加重量潜力乘数
"""

import sqlite3

from astrbot.api import logger


def up(cursor: sqlite3.Cursor):
    """添加鱼饵重量潜力乘数字段，并修正巨物诱饵默认效果。"""
    logger.info("[迁移044] 为鱼饵添加 weight_modifier 字段")
    try:
        cursor.execute("ALTER TABLE baits ADD COLUMN weight_modifier REAL DEFAULT 1.0")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            logger.debug("[迁移044] weight_modifier 字段已存在，跳过添加")
        else:
            raise

    cursor.execute("""
        UPDATE baits
        SET weight_modifier = 1.2
        WHERE name = '巨物诱饵'
    """)


def down(cursor: sqlite3.Cursor):
    """SQLite 不直接删除列；回滚时仅还原巨物诱饵数值。"""
    logger.info("[迁移044-回滚] 还原巨物诱饵 weight_modifier")
    cursor.execute("""
        UPDATE baits
        SET weight_modifier = 1.0
        WHERE name = '巨物诱饵'
    """)
