"""
迁移042：添加交易所仓库升级等级字段
"""

from astrbot.api import logger


def up(cursor):
    """添加 users.exchange_capacity_level 字段"""
    cursor.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cursor.fetchall()]

    if "exchange_capacity_level" not in columns:
        logger.info("[迁移042] 添加 exchange_capacity_level 字段到 users 表")
        cursor.execute("""
            ALTER TABLE users ADD COLUMN exchange_capacity_level INTEGER DEFAULT 0
        """)
        logger.info("[迁移042] exchange_capacity_level 字段添加成功")
    else:
        logger.info("[迁移042] exchange_capacity_level 字段已存在，跳过")


def down(cursor):
    """SQLite 不支持稳定删除列，这里保留字段"""
    logger.info("[迁移042-回滚] 跳过 exchange_capacity_level 字段删除")
