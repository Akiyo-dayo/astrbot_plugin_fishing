"""
迁移043：允许小钱袋直接使用并消耗
"""

from astrbot.api import logger


def up(cursor):
    """将已有数据库中的小钱袋标记为可消耗"""
    logger.info("[迁移043] 设置小钱袋为可消耗道具")
    cursor.execute("""
        UPDATE items
        SET is_consumable = 1,
            effect_type = COALESCE(effect_type, 'ADD_COINS'),
            effect_payload = COALESCE(effect_payload, '{"amount": 1000}')
        WHERE name = '小钱袋'
    """)


def down(cursor):
    """回滚为不可消耗"""
    logger.info("[迁移043-回滚] 设置小钱袋为不可消耗")
    cursor.execute("""
        UPDATE items
        SET is_consumable = 0
        WHERE name = '小钱袋'
    """)
