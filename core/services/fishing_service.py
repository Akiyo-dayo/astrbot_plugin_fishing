import json
import math
import random
import threading
import time
from typing import Dict, Any, Optional
from datetime import timedelta
from astrbot.api import logger

# 导入仓储接口和领域模型
from ..repositories.abstract_repository import (
    AbstractUserRepository,
    AbstractInventoryRepository,
    AbstractItemTemplateRepository,
    AbstractLogRepository,
    AbstractUserBuffRepository,
)
from ..domain.models import FishingRecord, TaxRecord, FishingZone
from ..services.fishing_zone_service import FishingZoneService
from .fish_weight_service import FishWeightService
from ..utils import get_now, get_last_reset_time, calculate_after_refine


class FishingService:
    """封装核心的钓鱼动作及后台任务"""

    def __init__(
        self,
        user_repo: AbstractUserRepository,
        inventory_repo: AbstractInventoryRepository,
        item_template_repo: AbstractItemTemplateRepository,
        log_repo: AbstractLogRepository,
        buff_repo: AbstractUserBuffRepository,
        fishing_zone_service: FishingZoneService,
        fish_weight_service: FishWeightService,
        config: Dict[str, Any],
    ):
        self.user_repo = user_repo
        self.inventory_repo = inventory_repo
        self.item_template_repo = item_template_repo
        self.log_repo = log_repo
        self.buff_repo = buff_repo
        self.fish_weight_service = fish_weight_service
        self.fishing_zone_service = fishing_zone_service
        self.config = config

        # 获取每日刷新时间配置
        self.daily_reset_hour = self.config.get("daily_reset_hour", 0)
        self.last_reset_time = get_last_reset_time(self.daily_reset_hour)
        # 自动钓鱼线程相关属性
        self.auto_fishing_thread: Optional[threading.Thread] = None
        self.auto_fishing_running = False
        # 税收线程相关属性
        self.tax_thread: Optional[threading.Thread] = None
        self.tax_running = False
        self.last_tax_reset_time = get_last_reset_time(self.daily_reset_hour)
        self.tax_execution_lock = threading.Lock()  # 防止税收并发执行的锁
        self.tax_start_lock = threading.Lock()  # 防止重复创建税收线程的锁
        self.rare_fish_reset_lock = threading.Lock()  # 防止稀有鱼重置并发执行的锁
        # 可选的消息通知回调：签名 (target: str, message: str) -> None，用于消息通知
        self._notifier = None
        # 通知目标可配置，默认群聊。可由 config['notifications']['relocation_target'] 覆盖
        notifications_cfg = self.config.get("notifications", {}) if isinstance(self.config, dict) else {}
        self._notification_target = notifications_cfg.get("relocation_target", "group")
        

    def register_notifier(self, notifier, default_target: Optional[str] = None):
        """
        注册一个用于发送系统消息的回调（如群聊推送）。
        回调应为同步函数，签名为 (target: str, message: str) -> None。
        默认目标可通过参数或配置指定。
        """
        self._notifier = notifier
        if default_target:
            self._notification_target = default_target

    def toggle_auto_fishing(self, user_id: str) -> Dict[str, Any]:
        """
        切换用户的自动钓鱼状态。

        Args:
            user_id: 用户ID。

        Returns:
            一个包含操作结果的字典。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "❌您还没有注册，请先使用 /注册 命令注册。"}

        user.auto_fishing_enabled = not user.auto_fishing_enabled
        self.user_repo.update(user)

        if user.auto_fishing_enabled:
            return {"success": True, "message": "🎣 自动钓鱼已开启！"}
        else:
            return {"success": True, "message": "🚫 自动钓鱼已关闭！"}

    def go_fish(self, user_id: str) -> Dict[str, Any]:
        """
        执行一次完整的钓鱼动作。

        Args:
            user_id: 尝试钓鱼的用户ID。

        Returns:
            一个包含结果的字典。
        """
        # 在执行钓鱼前，先检查并执行每日重置（如果需要）
        self._reset_rare_fish_daily_quota()
        
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在，无法钓鱼。"}

        # 1. 检查成本（从区域配置中读取）
        zone = self.inventory_repo.get_zone_by_id(user.fishing_zone_id)
        if not zone:
            return {"success": False, "message": "钓鱼区域不存在"}
        
        # 检查区域是否激活
        if not zone.is_active:
            return {"success": False, "message": "该钓鱼区域被浓雾隐匿了，暂时无法进入"}
        
        # 检查时间限制
        now = get_now()
        if zone.available_from and now < zone.available_from:
            return {"success": False, "message": f"该钓鱼区域将在 {zone.available_from.strftime('%Y-%m-%d %H:%M')} 开放"}
        
        if zone.available_until and now > zone.available_until:
            # 区域已关闭，自动传送回初始区域
            user.fishing_zone_id = 1
            self.user_repo.update(user)
            # 获取初始区域的名字
            first_zone = self.inventory_repo.get_zone_by_id(1)
            first_zone_name = first_zone.name if first_zone else "初始区域"
            return {"success": False, "message": f"该钓鱼区域已于 {zone.available_until.strftime('%Y-%m-%d %H:%M')} 关闭，已自动传送回{first_zone_name}"}
        
        fishing_cost = zone.fishing_cost
        if not user.can_afford(fishing_cost):
            return {"success": False, "message": f"金币不足，需要 {fishing_cost} 金币。"}

        # 先扣除成本
        user.coins -= fishing_cost

        # 2. 计算各种加成和修正值
        base_success_rate = 0.7 # 基础成功率70%
        quality_modifier = 1.0 # 品质加成
        quantity_modifier = 1.0 # 数量加成
        rare_chance = 0.0 # 稀有鱼出现几率
        coins_chance = 0.0 # 增加同稀有度高金币出现几率

        # --- 新增：应用 Buff 效果 ---
        active_buffs = self.buff_repo.get_all_active_by_user(user_id)
        for buff in active_buffs:
            if buff.buff_type == "RARE_FISH_BOOST":
                try:
                    payload = json.loads(buff.payload) if buff.payload else {}
                    multiplier = payload.get("multiplier", 1.0)
                    # 这里的实现是直接增加到 rare_chance
                    # 注意：如果基础值是0，乘法无意义，所以用加法或更复杂的逻辑
                    # 假设 payload 的 multiplier 是一个额外的概率加成
                    rare_chance += multiplier
                    logger.info(
                        f"用户 {user_id} 的 RARE_FISH_BOOST 生效，稀有几率增加 {multiplier}"
                    )
                except (json.JSONDecodeError, TypeError):
                    logger.error(f"解析 buff payload 失败: {buff.payload}")
        # --- Buff 应用结束 ---

        logger.debug(
            f"当前钓鱼概率： base_success_rate={base_success_rate}, quality_modifier={quality_modifier}, quantity_modifier={quantity_modifier}, rare_chance={rare_chance}, coins_chance={coins_chance}"
        )
        # 获取装备鱼竿并应用加成
        equipped_rod_instance = self.inventory_repo.get_user_equipped_rod(user.user_id)
        if equipped_rod_instance:
            rod_template = self.item_template_repo.get_rod_by_id(equipped_rod_instance.rod_id)
            if rod_template:
                quality_modifier *= calculate_after_refine(rod_template.bonus_fish_quality_modifier, refine_level= equipped_rod_instance.refine_level, rarity=rod_template.rarity)
                quantity_modifier *= calculate_after_refine(rod_template.bonus_fish_quantity_modifier, refine_level= equipped_rod_instance.refine_level, rarity=rod_template.rarity)
                rare_chance += calculate_after_refine(rod_template.bonus_rare_fish_chance, refine_level= equipped_rod_instance.refine_level, rarity=rod_template.rarity)
        logger.debug(f"装备鱼竿加成后： quality_modifier={quality_modifier}, quantity_modifier={quantity_modifier}, rare_chance={rare_chance}")
        # 获取装备饰品并应用加成
        equipped_accessory_instance = self.inventory_repo.get_user_equipped_accessory(user.user_id)
        if equipped_accessory_instance:
            acc_template = self.item_template_repo.get_accessory_by_id(equipped_accessory_instance.accessory_id)
            if acc_template:
                quality_modifier *= calculate_after_refine(acc_template.bonus_fish_quality_modifier, refine_level= equipped_accessory_instance.refine_level, rarity=acc_template.rarity)
                quantity_modifier *= calculate_after_refine(acc_template.bonus_fish_quantity_modifier, refine_level= equipped_accessory_instance.refine_level, rarity=acc_template.rarity)
                rare_chance += calculate_after_refine(acc_template.bonus_rare_fish_chance, refine_level= equipped_accessory_instance.refine_level, rarity=acc_template.rarity)
                coins_chance += calculate_after_refine(acc_template.bonus_coin_modifier, refine_level= equipped_accessory_instance.refine_level, rarity=acc_template.rarity) - 1
        logger.debug(f"装备饰品加成后： quality_modifier={quality_modifier}, quantity_modifier={quantity_modifier}, rare_chance={rare_chance}, coins_chance={coins_chance}")
        # 获取鱼饵并应用加成
        cur_bait_id = user.current_bait_id
        garbage_reduction_modifier = None

        # 判断鱼饵是否过期
        if user.current_bait_id is not None:
            bait_template = self.item_template_repo.get_bait_by_id(cur_bait_id)
            if bait_template and bait_template.duration_minutes > 0:
                # 检查鱼饵是否过期
                bait_expiry_time = user.bait_start_time
                if bait_expiry_time:
                    now = get_now()
                    expiry_time = bait_expiry_time + timedelta(minutes=bait_template.duration_minutes)
                    # 移除两个时间的时区信息
                    if now.tzinfo is not None:
                        now = now.replace(tzinfo=None)
                    if expiry_time.tzinfo is not None:
                        expiry_time = expiry_time.replace(tzinfo=None)
                    if now > expiry_time:
                        # 鱼饵已过期，清除当前鱼饵
                        user.current_bait_id = None
                        user.bait_start_time = None
                        self.inventory_repo.update_bait_quantity(user_id, cur_bait_id, -1)
                        self.user_repo.update(user)
                        logger.warning(f"用户 {user_id} 的当前鱼饵{bait_template}已过期，已被清除。")
            else:
                if bait_template:
                    # 如果鱼饵没有设置持续时间, 是一次性鱼饵，消耗一个鱼饵
                    user_bait_inventory = self.inventory_repo.get_user_bait_inventory(user_id)
                    if user_bait_inventory is not None and user_bait_inventory.get(user.current_bait_id, 0) > 0:
                        self.inventory_repo.update_bait_quantity(user_id, user.current_bait_id, -1)
                    else:
                        # 如果用户没有库存鱼饵，清除当前鱼饵
                        user.current_bait_id = None
                        user.bait_start_time = None
                        self.user_repo.update(user)
                        logger.warning(f"用户 {user_id} 的当前鱼饵{bait_template.bait_id}已被清除，因为库存不足。")
                else:
                    # 如果鱼饵模板不存在，清除当前鱼饵
                    user.current_bait_id = None
                    user.bait_start_time = None
                    self.user_repo.update(user)
                    logger.warning(f"用户 {user_id} 的当前鱼饵已被清除，因为鱼饵模板不存在。")

        if user.current_bait_id is None:
            is_renewed = False
            # 1. 优先尝试续上刚才使用的同款鱼饵
            if cur_bait_id is not None:
                user_bait_inventory = self.inventory_repo.get_user_bait_inventory(user_id)
                # 如果同款鱼饵还有库存，则自动续上同款
                if user_bait_inventory is not None and user_bait_inventory.get(cur_bait_id, 0) > 0:
                    user.current_bait_id = cur_bait_id
                    
                    # 如果是限时型鱼饵，自动续期时需要重置它的生效开始时间
                    bait_template = self.item_template_repo.get_bait_by_id(cur_bait_id)
                    if bait_template and bait_template.duration_minutes > 0:
                        user.bait_start_time = get_now()
                    
                    self.user_repo.update(user)
                    logger.info(f"用户 {user_id} 自动续装了同款鱼饵: {cur_bait_id}")
                    is_renewed = True
            
            # 2. 如果同款已经彻底用光了（未成功续杯），则随机抓取背包里的其他鱼饵
            if not is_renewed:
                random_bait_id = self.inventory_repo.get_random_bait(user.user_id)
                if random_bait_id:
                    user.current_bait_id = random_bait_id
                    
                    # 为新随机到的鱼饵初始化时间（如果抽到的是限时鱼饵）
                    new_bait_template = self.item_template_repo.get_bait_by_id(random_bait_id)
                    if new_bait_template and new_bait_template.duration_minutes > 0:
                        user.bait_start_time = get_now()
                    
                    self.user_repo.update(user)
                    logger.info(f"用户 {user_id} 同款耗尽，自动随机切换至新鱼饵: {random_bait_id}")

        if user.current_bait_id is not None:
            bait_template = self.item_template_repo.get_bait_by_id(user.current_bait_id)
            # logger.info(f"鱼饵信息: {bait_template}")
            if bait_template:
                quantity_modifier *= bait_template.quantity_modifier
                rare_chance += bait_template.rare_chance_modifier
                base_success_rate += bait_template.success_rate_modifier
                garbage_reduction_modifier = bait_template.garbage_reduction_modifier
                coins_chance += bait_template.value_modifier - 1
        logger.debug(f"使用鱼饵加成后： base_success_rate={base_success_rate}, quality_modifier={quality_modifier}, quantity_modifier={quantity_modifier}, rare_chance={rare_chance}, coins_chance={coins_chance}")
        # 3. 判断是否成功钓到
        if random.random() >= base_success_rate:
            # 失败逻辑
            user.last_fishing_time = get_now()
            self.user_repo.update(user)
            return {"success": False, "message": "💨 什么都没钓到..."}

        # 4. 成功，生成渔获
        # 使用区域策略获取基础稀有度分布
        strategy = self.fishing_zone_service.get_strategy(user.fishing_zone_id)
        rarity_distribution = strategy.get_fish_rarity_distribution(user)
        
        zone = self.inventory_repo.get_zone_by_id(user.fishing_zone_id)
        is_rare_fish_available = zone.rare_fish_caught_today < zone.daily_rare_fish_quota
        
        if not is_rare_fish_available:
            # 稀有鱼定义：4星及以上（包括5星和6+星组合）
            # 若达到配额，屏蔽4星、5星和6+星概率，其它星级不受影响
            if len(rarity_distribution) >= 4:
                rarity_distribution[3] = 0.0  # 4星
            if len(rarity_distribution) >= 5:
                rarity_distribution[4] = 0.0  # 5星
            if len(rarity_distribution) >= 6:
                rarity_distribution[5] = 0.0  # 6+星
            # 重新归一化概率分布
            total = sum(rarity_distribution)
            if total > 0:
                rarity_distribution = [x / total for x in rarity_distribution]
        
        # 应用稀有度加成（rare_chance）调整分布权重
        # 如果玩家有装备/Buff/鱼饵提供的稀有度加成，会提升 4-5 星鱼的概率
        # 6+ 星鱼的概率不受影响，保持其作为"运气时刻"的设计
        if rare_chance > 0:
            adjusted_distribution = self._apply_rare_chance_to_distribution(
                rarity_distribution, rare_chance
            )
        else:
            adjusted_distribution = rarity_distribution
        
        # 根据调整后的分布加权随机抽取稀有度
        rarity_index = random.choices(range(len(adjusted_distribution)), weights=adjusted_distribution, k=1)[0]
        
        if rarity_index == 5:  # 抽中6+星组合
            # 从6星及以上的鱼中随机选择，兼容区域限定鱼
            rarity = self._get_random_high_rarity(zone)
        else:
            # 1-5星直接对应
            rarity = rarity_index + 1
            
        fish_template = self._get_fish_template(rarity, zone, coins_chance)

        if not fish_template:
             return {"success": False, "message": "错误：当前条件下没有可钓的鱼！"}

        # 如果有垃圾鱼减少修正，则应用，价值 < 5则被视为垃圾鱼
        if garbage_reduction_modifier is not None and fish_template.base_value < 5:
            # 根据垃圾鱼减少修正值决定是否重新选择一次
            if random.random() < garbage_reduction_modifier:
                # 重新选择一条鱼
                new_rarity = random.choices(range(1, len(rarity_distribution) + 1), weights=rarity_distribution, k=1)[0]
                new_fish_template = self._get_fish_template(new_rarity, zone, coins_chance)

                if new_fish_template:
                    fish_template = new_fish_template

        # 计算最终属性
        weight = random.randint(fish_template.min_weight, fish_template.max_weight)
        value = fish_template.base_value

        # 4.2 按品质加成给予额外品质（重量/价值）奖励
        # 品质加成来自：鱼竿 × 饰品 × 鱼饵（乘法累积）
        # 使用对数压缩避免概率过高，保持高品质鱼的稀有性
        quality_bonus = False
        quality_level = 0  # 默认普通品质
        if quality_modifier > 1.0:
            # 对数压缩公式：处理乘法累积的品质加成
            # log2(x) 特性：log2(1)=0, log2(2)=1, log2(4)=2
            # 天然适合处理乘法累积：log2(a×b) = log2(a) + log2(b)
            log_value = math.log2(quality_modifier)
            
            # 从配置获取高品质鱼最大触发概率，默认35%
            max_quality_chance = self.config.get("quality_bonus_max_chance", 0.35)
            
            # 缩放到配置的上限，让 quality_modifier=4.0 时达到上限
            # 缩放系数 = max_chance / 2（因为 log2(4) = 2）
            scale_factor = max_quality_chance / 2.0
            adjusted_chance = log_value * scale_factor
            
            # 确保不超过配置的上限，避免高品质鱼过于常见
            final_chance = min(adjusted_chance, max_quality_chance)
            
            quality_bonus = random.random() <= final_chance
        if quality_bonus:
            extra_weight = random.randint(fish_template.min_weight, fish_template.max_weight)
            weight += extra_weight
            # 标记为高品质鱼，价值在出售时按2倍计算
            quality_level = 1

        # 4.3 按数量加成决定额外渔获数量
        total_catches = 1
        if quantity_modifier > 1.0:
            # 整数部分-1 为保证的额外数量；小数部分为额外+1的概率
            guaranteed_extra = max(0, int(quantity_modifier) - 1)
            total_catches += guaranteed_extra
            fractional = quantity_modifier - int(quantity_modifier)
            if fractional > 0 and random.random() < fractional:
                total_catches += 1

        # 5. 处理鱼塘容量（在确定总渔获量后）
        user_fish_inventory = self.inventory_repo.get_fish_inventory(user.user_id)
        current_fish_count = sum(item.quantity for item in user_fish_inventory)
        
        # 计算放入新鱼后是否会溢出，以及溢出多少
        overflow_amount = (current_fish_count + total_catches) - user.fish_pond_capacity

        if overflow_amount > 0:
            # 鱼塘空间不足，需要移除 `overflow_amount` 条鱼
            # 采用循环随机移除的策略，确保腾出足够空间
            for _ in range(overflow_amount):
                # 每次循环都重新获取一次库存，防止某个种类的鱼被移除完
                current_inventory_for_removal = self.inventory_repo.get_fish_inventory(user.user_id)
                if not current_inventory_for_removal:
                    break # 如果鱼塘已经空了，就停止移除
                
                # 随机选择一个鱼种（堆叠）来移除
                random_fish_stack = random.choice(current_inventory_for_removal)
                self.inventory_repo.update_fish_quantity(
                    user.user_id,
                    random_fish_stack.fish_id,
                    -1
                )

        if fish_template.rarity >= 4:
            # 如果是4星及以上稀有鱼，增加用户的稀有鱼捕获计数
            zone = self.inventory_repo.get_zone_by_id(user.fishing_zone_id)
            if zone:
                zone.rare_fish_caught_today += 1
                self.inventory_repo.update_fishing_zone(zone)

        # 6. 更新数据库
        self.inventory_repo.add_fish_to_inventory(user.user_id, fish_template.fish_id, quantity=total_catches, quality_level=quality_level)

        # 更新用户统计数据
        user.total_fishing_count += total_catches
        user.total_weight_caught += weight
        # 高品质鱼的统计价值按双倍计算
        if quality_level == 1:
            user.total_coins_earned += fish_template.base_value * total_catches * 2
        else:
            user.total_coins_earned += fish_template.base_value * total_catches
        user.last_fishing_time = get_now()
        
        # 处理装备耐久度消耗
        equipment_broken_messages = []

        # 判断用户的鱼竿是否存在并处理耐久度
        if user.equipped_rod_instance_id:
            rod_instance = self.inventory_repo.get_user_rod_instance_by_id(user.user_id, user.equipped_rod_instance_id)
            if not rod_instance:
                user.equipped_rod_instance_id = None
            else:
                # 减少鱼竿耐久度（仅当为有限耐久时）
                if rod_instance.current_durability is not None and rod_instance.current_durability > 0:
                    rod_instance.current_durability -= 1
                    self.inventory_repo.update_rod_instance(rod_instance)

                # 无论是刚减为0，还是之前就是0，都进行一次破损检查与卸下，保证一致性
                if rod_instance.current_durability is not None and rod_instance.current_durability <= 0:
                    # 鱼竿损坏，自动卸下（同步 user 与实例 is_equipped 状态）
                    user.equipped_rod_instance_id = None
                    # 统一使用仓储方法重置装备状态，避免前端/状态页不一致
                    self.inventory_repo.set_equipment_status(
                        user.user_id,
                        rod_instance_id=None,
                        accessory_instance_id=user.equipped_accessory_instance_id
                    )
                    rod_template = self.item_template_repo.get_rod_by_id(rod_instance.rod_id)
                    rod_name = rod_template.name if rod_template else "鱼竿"
                    equipment_broken_messages.append(f"⚠️ 您的{rod_name}已损坏，自动卸下！")
        
        # 判断用户的饰品是否存在（饰品暂时不消耗耐久度）
        if user.equipped_accessory_instance_id:
            accessory_instance = self.inventory_repo.get_user_accessory_instance_by_id(user.user_id, user.equipped_accessory_instance_id)
            if not accessory_instance:
                user.equipped_accessory_instance_id = None

        # 更新用户信息
        self.user_repo.update(user)

        # 记录日志
        record = FishingRecord(
            record_id=0, # DB自增
            user_id=user.user_id,
            fish_id=fish_template.fish_id,
            weight=weight,
            value=value,
            timestamp=user.last_fishing_time,
            rod_instance_id=user.equipped_rod_instance_id,
            accessory_instance_id=user.equipped_accessory_instance_id,
            bait_id=user.current_bait_id
        )
        self.log_repo.add_fishing_record(record)

        # 7. 构建成功返回结果
        result = {
            "success": True,
            "fish": {
                "name": fish_template.name,
                "rarity": fish_template.rarity,
                "weight": weight,
                "value": value * 2 if quality_level == 1 else value,  # 高品质鱼双倍价值
                "quality_level": quality_level,  # 添加品质等级
                "quality_label": "✨高品质" if quality_level == 1 else "普通"  # 添加品质标签
            }
        }
        
        # 添加装备损坏消息
        if equipment_broken_messages:
            result["equipment_broken_messages"] = equipment_broken_messages
        
        return result

    def get_user_pokedex(self, user_id: str) -> Dict[str, Any]:
        """获取用户的图鉴信息。"""
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        # 使用聚合统计作为图鉴数据来源
        stats = self.log_repo.get_user_fish_stats(user_id)
        if not stats:
            return {"success": True, "pokedex": []}
        all_fish_count = len(self.item_template_repo.get_all_fish())
        unlock_fish_count = len(stats)
        pokedex = []
        for stat in stats:
            fish_template = self.item_template_repo.get_fish_by_id(stat.fish_id)
            if fish_template:
                pokedex.append({
                    "fish_id": stat.fish_id,
                    "name": fish_template.name,
                    "rarity": fish_template.rarity,
                    "description": fish_template.description,
                    "value": fish_template.base_value,
                    "icon_url": fish_template.icon_url,
                    "first_caught_time": stat.first_caught_at,
                    "last_caught_time": stat.last_caught_at,
                    "max_weight": stat.max_weight,
                    "min_weight": stat.min_weight,
                    "total_caught": stat.total_caught,
                    "total_weight": stat.total_weight,
                })
        # 将图鉴按稀有度从大到小排序
        pokedex.sort(key=lambda x: x["rarity"], reverse=True)
        return {
            "success": True,
            "pokedex": pokedex,
            "total_fish_count": all_fish_count,
            "unlocked_fish_count": unlock_fish_count,
            "unlocked_percentage": (unlock_fish_count / all_fish_count) if all_fish_count > 0 else 0
    }

    def get_user_fish_log(self, user_id: str, limit: int = 10) -> Dict[str, Any]:
        """
        获取用户的钓鱼记录。

        Args:
            user_id: 用户ID。
            limit: 返回记录的数量限制。

        Returns:
            包含钓鱼记录的字典。
        """
        records = self.log_repo.get_fishing_records(user_id, limit)
        # 根据records中的 fish_id 获取鱼类名称 rod_instance_id 和 accessory_instance_id 以及 bait_id 获取鱼竿、饰品、鱼饵信息
        fish_details = []
        for record in records:
            fish_template = self.item_template_repo.get_fish_by_id(record.fish_id)
            bait_template = self.item_template_repo.get_bait_by_id(record.bait_id) if record.bait_id else None

            user_rod = self.inventory_repo.get_user_rod_instance_by_id(user_id, record.rod_instance_id) if record.rod_instance_id else None
            rod_instance = self.item_template_repo.get_rod_by_id(user_rod.rod_id) if user_rod else None
            user_accessory = self.inventory_repo.get_user_accessory_instance_by_id(user_id, record.accessory_instance_id) if record.accessory_instance_id else None
            accessory_instance = self.item_template_repo.get_accessory_by_id(user_accessory.accessory_id) if user_accessory else None

            fish_details.append({
                "fish_name": fish_template.name if fish_template else "未知鱼类",
                "fish_rarity": fish_template.rarity if fish_template else "未知稀有度",
                "fish_weight": record.weight,
                "fish_value": record.value,
                "timestamp": record.timestamp,
                "rod": rod_instance.name if rod_instance else "未装备鱼竿",
                "accessory": accessory_instance.name if accessory_instance else "未装备饰品",
                "bait": bait_template.name if bait_template else "未使用鱼饵",
            })
        return {
            "success": True,
            "records": fish_details
        }

    def get_user_fishing_zones(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的钓鱼区域信息。

        Args:
            user_id: 用户ID。

        Returns:
            包含钓鱼区域信息的字典。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        fishing_zones = self.inventory_repo.get_all_zones()
        zones_info = []
        
        for zone in fishing_zones:
            # 获取通行证道具名称
            required_item_name = None
            if zone.requires_pass and zone.required_item_id:
                item_template = self.item_template_repo.get_item_by_id(zone.required_item_id)
                required_item_name = item_template.name if item_template else f"道具ID{zone.required_item_id}"
            
            zones_info.append({
                "zone_id": zone.id,
                "name": zone.name,
                "description": zone.description,
                "daily_rare_fish_quota": zone.daily_rare_fish_quota,
                "rare_fish_caught_today": zone.rare_fish_caught_today,
                "whether_in_use": zone.id == user.fishing_zone_id,
                "is_active": zone.is_active,
                "requires_pass": zone.requires_pass,
                "required_item_id": zone.required_item_id,
                "required_item_name": required_item_name,
                "fishing_cost": zone.fishing_cost,
                "available_from": zone.available_from,
                "available_until": zone.available_until,
            })

        return {
            "success": True,
            "zones": zones_info
        }

    def _apply_rare_chance_to_distribution(self, distribution: list, rare_chance: float) -> list:
        """
        应用稀有度加成，调整鱼类稀有度分布权重。
        
        设计理念：
        - 装备/Buff/鱼饵的稀有度加成影响 4-5 星鱼（稀有鱼）的概率
        - 6+ 星鱼（超稀有/传说鱼）保持纯运气机制，不受装备影响
        - 通过从低星转移权重到中高星，确保概率总和始终为 1
        
        实现原理：
        1. 从 1-3 星的总权重中，按 rare_chance 比例转移部分权重
        2. 将转移的权重分配给 4-5 星，按其原始比例分配
        3. 6+ 星的概率保持不变，保证超稀有鱼的珍贵性
        
        示例效果（rare_chance = 0.46）：
        - 原始: 1-3星 60%, 4-5星 38%, 6+星 2%
        - 调整后: 1-3星 32%, 4-5星 66%, 6+星 2%（不变）
        
        Args:
            distribution: 原始稀有度分布列表 [1星, 2星, 3星, 4星, 5星, 6+星]
            rare_chance: 稀有度加成值，通常在 0.0-0.8 之间
        
        Returns:
            调整后的稀有度分布列表，概率总和为 1
        """
        if len(distribution) < 6:
            # 安全检查：如果分布数组长度不足，直接返回副本
            return distribution.copy()
        
        # 转换系数：1.0 表示 rare_chance 直接作为权重转移比例
        # 例如 rare_chance=0.46 → 从低星转移 46% 的权重到中高星
        TRANSFER_FACTOR = 1.0
        
        actual_boost = rare_chance * TRANSFER_FACTOR
        
        # 限制上限为 0.8，防止低星概率被转移到接近 0 导致游戏体验失衡
        actual_boost = min(actual_boost, 0.8)
        
        new_distribution = distribution.copy()
        
        # 分组计算：
        # - 低星（1-3星，索引 0-2）：普通鱼，作为权重来源
        # - 中高星（4-5星，索引 3-4）：稀有鱼，接收权重转移
        # - 超稀有（6+星，索引 5）：传说鱼，不参与计算以保持稀有性
        low_star_total = sum(new_distribution[:3])
        mid_high_star_total = sum(new_distribution[3:5])
        
        # 边界情况：如果某一组概率为 0，则无法进行权重转移
        if mid_high_star_total <= 0 or low_star_total <= 0:
            return new_distribution
        
        # 计算转移量：从低星总权重中按比例转移
        # 例如：低星总权重 60%，rare_chance 46% → 转移 27.6% 的绝对权重
        transfer_amount = low_star_total * actual_boost
        
        # 步骤 1：从低星（1-3星）按原始比例扣减权重
        # 保持各低星之间的相对比例不变，整体权重减少
        for i in range(3):
            if low_star_total > 0:
                ratio = new_distribution[i] / low_star_total
                new_distribution[i] = max(0, new_distribution[i] - transfer_amount * ratio)
        
        # 步骤 2：向中高星（4-5星）按原始比例分配转移的权重
        # 保持 4星和 5星之间的相对比例不变，整体权重增加
        for i in range(3, 5):
            if mid_high_star_total > 0:
                ratio = new_distribution[i] / mid_high_star_total
                new_distribution[i] = new_distribution[i] + transfer_amount * ratio
        
        # 步骤 3：6+星（索引 5）完全不参与上述计算，保持原值
        # 这确保了超稀有鱼的概率不受装备影响，维持其珍贵性和神秘感
        
        # 归一化处理：确保所有概率之和精确为 1.0
        # 这是必要的，因为浮点运算可能产生微小误差
        new_distribution = [x / sum(new_distribution) for x in new_distribution]
        
        return new_distribution

    def _get_fish_template(self, rarity: int, zone: FishingZone, coins_chance: float):
        """根据稀有度和区域配置获取鱼类模板"""
        
        # 检查 FishingZone 对象是否有 'specific_fish_ids' 属性
        specific_fish_ids = getattr(zone, 'specific_fish_ids', [])

        if specific_fish_ids:
            # 如果是区域限定鱼，那么就在限定的鱼里面抽
            fish_list = [self.item_template_repo.get_fish_by_id(fish_id) for fish_id in specific_fish_ids]
            fish_list = [fish for fish in fish_list if fish and fish.rarity == rarity]
        else:
            # 否则就在全局鱼里面抽
            fish_list = self.item_template_repo.get_fishes_by_rarity(rarity)

        if not fish_list:
            # 如果限定鱼或全局鱼列表为空，则从所有鱼中随机抽取一条
            return self.item_template_repo.get_random_fish(rarity)

        return self.fish_weight_service.choose_fish(fish_list, coins_chance) # <--- 改为使用注入的服务

    def _get_random_high_rarity(self, zone: FishingZone = None) -> int:
        """从6星及以上鱼类中随机选择一个稀有度，兼容区域限定鱼"""
        # 检查是否有区域限定鱼
        specific_fish_ids = getattr(zone, 'specific_fish_ids', []) if zone else []
        
        if specific_fish_ids:
            # 如果是区域限定鱼，只在限定鱼中查找高星级
            fish_list = [self.item_template_repo.get_fish_by_id(fish_id) for fish_id in specific_fish_ids]
            fish_list = [fish for fish in fish_list if fish]
        else:
            # 否则在全局鱼池中查找
            fish_list = self.item_template_repo.get_all_fish()
        
        # 找出所有6星及以上的稀有度
        high_rarities = set()
        for fish in fish_list:
            if fish.rarity >= 6:
                high_rarities.add(fish.rarity)
        
        if not high_rarities:
            # 如果没有6星及以上的鱼，返回5星
            return 5
            
        # 从高稀有度中随机选择一个
        return random.choice(list(high_rarities))

    def set_user_fishing_zone(self, user_id: str, zone_id: int) -> Dict[str, Any]:
        """
        设置用户的钓鱼区域。

        Args:
            user_id: 用户ID。
            zone_id: 要设置的钓鱼区域ID。

        Returns:
            包含操作结果的字典。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        zone = self.inventory_repo.get_zone_by_id(zone_id)
        if not zone:
            return {"success": False, "message": "钓鱼区域不存在"}

        # 检查区域是否激活
        if not zone.is_active:
            return {"success": False, "message": "该钓鱼区域暂未开放"}

        # 检查时间限制
        now = get_now()
        if zone.available_from and now < zone.available_from:
            return {"success": False, "message": f"该钓鱼区域将在 {zone.available_from.strftime('%Y-%m-%d %H:%M')} 开放"}
        
        if zone.available_until and now > zone.available_until:
            return {"success": False, "message": f"该钓鱼区域已于 {zone.available_until.strftime('%Y-%m-%d %H:%M')} 关闭"}

        # 检查通行证要求（从数据库读取）
        pass_consumed = False
        consumed_item_name = None
        if zone.requires_pass and zone.required_item_id:
            # 获取用户道具库存
            user_items = self.inventory_repo.get_user_item_inventory(user_id)
            current_quantity = user_items.get(zone.required_item_id, 0)
            
            if current_quantity < 1:
                # 获取道具名称用于显示
                item_template = self.item_template_repo.get_item_by_id(zone.required_item_id)
                item_name = item_template.name if item_template else f"道具ID{zone.required_item_id}"
                return {
                    "success": False, 
                    "message": f"❌ 进入该区域需要 {item_name}，您当前拥有 {current_quantity} 个"
                }
            
            # 消耗一个通行证道具
            self.inventory_repo.decrease_item_quantity(user_id, zone.required_item_id, 1)
            
            # 获取道具名称用于提示
            item_template = self.item_template_repo.get_item_by_id(zone.required_item_id)
            consumed_item_name = item_template.name if item_template else f"道具ID{zone.required_item_id}"
            pass_consumed = True
            
            # 记录日志
            self.log_repo.add_log(user_id, "zone_entry", f"使用通行证进入 {zone.name}")

        user.fishing_zone_id = zone.id
        self.user_repo.update(user)

        # 构建成功消息
        success_message = f"✅已将钓鱼区域设置为 {zone.name}"
        if pass_consumed and consumed_item_name:
            success_message += f"\n🔑 已消耗 1 个 {consumed_item_name}"

        return {"success": True, "message": success_message}

    def apply_daily_taxes(self) -> None:
        """对所有高价值用户征收每日税收。逐用户检查，确保不遗漏也不重复征收。"""
        import uuid
        
        # 生成执行ID用于追踪和调试
        execution_id = uuid.uuid4().hex[:8]
        
        tax_config = self.config.get("tax", {})
        if tax_config.get("is_tax", False) is False:
            logger.info(f"[税收-{execution_id}] 税收功能未启用，跳过")
            return
        
        logger.info(f"[税收-{execution_id}] 开始检查每日资产税（执行ID: {execution_id}）")
        
        threshold = tax_config.get("threshold", 1000000)
        step_coins = tax_config.get("step_coins", 1000000)
        step_rate = tax_config.get("step_rate", 0.01)
        min_rate = tax_config.get("min_rate", 0.001)
        max_rate = tax_config.get("max_rate", 0.2)
        
        logger.info(f"[税收-{execution_id}] 税收配置：起征点={threshold}, 步长={step_coins}, 步长税率={step_rate*100}%, 最小税率={min_rate*100}%, 最大税率={max_rate*100}%")

        high_value_users = self.user_repo.get_high_value_users(threshold)
        logger.info(f"[税收-{execution_id}] 检测到 {len(high_value_users)} 个达到税收阈值的用户，开始逐个检查")
        
        total_tax_collected = 0
        taxed_user_count = 0
        skipped_user_count = 0

        for user in high_value_users:
            # 检查该用户今天是否已经被征收过税
            if self.log_repo.has_user_daily_tax_today(user.user_id, self.daily_reset_hour):
                logger.debug(f"[税收-{execution_id}] 用户 {user.user_id} 今日已缴税，跳过")
                skipped_user_count += 1
                continue
            
            tax_rate = 0.0
            # 根据资产确定税率
            if user.coins >= threshold:
                steps = (user.coins - threshold) // step_coins
                tax_rate = min_rate + steps * step_rate
                if tax_rate > max_rate:
                    tax_rate = max_rate
            min_tax_amount = 1
            if tax_rate > 0:
                tax_amount = max(int(user.coins * tax_rate), min_tax_amount)
                original_coins = user.coins
                user.coins -= tax_amount

                self.user_repo.update(user)

                tax_log = TaxRecord(
                    tax_id=0, # DB会自增
                    user_id=user.user_id,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    original_amount=original_coins,
                    balance_after=user.coins,
                    timestamp=get_now(),
                    tax_type="每日资产税"
                )
                self.log_repo.add_tax_record(tax_log)
                
                total_tax_collected += tax_amount
                taxed_user_count += 1
        
        logger.info(f"[税收-{execution_id}] 每日资产税执行完成，征税 {taxed_user_count} 人，跳过 {skipped_user_count} 人（已缴税），总计 {total_tax_collected} 金币")

    def enforce_zone_pass_requirements_for_all_users(self) -> None:
        """
        每日检查：若用户当前所在钓鱼区域需要通行证，但其背包中已无对应道具，
        则将用户传送回 1 号钓鱼地，并通过群聊@通知相关玩家。
        """
        logger.info("开始执行每日区域通行证检查...")
        try:
            all_user_ids = self.user_repo.get_all_user_ids()
            logger.info(f"找到 {len(all_user_ids)} 个用户需要检查")
        except Exception as e:
            logger.error(f"获取用户列表失败: {e}")
            return

        relocated_users = []  # 存储被传送的用户信息

        for user_id in all_user_ids:
            try:
                user = self.user_repo.get_by_id(user_id)
                if not user:
                    continue

                zone = self.inventory_repo.get_zone_by_id(user.fishing_zone_id)
                if not zone or not getattr(zone, "requires_pass", False) or not getattr(zone, "required_item_id", None):
                    continue

                user_items = self.inventory_repo.get_user_item_inventory(user_id)
                current_quantity = user_items.get(zone.required_item_id, 0) if user_items else 0

                if current_quantity < 1:
                    # 移动到 1 号区域
                    user.fishing_zone_id = 1
                    self.user_repo.update(user)
                    
                    # 记录日志
                    try:
                        item_template = self.item_template_repo.get_item_by_id(zone.required_item_id)
                        item_name = item_template.name if item_template else f"道具ID{zone.required_item_id}"
                    except Exception:
                        item_name = f"道具ID{zone.required_item_id}"
                    self.log_repo.add_log(user_id, "zone_relocation", f"缺少 {item_name}，已被传送至 1 号钓鱼地")
                    
                    # 收集被传送用户信息
                    relocated_users.append({
                        "user_id": user_id,
                        "nickname": user.nickname,
                        "zone_name": zone.name,
                        "item_name": item_name
                    })
                else:
                    # 用户有道具，不需要重复扣除通行证
                    # 通行证只在切换区域时扣除一次，这里只做检查
                    try:
                        item_template = self.item_template_repo.get_item_by_id(zone.required_item_id)
                        item_name = item_template.name if item_template else f"道具ID{zone.required_item_id}"
                    except Exception:
                        item_name = f"道具ID{zone.required_item_id}"
                    
                    # 记录日志（不扣除道具）
                    self.log_repo.add_log(user_id, "zone_access_check", f"检查 {item_name} 剩余数量：{current_quantity}，继续留在 {zone.name}")
            except Exception:
                # 单个用户异常不影响其他用户
                continue

        # 记录检查结果
        logger.info(f"每日检查完成：{len(relocated_users)} 个用户被传送（缺少通行证）")
        
        # 记录被传送用户信息（不发送通知，避免凌晨打扰玩家）
        if relocated_users:
            logger.info(f"被传送用户详情：{relocated_users}")

    def _reset_rare_fish_daily_quota(self) -> bool:
        """
        检查并重置所有区域的稀有鱼每日配额计数。
        
        使用快速路径检查模式优化性能：
        1. 快速路径：无锁检查时间，如果不需要重置直接返回（99.9%的情况）
        2. 慢速路径：加锁后再次确认（double-check），避免并发问题
        
        Returns:
            bool: 如果执行了重置返回 True，否则返回 False
        """
        # 快速路径：无锁检查，避免大多数情况下的锁竞争
        current_reset_time = get_last_reset_time(self.daily_reset_hour)
        if current_reset_time == self.last_reset_time:
            # 不需要重置，直接返回（99.9%的情况）
            return False
        
        # 慢速路径：可能需要重置，获取锁后再次确认（double-check pattern）
        with self.rare_fish_reset_lock:
            # 再次检查，防止在获取锁的过程中其他线程已经执行了重置
            current_reset_time = get_last_reset_time(self.daily_reset_hour)
            if current_reset_time != self.last_reset_time:
                # 如果刷新时间点变了，执行每日重置任务
                logger.info(f"检测到刷新时间点变更（每日{self.daily_reset_hour}点刷新），从 {self.last_reset_time} 到 {current_reset_time}，开始执行稀有鱼配额重置...")
                self.last_reset_time = current_reset_time
                
                # 重置所有受配额限制区域的稀有鱼计数（4星及以上）
                all_zones = self.inventory_repo.get_all_zones()
                reset_count = 0
                for zone in all_zones:
                    if zone.daily_rare_fish_quota > 0:  # 只重置有配额的区域
                        zone.rare_fish_caught_today = 0
                        self.inventory_repo.update_fishing_zone(zone)
                        reset_count += 1
                
                logger.info(f"稀有鱼配额重置完成，共重置 {reset_count} 个区域的计数")
                return True
        
        return False

    def start_auto_fishing_task(self):
        """启动自动钓鱼的后台线程。"""
        if self.auto_fishing_thread and self.auto_fishing_thread.is_alive():
            logger.info("自动钓鱼线程已在运行中")
            return

        self.auto_fishing_running = True
        self.auto_fishing_thread = threading.Thread(target=self._auto_fishing_loop, daemon=True)
        self.auto_fishing_thread.start()
        logger.info("自动钓鱼线程已启动")

    def stop_auto_fishing_task(self):
        """停止自动钓鱼的后台线程。"""
        self.auto_fishing_running = False
        if self.auto_fishing_thread:
            self.auto_fishing_thread.join(timeout=1.0)
            logger.info("自动钓鱼线程已停止")

    def start_daily_tax_task(self):
        """启动每日税收的独立后台线程。"""
        # 使用锁确保线程创建检查和创建操作的原子性，防止重复创建线程
        with self.tax_start_lock:
            if self.tax_thread and self.tax_thread.is_alive():
                logger.info("税收线程已在运行中")
                return

            logger.info("正在启动每日税收线程...")
            self.tax_running = True
            self.tax_thread = threading.Thread(target=self._daily_tax_loop, daemon=True)
            self.tax_thread.start()
            logger.info(f"税收线程已启动，每日重置时间点：{self.daily_reset_hour}点")

    def stop_daily_tax_task(self):
        """停止每日税收的后台线程。"""
        self.tax_running = False
        if self.tax_thread:
            self.tax_thread.join(timeout=1.0)
            logger.info("税收线程已停止")

    def _daily_tax_loop(self):
        """每日税收独立循环任务，由后台线程执行。"""
        try:
            logger.info(f"[税收线程] 线程已进入运行循环，每日重置时间点：{self.daily_reset_hour}点")
            logger.info(f"[税收线程] 上次税收重置时间：{self.last_tax_reset_time}")
        except Exception as e:
            logger.error(f"[税收线程] 初始化日志输出失败: {e}")
        
        # 立即执行第一次检查，避免在重置时间点后重启时错过当天的税收
        first_check = True
        
        while self.tax_running:
            try:
                # 第一次检查不sleep，之后每小时检查一次
                if not first_check:
                    time.sleep(3600)
                
                # 检查是否到达每日重置时间点
                current_reset_time = get_last_reset_time(self.daily_reset_hour)
                
                # 判断是否需要执行税收检查：
                # 1. 时间点变更（跨天了）- 新的一天开始，需要检查所有用户
                # 2. 或者首次启动 - 检查是否有遗漏的用户（逐用户检查会自动跳过已缴税的用户）
                should_execute = False
                
                if current_reset_time != self.last_tax_reset_time:
                    # 时间点变更，新的一天开始
                    logger.info(f"[税收线程] 检测到刷新时间点变更（每日{self.daily_reset_hour}点刷新），从 {self.last_tax_reset_time} 到 {current_reset_time}")
                    should_execute = True
                    self.last_tax_reset_time = current_reset_time
                elif first_check:
                    # 首次检查，检查是否有遗漏的用户（逐用户检查会自动避免重复扣税）
                    logger.info(f"[税收线程] 首次检查，将检查所有高资产用户的缴税情况（已缴税用户会自动跳过）")
                    should_execute = True
                
                # 首次检查完成后，标记为非首次
                first_check = False
                
                if should_execute:
                    # 使用锁来防止并发执行税收（多层防护的第一层）
                    with self.tax_execution_lock:
                        logger.info("[税收线程] 已获取税收执行锁，开始执行税收")
                        self.apply_daily_taxes()
                        logger.info("[税收线程] 每日税收执行完成，释放锁")
                
            except Exception as e:
                logger.error(f"[税收线程] 出错: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(600)  # 出错后等待10分钟再重试
        
        logger.info("[税收线程] 线程循环已退出")

    def _auto_fishing_loop(self):
        """自动钓鱼循环任务，由后台线程执行。"""
        fishing_config = self.config.get("fishing", {})
        cooldown = fishing_config.get("cooldown_seconds", 180)
        cost = fishing_config.get("cost", 10)

        while self.auto_fishing_running:
            try:
                # 检查并执行每日重置（如果需要）
                if self._reset_rare_fish_daily_quota():
                    # 如果执行了重置，说明是新的一天，执行其他每日任务
                    logger.info("自动钓鱼线程检测到新的一天，开始执行每日任务...")
                    
                    # 注意：每日税收已由独立的税收线程处理，不再在此执行
                    
                    # 每日检查：需要通行证的区域玩家是否仍持有通行证
                    self.enforce_zone_pass_requirements_for_all_users()
                
                # 获取所有开启自动钓鱼的用户
                auto_users_ids = self.user_repo.get_all_user_ids(auto_fishing_only=True)

                for user_id in auto_users_ids:
                    user = self.user_repo.get_by_id(user_id)
                    if not user:
                        continue

                    # 检查CD
                    now_ts = get_now().timestamp()
                    last_ts = 0
                    if user.last_fishing_time and user.last_fishing_time.year > 1:
                        last_ts = user.last_fishing_time.timestamp()
                    elif user.last_fishing_time and user.last_fishing_time.year <= 1:
                        # 若 last_fishing_time 被重置为极早时间，将时间设为当前时间减去冷却时间，
                        # 这样下一轮自动钓鱼就能正常工作了
                        cooldown = fishing_config.get("cooldown_seconds", 180)
                        user.last_fishing_time = get_now() - timedelta(seconds=cooldown)
                        self.user_repo.update(user)
                        last_ts = user.last_fishing_time.timestamp()
                    # 检查用户是否装备了海洋之心
                    _cooldown = cooldown
                    equipped_accessory = self.inventory_repo.get_user_equipped_accessory(user_id)
                    if equipped_accessory:
                        accessory_template = self.item_template_repo.get_accessory_by_id(equipped_accessory.accessory_id)
                        if accessory_template and accessory_template.name == "海洋之心":
                            # 海洋之心装备时，CD时间减半
                            _cooldown /= 2
                    if now_ts - last_ts < _cooldown:
                        continue # CD中，跳过

                    # 检查成本（从区域配置中读取）
                    zone = self.inventory_repo.get_zone_by_id(user.fishing_zone_id)
                    if not zone:
                        continue
                    fishing_cost = zone.fishing_cost
                    if not user.can_afford(fishing_cost):
                        # 金币不足，关闭其自动钓鱼
                        user.auto_fishing_enabled = False
                        self.user_repo.update(user)
                        logger.warning(f"用户 {user_id} 金币不足（需要 {fishing_cost} 金币），已关闭自动钓鱼")
                        continue

                    # 执行钓鱼
                    result = self.go_fish(user_id)
                    
                    # 检查是否因为区域关闭被传送
                    if result and not result.get("success") and "已自动传送回" in result.get("message", ""):
                        # 区域关闭，给用户发送通知
                        try:
                            if self._notifier:
                                self._notifier(user_id, f"🌅 {result['message']}")
                        except Exception:
                            # 通知失败不影响主流程
                            pass
                    
                    # 自动钓鱼时，如装备损坏，尝试进行消息推送
                    if result and result.get("equipment_broken_messages"):
                        for msg in result["equipment_broken_messages"]:
                            try:
                                if self._notifier:
                                    self._notifier(user_id, msg)
                            except Exception:
                                # 通知失败不影响主流程
                                pass
                    # if result['success']:
                    #     fish = result["fish"]
                    #     logger.info(f"用户 {user_id} 自动钓鱼成功: {fish['name']}")
                    # else:
                    #      logger.info(f"用户 {user_id} 自动钓鱼失败: {result['message']}")

                # 每轮检查间隔
                time.sleep(40)

            except Exception as e:
                logger.error(f"自动钓鱼任务出错: {e}")
                # 打印堆栈信息
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(60)