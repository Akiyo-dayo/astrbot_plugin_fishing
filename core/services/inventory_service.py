import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# 导入仓储接口和领域模型
from ..repositories.abstract_repository import (
    AbstractInventoryRepository,
    AbstractUserRepository,
    AbstractItemTemplateRepository,
)
from .effect_manager import EffectManager
from ..utils import calculate_after_refine
from .game_mechanics_service import GameMechanicsService


class InventoryService:
    """封装与用户库存相关的业务逻辑"""

    def __init__(
        self,
        inventory_repo: AbstractInventoryRepository,
        user_repo: AbstractUserRepository,
        item_template_repo: AbstractItemTemplateRepository,
        effect_manager: EffectManager,
        game_mechanics_service: GameMechanicsService,
        config: Dict[str, Any],
    ):
        self.inventory_repo = inventory_repo
        self.user_repo = user_repo
        self.item_template_repo = item_template_repo
        self.effect_manager = effect_manager
        self.game_mechanics_service = game_mechanics_service
        self.config = config

    # === 短码解析 ===
    def _to_base36(self, n: int) -> str:
        if n < 0:
            raise ValueError("n must be non-negative")
        if n == 0:
            return "0"
        digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        out = []
        while n:
            n, rem = divmod(n, 36)
            out.append(digits[rem])
        return "".join(reversed(out))

    def _from_base36(self, s: str) -> int:
        s = (s or "").strip().upper()
        if not s:
            raise ValueError("empty")
        return int(s, 36)

    def resolve_rod_instance_id(self, user_id: str, token: str) -> Optional[int]:
        """将短码(Rxxxx)解析为 rod_instance_id。大小写不敏感。"""
        if token is None:
            return None
        tok = str(token).strip()
        code = tok.upper()
        if not code.startswith("R"):
            return None
        try:
            return self._from_base36(code[1:])
        except Exception:
            return None

    def resolve_accessory_instance_id(self, user_id: str, token: str) -> Optional[int]:
        """将短码(Axxxx)解析为 accessory_instance_id。大小写不敏感。"""
        if token is None:
            return None
        tok = str(token).strip()
        code = tok.upper()
        if not code.startswith("A"):
            return None
        try:
            return self._from_base36(code[1:])
        except Exception:
            return None

    def get_user_fish_pond(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的鱼塘信息（鱼类库存）。
        """
        inventory_items = self.inventory_repo.get_fish_inventory(user_id)
        total_value = self.inventory_repo.get_fish_inventory_value(user_id)

        # 为了丰富信息，可以从模板仓储获取鱼的详细信息
        enriched_items = []
        for item in inventory_items:
            fish_template = self.item_template_repo.get_fish_by_id(item.fish_id)
            if fish_template:
                # 计算实际价值（高品质鱼双倍价值）
                actual_value = fish_template.base_value * (1 + item.quality_level)
                enriched_items.append({
                    "fish_id": item.fish_id,  # 添加fish_id字段
                    "name": fish_template.name,
                    "rarity": fish_template.rarity,
                    "base_value": fish_template.base_value,
                    "quantity": item.quantity,
                    "quality_level": item.quality_level,  # 添加品质等级
                    "actual_value": actual_value,  # 添加实际价值
                    "quality_label": "✨高品质" if item.quality_level == 1 else "普通"  # 添加品质标签
                })

        return {
            "success": True,
            "fishes": enriched_items,
            "stats": {
                "total_count": sum(item["quantity"] for item in enriched_items),
                "total_value": total_value
            }
        }

    def get_user_rod_inventory(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的鱼竿库存。
        """
        rod_instances = self.inventory_repo.get_user_rod_instances(user_id)
        enriched_rods = []

        for rod_instance in rod_instances:
            rod_template = self.item_template_repo.get_rod_by_id(rod_instance.rod_id)
            if rod_template:
                # 计算精炼后的最大耐久度
                if rod_template.durability is not None:
                    # 每级精炼增加前一级50%的耐久上限
                    refine_bonus_multiplier = (1.5 ** (rod_instance.refine_level - 1))
                    refined_max_durability = int(rod_template.durability * refine_bonus_multiplier)
                else:
                    refined_max_durability = None
                
                enriched_rods.append({
                    "name": rod_template.name,
                    "rarity": rod_template.rarity,
                    "instance_id": rod_instance.rod_instance_id,
                    "display_code": getattr(rod_instance, 'display_code', f"R{self._to_base36(rod_instance.rod_instance_id)}"),
                    "description": rod_template.description,
                    "is_equipped": rod_instance.is_equipped,
                    "is_locked": rod_instance.is_locked,
                    "bonus_fish_quality_modifier": calculate_after_refine(rod_template.bonus_fish_quality_modifier, refine_level= rod_instance.refine_level, rarity=rod_template.rarity),
                    "bonus_fish_quantity_modifier": calculate_after_refine(rod_template.bonus_fish_quantity_modifier, refine_level= rod_instance.refine_level, rarity=rod_template.rarity),
                    "bonus_rare_fish_chance": calculate_after_refine(rod_template.bonus_rare_fish_chance, refine_level= rod_instance.refine_level, rarity=rod_template.rarity),
                    "refine_level": rod_instance.refine_level,
                    "current_durability": rod_instance.current_durability,
                    "max_durability": refined_max_durability,
                })
        # 排序：装备的鱼竿优先显示，然后按稀有度降序，最后按精炼等级降序
        enriched_rods.sort(key=lambda x: (
            not x["is_equipped"],  # False (装备中) 排在前面
            -x["rarity"],          # 稀有度降序
            -x["refine_level"]     # 精炼等级降序
        ))
        
        return {
            "success": True,
            "rods": enriched_rods
        }

    def get_user_bait_inventory(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的鱼饵库存。
        """
        bait_inventory = self.inventory_repo.get_user_bait_inventory(user_id)
        enriched_baits = []

        for bait_id, quantity in bait_inventory.items():
            bait_template = self.item_template_repo.get_bait_by_id(bait_id)
            if bait_template:
                enriched_baits.append({
                    "bait_id": bait_id,
                    "name": bait_template.name,
                    "rarity": bait_template.rarity,
                    "quantity": quantity,
                    "duration_minutes": bait_template.duration_minutes,
                    "effect_description": bait_template.effect_description
                })

        return {
            "success": True,
            "baits": enriched_baits
        }

    def get_user_accessory_inventory(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的饰品库存。
        """
        accessory_instances = self.inventory_repo.get_user_accessory_instances(user_id)
        enriched_accessories = []

        for accessory_instance in accessory_instances:
            accessory_template = self.item_template_repo.get_accessory_by_id(accessory_instance.accessory_id)
            if accessory_template:
                enriched_accessories.append({
                    "name": accessory_template.name,
                    "rarity": accessory_template.rarity,
                    "instance_id": accessory_instance.accessory_instance_id,
                    "display_code": getattr(accessory_instance, 'display_code', f"A{self._to_base36(accessory_instance.accessory_instance_id)}"),
                    "description": accessory_template.description,
                    "is_equipped": accessory_instance.is_equipped,
                    "is_locked": accessory_instance.is_locked,
                    "bonus_fish_quality_modifier": calculate_after_refine(accessory_template.bonus_fish_quality_modifier, refine_level=accessory_instance.refine_level, rarity=accessory_template.rarity),
                    "bonus_fish_quantity_modifier": calculate_after_refine(accessory_template.bonus_fish_quantity_modifier, refine_level=accessory_instance.refine_level, rarity=accessory_template.rarity),
                    "bonus_rare_fish_chance": calculate_after_refine(accessory_template.bonus_rare_fish_chance, refine_level=accessory_instance.refine_level, rarity=accessory_template.rarity),
                    "bonus_coin_modifier": calculate_after_refine(accessory_template.bonus_coin_modifier, refine_level=accessory_instance.refine_level, rarity=accessory_template.rarity),
                    "refine_level": accessory_instance.refine_level,
                })

        # 排序：装备的饰品优先显示，然后按稀有度降序，最后按精炼等级降序
        enriched_accessories.sort(key=lambda x: (
            not x["is_equipped"],  # False (装备中) 排在前面
            -x["rarity"],          # 稀有度降序
            -x["refine_level"]     # 精炼等级降序
        ))
        
        return {
            "success": True,
            "accessories": enriched_accessories
        }

    def get_user_item_inventory(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户的道具库存。
        """
        item_inventory = self.inventory_repo.get_user_item_inventory(user_id)
        enriched_items = []

        for item_id, quantity in item_inventory.items():
            item_template = self.item_template_repo.get_item_by_id(item_id)
            if item_template:
                enriched_items.append({
                    "item_id": item_id,
                    "name": item_template.name,
                    "rarity": item_template.rarity,
                    "quantity": quantity,
                    "effect_description": item_template.effect_description,
                    "effect_type": item_template.effect_type,
                    "is_consumable": getattr(item_template, "is_consumable", False),
                })

        return {
            "success": True,
            "items": enriched_items
        }

    def sell_all_fish(self, user_id: str, keep_one: bool = False) -> Dict[str, Any]:
        """
        向系统出售鱼。

        Args:
            user_id: 用户ID
            keep_one: 是否每种鱼保留一条
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        # 获取用户的鱼库存
        fish_inventory = self.inventory_repo.get_fish_inventory(user_id)
        if not fish_inventory:
            return {"success": False, "message": "❌ 你没有可以卖出的鱼"}
        
        # 计算总价值（高品质鱼双倍价值）
        total_value = 0
        sold_details = {"普通": 0, "✨高品质": 0}
        
        for item in fish_inventory:
            fish_template = self.item_template_repo.get_fish_by_id(item.fish_id)
            if fish_template:
                # 高品质鱼按双倍价值计算
                item_value = fish_template.base_value * item.quantity * (1 + item.quality_level)
                total_value += item_value
                
                if item.quality_level == 1:
                    sold_details["✨高品质"] += item.quantity
                else:
                    sold_details["普通"] += item.quantity
        
        if keep_one:
            # 调用仓储方法执行"保留一条"的数据库操作
            sold_value = self.inventory_repo.sell_fish_keep_one(user_id)
        else:
            sold_value = total_value
            self.inventory_repo.clear_fish_inventory(user_id)

        # 更新用户金币
        user.coins += sold_value
        self.user_repo.update(user)

        # 构建详细消息
        message = f"💰 成功卖出鱼，获得 {sold_value} 金币"
        if sold_details["✨高品质"] > 0:
            message += f"\n📊 出售详情：普通鱼 {sold_details['普通']} 条，✨高品质鱼 {sold_details['✨高品质']} 条"

        return {"success": True, "message": message}

    def sell_fish_by_rarity(self, user_id: str, rarity: int) -> Dict[str, Any]:
        """
        向系统出售指定稀有度的鱼。

        Args:
            user_id: 用户ID
            rarity: 鱼的稀有度
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 获取用户的鱼库存
        fish_inventory = self.inventory_repo.get_fish_inventory(user_id)
        total_value = 0
        sold_details = {"普通": 0, "✨高品质": 0}

        for item in fish_inventory:
            fish_id = item.fish_id
            fish_info = self.item_template_repo.get_fish_by_id(fish_id)
            if fish_info and fish_info.rarity == rarity:
                # 计算鱼的总价值（高品质鱼双倍价值）
                item_value = fish_info.base_value * item.quantity * (1 + item.quality_level)
                total_value += item_value
                
                if item.quality_level == 1:
                    sold_details["✨高品质"] += item.quantity
                else:
                    sold_details["普通"] += item.quantity
                
        # 如果没有可卖出的鱼，返回提示
        if total_value == 0:
            return {"success": False, "message": "❌ 没有可卖出的鱼"}
        
        # 删除该稀有度的所有鱼（包括普通和高品质）
        self.inventory_repo.clear_fish_inventory(user_id, rarity=rarity)
        
        # 更新用户金币
        user.coins += total_value
        self.user_repo.update(user)

        # 构建详细消息
        message = f"💰 成功卖出稀有度 {rarity} 的鱼，获得 {total_value} 金币"
        if sold_details["✨高品质"] > 0:
            message += f"\n📊 出售详情：普通鱼 {sold_details['普通']} 条，✨高品质鱼 {sold_details['✨高品质']} 条"

        return {"success": True, "message": message}

    def sell_fish_by_rarities(self, user_id: str, rarities: list[int]) -> Dict[str, Any]:
        """
        向系统出售指定稀有度列表的鱼。

        Args:
            user_id: 用户ID
            rarities: 鱼的稀有度列表, e.g., [3, 4, 5]
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 1. 验证并去重稀有度列表
        if not rarities:
            return {"success": False, "message": "❌ 请指定要出售的稀有度"}
        
        unique_rarities = set(r for r in rarities if 1 <= r <= 10)
        if not unique_rarities:
            return {"success": False, "message": "❌ 请提供有效的稀有度（1-10之间）"}

        # 2. 获取用户全部鱼类库存
        fish_inventory = self.inventory_repo.get_fish_inventory(user_id)
        if not fish_inventory:
            return {"success": False, "message": "❌ 你的鱼塘是空的，没有任何鱼可以卖"}

        # 3. 计算总价值并记录详情
        total_value = 0
        sold_fish_details = {}  # 用于记录每个稀有度卖出的数量和价值

        for item in fish_inventory:
            fish_template = self.item_template_repo.get_fish_by_id(item.fish_id)
            if fish_template and fish_template.rarity in unique_rarities:
                # 高品质鱼按双倍价值计算
                value = fish_template.base_value * item.quantity * (1 + item.quality_level)
                total_value += value
                
                # 累加售出详情
                if fish_template.rarity not in sold_fish_details:
                    sold_fish_details[fish_template.rarity] = {'count': 0, 'value': 0, 'normal': 0, 'high_quality': 0}
                sold_fish_details[fish_template.rarity]['count'] += item.quantity
                sold_fish_details[fish_template.rarity]['value'] += value
                
                # 分别统计普通和高品质数量
                if item.quality_level == 1:
                    sold_fish_details[fish_template.rarity]['high_quality'] += item.quantity
                else:
                    sold_fish_details[fish_template.rarity]['normal'] += item.quantity

        # 4. 如果没有符合条件的鱼，提前返回
        if total_value == 0:
            rarity_str = ", ".join(map(str, sorted(list(unique_rarities))))
            return {"success": False, "message": f"❌ 你没有任何稀有度为【{rarity_str}】的鱼可以出售"}

        # 5. 执行数据库删除操作
        for rarity in unique_rarities:
            self.inventory_repo.clear_fish_inventory(user_id, rarity=rarity)

        # 6. 更新用户金币
        user.coins += total_value
        self.user_repo.update(user)

        # 7. 构建并返回成功的消息
        rarity_str_sold = ", ".join(map(str, sorted(sold_fish_details.keys())))
        message = f"💰 成功卖出稀有度为【{rarity_str_sold}】的鱼，共获得 {total_value} 金币。\n\n"
        message += "📊 出售详情：\n"
        for r in sorted(sold_fish_details.keys()):
            details = sold_fish_details[r]
            quality_info = ""
            if details['high_quality'] > 0:
                quality_info = f"（普通 {details['normal']} 条，✨高品质 {details['high_quality']} 条）"
            message += f" - 稀有度 {r}: {details['count']} 条{quality_info}，价值 {details['value']} 金币\n"

        return {"success": True, "message": message, "gained_coins": total_value}

    def sell_everything_except_locked(self, user_id: str) -> Dict[str, Any]:
        """
        砸锅卖铁：出售所有未锁定且未装备的鱼竿、饰品和全部鱼类
        保留当前装备的鱼竿和饰品，以及所有锁定的装备
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        total_value = 0
        sold_items = {
            "fish_count": 0,
            "fish_value": 0,
            "rod_count": 0,
            "rod_value": 0,
            "accessory_count": 0,
            "accessory_value": 0,
        }

        # 1. 卖出所有鱼类
        fish_inventory = self.inventory_repo.get_fish_inventory(user_id)
        for item in fish_inventory:
            fish_id = item.fish_id
            fish_info = self.item_template_repo.get_fish_by_id(fish_id)
            if fish_info:
                # 高品质鱼按双倍价值计算
                fish_value = fish_info.base_value * item.quantity * (1 + item.quality_level)
                total_value += fish_value
                sold_items["fish_count"] += item.quantity
                sold_items["fish_value"] += fish_value
        
        # 清空所有鱼类
        self.inventory_repo.clear_fish_inventory(user_id)

        # 2. 卖出所有未锁定且未装备的鱼竿
        rod_instances = self.inventory_repo.get_user_rod_instances(user_id)
        for rod_instance in rod_instances:
            # 只卖出未锁定且未装备的鱼竿
            if not rod_instance.is_locked and not rod_instance.is_equipped:
                rod_template = self.item_template_repo.get_rod_by_id(rod_instance.rod_id)
                if rod_template:
                    # 计算售价（基础价格 × 精炼倍数）
                    base_price = self.config["sell_prices"]["rod"].get(str(rod_template.rarity), 100)
                    refine_multiplier = self.config["sell_prices"]["refine_multiplier"].get(str(rod_instance.refine_level), 1.0)
                    rod_price = int(base_price * refine_multiplier)
                    
                    total_value += rod_price
                    sold_items["rod_count"] += 1
                    sold_items["rod_value"] += rod_price
                    
                    # 删除鱼竿实例
                    self.inventory_repo.delete_rod_instance(rod_instance.rod_instance_id)

        # 3. 卖出所有未锁定且未装备的饰品
        accessory_instances = self.inventory_repo.get_user_accessory_instances(user_id)
        for accessory_instance in accessory_instances:
            # 只卖出未锁定且未装备的饰品
            if not accessory_instance.is_locked and not accessory_instance.is_equipped:
                accessory_template = self.item_template_repo.get_accessory_by_id(accessory_instance.accessory_id)
                if accessory_template:
                    # 计算售价（基础价格 × 精炼倍数）
                    base_price = self.config["sell_prices"]["accessory"].get(str(accessory_template.rarity), 100)
                    refine_multiplier = self.config["sell_prices"]["refine_multiplier"].get(str(accessory_instance.refine_level), 1.0)
                    accessory_price = int(base_price * refine_multiplier)
                    
                    total_value += accessory_price
                    sold_items["accessory_count"] += 1
                    sold_items["accessory_value"] += accessory_price
                    
                    # 删除饰品实例
                    self.inventory_repo.delete_accessory_instance(accessory_instance.accessory_instance_id)

        # 更新用户金币（出售所得）
        user.coins += total_value
        self.user_repo.update(user)

        # 4. 自动消耗“钱袋”类道具（ADD_COINS），并统计获得金币
        coins_from_bags = self._auto_consume_money_bags(user)

        # 构造详细的结果消息
        if total_value == 0:
            return {"success": False, "message": "❌ 没有可出售的物品（可能全部被锁定或仓库为空）"}
        
        grand_total = total_value + coins_from_bags
        message = f"💥 砸锅卖铁完成！总共获得 {grand_total} 金币\n\n"
        message += "📊 出售详情：\n"
        
        if sold_items["fish_count"] > 0:
            message += f"🐟 鱼类：{sold_items['fish_count']} 条 (💰 {sold_items['fish_value']} 金币)\n"
        
        if sold_items["rod_count"] > 0:
            message += f"🎣 鱼竿：{sold_items['rod_count']} 根 (💰 {sold_items['rod_value']} 金币)\n"
        
        if sold_items["accessory_count"] > 0:
            message += f"💍 饰品：{sold_items['accessory_count']} 件 (💰 {sold_items['accessory_value']} 金币)\n"

        if coins_from_bags > 0:
            message += f"👜 钱袋：自动开启获得 (💰 {coins_from_bags} 金币)\n"
        
        message += f"\n🔒 已锁定和装备中的装备已自动保留"
        
        return {"success": True, "message": message}

    def _auto_consume_money_bags(self, user) -> int:
        """
        自动消耗所有“钱袋”类道具（effect_type == "ADD_COINS"），返回获得金币总数。
        不产生单独消息，直接修改用户金币并统计总额，用于砸锅卖铁聚合展示。
        """
        try:
            # 获取用户道具持有情况与所有道具模板
            user_items = self.inventory_repo.get_user_item_inventory(user.user_id)
            all_items_tpl = self.item_template_repo.get_all_items()
        except Exception:
            return 0

        # 过滤出钱袋类可消耗道具
        money_bag_templates = []
        for tpl in all_items_tpl:
            try:
                if getattr(tpl, "effect_type", None) == "ADD_COINS" and getattr(tpl, "is_consumable", False):
                    money_bag_templates.append(tpl)
            except Exception:
                continue

        if not money_bag_templates:
            return 0

        total_gained = 0
        effect_handler = self.effect_manager.get_effect("ADD_COINS")
        if not effect_handler:
            return 0

        for tpl in money_bag_templates:
            qty = int(user_items.get(tpl.item_id, 0) or 0)
            if qty <= 0:
                continue
            # 先扣减背包数量
            try:
                self.inventory_repo.decrease_item_quantity(user.user_id, tpl.item_id, qty)
            except Exception:
                # 若扣减失败，跳过该模板
                continue
            # 解析负载并应用效果，累计金币
            try:
                payload = json.loads(tpl.effect_payload or "{}")
            except Exception:
                payload = {}

            try:
                result = effect_handler.apply(user, tpl, payload, quantity=qty)
                gained = int(result.get("coins_gained", 0) or 0)
                total_gained += max(gained, 0)
            except Exception:
                # 某个模板应用失败，不影响其他模板
                continue

        # 最终确保用户数据已持久（effect 内已 update，这里稳健再更新一次）
        try:
            self.user_repo.update(user)
        except Exception:
            pass

        return total_gained

    def sell_rod(self, user_id: str, rod_instance_id: int) -> Dict[str, Any]:
        """
        向系统出售指定的鱼竿。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 1. 验证鱼竿是否属于该用户
        user_rods = self.inventory_repo.get_user_rod_instances(user_id)
        rod_to_sell = next((r for r in user_rods if r.rod_instance_id == rod_instance_id), None)

        if not rod_to_sell:
            return {"success": False, "message": "鱼竿不存在或不属于你"}

        # 检查是否锁定
        if rod_to_sell.is_locked:
            return {"success": False, "message": "该鱼竿已锁定，无法出售"}

        # 2. 获取鱼竿模板以计算售价
        rod_template = self.item_template_repo.get_rod_by_id(rod_to_sell.rod_id)
        if not rod_template:
             return {"success": False, "message": "找不到鱼竿的基础信息"}

        # 3. 计算售价
        sell_price = self.game_mechanics_service.calculate_sell_price(
            item_type="rod",
            rarity=rod_template.rarity,
            refine_level=rod_to_sell.refine_level,
        )

        # 4. 执行操作
        # 如果卖出的是当前装备的鱼竿，需要先卸下
        if rod_to_sell.is_equipped:
            user.equipped_rod_instance_id = None

        self.inventory_repo.delete_rod_instance(rod_instance_id)
        user.coins += sell_price
        self.user_repo.update(user)

        return {"success": True, "message": f"成功出售鱼竿【{rod_template.name}】，获得 {sell_price} 金币"}

    def sell_all_rods(self, user_id: str) -> Dict[str, Any]:
        """
        向系统出售所有鱼竿。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 获取用户的鱼竿库存
        user_rods = self.inventory_repo.get_user_rod_instances(user_id)
        if not user_rods:
            return {"success": False, "message": "❌ 你没有可以卖出的鱼竿"}

        total_value = 0
        rods_to_sell = []
        
        # 只计算可以卖出的鱼竿（未锁定、未装备且小于5星）
        for rod_instance in user_rods:
            if rod_instance.is_equipped or rod_instance.is_locked:
                continue
            rod_template = self.item_template_repo.get_rod_by_id(rod_instance.rod_id)
            if rod_template and rod_template.rarity < 5:  # 只计算小于5星的鱼竿
                sell_price = self.game_mechanics_service.calculate_sell_price(
                    item_type="rod",
                    rarity=rod_template.rarity,
                    refine_level=rod_instance.refine_level,
                )
                total_value += sell_price
                rods_to_sell.append(rod_instance)
        
        if total_value == 0:
            return {"success": False, "message": "❌ 没有可以卖出的鱼竿（已自动保留锁定、已装备或5星以上的鱼竿）"}
        
        # 逐个删除可以卖出的鱼竿
        for rod_instance in rods_to_sell:
            self.inventory_repo.delete_rod_instance(rod_instance.rod_instance_id)
        
        # 更新用户金币
        user.coins += total_value
        self.user_repo.update(user)
        return {"success": True, "message": f"💰 成功卖出 {len(rods_to_sell)} 根鱼竿，获得 {total_value} 金币"}

    def sell_accessory(self, user_id: str, accessory_instance_id: int) -> Dict[str, Any]:
        """
        向系统出售指定的饰品。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 1. 验证饰品是否属于该用户
        user_accessories = self.inventory_repo.get_user_accessory_instances(user_id)
        accessory_to_sell = next((a for a in user_accessories if a.accessory_instance_id == accessory_instance_id), None)

        if not accessory_to_sell:
            return {"success": False, "message": "饰品不存在或不属于你"}

        # 检查是否锁定
        if accessory_to_sell.is_locked:
            return {"success": False, "message": "该饰品已锁定，无法出售"}

        # 2. 获取饰品模板以计算售价
        accessory_template = self.item_template_repo.get_accessory_by_id(accessory_to_sell.accessory_id)
        if not accessory_template:
            return {"success": False, "message": "找不到饰品的基础信息"}

        # 3. 计算售价
        sell_price = self.game_mechanics_service.calculate_sell_price(
            item_type="accessory",
            rarity=accessory_template.rarity,
            refine_level=accessory_to_sell.refine_level,
        )

        # 4. 执行操作
        # 如果卖出的是当前装备的饰品，需要先卸下
        if accessory_to_sell.is_equipped:
            user.equipped_accessory_instance_id = None
        self.inventory_repo.delete_accessory_instance(accessory_instance_id)
        user.coins += sell_price
        self.user_repo.update(user)
        return {"success": True, "message": f"成功出售饰品【{accessory_template.name}】，获得 {sell_price} 金币"}

    def sell_all_accessories(self, user_id: str) -> Dict[str, Any]:
        """
        向系统出售所有饰品。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 获取用户的饰品库存
        user_accessories = self.inventory_repo.get_user_accessory_instances(user_id)
        if not user_accessories:
            return {"success": False, "message": "❌ 你没有可以卖出的饰品"}

        total_value = 0
        accessories_to_sell = []
        
        # 只计算可以卖出的饰品（未锁定、未装备且小于5星）
        for accessory_instance in user_accessories:
            if accessory_instance.is_equipped or accessory_instance.is_locked:
                continue
            accessory_template = self.item_template_repo.get_accessory_by_id(accessory_instance.accessory_id)
            if accessory_template and accessory_template.rarity < 5:  # 只计算小于5星的饰品
                sell_price = self.game_mechanics_service.calculate_sell_price(
                    item_type="accessory",
                    rarity=accessory_template.rarity,
                    refine_level=accessory_instance.refine_level,
                )
                total_value += sell_price
                accessories_to_sell.append(accessory_instance)

        if total_value == 0:
            return {"success": False, "message": "❌ 没有可以卖出的饰品（已自动保留锁定、已装备或5星以上的饰品）"}

        # 逐个删除可以卖出的饰品
        for accessory_instance in accessories_to_sell:
            self.inventory_repo.delete_accessory_instance(accessory_instance.accessory_instance_id)
        
        # 更新用户金币
        user.coins += total_value
        self.user_repo.update(user)

        return {"success": True, "message": f"💰 成功卖出 {len(accessories_to_sell)} 件饰品，获得 {total_value} 金币"}

    def sell_equipment(self, user_id: str, instance_id: int, item_type: str) -> Dict[str, Any]:
        """
        统一出售装备方法 - 根据类型自动调用对应的出售方法
        
        Args:
            user_id: 用户ID
            instance_id: 物品实例ID
            item_type: 物品类型，"rod"或"accessory"
        """
        if item_type == "rod":
            return self.sell_rod(user_id, instance_id)
        elif item_type == "accessory":
            return self.sell_accessory(user_id, instance_id)
        else:
            return {"success": False, "message": "❌ 不支持的装备类型"}

    def equip_item(self, user_id: str, instance_id: int, item_type: str) -> Dict[str, Any]:
        """
        装备一个物品（鱼竿或饰品）。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        equip_item_name = ""
        equip_item_id = None
        # 验证物品归属
        if item_type == "rod":
            # 获取目标实例并校验归属
            target_instance = self.inventory_repo.get_user_rod_instance_by_id(user_id, instance_id)
            if not target_instance:
                return {"success": False, "message": "❌ 鱼竿不存在或不属于你"}
            equip_item_id = target_instance.rod_id

            # 阻止装备 0 耐久（非无限）鱼竿
            if target_instance.current_durability is not None and target_instance.current_durability <= 0:
                return {
                    "success": False,
                    "message": "❌ 该鱼竿已损坏（耐久为 0），无法装备。请精炼成功以恢复耐久或更换鱼竿。"
                }

            user.equipped_rod_instance_id = instance_id
            equip_item_name = self.item_template_repo.get_rod_by_id(equip_item_id).name

        elif item_type == "accessory":
            instances = self.inventory_repo.get_user_accessory_instances(user_id)
            for instance in instances:
                if instance.accessory_instance_id == instance_id:
                    equip_item_id = instance.accessory_id
                    break
            if instance_id not in [i.accessory_instance_id for i in instances]:
                return {"success": False, "message": "❌ 饰品不存在或不属于你"}
            user.equipped_accessory_instance_id = instance_id
            equip_item_name = self.item_template_repo.get_accessory_by_id(equip_item_id).name
        else:
            return {"success": False, "message": "❌ 不支持的装备类型"}

        # 统一由一个仓储方法处理装备状态的事务性
        self.inventory_repo.set_equipment_status(
            user_id,
            rod_instance_id=user.equipped_rod_instance_id,
            accessory_instance_id=user.equipped_accessory_instance_id
        )
        # 更新用户表
        self.user_repo.update(user)

        return {"success": True, "message": f"💫 装备 【{equip_item_name}】 成功！"}

    def use_bait(self, user_id: str, bait_id: int) -> Dict[str, Any]:
        """
        使用一个鱼饵。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 检查是否有此鱼饵
        bait_inventory = self.inventory_repo.get_user_bait_inventory(user_id)
        if bait_inventory.get(bait_id, 0) <= 0:
            return {"success": False, "message": "你没有这个鱼饵"}

        bait_template = self.item_template_repo.get_bait_by_id(bait_id)
        if not bait_template:
            return {"success": False, "message": "鱼饵信息不存在"}

        required_rod_rarity = getattr(bait_template, "required_rod_rarity", 0) or 0
        if required_rod_rarity > 0:
            equipped_rod = self.inventory_repo.get_user_equipped_rod(user_id)
            if not equipped_rod:
                return {
                    "success": False,
                    "message": f"使用【{bait_template.name}】需要装备 {required_rod_rarity} 星及以上鱼竿"
                }

            rod_template = self.item_template_repo.get_rod_by_id(equipped_rod.rod_id)
            if not rod_template or rod_template.rarity < required_rod_rarity:
                current_rarity = rod_template.rarity if rod_template else 0
                return {
                    "success": False,
                    "message": f"使用【{bait_template.name}】需要 {required_rod_rarity} 星及以上鱼竿，当前鱼竿为 {current_rarity} 星"
                }

        # 更新用户当前鱼饵状态
        user.current_bait_id = bait_id
        user.bait_start_time = datetime.now()

        self.user_repo.update(user)

        return {"success": True, "message": f"💫 成功使用鱼饵【{bait_template.name}】"}

    def get_user_fish_pond_capacity(self, user_id: str) -> Dict[str, Any]:
        """
        获取用户鱼塘容量以及当前容量。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        fish_inventory = self.inventory_repo.get_fish_inventory(user_id)
        return {
            "success": True,
            "fish_pond_capacity": user.fish_pond_capacity,
            "current_fish_count": sum(item.quantity for item in fish_inventory),
        }

    def upgrade_fish_pond(self, user_id: str) -> Dict[str, Any]:
        """
        升级鱼塘容量。
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        upgrade_path = self.config.get("pond_upgrades", [])
        current_capacity = user.fish_pond_capacity

        next_upgrade = None
        for upgrade in upgrade_path:
            if upgrade["from"] == current_capacity:
                next_upgrade = upgrade
                break

        if not next_upgrade:
            return {"success": False, "message": "鱼塘容量已达到最大，无法再升级"}

        cost = next_upgrade["cost"]
        if not user.can_afford(cost):
            return {"success": False, "message": f"金币不足，升级需要 {cost} 金币"}

        # 执行升级
        user.coins -= cost
        user.fish_pond_capacity = next_upgrade["to"]
        self.user_repo.update(user)

        return {
            "success": True,
            "message": f"鱼塘升级成功！新容量为 {user.fish_pond_capacity}。",
            "new_capacity": user.fish_pond_capacity,
            "cost": cost
        }
    def refine(self, user_id, instance_id: int, item_type: str):
        """
        精炼鱼竿或饰品，提升其属性。

        Args:
            user_id: 用户ID
            instance_id: 物品实例ID
            item_type: 物品类型，"rod"或"accessory"
        """
        # 检查用户是否存在
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 精炼费用表 (1-10级)
        refine_costs = {
            1: 10000, 2: 30000, 3: 50000, 4: 100000,
            5: 200000, 6: 500000, 7: 1000000, 8: 2000000,
            9: 5000000, 10: 10000000
        }

        # 根据物品类型设置相关配置
        if item_type not in ["rod", "accessory"]:
            return {"success": False, "message": "❌ 不支持的精炼类型"}

        config = self._get_item_config(item_type, instance_id, user_id)
        if not config["success"]:
            return config

        # 解包配置
        instance = config["instance"]
        template = config["template"]
        item_name = config["item_name"]
        id_field = config["id_field"]

        # 检查精炼等级
        if instance.refine_level >= 10:
            return {"success": False, "message": "已达到最高精炼等级"}

        # 获取装备稀有度
        rarity = template.rarity if hasattr(template, 'rarity') else 5

        # 根据稀有度调整精炼费用和成功率
        refine_costs, success_rates = self._get_refine_config_by_rarity(rarity, refine_costs)

        # 获取同类型物品列表
        same_items = config["same_items"]
        if len(same_items) < 2:
            return {"success": False, "message": f"需要至少两个同类型{item_name}进行精炼。当前拥有：{len(same_items)}个"}

        # 查找合适的消耗品进行精炼
        refine_result = self._find_refinement_candidate(
            user, instance, same_items, refine_costs, id_field, item_type, success_rates
        )

        if not refine_result["success"]:
            # 如果是成功率失败，直接返回
            if refine_result.get("failed", False):
                return refine_result
            # 其他失败情况（如金币不足）
            return refine_result

        # 成功路径：直接返回结果，避免落入后续错误分支
        return refine_result

        # 重构毁坏机制：根据稀有度调整毁坏概率
        if instance.refine_level >= 6:
            # 获取装备稀有度
            rarity = template.rarity if hasattr(template, 'rarity') else 5
            
            # 根据稀有度设置毁坏概率：低星装备毁坏概率更低
            if rarity <= 2:
                destruction_chance = 0.1  # 1-2星：10%毁坏概率
            elif rarity <= 4:
                destruction_chance = 0.2  # 3-4星：20%毁坏概率
            elif rarity <= 6:
                destruction_chance = 0.25  # 5-6星：25%毁坏概率（降低了10%）
            else:
                destruction_chance = 0.4   # 7星+：40%毁坏概率（降低了10%）
            
            import random
            if random.random() < destruction_chance:
                # 根据稀有度设置保留概率：低星装备更容易保留
                if rarity <= 2:
                    survival_chance = 0.5  # 1-2星：50%概率保留
                elif rarity <= 4:
                    survival_chance = 0.3  # 3-4星：30%概率保留
                else:
                    survival_chance = 0.1  # 5星+：10%概率保留
                
                if random.random() < survival_chance:
                    # 等级降1级，但保留装备
                    instance.refine_level = max(1, instance.refine_level - 1)
                    if item_type == "rod":
                        self.inventory_repo.update_rod_instance(instance)
                    else:  # accessory
                        self.inventory_repo.update_accessory_instance(instance)
                    
                    return {
                        "success": False,
                        "message": f"💥 精炼失败！{item_name}等级降为 {instance.refine_level}，但装备得以保留！",
                        "destroyed": False,
                        "level_reduced": True,
                        "new_refine_level": instance.refine_level
                    }
                else:
                    # 完全毁坏装备
                    if item_type == "rod":
                        self.inventory_repo.delete_rod_instance(instance.rod_instance_id)
                    else:  # accessory
                        self.inventory_repo.delete_accessory_instance(instance.accessory_instance_id)
                    
                    return {
                        "success": False,
                        "message": f"💥 精炼失败！{item_name}在精炼过程中毁坏了！",
                        "destroyed": True
                    }


    def _get_refine_config_by_rarity(self, rarity: int, base_costs: dict) -> tuple:
        """
        根据装备稀有度获取精炼费用和成功率
        重构设计：让低星装备更容易精炼到高等级，以追上高星装备的基础属性
        
        Args:
            rarity: 装备稀有度 (1-10星)
            base_costs: 基础费用表
            
        Returns:
            tuple: (调整后的费用表, 成功率表)
        """
        # 1-4星装备：逐级递减成功率，让高等级精炼有挑战性
        if rarity <= 4:
            # 费用大幅减少，让低星装备精炼更便宜
            cost_multiplier = 0.1 + (rarity - 1) * 0.05  # 1星10%, 2星15%, 3星20%, 4星25%
            adjusted_costs = {level: int(cost * cost_multiplier) for level, cost in base_costs.items()}
            
            # 重新设计成功率：低等级高成功率，高等级逐渐降低
            if rarity <= 2:  # 1-2星：保持较高成功率
                success_rates = {
                    1: 0.95, 2: 0.95, 3: 0.90, 4: 0.90,
                    5: 0.85, 6: 0.80, 7: 0.75, 8: 0.70,
                    9: 0.60, 10: 0.50
                }
            elif rarity == 3:  # 3星：中等成功率
                success_rates = {
                    1: 0.90, 2: 0.90, 3: 0.85, 4: 0.85,
                    5: 0.80, 6: 0.75, 7: 0.65, 8: 0.55,
                    9: 0.45, 10: 0.35
                }
            else:  # 4星：更有挑战性
                success_rates = {
                    1: 0.85, 2: 0.85, 3: 0.80, 4: 0.80,
                    5: 0.75, 6: 0.70, 7: 0.60, 8: 0.50,
                    9: 0.40, 10: 0.30
                }
            
        # 5-6星装备：中等费用；成功率按设计在6级附近≈50%，越往后越难
        elif rarity <= 6:
            # 费用适中
            cost_multiplier = 0.5 + (rarity - 5) * 0.2  # 5星50%, 6星70%
            adjusted_costs = {level: int(cost * cost_multiplier) for level, cost in base_costs.items()}

            # 区分5星与6星的成功率曲线
            if rarity == 5:
                success_rates = {
                    1: 0.85, 2: 0.85, 3: 0.80, 4: 0.75,
                    5: 0.65, 6: 0.50, 7: 0.40, 8: 0.35,
                    9: 0.30, 10: 0.25
                }
            else:  # rarity == 6
                success_rates = {
                    1: 0.80, 2: 0.80, 3: 0.75, 4: 0.70,
                    5: 0.60, 6: 0.45, 7: 0.35, 8: 0.30,
                    9: 0.25, 10: 0.20
                }
            
        # 7星及以上装备：保持挑战性
        else:
            adjusted_costs = base_costs.copy()
            success_rates = {
                1: 0.8, 2: 0.8, 3: 0.8, 4: 0.8,
                5: 0.7, 6: 0.6, 7: 0.5, 8: 0.4,
                9: 0.3, 10: 0.2
            }
        
        return adjusted_costs, success_rates

    def _determine_failure_type(self, instance, template) -> str:
        """
        确定精炼失败的类型：普通失败、降级失败、毁坏失败
        
        Args:
            instance: 装备实例
            template: 装备模板
            
        Returns:
            str: "normal", "downgrade", "destruction"
        """
        import random
        
        # 获取装备稀有度和精炼等级
        rarity = template.rarity if template and hasattr(template, 'rarity') else 5
        refine_level = instance.refine_level
        
        # 基础概率设置
        downgrade_chance = 0.10  # 固定10%概率降级
        destruction_chance = 0.0
            
        # 根据稀有度调整毁坏概率
        if refine_level >= 5:
            if rarity <= 2:
                destruction_chance = 0.30  # 10% + 20% = 30%
            elif rarity <= 4:
                destruction_chance = 0.35  # 15% + 20% = 35%
            elif rarity <= 6:
                destruction_chance = 0.40  # 20% + 20% = 40%
            else:
                destruction_chance = 0.50  # 30% + 20% = 50%
                
        # 随机决定失败类型
        rand = random.random()
        
        if rand < destruction_chance:
            return "destruction"
        elif rand < destruction_chance + downgrade_chance:
            return "downgrade"
        else:
            return "normal"

    def _get_item_config(self, item_type, instance_id, user_id) -> Dict[str, Any]:
        """获取物品配置信息"""        
        if item_type == "rod":
            instances = self.inventory_repo.get_user_rod_instances(user_id)
            instance = next((i for i in instances if i.rod_instance_id == instance_id), None)
            if not instance:
                return {"success": False, "message": "鱼竿不存在或不属于你"}

            # 锁定装备可以作为主装备精炼，但不能作为材料

            template = self.item_template_repo.get_rod_by_id(instance.rod_id)
            same_items = self.inventory_repo.get_same_rod_instances(user_id, instance.rod_id)

            return {
                "success": True,
                "instance": instance,
                "template": template,
                "same_items": same_items,
                "item_name": "鱼竿",
                "id_field": "rod_instance_id"
            }

        else:  # accessory
            instances = self.inventory_repo.get_user_accessory_instances(user_id)
            instance = next((i for i in instances if i.accessory_instance_id == instance_id), None)
            if not instance:
                return {"success": False, "message": "饰品不存在或不属于你"}

            # 锁定装备可以作为主装备精炼，但不能作为材料

            template = self.item_template_repo.get_accessory_by_id(instance.accessory_id)
            same_items = self.inventory_repo.get_same_accessory_instances(user_id, instance.accessory_id)

            return {
                "success": True,
                "instance": instance,
                "template": template,
                "same_items": same_items,
                "item_name": "饰品",
                "id_field": "accessory_instance_id"
            }

    def _find_refinement_candidate(self, user, instance, same_items, refine_costs, id_field, item_type, success_rates=None):
        """查找可用于精炼的候选物品"""
        refine_level_from = instance.refine_level
        min_cost = None
        available_candidates = 0

        # 优先使用未装备且精炼等级最低的材料，避免误用高精材料
        sorted_candidates = sorted(
            same_items,
            key=lambda i: (getattr(i, 'is_equipped', False), getattr(i, 'refine_level', 1))
        )

        # 遍历所有可能的消耗品（已排序）
        for candidate in sorted_candidates:
            # 跳过自身
            if getattr(candidate, id_field) == getattr(instance, id_field):
                continue
            # 跳过正在装备的材料
            if getattr(candidate, 'is_equipped', False):
                continue
            # 跳过锁定的材料（锁定的装备不能作为精炼材料）
            if getattr(candidate, 'is_locked', False):
                continue

            available_candidates += 1

            # 计算精炼后的等级：一次只提升1级，杜绝一口吃成胖子
            new_refine_level = min(refine_level_from + 1, 10)
            
            # 如果新等级和当前等级相同，跳过这个候选（已经达到上限）
            if new_refine_level == refine_level_from:
                continue

            # 计算精炼成本
            total_cost = 0
            for level in range(refine_level_from, new_refine_level):
                total_cost += refine_costs.get(level, 0)

            # 记录最低成本（通常每次只升1级，成本恒定，这里做稳健处理）
            if min_cost is None or total_cost < min_cost:
                min_cost = total_cost

            # 检查用户是否有足够的金币
            if not user.can_afford(total_cost):
                continue

            # 检查成功率（如果提供了成功率表）
            if success_rates:
                target_level = new_refine_level
                success_rate = success_rates.get(target_level, 1.0)
                
                import random
                if random.random() > success_rate:
                    # 失败分支：统一消耗金币与材料
                    if item_type == "rod":
                        template = self.item_template_repo.get_rod_by_id(instance.rod_id)
                        item_name_display = template.name if template else "鱼竿"
                    else:
                        template = self.item_template_repo.get_accessory_by_id(instance.accessory_id)
                        item_name_display = template.name if template else "饰品"

                    # 扣除金币
                    user.coins -= total_cost
                    self.user_repo.update(user)
                    # 消耗材料（候选）
                    if item_type == "rod":
                        self.inventory_repo.delete_rod_instance(candidate.rod_instance_id)
                    else:
                        self.inventory_repo.delete_accessory_instance(candidate.accessory_instance_id)

                    # 精炼失败时的三种结果：普通失败、降级失败、毁坏失败
                    failure_type = self._determine_failure_type(instance, template)
                    
                    if failure_type == "downgrade":
                        # 降级失败：只有天命护符·神佑能防止降级
                        try:
                            user_items = self.inventory_repo.get_user_item_inventory(user.user_id)
                        except Exception:
                            user_items = {}

                        # 查找天命护符·神佑（无max_rarity限制的keep模式护符）
                        chosen_tpl = None
                        try:
                            all_items_tpl = self.item_template_repo.get_all_items()
                            for tpl in all_items_tpl:
                                if getattr(tpl, "effect_type", None) == "REFINE_DESTRUCTION_SHIELD":
                                    qty = user_items.get(tpl.item_id, 0)
                                    if qty <= 0:
                                        continue
                                    payload = {}
                                    try:
                                        payload = json.loads(tpl.effect_payload or "{}")
                                    except Exception:
                                        pass
                                    mode = payload.get("mode", "keep")
                                    max_rarity = payload.get("max_rarity")
                                    
                                    # 只有无max_rarity限制的keep模式护符（天命护符·神佑）能防止降级
                                    if mode == "keep" and max_rarity is None:
                                        chosen_tpl = tpl
                                        break
                        except Exception:
                            pass

                        if chosen_tpl is not None:
                            # 自动消耗一个天命护符·神佑
                            self.inventory_repo.decrease_item_quantity(user.user_id, chosen_tpl.item_id, 1)
                            return {
                                "success": False,
                                "message": f"🛡 {chosen_tpl.name} 生效！避免了等级降级。",
                                "failed": True,
                                "destroyed": False
                            }

                        # 无天命护符：执行降级
                        instance.refine_level = max(1, instance.refine_level - 1)
                        if item_type == "rod":
                            self.inventory_repo.update_rod_instance(instance)
                        else:
                            self.inventory_repo.update_accessory_instance(instance)
                        return {
                            "success": False,
                            "message": f"📉 精炼失败！{item_name_display}等级降为 {instance.refine_level}（已消耗材料与金币）。",
                            "failed": True,
                            "destroyed": False,
                            "level_reduced": True,
                            "new_refine_level": instance.refine_level,
                            "target_level": target_level,
                            "success_rate": success_rate
                        }
                    elif failure_type == "destruction":
                        # 毁坏失败：检查护符道具
                                try:
                                    user_items = self.inventory_repo.get_user_item_inventory(user.user_id)
                                except Exception:
                                    user_items = {}

                                chosen_tpl = None
                                chosen_mode = None
                                # 从模板中筛选出护符道具
                                try:
                                    all_items_tpl = self.item_template_repo.get_all_items()
                                    shield_templates = []
                                    for tpl in all_items_tpl:
                                        if getattr(tpl, "effect_type", None) == "REFINE_DESTRUCTION_SHIELD":
                                            shield_templates.append(tpl)
                                    # 构建候选（用户拥有的）
                                    candidates_keep = []
                                    candidates_downgrade = []
                                    for tpl in shield_templates:
                                        qty = user_items.get(tpl.item_id, 0)
                                        if qty <= 0:
                                            continue
                                        payload = {}
                                        try:
                                            payload = json.loads(tpl.effect_payload or "{}")
                                        except Exception:
                                            pass
                                        mode = payload.get("mode", "keep")
                                        max_rarity = payload.get("max_rarity")
                                        
                                        # 检查护符是否对当前装备生效
                                        if max_rarity is not None and template.rarity > int(max_rarity):
                                            continue

                                        if mode == "downgrade":
                                            candidates_downgrade.append((tpl, qty))
                                        else:
                                            candidates_keep.append((tpl, qty))

                                    # 消耗优先级: keep(无限制) > keep(有限制) > downgrade
                                    # 先对keep类护符排序，优先消耗无限制的（天命）
                                    candidates_keep.sort(key=lambda x: json.loads(x[0].effect_payload or '{}').get('max_rarity', 99), reverse=True)

                                    if candidates_keep:
                                        chosen_tpl = candidates_keep[0][0]
                                        chosen_mode = "keep"
                                    elif candidates_downgrade:
                                        chosen_tpl = candidates_downgrade[0][0]
                                        chosen_mode = "downgrade"
                                except Exception:
                                    pass

                                if chosen_tpl is not None:
                                    # 自动消耗一个护符道具
                                    self.inventory_repo.decrease_item_quantity(user.user_id, chosen_tpl.item_id, 1)
                                    if chosen_mode == "downgrade":
                                        # 等级-1并保留
                                        instance.refine_level = max(1, instance.refine_level - 1)
                                        if item_type == "rod":
                                            self.inventory_repo.update_rod_instance(instance)
                                        else:
                                            self.inventory_repo.update_accessory_instance(instance)
                                        return {
                                            "success": False,
                                            "message": f"🛡 {chosen_tpl.name} 生效（降级）！等级降为 {instance.refine_level}，本体保留。",
                                            "failed": True,
                                            "destroyed": False,
                                            "level_reduced": True,
                                            "new_refine_level": instance.refine_level
                                        }
                                    else:
                                        # 保留本体不降级
                                        return {
                                            "success": False,
                                            "message": f"🛡 {chosen_tpl.name} 生效！避免了本体毁坏。",
                                            "failed": True,
                                            "destroyed": False
                                        }

                                # 若无护符道具，检查是否存在旧版Buff护符可抵消
                                try:
                                    shield_buff = self.game_mechanics_service.buff_repo.get_active_by_user_and_type(
                                        user.user_id, "REFINE_DESTRUCTION_SHIELD"
                                    )
                                except Exception:
                                    shield_buff = None

                                if shield_buff and getattr(shield_buff, "payload", None):
                                    try:
                                        shield_payload = json.loads(shield_buff.payload or "{}")
                                    except Exception:
                                        shield_payload = {}
                                    charges = int(shield_payload.get("charges", 0))
                                    mode = shield_payload.get("mode", "keep")
                                    if charges > 0:
                                        remaining = charges - 1
                                        if remaining <= 0:
                                            self.game_mechanics_service.buff_repo.delete(shield_buff.id)
                                        else:
                                            shield_payload.update({"charges": remaining, "mode": mode})
                                            shield_buff.payload = json.dumps(shield_payload)
                                            self.game_mechanics_service.buff_repo.update(shield_buff)
                                        # 根据护符模式处理
                                        if mode == "downgrade":
                                            # 等级-1并保留
                                            instance.refine_level = max(1, instance.refine_level - 1)
                                            if item_type == "rod":
                                                self.inventory_repo.update_rod_instance(instance)
                                            else:
                                                self.inventory_repo.update_accessory_instance(instance)
                                            return {
                                                "success": False,
                                                "message": f"🛡 精炼护符生效（降级）！等级降为 {instance.refine_level}，本体保留（剩余{remaining}）。",
                                                "failed": True,
                                                "destroyed": False,
                                                "level_reduced": True,
                                                "new_refine_level": instance.refine_level
                                            }
                                        else:
                                            # 保留本体（不降级）
                                            return {
                                                "success": False,
                                                "message": f"🛡 精炼护符生效！避免了本体毁坏（剩余{remaining}）。",
                                                "failed": True,
                                                "destroyed": False
                                            }

                                # 无护符：执行毁坏
                                if item_type == "rod":
                                    self.inventory_repo.delete_rod_instance(instance.rod_instance_id)
                                else:
                                    self.inventory_repo.delete_accessory_instance(instance.accessory_instance_id)
                                return {
                                    "success": False,
                                    "message": f"💥 精炼失败！{item_name_display}在精炼过程中毁坏了！",
                                    "destroyed": True
                                }

                    else:
                        # 普通失败：本体保留，但已消耗材料与金币
                        return {
                            "success": False,
                            "message": f"💔 精炼失败！{item_name_display}未能提升到{target_level}级（已消耗材料与金币）。成功率为{success_rate:.0%}，再试一次吧！",
                            "failed": True,
                            "destroyed": False,
                            "target_level": target_level,
                            "success_rate": success_rate
                        }

            # 执行精炼操作
            is_first_infinite = self._perform_refinement(user, instance, candidate, new_refine_level, total_cost, item_type)
            
            # 构建成功消息，包含耐久度信息
            if item_type == "rod":
                template = self.item_template_repo.get_rod_by_id(instance.rod_id)
            else:
                template = self.item_template_repo.get_accessory_by_id(instance.accessory_id)
            
            item_name = template.name if template else "装备"
            success_message = f"成功精炼{item_name}，新精炼等级为 {instance.refine_level}。"
            
            # 检查是否达到了无限耐久的条件（只有支持耐久度的装备才处理）
            if hasattr(instance, 'current_durability'):
                if instance.current_durability is None and is_first_infinite:
                    # 首次获得无限耐久的特殊庆祝消息
                    success_message += f" 🎉✨ 装备已达到完美状态，获得无限耐久！这是真正的神器！ ✨🎉"
                elif instance.current_durability is not None:
                    # 普通耐久度恢复消息
                    success_message += f" 耐久度已恢复并提升至 {instance.current_durability}！"
                # 已经是无限耐久的装备再次精炼：不添加特殊消息，保持简洁
            # 对于没有耐久度的装备（如配饰），不添加耐久度相关消息
            
            return {
                "success": True,
                "message": success_message,
                "new_refine_level": instance.refine_level
            }

        # 如果没有任何可用材料
        if available_candidates == 0:
            return {"success": False, "message": "❌ 没有可用于精炼的材料（需要至少1个未装备的同模板装备）"}

        # 如果没找到合适的候选品（通常是金币不足），返回更友好的错误
        if min_cost is None:
            min_cost = refine_costs.get(refine_level_from, 0)
        return {"success": False, "message": f"至少需要 {min_cost} 金币才能精炼，当前金币不足"}

    def _perform_refinement(
        self, user, instance, candidate, new_refine_level, cost, item_type
    ):
        """执行精炼操作，返回是否首次获得无限耐久"""
        # 扣除金币
        user.coins -= cost

        # 获取原始最大耐久度（用于计算精炼加成）
        if item_type == "rod":
            template = self.item_template_repo.get_rod_by_id(instance.rod_id)
        else:
            template = self.item_template_repo.get_accessory_by_id(instance.accessory_id)
        
        # 检查模板是否存在durability属性（配饰可能没有耐久度）
        original_max_durability = None
        if template and hasattr(template, 'durability') and template.durability is not None:
            original_max_durability = template.durability

        # 提升精炼等级
        old_refine_level = instance.refine_level
        instance.refine_level = new_refine_level

        # 检查精炼前是否已经是无限耐久（配饰可能没有耐久度属性）
        was_infinite_before = (hasattr(instance, 'current_durability') and 
                              instance.current_durability is None)

        # 处理耐久度恢复和上限提升
        is_first_infinite = False
        
        # 获取装备稀有度（对于所有装备类型）
        rarity = template.rarity if template and hasattr(template, 'rarity') else 1
        
        # 检查是否符合无限耐久条件（5星以上10级）
        if new_refine_level >= 10 and rarity >= 5:
            # 只有装备实例支持耐久度时才设置无限耐久
            if hasattr(instance, 'current_durability'):
                instance.current_durability = None  # 无限耐久
                # 标记是否首次获得无限耐久
                is_first_infinite = not was_infinite_before
            # 更新最大耐久度为None（如果装备实例有这个字段）
            if hasattr(instance, 'max_durability'):
                instance.max_durability = None
        elif original_max_durability is not None:
            # 普通精炼：计算新的最大耐久度（仅适用于有耐久度的装备）
            # 公式：新上限 = 原始上限 * (1.5)^精炼等级
            refine_bonus_multiplier = (1.5 ** (new_refine_level - 1))
            new_max_durability = int(original_max_durability * refine_bonus_multiplier)
            
            # 精炼成功时恢复全部耐久度到新的最大值（仅对支持耐久度的装备）
            if hasattr(instance, 'current_durability'):
                instance.current_durability = new_max_durability
            
            # 更新最大耐久度（如果装备实例有这个字段）
            if hasattr(instance, 'max_durability'):
                instance.max_durability = new_max_durability

        # 根据物品类型执行相应操作
        if item_type == "rod":
            self.inventory_repo.update_rod_instance(instance)
            self.inventory_repo.delete_rod_instance(candidate.rod_instance_id)
        else:  # accessory
            self.inventory_repo.update_accessory_instance(instance)
            self.inventory_repo.delete_accessory_instance(candidate.accessory_instance_id)

        # 更新用户信息
        self.user_repo.update(user)
        
        return is_first_infinite

    def use_item(self, user_id: str, item_id: int, quantity: int = 1) -> Dict[str, Any]:
        """
        使用一个或多个道具，并将效果处理委托给 EffectManager。
        """
        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}

        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        item_inventory = self.inventory_repo.get_user_item_inventory(user_id)
        available_quantity = item_inventory.get(item_id, 0)
        if available_quantity < quantity:
            return {"success": False, "message": f"你只有 {available_quantity} 个该道具，数量不足"}

        item_template = self.item_template_repo.get_item_by_id(item_id)
        if not item_template:
            return {"success": False, "message": "道具信息不存在"}

        if not getattr(item_template, "is_consumable", False):
            return {"success": False, "message": f"【{item_template.name}】无法直接使用。"}

        effect_type = item_template.effect_type
        if not effect_type:
            return {"success": True, "message": f"成功使用了 {quantity} 个【{item_template.name}】，但它似乎没什么效果。"}

        effect_handler = self.effect_manager.get_effect(effect_type)
        if not effect_handler:
            return {
                "success": False,
                "message": f"找不到 {effect_type} 效果的处理器，请检查配置。",
            }

        try:
            payload = (
                json.loads(item_template.effect_payload)
                if item_template.effect_payload
                else {}
            )
            
            # 传递 quantity 参数给效果处理器
            result = effect_handler.apply(user, item_template, payload, quantity=quantity)

            # 只有在效果处理成功时才消耗道具
            if result.get("success", False):
                self.inventory_repo.decrease_item_quantity(user_id, item_id, quantity)
                # 确保返回的消息包含道具名称和数量
                final_message = f"成功使用了 {quantity} 个【{item_template.name}】！{result.get('message', '')}"
                result["message"] = final_message
            else:
                # 效果处理失败，不消耗道具，但保持原始错误消息
                result["message"] = f"❌ 使用道具失败：{result.get('message', '')}"
            
            return result

        except Exception as e:
            # 异常处理，防止某个效果的bug导致整个流程中断
            # 在实际生产中，这里应该有更详细的日志记录
            return {"success": False, "message": f"使用道具时发生未知错误: {e}"}

    def open_all_money_bags(self, user_id: str) -> Dict[str, Any]:
        """
        开启用户拥有的所有钱袋类道具（effect_type == "ADD_COINS" 且 is_consumable == True）
        返回开启结果统计
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        try:
            # 获取用户道具持有情况与所有道具模板
            user_items = self.inventory_repo.get_user_item_inventory(user_id)
            all_items_tpl = self.item_template_repo.get_all_items()
        except Exception as e:
            return {"success": False, "message": f"获取道具信息失败: {e}"}

        # 过滤出钱袋类可消耗道具
        money_bag_templates = []
        for tpl in all_items_tpl:
            try:
                if (getattr(tpl, "effect_type", None) == "ADD_COINS" and 
                    getattr(tpl, "is_consumable", False)):
                    money_bag_templates.append(tpl)
            except Exception:
                continue

        if not money_bag_templates:
            return {"success": True, "message": "🎒 您当前没有可开启的钱袋", "total_gained": 0, "opened_bags": 0}

        # 统计用户拥有的钱袋
        owned_money_bags = []
        for tpl in money_bag_templates:
            quantity = user_items.get(tpl.item_id, 0)
            if quantity > 0:
                owned_money_bags.append((tpl, quantity))

        if not owned_money_bags:
            return {"success": True, "message": "🎒 您当前没有可开启的钱袋", "total_gained": 0, "opened_bags": 0}

        # 获取金币效果处理器
        effect_handler = self.effect_manager.get_effect("ADD_COINS")
        if not effect_handler:
            return {"success": False, "message": "金币效果处理器不可用"}

        total_gained = 0
        opened_bags = 0
        bag_details = []

        # 逐个开启钱袋
        for tpl, quantity in owned_money_bags:
            try:
                # 消耗道具
                self.inventory_repo.decrease_item_quantity(user_id, tpl.item_id, quantity)
                
                # 应用效果
                payload = json.loads(tpl.effect_payload) if tpl.effect_payload else {}
                result = effect_handler.apply(user, tpl, payload, quantity=quantity)
                
                if result.get("success", False):
                    coins_gained = result.get("coins_gained", 0)
                    total_gained += coins_gained
                    opened_bags += quantity
                    bag_details.append(f"  {tpl.name} x{quantity} → {coins_gained} 金币")
                else:
                    # 如果开启失败，恢复道具数量
                    self.inventory_repo.increase_item_quantity(user_id, tpl.item_id, quantity)
                    
            except Exception as e:
                # 如果开启失败，恢复道具数量
                try:
                    self.inventory_repo.increase_item_quantity(user_id, tpl.item_id, quantity)
                except:
                    pass
                continue

        # 构建返回消息
        if opened_bags == 0:
            return {"success": True, "message": "🎒 没有成功开启任何钱袋", "total_gained": 0, "opened_bags": 0}

        message = f"🎒 成功开启了 {opened_bags} 个钱袋，共获得 {total_gained} 金币！\n\n"
        message += "📋 开启详情：\n"
        message += "\n".join(bag_details)
        
        return {
            "success": True,
            "message": message,
            "total_gained": total_gained,
            "opened_bags": opened_bags
        }

    def sell_item(self, user_id: str, item_id: int, quantity: int = 1) -> Dict[str, Any]:
        """出售指定数量的道具，按照模板 cost 的一半计价（至少 1）。"""
        if quantity <= 0:
            return {"success": False, "message": "数量必须大于0"}

        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        inv = self.inventory_repo.get_user_item_inventory(user_id)
        owned_qty = inv.get(item_id, 0)
        if owned_qty <= 0:
            return {"success": False, "message": "❌ 你没有这个道具"}
        if quantity > owned_qty:
            return {"success": False, "message": f"❌ 数量不足，当前仅有 {owned_qty} 个"}

        tpl = self.item_template_repo.get_item_by_id(item_id)
        if not tpl:
            return {"success": False, "message": "道具信息不存在"}

        # 定价：模板 cost 的 50%，至少 1
        single_price = max(1, int((tpl.cost or 0) * 0.5))
        total = single_price * quantity

        # 扣减库存，增加金币
        self.inventory_repo.decrease_item_quantity(user_id, item_id, quantity)
        user.coins += total
        self.user_repo.update(user)

        return {
            "success": True,
            "message": f"💰 成功卖出【{tpl.name}】x{quantity}，获得 {total} 金币",
            "gained": total,
            "remaining": owned_qty - quantity
        }

    def lock_rod(self, user_id: str, rod_instance_id: int) -> Dict[str, Any]:
        """
        锁定指定的鱼竿，防止被当作精炼材料、卖出、上架
        注意：锁定的鱼竿仍可作为主装备进行精炼，精炼失败时仍会被碎掉
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 验证鱼竿是否属于该用户
        user_rods = self.inventory_repo.get_user_rod_instances(user_id)
        rod_to_lock = next((r for r in user_rods if r.rod_instance_id == rod_instance_id), None)
        
        if not rod_to_lock:
            return {"success": False, "message": "鱼竿不存在或不属于您"}

        if rod_to_lock.is_locked:
            return {"success": False, "message": "该鱼竿已经锁定"}

        # 锁定鱼竿
        rod_to_lock.is_locked = True
        self.inventory_repo.update_rod_instance(rod_to_lock)

        # 获取鱼竿模板信息用于显示
        rod_template = self.item_template_repo.get_rod_by_id(rod_to_lock.rod_id)
        rod_name = rod_template.name if rod_template else f"鱼竿#{rod_instance_id}"

        return {
            "success": True,
            "message": f"🔒 成功锁定【{rod_name}】，该鱼竿现在受到保护"
        }

    def unlock_rod(self, user_id: str, rod_instance_id: int) -> Dict[str, Any]:
        """
        解锁指定的鱼竿
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 验证鱼竿是否属于该用户
        user_rods = self.inventory_repo.get_user_rod_instances(user_id)
        rod_to_unlock = next((r for r in user_rods if r.rod_instance_id == rod_instance_id), None)
        
        if not rod_to_unlock:
            return {"success": False, "message": "鱼竿不存在或不属于您"}

        if not rod_to_unlock.is_locked:
            return {"success": False, "message": "该鱼竿未锁定"}

        # 解锁鱼竿
        rod_to_unlock.is_locked = False
        self.inventory_repo.update_rod_instance(rod_to_unlock)

        # 获取鱼竿模板信息用于显示
        rod_template = self.item_template_repo.get_rod_by_id(rod_to_unlock.rod_id)
        rod_name = rod_template.name if rod_template else f"鱼竿#{rod_instance_id}"

        return {
            "success": True,
            "message": f"🔓 成功解锁【{rod_name}】，该鱼竿现在可以正常操作"
        }

    def lock_accessory(self, user_id: str, accessory_instance_id: int) -> Dict[str, Any]:
        """
        锁定指定的饰品，防止被当作精炼材料、卖出、上架
        注意：锁定的饰品仍可作为主装备进行精炼，精炼失败时仍会被碎掉
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 验证饰品是否属于该用户
        user_accessories = self.inventory_repo.get_user_accessory_instances(user_id)
        accessory_to_lock = next((a for a in user_accessories if a.accessory_instance_id == accessory_instance_id), None)
        
        if not accessory_to_lock:
            return {"success": False, "message": "饰品不存在或不属于您"}

        if accessory_to_lock.is_locked:
            return {"success": False, "message": "该饰品已经锁定"}

        # 锁定饰品
        accessory_to_lock.is_locked = True
        self.inventory_repo.update_accessory_instance(accessory_to_lock)

        # 获取饰品模板信息用于显示
        accessory_template = self.item_template_repo.get_accessory_by_id(accessory_to_lock.accessory_id)
        accessory_name = accessory_template.name if accessory_template else f"饰品#{accessory_instance_id}"

        return {
            "success": True,
            "message": f"🔒 成功锁定【{accessory_name}】，该饰品现在受到保护"
        }

    def unlock_accessory(self, user_id: str, accessory_instance_id: int) -> Dict[str, Any]:
        """
        解锁指定的饰品
        """
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        # 验证饰品是否属于该用户
        user_accessories = self.inventory_repo.get_user_accessory_instances(user_id)
        accessory_to_unlock = next((a for a in user_accessories if a.accessory_instance_id == accessory_instance_id), None)
        
        if not accessory_to_unlock:
            return {"success": False, "message": "饰品不存在或不属于您"}

        if not accessory_to_unlock.is_locked:
            return {"success": False, "message": "该饰品未锁定"}

        # 解锁饰品
        accessory_to_unlock.is_locked = False
        self.inventory_repo.update_accessory_instance(accessory_to_unlock)

        # 获取饰品模板信息用于显示
        accessory_template = self.item_template_repo.get_accessory_by_id(accessory_to_unlock.accessory_id)
        accessory_name = accessory_template.name if accessory_template else f"饰品#{accessory_instance_id}"

        return {
            "success": True,
            "message": f"🔓 成功解锁【{accessory_name}】，该饰品现在可以正常操作"
        }