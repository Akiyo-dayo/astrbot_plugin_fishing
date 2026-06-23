from datetime import datetime, timedelta
from importlib import import_module
from typing import Dict, List, Any, Optional

try:
    logger = import_module("astrbot.api").logger
except ModuleNotFoundError:  # pragma: no cover - fallback for testing environments
    import logging

    logger = logging.getLogger(__name__)

from ..domain.models import User, UserCommodity
from ..repositories.abstract_repository import AbstractExchangeRepository, AbstractUserRepository, AbstractLogRepository


class ExchangeInventoryService:
    """交易所库存管理服务"""

    CAPACITY_UPGRADE_COST = 500000
    CAPACITY_LEVEL_MULTIPLIERS = {
        0: 1.0,
        1: 1.5,
        2: 2.0,
        3: 2.5,
        4: 3.0,
    }
    
    def __init__(self, user_repo: AbstractUserRepository, exchange_repo: AbstractExchangeRepository, 
                 config: Dict[str, Any], log_repo: AbstractLogRepository, market_service=None):
        self.user_repo = user_repo
        self.exchange_repo = exchange_repo
        self.log_repo = log_repo
        self.market_service = market_service
        self.config = config.get("exchange", {})
        
        # 商品定义
        self.commodities = {
            "dried_fish": {"name": "鱼干", "description": "经过晾晒处理的鱼类，保质期较长"},
            "fish_roe": {"name": "鱼卵", "description": "珍贵的鱼类卵子，营养价值极高"},
            "fish_oil": {"name": "鱼油", "description": "从鱼类中提取的油脂，用途广泛"}
        }

    def _get_base_capacity(self) -> int:
        return int(self.config.get("capacity", 1000))

    def _capacity_for_level(self, level: int) -> int:
        multiplier = self.CAPACITY_LEVEL_MULTIPLIERS.get(level, 1.0)
        return int(self._get_base_capacity() * multiplier)

    def get_capacity_info(self, user_id: str) -> Dict[str, Any]:
        """获取用户交易所仓库容量和升级信息"""
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}

        level = max(0, min(getattr(user, "exchange_capacity_level", 0), max(self.CAPACITY_LEVEL_MULTIPLIERS)))
        capacity = self._capacity_for_level(level)
        current_quantity = self._get_user_total_commodity_quantity(user_id)
        next_level = level + 1

        result = {
            "success": True,
            "level": level,
            "base_capacity": self._get_base_capacity(),
            "multiplier": self.CAPACITY_LEVEL_MULTIPLIERS[level],
            "capacity": capacity,
            "current_quantity": current_quantity,
            "upgrade_cost": self.CAPACITY_UPGRADE_COST,
            "max_level": max(self.CAPACITY_LEVEL_MULTIPLIERS),
        }

        if next_level in self.CAPACITY_LEVEL_MULTIPLIERS:
            result.update({
                "next_level": next_level,
                "next_multiplier": self.CAPACITY_LEVEL_MULTIPLIERS[next_level],
                "next_capacity": self._capacity_for_level(next_level),
                "can_upgrade": True,
            })
        else:
            result.update({
                "next_level": None,
                "next_multiplier": None,
                "next_capacity": None,
                "can_upgrade": False,
            })

        return result

    def upgrade_capacity(self, user_id: str) -> Dict[str, Any]:
        """升级用户交易所仓库容量"""
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在"}
        if not user.exchange_account_status:
            return {"success": False, "message": "请先使用【交易所 开户】开通交易所账户"}

        current_level = max(0, getattr(user, "exchange_capacity_level", 0))
        next_level = current_level + 1
        if next_level not in self.CAPACITY_LEVEL_MULTIPLIERS:
            return {"success": False, "message": "交易所仓库容量已达到最高等级"}

        cost = self.CAPACITY_UPGRADE_COST
        if user.coins < cost:
            return {"success": False, "message": f"金币不足，升级需要 {cost:,} 金币"}

        old_capacity = self._capacity_for_level(current_level)
        new_capacity = self._capacity_for_level(next_level)
        user.coins -= cost
        user.exchange_capacity_level = next_level
        self.user_repo.update(user)

        return {
            "success": True,
            "message": (
                f"交易所仓库升级成功！等级 {current_level} → {next_level}，"
                f"容量 {old_capacity} → {new_capacity}，消耗 {cost:,} 金币"
            ),
            "old_capacity": old_capacity,
            "new_capacity": new_capacity,
            "level": next_level,
            "multiplier": self.CAPACITY_LEVEL_MULTIPLIERS[next_level],
        }

    def get_user_commodities(self, user_id: str) -> List[UserCommodity]:
        """获取用户的大宗商品库存"""
        return self.exchange_repo.get_user_commodities(user_id)

    def get_user_inventory(self, user_id: str) -> Dict[str, Any]:
        """获取用户库存信息"""
        try:
            inventory = self.get_user_commodities(user_id)
            
            # 按商品分组统计
            inventory_summary = {}
            for item in inventory:
                commodity_id = item.commodity_id
                if commodity_id not in inventory_summary:
                    inventory_summary[commodity_id] = {
                        "name": self.commodities[commodity_id]["name"],
                        "total_quantity": 0,
                        "total_cost": 0,
                        "items": []
                    }
                
                inventory_summary[commodity_id]["total_quantity"] += item.quantity
                inventory_summary[commodity_id]["total_cost"] += item.purchase_price * item.quantity
                inventory_summary[commodity_id]["items"].append({
                    "instance_id": item.instance_id,
                    "quantity": item.quantity,
                    "purchase_price": item.purchase_price,
                    "purchased_at": item.purchased_at,
                    "expires_at": item.expires_at
                })
            
            return {
                "success": True,
                "inventory": inventory_summary,
                "total_items": len(inventory)
            }
        except Exception as e:
            logger.error(f"获取用户库存失败: {e}")
            return {"success": False, "message": f"获取库存失败: {str(e)}"}

    def purchase_commodity(self, user_id: str, commodity_id: str, quantity: int, current_price: int) -> Dict[str, Any]:
        """购买大宗商品"""
        try:
            # 检查用户是否存在
            user = self.user_repo.get_by_id(user_id)
            if not user:
                return {"success": False, "message": "用户不存在"}
            
            # 检查商品是否存在
            if commodity_id not in self.commodities:
                return {"success": False, "message": "商品不存在"}
            
            # 计算总价格
            total_cost = current_price * quantity
            
            # 检查用户金币是否足够
            if user.coins < total_cost:
                return {"success": False, "message": f"金币不足，需要 {total_cost:,} 金币"}
            
            # 清理腐败商品
            cleared_count = self.exchange_repo.clear_expired_commodities(user_id)
            if cleared_count > 0:
                logger.info(f"用户 {user_id} 清理了 {cleared_count} 个腐败商品")
            
            # 检查容量限制
            capacity_info = self.get_capacity_info(user_id)
            if not capacity_info["success"]:
                return capacity_info
            capacity = capacity_info["capacity"]
            current_quantity = self._get_user_total_commodity_quantity(user_id)
            
            if current_quantity + quantity > capacity:
                return {"success": False, "message": f"超出容量限制，当前持仓: {current_quantity}/{capacity}"}
            
            # 扣除金币
            user.coins -= total_cost
            self.user_repo.update(user)
            
            # 添加商品到库存
            # 根据商品类型设置不同的腐败时间
            if commodity_id == 'dried_fish':
                expires_at = datetime.now() + timedelta(days=3)  # 鱼干：3天
            elif commodity_id == 'fish_roe':
                expires_at = datetime.now() + timedelta(days=2)  # 鱼卵：2天
            elif commodity_id == 'fish_oil':
                # 鱼油：1-3天随机（每日固定）
                from datetime import date
                today = date.today()
                day_of_year = today.timetuple().tm_yday
                days = (day_of_year % 3) + 1  # 1, 2, 3 循环
                expires_at = datetime.now() + timedelta(days=days)
            else:
                expires_at = datetime.now() + timedelta(days=3)  # 默认3天
            from ..domain.models import UserCommodity
            user_commodity = UserCommodity(
                instance_id=0,  # 临时值，数据库会自动生成
                user_id=user_id,
                commodity_id=commodity_id,
                quantity=quantity,
                purchase_price=current_price,
                purchased_at=datetime.now(),
                expires_at=expires_at
            )
            self.exchange_repo.add_user_commodity(user_commodity)
            
            # 计算腐败时间提示
            time_left = expires_at - datetime.now()
            if time_left.total_seconds() <= 0:
                corruption_warning = "，已腐败"
            elif time_left.total_seconds() < 86400:  # 24小时内
                hours = int(time_left.total_seconds() // 3600)
                corruption_warning = f"，{hours}小时后将腐败"
            else:
                days = int(time_left.total_seconds() // 86400)
                hours = int((time_left.total_seconds() % 86400) // 3600)
                if hours > 0:
                    corruption_warning = f"，{days}天{hours}小时后将腐败"
                else:
                    corruption_warning = f"，{days}天后将腐败"
            
            return {
                "success": True,
                "message": f"购买成功！获得 {self.commodities[commodity_id]['name']} x{quantity}{corruption_warning}",
                "total_cost": total_cost,
                "current_price": current_price
            }
        except Exception as e:
            logger.error(f"购买大宗商品失败: {e}")
            return {"success": False, "message": f"购买失败: {str(e)}"}

    def sell_commodity(self, user_id: str, commodity_id: str, quantity: int, current_price: int) -> Dict[str, Any]:
        """卖出大宗商品"""
        try:
            # 检查用户是否存在
            user = self.user_repo.get_by_id(user_id)
            if not user:
                return {"success": False, "message": "用户不存在"}
            
            # 检查商品是否存在
            if commodity_id not in self.commodities:
                return {"success": False, "message": "商品不存在"}
            
            # 获取用户库存
            inventory = self.exchange_repo.get_user_commodities(user_id)
            commodity_items = [item for item in inventory if item.commodity_id == commodity_id]
            
            if not commodity_items:
                return {"success": False, "message": f"您没有 {self.commodities[commodity_id]['name']}"}
            
            # 检查商品是否已腐败，并分别计算价值
            now = datetime.now()
            expired_items = []
            valid_items = []
            
            for item in commodity_items:
                if item.expires_at and isinstance(item.expires_at, datetime):
                    if item.expires_at <= now:
                        expired_items.append(item)
                    else:
                        valid_items.append(item)
                else:
                    valid_items.append(item)
            
            # 计算可卖出数量（包括腐败商品）
            available_quantity = sum(item.quantity for item in commodity_items)
            if available_quantity < quantity:
                return {"success": False, "message": f"库存不足，只有 {available_quantity} 个"}
            
            # 计算需要卖出的腐败和有效商品数量
            remaining_quantity = quantity
            expired_sold = 0
            valid_sold = 0
            
            # 优先卖出腐败商品
            for item in expired_items:
                if remaining_quantity <= 0:
                    break
                sold = min(item.quantity, remaining_quantity)
                expired_sold += sold
                remaining_quantity -= sold
            
            # 然后卖出有效商品
            for item in valid_items:
                if remaining_quantity <= 0:
                    break
                sold = min(item.quantity, remaining_quantity)
                valid_sold += sold
                remaining_quantity -= sold
            
            # 计算总收益（只有有效商品有价值）
            items_to_process = expired_items + valid_items  # 供盈亏分析和库存扣减复用
            total_income = current_price * valid_sold
            is_all_expired = (expired_sold == quantity)
            
            # 计算盈亏分析（用于确定税基）
            profit_loss = self._calculate_profit_loss_analysis(items_to_process, quantity, current_price)
            taxable_profit = max(int(profit_loss.get("profit_loss", 0)), 0)

            # 计算税费（仅对盈利部分征税）
            tax_rate = self.config.get("tax_rate", 0.05)
            tax_amount = int(taxable_profit * tax_rate)
            net_income = total_income - tax_amount
            
            # 扣除库存（优先扣除腐败商品）
            remaining_quantity = quantity
            items_to_process = expired_items + valid_items  # 优先处理腐败商品
            
            for item in items_to_process:
                if remaining_quantity <= 0:
                    break
                
                if item.quantity <= remaining_quantity:
                    # 完全消耗这个物品
                    self.exchange_repo.delete_user_commodity(item.instance_id)
                    remaining_quantity -= item.quantity
                else:
                    # 部分消耗
                    self.exchange_repo.update_user_commodity_quantity(item.instance_id, item.quantity - remaining_quantity)
                    remaining_quantity = 0
            
            # 添加金币
            user.coins += net_income
            self.user_repo.update(user)
            
            # 记录税费
            if self.log_repo:
                from ..domain.models import TaxRecord
                tax_record = TaxRecord(
                    tax_id=0,  # 数据库自增
                    user_id=user_id,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    original_amount=taxable_profit,
                    balance_after=user.coins,
                    tax_type=(f"卖出 {self.commodities[commodity_id]['name']} x{quantity} | "
                              f"毛收入 {total_income:,} 金币 | 税基 {taxable_profit:,} 金币"),
                    timestamp=datetime.now()
                )
                if tax_amount == 0:
                    tax_record.tax_type += " | 未盈利免税"
                self.log_repo.add_tax_record(tax_record)
            
            # 构造返回消息
            tax_message = "💸 本次无税费（未盈利）"
            if tax_amount > 0:
                tax_message = (f"💸 盈利税：{tax_amount:,} 金币 "
                               f"(税率 {tax_rate*100:.1f}%，税基 {taxable_profit:,} 金币)")

            if is_all_expired:
                message = (
                    f"💀 清理腐败商品成功！处理了 {expired_sold} 个腐败的{self.commodities[commodity_id]['name']}，"
                    "获得 0 金币（腐败商品无价值）\n"
                    f"{tax_message}"
                )
            elif expired_sold > 0:
                message = (
                    f"✅ 卖出成功！处理了 {quantity} 个商品（其中 {expired_sold} 个已腐败），"
                    f"获得 {net_income:,} 金币\n{tax_message}\n💀 提示：腐败商品价值为0"
                )
            else:
                message = f"✅ 卖出成功！获得 {net_income:,} 金币\n{tax_message}"
            
            return {
                "success": True,
                "message": message,
                "total_income": total_income,
                "tax_amount": tax_amount,
                "net_income": net_income,
                "current_price": current_price,
                "profit_loss": profit_loss,
                "expired_sold": expired_sold,
                "valid_sold": valid_sold
            }
        except Exception as e:
            logger.error(f"卖出大宗商品失败: {e}")
            return {"success": False, "message": f"卖出失败: {str(e)}"}

    def sell_commodity_by_instance(self, user_id: str, instance_id: int, quantity: int, current_price: int) -> Dict[str, Any]:
        """通过实例ID卖出大宗商品"""
        try:
            # 获取商品实例
            commodity_item = self.exchange_repo.get_user_commodity_by_instance_id(instance_id)
            if not commodity_item:
                return {"success": False, "message": "商品实例不存在"}
            
            # 检查数量
            if commodity_item.quantity < quantity:
                return {"success": False, "message": f"库存不足，只有 {commodity_item.quantity} 个"}
            
            # 调用通用卖出方法
            return self.sell_commodity(user_id, commodity_item.commodity_id, quantity, current_price)
        except Exception as e:
            logger.error(f"卖出大宗商品失败: {e}")
            return {"success": False, "message": f"卖出失败: {str(e)}"}

    def clear_all_inventory(self, user_id: str) -> Dict[str, Any]:
        """清空用户所有大宗商品库存"""
        try:
            # 检查用户是否存在
            user = self.user_repo.get_by_id(user_id)
            if not user:
                return {"success": False, "message": "用户不存在"}
            
            # 获取用户库存
            inventory = self.exchange_repo.get_user_commodities(user_id)
            if not inventory:
                return {"success": False, "message": "库存为空"}
            
            # 获取当前市场价格：先查今日，无则查昨日；仍无则失败
            today = datetime.now().date()
            today_str = today.strftime("%Y-%m-%d")
            prices = self.exchange_repo.get_prices_for_date(today_str)

            if not prices:
                yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
                prices = self.exchange_repo.get_prices_for_date(yesterday_str)

            if not prices:
                return {
                    "success": False,
                    "message": "暂无今日与昨日市场价格，无法执行清仓",
                }

            current_prices = {price.commodity_id: price.price for price in prices}
            
            # 按商品分组计算详细盈亏
            commodity_summary = {}
            total_cost = 0
            total_current_value = 0
            
            for item in inventory:
                commodity_id = item.commodity_id
                if commodity_id not in commodity_summary:
                    commodity_summary[commodity_id] = {
                        "name": self.commodities.get(commodity_id, {}).get("name", "未知商品"),
                        "total_quantity": 0,
                        "total_cost": 0,
                        "total_current_value": 0,
                        "items": []
                    }
                
                # 检查是否已腐败
                now = datetime.now()
                is_expired = item.expires_at <= now
                
                current_price = current_prices.get(commodity_id, 0)
                item_cost = item.purchase_price * item.quantity
                
                if is_expired:
                    # 腐败商品按0价值计算
                    item_current_value = 0
                else:
                    # 未腐败商品按当前市场价格计算
                    item_current_value = current_price * item.quantity
                
                item_profit_loss = item_current_value - item_cost
                
                commodity_summary[commodity_id]["total_quantity"] += item.quantity
                commodity_summary[commodity_id]["total_cost"] += item_cost
                commodity_summary[commodity_id]["total_current_value"] += item_current_value
                commodity_summary[commodity_id]["items"].append({
                    "instance_id": item.instance_id,
                    "quantity": item.quantity,
                    "purchase_price": item.purchase_price,
                    "current_price": current_price,
                    "cost": item_cost,
                    "current_value": item_current_value,
                    "profit_loss": item_profit_loss,
                    "is_expired": is_expired
                })
                
                total_cost += item_cost
                total_current_value += item_current_value
            
            # 计算税费，仅对盈利部分征税
            total_profit_loss = total_current_value - total_cost
            tax_rate = self.config.get("tax_rate", 0.05)
            taxable_profit = max(total_profit_loss, 0)
            tax_amount = int(taxable_profit * tax_rate)
            net_income = total_current_value - tax_amount
            
            # 清空库存
            for item in inventory:
                self.exchange_repo.delete_user_commodity(item.instance_id)
            
            # 添加金币
            user.coins += net_income
            self.user_repo.update(user)
            
            # 记录税费
            if self.log_repo:
                from ..domain.models import TaxRecord
                tax_record = TaxRecord(
                    tax_id=0,  # 数据库自增
                    user_id=user_id,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    original_amount=taxable_profit,
                    balance_after=user.coins,
                    tax_type=(f"清仓所有大宗商品 | 当前价值 {total_current_value:,} 金币 | "
                              f"税基 {taxable_profit:,} 金币"),
                    timestamp=datetime.now()
                )
                if tax_amount == 0:
                    tax_record.tax_type += " | 未盈利免税"
                self.log_repo.add_tax_record(tax_record)
            
            # 构建详细消息
            profit_status = "📈盈利" if total_profit_loss > 0 else "📉亏损" if total_profit_loss < 0 else "➖持平"
            message = f"【📦 清仓完成】\n"
            message += f"═" * 25 + "\n"
            message += f"📊 总体盈亏：{total_profit_loss:+,} 金币 {profit_status}\n"
            message += f"💰 总成本：{total_cost:,} 金币\n"
            message += f"💎 当前价值：{total_current_value:,} 金币\n"
            message += f"📈 盈利率：{(total_profit_loss/total_cost*100):+.1f}%\n"
            if tax_amount > 0:
                message += (f"💸 税费：{tax_amount:,} 金币 "
                            f"(税率 {tax_rate*100:.1f}%，税基 {taxable_profit:,} 金币)\n")
            else:
                message += "💸 税费：0 金币（未盈利，免税）\n"
            message += f"💵 净收入：{net_income:,} 金币\n"
            message += f"─" * 25 + "\n"
            
            # 添加每种商品的详细盈亏
            for commodity_id, data in commodity_summary.items():
                commodity_profit_loss = data["total_current_value"] - data["total_cost"]
                commodity_profit_status = "📈" if commodity_profit_loss > 0 else "📉" if commodity_profit_loss < 0 else "➖"
                message += f"{data['name']} ({data['total_quantity']}个) - 盈亏: {commodity_profit_loss:+,}金币 {commodity_profit_status}\n"
                
                # 显示每个实例的详细信息
                for item_data in data["items"]:
                    instance_profit_loss = item_data["profit_loss"]
                    instance_profit_status = "📈" if instance_profit_loss > 0 else "📉" if instance_profit_loss < 0 else "➖"
                    is_expired = item_data.get("is_expired", False)
                    
                    if is_expired:
                        message += f"  └─ C{self._to_base36(item_data['instance_id'])}: {item_data['quantity']}个 (💀 已腐败) "
                        message += f"{instance_profit_loss:+,}金币 {instance_profit_status}\n"
                    else:
                        message += f"  └─ C{self._to_base36(item_data['instance_id'])}: {item_data['quantity']}个 "
                        message += f"({item_data['purchase_price']}→{item_data['current_price']} 金币) "
                        message += f"{instance_profit_loss:+,}金币 {instance_profit_status}\n"
            
            message += f"═" * 25 + "\n"
            message += f"💡 清仓完成，共获得 {net_income:,} 金币"
            
            return {
                "success": True,
                "message": message,
                "total_cost": total_cost,
                "total_current_value": total_current_value,
                "total_profit_loss": total_profit_loss,
                "tax_amount": tax_amount,
                "net_income": net_income,
                "commodity_summary": commodity_summary
            }
        except Exception as e:
            logger.error(f"清仓失败: {e}")
            return {"success": False, "message": f"清仓失败: {str(e)}"}

    def _to_base36(self, n: int) -> str:
        """将数字转换为Base36字符串"""
        if n == 0:
            return "0"
        out = []
        while n > 0:
            n, remainder = divmod(n, 36)
            out.append("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"[remainder])
        return "".join(reversed(out))

    def clear_commodity_inventory(self, user_id: str, commodity_id: str) -> Dict[str, Any]:
        """清空指定商品库存"""
        try:
            # 检查用户是否存在
            user = self.user_repo.get_by_id(user_id)
            if not user:
                return {"success": False, "message": "用户不存在"}
            
            # 检查商品是否存在
            if commodity_id not in self.commodities:
                return {"success": False, "message": "商品不存在"}
            
            # 获取指定商品库存
            inventory = self.exchange_repo.get_user_commodities(user_id)
            commodity_items = [item for item in inventory if item.commodity_id == commodity_id]
            
            if not commodity_items:
                return {"success": False, "message": f"您没有 {self.commodities[commodity_id]['name']}"}
            
            # 获取当前市场价格：先查今日，无则查昨日；仍无则失败
            today = datetime.now().date()
            today_str = today.strftime("%Y-%m-%d")
            prices = self.exchange_repo.get_prices_for_date(today_str)

            if not prices:
                yesterday_str = (today - timedelta(days=1)).strftime("%Y-%m-%d")
                prices = self.exchange_repo.get_prices_for_date(yesterday_str)

            if not prices:
                return {
                    "success": False,
                    "message": "暂无今日与昨日市场价格，无法执行清仓",
                }

            current_prices = {price.commodity_id: price.price for price in prices}
            current_price = current_prices.get(commodity_id, 0)
            
            # 计算详细盈亏
            total_cost = 0
            total_current_value = 0
            now = datetime.now()
            
            for item in commodity_items:
                item_cost = item.purchase_price * item.quantity
                total_cost += item_cost
                
                # 检查是否已腐败
                is_expired = item.expires_at <= now
                if is_expired:
                    # 腐败商品按0价值计算
                    item_current_value = 0
                else:
                    # 未腐败商品按当前市场价格计算
                    item_current_value = current_price * item.quantity
                
                total_current_value += item_current_value
            
            total_profit_loss = total_current_value - total_cost
        
            # 计算税费（仅对盈利征税）
            tax_rate = self.config.get("tax_rate", 0.05)
            taxable_profit = max(total_profit_loss, 0)
            tax_amount = int(taxable_profit * tax_rate)
            net_income = total_current_value - tax_amount
            
            # 清空指定商品库存
            for item in commodity_items:
                self.exchange_repo.delete_user_commodity(item.instance_id)
            
            # 添加金币
            user.coins += net_income
            self.user_repo.update(user)
            
            # 记录税费
            if self.log_repo:
                from ..domain.models import TaxRecord
                tax_record = TaxRecord(
                    tax_id=0,  # 数据库自增
                    user_id=user_id,
                    tax_amount=tax_amount,
                    tax_rate=tax_rate,
                    original_amount=taxable_profit,
                    balance_after=user.coins,
                    tax_type=(f"清仓 {self.commodities[commodity_id]['name']} | "
                              f"当前价值 {total_current_value:,} 金币 | 税基 {taxable_profit:,} 金币"),
                    timestamp=datetime.now()
                )
                if tax_amount == 0:
                    tax_record.tax_type += " | 未盈利免税"
                self.log_repo.add_tax_record(tax_record)
            
            # 构建详细消息
            commodity_name = self.commodities[commodity_id]['name']
            total_quantity = sum(item.quantity for item in commodity_items)
            profit_status = "📈盈利" if total_profit_loss > 0 else "📉亏损" if total_profit_loss < 0 else "➖持平"
            
            message = f"【📦 清仓 {commodity_name}】\n"
            message += f"═" * 25 + "\n"
            message += f"📊 总体盈亏：{total_profit_loss:+,} 金币 {profit_status}\n"
            message += f"💰 总成本：{total_cost:,} 金币\n"
            message += f"💎 当前价值：{total_current_value:,} 金币\n"
            message += f"📈 盈利率：{(total_profit_loss/total_cost*100):+.1f}%\n"
            if tax_amount > 0:
                message += (f"💸 税费：{tax_amount:,} 金币 "
                            f"(税率 {tax_rate*100:.1f}%，税基 {taxable_profit:,} 金币)\n")
            else:
                message += "💸 税费：0 金币（未盈利，免税）\n"
            message += f"💵 净收入：{net_income:,} 金币\n"
            message += f"─" * 25 + "\n"
            
            # 显示每个实例的详细信息
            for item in commodity_items:
                item_cost = item.purchase_price * item.quantity
                is_expired = item.expires_at <= now
                
                if is_expired:
                    # 腐败商品按0价值计算
                    item_current_value = 0
                else:
                    # 未腐败商品按当前市场价格计算
                    item_current_value = current_price * item.quantity
                
                item_profit_loss = item_current_value - item_cost
                item_profit_status = "📈" if item_profit_loss > 0 else "📉" if item_profit_loss < 0 else "➖"
                
                if is_expired:
                    message += f"C{self._to_base36(item.instance_id)}: {item.quantity}个 (💀 已腐败) "
                    message += f"{item_profit_loss:+,}金币 {item_profit_status}\n"
                else:
                    message += f"C{self._to_base36(item.instance_id)}: {item.quantity}个 "
                    message += f"({item.purchase_price}→{current_price} 金币) "
                    message += f"{item_profit_loss:+,}金币 {item_profit_status}\n"
            
            message += f"═" * 25 + "\n"
            message += f"💡 清仓完成，共获得 {net_income:,} 金币"
            
            return {
                "success": True,
                "message": message,
                "total_cost": total_cost,
                "total_current_value": total_current_value,
                "total_profit_loss": total_profit_loss,
                "tax_amount": tax_amount,
                "net_income": net_income
            }
        except Exception as e:
            logger.error(f"清仓失败: {e}")
            return {"success": False, "message": f"清仓失败: {str(e)}"}

    def _get_user_total_commodity_quantity(self, user_id: str) -> int:
        """获取用户总的大宗商品数量（包括交易所库存和市场上架的商品）"""
        # 交易所库存数量
        inventory_quantity = 0
        inventory = self.exchange_repo.get_user_commodities(user_id)
        for item in inventory:
            inventory_quantity += item.quantity
        
        # 市场上架数量
        market_quantity = 0
        if self.market_service:
            try:
                # 获取用户所有上架商品
                user_listings_result = self.market_service.get_user_listings(user_id)
                if user_listings_result.get("success", False):
                    user_listings = user_listings_result.get("listings", [])
                    # 统计大宗商品数量
                    for listing in user_listings:
                        if listing.item_type == "commodity":
                            market_quantity += listing.quantity
            except Exception as e:
                logger.warning(f"获取用户市场商品数量失败: {e}")
        
        return inventory_quantity + market_quantity

    def _calculate_profit_loss_analysis(self, commodity_items: List[UserCommodity], sell_quantity: int, sell_price: int) -> Dict[str, Any]:
        """计算卖出时的盈亏分析"""
        total_cost = 0
        remaining_quantity = sell_quantity
        
        for item in commodity_items:
            if remaining_quantity <= 0:
                break
            
            item_quantity = min(item.quantity, remaining_quantity)
            total_cost += item.purchase_price * item_quantity
            remaining_quantity -= item_quantity
        
        total_income = sell_price * sell_quantity
        profit_loss = total_income - total_cost
        profit_rate = (profit_loss / total_cost * 100) if total_cost > 0 else 0
        
        return {
            "total_cost": total_cost,
            "total_income": total_income,
            "profit_loss": profit_loss,
            "profit_rate": profit_rate,
            "is_profit": profit_loss > 0
        }

    def calculate_holdings_profit_loss(self, user_commodities: List[UserCommodity], current_prices: Dict[str, int]) -> Dict[str, Any]:
        """计算持仓盈亏分析"""
        try:
            total_cost = 0
            total_current_value = 0
            
            for commodity in user_commodities:
                # 计算成本
                cost = commodity.purchase_price * commodity.quantity
                total_cost += cost
                
                # 检查是否已腐败
                now = datetime.now()
                is_expired = commodity.expires_at <= now
                
                if is_expired:
                    # 腐败商品按0价值计算
                    current_value = 0
                else:
                    # 未腐败商品按当前市场价格计算
                    current_price = current_prices.get(commodity.commodity_id, 0)
                    current_value = current_price * commodity.quantity
                
                total_current_value += current_value
            
            # 计算盈亏
            profit_loss = total_current_value - total_cost
            profit_rate = (profit_loss / total_cost * 100) if total_cost > 0 else 0
            
            return {
                "total_cost": total_cost,
                "total_current_value": total_current_value,
                "profit_loss": profit_loss,
                "profit_rate": profit_rate,
                "is_profit": profit_loss > 0
            }
        except Exception as e:
            logger.error(f"计算持仓盈亏分析失败: {e}")
            return {
                "total_cost": 0,
                "total_current_value": 0,
                "profit_loss": 0,
                "profit_rate": 0,
                "is_profit": False
            }

    def get_user_commodity_stats(self) -> Dict[str, Any]:
        """获取用户大宗商品统计"""
        try:
            # 获取所有用户的大宗商品数据
            all_commodities = self.exchange_repo.get_all_user_commodities()
            
            # 按商品分组统计
            commodity_stats = {}
            for commodity_id in self.commodities.keys():
                commodity_stats[commodity_id] = {
                    "name": self.commodities[commodity_id]["name"],
                    "total_quantity": 0,
                    "user_count": 0,
                    "average_quantity": 0,
                    "users": []
                }
            
            # 统计每个用户的数据
            user_stats = {}
            for commodity in all_commodities:
                user_id = commodity.user_id
                commodity_id = commodity.commodity_id
                
                if user_id not in user_stats:
                    user_stats[user_id] = {}
                
                if commodity_id not in user_stats[user_id]:
                    user_stats[user_id][commodity_id] = 0
                
                user_stats[user_id][commodity_id] += commodity.quantity
                commodity_stats[commodity_id]["total_quantity"] += commodity.quantity
            
            # 整理用户数据
            for user_id, user_commodities in user_stats.items():
                for commodity_id, quantity in user_commodities.items():
                    if quantity > 0:
                        commodity_stats[commodity_id]["user_count"] += 1
                        commodity_stats[commodity_id]["users"].append({
                            "user_id": user_id,
                            "quantity": quantity
                        })
            
            # 计算平均数量
            for commodity_id, stats in commodity_stats.items():
                if stats["user_count"] > 0:
                    stats["average_quantity"] = stats["total_quantity"] / stats["user_count"]
            
            # 计算总用户数和总持仓量
            total_users = len(user_stats)
            total_holdings = sum(stats["total_quantity"] for stats in commodity_stats.values())
            
            return {
                "success": True,
                "stats": {
                    "total_users": total_users,
                    "total_holdings": total_holdings,
                    "commodity_stats": commodity_stats
                }
            }
        except Exception as e:
            logger.error(f"获取用户大宗商品统计失败: {e}")
            return {"success": False, "message": str(e)}
