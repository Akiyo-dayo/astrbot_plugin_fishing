import random
from astrbot.api import logger

class FishWeightService:
    """处理鱼类权重计算与期望价值拟合的服务"""
    def __init__(self, max_cache_size=1000):
        self.weight_cache = {}
        self.max_cache_size = max_cache_size # 新增：设定最大缓存条目数

    def _calculate_ev(self, fish_list, weights):
        total_weight = sum(weights)
        if total_weight <= 0:
            return 0
        return sum(f.base_value * w for f, w in zip(fish_list, weights)) / total_weight

    def get_weights(self, fish_list, coins_chance):
        cache_key = (tuple(f.fish_id for f in fish_list), round(coins_chance, 6))
        if cache_key in self.weight_cache:
            # 把它弹出来再重新塞进去，它就会自动跑到字典的最末尾（也就是“最新鲜”的位置）
            weights = self.weight_cache.pop(cache_key)
            self.weight_cache[cache_key] = weights
            return weights

        base_weights = [1.0 for _ in fish_list] 
        base_ev = self._calculate_ev(fish_list, base_weights)
        target_ev = base_ev + abs(base_ev) * coins_chance # 修正负数期望的边界条件
        safe_base_ev = max(abs(base_ev), 1.0) # 修正价值为0物品的边界条件
        max_value = max(f.base_value for f in fish_list)

        if target_ev >= max_value:
            final_weights = [1.0 if f.base_value == max_value else 0.0 for f in fish_list]
        else:
            low, high = 0.0, 10.0
            final_weights = base_weights
            i = 0
            for _ in range(50):
                mid = (low + high) / 2.0
                try:
                    # 3. 核心底数保护：max(f.base_value, 1)
                    # 这样负数物品的数学权重计算会被强制视为 1
                    # 意味着当 mid 增大时，负数物品会被系统视为“最低价值的垃圾”，受到最大程度的概率打压
                    temp_weights = [w * ((max(f.base_value, 1) / safe_base_ev) ** mid) for f, w in zip(fish_list, base_weights)]
                except OverflowError:
                    high = mid
                    continue
                    
                current_ev = self._calculate_ev(fish_list, temp_weights)
                if abs(current_ev - target_ev) < 0.01:
                    final_weights = temp_weights
                    break
                
                if current_ev < target_ev:
                    low = mid
                else:
                    high = mid
            else:
                final_weights = temp_weights 

        self.weight_cache[cache_key] = final_weights
        
        # 核心淘汰逻辑：如果塞入后超过了设定的最大容量
        if len(self.weight_cache) > self.max_cache_size:
            # 获取字典里最老的那个 Key（即排在字典最开头的元素）
            oldest_key = next(iter(self.weight_cache))
            # 无情抹杀
            del self.weight_cache[oldest_key]  

        return final_weights

    def choose_fish(self, new_fish_list, coins_chance):
        """替代原来的 get_fish_template 函数"""
        if not new_fish_list:
            return None
        if len(new_fish_list) == 1:
            return new_fish_list[0]

        weights = self.get_weights(new_fish_list, coins_chance)
        logger.debug(f"根据 coins_chance={coins_chance} 计算得到的权重列表: {weights}")
        return random.choices(new_fish_list, weights=weights, k=1)[0]