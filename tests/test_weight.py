import sys
import os
import pytest
import types

class MockFish:
    def __init__(self, fish_id, name, base_value):
        self.fish_id = fish_id
        self.name = name
        self.base_value = base_value

# =====================================================================
# 核心重构：使用 autouse fixture 和 monkeypatch 进行环境隔离
# =====================================================================
@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    monkeypatch.syspath_prepend(project_root)

    if 'astrbot.api' not in sys.modules:
        dummy_astrbot = types.ModuleType('astrbot')
        dummy_api = types.ModuleType('astrbot.api')
        
        class DummyLogger:
            def debug(self, *args, **kwargs): pass # 测试环境下静默常规日志
            def info(self, *args, **kwargs): pass
            def error(self, *args, **kwargs): print(*args, **kwargs) # 仅保留错误打印
            def warning(self, *args, **kwargs): pass
            
        dummy_api.logger = DummyLogger()
        
        monkeypatch.setitem(sys.modules, 'astrbot', dummy_astrbot)
        monkeypatch.setitem(sys.modules, 'astrbot.api', dummy_api)

@pytest.fixture
def service():
    from core.services.fish_weight_service import FishWeightService
    return FishWeightService()

def get_mock_fishes_by_rarity(target_rarity):
    from core.initial_data import FISH_DATA
    fish_list = []
    fish_id_counter = 1
    for data in FISH_DATA:
        name = data[0]
        rarity = data[2]
        base_value = data[3]
        if rarity == target_rarity:
            fish_list.append(MockFish(fish_id_counter, name, base_value))
        fish_id_counter += 1
    return fish_list

# =====================================================================
# 测试用例 1：矩阵化参数测试
# =====================================================================
@pytest.mark.parametrize("rarity", [i for i in range(1, 9)])
@pytest.mark.parametrize("coins_chance", [-100.0, -1.0, -0.8, -0.5, 0.0, 0.5, 1.2, 2.0, 100.0])
def test_real_fish_ev(service, rarity, coins_chance):
    fish_list = get_mock_fishes_by_rarity(rarity)
    if not fish_list:
        pytest.skip(f"配置表中没有找到 {rarity} 星的鱼，跳过该项测试。")
        
    base_weights = service.get_weights(fish_list, 0.0)
    base_ev = service._calculate_ev(fish_list, base_weights)
    
    weights = service.get_weights(fish_list, coins_chance)
    actual_ev = service._calculate_ev(fish_list, weights)
    
    target_ev = base_ev * (1 + coins_chance)
    max_value = max(f.base_value for f in fish_list)
    min_value = min(f.base_value for f in fish_list)
    expected_ev = max(min(target_ev, max_value), min_value) 
    
    # 构建仅在失败时显示的详细诊断报告
    error_msg = (
        f"\n[期望偏差过大] {rarity}星鱼池 | 加成 {coins_chance*100}%\n"
        f"基础EV: {base_ev:.2f} | 目标EV(带物理极限): {expected_ev:.2f} | 实际拟合EV: {actual_ev:.2f}\n"
        f"误差值: {abs(actual_ev - expected_ev):.4f}"
    )
    
    assert abs(actual_ev - expected_ev) < 0.1, error_msg
    
# =====================================================================
# 测试用例 2：0元边界条件（但是似乎1星鱼池里面有）
# =====================================================================
def test_zero_value_edge_case(service):
    edge_case_pool = [MockFish(1, "0元石头", 0), MockFish(2, "25元活虾", 25)]
    coins_chance = 0.5
    
    base_weights = service.get_weights(edge_case_pool, 0.0)
    base_ev = service._calculate_ev(edge_case_pool, base_weights)
    weights = service.get_weights(edge_case_pool, coins_chance)
    actual_ev = service._calculate_ev(edge_case_pool, weights)
    
    target_ev = base_ev * (1 + coins_chance)
    expected_ev = min(target_ev, max(f.base_value for f in edge_case_pool))
    
    error_msg = (
        f"\n[0元边界测试失败] 目标EV: {expected_ev:.2f} | 实际EV: {actual_ev:.2f} | 误差: {abs(actual_ev - expected_ev):.4f}"
    )
    
    assert abs(actual_ev - expected_ev) < 0.1, error_msg