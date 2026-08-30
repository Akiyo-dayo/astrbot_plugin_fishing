from __future__ import annotations

import json
import math
import os
import secrets
import stat
from copy import deepcopy
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping


def _field(
    path: str,
    label: str,
    value_type: str,
    default: Any,
    *,
    runtime_path: str | None = None,
    runtime_paths: tuple[str, ...] | None = None,
    minimum: float | int | None = None,
    maximum: float | int | None = None,
    step: str | None = None,
    choices: tuple[tuple[str, str], ...] | None = None,
    help_text: str = "",
) -> dict[str, Any]:
    return {
        "path": path,
        "label": label,
        "type": value_type,
        "default": default,
        "runtime_paths": runtime_paths or (runtime_path or path,),
        "min": minimum,
        "max": maximum,
        "step": step,
        "choices": choices,
        "help": help_text,
    }


MESSAGE_MODE_CHOICES = (("image", "图片"), ("text", "文字"))


RUNTIME_SETTING_GROUPS: dict[str, dict[str, Any]] = {
    "gameplay": {
        "title": "基础玩法",
        "description": "钓鱼、偷鱼、电鱼、每日刷新和新用户规则。",
        "fields": [
            _field("fishing.cooldown_seconds", "钓鱼冷却（秒）", "int", 180, minimum=0, maximum=86400),
            _field(
                "fishing.quality_bonus_max_chance",
                "高品质概率上限",
                "float",
                0.35,
                runtime_path="quality_bonus_max_chance",
                minimum=0,
                maximum=1,
                step="0.01",
            ),
            _field("steal.cooldown_seconds", "偷鱼冷却（秒）", "int", 14400, minimum=0, maximum=604800),
            _field("electric_fish.enabled", "启用电鱼", "bool", True),
            _field("electric_fish.cooldown_seconds", "电鱼冷却（秒）", "int", 7200, minimum=0, maximum=604800),
            _field("electric_fish.base_success_rate", "电鱼基础成功率", "float", 0.6, minimum=0, maximum=1, step="0.01"),
            _field("electric_fish.failure_penalty_max_rate", "电鱼失败最大损失率", "float", 0.5, minimum=0, maximum=1, step="0.01"),
            _field("game.daily_reset_hour", "每日重置小时", "int", 0, runtime_path="daily_reset_hour", minimum=0, maximum=23),
            _field("game.wheel_of_fate_daily_limit", "命运之轮每日次数", "int", 3, runtime_path="wheel_of_fate_daily_limit", minimum=0, maximum=1000),
            _field("game.wipe_bomb_attempts", "擦弹每日基础次数", "int", 3, runtime_path="wipe_bomb.max_attempts_per_day", minimum=0, maximum=1000),
            _field("user.initial_coins", "新用户初始金币", "int", 200, minimum=0),
        ],
    },
    "resale": {
        "title": "装备回收价",
        "description": "鱼竿和饰品按星级共用基础回收价，保存后下一次出售立即生效。",
        "fields": [
            *[
                _field(
                    f"sell_prices.by_rarity_{rarity}",
                    f"{rarity} 星装备基础回收价",
                    "int",
                    default,
                    runtime_paths=(
                        f"sell_prices.rod.{rarity}",
                        f"sell_prices.accessory.{rarity}",
                    ),
                    minimum=0,
                )
                for rarity, default in enumerate(
                    (100, 500, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000),
                    start=1,
                )
            ],
        ],
    },
    "loan": {
        "title": "借贷规则",
        "description": "保存后影响新借款和后续催收；已经生成的借条金额与期限不会改写。",
        "fields": [
            _field("loan.default_interest_rate", "玩家借款默认利率", "float", 0.05, minimum=0, maximum=1, step="0.01"),
            _field("loan.system_loan_ratio", "系统借款额度比例", "float", 0.10, minimum=0, maximum=1, step="0.01"),
            _field("loan.system_loan_days", "系统借款期限（天）", "int", 7, minimum=1, maximum=3650),
            _field("loan.collect_from_fixed", "强制收款可解约定期", "bool", True),
        ],
    },
    "gambling": {
        "title": "博弈玩法",
        "description": "保存后影响新开局；已经开始的倒计时不会被追溯重排。",
        "fields": [
            _field("sicbo.countdown_seconds", "骰宝倒计时（秒）", "int", 60, minimum=10, maximum=300),
            _field("sicbo.min_bet", "骰宝最小下注", "int", 100, minimum=1),
            _field("sicbo.max_bet", "骰宝最大下注", "int", 1000000, minimum=1),
            _field("sicbo.min_banker_coins", "骰宝开庄最低金币", "int", 1000000, minimum=0),
            _field("sicbo.message_mode", "骰宝消息模式", "choice", "image", choices=MESSAGE_MODE_CHOICES),
            _field("blackjack.min_bet", "21 点最小下注", "int", 100, minimum=1),
            _field("blackjack.max_bet", "21 点最大下注", "int", 1000000, minimum=1),
            _field("blackjack.min_banker_coins", "21 点开庄最低金币", "int", 1000000, minimum=0),
            _field("blackjack.join_timeout", "21 点加入等待（秒）", "int", 60, minimum=1, maximum=3600),
            _field("blackjack.action_timeout", "21 点操作超时（秒）", "int", 30, minimum=1, maximum=3600),
            _field("blackjack.message_mode", "21 点消息模式", "choice", "image", choices=MESSAGE_MODE_CHOICES),
            _field("blackjack.streak_win_bonus_threshold", "连胜奖励触发局数", "int", 3, minimum=0),
            _field("blackjack.streak_win_bonus_rate", "连胜奖励比例", "float", 0.1, minimum=0, maximum=1, step="0.01"),
            _field("blackjack.streak_lose_consolation_threshold", "连败安慰触发局数", "int", 3, minimum=0),
            _field("blackjack.streak_lose_consolation", "连败安慰金币", "int", 500, minimum=0),
            _field("slot.daily_limit", "拉杆机每日次数", "int", 50, minimum=0),
            _field("slot.max_multi_spin", "拉杆机最大连转", "int", 10, minimum=1, maximum=1000),
            _field("slot.streak_protection", "拉杆机保底连续次数", "int", 20, minimum=1),
            _field("slot.message_mode", "拉杆机消息模式", "choice", "image", choices=MESSAGE_MODE_CHOICES),
        ],
    },
    "bank": {
        "title": "银行规则",
        "description": "保存后用于后续银行操作；定期收益率只影响新开存单。",
        "fields": [
            _field("bank.enabled", "启用银行入金", "bool", True),
            _field("bank.daily_free_withdraw_limit", "每日免费提现额度", "int", 1000000, minimum=0),
            _field("bank.withdraw_fee_rate", "超额取款手续费率", "float", 0.03, minimum=0, maximum=1, step="0.01"),
            _field("bank.reservation_threshold", "大额取款预约门槛", "int", 5000000, minimum=0),
            _field("bank.reservation_delay_hours", "预约等待小时", "int", 24, minimum=0),
            _field("bank.reservation_expire_hours", "可取后过期小时", "int", 72, minimum=1),
            _field("bank.max_pending_reservations", "最大待确认预约数", "int", 1, minimum=1, maximum=100),
            _field("bank.block_inflow_when_in_debt", "欠税时禁止转出资产", "bool", True),
            _field("bank.fixed_deposit.enabled", "启用定期存款", "bool", True),
            _field("bank.fixed_deposit.min_amount", "单笔定期最低金额", "int", 100000, minimum=1),
            _field("bank.fixed_deposit.max_amount", "单笔定期最高金额", "int", 20000000, minimum=1),
            _field("bank.fixed_deposit.max_active_deposits", "最大进行中存单数", "int", 5, minimum=1, maximum=1000),
            _field("bank.fixed_deposit.auto_settle_matured", "自动结算到期定期", "bool", True),
            _field("bank.fixed_deposit.early_withdraw_penalty_rate", "提前支取违约金率", "float", 0.01, minimum=0, maximum=1, step="0.01"),
            _field("bank.fixed_deposit.early_withdraw_penalty_threshold", "违约金起征本金", "int", 1000000, minimum=0),
            _field("bank.fixed_deposit.terms.1", "1 天定期收益率", "float", 0.001, minimum=0, maximum=1, step="0.0001"),
            _field("bank.fixed_deposit.terms.3", "3 天定期收益率", "float", 0.004, minimum=0, maximum=1, step="0.0001"),
            _field("bank.fixed_deposit.terms.7", "7 天定期收益率", "float", 0.01, minimum=0, maximum=1, step="0.0001"),
            _field("bank.fixed_deposit.terms.30", "30 天定期收益率", "float", 0.05, minimum=0, maximum=1, step="0.0001"),
        ],
    },
    "exchange": {
        "title": "交易所规则",
        "description": "容量和盈利税用于后续交易；波动参数用于下一次调价；重置价仅在执行价格重置时使用。",
        "fields": [
            _field("exchange.capacity", "交易所基础容量", "int", 1000, minimum=1),
            _field("exchange.tax_rate", "交易所盈利税率", "float", 0.05, minimum=0, maximum=1, step="0.01"),
            _field("exchange.max_change_rate", "单次价格最大波动", "float", 0.2, minimum=0, maximum=1, step="0.01"),
            _field("exchange.volatility.dried_fish", "鱼干波动率", "float", 0.08, minimum=0, maximum=1, step="0.01"),
            _field("exchange.volatility.fish_roe", "鱼卵波动率", "float", 0.12, minimum=0, maximum=1, step="0.01"),
            _field("exchange.volatility.fish_oil", "鱼油波动率", "float", 0.10, minimum=0, maximum=1, step="0.01"),
            _field("exchange.initial_prices.dried_fish", "鱼干重置价格", "int", 6000, minimum=1),
            _field("exchange.initial_prices.fish_roe", "鱼卵重置价格", "int", 12000, minimum=1),
            _field("exchange.initial_prices.fish_oil", "鱼油重置价格", "int", 10000, minimum=1),
        ],
    },
    "market": {
        "title": "市场规则",
        "description": "保存后对新上架的玩家市场商品立即生效。",
        "fields": [
            _field("market.listing_tax_rate", "市场上架税率", "float", 0.05, minimum=0, maximum=1, step="0.01"),
        ],
    },
}


OTHER_SETTING_GROUP_KEYS = ("gameplay", "resale", "loan", "gambling")


def deep_merge_config(
    current: Mapping[str, Any] | None,
    updates: Mapping[str, Any],
) -> dict[str, Any]:
    """递归合并配置，保留 Web 表单没有展示的兄弟键。"""
    merged = deepcopy(dict(current or {}))
    for key, value in updates.items():
        existing = merged.get(key)
        if isinstance(existing, Mapping) and isinstance(value, Mapping):
            merged[key] = deep_merge_config(existing, value)
        else:
            merged[key] = deepcopy(value)
    return merged


def persist_config_updates(
    updates: Mapping[str, Any],
    *,
    astrbot_config: Any = None,
    config_path: Path | None = None,
    on_warning: Callable[[str], None] | None = None,
) -> list[str]:
    """优先通过 AstrBotConfig 保存，必要时原子回退到精确配置文件。"""
    warn = on_warning or (lambda _message: None)
    save_config = getattr(astrbot_config, "save_config", None)
    if astrbot_config is not None and callable(save_config):
        previous_values: dict[str, tuple[bool, Any]] = {}
        try:
            for section, section_updates in updates.items():
                existed = section in astrbot_config
                previous_values[section] = (
                    existed,
                    deepcopy(astrbot_config.get(section)) if existed else None,
                )
                if isinstance(section_updates, Mapping):
                    astrbot_config[section] = deep_merge_config(
                        astrbot_config.get(section, {}), section_updates
                    )
                else:
                    astrbot_config[section] = deepcopy(section_updates)
            save_config()
            return ["framework:AstrBotConfig"]
        except Exception as exc:
            for section, (existed, old_value) in previous_values.items():
                try:
                    if existed:
                        astrbot_config[section] = old_value
                    else:
                        del astrbot_config[section]
                except Exception as restore_error:
                    warn(
                        f"恢复框架配置分组 {section} 失败: {restore_error}"
                    )
            warn(f"通过框架保存配置失败，回退到直接写文件: {exc}")

    if config_path is None:
        return []
    path = Path(config_path)
    temporary_path = path.with_name(
        f".{path.name}.{secrets.token_hex(6)}.tmp"
    )
    try:
        original_mode = stat.S_IMODE(path.stat().st_mode)
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        data = deep_merge_config(data, updates)
        temporary_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8-sig",
        )
        os.chmod(temporary_path, original_mode)
        os.replace(temporary_path, path)
        return [str(path)]
    except Exception as exc:
        warn(f"写入配置失败: {path} - {exc}")
        return []
    finally:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass


def _get_nested(data: Mapping[str, Any], path: str, default: Any = None) -> Any:
    value: Any = data
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return default
        value = value[part]
    return value


def _set_nested(data: MutableMapping[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    target = data
    for part in parts[:-1]:
        child = target.get(part)
        if not isinstance(child, MutableMapping):
            child = {}
            target[part] = child
        target = child
    target[parts[-1]] = value


def settings_groups_for_view(
    game_config: Mapping[str, Any],
    group_keys: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    groups = []
    keys = group_keys or tuple(RUNTIME_SETTING_GROUPS)
    for key in keys:
        group = RUNTIME_SETTING_GROUPS.get(key)
        if group is None:
            raise ValueError(f"未知的设置分组：{key}")
        fields = []
        for definition in group["fields"]:
            item = dict(definition)
            item["value"] = _get_nested(
                game_config,
                definition["runtime_paths"][0],
                definition["default"],
            )
            fields.append(item)
        groups.append(
            {
                "key": key,
                "title": group["title"],
                "description": group["description"],
                "fields": fields,
            }
        )
    return groups


def setting_group_for_view(
    game_config: Mapping[str, Any], group_key: str
) -> dict[str, Any]:
    return settings_groups_for_view(game_config, [group_key])[0]


def parse_runtime_settings_form(form: Mapping[str, Any], group_key: str) -> dict[str, Any]:
    group = RUNTIME_SETTING_GROUPS.get(group_key)
    if group is None:
        raise ValueError("未知的设置分组")

    values: dict[str, Any] = {}
    for field in group["fields"]:
        path = field["path"]
        value_type = field["type"]
        raw = form.get(path)
        try:
            if value_type == "bool":
                value = raw in ("on", "true", "1", True, 1)
            elif value_type == "int":
                if raw is None or str(raw).strip() == "":
                    raise ValueError
                value = int(str(raw).strip())
                if str(raw).strip() != str(value):
                    raise ValueError
            elif value_type == "float":
                if raw is None or str(raw).strip() == "":
                    raise ValueError
                value = float(str(raw).strip())
                if not math.isfinite(value):
                    raise ValueError
            elif value_type == "choice":
                allowed = {choice[0] for choice in field["choices"]}
                if raw not in allowed:
                    raise ValueError
                value = str(raw)
            else:
                raise ValueError
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field['label']}格式无效") from exc

        minimum = field.get("min")
        maximum = field.get("max")
        if minimum is not None and value < minimum:
            raise ValueError(f"{field['label']}不能小于 {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{field['label']}不能大于 {maximum}")
        values[path] = value

    def require_order(lower_path: str, upper_path: str, message: str) -> None:
        if lower_path in values and values[lower_path] > values[upper_path]:
            raise ValueError(message)

    require_order(
        "bank.fixed_deposit.min_amount",
        "bank.fixed_deposit.max_amount",
        "单笔定期最低金额不能大于最高金额",
    )
    require_order("sicbo.min_bet", "sicbo.max_bet", "骰宝最小下注不能大于最大下注")
    require_order(
        "blackjack.min_bet",
        "blackjack.max_bet",
        "21 点最小下注不能大于最大下注",
    )
    return values


def framework_updates(values: Mapping[str, Any]) -> dict[str, Any]:
    updates: dict[str, Any] = {}
    for path, value in values.items():
        _set_nested(updates, path, value)
    return updates


def apply_runtime_settings(
    game_config: MutableMapping[str, Any],
    values: Mapping[str, Any],
    services: Mapping[str, Any],
) -> None:
    definitions = {
        field["path"]: field
        for group in RUNTIME_SETTING_GROUPS.values()
        for field in group["fields"]
    }
    for path, value in values.items():
        for runtime_path in definitions[path]["runtime_paths"]:
            _set_nested(game_config, runtime_path, deepcopy(value))

    if "game.daily_reset_hour" in values:
        fishing_service = services.get("fishing_service")
        if fishing_service is not None:
            setter = getattr(fishing_service, "set_daily_reset_hour", None)
            if callable(setter):
                setter(values["game.daily_reset_hour"])

    loan_service = services.get("loan_service")
    if loan_service is not None:
        loan_attributes = {
            "loan.default_interest_rate": "default_interest_rate",
            "loan.system_loan_ratio": "system_loan_ratio",
            "loan.system_loan_days": "system_loan_days",
            "loan.collect_from_fixed": "collect_from_fixed",
        }
        for path, attribute in loan_attributes.items():
            if path in values:
                setattr(loan_service, attribute, values[path])

    cached_services = {
        "sicbo": services.get("sicbo_service"),
        "blackjack": services.get("blackjack_service"),
        "slot": services.get("slot_service"),
    }
    for prefix, service in cached_services.items():
        if service is None:
            continue
        for path, value in values.items():
            if path.startswith(prefix + "."):
                setattr(service, path.split(".", 1)[1], value)

    exchange_service = services.get("exchange_service")
    if exchange_service is not None and any(
        path.startswith("exchange.") for path in values
    ):
        exchange_config = game_config.get("exchange", {})
        for child_name in ("price_service", "inventory_service"):
            child = getattr(exchange_service, child_name, None)
            child_config = getattr(child, "config", None)
            if isinstance(child_config, MutableMapping) and child_config is not exchange_config:
                child_config.clear()
                child_config.update(deepcopy(exchange_config))
