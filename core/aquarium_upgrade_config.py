from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping


DEFAULT_AQUARIUM_UPGRADES_PATH = (
    Path(__file__).resolve().parent / "config" / "aquarium_upgrades.json"
)
DEFAULT_FISH_POND_UPGRADES_PATH = (
    Path(__file__).resolve().parent / "config" / "fish_pond_upgrades.json"
)
MAX_AQUARIUM_LEVELS = 100
MAX_DESCRIPTION_LENGTH = 200
MAX_SQLITE_INTEGER = 9_223_372_036_854_775_807


class AquariumUpgradeConfigError(ValueError):
    """水族箱升级档位配置不合法。"""


def _parse_integer(value: Any, label: str) -> int:
    if isinstance(value, bool):
        raise AquariumUpgradeConfigError(f"{label}必须是整数")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise AquariumUpgradeConfigError(f"{label}必须是整数") from exc
    if isinstance(value, float) and not value.is_integer():
        raise AquariumUpgradeConfigError(f"{label}必须是整数")
    if isinstance(value, str) and value.strip() != str(parsed):
        raise AquariumUpgradeConfigError(f"{label}必须是整数")
    return parsed


def normalize_aquarium_upgrades(
    rows: Iterable[Mapping[str, Any]],
    *,
    system_label: str = "水族箱",
) -> list[dict[str, Any]]:
    """规范化并校验一整套连续的容量升级档位。"""
    try:
        source_rows = list(rows)
    except TypeError as exc:
        raise AquariumUpgradeConfigError(f"{system_label}升级档位必须是列表") from exc

    if not source_rows:
        raise AquariumUpgradeConfigError(f"至少需要保留 1 个{system_label}等级")
    if len(source_rows) > MAX_AQUARIUM_LEVELS:
        raise AquariumUpgradeConfigError(
            f"{system_label}等级不能超过 {MAX_AQUARIUM_LEVELS} 级"
        )

    normalized: list[dict[str, Any]] = []
    previous_capacity = 0
    for index, row in enumerate(source_rows, start=1):
        if not isinstance(row, Mapping):
            raise AquariumUpgradeConfigError(f"第 {index} 行配置格式错误")

        level = _parse_integer(row.get("level"), f"第 {index} 行等级")
        capacity = _parse_integer(row.get("capacity"), f"第 {index} 行容量")
        cost_coins = _parse_integer(row.get("cost_coins"), f"第 {index} 行金币费用")
        cost_premium = _parse_integer(
            row.get("cost_premium", 0), f"第 {index} 行钻石费用"
        )
        description_value = row.get("description")
        description = (
            "" if description_value is None else str(description_value).strip()
        )

        if level != index:
            raise AquariumUpgradeConfigError(
                f"等级必须从 1 连续递增；第 {index} 行应为等级 {index}"
            )
        if capacity <= previous_capacity:
            raise AquariumUpgradeConfigError(
                f"等级 {level} 的容量必须大于前一级容量 {previous_capacity}"
            )
        if capacity > MAX_SQLITE_INTEGER:
            raise AquariumUpgradeConfigError(f"等级 {level} 的容量过大")
        if cost_coins < 0 or cost_coins > MAX_SQLITE_INTEGER:
            raise AquariumUpgradeConfigError(f"等级 {level} 的金币费用范围无效")
        if cost_premium < 0 or cost_premium > MAX_SQLITE_INTEGER:
            raise AquariumUpgradeConfigError(f"等级 {level} 的钻石费用范围无效")
        if level == 1 and (cost_coins != 0 or cost_premium != 0):
            raise AquariumUpgradeConfigError("等级 1 是初始档位，金币和钻石费用必须为 0")
        if len(description) > MAX_DESCRIPTION_LENGTH:
            raise AquariumUpgradeConfigError(
                f"等级 {level} 的说明不能超过 {MAX_DESCRIPTION_LENGTH} 个字符"
            )

        normalized.append(
            {
                "level": level,
                "capacity": capacity,
                "cost_coins": cost_coins,
                "cost_premium": cost_premium,
                "description": description,
            }
        )
        previous_capacity = capacity

    return normalized


def _load_default_upgrades(
    path: Path, system_label: str
) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, list):
        raise AquariumUpgradeConfigError(f"默认{system_label}升级配置必须是列表")
    return normalize_aquarium_upgrades(payload, system_label=system_label)


def load_default_aquarium_upgrades() -> list[dict[str, Any]]:
    """读取版本化水族箱默认档位。"""
    return _load_default_upgrades(DEFAULT_AQUARIUM_UPGRADES_PATH, "水族箱")


def load_default_fish_pond_upgrades() -> list[dict[str, Any]]:
    """读取版本化鱼塘默认档位。"""
    return _load_default_upgrades(DEFAULT_FISH_POND_UPGRADES_PATH, "鱼塘")
