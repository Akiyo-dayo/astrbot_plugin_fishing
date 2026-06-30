from typing import TYPE_CHECKING

from astrbot.api.event import AstrMessageEvent

from ..utils import parse_amount, safe_datetime_handler

if TYPE_CHECKING:
    from ..main import FishingPlugin


def _split_args(event: AstrMessageEvent):
    return event.message_str.strip().split()


async def bank_main(plugin: "FishingPlugin", event: AstrMessageEvent):
    """银行主命令。"""
    args = _split_args(event)
    user_id = plugin._get_effective_user_id(event)

    if len(args) == 1:
        result = plugin.bank_service.get_overview(user_id)
        yield event.plain_result(_format_overview(result))
        return

    action = args[1]
    if action in ("存款", "存", "deposit"):
        async for r in deposit(plugin, event, amount_arg=args[2] if len(args) >= 3 else None):
            yield r
    elif action in ("取款", "取", "withdraw"):
        async for r in withdraw(plugin, event, amount_arg=args[2] if len(args) >= 3 else None):
            yield r
    elif action in ("预约取款", "预约", "大额取款"):
        async for r in reserve_withdraw(plugin, event, amount_arg=args[2] if len(args) >= 3 else None):
            yield r
    elif action in ("确认预约", "确认取款", "确认"):
        result = plugin.bank_service.confirm_reservation(user_id)
        yield event.plain_result(result["message"])
    elif action in ("取消预约", "取消取款", "取消"):
        result = plugin.bank_service.cancel_reservation(user_id)
        yield event.plain_result(result["message"])
    else:
        yield event.plain_result(_usage())


async def deposit(plugin: "FishingPlugin", event: AstrMessageEvent, amount_arg: str = None):
    user_id = plugin._get_effective_user_id(event)
    amount, error = _parse_amount_arg(event, amount_arg, "存款")
    if error:
        yield event.plain_result(error)
        return
    result = plugin.bank_service.deposit(user_id, amount)
    yield event.plain_result(result["message"])


async def withdraw(plugin: "FishingPlugin", event: AstrMessageEvent, amount_arg: str = None):
    user_id = plugin._get_effective_user_id(event)
    amount, error = _parse_amount_arg(event, amount_arg, "取款")
    if error:
        yield event.plain_result(error)
        return
    result = plugin.bank_service.withdraw(user_id, amount)
    yield event.plain_result(result["message"])


async def reserve_withdraw(plugin: "FishingPlugin", event: AstrMessageEvent, amount_arg: str = None):
    user_id = plugin._get_effective_user_id(event)
    amount, error = _parse_amount_arg(event, amount_arg, "预约取款")
    if error:
        yield event.plain_result(error)
        return
    result = plugin.bank_service.create_reservation(user_id, amount)
    yield event.plain_result(result["message"])


def _parse_amount_arg(event: AstrMessageEvent, amount_arg: str, action_name: str):
    if amount_arg is None:
        args = _split_args(event)
        amount_arg = args[1] if len(args) >= 2 else None
    if not amount_arg:
        return None, f"❌ 请指定{action_name}金额，例如：/{action_name} 100万"
    try:
        amount = parse_amount(amount_arg)
    except ValueError as e:
        return None, f"❌ {action_name}金额格式错误：{e}"
    return amount, None


def _format_overview(result):
    if not result.get("success"):
        return result.get("message", "查看银行失败")

    user = result["user"]
    account = result["account"]
    pending = result.get("pending")
    message = (
        "【🏦 银行账户】\n"
        f"👛 钱包余额：{user.coins:,} 金币\n"
        f"🏦 银行余额：{account.balance:,} 金币\n"
        f"🆓 今日免费提现剩余：{result['free_remaining']:,}/{result['daily_free_limit']:,} 金币\n"
        f"💸 超额取款手续费：{result['withdraw_fee_rate'] * 100:.1f}%\n"
        f"📌 大额预约门槛：{result['reservation_threshold']:,} 金币\n"
    )
    if pending:
        ready_at = safe_datetime_handler(pending.ready_at)
        message += (
            "\n【待确认预约】\n"
            f"🧾 编号：#{pending.reservation_id}\n"
            f"💰 金额：{pending.amount:,} 金币\n"
            f"💸 预计手续费：{pending.fee_amount:,} 金币\n"
            f"⏱️ 可确认时间：{ready_at}\n"
            "💡 使用：/银行 确认预约"
        )
    else:
        message += "\n暂无待确认预约。"
    return message


def _usage():
    return (
        "【🏦 银行帮助】\n"
        "/银行 - 查看银行账户\n"
        "/银行 存款 金额\n"
        "/银行 取款 金额\n"
        "/银行 预约取款 金额\n"
        "/银行 确认预约\n"
        "/银行 取消预约"
    )
