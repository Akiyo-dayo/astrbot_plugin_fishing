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
    elif action in ("定期", "定期帮助"):
        result = plugin.bank_service.get_fixed_terms()
        yield event.plain_result(_format_fixed_terms(result))
    elif action in ("定期存款", "存定期"):
        amount, error = _parse_amount_arg(event, args[2] if len(args) >= 3 else None, "定期存款")
        if error:
            yield event.plain_result(error)
            return
        term_days, error = _parse_term_days(args[3] if len(args) >= 4 else None)
        if error:
            yield event.plain_result(error)
            return
        result = plugin.bank_service.create_fixed_deposit(user_id, amount, term_days)
        yield event.plain_result(result["message"])
    elif action in ("定期列表", "我的定期"):
        result = plugin.bank_service.list_fixed_deposits(user_id)
        yield event.plain_result(_format_fixed_deposits(result))
    elif action in ("定期取出", "领取定期", "取出定期"):
        deposit_id, error = _parse_deposit_id(args[2] if len(args) >= 3 else None)
        if error:
            yield event.plain_result(error)
            return
        result = plugin.bank_service.complete_fixed_deposit(user_id, deposit_id)
        yield event.plain_result(result["message"])
    elif action in ("提前取出", "取消定期"):
        deposit_id, error = _parse_deposit_id(args[2] if len(args) >= 3 else None)
        if error:
            yield event.plain_result(error)
            return
        result = plugin.bank_service.cancel_fixed_deposit(user_id, deposit_id)
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
        return None, f"❌ 请指定{action_name}金额，例如：/钓鱼{action_name} 100万"
    try:
        amount = parse_amount(amount_arg)
    except ValueError as e:
        return None, f"❌ {action_name}金额格式错误：{e}"
    return amount, None


def _parse_term_days(term_arg: str):
    if not term_arg:
        return None, "❌ 请指定定期天数，例如：/钓鱼银行 定期存款 100万 7"
    try:
        term_days = int(term_arg.replace("天", ""))
    except ValueError:
        return None, "❌ 定期天数格式错误，请使用 /钓鱼银行 定期 查看可选档位"
    return term_days, None


def _parse_deposit_id(deposit_id_arg: str):
    if not deposit_id_arg:
        return None, "❌ 请指定定期编号，例如：/钓鱼银行 定期取出 1"
    try:
        deposit_id = int(deposit_id_arg.lstrip("#"))
    except ValueError:
        return None, "❌ 定期编号格式错误"
    return deposit_id, None


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
        f"📄 进行中定期：{result.get('fixed_count', 0)} 笔\n"
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
            "💡 使用：/钓鱼银行 确认预约"
        )
    else:
        message += "\n暂无待确认预约。"
    return message


def _format_fixed_terms(result):
    if not result.get("success"):
        return result.get("message", "查看定期规则失败")
    message = (
        "【🏦 银行定期】\n"
        f"单笔范围：{result['min_amount']:,} - {result['max_amount']:,} 金币\n"
        f"最多进行中：{result['max_active']} 笔\n"
        f"提前取出：收益清零，本金超过 {result['early_withdraw_penalty_threshold']:,} 金币收 "
        f"{result['early_withdraw_penalty_rate'] * 100:.1f}% 违约金\n\n"
        "可选档位：\n"
    )
    for days, rate in sorted(result["terms"].items()):
        message += f"- {days} 天：{rate * 100:.2f}%\n"
    message += "\n用法：/钓鱼银行 定期存款 金额 天数"
    return message


def _format_fixed_deposits(result):
    if not result.get("success"):
        return result.get("message", "查看定期列表失败")
    deposits = result.get("deposits", [])
    if not deposits:
        return "📄 你还没有定期存款。"
    message = "【📄 我的定期存款】\n"
    for deposit in deposits:
        matures_at = safe_datetime_handler(deposit.matures_at)
        status_text = {
            "active": "进行中",
            "completed": "已领取",
            "cancelled": "已提前取出",
        }.get(deposit.status, deposit.status)
        message += (
            f"\n#{deposit.deposit_id} [{status_text}]\n"
            f"本金：{deposit.principal:,} 金币\n"
            f"周期：{deposit.term_days} 天\n"
            f"收益：{deposit.expected_interest:,} 金币（{deposit.interest_rate * 100:.2f}%）\n"
            f"到期：{matures_at}\n"
        )
    message += "\n领取：/钓鱼银行 定期取出 编号\n提前取出：/钓鱼银行 提前取出 编号"
    return message


def _usage():
    return (
        "【🏦 银行帮助】\n"
        "/钓鱼银行 - 查看银行账户\n"
        "/钓鱼银行 存款 金额\n"
        "/钓鱼银行 取款 金额\n"
        "/钓鱼银行 预约取款 金额\n"
        "/钓鱼银行 确认预约\n"
        "/钓鱼银行 取消预约\n"
        "/钓鱼银行 定期\n"
        "/钓鱼银行 定期存款 金额 天数\n"
        "/钓鱼银行 定期列表\n"
        "/钓鱼银行 定期取出 编号\n"
        "/钓鱼银行 提前取出 编号"
    )
