from datetime import datetime, timedelta
from typing import Dict, Any

from ..domain.models import TaxRecord
from ..utils import get_last_reset_time, get_now


class BankService:
    """银行服务：处理存款、取款、预约和免费提现额度。"""

    def __init__(self, bank_repo, user_repo, log_repo, config: Dict[str, Any]):
        self.bank_repo = bank_repo
        self.user_repo = user_repo
        self.log_repo = log_repo
        self.config = config

    @property
    def bank_config(self) -> Dict[str, Any]:
        return self.config.get("bank", {})

    def is_enabled(self) -> bool:
        return self.bank_config.get("enabled", True)

    def _daily_free_limit(self) -> int:
        return int(self.bank_config.get("daily_free_withdraw_limit", 1_000_000))

    def _withdraw_fee_rate(self) -> float:
        return float(self.bank_config.get("withdraw_fee_rate", 0.03))

    def _reservation_threshold(self) -> int:
        return int(self.bank_config.get("reservation_threshold", 5_000_000))

    def _reservation_delay_hours(self) -> int:
        return int(self.bank_config.get("reservation_delay_hours", 24))

    def _max_pending_reservations(self) -> int:
        return int(self.bank_config.get("max_pending_reservations", 1))

    def _reset_date(self) -> str:
        reset_hour = self.config.get("daily_reset_hour", 0)
        return get_last_reset_time(reset_hour).date().isoformat()

    def _refresh_account(self, user_id: str):
        return self.bank_repo.reset_daily_withdrawal_if_needed(user_id, self._reset_date())

    def _calculate_fee(self, account, amount: int) -> int:
        free_limit = self._daily_free_limit()
        already_withdrawn = max(account.today_withdrawn, 0)
        free_remaining = max(free_limit - already_withdrawn, 0)
        taxable_amount = max(amount - free_remaining, 0)
        return int(taxable_amount * self._withdraw_fee_rate())

    def get_overview(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return {"success": False, "message": "用户不存在，请先注册"}
        account = self._refresh_account(user_id)
        pending = self.bank_repo.get_pending_reservation(user_id)
        free_remaining = max(self._daily_free_limit() - account.today_withdrawn, 0)
        return {
            "success": True,
            "user": user,
            "account": account,
            "pending": pending,
            "free_remaining": free_remaining,
            "daily_free_limit": self._daily_free_limit(),
            "withdraw_fee_rate": self._withdraw_fee_rate(),
            "reservation_threshold": self._reservation_threshold(),
            "reservation_delay_hours": self._reservation_delay_hours(),
        }

    def deposit(self, user_id: str, amount: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if amount <= 0:
            return {"success": False, "message": "存款金额必须大于0"}
        success, message, account, wallet_after = self.bank_repo.deposit(user_id, amount)
        if not success:
            return {"success": False, "message": message}
        return {
            "success": True,
            "message": (
                f"✅ 存款成功！\n"
                f"💰 存入：{amount:,} 金币\n"
                f"🏦 银行余额：{account.balance:,} 金币\n"
                f"👛 钱包余额：{wallet_after:,} 金币"
            ),
            "account": account,
            "wallet_after": wallet_after,
        }

    def withdraw(self, user_id: str, amount: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if amount <= 0:
            return {"success": False, "message": "取款金额必须大于0"}
        if amount >= self._reservation_threshold():
            return {
                "success": False,
                "message": (
                    f"❌ 单笔取款达到 {self._reservation_threshold():,} 金币需要预约。\n"
                    f"💡 请使用：/银行 预约取款 {amount}"
                ),
            }

        account = self._refresh_account(user_id)
        fee_amount = self._calculate_fee(account, amount)
        success, message, account, wallet_after = self.bank_repo.withdraw(
            user_id, amount, fee_amount, self._reset_date()
        )
        if not success:
            return {"success": False, "message": message}

        self._record_withdraw_fee(user_id, fee_amount, amount, wallet_after)
        net_amount = amount - fee_amount
        return {
            "success": True,
            "message": self._format_withdraw_success(amount, fee_amount, net_amount, account.balance, wallet_after),
            "account": account,
            "wallet_after": wallet_after,
            "fee_amount": fee_amount,
        }

    def create_reservation(self, user_id: str, amount: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if amount <= 0:
            return {"success": False, "message": "预约取款金额必须大于0"}
        if amount < self._reservation_threshold():
            return {
                "success": False,
                "message": (
                    f"❌ 低于 {self._reservation_threshold():,} 金币无需预约。\n"
                    f"💡 请直接使用：/银行 取款 {amount}"
                ),
            }
        account = self._refresh_account(user_id)
        fee_amount = self._calculate_fee(account, amount)
        ready_at = datetime.now() + timedelta(hours=self._reservation_delay_hours())
        success, message, reservation = self.bank_repo.create_reservation(
            user_id,
            amount,
            fee_amount,
            ready_at,
            self._max_pending_reservations(),
        )
        if not success:
            return {"success": False, "message": message}
        return {
            "success": True,
            "message": (
                f"✅ 大额取款预约成功！\n"
                f"💰 预约金额：{amount:,} 金币\n"
                f"💸 预计手续费：{fee_amount:,} 金币\n"
                f"⏱️ 可确认时间：{ready_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"💡 到时使用：/银行 确认预约"
            ),
            "reservation": reservation,
        }

    def confirm_reservation(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        success, message, reservation, account, wallet_after = self.bank_repo.complete_pending_reservation(
            user_id, self._reset_date()
        )
        if not success:
            if reservation and message == "预约尚未到可取时间":
                return {
                    "success": False,
                    "message": f"❌ 预约尚未到可取时间。\n⏱️ 可确认时间：{reservation.ready_at.strftime('%Y-%m-%d %H:%M:%S')}",
                }
            return {"success": False, "message": message}

        self._record_withdraw_fee(user_id, reservation.fee_amount, reservation.amount, wallet_after)
        net_amount = reservation.amount - reservation.fee_amount
        return {
            "success": True,
            "message": self._format_withdraw_success(
                reservation.amount,
                reservation.fee_amount,
                net_amount,
                account.balance,
                wallet_after,
                prefix="✅ 预约取款完成！",
            ),
            "reservation": reservation,
            "account": account,
            "wallet_after": wallet_after,
        }

    def cancel_reservation(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        success, message, reservation = self.bank_repo.cancel_pending_reservation(user_id)
        if not success:
            return {"success": False, "message": message}
        return {
            "success": True,
            "message": f"✅ 已取消大额取款预约 #{reservation.reservation_id}。",
            "reservation": reservation,
        }

    def _record_withdraw_fee(self, user_id: str, fee_amount: int, amount: int, wallet_after: int) -> None:
        if fee_amount <= 0:
            return
        tax_record = TaxRecord(
            tax_id=0,
            user_id=user_id,
            tax_amount=fee_amount,
            tax_rate=self._withdraw_fee_rate(),
            original_amount=amount,
            balance_after=wallet_after,
            timestamp=get_now(),
            tax_type="银行取款手续费",
        )
        self.log_repo.add_tax_record(tax_record)

    def _format_withdraw_success(
        self,
        amount: int,
        fee_amount: int,
        net_amount: int,
        bank_balance: int,
        wallet_after: int,
        prefix: str = "✅ 取款成功！",
    ) -> str:
        message = (
            f"{prefix}\n"
            f"💰 取款金额：{amount:,} 金币\n"
            f"📥 实际到账：{net_amount:,} 金币\n"
        )
        if fee_amount > 0:
            message += f"💸 取款手续费：{fee_amount:,} 金币\n"
        else:
            message += "💸 取款手续费：0 金币\n"
        message += (
            f"🏦 银行余额：{bank_balance:,} 金币\n"
            f"👛 钱包余额：{wallet_after:,} 金币"
        )
        return message
