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

    def _fixed_deposit_config(self) -> Dict[str, Any]:
        return self.bank_config.get("fixed_deposit", {})

    def _fixed_deposit_enabled(self) -> bool:
        return self._fixed_deposit_config().get("enabled", True)

    def _fixed_min_amount(self) -> int:
        return int(self._fixed_deposit_config().get("min_amount", 100_000))

    def _fixed_max_amount(self) -> int:
        return int(self._fixed_deposit_config().get("max_amount", 20_000_000))

    def _fixed_max_active(self) -> int:
        return int(self._fixed_deposit_config().get("max_active_deposits", 5))

    def _fixed_terms(self) -> Dict[str, float]:
        return self._fixed_deposit_config().get("terms", {"1": 0.001, "3": 0.004, "7": 0.01, "30": 0.05})

    def _early_withdraw_penalty_rate(self) -> float:
        return float(self._fixed_deposit_config().get("early_withdraw_penalty_rate", 0.01))

    def _early_withdraw_penalty_threshold(self) -> int:
        return int(self._fixed_deposit_config().get("early_withdraw_penalty_threshold", 1_000_000))

    def _reset_date(self) -> str:
        reset_hour = self.config.get("daily_reset_hour", 0)
        return get_last_reset_time(reset_hour).date().isoformat()

    def _refresh_account(self, user_id: str):
        return self.bank_repo.reset_daily_withdrawal_if_needed(user_id, self._reset_date())

    def _require_user(self, user_id: str):
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return None, {"success": False, "message": "用户不存在，请先注册"}
        return user, None

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
        fixed_count = self.bank_repo.get_active_fixed_deposit_count(user_id)
        free_remaining = max(self._daily_free_limit() - account.today_withdrawn, 0)
        return {
            "success": True,
            "user": user,
            "account": account,
            "pending": pending,
            "fixed_count": fixed_count,
            "free_remaining": free_remaining,
            "daily_free_limit": self._daily_free_limit(),
            "withdraw_fee_rate": self._withdraw_fee_rate(),
            "reservation_threshold": self._reservation_threshold(),
            "reservation_delay_hours": self._reservation_delay_hours(),
        }

    def deposit(self, user_id: str, amount: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
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
        _, error = self._require_user(user_id)
        if error:
            return error
        if amount <= 0:
            return {"success": False, "message": "取款金额必须大于0"}
        if amount >= self._reservation_threshold():
            return {
                "success": False,
                "message": (
                    f"❌ 单笔取款达到 {self._reservation_threshold():,} 金币需要预约。\n"
                    f"💡 请使用：/钓鱼银行 预约取款 {amount}"
                ),
            }

        account = self._refresh_account(user_id)
        fee_amount = self._calculate_fee(account, amount)
        success, message, account, wallet_after, debt_paid = self.bank_repo.withdraw(
            user_id, amount, fee_amount, self._reset_date()
        )
        if not success:
            return {"success": False, "message": message}

        self._record_withdraw_fee(user_id, fee_amount, amount, wallet_after)
        self._record_tax_debt_payment(user_id, debt_paid, amount, wallet_after)
        net_amount = amount - fee_amount
        net_after_debt = net_amount - debt_paid
        return {
            "success": True,
            "message": self._format_withdraw_success(amount, fee_amount, net_after_debt, account.balance, wallet_after, debt_paid=debt_paid),
            "account": account,
            "wallet_after": wallet_after,
            "fee_amount": fee_amount,
            "debt_paid": debt_paid,
        }

    def create_reservation(self, user_id: str, amount: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        if amount <= 0:
            return {"success": False, "message": "预约取款金额必须大于0"}
        if amount < self._reservation_threshold():
            return {
                "success": False,
                "message": (
                    f"❌ 低于 {self._reservation_threshold():,} 金币无需预约。\n"
                    f"💡 请直接使用：/钓鱼银行 取款 {amount}"
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
                f"💡 到时使用：/钓鱼银行 确认预约"
            ),
            "reservation": reservation,
        }

    def get_fixed_terms(self) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if not self._fixed_deposit_enabled():
            return {"success": False, "message": "银行定期存款暂未启用"}
        terms = {
            int(days): float(rate)
            for days, rate in self._fixed_terms().items()
        }
        return {
            "success": True,
            "terms": terms,
            "min_amount": self._fixed_min_amount(),
            "max_amount": self._fixed_max_amount(),
            "max_active": self._fixed_max_active(),
            "early_withdraw_penalty_rate": self._early_withdraw_penalty_rate(),
            "early_withdraw_penalty_threshold": self._early_withdraw_penalty_threshold(),
        }

    def create_fixed_deposit(self, user_id: str, amount: int, term_days: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if not self._fixed_deposit_enabled():
            return {"success": False, "message": "银行定期存款暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        if amount <= 0:
            return {"success": False, "message": "定期存款金额必须大于0"}
        if amount < self._fixed_min_amount():
            return {"success": False, "message": f"定期存款最低金额为 {self._fixed_min_amount():,} 金币"}
        if amount > self._fixed_max_amount():
            return {"success": False, "message": f"定期存款最高金额为 {self._fixed_max_amount():,} 金币"}

        terms = self._fixed_terms()
        term_key = str(term_days)
        if term_key not in terms:
            available = "、".join(sorted(terms.keys(), key=lambda x: int(x)))
            return {"success": False, "message": f"不支持的定期天数，可选：{available} 天"}

        interest_rate = float(terms[term_key])
        expected_interest = int(amount * interest_rate)
        matures_at = datetime.now() + timedelta(days=term_days)
        success, message, deposit, account = self.bank_repo.create_fixed_deposit(
            user_id=user_id,
            principal=amount,
            term_days=term_days,
            interest_rate=interest_rate,
            expected_interest=expected_interest,
            matures_at=matures_at,
            max_active=self._fixed_max_active(),
        )
        if not success:
            return {"success": False, "message": message}
        return {
            "success": True,
            "message": (
                f"✅ 定期存款创建成功！\n"
                f"🧾 编号：#{deposit.deposit_id}\n"
                f"💰 本金：{amount:,} 金币\n"
                f"📈 到期收益：{expected_interest:,} 金币（{interest_rate * 100:.2f}%）\n"
                f"⏱️ 到期时间：{matures_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🏦 活期余额：{account.balance:,} 金币"
            ),
            "deposit": deposit,
            "account": account,
        }

    def list_fixed_deposits(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if not self._fixed_deposit_enabled():
            return {"success": False, "message": "银行定期存款暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        deposits = self.bank_repo.get_fixed_deposits(user_id)
        return {
            "success": True,
            "deposits": deposits,
            "terms": self.get_fixed_terms(),
        }

    def get_admin_summary_for_users(self, users) -> Dict[str, Dict[str, Any]]:
        user_ids = [user.user_id for user in users]
        return self.bank_repo.get_admin_summary_for_users(user_ids)

    def get_admin_totals(self) -> Dict[str, int]:
        return self.bank_repo.get_admin_totals()

    def get_fixed_deposits_for_admin(self, search: str = None, limit: int = 100):
        return self.bank_repo.get_fixed_deposits_for_admin(search=search or None, limit=limit)

    def get_tax_debt_summary(self, user_id: str = None) -> Dict[str, int]:
        return self.bank_repo.get_tax_debt_summary(user_id=user_id or None)

    def get_tax_debts_for_admin(self, user_id: str = None, limit: int = 50):
        return self.bank_repo.get_tax_debts_for_admin(user_id=user_id or None, limit=limit)

    def complete_fixed_deposit(self, user_id: str, deposit_id: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if not self._fixed_deposit_enabled():
            return {"success": False, "message": "银行定期存款暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        success, message, deposit, account, debt_paid = self.bank_repo.complete_fixed_deposit(user_id, deposit_id)
        if not success:
            if deposit and message == "定期存款尚未到期":
                return {
                    "success": False,
                    "message": f"❌ 定期存款尚未到期。\n⏱️ 到期时间：{deposit.matures_at.strftime('%Y-%m-%d %H:%M:%S')}",
                }
            return {"success": False, "message": message}
        payout = deposit.principal + deposit.expected_interest
        net_payout = payout - debt_paid
        self._record_tax_debt_payment(user_id, debt_paid, payout, account.balance)
        return {
            "success": True,
            "message": (
                f"✅ 定期存款领取成功！\n"
                f"🧾 编号：#{deposit.deposit_id}\n"
                f"💰 本金：{deposit.principal:,} 金币\n"
                f"📈 收益：{deposit.expected_interest:,} 金币\n"
                f"📥 入账活期：{net_payout:,} 金币\n"
                f"🧾 欠税补扣：{debt_paid:,} 金币\n"
                f"🏦 活期余额：{account.balance:,} 金币"
            ),
            "deposit": deposit,
            "account": account,
        }

    def cancel_fixed_deposit(self, user_id: str, deposit_id: int) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        if not self._fixed_deposit_enabled():
            return {"success": False, "message": "银行定期存款暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        deposit_candidates = self.bank_repo.get_fixed_deposits(user_id, limit=50)
        target = next((d for d in deposit_candidates if d.deposit_id == deposit_id and d.status == "active"), None)
        penalty_amount = 0
        if target and target.principal > self._early_withdraw_penalty_threshold():
            penalty_amount = int(target.principal * self._early_withdraw_penalty_rate())
        success, message, deposit, account, penalty_amount, debt_paid = self.bank_repo.cancel_fixed_deposit(
            user_id, deposit_id, penalty_amount
        )
        if not success:
            return {"success": False, "message": message}
        returned_amount = deposit.principal - penalty_amount
        net_returned = returned_amount - debt_paid
        self._record_tax_debt_payment(user_id, debt_paid, returned_amount, account.balance)
        return {
            "success": True,
            "message": (
                f"✅ 定期存款已提前取出。\n"
                f"🧾 编号：#{deposit.deposit_id}\n"
                f"💰 返还本金：{net_returned:,} 金币\n"
                f"📈 到期收益：0 金币\n"
                f"💸 违约金：{penalty_amount:,} 金币\n"
                f"🧾 欠税补扣：{debt_paid:,} 金币\n"
                f"🏦 活期余额：{account.balance:,} 金币"
            ),
            "deposit": deposit,
            "account": account,
            "penalty_amount": penalty_amount,
        }

    def confirm_reservation(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
        pending = self.bank_repo.get_pending_reservation(user_id)
        if pending and pending.amount < self._reservation_threshold():
            cancel_result = self.cancel_reservation(user_id)
            if cancel_result.get("success"):
                return {
                    "success": False,
                    "message": (
                        f"❌ 预约金额已低于当前大额预约门槛 {self._reservation_threshold():,} 金币，"
                        f"已自动取消预约 #{pending.reservation_id}。\n"
                        f"💡 请改用：/钓鱼银行 取款 {pending.amount}"
                    ),
                }
            return cancel_result
        success, message, reservation, account, wallet_after, debt_paid = self.bank_repo.complete_pending_reservation(
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
        self._record_tax_debt_payment(user_id, debt_paid, reservation.amount, wallet_after)
        net_amount = reservation.amount - reservation.fee_amount - debt_paid
        return {
            "success": True,
            "message": self._format_withdraw_success(
                reservation.amount,
                reservation.fee_amount,
                net_amount,
                account.balance,
                wallet_after,
                prefix="✅ 预约取款完成！",
                debt_paid=debt_paid,
            ),
            "reservation": reservation,
            "account": account,
            "wallet_after": wallet_after,
        }

    def cancel_reservation(self, user_id: str) -> Dict[str, Any]:
        if not self.is_enabled():
            return {"success": False, "message": "银行系统暂未启用"}
        _, error = self._require_user(user_id)
        if error:
            return error
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

    def _record_tax_debt_payment(self, user_id: str, debt_paid: int, original_amount: int, balance_after: int) -> None:
        if debt_paid <= 0:
            return
        tax_record = TaxRecord(
            tax_id=0,
            user_id=user_id,
            tax_amount=debt_paid,
            tax_rate=0.0,
            original_amount=original_amount,
            balance_after=balance_after,
            timestamp=get_now(),
            tax_type="欠税补扣",
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
        debt_paid: int = 0,
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
        if debt_paid > 0:
            message += f"🧾 欠税补扣：{debt_paid:,} 金币\n"
        message += (
            f"🏦 银行余额：{bank_balance:,} 金币\n"
            f"👛 钱包余额：{wallet_after:,} 金币"
        )
        return message
