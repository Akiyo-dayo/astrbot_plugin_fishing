"""
银行系统领域模型
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class BankAccount:
    user_id: str
    balance: int = 0
    today_withdrawn: int = 0
    last_withdraw_reset_date: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class BankWithdrawReservation:
    reservation_id: Optional[int]
    user_id: str
    amount: int
    fee_amount: int
    status: str
    ready_at: datetime
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
