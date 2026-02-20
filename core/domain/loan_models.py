"""
借贷系统领域模型
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional


@dataclass
class Loan:
    """借条模型"""
    loan_id: Optional[int] = None  # 借条ID（数据库自增）
    lender_id: str = ""  # 放贷人ID（"SYSTEM"表示系统借款）
    borrower_id: str = ""  # 借款人ID
    principal: int = 0  # 本金
    interest_rate: float = 0.05  # 利息率（默认5%）
    borrowed_at: Optional[datetime] = None  # 借款时间
    due_amount: int = 0  # 应还金额（本金+利息）
    repaid_amount: int = 0  # 已还金额
    status: str = "active"  # 状态: active(进行中), paid(已还清), overdue(逾期), pending(待确认)
    due_date: Optional[datetime] = None  # 还款期限（系统借款专用）
    created_at: Optional[datetime] = None  # 创建时间
    updated_at: Optional[datetime] = None  # 更新时间

    def calculate_due_amount(self) -> int:
        """计算应还总额"""
        return int(self.principal * (1 + self.interest_rate))

    def remaining_amount(self) -> int:
        """计算剩余欠款"""
        return max(0, self.due_amount - self.repaid_amount)

    def is_paid_off(self) -> bool:
        """是否已还清"""
        return self.repaid_amount >= self.due_amount
    
    def is_overdue(self) -> bool:
        """是否已逾期（仅系统借款）"""
        if self.lender_id != "SYSTEM":
            return False
        if not self.due_date:
            return False
        if self.status == "paid":
            return False
        return datetime.now() > self.due_date
    
    def is_system_loan(self) -> bool:
        """是否为系统借款"""
        return self.lender_id == "SYSTEM"
