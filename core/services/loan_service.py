"""
借贷系统服务层
"""

from datetime import datetime, timedelta
from typing import List, Tuple

from astrbot.api import logger

from ..repositories.sqlite_loan_repo import SqliteLoanRepository
from ..repositories.sqlite_user_repo import SqliteUserRepository
from ..domain.loan_models import Loan


class LoanService:
    """借贷业务逻辑服务"""

    def __init__(
        self,
        loan_repo: SqliteLoanRepository,
        user_repo: SqliteUserRepository,
        default_interest_rate: float = 0.05,
        system_loan_ratio: float = 0.10,
        system_loan_days: int = 7
    ):
        self.loan_repo = loan_repo
        self.user_repo = user_repo
        self.default_interest_rate = default_interest_rate
        self.system_loan_ratio = system_loan_ratio  # 系统借款比例（历史最高金币的10%）
        self.system_loan_days = system_loan_days  # 系统借款期限（天）

    def _update_coins(self, user_id: str, amount: int) -> bool:
        """更新用户金币的辅助方法"""
        user = self.user_repo.get_by_id(user_id)
        if not user:
            return False
        user.coins += amount
        if user.coins < 0:
            user.coins = 0
        try:
            self.user_repo.update(user)
            return True
        except Exception as e:
            logger.error(f"更新用户 {user_id} 金币失败: {e}")
            return False

    def create_loan(
        self,
        lender_id: str,
        borrower_id: str,
        principal: int,
        interest_rate: float = None
    ) -> Tuple[bool, str, Loan]:
        """
        创建借条
        
        返回: (成功标志, 消息, 借条对象)
        """
        # 参数验证
        if lender_id == borrower_id:
            return False, "❌ 不能借钱给自己", None
        
        if principal <= 0:
            return False, "❌ 借款金额必须大于0", None

        # 使用默认利率或自定义利率
        if interest_rate is None:
            interest_rate = self.default_interest_rate

        # 计算应还金额
        due_amount = int(principal * (1 + interest_rate))

        # 创建借条对象
        loan = Loan(
            lender_id=lender_id,
            borrower_id=borrower_id,
            principal=principal,
            interest_rate=interest_rate,
            borrowed_at=datetime.now(),
            due_amount=due_amount,
            repaid_amount=0,
            status="pending" if lender_id != "SYSTEM" else "active"
        )

        try:
            # 如果是玩家间借款，先不扣款，只创建待确认记录
            if lender_id != "SYSTEM":
                loan_id = self.loan_repo.create_loan(loan)
                loan.loan_id = loan_id
                logger.info(f"创建待确认借条: {lender_id} -> {borrower_id}, 本金: {principal}")
                return True, (
                    f"📝 借款申请已发起！\n"
                    f"👤 借款人：{borrower_id}\n"
                    f"💰 金额：{principal:,} 金币\n"
                    f"📈 利率：{interest_rate*100:.2f}%\n"
                    f"🔖 借条ID：#{loan_id}\n\n"
                    f"💡 请借款人输入「确认借款 #{loan_id}」以领取金币。"
                ), loan

            # 开启事务（系统借款立即生效）
            with self.user_repo._get_connection() as conn:
                # 再次检查放贷人余额（在同一个连接中）
                cursor = conn.cursor()
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (lender_id,))
                row = cursor.fetchone()
                if not row:
                    return False, "❌ 放贷人账户不存在", None
                if row[0] < principal:
                    return False, f"❌ 你的金币不足，当前余额：{row[0]:,} 金币", None
                
                # 检查借款人
                cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (borrower_id,))
                if not cursor.fetchone():
                    return False, "❌ 借款人账户不存在", None

                # 执行扣款
                cursor.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (principal, lender_id))
                # 执行放款
                cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (principal, borrower_id))
                # 更新 max_coins
                cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (borrower_id,))

                # 保存借条
                now = datetime.now()
                cursor.execute("""
                    INSERT INTO loans (
                        lender_id, borrower_id, principal, interest_rate,
                        borrowed_at, due_amount, repaid_amount, status,
                        due_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    loan.lender_id, loan.borrower_id, loan.principal, loan.interest_rate,
                    loan.borrowed_at, loan.due_amount, loan.repaid_amount, loan.status,
                    loan.due_date, now, now
                ))
                loan_id = cursor.lastrowid
                loan.loan_id = loan_id
                
                # Commit is handled by the 'with' block of the connection

            logger.info(f"创建借条成功: {lender_id} -> {borrower_id}, 本金: {principal}, 利率: {interest_rate}")
            return True, f"✅ 借款成功！\n💰 本金：{principal:,} 金币\n📈 利率：{interest_rate*100:.2f}%\n💵 应还：{due_amount:,} 金币\n🔖 借条ID：#{loan_id}", loan

        except Exception as e:
            logger.error(f"创建借条失败: {e}")
            return False, f"❌ 创建借条失败：{str(e)}", None

    def confirm_loan(self, loan_id: int, user_id: str) -> Tuple[bool, str]:
        """
        确认（接受）借款申请
        
        返回: (成功标志, 消息)
        """
        loan = self.loan_repo.get_loan_by_id(loan_id)
        if not loan:
            return False, "❌ 借条不存在"
        
        if loan.borrower_id != user_id:
            return False, "❌ 你不是这笔借款的借款人"
        
        if loan.status != "pending":
            return False, f"❌ 该借条状态为 {loan.status}，无法确认"

        try:
            with self.user_repo._get_connection() as conn:
                cursor = conn.cursor()
                
                # 检查放贷人余额
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (loan.lender_id,))
                row = cursor.fetchone()
                if not row:
                    return False, "❌ 放贷人账户不存在"
                
                if row[0] < loan.principal:
                    return False, "❌ 放贷人账户金币不足，该借条已失效"

                # 执行扣款
                cursor.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (loan.principal, loan.lender_id))
                # 执行放款
                cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (loan.principal, loan.borrower_id))
                # 更新 max_coins
                cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (loan.borrower_id,))

                # 更新借条状态
                cursor.execute("""
                    UPDATE loans SET status = 'active', borrowed_at = ?, updated_at = ? WHERE loan_id = ?
                """, (datetime.now(), datetime.now(), loan_id))

            logger.info(f"确认借条成功: #{loan_id}, {loan.lender_id} -> {loan.borrower_id}")
            return True, f"✅ 借款确认成功！你已收到 {loan.principal:,} 金币。"

        except Exception as e:
            logger.error(f"确认借条失败: {e}")
            return False, f"❌ 确认失败：{str(e)}"

    def repay_all_loans(self, borrower_id: str) -> Tuple[bool, str]:
        """
        一键还清所有能还的借条
        优先还系统借款，然后按利率从高到低排序，最后按时间
        """
        try:
            with self.user_repo._get_connection() as conn:
                cursor = conn.cursor()
                
                # 检查余额
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (borrower_id,))
                row = cursor.fetchone()
                if not row or row[0] <= 0:
                    return False, "❌ 你兜里一分钱都没有，还什么债呀"
                
                initial_balance = row[0]
                remaining_balance = initial_balance

                # 获取所有待还借条
                cursor.execute("""
                    SELECT * FROM loans 
                    WHERE borrower_id = ? AND status IN ('active', 'overdue')
                """, (borrower_id,))
                
                rows = cursor.fetchall()
                if not rows:
                    return True, "✅ 你目前没有欠债，无债一身轻！"

                all_loans = [self.loan_repo._row_to_loan(r) for r in rows]
                
                # 排序逻辑：
                # 1. 系统借款优先 (is_system_loan=True)
                # 2. 利率从高到低 (interest_rate descending)
                # 3. 借款时间从早到晚 (borrowed_at ascending)
                all_loans.sort(key=lambda x: (
                    0 if x.is_system_loan() else 1,
                    -x.interest_rate,
                    x.borrowed_at
                ))

                total_repaid = 0
                repaid_details = []

                for loan in all_loans:
                    if remaining_balance <= 0:
                        break
                    
                    debt = loan.remaining_amount()
                    repay_amount = min(remaining_balance, debt)
                    
                    if repay_amount <= 0:
                        continue
                        
                    new_repaid = loan.repaid_amount + repay_amount
                    new_status = "paid" if new_repaid >= loan.due_amount else loan.status
                    
                    # 更新借条
                    cursor.execute("""
                        UPDATE loans SET repaid_amount = ?, status = ?, updated_at = ? WHERE loan_id = ?
                    """, (new_repaid, new_status, datetime.now(), loan.loan_id))
                    
                    # 如果不是系统借款，钱给放贷人
                    if not loan.is_system_loan():
                        cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (repay_amount, loan.lender_id))
                        cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (loan.lender_id,))
                    
                    total_repaid += repay_amount
                    remaining_balance -= repay_amount
                    
                    lender_name = "系统" if loan.is_system_loan() else f"玩家({loan.lender_id})"
                    repaid_details.append(f"#{loan.loan_id}({lender_name}): {repay_amount:,}")

                # 扣除借款人余额
                cursor.execute("UPDATE users SET coins = ? WHERE user_id = ?", (remaining_balance, borrower_id))

            msg = f"🏦 **一键还债结算**\n"
            msg += f"💰 总计偿还：{total_repaid:,} 金币\n"
            msg += f"👛 剩余余额：{remaining_balance:,} 金币\n\n"
            msg += "📝 详情：\n" + "\n".join(repaid_details)
            
            return True, msg

        except Exception as e:
            logger.error(f"一键还债失败: {e}")
            return False, f"❌ 一键还债失败：{str(e)}"

    def repay_loan(
        self,
        borrower_id: str,
        lender_id: str,
        amount: int
    ) -> Tuple[bool, str]:
        """
        借款人还款（支持还系统借款和玩家借款）
        
        返回: (成功标志, 消息)
        """
        # 参数验证
        if amount <= 0:
            return False, "❌ 还款金额必须大于0"

        try:
            with self.user_repo._get_connection() as conn:
                cursor = conn.cursor()
                # 检查借款人余额
                cursor.execute("SELECT coins FROM users WHERE user_id = ?", (borrower_id,))
                row = cursor.fetchone()
                if not row:
                    return False, "❌ 借款人账户不存在"
                if row[0] < amount:
                    return False, f"❌ 你的金币不足，当前余额：{row[0]:,} 金币"

                # 获取借条
                if lender_id == "SYSTEM":
                    cursor.execute("""
                        SELECT * FROM loans WHERE borrower_id = ? AND lender_id = 'SYSTEM' AND status IN ('active', 'overdue')
                        ORDER BY borrowed_at ASC
                    """, (borrower_id,))
                else:
                    cursor.execute("""
                        SELECT * FROM loans WHERE borrower_id = ? AND lender_id = ? AND status IN ('active', 'overdue')
                        ORDER BY borrowed_at ASC
                    """, (borrower_id, lender_id))
                
                rows = cursor.fetchall()
                if not rows:
                    lender_name = "系统" if lender_id == "SYSTEM" else "对方"
                    return False, f"❌ 你没有欠{lender_name}的借条"

                active_loans = [self.loan_repo._row_to_loan(r) for r in rows]
                
                total_repaid = 0
                paid_off_loans = []
                remaining_amount = amount

                for loan in active_loans:
                    if remaining_amount <= 0:
                        break
                    
                    # 计算这笔借条还需要还多少
                    remaining_debt = loan.remaining_amount()
                    repay_this_loan = min(remaining_amount, remaining_debt)
                    
                    new_repaid_amount = loan.repaid_amount + repay_this_loan
                    new_status = "paid" if new_repaid_amount >= loan.due_amount else "active"
                    
                    # 更新借条
                    cursor.execute("""
                        UPDATE loans SET repaid_amount = ?, status = ?, updated_at = ? WHERE loan_id = ?
                    """, (new_repaid_amount, new_status, datetime.now(), loan.loan_id))
                    
                    total_repaid += repay_this_loan
                    remaining_amount -= repay_this_loan
                    
                    if new_status == "paid":
                        paid_off_loans.append(loan.loan_id)

                # 扣除借款人金币
                cursor.execute("UPDATE users SET coins = MAX(0, coins - ?) WHERE user_id = ?", (total_repaid, borrower_id))
                
                # 增加放贷人金币
                if lender_id != "SYSTEM":
                    cursor.execute("UPDATE users SET coins = coins + ? WHERE user_id = ?", (total_repaid, lender_id))
                    # 更新放贷人的历史最高金币
                    cursor.execute("UPDATE users SET max_coins = coins WHERE user_id = ? AND coins > max_coins", (lender_id,))

            logger.info(f"还款成功: {borrower_id} -> {lender_id}, 金额: {total_repaid}")
            lender_name = "系统" if lender_id == "SYSTEM" else "对方"
            msg = f"✅ 还款成功！\n💰 已还：{total_repaid:,} 金币"
            if paid_off_loans:
                msg += f"\n🎉 已还清借条：{', '.join([f'#{lid}' for lid in paid_off_loans])}"
            
            # 这里可以用普通的 repo 方法，因为上面的事务已经 commit 了
            if lender_id == "SYSTEM":
                remaining_loan = self.loan_repo.get_active_system_loan(borrower_id)
                remaining_loans = [remaining_loan] if remaining_loan else []
            else:
                remaining_loans = self.loan_repo.get_active_loans_between_users(lender_id, borrower_id)
            
            if remaining_loans:
                total_remaining = sum(l.remaining_amount() for l in remaining_loans)
                msg += f"\n📋 剩余欠款：{total_remaining:,} 金币"
            else:
                msg += f"\n🎊 已还清所有欠{lender_name}的借条！"
            
            return True, msg

        except Exception as e:
            logger.error(f"还款失败: {e}")
            return False, f"❌ 还款失败：{str(e)}"

    def force_collect(
        self,
        lender_id: str,
        borrower_id: str,
        amount: int = None
    ) -> Tuple[bool, str]:
        """
        放贷人强制收款
        
        amount为None时收取全部欠款
        返回: (成功标志, 消息)
        """
        # 获取两人之间的进行中借条
        active_loans = self.loan_repo.get_active_loans_between_users(lender_id, borrower_id)
        if not active_loans:
            return False, f"❌ 对方没有欠你的借条"

        # 计算总欠款
        total_debt = sum(loan.remaining_amount() for loan in active_loans)
        
        # 确定收款金额
        if amount is None:
            collect_amount = total_debt
        else:
            if amount <= 0:
                return False, "❌ 收款金额必须大于0"
            collect_amount = min(amount, total_debt)

        # 检查借款人余额
        borrower = self.user_repo.get_by_id(borrower_id)
        if not borrower:
            return False, "❌ 借款人账户不存在"
        
        # 实际能收到的金额（不能超过借款人余额）
        actual_collect = min(collect_amount, borrower.coins)
        
        if actual_collect <= 0:
            return False, f"❌ 对方金币余额为0，无法收款"

        # 按照借款时间排序，优先收最早的借条
        active_loans.sort(key=lambda x: x.borrowed_at)
        
        total_collected = 0
        paid_off_loans = []
        remaining_amount = actual_collect

        try:
            for loan in active_loans:
                if remaining_amount <= 0:
                    break

                # 计算这笔借条还需要还多少
                remaining_debt = loan.remaining_amount()
                
                # 计算这次能收多少
                collect_this_loan = min(remaining_amount, remaining_debt)
                
                # 更新借条的已还金额
                new_repaid_amount = loan.repaid_amount + collect_this_loan
                new_status = "paid" if new_repaid_amount >= loan.due_amount else "active"
                
                self.loan_repo.update_loan_repayment(loan.loan_id, new_repaid_amount, new_status)
                
                # 更新统计
                total_collected += collect_this_loan
                remaining_amount -= collect_this_loan
                
                if new_status == "paid":
                    paid_off_loans.append(loan.loan_id)

            # 扣除借款人金币
            success = self._update_coins(borrower_id, -total_collected)
            if not success:
                return False, "❌ 扣除借款人金币失败"

            # 增加放贷人金币
            success = self._update_coins(lender_id, total_collected)
            if not success:
                # 回滚借款人金币
                self._update_coins(borrower_id, total_collected)
                return False, "❌ 增加放贷人金币失败"

            logger.info(f"强制收款成功: {lender_id} <- {borrower_id}, 金额: {total_collected}")
            
            msg = f"✅ 强制收款成功！\n💰 已收：{total_collected:,} 金币"
            if paid_off_loans:
                msg += f"\n🎉 已还清借条：{', '.join([f'#{lid}' for lid in paid_off_loans])}"
            
            # 检查是否还有未还清的借条
            remaining_loans = self.loan_repo.get_active_loans_between_users(lender_id, borrower_id)
            if remaining_loans:
                total_remaining = sum(loan.remaining_amount() for loan in remaining_loans)
                msg += f"\n📋 剩余欠款：{total_remaining:,} 金币"
            
            if actual_collect < collect_amount:
                msg += f"\n⚠️ 对方余额不足，仅收到 {actual_collect:,} / {collect_amount:,} 金币"
            
            return True, msg

        except Exception as e:
            logger.error(f"强制收款失败: {e}")
            return False, f"❌ 强制收款失败：{str(e)}"

    def get_user_loans_summary(self, user_id: str) -> str:
        """获取用户借贷汇总信息"""
        # 作为放贷人的借条
        lent_loans = self.loan_repo.get_loans_by_lender(user_id, status="active")
        total_lent = sum(loan.principal for loan in lent_loans)
        total_receivable = sum(loan.remaining_amount() for loan in lent_loans)

        # 作为借款人的借条
        borrowed_loans = self.loan_repo.get_loans_by_borrower(user_id, status="active")
        total_borrowed = sum(loan.principal for loan in borrowed_loans)
        total_payable = sum(loan.remaining_amount() for loan in borrowed_loans)

        msg = "📊 你的借贷汇总\n\n"
        msg += f"💸 放贷中：{len(lent_loans)} 笔\n"
        msg += f"   本金：{total_lent:,} 金币\n"
        msg += f"   应收：{total_receivable:,} 金币\n\n"
        msg += f"💰 借款中：{len(borrowed_loans)} 笔\n"
        msg += f"   本金：{total_borrowed:,} 金币\n"
        msg += f"   应还：{total_payable:,} 金币\n"

        return msg

    def get_all_loans_list(self, user_id: str = None) -> str:
        """获取所有借条列表（可选过滤某个用户）"""
        if user_id:
            lent_loans = self.loan_repo.get_loans_by_lender(user_id, status="active")
            borrowed_loans = self.loan_repo.get_loans_by_borrower(user_id, status="active")
            all_loans = lent_loans + borrowed_loans
            
            # 去重（避免同一笔借条出现两次）
            seen = set()
            unique_loans = []
            for loan in all_loans:
                if loan.loan_id not in seen:
                    seen.add(loan.loan_id)
                    unique_loans.append(loan)
            
            loans = sorted(unique_loans, key=lambda x: x.borrowed_at, reverse=True)
        else:
            loans = self.loan_repo.get_all_active_loans()

        if not loans:
            return "📋 当前没有进行中的借条"

        msg = "📋 借条列表\n\n"
        for i, loan in enumerate(loans[:20], 1):  # 限制显示20条
            remaining = loan.remaining_amount()
            
            # 实时检查逾期状态
            if loan.is_overdue() and loan.status == "active":
                self.loan_repo.update_loan_repayment(loan.loan_id, loan.repaid_amount, "overdue")
                loan.status = "overdue"
            
            # 状态标识
            status_icon = ""
            if loan.is_system_loan():
                if loan.status == "overdue":
                    status_icon = "🔴逾期"
                elif loan.due_date:
                    days_left = (loan.due_date - datetime.now()).days
                    if days_left <= 1:
                        status_icon = "⏰紧急"
                    elif days_left <= 3:
                        status_icon = "⚠️即将到期"
            
            msg += f"{i}. 借条 #{loan.loan_id} {status_icon}\n"
            
            # 放贷人显示
            lender_display = "系统" if loan.lender_id == "SYSTEM" else loan.lender_id
            msg += f"   放贷人：{lender_display}\n"
            msg += f"   借款人：{loan.borrower_id}\n"
            msg += f"   本金：{loan.principal:,} 金币\n"
            msg += f"   利率：{loan.interest_rate*100:.2f}%\n"
            msg += f"   应还：{loan.due_amount:,} 金币\n"
            msg += f"   已还：{loan.repaid_amount:,} 金币\n"
            msg += f"   剩余：{remaining:,} 金币\n"
            
            # 系统借款显示期限
            if loan.is_system_loan() and loan.due_date:
                days_left = (loan.due_date - datetime.now()).days
                hours_left = int((loan.due_date - datetime.now()).total_seconds() / 3600)
                
                if days_left > 0:
                    msg += f"   ⏰ 剩余：{days_left}天\n"
                elif hours_left > 0:
                    msg += f"   ⏰ 剩余：{hours_left}小时\n"
                else:
                    msg += f"   ⏰ 已逾期\n"
                    
            msg += f"   时间：{loan.borrowed_at.strftime('%Y-%m-%d %H:%M')}\n\n"

        if len(loans) > 20:
            msg += f"... 还有 {len(loans) - 20} 笔借条未显示"

        return msg

    def borrow_from_system(self, borrower_id: str, amount: int = None) -> Tuple[bool, str, Loan]:
        """
        向系统借款
        
        amount为None时自动借最大额度
        返回: (成功标志, 消息, 借条对象)
        """
        # 检查借款人账户
        borrower = self.user_repo.get_by_id(borrower_id)
        if not borrower:
            return False, "❌ 账户不存在", None

        # 检查是否已有未还清的系统借款
        existing_loan = self.loan_repo.get_active_system_loan(borrower_id)
        if existing_loan:
            remaining = existing_loan.remaining_amount()
            return False, f"❌ 你已有未还清的系统借款\n💰 剩余欠款：{remaining:,} 金币\n💡 请先还清后再借款", None

        # 检查是否有逾期借款
        if self.loan_repo.has_overdue_system_loan(borrower_id):
            return False, "❌ 你有逾期未还的系统借款，暂时无法借款\n💡 请先还清逾期欠款", None

        # 计算可借额度（历史最高金币的10%）
        max_coins = getattr(borrower, 'max_coins', borrower.coins)
        max_borrow_amount = int(max_coins * self.system_loan_ratio)

        if max_borrow_amount <= 0:
            return False, "❌ 你的借款额度不足\n💡 额度 = 历史最高金币 × 10%\n💡 多赚点金币再来吧~", None

        # 确定借款金额
        if amount is None:
            amount = max_borrow_amount
        else:
            if amount <= 0:
                return False, "❌ 借款金额必须大于0", None
            if amount > max_borrow_amount:
                return False, f"❌ 借款金额超出额度\n💰 你的最大额度：{max_borrow_amount:,} 金币\n💡 额度 = 历史最高金币({max_coins:,}) × 10%", None

        # 计算应还金额和还款期限
        due_amount = int(amount * (1 + self.default_interest_rate))
        due_date = datetime.now() + timedelta(days=self.system_loan_days)

        # 创建系统借条
        loan = Loan(
            lender_id="SYSTEM",
            borrower_id=borrower_id,
            principal=amount,
            interest_rate=self.default_interest_rate,
            borrowed_at=datetime.now(),
            due_amount=due_amount,
            repaid_amount=0,
            status="active",
            due_date=due_date
        )

        try:
            # 增加借款人金币
            success = self._update_coins(borrower_id, amount)
            if not success:
                return False, "❌ 系统借款失败：无法发放金币", None

            # 保存借条
            loan_id = self.loan_repo.create_loan(loan)
            loan.loan_id = loan_id

            logger.info(f"系统借款成功: {borrower_id}, 金额: {amount}, 期限: {self.system_loan_days}天")
            
            return True, (
                f"✅ 系统借款成功！\n"
                f"💰 本金：{amount:,} 金币\n"
                f"📈 利率：{self.default_interest_rate*100:.2f}%\n"
                f"💵 应还：{due_amount:,} 金币\n"
                f"⏰ 还款期限：{due_date.strftime('%Y-%m-%d %H:%M')}\n"
                f"⚠️ 逾期将禁止参与骰宝和擦弹游戏\n"
                f"🔖 借条ID：#{loan_id}"
            ), loan

        except Exception as e:
            logger.error(f"系统借款失败: {e}")
            return False, f"❌ 系统借款失败：{str(e)}", None

    def check_user_overdue_status(self, user_id: str) -> Tuple[bool, str]:
        """
        检查用户是否有逾期借款（用于游戏限制）
        实时判断并更新逾期状态
        
        返回: (是否逾期, 提示消息)
        """
        # 获取用户的所有系统借款
        loans = self.loan_repo.get_loans_by_borrower(user_id)
        system_loans = [loan for loan in loans if loan.is_system_loan() and loan.status in ('active', 'overdue')]
        
        overdue_loans = []
        for loan in system_loans:
            if loan.is_overdue():
                # 实时更新逾期状态
                if loan.status == "active":
                    self.loan_repo.update_loan_repayment(loan.loan_id, loan.repaid_amount, "overdue")
                    loan.status = "overdue"
                overdue_loans.append(loan)
        
        if overdue_loans:
            total_debt = sum(loan.remaining_amount() for loan in overdue_loans)
            return True, (
                f"❌ 你有逾期未还的系统借款，暂时无法参与该游戏\n"
                f"💰 逾期欠款：{total_debt:,} 金币\n"
                f"💡 请尽快还款以解除限制"
            )
        
        return False, ""

    def get_total_debt(self, user_id: str) -> int:
        """
        获取用户的总欠款（包括系统借款和玩家借款）
        
        返回: 总欠款金额
        """
        borrowed_loans = self.loan_repo.get_loans_by_borrower(user_id, status="active")
        
        # 实时检查并更新逾期状态
        for loan in borrowed_loans:
            if loan.is_overdue() and loan.status == "active":
                self.loan_repo.update_loan_repayment(loan.loan_id, loan.repaid_amount, "overdue")
        
        # 重新获取（包括刚标记为逾期的）
        all_borrowed = self.loan_repo.get_loans_by_borrower(user_id)
        active_borrowed = [loan for loan in all_borrowed if loan.status in ('active', 'overdue')]
        
        return sum(loan.remaining_amount() for loan in active_borrowed)
