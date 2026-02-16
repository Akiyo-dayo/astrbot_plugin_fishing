"""
借贷系统指令处理器
"""

import re
from typing import Optional

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent

from ..utils import parse_amount, parse_target_user_id


class LoanHandlers:
    """借贷系统指令处理器"""

    def __init__(self, loan_service, user_service):
        self.loan_service = loan_service
        self.user_service = user_service

    async def handle_borrow_money(self, event: AstrMessageEvent, args: list):
        """
        处理借钱指令
        格式：借他@用户 金额 或 借她@用户 金额 或 借它@用户 金额
        """
        text = event.message_str
        
        # 匹配借款格式：借他/借她/借它 @用户 金额
        pattern = r"借[他她它]\s*@(\S+)\s+(.+)"
        match = re.search(pattern, text)
        
        if not match:
            yield event.plain_result(
                "❌ 格式错误！\n"
                "💡 正确格式：借他@用户 金额\n"
                "📝 示例：借她@张三 1000 或 借他@李四 一万"
            )
            return

        target_user_str = match.group(1)
        amount_str = match.group(2).strip()

        # 解析目标用户ID
        borrower_id = parse_target_user_id(event, target_user_str)
        if not borrower_id:
            yield event.plain_result(
                "❌ 无法识别借款人\n"
                "💡 请使用 @用户 或 QQ号"
            )
            return

        # 解析金额
        try:
            amount = parse_amount(amount_str)
        except ValueError as e:
            yield event.plain_result(f"❌ 金额格式错误：{str(e)}\n💡 支持：1000、一千、1万等")
            return

        if amount <= 0:
            yield event.plain_result("❌ 借款金额必须大于0")
            return

        # 获取放贷人ID
        lender_id = event.get_sender_id()

        # 创建借条
        success, message, loan = self.loan_service.create_loan(
            lender_id=lender_id,
            borrower_id=borrower_id,
            principal=amount
        )

        yield event.plain_result(message)

    async def handle_repay_money(self, event: AstrMessageEvent, args: list):
        """
        处理还钱指令
        格式：还他@用户 金额 或 还她@用户 金额 或 还它@用户 金额
        也支持：还系统 金额 或 还钱 金额（还系统借款）
        """
        text = event.message_str
        
        # 匹配格式1：还他/还她/还它 @用户 金额
        pattern1 = r"还[他她它]\s*@(\S+)\s+(.+)"
        match1 = re.search(pattern1, text)
        
        # 匹配格式2：还系统 金额 或 还钱 金额
        pattern2 = r"还(?:系统|钱)\s+(.+)"
        match2 = re.search(pattern2, text)
        
        if match1:
            # 玩家间还款或还@SYSTEM
            target_user_str = match1.group(1)
            amount_str = match1.group(2).strip()
            
            # 解析目标用户ID
            if target_user_str.upper() == "SYSTEM" or target_user_str == "系统":
                lender_id = "SYSTEM"
            else:
                lender_id = parse_target_user_id(event, target_user_str)
                if not lender_id:
                    yield event.plain_result(
                        "❌ 无法识别放贷人\n"
                        "💡 请使用 @用户 或 QQ号\n"
                        "💡 还系统借款请用：还系统 金额"
                    )
                    return
        elif match2:
            # 还系统借款的简化格式
            lender_id = "SYSTEM"
            amount_str = match2.group(1).strip()
        else:
            yield event.plain_result(
                "❌ 格式错误！\n"
                "💡 玩家借款：还他@用户 金额\n"
                "💡 系统借款：还系统 金额 或 还钱 金额\n"
                "📝 示例：还她@张三 1000 或 还系统 五千"
            )
            return

        # 解析金额
        try:
            amount = parse_amount(amount_str)
        except ValueError as e:
            yield event.plain_result(f"❌ 金额格式错误：{str(e)}\n💡 支持：1000、一千、1万等")
            return

        if amount <= 0:
            yield event.plain_result("❌ 还款金额必须大于0")
            return

        # 获取借款人ID
        borrower_id = event.get_sender_id()

        # 执行还款
        success, message = self.loan_service.repay_loan(
            borrower_id=borrower_id,
            lender_id=lender_id,
            amount=amount
        )

        yield event.plain_result(message)

    async def handle_force_collect(self, event: AstrMessageEvent, args: list):
        """
        处理强制收款指令
        格式：收他@用户 [金额] 或 收她@用户 [金额] 或 收它@用户 [金额]
        金额可选，不填则收取全部欠款
        """
        text = event.message_str
        
        # 匹配收款格式：收他/收她/收它 @用户 [金额]
        pattern = r"收[他她它]\s*@(\S+)(?:\s+(.+))?"
        match = re.search(pattern, text)
        
        if not match:
            yield event.plain_result(
                "❌ 格式错误！\n"
                "💡 正确格式：收他@用户 [金额]\n"
                "📝 示例：收她@张三 或 收他@李四 1000"
            )
            return

        target_user_str = match.group(1)
        amount_str = match.group(2)

        # 解析目标用户ID（借款人）
        borrower_id = parse_target_user_id(event, target_user_str)
        if not borrower_id:
            yield event.plain_result(
                "❌ 无法识别借款人\n"
                "💡 请使用 @用户 或 QQ号"
            )
            return

        # 解析金额（可选）
        amount = None
        if amount_str:
            try:
                amount = parse_amount(amount_str.strip())
            except ValueError as e:
                yield event.plain_result(f"❌ 金额格式错误：{str(e)}\n💡 支持：1000、一千、1万等")
                return

            if amount <= 0:
                yield event.plain_result("❌ 收款金额必须大于0")
                return

        # 获取放贷人ID
        lender_id = event.get_sender_id()

        # 执行强制收款
        success, message = self.loan_service.force_collect(
            lender_id=lender_id,
            borrower_id=borrower_id,
            amount=amount
        )

        yield event.plain_result(message)

    async def handle_view_loans(self, event: AstrMessageEvent, args: list):
        """
        查看借贷记录
        格式：借条 或 我的借条
        """
        user_id = event.get_sender_id()
        
        # 获取汇总信息
        summary = self.loan_service.get_user_loans_summary(user_id)
        
        # 获取详细列表
        loan_list = self.loan_service.get_all_loans_list(user_id)
        
        result = f"{summary}\n\n{loan_list}"
        
        yield event.plain_result(result)

    async def handle_view_all_loans(self, event: AstrMessageEvent, args: list):
        """
        查看所有借条（管理员功能）
        格式：所有借条
        """
        loan_list = self.loan_service.get_all_loans_list()
        yield event.plain_result(loan_list)

    async def handle_system_loan(self, event: AstrMessageEvent, args: list):
        """
        向系统借款
        格式：系统借款 [金额]
        金额可选，不填则借最大额度
        """
        text = event.message_str
        borrower_id = event.get_sender_id()

        # 解析金额（可选）
        amount = None
        parts = text.split()
        if len(parts) > 1:
            amount_str = parts[1].strip()
            try:
                amount = parse_amount(amount_str)
            except ValueError as e:
                yield event.plain_result(f"❌ 金额格式错误：{str(e)}\n💡 支持：1000、一千、1万等\n💡 不填金额则自动借最大额度")
                return

            if amount <= 0:
                yield event.plain_result("❌ 借款金额必须大于0")
                return

        # 向系统借款
        success, message, loan = self.loan_service.borrow_from_system(borrower_id, amount)
        yield event.plain_result(message)
