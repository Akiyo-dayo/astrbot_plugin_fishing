import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Optional

from astrbot.api import logger


class DatabaseConnectionManager:
    """数据库连接管理器，提供线程安全的连接管理和重试机制"""

    _ALLOWED_SYNCHRONOUS = {"OFF", "NORMAL", "FULL", "EXTRA"}

    def __init__(
        self,
        db_path: str,
        timeout: float = 5,
        max_retries: int = 3,
        retry_delay: float = 0.1,
        retry_timeout: Optional[float] = 30,
        detect_types: int = sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        row_factory: Optional[Callable[[sqlite3.Cursor, tuple], Any]] = sqlite3.Row,
        foreign_keys: bool = True,
        synchronous: Optional[str] = "NORMAL",
    ):
        self.db_path = db_path
        self.timeout = float(timeout)
        self.max_retries = int(max_retries)
        self.retry_delay = float(retry_delay)
        self.retry_timeout = (
            None if retry_timeout is None else float(retry_timeout)
        )
        self.detect_types = int(detect_types)
        self.row_factory = row_factory
        self.foreign_keys = bool(foreign_keys)
        self.synchronous = (
            None if synchronous is None else str(synchronous).upper()
        )

        if self.timeout < 0:
            raise ValueError("timeout 不能小于 0")
        if self.max_retries < 0:
            raise ValueError("max_retries 不能小于 0")
        if self.retry_delay < 0:
            raise ValueError("retry_delay 不能小于 0")
        if self.retry_timeout is not None and self.retry_timeout <= 0:
            raise ValueError("retry_timeout 必须大于 0 或为 None")
        if (
            self.synchronous is not None
            and self.synchronous not in self._ALLOWED_SYNCHRONOUS
        ):
            raise ValueError(f"不支持的 synchronous 模式: {synchronous}")

        self._local = threading.local()

    def _get_connection(self) -> sqlite3.Connection:
        """获取一个线程安全的数据库连接"""
        conn = getattr(self._local, "connection", None)
        if conn is None:
            conn = self._create_connection()
            self._local.connection = conn
        return conn

    def _create_connection(self) -> sqlite3.Connection:
        """创建新的数据库连接"""
        conn = sqlite3.connect(
            self.db_path,
            detect_types=self.detect_types,
            timeout=self.timeout,
        )
        conn.row_factory = self.row_factory
        conn.execute(
            f"PRAGMA foreign_keys = {'ON' if self.foreign_keys else 'OFF'};"
        )
        # journal_mode 是数据库级设置，由初始化/迁移负责。连接建立时切换模式
        # 会与正在进行的写事务竞争锁，反而可能让重试尚未开始就失败。
        if self.synchronous is not None:
            conn.execute(f"PRAGMA synchronous = {self.synchronous};")
        return conn

    def _new_retry_deadline(self) -> Optional[float]:
        if self.retry_timeout is None:
            return None
        return time.monotonic() + self.retry_timeout

    def _configure_busy_timeout(
        self, conn: sqlite3.Connection, deadline: Optional[float]
    ) -> None:
        """将本次 SQLite 锁等待限制在单次超时和剩余总预算以内。"""
        attempt_timeout = self.timeout
        if deadline is not None:
            remaining = max(deadline - time.monotonic(), 0.0)
            attempt_timeout = min(attempt_timeout, remaining)
        timeout_ms = max(int(attempt_timeout * 1000), 0)
        conn.execute(f"PRAGMA busy_timeout = {timeout_ms}")

    def _restore_busy_timeout(self, conn: sqlite3.Connection) -> None:
        if getattr(self._local, "connection", None) is not conn:
            return
        try:
            self._configure_busy_timeout(conn, None)
        except sqlite3.Error:
            self.close_connection()

    def _wait_before_retry(
        self,
        attempt: int,
        deadline: Optional[float],
        error: sqlite3.OperationalError,
        operation_label: str,
    ) -> bool:
        if attempt >= self.max_retries:
            return False

        delay = self.retry_delay * (attempt + 1)
        if deadline is not None:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or delay >= remaining:
                logger.warning(
                    f"数据库锁定，{operation_label}已耗尽总重试预算 "
                    f"({self.retry_timeout}s): {error}"
                )
                return False

        logger.warning(
            f"数据库锁定，{operation_label}第 {attempt + 1}/{self.max_retries} 次重试，"
            f"等待 {delay:.2f}s: {error}"
        )
        time.sleep(delay)
        return True

    @contextmanager
    def get_connection(self):
        """获取线程本地连接，并确保失败事务不会遗留数据库锁。"""
        conn = self._get_connection()
        self._configure_busy_timeout(conn, None)
        try:
            yield conn
        except sqlite3.OperationalError as e:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            if "database is locked" in str(e).lower():
                logger.warning(f"数据库锁定，操作无法在超时 ({self.timeout}s) 内完成: {e}")
            self.close_connection()
            raise
        except Exception as e:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            logger.error(f"数据库操作发生未知错误: {e}")
            raise

    @contextmanager
    def transaction(
        self, immediate: bool = True, _deadline: Optional[float] = None
    ):
        """在单个写事务内执行多条语句，异常时整体回滚并释放写锁。

        与 get_connection 的区别是显式接管 BEGIN/COMMIT，供需要跨多张表
        保持原子性的仓储使用（如银行的钱包<->银行余额划转）。
        """
        conn = self._get_connection()
        self._configure_busy_timeout(conn, _deadline)
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield cursor
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            raise
        finally:
            self._restore_busy_timeout(conn)

    def run_in_transaction(self, operation, immediate: bool = True):
        """执行 operation(cursor)，遇到数据库锁定时重放整个事务。

        事务失败会整体回滚，因此重放是安全的；operation 不应带有数据库之外
        的副作用。
        """
        deadline = self._new_retry_deadline()
        for attempt in range(self.max_retries + 1):
            try:
                with self.transaction(immediate=immediate, _deadline=deadline) as cursor:
                    return operation(cursor)
            except sqlite3.OperationalError as e:
                is_locked = "database is locked" in str(e).lower()
                self.close_connection()
                if not is_locked or not self._wait_before_retry(
                    attempt, deadline, e, "事务"
                ):
                    raise
            except Exception:
                self.close_connection()
                raise

    def close_connection(self):
        """关闭当前线程的数据库连接"""
        if hasattr(self._local, "connection"):
            try:
                conn = self._local.connection
                if conn.in_transaction:
                    conn.rollback()
                conn.close()
            except sqlite3.Error:
                pass
            finally:
                delattr(self._local, "connection")

    def execute_with_retry(self, query: str, params: tuple = (), fetch: str = "none"):
        """执行SQL查询，支持重试机制
        
        Args:
            query: SQL查询语句
            params: 查询参数
            fetch: 获取结果的方式 ("none", "one", "all")
        """
        if fetch not in {"none", "one", "all"}:
            raise ValueError(f"不支持的 fetch 类型: {fetch}")

        deadline = self._new_retry_deadline()
        for attempt in range(self.max_retries + 1):
            conn = self._get_connection()
            try:
                self._configure_busy_timeout(conn, deadline)
                cursor = conn.cursor()
                cursor.execute(query, params)

                if fetch == "one":
                    return cursor.fetchone()
                if fetch == "all":
                    return cursor.fetchall()

                conn.commit()
                return cursor.lastrowid if cursor.lastrowid else None
            except sqlite3.OperationalError as e:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass

                is_locked = "database is locked" in str(e).lower()
                self.close_connection()
                if not is_locked or not self._wait_before_retry(
                    attempt, deadline, e, "操作"
                ):
                    raise
            except Exception:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                self.close_connection()
                raise
            finally:
                self._restore_busy_timeout(conn)
