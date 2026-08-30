import importlib
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


class _Logger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


def _install_astrbot_stub():
    astrbot = types.ModuleType("astrbot")
    api = types.ModuleType("astrbot.api")
    api.logger = _Logger()
    astrbot.api = api
    sys.modules.setdefault("astrbot", astrbot)
    sys.modules.setdefault("astrbot.api", api)


class _UserRepositoryStub:
    def __init__(self, existing_user_ids):
        self.existing_user_ids = set(existing_user_ids)

    def check_exists(self, user_id):
        return user_id in self.existing_user_ids


class _LoanRepositoryStub:
    def __init__(self):
        self.create_called = False

    def create_loan(self, loan):
        self.create_called = True
        return 1


class LoanTransactionSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.loan_models = importlib.import_module("core.domain.loan_models")
        cls.loan_repo_module = importlib.import_module(
            "core.repositories.sqlite_loan_repo"
        )
        cls.loan_service_module = importlib.import_module("core.services.loan_service")
        cls.fishing_service_module = importlib.import_module(
            "core.services.fishing_service"
        )

    def _create_database(self, db_path: Path):
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA foreign_keys = ON")
            conn.execute("CREATE TABLE users (user_id TEXT PRIMARY KEY)")
            conn.execute(
                """
                CREATE TABLE loans (
                    loan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lender_id TEXT NOT NULL REFERENCES users(user_id),
                    borrower_id TEXT NOT NULL REFERENCES users(user_id),
                    principal INTEGER NOT NULL,
                    interest_rate REAL NOT NULL,
                    borrowed_at TIMESTAMP NOT NULL,
                    due_amount INTEGER NOT NULL,
                    repaid_amount INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    due_date TIMESTAMP,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
            conn.executemany(
                "INSERT INTO users (user_id) VALUES (?)",
                (("lender",), ("borrower",)),
            )

    def test_foreign_key_failure_rolls_back_and_releases_write_lock(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self.loan_repo_module.SqliteLoanRepository(str(db_path))
            loan = self.loan_models.Loan(
                lender_id="missing-lender",
                borrower_id="borrower",
                principal=100,
                due_amount=105,
                status="pending",
            )

            with self.assertRaises(sqlite3.IntegrityError):
                repo.create_loan(loan)

            self.assertFalse(repo._conn_mgr._get_connection().in_transaction)
            with sqlite3.connect(db_path, timeout=0.1) as other_conn:
                other_conn.execute("INSERT INTO users (user_id) VALUES ('other')")

            repo.close_connection()

    def test_create_and_update_use_replayed_manager_transactions(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self.loan_repo_module.SqliteLoanRepository(str(db_path))
            loan = self.loan_models.Loan(
                lender_id="lender",
                borrower_id="borrower",
                principal=100,
                due_amount=105,
                status="active",
            )

            with patch.object(
                repo._conn_mgr,
                "run_in_transaction",
                wraps=repo._conn_mgr.run_in_transaction,
            ) as run_transaction:
                loan_id = repo.create_loan(loan)
                updated = repo.update_loan_repayment(loan_id, 105, "paid")

            self.assertEqual(run_transaction.call_count, 2)
            self.assertTrue(updated)
            stored = repo.get_loan_by_id(loan_id)
            self.assertEqual(stored.repaid_amount, 105)
            self.assertEqual(stored.status, "paid")
            self.assertEqual(repo._conn_mgr.detect_types, 0)
            repo.close_connection()

    def test_nonstandard_timestamp_text_remains_readable(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            with sqlite3.connect(db_path) as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO loans (
                        lender_id, borrower_id, principal, interest_rate,
                        borrowed_at, due_amount, repaid_amount, status,
                        due_date, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "lender",
                        "borrower",
                        100,
                        0.05,
                        "not-a-date",
                        105,
                        0,
                        "active",
                        "2026-08-29T12:34:56+08:00",
                        "2026-08-29 04:34:56.123456",
                        "2026-08-29 04:35",
                    ),
                )
                loan_id = cursor.lastrowid

            repo = self.loan_repo_module.SqliteLoanRepository(str(db_path))
            loan = repo.get_loan_by_id(loan_id)

            self.assertIsNone(loan.borrowed_at)
            self.assertEqual(loan.due_date.isoformat(), "2026-08-29T12:34:56+08:00")
            self.assertEqual(loan.created_at.microsecond, 123456)
            self.assertEqual(loan.updated_at.second, 0)
            repo.close_connection()

    def test_close_releases_thread_local_manager_connection(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            self._create_database(db_path)
            repo = self.loan_repo_module.SqliteLoanRepository(str(db_path))

            repo.get_all_active_loans()
            self.assertTrue(hasattr(repo._conn_mgr._local, "connection"))
            repo.close_connection()
            self.assertFalse(hasattr(repo._conn_mgr._local, "connection"))

    def test_create_loan_rejects_unregistered_lender_before_insert(self):
        loan_repo = _LoanRepositoryStub()
        user_repo = _UserRepositoryStub({"borrower"})
        service = self.loan_service_module.LoanService(loan_repo, user_repo)

        success, message, loan = service.create_loan(
            "missing-lender", "borrower", 100
        )

        self.assertFalse(success)
        self.assertIn("放贷人账户不存在", message)
        self.assertIsNone(loan)
        self.assertFalse(loan_repo.create_called)

    def test_create_loan_rejects_unregistered_borrower_before_insert(self):
        loan_repo = _LoanRepositoryStub()
        user_repo = _UserRepositoryStub({"lender"})
        service = self.loan_service_module.LoanService(loan_repo, user_repo)

        success, message, loan = service.create_loan(
            "lender", "missing-borrower", 100
        )

        self.assertFalse(success)
        self.assertIn("借款人账户不存在", message)
        self.assertIsNone(loan)
        self.assertFalse(loan_repo.create_called)

    def test_auto_fishing_continues_after_one_user_fails(self):
        service = self.fishing_service_module.FishingService.__new__(
            self.fishing_service_module.FishingService
        )
        service.config = {"fishing": {"cooldown_seconds": 180}}
        service.auto_fishing_running = True
        service._reset_rare_fish_daily_quota = lambda: False
        service.user_repo = types.SimpleNamespace(
            get_all_user_ids=lambda auto_fishing_only: ["bad-user", "good-user"]
        )
        processed_users = []

        def process_user(user_id, cooldown):
            processed_users.append(user_id)
            if user_id == "bad-user":
                raise sqlite3.OperationalError("database is locked")

        service._process_auto_fishing_user = process_user

        def stop_loop(_seconds):
            service.auto_fishing_running = False

        with patch.object(self.fishing_service_module.time, "sleep", stop_loop):
            service._auto_fishing_loop()

        self.assertEqual(processed_users, ["bad-user", "good-user"])


if __name__ == "__main__":
    unittest.main()
