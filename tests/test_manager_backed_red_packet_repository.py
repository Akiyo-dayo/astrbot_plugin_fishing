import importlib
import sqlite3
import sys
import types
import unittest
from datetime import datetime, timedelta
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


class ManagerBackedRedPacketRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.models = importlib.import_module("core.domain.models")
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_red_packet_repo"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                PRAGMA journal_mode = WAL;
                PRAGMA foreign_keys = ON;
                CREATE TABLE red_packets (
                    packet_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    packet_type TEXT NOT NULL,
                    total_amount INTEGER NOT NULL,
                    total_count INTEGER NOT NULL,
                    remaining_amount INTEGER NOT NULL,
                    remaining_count INTEGER NOT NULL,
                    password TEXT,
                    created_at TIMESTAMP NOT NULL,
                    expires_at TIMESTAMP NOT NULL,
                    is_expired INTEGER DEFAULT 0
                );
                CREATE TABLE red_packet_records (
                    record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    packet_id INTEGER NOT NULL,
                    user_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    claimed_at TIMESTAMP NOT NULL,
                    FOREIGN KEY (packet_id) REFERENCES red_packets(packet_id)
                );
                """
            )
        self.repo = self.repo_module.SqliteRedPacketRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _packet(
        self,
        group_id,
        *,
        expires_at=None,
        is_expired=False,
        amount=100,
    ):
        now = datetime.now()
        return self.models.RedPacket(
            packet_id=None,
            sender_id="sender",
            group_id=group_id,
            packet_type="normal",
            total_amount=amount,
            total_count=2,
            remaining_amount=amount,
            remaining_count=2,
            created_at=now,
            expires_at=expires_at or now + timedelta(hours=1),
            is_expired=is_expired,
        )

    def test_repository_writes_use_replayed_transactions(self):
        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            packet = self._packet("g1")
            packet.packet_id = self.repo.create_red_packet(packet)
            packet.remaining_amount = 50
            packet.remaining_count = 1
            self.repo.update_red_packet(packet)
            self.repo.create_claim_record(
                self.models.RedPacketRecord(
                    record_id=None,
                    packet_id=packet.packet_id,
                    user_id="u1",
                    amount=50,
                    claimed_at=datetime.now(),
                )
            )
            self.assertEqual(self.repo.expire_old_packets(datetime.now()), 0)
            self.assertEqual(self.repo.revoke_group_red_packets("g1")[:2], (1, 50))

            packet2 = self._packet("g2", amount=40)
            packet2.packet_id = self.repo.create_red_packet(packet2)
            self.assertEqual(self.repo.revoke_all_red_packets()[:2], (1, 40))

            old_packet = self._packet(
                "old",
                expires_at=datetime.now() - timedelta(days=10),
                is_expired=True,
            )
            old_packet.packet_id = self.repo.create_red_packet(old_packet)
            self.assertEqual(self.repo.clean_old_red_packets(days_to_keep=7), 1)

            group_packet = self._packet("delete-group")
            self.repo.create_red_packet(group_packet)
            self.assertEqual(self.repo.delete_group_red_packets("delete-group"), 1)

            self.repo.create_red_packet(self._packet("delete-all"))
            self.assertEqual(self.repo.delete_all_red_packets(), 3)

        self.assertEqual(run_transaction.call_count, 13)
        self.assertEqual(self.repo.get_group_red_packets("g1"), [])

    def test_clean_old_packets_rolls_back_record_deletion_on_failure(self):
        packet = self._packet(
            "old",
            expires_at=datetime.now() - timedelta(days=10),
            is_expired=True,
        )
        packet.packet_id = self.repo.create_red_packet(packet)
        self.repo.create_claim_record(
            self.models.RedPacketRecord(
                record_id=None,
                packet_id=packet.packet_id,
                user_id="u1",
                amount=10,
                claimed_at=datetime.now(),
            )
        )
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TRIGGER reject_packet_delete
                BEFORE DELETE ON red_packets
                BEGIN
                    SELECT RAISE(ABORT, 'forced packet delete failure');
                END
                """
            )

        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.clean_old_red_packets(days_to_keep=7)

        self.assertFalse(self.repo._conn_mgr._get_connection().in_transaction)
        with sqlite3.connect(self.db_path, timeout=0.1) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM red_packets").fetchone()[0], 1
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM red_packet_records").fetchone()[0],
                1,
            )
            conn.execute(
                "INSERT INTO red_packets VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 0)",
                (
                    "other",
                    "other",
                    "normal",
                    1,
                    1,
                    1,
                    1,
                    datetime.now(),
                    datetime.now() + timedelta(hours=1),
                ),
            )

    def test_scheduled_cleanup_uses_current_schema_and_24_hour_retention(self):
        old_packet = self._packet(
            "old",
            expires_at=datetime.now() - timedelta(hours=25),
        )
        old_packet.packet_id = self.repo.create_red_packet(old_packet)
        self.repo.create_claim_record(
            self.models.RedPacketRecord(
                record_id=None,
                packet_id=old_packet.packet_id,
                user_id="u1",
                amount=10,
                claimed_at=datetime.now() - timedelta(hours=25),
            )
        )
        recent_packet = self._packet(
            "recent",
            expires_at=datetime.now() - timedelta(hours=23),
            is_expired=True,
        )
        recent_packet.packet_id = self.repo.create_red_packet(recent_packet)

        self.assertEqual(self.repo.cleanup_expired_red_packets(), 1)

        with sqlite3.connect(self.db_path) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT packet_id FROM red_packets"
                ).fetchall(),
                [(recent_packet.packet_id,)],
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM red_packet_records"
                ).fetchone()[0],
                0,
            )

    def test_parse_and_read_contract_is_preserved(self):
        packet = self._packet("g1")
        packet.packet_id = self.repo.create_red_packet(packet)
        stored = self.repo.get_red_packet_by_id(packet.packet_id)

        self.assertEqual(stored.group_id, "g1")
        self.assertIsInstance(stored.created_at, datetime)
        self.assertFalse(stored.is_expired)
        self.assertFalse(self.repo.has_user_claimed(packet.packet_id, "nobody"))

    def test_close_releases_thread_local_manager_connection(self):
        self.repo.get_active_red_packets_in_group("g1")
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
