import importlib
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


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


class ManagerBackedItemTemplateRepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.module = importlib.import_module(
            "core.repositories.sqlite_item_template_repo"
        )
        cls.models = importlib.import_module("core.domain.models")

    def setUp(self):
        self.temp_dir = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.temp_dir.name) / "fish.db"
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE fish (
                    fish_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, description TEXT, rarity INTEGER, base_value INTEGER,
                    min_weight INTEGER, max_weight INTEGER, icon_url TEXT
                );
                CREATE TABLE rods (
                    rod_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, description TEXT, rarity INTEGER, source TEXT,
                    purchase_cost INTEGER, bonus_fish_quality_modifier REAL,
                    bonus_fish_quantity_modifier REAL, bonus_rare_fish_chance REAL,
                    durability INTEGER, icon_url TEXT
                );
                CREATE TABLE baits (
                    bait_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, description TEXT, rarity INTEGER,
                    effect_description TEXT, duration_minutes INTEGER, cost INTEGER,
                    required_rod_rarity INTEGER, success_rate_modifier REAL,
                    rare_chance_modifier REAL, garbage_reduction_modifier REAL,
                    value_modifier REAL, quantity_modifier REAL,
                    is_consumable INTEGER, weight_modifier REAL
                );
                CREATE TABLE accessories (
                    accessory_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, description TEXT, rarity INTEGER, slot_type TEXT,
                    bonus_fish_quality_modifier REAL,
                    bonus_fish_quantity_modifier REAL, bonus_rare_fish_chance REAL,
                    bonus_coin_modifier REAL, other_bonus_description TEXT,
                    icon_url TEXT
                );
                CREATE TABLE items (
                    item_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT, description TEXT, rarity INTEGER,
                    effect_description TEXT, cost INTEGER, is_consumable INTEGER,
                    icon_url TEXT, effect_type TEXT, effect_payload TEXT
                );
                CREATE TABLE titles (
                    title_id INTEGER PRIMARY KEY,
                    name TEXT, description TEXT, display_format TEXT
                );
                """
            )
        self.repo = self.module.SqliteItemTemplateRepository(str(self.db_path))

    def tearDown(self):
        self.repo.close_connection()
        self.temp_dir.cleanup()

    def test_all_template_write_families_use_replayed_transactions(self):
        generic_item = self.models.Item(
            item_id=0,
            name="generic",
            rarity=1,
            description="d",
            effect_description="e",
            cost=1,
            is_consumable=True,
            icon_url=None,
            effect_type="noop",
            effect_payload="{}",
        )
        fish = {
            "name": "fish",
            "description": "d",
            "rarity": 2,
            "base_value": 10,
            "min_weight": 1,
            "max_weight": 2,
            "icon_url": None,
        }
        rod = {
            "name": "rod",
            "description": "d",
            "rarity": 2,
            "source": "shop",
            "purchase_cost": 10,
            "bonus_fish_quality_modifier": 1.0,
            "bonus_fish_quantity_modifier": 1.0,
            "bonus_rare_fish_chance": 0.0,
            "durability": 0,
            "icon_url": None,
        }
        bait = {
            "name": "bait",
            "description": "d",
            "rarity": 1,
            "effect_description": "e",
            "duration_minutes": 1,
            "cost": 2,
            "required_rod_rarity": 0,
            "success_rate_modifier": 0.0,
            "rare_chance_modifier": 0.0,
            "garbage_reduction_modifier": 0.0,
            "value_modifier": 1.0,
            "quantity_modifier": 1.0,
            "weight_modifier": 1.0,
            "is_consumable": True,
        }
        accessory = {
            "name": "accessory",
            "description": "d",
            "rarity": 1,
            "slot_type": "general",
            "bonus_fish_quality_modifier": 1.0,
            "bonus_fish_quantity_modifier": 1.0,
            "bonus_rare_fish_chance": 0.0,
            "bonus_coin_modifier": 1.0,
            "other_bonus_description": None,
            "icon_url": None,
        }
        admin_item = {
            "name": "admin-item",
            "description": "d",
            "rarity": 1,
            "effect_description": "e",
            "cost": 3,
            "is_consumable": True,
            "icon_url": None,
        }
        title = {
            "title_id": 9,
            "name": "title",
            "description": "d",
            "display_format": "{name}",
        }

        with patch.object(
            self.repo._conn_mgr,
            "run_in_transaction",
            wraps=self.repo._conn_mgr.run_in_transaction,
        ) as run_transaction:
            self.repo.add(generic_item)
            stored_item = self.repo.get_by_name("generic")
            stored_item.cost = 5
            self.repo.update(stored_item)

            self.repo.add_fish_template(fish)
            fish_id = self.repo.get_all_fish()[0].fish_id
            self.repo.update_fish_template(fish_id, {**fish, "base_value": 20})
            self.repo.delete_fish_template(fish_id)

            self.repo.add_rod_template(rod)
            rod_id = self.repo.get_all_rods()[0].rod_id
            self.repo.update_rod_template(rod_id, {**rod, "durability": 5})
            self.repo.delete_rod_template(rod_id)

            self.repo.add_bait_template(bait)
            bait_id = self.repo.get_all_baits()[0].bait_id
            self.repo.update_bait_template(bait_id, {**bait, "cost": 4})
            self.repo.delete_bait_template(bait_id)

            self.repo.add_accessory_template(accessory)
            accessory_id = self.repo.get_all_accessories()[0].accessory_id
            self.repo.update_accessory_template(
                accessory_id, {**accessory, "bonus_coin_modifier": 1.1}
            )
            self.repo.delete_accessory_template(accessory_id)

            self.repo.add_item_template(admin_item)
            admin_item_id = self.repo.get_by_name("admin-item").item_id
            self.repo.update_item_template(
                admin_item_id, {**admin_item, "cost": 6}
            )
            self.repo.delete_item_template(admin_item_id)

            self.repo.add_title_template(title)
            self.repo.update_title_template(9, {**title, "name": "renamed"})
            self.repo.delete_title_template(9)

        self.assertEqual(run_transaction.call_count, 20)
        self.assertEqual(self.repo.get_by_id(stored_item.item_id).cost, 5)
        self.assertEqual(self.repo._conn_mgr.detect_types, 0)

    def test_close_releases_thread_local_connection(self):
        self.repo.get_all_items()
        self.assertTrue(hasattr(self.repo._conn_mgr._local, "connection"))
        self.repo.close_connection()
        self.assertFalse(hasattr(self.repo._conn_mgr._local, "connection"))


if __name__ == "__main__":
    unittest.main()
