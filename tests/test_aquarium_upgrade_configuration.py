import importlib
import json
import sqlite3
import stat
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
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


class AquariumUpgradeConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.config_module = importlib.import_module("core.aquarium_upgrade_config")
        cls.repo_module = importlib.import_module(
            "core.repositories.sqlite_aquarium_config_repo"
        )
        cls.service_module = importlib.import_module(
            "core.services.aquarium_service"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        defaults = self.config_module.load_default_aquarium_upgrades()
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE users (
                    user_id TEXT PRIMARY KEY,
                    aquarium_capacity INTEGER NOT NULL DEFAULT 50
                );
                CREATE TABLE user_aquarium (
                    user_id TEXT NOT NULL,
                    fish_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 0,
                    quality_level INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, fish_id, quality_level)
                );
                CREATE TABLE aquarium_upgrades (
                    upgrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level INTEGER NOT NULL UNIQUE,
                    capacity INTEGER NOT NULL,
                    cost_coins INTEGER NOT NULL,
                    cost_premium INTEGER DEFAULT 0,
                    description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            conn.executemany(
                """
                INSERT INTO aquarium_upgrades (
                    level, capacity, cost_coins, cost_premium, description
                ) VALUES (:level, :capacity, :cost_coins, :cost_premium, :description)
                """,
                defaults,
            )
        self.repo = self.repo_module.SqliteAquariumConfigRepository(
            str(self.db_path)
        )
        self.service = self.service_module.AquariumService(
            object(), object(), object(), self.repo
        )

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _defaults(self):
        return self.config_module.load_default_aquarium_upgrades()

    def _insert_user(self, user_id, capacity, fish_count=0):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO users (user_id, aquarium_capacity) VALUES (?, ?)",
                (user_id, capacity),
            )
            if fish_count:
                conn.execute(
                    """
                    INSERT INTO user_aquarium (
                        user_id, fish_id, quantity, quality_level
                    ) VALUES (?, 1, ?, 0)
                    """,
                    (user_id, fish_count),
                )

    def test_defaults_are_loaded_from_independent_json(self):
        defaults = self._defaults()
        self.assertEqual(len(defaults), 10)
        self.assertEqual(defaults[0]["capacity"], 50)
        self.assertEqual(defaults[-1]["cost_coins"], 5_000_000)

    def test_validation_requires_consecutive_levels_and_increasing_capacity(self):
        invalid_levels = self._defaults()
        invalid_levels[1]["level"] = 3
        with self.assertRaisesRegex(ValueError, "等级必须从 1 连续递增"):
            self.config_module.normalize_aquarium_upgrades(invalid_levels)

        invalid_capacity = self._defaults()
        invalid_capacity[1]["capacity"] = invalid_capacity[0]["capacity"]
        with self.assertRaisesRegex(ValueError, "容量必须大于前一级"):
            self.config_module.normalize_aquarium_upgrades(invalid_capacity)

    def test_save_is_immediately_visible_and_preserves_player_levels(self):
        self._insert_user("level-1", 50, 10)
        self._insert_user("level-2", 100, 80)
        self._insert_user("level-3", 150, 120)
        changed = self._defaults()
        changed[0]["capacity"] = 60
        changed[1]["capacity"] = 120
        changed[1]["cost_coins"] = 12_345
        changed[2]["capacity"] = 180

        result = self.service.update_aquarium_upgrades(changed)

        self.assertTrue(result["success"])
        self.assertEqual(result["affected_users"], 3)
        self.assertEqual(
            self.service.aquarium_config_repo.get_by_level(2).cost_coins,
            12_345,
        )
        with sqlite3.connect(self.db_path) as conn:
            capacities = dict(
                conn.execute(
                    "SELECT user_id, aquarium_capacity FROM users ORDER BY user_id"
                ).fetchall()
            )
        self.assertEqual(
            capacities,
            {"level-1": 60, "level-2": 120, "level-3": 180},
        )

    def test_save_rolls_back_when_new_capacity_is_below_existing_fish(self):
        self._insert_user("crowded", 100, 90)
        changed = self._defaults()
        changed[1]["capacity"] = 80
        changed[1]["cost_coins"] = 999

        result = self.service.update_aquarium_upgrades(changed)

        self.assertFalse(result["success"])
        self.assertIn("小于玩家现有水族箱数量", result["message"])
        self.assertEqual(self.repo.get_by_level(2).capacity, 100)
        self.assertEqual(self.repo.get_by_level(2).cost_coins, 10_000)
        with sqlite3.connect(self.db_path) as conn:
            capacity = conn.execute(
                "SELECT aquarium_capacity FROM users WHERE user_id = 'crowded'"
            ).fetchone()[0]
        self.assertEqual(capacity, 100)

    def test_save_rejects_removing_a_level_still_in_use(self):
        self._insert_user("top-user", 2000)

        result = self.service.update_aquarium_upgrades(self._defaults()[:-1])

        self.assertFalse(result["success"])
        self.assertIn("不能删除等级 10", result["message"])
        self.assertIsNotNone(self.repo.get_by_level(10))

    def test_reset_uses_independent_defaults(self):
        changed = self._defaults()
        changed[1]["cost_coins"] = 123
        self.assertTrue(self.service.update_aquarium_upgrades(changed)["success"])

        result = self.service.reset_aquarium_upgrades()

        self.assertTrue(result["success"])
        self.assertEqual(self.repo.get_by_level(2).cost_coins, 10_000)


class AquariumMigrationDefaultTests(unittest.TestCase):
    def test_migration_027_reads_the_independent_default_file(self):
        _install_astrbot_stub()
        config_module = importlib.import_module("core.aquarium_upgrade_config")
        migration = importlib.import_module(
            "core.database.migrations.027_add_aquarium_system"
        )

        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            with sqlite3.connect(db_path) as conn:
                conn.executescript(
                    """
                    CREATE TABLE users (user_id TEXT PRIMARY KEY);
                    CREATE TABLE fish (fish_id INTEGER PRIMARY KEY);
                    """
                )
                migration.up(conn.cursor())
                rows = conn.execute(
                    """
                    SELECT level, capacity, cost_coins, cost_premium, description
                    FROM aquarium_upgrades ORDER BY level
                    """
                ).fetchall()

            defaults = config_module.load_default_aquarium_upgrades()
            expected = [
                (
                    row["level"],
                    row["capacity"],
                    row["cost_coins"],
                    row["cost_premium"],
                    row["description"],
                )
                for row in defaults
            ]
            self.assertEqual(rows, expected)


class FishPondUpgradeConfigurationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        plugins_dir = PROJECT_ROOT.parent
        if str(plugins_dir) not in sys.path:
            sys.path.insert(0, str(plugins_dir))
        package = PROJECT_ROOT.name
        cls.config_module = importlib.import_module(
            f"{package}.core.aquarium_upgrade_config"
        )
        cls.repo_module = importlib.import_module(
            f"{package}.core.repositories.sqlite_aquarium_config_repo"
        )
        cls.inventory_service_module = importlib.import_module(
            f"{package}.core.services.inventory_service"
        )

    def setUp(self):
        self._temp = TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self._temp.name) / "fish.db"
        defaults = self.config_module.load_default_fish_pond_upgrades()
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(
                """
                CREATE TABLE users (
                    user_id TEXT PRIMARY KEY,
                    fish_pond_capacity INTEGER NOT NULL DEFAULT 480
                );
                CREATE TABLE user_fish_inventory (
                    user_id TEXT NOT NULL,
                    fish_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL DEFAULT 0,
                    quality_level INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (user_id, fish_id, quality_level)
                );
                CREATE TABLE fish_pond_upgrades (
                    upgrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    level INTEGER NOT NULL UNIQUE,
                    capacity INTEGER NOT NULL,
                    cost_coins INTEGER NOT NULL,
                    cost_premium INTEGER DEFAULT 0,
                    description TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            conn.executemany(
                """
                INSERT INTO fish_pond_upgrades (
                    level, capacity, cost_coins, cost_premium, description
                ) VALUES (:level, :capacity, :cost_coins, :cost_premium, :description)
                """,
                defaults,
            )
        self.repo = self.repo_module.SqliteFishPondConfigRepository(
            str(self.db_path)
        )
        self.service = self.inventory_service_module.InventoryService(
            object(), object(), object(), None, object(), {}, self.repo
        )

    def tearDown(self):
        self.repo.close_connection()
        self._temp.cleanup()

    def _defaults(self):
        return self.config_module.load_default_fish_pond_upgrades()

    def _insert_user(self, user_id, capacity, fish_count=0):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO users (user_id, fish_pond_capacity) VALUES (?, ?)",
                (user_id, capacity),
            )
            if fish_count:
                conn.execute(
                    """
                    INSERT INTO user_fish_inventory (
                        user_id, fish_id, quantity, quality_level
                    ) VALUES (?, 1, ?, 0)
                    """,
                    (user_id, fish_count),
                )

    def test_defaults_match_the_previous_hard_coded_upgrade_path(self):
        defaults = self._defaults()
        self.assertEqual(
            [row["capacity"] for row in defaults],
            [480, 999, 9_999, 99_999, 999_999],
        )
        self.assertEqual(defaults[-1]["cost_coins"], 5_000_000_000)

    def test_save_is_live_and_preserves_fish_pond_levels(self):
        self._insert_user("starter", 480, 20)
        self._insert_user("upgraded", 999, 700)
        changed = self._defaults()
        changed[0]["capacity"] = 600
        changed[1]["capacity"] = 1_200
        changed[1]["cost_premium"] = 3

        result = self.service.update_fish_pond_upgrades(changed)

        self.assertTrue(result["success"])
        self.assertEqual(result["affected_users"], 2)
        self.assertEqual(self.repo.get_by_level(2).cost_premium, 3)
        with sqlite3.connect(self.db_path) as conn:
            capacities = dict(
                conn.execute(
                    "SELECT user_id, fish_pond_capacity FROM users ORDER BY user_id"
                ).fetchall()
            )
        self.assertEqual(capacities, {"starter": 600, "upgraded": 1_200})

    def test_fish_pond_shrink_rolls_back_when_existing_fish_would_not_fit(self):
        self._insert_user("crowded", 999, 900)
        changed = self._defaults()
        changed[1]["capacity"] = 800

        result = self.service.update_fish_pond_upgrades(changed)

        self.assertFalse(result["success"])
        self.assertIn("小于玩家现有鱼塘数量", result["message"])
        self.assertEqual(self.repo.get_by_level(2).capacity, 999)

    def test_migration_050_seeds_the_versioned_fish_pond_defaults(self):
        migration = importlib.import_module(
            f"{PROJECT_ROOT.name}.core.database.migrations.050_add_fish_pond_upgrade_config"
        )
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            db_path = Path(temp_dir) / "fish.db"
            with sqlite3.connect(db_path) as conn:
                migration.up(conn.cursor())
                rows = conn.execute(
                    """
                    SELECT level, capacity, cost_coins, cost_premium, description
                    FROM fish_pond_upgrades ORDER BY level
                    """
                ).fetchall()

        expected = [
            (
                row["level"],
                row["capacity"],
                row["cost_coins"],
                row["cost_premium"],
                row["description"],
            )
            for row in self._defaults()
        ]
        self.assertEqual(rows, expected)


class RuntimeSettingsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _install_astrbot_stub()
        cls.settings = importlib.import_module("core.runtime_settings")

    @staticmethod
    def _default_form(group):
        form = {}
        for field in group["fields"]:
            if field["type"] == "bool":
                if field["default"]:
                    form[field["path"]] = "on"
            else:
                form[field["path"]] = str(field["default"])
        return form

    def test_groups_are_routed_to_their_own_web_systems(self):
        self.assertEqual(
            self.settings.OTHER_SETTING_GROUP_KEYS,
            ("gameplay", "resale", "loan", "gambling"),
        )
        other_paths = {
            field["path"]
            for key in self.settings.OTHER_SETTING_GROUP_KEYS
            for field in self.settings.RUNTIME_SETTING_GROUPS[key]["fields"]
        }
        self.assertFalse(any(path.startswith("bank.") for path in other_paths))
        self.assertFalse(any(path.startswith("exchange.") for path in other_paths))
        self.assertFalse(any(path.startswith("market.") for path in other_paths))
        self.assertTrue(
            all(
                field["path"].startswith("bank.")
                for field in self.settings.RUNTIME_SETTING_GROUPS["bank"]["fields"]
            )
        )
        self.assertTrue(
            all(
                field["path"].startswith("exchange.")
                for field in self.settings.RUNTIME_SETTING_GROUPS["exchange"]["fields"]
            )
        )

    def test_gameplay_save_updates_runtime_paths_and_daily_boundary(self):
        group = self.settings.RUNTIME_SETTING_GROUPS["gameplay"]
        form = self._default_form(group)
        form["fishing.cooldown_seconds"] = "45"
        form["fishing.quality_bonus_max_chance"] = "0.42"
        form["game.daily_reset_hour"] = "6"
        values = self.settings.parse_runtime_settings_form(form, "gameplay")

        class FishingServiceStub:
            reset_hour = None

            def set_daily_reset_hour(self, hour):
                self.reset_hour = hour

        fishing_service = FishingServiceStub()
        game_config = {
            "fishing": {"cooldown_seconds": 180},
            "quality_bonus_max_chance": 0.35,
        }
        self.settings.apply_runtime_settings(
            game_config,
            values,
            {"fishing_service": fishing_service},
        )

        self.assertEqual(game_config["fishing"]["cooldown_seconds"], 45)
        self.assertEqual(game_config["quality_bonus_max_chance"], 0.42)
        self.assertEqual(game_config["daily_reset_hour"], 6)
        self.assertEqual(fishing_service.reset_hour, 6)

    def test_cached_loan_and_gambling_services_are_updated(self):
        loan_group = self.settings.RUNTIME_SETTING_GROUPS["loan"]
        loan_form = self._default_form(loan_group)
        loan_form["loan.system_loan_days"] = "14"
        loan_values = self.settings.parse_runtime_settings_form(loan_form, "loan")
        loan_service = SimpleNamespace()
        game_config = {}
        self.settings.apply_runtime_settings(
            game_config, loan_values, {"loan_service": loan_service}
        )
        self.assertEqual(loan_service.system_loan_days, 14)

        gambling_group = self.settings.RUNTIME_SETTING_GROUPS["gambling"]
        gambling_form = self._default_form(gambling_group)
        gambling_form["sicbo.countdown_seconds"] = "90"
        gambling_form["slot.daily_limit"] = "12"
        gambling_values = self.settings.parse_runtime_settings_form(
            gambling_form, "gambling"
        )
        sicbo = SimpleNamespace()
        slot = SimpleNamespace()
        self.settings.apply_runtime_settings(
            game_config,
            gambling_values,
            {"sicbo_service": sicbo, "slot_service": slot},
        )
        self.assertEqual(sicbo.countdown_seconds, 90)
        self.assertEqual(slot.daily_limit, 12)

    def test_framework_updates_preserve_distinct_persistent_and_runtime_paths(self):
        updates = self.settings.framework_updates(
            {
                "fishing.quality_bonus_max_chance": 0.4,
                "sell_prices.by_rarity_10": 999_999,
            }
        )
        self.assertEqual(updates["fishing"]["quality_bonus_max_chance"], 0.4)
        self.assertEqual(updates["sell_prices"]["by_rarity_10"], 999_999)

    def test_exchange_price_service_reads_the_exposed_volatility_path(self):
        module = importlib.import_module("core.services.exchange_price_service")
        service = module.ExchangePriceService(
            object(),
            {
                "exchange": {
                    "volatility": {"dried_fish": 0.08},
                    "max_change_rate": 0.2,
                    "commodities": {
                        "dried_fish": {"volatility": 0.19}
                    },
                }
            },
        )
        with patch.object(module.random, "uniform", return_value=1.0):
            self.assertEqual(service._calculate_new_price("dried_fish", 100), 108)

    def test_config_schema_exposes_all_ten_resale_rarities(self):
        schema = json.loads(
            (PROJECT_ROOT / "_conf_schema.json").read_text(encoding="utf-8-sig")
        )
        fields = schema["sell_prices"]["items"]
        self.assertEqual(
            {f"by_rarity_{rarity}" for rarity in range(1, 11)},
            set(fields),
        )


class RuntimeSettingsPersistenceAndTemplateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.settings = importlib.import_module("core.runtime_settings")

    def test_framework_save_deep_merges_without_losing_hidden_keys(self):
        class SavedConfig(dict):
            save_count = 0

            def save_config(self):
                self.save_count += 1

        config = SavedConfig(
            {
                "bank": {
                    "hidden": "keep",
                    "fixed_deposit": {
                        "enabled": True,
                        "terms": {"1": 0.001, "3": 0.004},
                    },
                },
                "webui": {"secret_key": "untouched"},
            }
        )
        result = self.settings.persist_config_updates(
            {"bank": {"fixed_deposit": {"terms": {"1": 0.002}}}},
            astrbot_config=config,
        )

        self.assertEqual(result, ["framework:AstrBotConfig"])
        self.assertEqual(config.save_count, 1)
        self.assertEqual(config["bank"]["hidden"], "keep")
        self.assertEqual(config["bank"]["fixed_deposit"]["terms"]["1"], 0.002)
        self.assertEqual(config["bank"]["fixed_deposit"]["terms"]["3"], 0.004)
        self.assertEqual(config["webui"]["secret_key"], "untouched")

    def test_failed_framework_save_restores_memory_before_fallback(self):
        class BrokenConfig(dict):
            def save_config(self):
                raise OSError("save failed")

        config = BrokenConfig({"market": {"listing_tax_rate": 0.05}})
        warnings = []
        result = self.settings.persist_config_updates(
            {"market": {"listing_tax_rate": 0.2}},
            astrbot_config=config,
            on_warning=warnings.append,
        )

        self.assertEqual(result, [])
        self.assertEqual(config["market"]["listing_tax_rate"], 0.05)
        self.assertTrue(any("回退" in warning for warning in warnings))

    def test_file_fallback_is_deep_merged_and_atomic(self):
        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            path = Path(temp_dir) / "plugin_config.json"
            path.write_text(
                json.dumps(
                    {
                        "exchange": {
                            "volatility": {
                                "dried_fish": 0.08,
                                "fish_roe": 0.12,
                            },
                            "update_timing": "9:00, 15:00, 21:00",
                        },
                        "webui": {"secret_key": "untouched"},
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            path.chmod(0o600)
            result = self.settings.persist_config_updates(
                {"exchange": {"volatility": {"dried_fish": 0.2}}},
                config_path=path,
            )
            saved = json.loads(path.read_text(encoding="utf-8-sig"))

            self.assertEqual(result, [str(path)])
            self.assertEqual(saved["exchange"]["volatility"]["dried_fish"], 0.2)
            self.assertEqual(saved["exchange"]["volatility"]["fish_roe"], 0.12)
            self.assertEqual(saved["exchange"]["update_timing"], "9:00, 15:00, 21:00")
            self.assertEqual(saved["webui"]["secret_key"], "untouched")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertFalse(
                any(item.name.endswith(".tmp") for item in path.parent.iterdir())
            )

    def test_templates_parse_and_settings_are_embedded_in_the_intended_pages(self):
        from jinja2 import Environment

        template_dir = PROJECT_ROOT / "manager" / "templates"
        environment = Environment()
        for path in template_dir.glob("*.html"):
            environment.parse(path.read_text(encoding="utf-8"))

        other = (template_dir / "other_settings.html").read_text(encoding="utf-8")
        self.assertIn("fish_pond_upgrades", other)
        self.assertIn("aquarium_upgrades", other)
        self.assertIn("settings_groups", other)

        for filename in ("bank.html", "exchange.html", "market.html"):
            source = (template_dir / filename).read_text(encoding="utf-8")
            self.assertIn('_runtime_settings_form.html', source)

        layout = (template_dir / "layout.html").read_text(encoding="utf-8")
        self.assertIn("manage_other_settings", layout)
        self.assertNotIn("manage_aquarium_upgrades') }}\"><i", layout)


if __name__ == "__main__":
    unittest.main()
