import importlib
import sqlite3
import sys
import types
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory


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


class DynamicPluginPathTests(unittest.TestCase):
    def test_migrations_load_from_supplied_directory_without_old_plugin_package(self):
        _install_astrbot_stub()
        migration = importlib.import_module("core.database.migration")

        with TemporaryDirectory(ignore_cleanup_errors=True) as temp_dir:
            temp_path = Path(temp_dir)
            migrations_dir = temp_path / "plugin_again" / "core" / "database" / "migrations"
            migrations_dir.mkdir(parents=True)
            (migrations_dir / "001_create_marker.py").write_text(
                "def up(cursor):\n"
                "    cursor.execute('CREATE TABLE marker (id INTEGER PRIMARY KEY)')\n",
                encoding="utf-8",
            )
            db_path = temp_path / "fish.db"

            migration.run_migrations(str(db_path), str(migrations_dir))

            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'marker'"
                )
                self.assertEqual(cursor.fetchone()[0], "marker")
                cursor.execute("SELECT version FROM schema_version")
                self.assertEqual(cursor.fetchone()[0], 1)

    def test_main_does_not_hardcode_the_old_plugin_package_for_effects(self):
        main_py = Path(__file__).resolve().parents[1] / "main.py"
        source = main_py.read_text(encoding="utf-8")

        self.assertNotIn(
            "data.plugins.astrbot_plugin_fishing.core.services.item_effects",
            source,
        )
        self.assertIn(
            'effects_package_path=f"{__package__}.core.services.item_effects"',
            source,
        )


if __name__ == "__main__":
    unittest.main()
