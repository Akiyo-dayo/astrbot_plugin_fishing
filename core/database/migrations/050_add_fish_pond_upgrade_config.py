import json
import sqlite3
from pathlib import Path


DEFAULT_UPGRADES_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "fish_pond_upgrades.json"
)


def _load_default_upgrades():
    with DEFAULT_UPGRADES_PATH.open("r", encoding="utf-8") as file:
        rows = json.load(file)
    return [
        (
            int(row["level"]),
            int(row["capacity"]),
            int(row["cost_coins"]),
            int(row.get("cost_premium", 0)),
            str(row.get("description") or ""),
        )
        for row in rows
    ]


def up(cursor: sqlite3.Cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS fish_pond_upgrades (
            upgrade_id INTEGER PRIMARY KEY AUTOINCREMENT,
            level INTEGER NOT NULL UNIQUE,
            capacity INTEGER NOT NULL,
            cost_coins INTEGER NOT NULL,
            cost_premium INTEGER DEFAULT 0,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.executemany(
        """
        INSERT OR IGNORE INTO fish_pond_upgrades (
            level, capacity, cost_coins, cost_premium, description
        ) VALUES (?, ?, ?, ?, ?)
        """,
        _load_default_upgrades(),
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_fish_pond_upgrades_level
        ON fish_pond_upgrades(level)
        """
    )


def down(cursor: sqlite3.Cursor):
    cursor.execute("DROP TABLE IF EXISTS fish_pond_upgrades")
