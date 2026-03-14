"""
lib/db.py
=========
Shared database connection for all pipeline scripts.
Reads credentials from F:\\weatherbetterv2\\tokens\\db\\db.key

db.key format (one value per line, no labels):
    line 1 — host
    line 2 — port
    line 3 — dbname
    line 4 — user
    line 5 — password
"""

from pathlib import Path
import psycopg2
import psycopg2.extras

DB_KEY_PATH = Path(r"F:\weatherbetterv2\tokens\db\db.key")


def load_db_config() -> dict:
    lines = DB_KEY_PATH.read_text(encoding="utf-8").strip().splitlines()
    if len(lines) < 5:
        raise ValueError(
            f"db.key must have 5 lines (host, port, dbname, user, password). "
            f"Found {len(lines)} lines in {DB_KEY_PATH}"
        )
    return {
        "host":     lines[0].strip(),
        "port":     lines[1].strip(),
        "dbname":   lines[2].strip(),
        "user":     lines[3].strip(),
        "password": lines[4].strip(),
    }


def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection. Caller is responsible for closing."""
    config = load_db_config()
    return psycopg2.connect(**config)


def get_dict_cursor(conn):
    """Return a RealDictCursor for the given connection."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)