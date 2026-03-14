"""
migrate_to_neon.py
------------------
One-time migration: copies schema + all data from local PostgreSQL to Neon.
Run once, then update db.key to point to Neon.
"""

import psycopg2
import psycopg2.extras
from pathlib import Path

# Local DB
local_lines = Path(r"F:/weatherbetterv2/tokens/db/db.key").read_text().strip().splitlines()
LOCAL = dict(host=local_lines[0], port=int(local_lines[1]),
             dbname=local_lines[2], user=local_lines[3], password=local_lines[4])

# Neon
NEON = dict(
    host="ep-gentle-dust-adny4xnh-pooler.c-2.us-east-1.aws.neon.tech",
    port=5432, dbname="neondb", user="neondb_owner",
    password="npg_y3LDcxtX8wqE", sslmode="require"
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS cities (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    state           TEXT,
    timezone        TEXT,
    latitude        NUMERIC,
    longitude       NUMERIC,
    nws_site        TEXT NOT NULL,
    nws_issuedby    TEXT NOT NULL,
    asos_station    TEXT NOT NULL,
    ghcnd_station   TEXT NOT NULL,
    kalshi_ticker   TEXT,
    observes_dst    BOOLEAN DEFAULT true,
    active          BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    nws_grid_x      INTEGER,
    nws_grid_y      INTEGER
);

CREATE TABLE IF NOT EXISTS forecast_sources (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    display_name    TEXT,
    api_base_url    TEXT,
    model_name      TEXT,
    notes           TEXT,
    active          BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id                  SERIAL PRIMARY KEY,
    run_type            TEXT NOT NULL,
    started_at          TIMESTAMPTZ DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              TEXT DEFAULT 'running',
    cities_processed    INTEGER,
    rows_inserted       INTEGER,
    rows_updated        INTEGER,
    error_message       TEXT,
    notes               TEXT
);

CREATE TABLE IF NOT EXISTS observations (
    id                  SERIAL PRIMARY KEY,
    city_id             INTEGER NOT NULL REFERENCES cities(id),
    obs_date            DATE NOT NULL,
    temp_high           NUMERIC,
    temp_high_time      TIME,
    temp_low            NUMERIC,
    temp_low_time       TIME,
    temp_avg            NUMERIC,
    precip              NUMERIC,
    precip_is_trace     BOOLEAN DEFAULT false NOT NULL,
    snow                NUMERIC,
    snow_depth          NUMERIC,
    wind_speed_max      NUMERIC,
    wind_gust_max       NUMERIC,
    wind_avg            NUMERIC,
    humidity_avg        NUMERIC,
    humidity_high       NUMERIC,
    humidity_low        NUMERIC,
    sky_cover_avg       NUMERIC,
    temp_high_normal    NUMERIC,
    temp_low_normal     NUMERIC,
    temp_high_depart    NUMERIC,
    cli_raw_id          INTEGER,
    data_source         TEXT DEFAULT 'nws_cli' NOT NULL,
    is_backfill         BOOLEAN DEFAULT false NOT NULL,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (city_id, obs_date)
);

CREATE TABLE IF NOT EXISTS cli_raw (
    id              SERIAL PRIMARY KEY,
    city_id         INTEGER NOT NULL REFERENCES cities(id),
    obs_date        DATE NOT NULL,
    version         INTEGER DEFAULT 1 NOT NULL,
    fetched_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    raw_text        TEXT NOT NULL,
    is_final        BOOLEAN DEFAULT false NOT NULL,
    UNIQUE (city_id, obs_date, version)
);

CREATE TABLE IF NOT EXISTS forecasts (
    id              SERIAL PRIMARY KEY,
    city_id         INTEGER NOT NULL REFERENCES cities(id),
    source_id       INTEGER NOT NULL REFERENCES forecast_sources(id),
    pipeline_run_id INTEGER REFERENCES pipeline_runs(id),
    target_date     DATE NOT NULL,
    fetched_at      TIMESTAMPTZ NOT NULL,
    lead_days       INTEGER NOT NULL CHECK (lead_days >= 0 AND lead_days <= 16),
    temp_high       NUMERIC,
    temp_low        NUMERIC,
    temp_avg        NUMERIC,
    precip          NUMERIC,
    precip_prob     NUMERIC CHECK (precip_prob IS NULL OR (precip_prob >= 0 AND precip_prob <= 100)),
    wind_speed_max  NUMERIC,
    wind_gust_max   NUMERIC,
    humidity_avg    NUMERIC,
    created_at      TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE (city_id, source_id, target_date, lead_days),
    CHECK (
        (temp_high IS NULL OR (temp_high >= -60 AND temp_high <= 140)) AND
        (temp_low  IS NULL OR (temp_low  >= -60 AND temp_low  <= 130)) AND
        (temp_high IS NULL OR temp_low IS NULL OR temp_high >= temp_low)
    )
);

CREATE TABLE IF NOT EXISTS forecast_verifications (
    id                          SERIAL PRIMARY KEY,
    forecast_id                 INTEGER NOT NULL UNIQUE REFERENCES forecasts(id),
    observation_id              INTEGER NOT NULL REFERENCES observations(id),
    pipeline_run_id             INTEGER REFERENCES pipeline_runs(id),
    high_error                  NUMERIC NOT NULL,
    high_abs_error              NUMERIC NOT NULL,
    high_bias                   NUMERIC NOT NULL,
    low_error                   NUMERIC,
    low_abs_error               NUMERIC,
    high_within_1f              BOOLEAN NOT NULL,
    high_within_2f              BOOLEAN NOT NULL,
    high_within_3f              BOOLEAN NOT NULL,
    high_within_5f              BOOLEAN NOT NULL,
    high_above_normal           BOOLEAN,
    forecast_above_normal       BOOLEAN,
    normal_direction_correct    BOOLEAN,
    verified_at                 TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
"""

# Tables in dependency order for copying
TABLES = [
    "cities",
    "forecast_sources",
    "pipeline_runs",
    "observations",
    "cli_raw",
    "forecasts",
    "forecast_verifications",
]


def copy_table(local_conn, neon_conn, table):
    local_cur = local_conn.cursor()
    neon_cur = neon_conn.cursor()

    local_cur.execute(f"SELECT COUNT(*) FROM {table}")
    total = local_cur.fetchone()[0]

    if total == 0:
        print(f"  {table}: 0 rows — skipping")
        return 0

    # Get column names
    local_cur.execute(f"SELECT * FROM {table} LIMIT 0")
    cols = [d[0] for d in local_cur.description]
    cols_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    # Fetch all rows
    local_cur.execute(f"SELECT {cols_str} FROM {table} ORDER BY id")
    rows = local_cur.fetchall()

    # Clear target and insert
    neon_cur.execute(f"TRUNCATE {table} CASCADE")
    psycopg2.extras.execute_batch(
        neon_cur,
        f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})",
        rows,
        page_size=500
    )

    print(f"  {table}: {len(rows):,} rows copied")
    return len(rows)


def reset_sequences(neon_conn):
    """Reset all sequences to max(id) so new inserts don't conflict."""
    cur = neon_conn.cursor()
    cur.execute("""
        SELECT sequence_name FROM information_schema.sequences
        WHERE sequence_schema = 'public'
    """)
    seqs = [r[0] for r in cur.fetchall()]
    for seq in seqs:
        # Derive table name from sequence (e.g. cities_id_seq → cities)
        table = seq.replace("_id_seq", "")
        try:
            cur.execute(f"SELECT setval('{seq}', COALESCE((SELECT MAX(id) FROM {table}), 1))")
        except Exception:
            neon_conn.rollback()
    neon_conn.commit()
    print(f"  Reset {len(seqs)} sequences")


def main():
    print("Connecting to local DB...")
    local_conn = psycopg2.connect(**LOCAL)
    print("Connecting to Neon...")
    neon_conn = psycopg2.connect(**NEON)

    print("\nCreating schema on Neon...")
    with neon_conn:
        with neon_conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    print("  Schema created")

    print("\nCopying data...")
    total = 0
    with neon_conn:
        for table in TABLES:
            total += copy_table(local_conn, neon_conn, table)

    print("\nResetting sequences...")
    reset_sequences(neon_conn)

    print(f"\nMigration complete — {total:,} total rows copied")

    # Verify
    print("\nVerification:")
    neon_cur = neon_conn.cursor()
    for table in TABLES:
        neon_cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"  {table}: {neon_cur.fetchone()[0]:,} rows")

    local_conn.close()
    neon_conn.close()


if __name__ == "__main__":
    main()
