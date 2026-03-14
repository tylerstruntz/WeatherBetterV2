-- =============================================================================
-- WEATHER DATABASE SCHEMA
-- =============================================================================
-- Purpose : Store NWS observations, multi-source forecasts, ML accuracy
--           scores, and raw CLI reports for Kalshi temperature market analysis.
-- DB name : weather
-- Postgres : 14+
-- =============================================================================
-- TABLE ORDER (dependency-safe)
--   1.  extensions
--   2.  pipeline_runs          (no dependencies)
--   3.  cities                 (no dependencies)
--   4.  forecast_sources       (no dependencies)
--   5.  cli_raw                (depends on: cities)
--   6.  observations           (depends on: cities — cli_raw FK added at step 10)
--   7.  forecasts              (depends on: cities, forecast_sources, pipeline_runs)
--   8.  forecast_verifications (depends on: forecasts, observations, pipeline_runs)
--   9.  indexes
--  10.  deferred FK            (observations.cli_raw_id added after cli_raw exists)
--  11.  constraints
--  12.  seed data — cities
--  13.  seed data — forecast_sources
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. EXTENSIONS
-- -----------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS btree_gist;


-- -----------------------------------------------------------------------------
-- 2. PIPELINE_RUNS
-- -----------------------------------------------------------------------------
-- Defined first because forecasts + forecast_verifications both FK to it.

CREATE TABLE pipeline_runs (
    id               SERIAL       PRIMARY KEY,
    run_type         TEXT         NOT NULL,          -- 'ingest' | 'score' | 'backfill' | 'cli_fetch'
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           TEXT         NOT NULL DEFAULT 'running', -- 'running' | 'success' | 'failed'
    cities_processed INTEGER,
    rows_inserted    INTEGER,
    rows_updated     INTEGER,
    error_message    TEXT,
    notes            TEXT
);

COMMENT ON TABLE pipeline_runs IS 'Audit log for every pipeline execution. Insert a run row first, update on finish.';


-- -----------------------------------------------------------------------------
-- 3. CITIES
-- -----------------------------------------------------------------------------

CREATE TABLE cities (
    id               SERIAL       PRIMARY KEY,
    name             TEXT         NOT NULL,
    state            TEXT         NOT NULL,
    timezone         TEXT         NOT NULL,          -- IANA tz, e.g. 'America/Los_Angeles'
    latitude         NUMERIC(7,4) NOT NULL,
    longitude        NUMERIC(7,4) NOT NULL,

    -- NWS CLI product identifiers
    nws_site         TEXT         NOT NULL,          -- e.g. 'MTR'
    nws_issuedby     TEXT         NOT NULL,          -- e.g. 'SFO'

    -- ASOS station — physical sensor Kalshi settles against
    asos_station     TEXT         NOT NULL UNIQUE,   -- e.g. 'KSFO'

    -- GHCND station — for 7-year historical backfill via NOAA API
    ghcnd_station    TEXT         NOT NULL UNIQUE,   -- e.g. 'USW00023234'

    -- Kalshi market slug city code
    kalshi_ticker    TEXT,

    -- FALSE only for Phoenix (America/Phoenix never observes DST)
    observes_dst     BOOLEAN      NOT NULL DEFAULT TRUE,

    active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  cities                IS 'One row per Kalshi weather market city.';
COMMENT ON COLUMN cities.nws_site       IS 'NWS office site code, e.g. MTR for Bay Area.';
COMMENT ON COLUMN cities.nws_issuedby   IS 'CLI issuedby code — matches ASOS station without leading K.';
COMMENT ON COLUMN cities.asos_station   IS 'ICAO station ID — the physical sensor Kalshi settles against.';
COMMENT ON COLUMN cities.ghcnd_station  IS 'NOAA GHCND station ID for historical backfill.';
COMMENT ON COLUMN cities.observes_dst   IS 'FALSE only for Phoenix (America/Phoenix).';


-- -----------------------------------------------------------------------------
-- 4. FORECAST_SOURCES
-- -----------------------------------------------------------------------------

CREATE TABLE forecast_sources (
    id               SERIAL       PRIMARY KEY,
    name             TEXT         NOT NULL UNIQUE,   -- e.g. 'open_meteo_gfs'
    display_name     TEXT         NOT NULL,
    api_base_url     TEXT,
    model_name       TEXT,                           -- underlying NWP model, e.g. 'GFS'
    notes            TEXT,
    active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE forecast_sources IS 'Registry of forecast providers. One row per source/model combination.';


-- -----------------------------------------------------------------------------
-- 5. CLI_RAW
-- -----------------------------------------------------------------------------
-- Raw NWS CLI product text stored verbatim for auditability and re-parsing.
-- Always use the highest version number for settlement — never assume version 1
-- is final. Pipeline polls until no new version appears for ~4 hours.

CREATE TABLE cli_raw (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    obs_date         DATE         NOT NULL,
    version          INTEGER      NOT NULL DEFAULT 1,
    fetched_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    raw_text         TEXT         NOT NULL,
    is_final         BOOLEAN      NOT NULL DEFAULT FALSE,

    UNIQUE (city_id, obs_date, version)
);

COMMENT ON TABLE  cli_raw          IS 'Verbatim NWS CLI product text. Kept for auditability and re-parsing.';
COMMENT ON COLUMN cli_raw.version  IS 'NWS amendment version. Always use highest version for settlement.';
COMMENT ON COLUMN cli_raw.is_final IS 'Set TRUE by pipeline after no new version found for ~4 hours.';


-- -----------------------------------------------------------------------------
-- 6. OBSERVATIONS
-- -----------------------------------------------------------------------------
-- Official daily values parsed from NWS CLI reports (ASOS stations).
-- These are the exact values Kalshi settles against.
-- One row per city per date.
--
-- DST note: NWS records the daily high over local standard time midnight-to-
-- midnight. During DST the observation window shifts to 01:00-00:59 the next
-- wall-clock day. obs_date always reflects the NWS climate summary date.

CREATE TABLE observations (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    obs_date         DATE         NOT NULL,

    -- Temperature — primary Kalshi settlement values
    temp_high        NUMERIC(5,1),                   -- °F daily maximum
    temp_high_time   TIME,                           -- LST time of observed high
    temp_low         NUMERIC(5,1),                   -- °F daily minimum
    temp_low_time    TIME,                           -- LST time of observed low
    temp_avg         NUMERIC(5,1),                   -- °F (high+low)/2

    -- Precipitation
    precip           NUMERIC(6,2),                   -- inches; trace stored as 0.001
    precip_is_trace  BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Snowfall
    snow             NUMERIC(6,1),                   -- inches
    snow_depth       NUMERIC(6,1),                   -- inches on ground

    -- Wind
    wind_speed_max   NUMERIC(5,1),                   -- mph highest sustained
    wind_gust_max    NUMERIC(5,1),                   -- mph highest gust
    wind_avg         NUMERIC(5,1),                   -- mph average

    -- Humidity
    humidity_avg     NUMERIC(5,1),                   -- percent
    humidity_high    NUMERIC(5,1),
    humidity_low     NUMERIC(5,1),

    -- Sky
    sky_cover_avg    NUMERIC(3,1),                   -- 0.0-1.0 (tenths)

    -- Departure from normal (1991-2020 baseline, from CLI report)
    temp_high_normal NUMERIC(5,1),                   -- °F 1991-2020 normal high
    temp_low_normal  NUMERIC(5,1),                   -- °F 1991-2020 normal low
    temp_high_depart NUMERIC(5,1),                   -- °F observed minus normal

    -- Source tracking
    cli_raw_id       INTEGER,                        -- FK added at step 10 below
    data_source      TEXT         NOT NULL DEFAULT 'nws_cli', -- 'nws_cli' | 'ghcnd_backfill'
    is_backfill      BOOLEAN      NOT NULL DEFAULT FALSE,

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (city_id, obs_date)
);

COMMENT ON TABLE  observations              IS 'Official daily observations — ground truth for Kalshi settlement.';
COMMENT ON COLUMN observations.precip       IS 'Inches. Trace amounts stored as 0.001 with precip_is_trace = TRUE.';
COMMENT ON COLUMN observations.data_source  IS 'nws_cli for live pipeline rows, ghcnd_backfill for historical load.';
COMMENT ON COLUMN observations.cli_raw_id   IS 'FK to cli_raw — which CLI version this row was parsed from.';


-- -----------------------------------------------------------------------------
-- 7. FORECASTS
-- -----------------------------------------------------------------------------
-- Daily forecast snapshots captured at pipeline run time.
-- The same target_date will have many rows — one per (source, city, lead_days).
-- lead_days = target_date - DATE(fetched_at in city local time)

CREATE TABLE forecasts (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    source_id        INTEGER      NOT NULL REFERENCES forecast_sources(id),
    pipeline_run_id  INTEGER      REFERENCES pipeline_runs(id),

    target_date      DATE         NOT NULL,
    fetched_at       TIMESTAMPTZ  NOT NULL,
    lead_days        INTEGER      NOT NULL CHECK (lead_days BETWEEN 0 AND 16),

    -- Temperature — Kalshi-relevant values
    temp_high        NUMERIC(5,1),                   -- °F predicted high
    temp_low         NUMERIC(5,1),                   -- °F predicted low
    temp_avg         NUMERIC(5,1),                   -- °F predicted average

    -- Precipitation
    precip           NUMERIC(6,2),                   -- inches predicted
    precip_prob      NUMERIC(5,1),                   -- % probability of precipitation

    -- Wind
    wind_speed_max   NUMERIC(5,1),                   -- mph
    wind_gust_max    NUMERIC(5,1),                   -- mph

    -- Humidity
    humidity_avg     NUMERIC(5,1),                   -- percent

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (city_id, source_id, target_date, lead_days)
);

COMMENT ON TABLE  forecasts           IS 'Daily forecast snapshots — one row per source + city + date + lead_days.';
COMMENT ON COLUMN forecasts.lead_days IS 'Days from fetch to target. Core ML feature — 1-day vs 7-day accuracy differs significantly.';
COMMENT ON COLUMN forecasts.temp_high IS 'Predicted daily high — primary value for Kalshi contract verification.';


-- -----------------------------------------------------------------------------
-- 8. FORECAST_VERIFICATIONS
-- -----------------------------------------------------------------------------
-- Created by score.py once obs_date has passed and an observation exists.
-- Error convention: error = forecast - observed
--   positive = forecast ran hot (overestimated)
--   negative = forecast ran cold (underestimated)

CREATE TABLE forecast_verifications (
    id                       SERIAL       PRIMARY KEY,
    forecast_id              INTEGER      NOT NULL UNIQUE REFERENCES forecasts(id),
    observation_id           INTEGER      NOT NULL REFERENCES observations(id),
    pipeline_run_id          INTEGER      REFERENCES pipeline_runs(id),

    -- High temperature errors — primary Kalshi metric
    high_error               NUMERIC(5,1) NOT NULL,  -- forecast_high - observed_high
    high_abs_error           NUMERIC(5,1) NOT NULL,  -- ABS(high_error)
    high_bias                NUMERIC(5,1) NOT NULL,  -- signed error (explicit for query clarity)

    -- Low temperature errors
    low_error                NUMERIC(5,1),
    low_abs_error            NUMERIC(5,1),

    -- Threshold proximity — pre-computed for fast Kalshi edge queries
    high_within_1f           BOOLEAN      NOT NULL,
    high_within_2f           BOOLEAN      NOT NULL,
    high_within_3f           BOOLEAN      NOT NULL,
    high_within_5f           BOOLEAN      NOT NULL,

    -- Above/below normal accuracy — key edge signal
    high_above_normal        BOOLEAN,                -- was observed high above 1991-2020 normal?
    forecast_above_normal    BOOLEAN,                -- did forecast predict above normal?
    normal_direction_correct BOOLEAN,                -- did forecast get the above/below call right?

    verified_at              TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  forecast_verifications                       IS 'ML scoring output. One row per forecast, created after obs_date passes.';
COMMENT ON COLUMN forecast_verifications.high_error            IS 'Positive = forecast ran hot. Negative = forecast ran cold.';
COMMENT ON COLUMN forecast_verifications.high_within_1f        IS 'Pre-computed for fast threshold probability queries.';
COMMENT ON COLUMN forecast_verifications.normal_direction_correct IS 'Key edge signal: did forecast correctly call above/below normal?';


-- -----------------------------------------------------------------------------
-- 9. INDEXES
-- -----------------------------------------------------------------------------

CREATE INDEX idx_runs_type_status    ON pipeline_runs (run_type, status, started_at DESC);
CREATE INDEX idx_cities_active       ON cities (active) WHERE active = TRUE;
CREATE INDEX idx_cli_city_date       ON cli_raw (city_id, obs_date DESC);
CREATE INDEX idx_cli_not_final       ON cli_raw (city_id, obs_date) WHERE is_final = FALSE;
CREATE INDEX idx_obs_city_date       ON observations (city_id, obs_date DESC);
CREATE INDEX idx_obs_date            ON observations (obs_date DESC);
CREATE INDEX idx_obs_backfill        ON observations (is_backfill) WHERE is_backfill = TRUE;
CREATE INDEX idx_fcast_city_target   ON forecasts (city_id, target_date DESC);
CREATE INDEX idx_fcast_city_lead     ON forecasts (city_id, lead_days, target_date DESC);
CREATE INDEX idx_fcast_source_city   ON forecasts (source_id, city_id, target_date DESC);
CREATE INDEX idx_fv_obs              ON forecast_verifications (observation_id);
CREATE INDEX idx_fv_verified_at      ON forecast_verifications (verified_at DESC);


-- -----------------------------------------------------------------------------
-- 10. DEFERRED FOREIGN KEY
-- -----------------------------------------------------------------------------
-- observations.cli_raw_id could not be declared inline because the correct
-- table creation order puts cli_raw before observations but forecasts after.
-- Adding it here now that both tables exist.

ALTER TABLE observations
    ADD CONSTRAINT fk_obs_cli_raw
    FOREIGN KEY (cli_raw_id) REFERENCES cli_raw(id);


-- -----------------------------------------------------------------------------
-- 11. CONSTRAINTS
-- -----------------------------------------------------------------------------

ALTER TABLE observations ADD CONSTRAINT chk_obs_temp_range
    CHECK (
        (temp_high IS NULL OR temp_high BETWEEN -60 AND 140) AND
        (temp_low  IS NULL OR temp_low  BETWEEN -60 AND 130) AND
        (temp_high IS NULL OR temp_low  IS NULL OR temp_high >= temp_low)
    );

ALTER TABLE observations ADD CONSTRAINT chk_precip_trace
    CHECK (precip IS NULL OR precip >= 0);

ALTER TABLE forecasts ADD CONSTRAINT chk_fcast_temp_range
    CHECK (
        (temp_high IS NULL OR temp_high BETWEEN -60 AND 140) AND
        (temp_low  IS NULL OR temp_low  BETWEEN -60 AND 130) AND
        (temp_high IS NULL OR temp_low  IS NULL OR temp_high >= temp_low)
    );

ALTER TABLE forecasts ADD CONSTRAINT chk_precip_prob
    CHECK (precip_prob IS NULL OR precip_prob BETWEEN 0 AND 100);


-- -----------------------------------------------------------------------------
-- 12. SEED DATA — CITIES
-- -----------------------------------------------------------------------------

INSERT INTO cities (name, state, timezone, latitude, longitude,
                    nws_site, nws_issuedby, asos_station, ghcnd_station,
                    kalshi_ticker, observes_dst)
VALUES
    ('New Orleans',       'LA', 'America/Chicago',      29.9934,  -90.2580, 'LIX', 'MSY', 'KMSY', 'USW00012916', 'MSY', TRUE),
    ('Washington DC',     'DC', 'America/New_York',     38.8521,  -77.0378, 'LWX', 'DCA', 'KDCA', 'USW00013743', 'DCA', TRUE),
    ('Miami',             'FL', 'America/New_York',     25.7959,  -80.2870, 'MFL', 'MIA', 'KMIA', 'USW00012839', 'MIA', TRUE),
    ('Oklahoma City',     'OK', 'America/Chicago',      35.3931,  -97.6007, 'OUN', 'OKC', 'KOKC', 'USW00013967', 'OKC', TRUE),
    ('San Antonio',       'TX', 'America/Chicago',      29.5338,  -98.4698, 'EWX', 'SAT', 'KSAT', 'USW00012921', 'SAT', TRUE),
    ('Dallas-Fort Worth', 'TX', 'America/Chicago',      32.8998,  -97.0403, 'FWD', 'DFW', 'KDFW', 'USW00003927', 'DFW', TRUE),
    ('Los Angeles',       'CA', 'America/Los_Angeles',  33.9425, -118.4081, 'LOX', 'LAX', 'KLAX', 'USW00023174', 'LAX', TRUE),
    ('Seattle',           'WA', 'America/Los_Angeles',  47.4489, -122.3094, 'SEW', 'SEA', 'KSEA', 'USW00024233', 'SEA', TRUE),
    ('Houston',           'TX', 'America/Chicago',      29.6454,  -95.2789, 'HGX', 'HOU', 'KHOU', 'USW00012918', 'HOU', TRUE),
    ('Philadelphia',      'PA', 'America/New_York',     39.8719,  -75.2411, 'PHI', 'PHL', 'KPHL', 'USW00013739', 'PHL', TRUE),
    ('Phoenix',           'AZ', 'America/Phoenix',      33.4373, -112.0078, 'PSR', 'PHX', 'KPHX', 'USW00023183', 'PHX', FALSE),
    ('Boston',            'MA', 'America/New_York',     42.3606,  -71.0097, 'BOX', 'BOS', 'KBOS', 'USW00014739', 'BOS', TRUE),
    ('Las Vegas',         'NV', 'America/Los_Angeles',  36.0840, -115.1522, 'VEF', 'LAS', 'KLAS', 'USW00023169', 'LAS', TRUE),
    ('Denver',            'CO', 'America/Denver',       39.8561, -104.6737, 'BOU', 'DEN', 'KDEN', 'USW00003017', 'DEN', TRUE),
    ('Chicago Midway',    'IL', 'America/Chicago',      41.7868,  -87.7522, 'LOT', 'MDW', 'KMDW', 'USW00014819', 'MDW', TRUE),
    ('Atlanta',           'GA', 'America/New_York',     33.6367,  -84.4281, 'FFC', 'ATL', 'KATL', 'USW00013874', 'ATL', TRUE),
    ('New York City',     'NY', 'America/New_York',     40.7789,  -73.9692, 'OKX', 'NYC', 'KNYC', 'USW00094728', 'NYC', TRUE),
    ('Minneapolis',       'MN', 'America/Chicago',      44.8831,  -93.2289, 'MPX', 'MSP', 'KMSP', 'USW00014922', 'MSP', TRUE),
    ('Austin',            'TX', 'America/Chicago',      30.1945,  -97.6699, 'EWX', 'AUS', 'KAUS', 'USW00013904', 'AUS', TRUE),
    ('San Francisco',     'CA', 'America/Los_Angeles',  37.6197, -122.3647, 'MTR', 'SFO', 'KSFO', 'USW00023234', 'SFO', TRUE);


-- -----------------------------------------------------------------------------
-- 13. SEED DATA — FORECAST SOURCES
-- -----------------------------------------------------------------------------

INSERT INTO forecast_sources (name, display_name, api_base_url, model_name, notes)
VALUES
    (
        'open_meteo_gfs',
        'Open-Meteo (GFS)',
        'https://api.open-meteo.com/v1/forecast',
        'GFS',
        'NOAA GFS model via Open-Meteo. Free, no API key required. 16-day forecast window. Primary source.'
    ),
    (
        'open_meteo_ecmwf',
        'Open-Meteo (ECMWF IFS)',
        'https://api.open-meteo.com/v1/forecast',
        'ECMWF_IFS025',
        'European Centre model via Open-Meteo. Free tier. Generally more accurate at 5-10 day range.'
    ),
    (
        'nws_api',
        'NWS API (Official)',
        'https://api.weather.gov/points',
        'NAM/GFS blend',
        'Official NWS gridded forecast. What most market makers reference. This is the baseline to beat.'
    );


-- -----------------------------------------------------------------------------
-- END OF SCHEMA
-- -----------------------------------------------------------------------------