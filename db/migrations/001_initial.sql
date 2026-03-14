-- =============================================================================
-- WEATHER DATABASE SCHEMA
-- =============================================================================
-- Purpose : Store NWS observations, multi-source forecasts, ML accuracy
--           scores, and raw CLI reports for Kalshi temperature market analysis.
-- DB name : weather
-- Postgres : 14+
-- Created  : 2026
-- =============================================================================
-- TABLE OF CONTENTS
--   1. extensions
--   2. cities
--   3. forecast_sources
--   4. observations
--   5. cli_raw
--   6. forecasts
--   7. forecast_verifications
--   8. pipeline_runs
--   9. indexes
--  10. constraints
--  11. seed data — cities
--  12. seed data — forecast_sources
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. EXTENSIONS
-- -----------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS btree_gist;   -- needed for exclusion constraints


-- -----------------------------------------------------------------------------
-- 2. CITIES
-- -----------------------------------------------------------------------------
-- One row per market city. All NWS + ASOS codes live here so scripts never
-- hardcode them. kalshi_ticker is the city code Kalshi uses in market slugs.

CREATE TABLE cities (
    id               SERIAL       PRIMARY KEY,
    name             TEXT         NOT NULL,          -- e.g. 'San Francisco'
    state            TEXT         NOT NULL,          -- e.g. 'CA'
    timezone         TEXT         NOT NULL,          -- IANA tz, e.g. 'America/Los_Angeles'
    latitude         NUMERIC(7,4) NOT NULL,
    longitude        NUMERIC(7,4) NOT NULL,

    -- NWS CLI product identifiers — used to fetch daily climate reports
    nws_site         TEXT         NOT NULL,          -- e.g. 'MTR'
    nws_issuedby     TEXT         NOT NULL,          -- e.g. 'SFO'  (also = ASOS station w/o K)

    -- ASOS station — the physical sensor Kalshi settles against
    asos_station     TEXT         NOT NULL UNIQUE,   -- e.g. 'KSFO'

    -- GHCND station — used for 7-year historical backfill via NOAA API
    ghcnd_station    TEXT         NOT NULL UNIQUE,   -- e.g. 'USW00023234'

    -- Kalshi market slug prefix — e.g. 'KXSFO' (confirm per market before use)
    kalshi_ticker    TEXT,

    -- DST flag: FALSE for Phoenix only (America/Phoenix never changes)
    observes_dst     BOOLEAN      NOT NULL DEFAULT TRUE,

    active           BOOLEAN      NOT NULL DEFAULT TRUE,

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  cities                IS 'One row per Kalshi weather market city.';
COMMENT ON COLUMN cities.nws_site       IS 'NWS office site code, e.g. MTR for Bay Area.';
COMMENT ON COLUMN cities.nws_issuedby   IS 'CLI issuedby code, matches ASOS station without leading K.';
COMMENT ON COLUMN cities.asos_station   IS 'ICAO station ID — the sensor Kalshi settles against.';
COMMENT ON COLUMN cities.ghcnd_station  IS 'NOAA GHCND station ID for historical backfill.';
COMMENT ON COLUMN cities.observes_dst   IS 'FALSE only for Phoenix (America/Phoenix).';


-- -----------------------------------------------------------------------------
-- 3. FORECAST SOURCES
-- -----------------------------------------------------------------------------
-- Each forecasting provider / model gets a row. Adding a new source later is
-- just an INSERT here plus wiring up a new fetcher script.

CREATE TABLE forecast_sources (
    id               SERIAL       PRIMARY KEY,
    name             TEXT         NOT NULL UNIQUE,   -- e.g. 'open_meteo_gfs'
    display_name     TEXT         NOT NULL,          -- e.g. 'Open-Meteo (GFS)'
    api_base_url     TEXT,
    model_name       TEXT,                           -- underlying NWP model, e.g. 'GFS'
    notes            TEXT,
    active           BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE forecast_sources IS 'Registry of forecast providers. One row per source/model combination.';


-- -----------------------------------------------------------------------------
-- 4. OBSERVATIONS
-- -----------------------------------------------------------------------------
-- Ground truth daily values from NWS CLI reports (via ASOS stations).
-- These are the values Kalshi actually settles against.
-- One row per city per date. Unique constraint prevents duplicate ingestion.
--
-- DST note: NWS records the daily high over local standard time midnight-to-
-- midnight. During DST the observation window is 01:00–00:59 the next calendar
-- day in wall-clock time. The pipeline must handle this — obs_date here always
-- reflects the NWS climate summary date, NOT the wall-clock date.

CREATE TABLE observations (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    obs_date         DATE         NOT NULL,          -- NWS climate summary date

    -- Temperature — the primary Kalshi settlement values
    temp_high        NUMERIC(5,1),                   -- °F, daily maximum
    temp_high_time   TIME,                           -- LST time of observed high
    temp_low         NUMERIC(5,1),                   -- °F, daily minimum
    temp_low_time    TIME,                           -- LST time of observed low
    temp_avg         NUMERIC(5,1),                   -- °F, (high+low)/2

    -- Precipitation
    precip           NUMERIC(6,2),                   -- inches, T stored as 0.001
    precip_is_trace  BOOLEAN      NOT NULL DEFAULT FALSE,

    -- Snowfall
    snow             NUMERIC(6,1),                   -- inches
    snow_depth       NUMERIC(6,1),                   -- inches, snow on ground

    -- Wind
    wind_speed_max   NUMERIC(5,1),                   -- mph, highest sustained
    wind_gust_max    NUMERIC(5,1),                   -- mph, highest gust
    wind_avg         NUMERIC(5,1),                   -- mph, average

    -- Humidity
    humidity_avg     NUMERIC(5,1),                   -- percent
    humidity_high    NUMERIC(5,1),
    humidity_low     NUMERIC(5,1),

    -- Sky
    sky_cover_avg    NUMERIC(3,1),                   -- tenths, 0.0–1.0

    -- Departure from normal (stored for convenience, from CLI report)
    temp_high_normal NUMERIC(5,1),                   -- °F, 1991-2020 normal high
    temp_low_normal  NUMERIC(5,1),                   -- °F, 1991-2020 normal low
    temp_high_depart NUMERIC(5,1),                   -- °F, observed minus normal

    -- Source tracking
    cli_raw_id       INTEGER      REFERENCES cli_raw(id),  -- which CLI version sourced this
    data_source      TEXT         NOT NULL DEFAULT 'nws_cli', -- 'nws_cli' | 'ghcnd_backfill'
    is_backfill      BOOLEAN      NOT NULL DEFAULT FALSE,

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    UNIQUE (city_id, obs_date)
);

COMMENT ON TABLE  observations              IS 'Official daily observations — ground truth for Kalshi settlement.';
COMMENT ON COLUMN observations.precip       IS 'Inches. Trace amounts stored as 0.001 with precip_is_trace = TRUE.';
COMMENT ON COLUMN observations.data_source  IS 'nws_cli for live pipeline, ghcnd_backfill for historical load.';
COMMENT ON COLUMN observations.cli_raw_id   IS 'FK to the specific CLI version this row was parsed from.';


-- -----------------------------------------------------------------------------
-- 5. CLI_RAW
-- -----------------------------------------------------------------------------
-- Raw NWS CLI product text. Stored verbatim so we can re-parse if our parser
-- has a bug, and to track amendments (up to 50 versions per city per day).
-- The pipeline fetches version 1 on first run, then re-checks for higher
-- version numbers until no new version appears for 2+ hours.

CREATE TABLE cli_raw (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    obs_date         DATE         NOT NULL,          -- the climate summary date inside the report
    version          INTEGER      NOT NULL DEFAULT 1,-- NWS version number (1 = first issued)
    fetched_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    raw_text         TEXT         NOT NULL,          -- full CLI product text verbatim
    is_final         BOOLEAN      NOT NULL DEFAULT FALSE, -- TRUE once no newer version found for 4h

    UNIQUE (city_id, obs_date, version)
);

COMMENT ON TABLE  cli_raw          IS 'Verbatim NWS CLI product text. Kept for auditability and re-parsing.';
COMMENT ON COLUMN cli_raw.version  IS 'NWS amendment version. Always use highest version for settlement.';
COMMENT ON COLUMN cli_raw.is_final IS 'Set TRUE by pipeline after no new version found for ~4 hours.';


-- -----------------------------------------------------------------------------
-- 6. FORECASTS
-- -----------------------------------------------------------------------------
-- Daily forecast snapshots captured at pipeline run time.
-- Key design: the same target_date will have multiple rows — one per
-- (source, city, lead_days). This lead_days dimension is core to the ML model:
-- a 7-day forecast and a 1-day forecast for the same date are different data.
--
-- lead_days = (target_date - DATE(fetched_at in city local time))
-- e.g. forecast fetched on June 1 for June 6 → lead_days = 5

CREATE TABLE forecasts (
    id               SERIAL       PRIMARY KEY,
    city_id          INTEGER      NOT NULL REFERENCES cities(id),
    source_id        INTEGER      NOT NULL REFERENCES forecast_sources(id),

    target_date      DATE         NOT NULL,          -- the date being forecast
    fetched_at       TIMESTAMPTZ  NOT NULL,           -- when we captured this forecast
    lead_days        INTEGER      NOT NULL            -- days between fetch and target
                         CHECK (lead_days BETWEEN 0 AND 16),

    -- Temperature forecasts — the Kalshi-relevant values
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

    -- Pipeline run that captured this forecast
    pipeline_run_id  INTEGER      REFERENCES pipeline_runs(id),

    created_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    -- One forecast snapshot per source/city/date/lead combination
    UNIQUE (city_id, source_id, target_date, lead_days)
);

COMMENT ON TABLE  forecasts           IS 'Daily forecast snapshots, one row per source+city+date+lead_days.';
COMMENT ON COLUMN forecasts.lead_days IS 'Days from fetch to target. Core ML feature — 1-day vs 7-day accuracy differs significantly.';
COMMENT ON COLUMN forecasts.temp_high IS 'Predicted daily high — primary value for Kalshi contract verification.';


-- -----------------------------------------------------------------------------
-- 7. FORECAST_VERIFICATIONS
-- -----------------------------------------------------------------------------
-- Created by score.py after obs_date has passed and an observation exists.
-- Pairs each forecast row with its ground-truth observation and computes
-- error metrics. Pre-computed columns avoid expensive recalculation at
-- query time when scanning thousands of rows for edge analysis.
--
-- Error convention: error = forecast - observed  (positive = forecast ran hot)

CREATE TABLE forecast_verifications (
    id                  SERIAL       PRIMARY KEY,
    forecast_id         INTEGER      NOT NULL REFERENCES forecasts(id) UNIQUE,
    observation_id      INTEGER      NOT NULL REFERENCES observations(id),

    -- High temperature errors — primary Kalshi metric
    high_error          NUMERIC(5,1) NOT NULL,   -- forecast_high - observed_high
    high_abs_error      NUMERIC(5,1) NOT NULL,   -- ABS(high_error)
    high_bias           NUMERIC(5,1) NOT NULL,   -- signed, same as high_error (explicit for clarity)

    -- Low temperature errors
    low_error           NUMERIC(5,1),            -- forecast_low - observed_low
    low_abs_error       NUMERIC(5,1),

    -- Threshold proximity flags — pre-computed for fast Kalshi edge queries
    -- "Was the observed high within N°F of the forecast high?"
    high_within_1f      BOOLEAN      NOT NULL,
    high_within_2f      BOOLEAN      NOT NULL,
    high_within_3f      BOOLEAN      NOT NULL,
    high_within_5f      BOOLEAN      NOT NULL,

    -- Direction flag — did forecast predict the correct side of normal?
    high_above_normal   BOOLEAN,                 -- was observed high above the 1991-2020 normal?
    forecast_above_normal BOOLEAN,               -- did the forecast predict above normal?
    normal_direction_correct BOOLEAN,            -- did forecast get the above/below call right?

    -- Scoring metadata
    verified_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    pipeline_run_id     INTEGER      REFERENCES pipeline_runs(id)
);

COMMENT ON TABLE  forecast_verifications                  IS 'ML scoring output. One row per forecast once obs_date has passed.';
COMMENT ON COLUMN forecast_verifications.high_error       IS 'Positive = forecast ran hot. Negative = forecast ran cold.';
COMMENT ON COLUMN forecast_verifications.high_within_1f   IS 'Pre-computed for fast threshold probability queries.';
COMMENT ON COLUMN forecast_verifications.normal_direction_correct IS 'Key edge signal: did forecast call above/below normal correctly?';


-- -----------------------------------------------------------------------------
-- 8. PIPELINE_RUNS
-- -----------------------------------------------------------------------------
-- Audit log for every pipeline execution. Lets you diagnose missing data,
-- API failures, and drift in run timing. Referenced by forecasts and
-- forecast_verifications so you can trace any row back to the run that created it.

CREATE TABLE pipeline_runs (
    id               SERIAL       PRIMARY KEY,
    run_type         TEXT         NOT NULL,          -- 'ingest' | 'score' | 'backfill' | 'cli_fetch'
    started_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMPTZ,
    status           TEXT         NOT NULL DEFAULT 'running', -- 'running' | 'success' | 'failed'
    cities_processed INTEGER,
    rows_inserted    INTEGER,
    rows_updated     INTEGER,
    error_message    TEXT,                           -- NULL on success
    notes            TEXT                            -- free-form, e.g. 'initial 7yr backfill'
);

COMMENT ON TABLE pipeline_runs IS 'Audit log for every pipeline execution. Always create a run row first, update on finish.';


-- -----------------------------------------------------------------------------
-- 9. INDEXES
-- -----------------------------------------------------------------------------

-- observations — most queries filter by city + date range
CREATE INDEX idx_obs_city_date       ON observations (city_id, obs_date DESC);
CREATE INDEX idx_obs_date            ON observations (obs_date DESC);
CREATE INDEX idx_obs_backfill        ON observations (is_backfill) WHERE is_backfill = TRUE;

-- cli_raw — fetcher needs to find latest version quickly
CREATE INDEX idx_cli_city_date       ON cli_raw (city_id, obs_date DESC);
CREATE INDEX idx_cli_not_final       ON cli_raw (city_id, obs_date) WHERE is_final = FALSE;

-- forecasts — core ML query: all forecasts for a city at a given lead
CREATE INDEX idx_fcast_city_target   ON forecasts (city_id, target_date DESC);
CREATE INDEX idx_fcast_city_lead     ON forecasts (city_id, lead_days, target_date DESC);
CREATE INDEX idx_fcast_source_city   ON forecasts (source_id, city_id, target_date DESC);
CREATE INDEX idx_fcast_unverified    ON forecasts (target_date)
    WHERE id NOT IN (SELECT forecast_id FROM forecast_verifications);

-- forecast_verifications — edge analysis queries
CREATE INDEX idx_fv_obs              ON forecast_verifications (observation_id);
CREATE INDEX idx_fv_verified_at      ON forecast_verifications (verified_at DESC);

-- pipeline_runs
CREATE INDEX idx_runs_type_status    ON pipeline_runs (run_type, status, started_at DESC);


-- -----------------------------------------------------------------------------
-- 10. CONSTRAINTS
-- -----------------------------------------------------------------------------

-- lead_days must equal the actual difference between fetch date and target date
-- (enforced in application layer — too complex for a pure SQL check constraint
--  due to timezone handling, but validated in ingest.py before INSERT)

-- temp values must be plausible for US cities (sanity check, not settlement logic)
ALTER TABLE observations ADD CONSTRAINT chk_obs_temp_range
    CHECK (
        (temp_high IS NULL OR temp_high BETWEEN -60 AND 140) AND
        (temp_low  IS NULL OR temp_low  BETWEEN -60 AND 130) AND
        (temp_high IS NULL OR temp_low  IS NULL OR temp_high >= temp_low)
    );

ALTER TABLE forecasts ADD CONSTRAINT chk_fcast_temp_range
    CHECK (
        (temp_high IS NULL OR temp_high BETWEEN -60 AND 140) AND
        (temp_low  IS NULL OR temp_low  BETWEEN -60 AND 130) AND
        (temp_high IS NULL OR temp_low  IS NULL OR temp_high >= temp_low)
    );

ALTER TABLE observations ADD CONSTRAINT chk_precip_trace
    CHECK (precip >= 0 OR precip IS NULL);

ALTER TABLE forecasts ADD CONSTRAINT chk_precip_prob
    CHECK (precip_prob IS NULL OR precip_prob BETWEEN 0 AND 100);


-- -----------------------------------------------------------------------------
-- 11. SEED DATA — CITIES
-- -----------------------------------------------------------------------------

INSERT INTO cities (name, state, timezone, latitude, longitude,
                    nws_site, nws_issuedby, asos_station, ghcnd_station,
                    kalshi_ticker, observes_dst)
VALUES
    ('New Orleans',    'LA', 'America/Chicago',       29.9934, -90.2580, 'LIX', 'MSY', 'KMSY', 'USW00012916', 'MSY',  TRUE),
    ('Washington DC',  'DC', 'America/New_York',      38.8521, -77.0378, 'LWX', 'DCA', 'KDCA', 'USW00013743', 'DCA',  TRUE),
    ('Miami',          'FL', 'America/New_York',      25.7959, -80.2870, 'MFL', 'MIA', 'KMIA', 'USW00012839', 'MIA',  TRUE),
    ('Oklahoma City',  'OK', 'America/Chicago',       35.3931, -97.6007, 'OUN', 'OKC', 'KOKC', 'USW00013967', 'OKC',  TRUE),
    ('San Antonio',    'TX', 'America/Chicago',       29.5338, -98.4698, 'EWX', 'SAT', 'KSAT', 'USW00012921', 'SAT',  TRUE),
    ('Dallas-Fort Worth', 'TX', 'America/Chicago',   32.8998, -97.0403, 'FWD', 'DFW', 'KDFW', 'USW00003927', 'DFW',  TRUE),
    ('Los Angeles',    'CA', 'America/Los_Angeles',   33.9425, -118.4081,'LOX', 'LAX', 'KLAX', 'USW00023174', 'LAX',  TRUE),
    ('Seattle',        'WA', 'America/Los_Angeles',   47.4489, -122.3094,'SEW', 'SEA', 'KSEA', 'USW00024233', 'SEA',  TRUE),
    ('Houston',        'TX', 'America/Chicago',       29.6454, -95.2789, 'HGX', 'HOU', 'KHOU', 'USW00012918', 'HOU',  TRUE),
    ('Philadelphia',   'PA', 'America/New_York',      39.8719, -75.2411, 'PHI', 'PHL', 'KPHL', 'USW00013739', 'PHL',  TRUE),
    ('Phoenix',        'AZ', 'America/Phoenix',       33.4373, -112.0078,'PSR', 'PHX', 'KPHX', 'USW00023183', 'PHX',  FALSE),
    ('Boston',         'MA', 'America/New_York',      42.3606, -71.0097, 'BOX', 'BOS', 'KBOS', 'USW00014739', 'BOS',  TRUE),
    ('Las Vegas',      'NV', 'America/Los_Angeles',   36.0840, -115.1522,'VEF', 'LAS', 'KLAS', 'USW00023169', 'LAS',  TRUE),
    ('Denver',         'CO', 'America/Denver',        39.8561, -104.6737,'BOU', 'DEN', 'KDEN', 'USW00003017', 'DEN',  TRUE),
    ('Chicago Midway', 'IL', 'America/Chicago',       41.7868, -87.7522, 'LOT', 'MDW', 'KMDW', 'USW00014819', 'MDW',  TRUE),
    ('Atlanta',        'GA', 'America/New_York',      33.6367, -84.4281, 'FFC', 'ATL', 'KATL', 'USW00013874', 'ATL',  TRUE),
    ('New York City',  'NY', 'America/New_York',      40.7789, -73.9692, 'OKX', 'NYC', 'KNYC', 'USW00094728', 'NYC',  TRUE),
    ('Minneapolis',    'MN', 'America/Chicago',       44.8831, -93.2289, 'MPX', 'MSP', 'KMSP', 'USW00014922', 'MSP',  TRUE),
    ('Austin',         'TX', 'America/Chicago',       30.1945, -97.6699, 'EWX', 'AUS', 'KAUS', 'USW00013904', 'AUS',  TRUE),
    ('San Francisco',  'CA', 'America/Los_Angeles',   37.6197, -122.3647,'MTR', 'SFO', 'KSFO', 'USW00023234', 'SFO',  TRUE);


-- -----------------------------------------------------------------------------
-- 12. SEED DATA — FORECAST SOURCES
-- -----------------------------------------------------------------------------
-- Start with three sources. Add more by inserting here + writing a fetcher.

INSERT INTO forecast_sources (name, display_name, api_base_url, model_name, notes)
VALUES
    (
        'open_meteo_gfs',
        'Open-Meteo (GFS)',
        'https://api.open-meteo.com/v1/forecast',
        'GFS',
        'NOAA GFS model via Open-Meteo. Free, no API key. 16-day forecast. Primary source.'
    ),
    (
        'open_meteo_ecmwf',
        'Open-Meteo (ECMWF IFS)',
        'https://api.open-meteo.com/v1/forecast',
        'ECMWF_IFS025',
        'European Centre model via Open-Meteo. Free tier. Often more accurate at 5-10 day range.'
    ),
    (
        'nws_api',
        'NWS API (Official)',
        'https://api.weather.gov/points',
        'NAM/GFS blend',
        'Official NWS gridded forecast. This is what most people and Kalshi market makers reference. '
        'Baseline to beat.'
    );


-- -----------------------------------------------------------------------------
-- END OF SCHEMA
-- -----------------------------------------------------------------------------