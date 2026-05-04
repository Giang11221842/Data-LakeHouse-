# =============================================================================
# Apache Superset — Custom Configuration
# BUG-025: Fix default 60s SQLLAB_TIMEOUT + performance tuning
# PERF-001: Switch from SimpleCache → Redis-backed cache for shared,
#           persistent caching across all gunicorn workers.
#
# This file is mounted into the container at /app/superset_config.py
# and loaded via SUPERSET_CONFIG_PATH=/app/superset_config.py
# =============================================================================

import logging

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Redis connection settings
# Redis is already running in the stack (docker-compose redis service).
# Using separate DB indices per cache role to avoid key collisions and allow
# independent flush/TTL management per cache type.
# -----------------------------------------------------------------------------
REDIS_HOST = "redis"
REDIS_PORT = 6379

# -----------------------------------------------------------------------------
# Secret key (must match docker-compose env SUPERSET_SECRET_KEY)
# -----------------------------------------------------------------------------
SECRET_KEY = "nyc-taxi-superset-secret-2024"

# -----------------------------------------------------------------------------
# Metadata Database — use PostgreSQL instead of default SQLite
# SQLite uses NullPool which rejects pool_size/max_overflow/pool_timeout args.
# PostgreSQL supports full connection pooling → all pool settings below work.
# -----------------------------------------------------------------------------
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://postgres:postgres@postgres:5432/superset"

# -----------------------------------------------------------------------------
# SQL Lab Timeouts — ROOT FIX for BUG-025
# Default SQLLAB_TIMEOUT = 60s causes queries to timeout before Trino finishes.
# Increased to 300s (5 minutes) to accommodate complex report queries on
# multi-year NYC taxi data via Trino → Iceberg → MinIO.
# -----------------------------------------------------------------------------

# Maximum time (seconds) for synchronous SQL Lab queries before timeout
SQLLAB_TIMEOUT = 800  # was: 60 (default) → now: 300

# Maximum time (seconds) for async SQL Lab queries (background execution)
SQLLAB_ASYNC_TIME_LIMIT_SEC = 800  # was: 60 (default) → now: 300

# Webserver request timeout — aligns with SQL Lab timeout
SUPERSET_WEBSERVER_TIMEOUT = 800  # was: not set → now: 300

# -----------------------------------------------------------------------------
# Row limits — prevent runaway result sets from overwhelming the browser
# -----------------------------------------------------------------------------

# Maximum rows returned by a query in SQL Lab
ROW_LIMIT = 50000

# Maximum rows for chart/explore sample queries
SAMPLES_ROW_LIMIT = 1000

# Maximum rows for chart data queries
VIZ_ROW_LIMIT = 10000

# -----------------------------------------------------------------------------
# Query result caching — PERF-001: Redis-backed cache
#
# WHY Redis instead of SimpleCache:
#   SimpleCache is in-process: each gunicorn worker (--workers 2) maintains its
#   own isolated cache. A chart request hitting worker-1 populates worker-1's
#   cache; the next request routed to worker-2 is a cache miss and re-runs the
#   full Trino query. With Redis, all workers share one cache → true cache hits.
#   Redis also persists across container restarts (unlike in-process memory).
#
# TIMEOUT STRATEGY (differentiated per cache role):
#   CACHE_CONFIG          7200s (2h)  — UI metadata, schema info; changes rarely
#   DATA_CACHE_CONFIG     3600s (1h)  — chart query results; pipeline is monthly
#   FILTER_STATE_CACHE   86400s (24h) — dashboard filter selections per user
#   EXPLORE_FORM_DATA    86400s (24h) — explore form state per user session
# -----------------------------------------------------------------------------

# UI metadata cache (schema browser, dataset lists, etc.)
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 7200,          # 2 hours — UI metadata changes rarely
    "CACHE_KEY_PREFIX": "superset_meta_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,                    # DB 1 — metadata/UI cache
}

# Chart data cache (results of Trino queries powering charts)
DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 3600,          # 1 hour — aligns with monthly batch cadence
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,                    # DB 2 — chart data cache
}

# Dashboard filter state cache (persists user filter selections across page reloads)
FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,         # 24 hours — persist user sessions
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 3,                    # DB 3 — filter state cache
}

# Explore form data cache (persists chart builder state across page reloads)
EXPLORE_FORM_DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,         # 24 hours — persist explore sessions
    "CACHE_KEY_PREFIX": "superset_explore_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 4,                    # DB 4 — explore form cache
}

# -----------------------------------------------------------------------------
# CSRF — prevent token expiry during long-running report sessions
# -----------------------------------------------------------------------------
WTF_CSRF_TIME_LIMIT = 60 * 60 * 24  # 24 hours (default is 3600 = 1 hour)

# -----------------------------------------------------------------------------
# Feature flags
# SQLLAB_BACKEND_PERSISTENCE: persist async query state in DB so results
#   survive page refresh / browser close during long Trino queries.
# ENABLE_TEMPLATE_PROCESSING: allow Jinja templating in SQL queries.
# -----------------------------------------------------------------------------
FEATURE_FLAGS = {
    "SQLLAB_BACKEND_PERSISTENCE": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,  # disable — no email/slack configured
    "DRILL_TO_DETAIL": True,
    "DRILL_BY": True,
}

# -----------------------------------------------------------------------------
# Database connection pool settings
# Tuned for Trino connections which can be long-lived during report generation
# -----------------------------------------------------------------------------
SQLALCHEMY_POOL_SIZE = 5
SQLALCHEMY_MAX_OVERFLOW = 10
SQLALCHEMY_POOL_TIMEOUT = 600   # seconds to wait for a connection from pool
SQLALCHEMY_POOL_RECYCLE = 3600  # recycle connections after 1 hour
SQLALCHEMY_ENGINE_OPTIONS = {"pool_pre_ping": True}

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = logging.INFO

logger.info(
    "[superset_config] Loaded: SQLLAB_TIMEOUT=%ds, SQLLAB_ASYNC_TIME_LIMIT_SEC=%ds, "
    "CACHE_BACKEND=RedisCache (host=%s port=%d), "
    "CACHE_CONFIG_TTL=%ds, DATA_CACHE_TTL=%ds, FILTER_STATE_TTL=%ds, EXPLORE_TTL=%ds",
    SQLLAB_TIMEOUT,
    SQLLAB_ASYNC_TIME_LIMIT_SEC,
    REDIS_HOST,
    REDIS_PORT,
    CACHE_CONFIG["CACHE_DEFAULT_TIMEOUT"],
    DATA_CACHE_CONFIG["CACHE_DEFAULT_TIMEOUT"],
    FILTER_STATE_CACHE_CONFIG["CACHE_DEFAULT_TIMEOUT"],
    EXPLORE_FORM_DATA_CACHE_CONFIG["CACHE_DEFAULT_TIMEOUT"],
)
