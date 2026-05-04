#!/bin/bash
set -e

echo "[superset-init] Installing Python dependencies..."
pip install trino sqlalchemy-trino psycopg2-binary --quiet --no-cache-dir

echo "[superset-init] Creating superset database in PostgreSQL if not exists..."
python << 'PYEOF'
import psycopg2, sys
try:
    conn = psycopg2.connect(
        host='postgres', port=5432,
        user='postgres', password='postgres', dbname='postgres'
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname='superset'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE superset")
        print("[superset-init] Created 'superset' database.")
    else:
        print("[superset-init] 'superset' database already exists.")
    conn.close()
except Exception as e:
    print(f"[superset-init] ERROR creating database: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF

echo "[superset-init] Running superset db upgrade..."
superset db upgrade

echo "[superset-init] Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@superset.com \
    --password admin 2>/dev/null || true

echo "[superset-init] Running superset init..."
superset init

echo "[superset-init] Done."
