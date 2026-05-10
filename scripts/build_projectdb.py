"""
Stage 1 — Build PostgreSQL database for the project.

Creates tables, imports CSV data, and verifies the result.
Uses psycopg2 with COPY FROM STDIN for efficient bulk loading.

Usage:
    python scripts/build_projectdb.py
"""

import os
import sys
from pprint import pprint

import psycopg2 as psql


DB_HOST = "hadoop-04.uni.innopolis.ru"
DB_PORT = "5432"
DB_NAME = "team28_projectdb"
DB_USER = "team28"

DATA_DIR = "data"
SQL_DIR = "sql"
SECRETS_DIR = "secrets"

CSV_FILES = ["repositories.csv", "events_clean.csv"]


def read_password():
    """Read the database password from secrets file."""
    path = os.path.join(SECRETS_DIR, ".psql.pass")
    if not os.path.isfile(path):
        print(f"ERROR: Password file not found: {path}", file=sys.stderr)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        return f.read().rstrip()


def get_connection_string(password):
    """Build psycopg2 connection string."""
    return (
        f"host={DB_HOST} port={DB_PORT} "
        f"user={DB_USER} dbname={DB_NAME} "
        f"password={password}"
    )


def create_tables(conn):
    """Execute DDL statements to create tables."""
    print("[1/3] Creating tables...")
    cur = conn.cursor()
    with open(os.path.join(SQL_DIR, "create_tables.sql"), encoding="utf-8") as f:
        cur.execute(f.read())
    conn.commit()
    print("  Tables created.")


def import_data(conn):
    """Load CSV files into PostgreSQL via COPY FROM STDIN."""
    print("[2/3] Importing data...")
    cur = conn.cursor()

    with open(os.path.join(SQL_DIR, "import_data.sql"), encoding="utf-8") as f:
        commands = f.readlines()

    for i, csv_file in enumerate(CSV_FILES):
        csv_path = os.path.join(DATA_DIR, csv_file)
        if not os.path.isfile(csv_path):
            print(f"  ERROR: {csv_path} not found!", file=sys.stderr)
            sys.exit(1)
        size_mb = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"  Loading {csv_file} ({size_mb:.0f} MB)...")
        with open(csv_path, "r", encoding="utf-8") as data_file:
            cur.copy_expert(commands[i], data_file)
        conn.commit()
        print(f"  {csv_file} loaded.")

    print("  All data imported.")


def test_database(conn):
    """Run verification queries and print results."""
    print("[3/3] Verifying database...")
    cur = conn.cursor()

    with open(os.path.join(SQL_DIR, "test_database.sql"), encoding="utf-8") as f:
        commands = f.readlines()

    for command in commands:
        if command.strip() and not command.strip().startswith("--"):
            cur.execute(command)
            pprint(cur.fetchall())


def main():
    """Build the project database end-to-end."""
    print("=" * 60)
    print("Stage 1 — Building PostgreSQL database")
    print("=" * 60)

    password = read_password()
    conn_string = get_connection_string(password)

    with psql.connect(conn_string) as conn:
        create_tables(conn)
        import_data(conn)
        test_database(conn)

    print("\n" + "=" * 60)
    print("PostgreSQL database ready.")
    print("=" * 60)


if __name__ == "__main__":
    main()
