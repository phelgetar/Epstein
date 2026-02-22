#!/usr/bin/env python3
"""
Initialize the MySQL database schema for Epstein DOJ Files analytics.

Creates tables for search query logging, page view tracking, and file inventory.

Usage:
    python -m src.init_db --host HOST --user USER --password PASS --database DB
    python -m src.init_db --populate-files   # Scan epstein_doj_files/ and insert inventory
    python -m src.init_db --drop             # Drop and recreate all tables
"""

import argparse
import os
import sys
from pathlib import Path

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import PROJECT_ROOT, PDF_DIR, DATABASE_URL

# ─── SQL Schemas ─────────────────────────────────────────────

CREATE_SEARCH_QUERIES = """
CREATE TABLE IF NOT EXISTS search_queries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    query VARCHAR(500) NOT NULL,
    total_results INT,
    total_matches INT,
    dataset_filter INT,
    sort VARCHAR(20),
    page INT,
    per_page INT,
    duration_ms FLOAT,
    client_ip VARCHAR(45),
    user_agent VARCHAR(500),
    INDEX idx_timestamp (timestamp),
    INDEX idx_query (query(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_PAGE_VIEWS = """
CREATE TABLE IF NOT EXISTS page_views (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    method VARCHAR(10),
    path VARCHAR(500),
    query_string VARCHAR(1000),
    status_code INT,
    duration_ms FLOAT,
    client_ip VARCHAR(45),
    user_agent VARCHAR(500),
    INDEX idx_timestamp (timestamp),
    INDEX idx_path (path(100))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_FILES = """
CREATE TABLE IF NOT EXISTS files (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    dataset INT,
    filename VARCHAR(255) NOT NULL,
    filepath VARCHAR(500) NOT NULL,
    file_type VARCHAR(10),
    file_size BIGINT,
    source VARCHAR(50),
    subfolder VARCHAR(100),
    pages INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE INDEX idx_filepath (filepath(255)),
    INDEX idx_dataset (dataset),
    INDEX idx_file_type (file_type),
    INDEX idx_source (source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DROP_TABLES = [
    "DROP TABLE IF EXISTS search_queries;",
    "DROP TABLE IF EXISTS page_views;",
    "DROP TABLE IF EXISTS files;",
]

ALL_TABLES = [CREATE_SEARCH_QUERIES, CREATE_PAGE_VIEWS, CREATE_FILES]


def parse_database_url(url: str) -> dict:
    """Parse mysql+pymysql://user:pass@host/db into connection kwargs."""
    # mysql+pymysql://user:pass@host/db or mysql://user:pass@host/db
    url = url.replace("mysql+pymysql://", "").replace("mysql://", "")
    userpass, hostdb = url.split("@", 1)
    user, password = userpass.split(":", 1)
    host, database = hostdb.split("/", 1)
    port = 3306
    if ":" in host:
        host, port_str = host.split(":", 1)
        port = int(port_str)
    return dict(host=host, user=user, password=password, database=database, port=port)


def get_connection(args) -> pymysql.Connection:
    """Create a MySQL connection from CLI args or DATABASE_URL."""
    if args.host:
        return pymysql.connect(
            host=args.host,
            user=args.user,
            password=args.password,
            database=args.database,
            port=args.port,
            charset="utf8mb4",
            autocommit=True,
        )
    elif DATABASE_URL:
        kwargs = parse_database_url(DATABASE_URL)
        kwargs["charset"] = "utf8mb4"
        kwargs["autocommit"] = True
        return pymysql.connect(**kwargs)
    else:
        print("Error: Provide --host/--user/--password/--database or set DATABASE_URL")
        sys.exit(1)


def create_tables(conn, drop=False):
    """Create (or drop+recreate) all tables."""
    with conn.cursor() as cur:
        if drop:
            print("  Dropping existing tables...")
            for sql in DROP_TABLES:
                cur.execute(sql)
                print(f"    {sql.strip()}")

        print("  Creating tables...")
        for sql in ALL_TABLES:
            cur.execute(sql)
            # Extract table name from SQL
            table_name = sql.split("EXISTS")[1].split("(")[0].strip()
            print(f"    Created: {table_name}")

    print("  Done.\n")


def populate_files(conn):
    """Scan epstein_doj_files/ and insert file inventory into the files table."""
    if not PDF_DIR.exists():
        print(f"  Error: {PDF_DIR} does not exist.")
        return

    print(f"  Scanning {PDF_DIR}...")

    # Known file extensions to track
    tracked_extensions = {".pdf", ".jpg", ".jpeg", ".tif", ".tiff", ".png", ".mp4", ".wav"}

    files_found = []
    for filepath in sorted(PDF_DIR.rglob("*")):
        if not filepath.is_file():
            continue
        ext = filepath.suffix.lower()
        if ext not in tracked_extensions:
            continue

        rel = filepath.relative_to(PROJECT_ROOT)
        parts = filepath.relative_to(PDF_DIR).parts

        # Determine dataset and source
        dataset = None
        source = None
        subfolder = None

        if parts[0] == "Google_Drive_Files":
            source = "gdrive"
            if len(parts) > 1:
                subfolder = parts[1]
        elif parts[0] == "videos":
            source = "doj"
            subfolder = "videos"
        elif parts[0].startswith("data-set-"):
            try:
                dataset = int(parts[0].replace("data-set-", ""))
            except ValueError:
                pass
            source = "doj"
        else:
            source = "doj"

        # Normalize file_type
        file_type = ext.lstrip(".")
        if file_type == "tiff":
            file_type = "tif"
        if file_type == "jpeg":
            file_type = "jpg"

        files_found.append({
            "dataset": dataset,
            "filename": filepath.name,
            "filepath": str(rel),
            "file_type": file_type,
            "file_size": filepath.stat().st_size,
            "source": source,
            "subfolder": subfolder,
        })

    print(f"  Found {len(files_found):,} files.")

    if not files_found:
        return

    # Batch insert using INSERT ... ON DUPLICATE KEY UPDATE
    print("  Inserting into database...")
    insert_sql = """
        INSERT INTO files (dataset, filename, filepath, file_type, file_size, source, subfolder)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            file_size = VALUES(file_size),
            file_type = VALUES(file_type),
            source = VALUES(source),
            subfolder = VALUES(subfolder)
    """

    batch_size = 1000
    with conn.cursor() as cur:
        for i in range(0, len(files_found), batch_size):
            batch = files_found[i:i + batch_size]
            values = [
                (f["dataset"], f["filename"], f["filepath"],
                 f["file_type"], f["file_size"], f["source"], f["subfolder"])
                for f in batch
            ]
            cur.executemany(insert_sql, values)
            print(f"    Inserted {min(i + batch_size, len(files_found)):,} / {len(files_found):,}")

    print(f"  Done. {len(files_found):,} files in inventory.\n")


def main():
    parser = argparse.ArgumentParser(description="Initialize MySQL database schema")
    parser.add_argument("--host", default="", help="MySQL host")
    parser.add_argument("--user", default="", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument("--database", default="", help="MySQL database name")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--drop", action="store_true", help="Drop and recreate tables")
    parser.add_argument("--populate-files", action="store_true", help="Scan and insert file inventory")
    args = parser.parse_args()

    print("=" * 70)
    print("Epstein DOJ Files — MySQL Database Init")
    print("=" * 70)

    conn = get_connection(args)
    print(f"  Connected to {conn.host_info}\n")

    create_tables(conn, drop=args.drop)

    if args.populate_files:
        populate_files(conn)

    conn.close()
    print("  Connection closed.")


if __name__ == "__main__":
    main()
