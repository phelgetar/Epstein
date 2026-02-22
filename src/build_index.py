#!/usr/bin/env python3
"""
Build a SQLite FTS5 search index from the extracted JSON files.

Reads epstein_pdfs_search_index.json (or epstein_pdfs_full.json) and creates
a SQLite database with full-text search via FTS5. This replaces the in-memory
JSON search (~7 GB RAM) with an on-disk index (~300 MB) that supports instant
ranked queries.

Usage:
    python -m src.build_index            # Build index
    python -m src.build_index --force    # Rebuild from scratch
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import DATA_DIR, JSON_SEARCH_INDEX, JSON_FULL, SEARCH_DB
from src.logging_setup import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

BATCH_SIZE = 10000  # Documents per INSERT batch


def find_json_file():
    """Find the search index JSON file."""
    candidates = [
        DATA_DIR / JSON_SEARCH_INDEX,
        DATA_DIR / JSON_FULL,
        Path(JSON_SEARCH_INDEX),
        Path(JSON_FULL),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def build_index(db_path: Path, json_path: Path):
    """Build the SQLite FTS5 index from JSON data."""
    start = time.perf_counter()

    print("=" * 70)
    print("Epstein DOJ Files — FTS5 Index Builder")
    print("=" * 70)
    print(f"\n  Source:  {json_path}")
    print(f"  Output:  {db_path}")

    logger.info("build_index_started", extra={"data": {
        "source": str(json_path), "output": str(db_path),
    }})

    # Load JSON
    print("\n  Loading JSON...", end=" ", flush=True)
    t0 = time.perf_counter()
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"{len(data):,} documents ({time.perf_counter() - t0:.1f}s)")

    # Create database
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")  # 64 MB cache

    # Create tables
    conn.executescript("""
        DROP TABLE IF EXISTS documents_fts;
        DROP TABLE IF EXISTS documents;

        CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            dataset INTEGER NOT NULL,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            pages INTEGER NOT NULL,
            page_offsets TEXT
        );

        CREATE VIRTUAL TABLE documents_fts USING fts5(
            text,
            tokenize='porter unicode61'
        );
    """)

    # Insert documents in batches
    print("  Indexing...", flush=True)
    doc_rows = []
    fts_rows = []
    total = len(data)
    errors = 0

    for i, doc in enumerate(data):
        doc_id = i + 1
        try:
            doc_rows.append((
                doc_id,
                doc["dataset"],
                doc["filename"],
                doc["filepath"],
                doc["pages"],
                json.dumps(doc.get("page_offsets")) if doc.get("page_offsets") else None,
            ))
            fts_rows.append((doc_id, doc.get("text", "")))
        except Exception:
            errors += 1
            logger.error("build_index_doc_error", extra={"data": {
                "index": i, "filename": doc.get("filename", "?"),
            }}, exc_info=True)
            continue

        if len(doc_rows) >= BATCH_SIZE:
            conn.executemany(
                "INSERT INTO documents (id, dataset, filename, filepath, pages, page_offsets) "
                "VALUES (?, ?, ?, ?, ?, ?)", doc_rows
            )
            conn.executemany(
                "INSERT INTO documents_fts (rowid, text) VALUES (?, ?)", fts_rows
            )
            conn.commit()
            pct = (i + 1) / total * 100
            print(f"    {i + 1:>10,} / {total:,} ({pct:.1f}%)", flush=True)
            doc_rows.clear()
            fts_rows.clear()

    # Final batch
    if doc_rows:
        conn.executemany(
            "INSERT INTO documents (id, dataset, filename, filepath, pages, page_offsets) "
            "VALUES (?, ?, ?, ?, ?, ?)", doc_rows
        )
        conn.executemany(
            "INSERT INTO documents_fts (rowid, text) VALUES (?, ?)", fts_rows
        )
        conn.commit()

    # Create indexes for common filters
    conn.execute("CREATE INDEX idx_documents_dataset ON documents(dataset)")
    conn.execute("CREATE INDEX idx_documents_filename ON documents(filename)")
    conn.commit()

    # Optimize FTS index
    print("  Optimizing FTS index...", end=" ", flush=True)
    t0 = time.perf_counter()
    conn.execute("INSERT INTO documents_fts(documents_fts) VALUES('optimize')")
    conn.commit()
    print(f"({time.perf_counter() - t0:.1f}s)")

    conn.close()

    # Summary
    elapsed = time.perf_counter() - start
    db_size_mb = db_path.stat().st_size / (1024 * 1024)
    indexed = total - errors

    print(f"\n  Documents: {indexed:,} indexed" +
          (f" ({errors} errors)" if errors else ""))
    print(f"  Database:  {db_size_mb:,.1f} MB")
    print(f"  Time:      {elapsed:.1f}s")
    print("\n" + "=" * 70)

    logger.info("build_index_complete", extra={"data": {
        "documents": indexed, "errors": errors,
        "db_size_mb": round(db_size_mb, 1),
        "elapsed_s": round(elapsed, 1),
    }})


def main():
    parser = argparse.ArgumentParser(description="Build SQLite FTS5 search index")
    parser.add_argument("--force", action="store_true",
                        help="Rebuild index even if it already exists")
    args = parser.parse_args()

    json_path = find_json_file()
    if not json_path:
        print("Error: No JSON search index found.")
        print("Run the extractor first: python -m src.extractor")
        sys.exit(1)

    if SEARCH_DB.exists() and not args.force:
        db_size = SEARCH_DB.stat().st_size / (1024 * 1024)
        print(f"Index already exists: {SEARCH_DB} ({db_size:,.1f} MB)")
        print("Use --force to rebuild.")
        sys.exit(0)

    if SEARCH_DB.exists():
        os.remove(SEARCH_DB)

    build_index(SEARCH_DB, json_path)


if __name__ == "__main__":
    main()
