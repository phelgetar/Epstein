#!/usr/bin/env python3
"""
Epstein DOJ Files - CLI Search Utility
Search through the extracted PDF text via command line or interactive mode.

Supports: AND, OR, NOT operators, "quoted phrases", NEAR/N proximity search.
"""

import argparse
import bisect
import csv
import json
import logging
import re
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import DATA_DIR, JSON_SEARCH_INDEX, JSON_FULL, SEARCH_DB

logger = logging.getLogger(__name__)


def _position_to_page(position, page_offsets):
    """Convert a character position to a 1-based page number."""
    page_index = bisect.bisect_right(page_offsets, position) - 1
    return max(1, page_index + 1)


class PDFSearcher:
    def __init__(self, json_file=None):
        """Initialize searcher with JSON data."""
        if json_file is None:
            # Try data/ directory first, then project root
            candidates = [
                DATA_DIR / JSON_SEARCH_INDEX,
                DATA_DIR / JSON_FULL,
                Path(JSON_SEARCH_INDEX),
                Path(JSON_FULL),
            ]
            for candidate in candidates:
                if candidate.exists():
                    json_file = str(candidate)
                    break
            else:
                logger.error("search_index_not_found", extra={"data": {
                    "candidates": [str(c) for c in candidates],
                }})
                print("Error: No JSON search files found")
                print("Run the extractor first: python -m src.extractor")
                sys.exit(1)

        with open(json_file, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        logger.info("search_index_loaded", extra={"data": {
            "file": json_file, "document_count": len(self.data),
        }})

    def search(self, query, case_sensitive=False, whole_word=False, context_chars=300):
        """Search for a term in all documents."""
        results = []
        flags = 0 if case_sensitive else re.IGNORECASE

        if whole_word:
            pattern = r"\b" + re.escape(query) + r"\b"
        else:
            pattern = re.escape(query)

        regex = re.compile(pattern, flags)

        for doc in self.data:
            text = doc["text"]
            page_offsets = doc.get("page_offsets")
            matches = list(regex.finditer(text))

            if matches:
                contexts = []
                for match in matches:
                    start = max(0, match.start() - context_chars)
                    end = min(len(text), match.end() + context_chars)
                    context = text[start:end]
                    if start > 0:
                        context = "..." + context
                    if end < len(text):
                        context = context + "..."
                    ctx = {
                        "position": match.start(),
                        "context": context,
                        "match": match.group(),
                    }
                    if page_offsets:
                        ctx["page"] = _position_to_page(match.start(), page_offsets)
                    contexts.append(ctx)

                results.append({
                    "dataset": doc["dataset"],
                    "filename": doc["filename"],
                    "filepath": doc["filepath"],
                    "pages": doc["pages"],
                    "match_count": len(matches),
                    "contexts": contexts,
                })

        logger.info("search_executed", extra={"data": {
            "query": query, "result_count": len(results),
            "match_count": sum(r["match_count"] for r in results),
            "case_sensitive": case_sensitive, "whole_word": whole_word,
        }})
        return results

    def search_multiple(self, queries, operator="AND"):
        """Search for multiple terms with AND/OR logic."""
        if operator.upper() == "AND":
            results = self.search(queries[0])
            result_files = {r["filename"] for r in results}

            for query in queries[1:]:
                query_results = self.search(query)
                query_files = {r["filename"] for r in query_results}
                result_files &= query_files

            return [r for r in results if r["filename"] in result_files]
        else:
            all_results = {}
            for query in queries:
                for result in self.search(query):
                    filename = result["filename"]
                    if filename not in all_results:
                        all_results[filename] = result
                    else:
                        all_results[filename]["match_count"] += result["match_count"]
                        all_results[filename]["contexts"].extend(result["contexts"])
            return list(all_results.values())

    def search_proximity(self, term1, term2, max_distance, case_sensitive=False, context_chars=300):
        """Find documents where term1 and term2 appear within max_distance words."""
        flags = 0 if case_sensitive else re.IGNORECASE
        pattern1 = re.compile(re.escape(term1), flags)
        pattern2 = re.compile(re.escape(term2), flags)

        results = []
        for doc in self.data:
            text = doc["text"]
            positions1 = [(m.start(), m.end()) for m in pattern1.finditer(text)]
            positions2 = [(m.start(), m.end()) for m in pattern2.finditer(text)]

            if not positions1 or not positions2:
                continue

            found_pairs = []
            for s1, e1 in positions1:
                for s2, e2 in positions2:
                    start = min(e1, e2)
                    end = max(s1, s2)
                    if start >= end:
                        continue
                    between = text[start:end].strip().split()
                    if len(between) <= max_distance:
                        found_pairs.append((min(s1, s2), max(e1, e2)))
                if len(found_pairs) >= 50:
                    break

            if found_pairs:
                page_offsets = doc.get("page_offsets")
                contexts = []
                for pair_start, pair_end in found_pairs:
                    ctx_start = max(0, pair_start - context_chars)
                    ctx_end = min(len(text), pair_end + context_chars)
                    context = text[ctx_start:ctx_end]
                    if ctx_start > 0:
                        context = "..." + context
                    if ctx_end < len(text):
                        context = context + "..."
                    ctx = {
                        "position": pair_start,
                        "context": context,
                        "match": f"{term1}...{term2}",
                    }
                    if page_offsets:
                        ctx["page"] = _position_to_page(pair_start, page_offsets)
                    contexts.append(ctx)

                results.append({
                    "dataset": doc["dataset"],
                    "filename": doc["filename"],
                    "filepath": doc["filepath"],
                    "pages": doc["pages"],
                    "match_count": len(found_pairs),
                    "contexts": contexts,
                })

        logger.info("search_proximity", extra={"data": {
            "term1": term1, "term2": term2,
            "max_distance": max_distance, "result_count": len(results),
        }})
        return results

    def print_results(self, results, max_contexts=3):
        """Pretty print search results."""
        if not results:
            print("\nNo results found.")
            return

        total_matches = sum(r["match_count"] for r in results)
        print(f"\n{'=' * 80}")
        print(f"Found {len(results)} document(s) with {total_matches} total match(es)")
        print(f"{'=' * 80}\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['filename']} (Data Set {result['dataset']})")
            print(f"   Pages: {result['pages']} | Matches: {result['match_count']}")
            print(f"   Path: {result['filepath']}")

            contexts_to_show = min(max_contexts, len(result["contexts"]))
            for j, ctx in enumerate(result["contexts"][:contexts_to_show], 1):
                page_info = f" (Page {ctx['page']})" if "page" in ctx else ""
                print(f"\n   Match {j}{page_info}:")
                print(f"   {ctx['context']}")

            if len(result["contexts"]) > max_contexts:
                remaining = len(result["contexts"]) - max_contexts
                print(f"\n   ... and {remaining} more match(es)")
            print()


class SQLiteSearcher:
    """FTS5-backed searcher. Uses SQLite instead of loading JSON into memory."""

    def __init__(self, db_path=None):
        if db_path is None:
            db_path = str(SEARCH_DB)
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")

        row = self.conn.execute("SELECT COUNT(*) AS c FROM documents").fetchone()
        self.doc_count = row["c"]
        logger.info("sqlite_index_loaded", extra={"data": {
            "db_path": self.db_path, "document_count": self.doc_count,
        }})

    def _translate_query(self, query):
        """Translate our query syntax to FTS5 syntax.

        Our syntax:  Maxwell AND island
        FTS5 syntax: maxwell island          (implicit AND)

        Our syntax:  Maxwell OR Epstein
        FTS5 syntax: maxwell OR epstein

        Our syntax:  Maxwell NOT flight
        FTS5 syntax: maxwell NOT flight

        Our syntax:  "grand jury"
        FTS5 syntax: "grand jury"

        Our syntax:  Epstein NEAR/5 island
        FTS5 syntax: NEAR(epstein island, 5)
        """
        # Step 1: Extract quoted phrases
        phrases = []

        def replace_phrase(m):
            phrases.append(m.group(0))  # keep the quotes
            return f"__PH{len(phrases) - 1}__"

        working = re.sub(r'"[^"]+"', replace_phrase, query)

        def restore(s):
            return re.sub(r"__PH(\d+)__", lambda m: phrases[int(m.group(1))], s)

        # Step 2: Handle NEAR/N → NEAR(term1 term2, N)
        def near_replace(m):
            t1 = restore(m.group(1))
            t2 = restore(m.group(3))
            n = m.group(2)
            return f"NEAR({t1} {t2}, {n})"

        working = re.sub(r"(\S+)\s+NEAR/(\d+)\s+(\S+)", near_replace, working, flags=re.IGNORECASE)

        # Step 3: Remove AND (FTS5 uses implicit AND)
        working = re.sub(r"\s+AND\s+", " ", working, flags=re.IGNORECASE)

        # Restore phrases and return
        return restore(working).strip()

    def _execute_fts(self, fts_query, query, dataset=None, limit=None, offset=0):
        """Execute an FTS5 query and return results.

        Uses a 2-phase approach for performance:
        1. Match + metadata (fast, no snippet)
        2. Snippet only for the paginated slice
        """
        # Build WHERE clause
        where = "documents_fts MATCH ?"
        params = [fts_query]
        if dataset is not None:
            where += " AND d.dataset = ?"
            params.append(dataset)

        # Phase 1: Count total matches (very fast)
        count_row = self.conn.execute(f"""
            SELECT COUNT(*) AS c
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE {where}
        """, params).fetchone()
        total = count_row["c"]

        if total == 0:
            return [], 0

        # Phase 2: Get paginated results with snippets
        sql = f"""
            SELECT d.id, d.dataset, d.filename, d.filepath, d.pages, d.page_offsets,
                   snippet(documents_fts, 0, '>>>>', '<<<<', '...', 64) AS snippet,
                   bm25(documents_fts) AS rank
            FROM documents_fts
            JOIN documents d ON d.id = documents_fts.rowid
            WHERE {where}
            ORDER BY rank
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)} OFFSET {int(offset)}"
        params_copy = list(params)

        rows = self.conn.execute(sql, params_copy).fetchall()

        results = []
        for row in rows:
            snippet_text = row["snippet"]
            match_count = snippet_text.count(">>>>")
            context = snippet_text.replace(">>>>", "").replace("<<<<", "")

            page_offsets = json.loads(row["page_offsets"]) if row["page_offsets"] else None

            contexts = [{
                "position": 0,
                "context": context,
                "match": query,
            }]
            if page_offsets and len(page_offsets) > 1:
                contexts[0]["page"] = 1

            results.append({
                "dataset": row["dataset"],
                "filename": row["filename"],
                "filepath": row["filepath"],
                "pages": row["pages"],
                "match_count": max(1, match_count),
                "contexts": contexts,
            })

        return results, total

    def search(self, query, case_sensitive=False, whole_word=False, context_chars=300,
               dataset=None, limit=None, offset=0):
        """Search using FTS5 MATCH."""
        fts_query = self._translate_query(query)
        if not fts_query:
            return [], 0

        try:
            return self._execute_fts(fts_query, query, dataset, limit, offset)
        except sqlite3.OperationalError as e:
            logger.warning("sqlite_search_error", extra={"data": {
                "query": query, "fts_query": fts_query, "error": str(e),
            }})
            # Fall back to simple term search if FTS5 syntax fails
            simple = re.sub(r'[^\w\s"]', '', query).strip()
            if simple and simple != fts_query:
                try:
                    return self._execute_fts(simple, query, dataset, limit, offset)
                except sqlite3.OperationalError:
                    return [], 0
            return [], 0

    def search_multiple(self, queries, operator="AND", **kwargs):
        """Search for multiple terms with AND/OR logic via FTS5."""
        if operator.upper() == "AND":
            combined = " ".join(queries)
        else:
            combined = " OR ".join(queries)
        return self.search(combined, **kwargs)

    def search_proximity(self, term1, term2, max_distance, case_sensitive=False,
                         context_chars=300, **kwargs):
        """Proximity search using FTS5 NEAR()."""
        query = f"{term1} NEAR/{max_distance} {term2}"
        return self.search(query, **kwargs)

    def print_results(self, results, max_contexts=3):
        """Pretty print search results (same as PDFSearcher)."""
        if not results:
            print("\nNo results found.")
            return

        total_matches = sum(r["match_count"] for r in results)
        print(f"\n{'=' * 80}")
        print(f"Found {len(results)} document(s) with {total_matches} total match(es)")
        print(f"{'=' * 80}\n")

        for i, result in enumerate(results, 1):
            print(f"{i}. {result['filename']} (Data Set {result['dataset']})")
            print(f"   Pages: {result['pages']} | Matches: {result['match_count']}")
            print(f"   Path: {result['filepath']}")

            contexts_to_show = min(max_contexts, len(result["contexts"]))
            for j, ctx in enumerate(result["contexts"][:contexts_to_show], 1):
                page_info = f" (Page {ctx['page']})" if "page" in ctx else ""
                print(f"\n   Match {j}{page_info}:")
                print(f"   {ctx['context']}")

            if len(result["contexts"]) > max_contexts:
                remaining = len(result["contexts"]) - max_contexts
                print(f"\n   ... and {remaining} more match(es)")
            print()


def _parse_and_search(searcher, query):
    """Parse a query string for operators and execute the search.

    Supports: "quoted phrases", NEAR/N proximity, NOT, AND, OR.
    Works with both PDFSearcher and SQLiteSearcher.
    """
    # Step 1: Extract quoted phrases into placeholders
    phrases = []

    def replace_phrase(m):
        phrases.append(m.group(1))
        return f"__PH{len(phrases) - 1}__"

    working = re.sub(r'"([^"]+)"', replace_phrase, query)

    def restore(s):
        return re.sub(r"__PH(\d+)__", lambda m: phrases[int(m.group(1))], s)

    # Step 2: Extract NEAR/N pairs
    proximity_pairs = []

    def replace_near(m):
        proximity_pairs.append({
            "term1": restore(m.group(1)),
            "term2": restore(m.group(3)),
            "distance": int(m.group(2)),
        })
        return ""

    working = re.sub(r"(\S+)\s+NEAR/(\d+)\s+(\S+)", replace_near, working, flags=re.IGNORECASE)
    working = working.strip()

    # Step 3: Handle NOT
    not_terms = []
    if " NOT " in working.upper():
        parts = re.split(r"\s+NOT\s+", working, flags=re.IGNORECASE)
        working = parts[0].strip()
        not_terms = [restore(t.strip()) for t in parts[1:] if t.strip()]

    # Step 4: Handle AND/OR
    working = working.strip()
    if re.search(r"\s+AND\s+", working, re.IGNORECASE):
        terms = [restore(t.strip()) for t in re.split(r"\s+AND\s+", working, flags=re.IGNORECASE) if t.strip()]
        results = searcher.search_multiple(terms, "AND")
    elif re.search(r"\s+OR\s+", working, re.IGNORECASE):
        terms = [restore(t.strip()) for t in re.split(r"\s+OR\s+", working, flags=re.IGNORECASE) if t.strip()]
        results = searcher.search_multiple(terms, "OR")
    else:
        t = restore(working)
        results = searcher.search(t) if t else []

    # Step 5: Apply NOT exclusions
    if not_terms:
        exclude = set()
        for nt in not_terms:
            for r in searcher.search(nt):
                exclude.add(r["filename"])
        results = [r for r in results if r["filename"] not in exclude]

    # Step 6: Apply proximity searches and merge
    if proximity_pairs:
        result_map = {r["filename"]: r for r in results}
        for pp in proximity_pairs:
            prox_results = searcher.search_proximity(pp["term1"], pp["term2"], pp["distance"])
            for r in prox_results:
                if r["filename"] in result_map:
                    result_map[r["filename"]]["match_count"] += r["match_count"]
                    result_map[r["filename"]]["contexts"].extend(r["contexts"])
                else:
                    result_map[r["filename"]] = r
        results = list(result_map.values())

    return results


def _export_csv(results, output=None):
    """Export results as CSV."""
    out = output or sys.stdout
    writer = csv.writer(out)
    writer.writerow(["filename", "dataset", "pages", "match_count"])
    for r in results:
        writer.writerow([r["filename"], r["dataset"], r["pages"], r["match_count"]])
    logger.info("search_export", extra={"data": {
        "format": "csv", "result_count": len(results),
    }})


def _export_json(results, output=None):
    """Export results as JSON."""
    out = output or sys.stdout
    export_data = []
    for r in results:
        export_data.append({
            "filename": r["filename"],
            "dataset": r["dataset"],
            "pages": r["pages"],
            "match_count": r["match_count"],
            "filepath": r["filepath"],
        })
    out.write(json.dumps(export_data, indent=2) + "\n")
    logger.info("search_export", extra={"data": {
        "format": "json", "result_count": len(results),
    }})


def interactive_search():
    """Interactive search mode."""
    print("=" * 80)
    print("Epstein DOJ Files — Interactive Search")
    print("=" * 80)
    print('  Operators: AND, OR, NOT, NEAR/N, "quoted phrases"')
    print("  Type 'quit' to exit")
    print()

    logger.info("search_started", extra={"data": {
        "mode": "interactive",
    }})

    searcher = PDFSearcher()

    while True:
        try:
            query = input("Search> ").strip()
            if not query:
                continue
            if query.lower() in ("quit", "exit", "q"):
                print("Goodbye!")
                break
            results = _parse_and_search(searcher, query)
            searcher.print_results(results)
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            logger.error("search_error", extra={"data": {"query": query}}, exc_info=True)
            print(f"Error: {e}")


def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files - CLI Search",
        epilog='Examples:\n'
               '  python -m src.search "Maxwell"\n'
               '  python -m src.search "Maxwell AND island"\n'
               '  python -m src.search "Maxwell NOT flight"\n'
               '  python -m src.search \'"grand jury"\'\n'
               '  python -m src.search "Epstein NEAR/5 island"\n'
               '  python -m src.search "Maxwell" --dataset 1 --sort relevance\n'
               '  python -m src.search "Epstein" --export csv\n',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("query", nargs="*", help="Search query (omit for interactive mode)")
    parser.add_argument("--dataset", type=int, help="Filter by dataset number (1-12)")
    parser.add_argument("--min-pages", type=int, default=0, help="Minimum page count")
    parser.add_argument("--max-pages", type=int, default=None, help="Maximum page count")
    parser.add_argument("--sort", choices=["relevance", "filename", "dataset"], default="relevance",
                        help="Sort results (default: relevance)")
    parser.add_argument("--export", choices=["csv", "json"], help="Export results instead of printing")
    args = parser.parse_args()

    if args.query:
        query = " ".join(args.query)

        logger.info("search_started", extra={"data": {
            "query": query, "mode": "cli",
            "dataset_filter": args.dataset,
            "sort": args.sort, "export": args.export,
            "min_pages": args.min_pages, "max_pages": args.max_pages,
        }})

        searcher = PDFSearcher()
        results = _parse_and_search(searcher, query)

        # Apply filters
        if args.dataset:
            results = [r for r in results if r["dataset"] == args.dataset]
        results = [r for r in results if r["pages"] >= args.min_pages]
        if args.max_pages:
            results = [r for r in results if r["pages"] <= args.max_pages]

        # Sort
        if args.sort == "relevance":
            results.sort(key=lambda r: r["match_count"], reverse=True)
        elif args.sort == "filename":
            results.sort(key=lambda r: r["filename"])
        elif args.sort == "dataset":
            results.sort(key=lambda r: (r["dataset"], r["filename"]))

        total_matches = sum(r["match_count"] for r in results)
        logger.info("search_complete", extra={"data": {
            "query": query, "documents_found": len(results),
            "total_matches": total_matches,
            "dataset_filter": args.dataset,
            "export": args.export,
        }})

        # Output
        if args.export == "csv":
            _export_csv(results)
        elif args.export == "json":
            _export_json(results)
        else:
            searcher.print_results(results)
    else:
        interactive_search()


if __name__ == "__main__":
    main()
