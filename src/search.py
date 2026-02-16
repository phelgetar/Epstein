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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import DATA_DIR, JSON_SEARCH_INDEX, JSON_FULL

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

        print(f"Loading {json_file}...")
        with open(json_file, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        logger.info("search_index_loaded", extra={"data": {
            "file": json_file, "document_count": len(self.data),
        }})
        print(f"Loaded {len(self.data)} documents")

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


def _parse_and_search(searcher, query):
    """Parse a query string for operators and execute the search.

    Supports: "quoted phrases", NEAR/N proximity, NOT, AND, OR.
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
    print("Epstein DOJ Files - Interactive Search")
    print("=" * 80)
    print()

    searcher = PDFSearcher()

    print("\nSearch Commands:")
    print('  <term>                  - Search for a term')
    print('  "exact phrase"          - Search for an exact phrase')
    print("  term1 AND term2         - Documents with both terms")
    print("  term1 OR term2          - Documents with either term")
    print("  term1 NOT term2         - Exclude documents with term2")
    print("  term1 NEAR/5 term2      - Terms within 5 words of each other")
    print("  quit or exit            - Exit")
    print()

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
