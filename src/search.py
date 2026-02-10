#!/usr/bin/env python3
"""
Epstein DOJ Files - CLI Search Utility
Search through the extracted PDF text via command line or interactive mode.
"""

import bisect
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import DATA_DIR, JSON_SEARCH_INDEX, JSON_FULL


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
                print("Error: No JSON search files found")
                print("Run the extractor first: python -m src.extractor")
                sys.exit(1)

        print(f"Loading {json_file}...")
        with open(json_file, "r", encoding="utf-8") as f:
            self.data = json.load(f)
        print(f"Loaded {len(self.data)} documents")

    def search(self, query, case_sensitive=False, whole_word=False, context_chars=200):
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

    def print_results(self, results, max_contexts=3):
        """Pretty print search results."""
        if not results:
            print("\nNo results found.")
            return

        print(f"\n{'=' * 80}")
        print(f"Found {len(results)} document(s) with matches")
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
    """Parse a query string for AND/OR operators and execute the search."""
    if " AND " in query.upper():
        terms = [t.strip() for t in re.split(r"\s+AND\s+", query, flags=re.IGNORECASE)]
        return searcher.search_multiple(terms, "AND")
    elif " OR " in query.upper():
        terms = [t.strip() for t in re.split(r"\s+OR\s+", query, flags=re.IGNORECASE)]
        return searcher.search_multiple(terms, "OR")
    else:
        return searcher.search(query)


def interactive_search():
    """Interactive search mode."""
    print("=" * 80)
    print("Epstein DOJ Files - Interactive Search")
    print("=" * 80)
    print()

    searcher = PDFSearcher()

    print("\nSearch Commands:")
    print("  <term>              - Search for a term")
    print("  term1 AND term2     - Search for documents with both terms")
    print("  term1 OR term2      - Search for documents with either term")
    print("  quit or exit        - Exit")
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
            print(f"Error: {e}")


def main():
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
        searcher = PDFSearcher()
        results = _parse_and_search(searcher, query)
        searcher.print_results(results)
    else:
        interactive_search()


if __name__ == "__main__":
    main()
