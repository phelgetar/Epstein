#!/usr/bin/env python3
"""
Epstein DOJ Files Downloader with Pagination Support.

Downloads public PDF documents from justice.gov/epstein/doj-disclosures.
Handles paginated file listings across all 12 datasets.

Usage:
    python -m src.downloader                     # Download all datasets
    python -m src.downloader --dataset 1         # Download dataset 1 only
    python -m src.downloader --dataset 1 3 5     # Download specific datasets
    python -m src.downloader --workers 10        # Use 10 concurrent threads
    python -m src.downloader --dry-run           # Count files without downloading
"""

import argparse
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PDF_DIR, SOURCE_URL, NUM_DATASETS,
    DOWNLOAD_WORKERS, PAGE_FETCH_DELAY,
)


# ─── HTML Parsing ────────────────────────────────────────────

def get_last_page(html):
    """Parse the 'Last page' link to find total pages (0-indexed).

    Returns 0 if there's no pagination (single page).
    """
    soup = BeautifulSoup(html, "html.parser")

    # Look for "Last" pagination link: <a href="?page=N" ...>Last</a>
    last_link = soup.find("a", string=re.compile(r"Last", re.IGNORECASE))
    if last_link and last_link.get("href"):
        match = re.search(r"[?&]page=(\d+)", last_link["href"])
        if match:
            return int(match.group(1))

    # Fallback: find the highest page number in pagination links
    max_page = 0
    for link in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
        match = re.search(r"[?&]page=(\d+)", link["href"])
        if match:
            max_page = max(max_page, int(match.group(1)))

    return max_page


def extract_pdf_links(html, base_url):
    """Extract all PDF download URLs from page HTML."""
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if ".pdf" in href.lower():
            # Resolve relative URLs
            full_url = urljoin(base_url, href)
            links.add(full_url)

    return sorted(links)


# ─── Download Functions ──────────────────────────────────────

def is_valid_pdf(filepath):
    """Check if a file exists and starts with the PDF magic bytes."""
    try:
        if filepath.exists() and filepath.stat().st_size > 0:
            with open(filepath, "rb") as f:
                return f.read(5).startswith(b"%PDF-")
    except Exception:
        pass
    return False


def download_pdf(url, output_path, session):
    """Download a single PDF file. Returns (url, success, message)."""
    filename = output_path.name

    if is_valid_pdf(output_path):
        return url, True, f"  Skipped (exists): {filename}"

    try:
        response = session.get(url, timeout=60)
        if response.status_code != 200:
            return url, False, f"  HTTP {response.status_code}: {filename}"

        # Verify content is actually a PDF
        if not response.content[:5].startswith(b"%PDF-"):
            return url, False, f"  Not a PDF: {filename}"

        with open(output_path, "wb") as f:
            f.write(response.content)

        size = output_path.stat().st_size
        return url, True, f"  Downloaded: {filename} ({size:,} bytes)"

    except requests.exceptions.Timeout:
        return url, False, f"  Timeout: {filename}"
    except Exception as e:
        return url, False, f"  Error: {filename} — {e}"


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


def fetch_page(session, url):
    """Fetch a page with retry logic.

    Uses a fresh request (not session) for listing pages to avoid
    Akamai CDN blocking sequential same-connection requests.
    """
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code in (403, 429):
                # Exponential backoff: 10s, 20s, 40s, 80s, 160s
                wait = 10 * (2 ** attempt)
                print(f"  HTTP {resp.status_code}, backing off {wait}s "
                      f"(attempt {attempt + 1}/5)...")
                time.sleep(wait)
                continue
            print(f"  HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            if attempt < 4:
                time.sleep(5)
            else:
                print(f"  Failed to fetch {url}: {e}")
                return None
    return None


# ─── Dataset Download ────────────────────────────────────────

def download_dataset(dataset_num, workers, dry_run=False):
    """Download all PDFs for one dataset across all paginated pages."""
    print(f"\n{'=' * 70}")
    print(f"  Data Set {dataset_num}")
    print(f"{'=' * 70}")

    dataset_dir = PDF_DIR / f"data-set-{dataset_num}"
    if not dry_run:
        dataset_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(REQUEST_HEADERS)

    base_url = f"{SOURCE_URL}/data-set-{dataset_num}-files"

    # Fetch first page to discover pagination
    print(f"  Fetching: {base_url}")
    html = fetch_page(session, base_url)
    if html is None:
        print("  Failed to load dataset page")
        return 0

    last_page = get_last_page(html)
    total_pages = last_page + 1
    print(f"  Pages: {total_pages} (page 0 to {last_page})")

    # Collect all PDF links across all pages
    all_pdf_links = []
    pdf_links_page0 = extract_pdf_links(html, base_url)
    all_pdf_links.extend(pdf_links_page0)
    print(f"  Page 0: {len(pdf_links_page0)} PDF links")

    for page_num in range(1, total_pages):
        time.sleep(PAGE_FETCH_DELAY)
        page_url = f"{base_url}?page={page_num}"
        page_html = fetch_page(session, page_url)
        if page_html is None:
            print(f"  Page {page_num}: FAILED")
            continue
        links = extract_pdf_links(page_html, page_url)
        all_pdf_links.extend(links)

        # Progress every 10 pages or on last page
        if page_num % 10 == 0 or page_num == last_page:
            print(f"  Page {page_num}/{last_page}: {len(links)} links "
                  f"(total so far: {len(all_pdf_links)})")

    # Deduplicate
    all_pdf_links = sorted(set(all_pdf_links))
    print(f"\n  Total unique PDF links: {len(all_pdf_links)}")

    if dry_run:
        # Count how many already exist
        existing = sum(
            1 for url in all_pdf_links
            if is_valid_pdf(dataset_dir / os.path.basename(url.split("?")[0]))
        )
        print(f"  Already downloaded: {existing}")
        print(f"  Remaining: {len(all_pdf_links) - existing}")
        return len(all_pdf_links)

    # Download PDFs with thread pool
    downloaded = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for url in all_pdf_links:
            filename = os.path.basename(url.split("?")[0])
            output_path = dataset_dir / filename
            future = pool.submit(download_pdf, url, output_path, session)
            futures[future] = url

        for future in as_completed(futures):
            url, success, message = future.result()
            if success:
                if "Skipped" in message:
                    skipped += 1
                else:
                    downloaded += 1
                    print(message)
            else:
                failed += 1
                print(message)

    print(f"\n  Data Set {dataset_num} complete:")
    print(f"    Downloaded: {downloaded}")
    print(f"    Skipped:    {skipped}")
    print(f"    Failed:     {failed}")
    print(f"    Total:      {downloaded + skipped + failed}")

    return downloaded + skipped


# ─── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files Downloader",
        epilog="Examples:\n"
               "  python -m src.downloader                   # All datasets\n"
               "  python -m src.downloader --dataset 1       # Dataset 1 only\n"
               "  python -m src.downloader --dataset 1 3 5   # Specific datasets\n"
               "  python -m src.downloader --workers 10      # 10 threads\n"
               "  python -m src.downloader --dry-run         # Count only\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset", nargs="+", type=int,
        help="Dataset number(s) to download (default: all 1-12)",
    )
    parser.add_argument(
        "--workers", type=int, default=DOWNLOAD_WORKERS,
        help=f"Concurrent download threads (default: {DOWNLOAD_WORKERS})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count files and pages without downloading",
    )
    args = parser.parse_args()

    datasets = args.dataset or list(range(1, NUM_DATASETS + 1))

    # Validate dataset numbers
    for d in datasets:
        if d < 1 or d > NUM_DATASETS:
            print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
            sys.exit(1)

    print("=" * 70)
    print("Epstein DOJ Files Downloader")
    print("=" * 70)
    print(f"  Datasets:  {', '.join(str(d) for d in datasets)}")
    print(f"  Workers:   {args.workers}")
    print(f"  Output:    {PDF_DIR.resolve()}")
    if args.dry_run:
        print("  Mode:      DRY RUN (no downloads)")
    print()

    if not args.dry_run:
        PDF_DIR.mkdir(exist_ok=True)

    grand_total = 0
    for dataset_num in datasets:
        count = download_dataset(dataset_num, args.workers, args.dry_run)
        grand_total += count
        time.sleep(1)

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"  Dry run complete. Total PDF links found: {grand_total}")
    else:
        print(f"  Complete! Total PDFs processed: {grand_total}")
        print(f"  Files saved in: {PDF_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
