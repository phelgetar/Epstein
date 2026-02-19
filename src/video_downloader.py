#!/usr/bin/env python3
"""
Epstein DOJ Files — Video Downloader (Search-based).

Downloads files from the DOJ search page at justice.gov/epstein/search.
Searches for a query (default: "No Images Produced"), paginates through
results, downloads each .pdf link, and saves with .mp4 extension.

Uses Playwright in headed mode with stealth patches to bypass Akamai CDN
bot detection, then downloads files via requests with multithreading.

Usage:
    python -m src.video_downloader                                # Download all
    python -m src.video_downloader --query "No Images Produced"   # Custom query
    python -m src.video_downloader --workers 10                   # Concurrent threads
    python -m src.video_downloader --batch-size 20                # Pages per batch
    python -m src.video_downloader --dry-run                      # Count only
    python -m src.video_downloader --headless                     # Headless mode
"""

import argparse
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from playwright_stealth import Stealth

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    SEARCH_URL, VIDEO_DIR,
    VIDEO_DOWNLOAD_WORKERS, VIDEO_BATCH_SIZE, PAGE_FETCH_DELAY,
)
from src.downloader import handle_barriers

logger = logging.getLogger(__name__)

DEFAULT_QUERY = "No Images Produced"


# ─── Browser Page Fetching ───────────────────────────────────

def submit_search(page, query):
    """Fill the search box and submit the query.

    Tries multiple strategies to locate the search input since the
    exact Drupal form structure may vary.
    """
    # Strategy 1: input with name containing "search" or "keys"
    for selector in [
        "input[name*='search']",
        "input[name*='keys']",
        "input[type='search']",
        "input[id*='search']",
        "input[id*='edit-keys']",
    ]:
        try:
            el = page.locator(selector).first
            if el.is_visible(timeout=2000):
                el.fill(query)
                el.press("Enter")
                page.wait_for_load_state("networkidle", timeout=30000)
                time.sleep(2)
                logger.info("search_submitted", extra={"data": {
                    "query": query, "selector": selector,
                }})
                return True
        except Exception:
            continue

    # Strategy 2: find any text input near a submit button
    try:
        text_inputs = page.locator("input[type='text']")
        for i in range(text_inputs.count()):
            inp = text_inputs.nth(i)
            if inp.is_visible(timeout=1000):
                inp.fill(query)
                inp.press("Enter")
                page.wait_for_load_state("networkidle", timeout=30000)
                time.sleep(2)
                logger.info("search_submitted", extra={"data": {
                    "query": query, "selector": "input[type='text']",
                }})
                return True
    except Exception:
        pass

    logger.error("search_submit_failed", extra={"data": {
        "query": query,
    }})
    print("  Error: Could not find search input on page")
    return False


def get_last_page_from_browser(page):
    """Extract the last page number from pagination in the browser DOM."""
    # Look for "Last" link first
    last_link = page.locator("a:has-text('Last')")
    try:
        if last_link.count() > 0:
            href = last_link.first.get_attribute("href")
            if href:
                match = re.search(r"[?&]page=(\d+)", href)
                if match:
                    return int(match.group(1))
    except Exception:
        pass

    # Fallback: find highest page number in pagination links
    max_page = 0
    pagination_links = page.locator("nav[aria-label='Pagination'] a[href*='page=']")
    try:
        count = pagination_links.count()
        for i in range(count):
            href = pagination_links.nth(i).get_attribute("href")
            if href:
                match = re.search(r"[?&]page=(\d+)", href)
                if match:
                    max_page = max(max_page, int(match.group(1)))
    except Exception:
        pass

    return max_page


def extract_file_links(page, base_url):
    """Extract all PDF/file download URLs from the current page."""
    links = set()
    all_anchors = page.locator("a[href*='.pdf']")
    try:
        count = all_anchors.count()
        for i in range(count):
            href = all_anchors.nth(i).get_attribute("href")
            if href:
                full_url = urljoin(base_url, href)
                links.add(full_url)
    except Exception:
        pass
    return links


def fetch_page_links(page, current_url, page_num, max_retries=3):
    """Navigate to a paginated search result page and extract file links.

    The search URL already has query parameters, so we append &page=N.
    """
    if page_num > 0:
        # Preserve existing query params, add page
        separator = "&" if "?" in current_url else "?"
        page_url = f"{current_url}{separator}page={page_num}"
    else:
        page_url = current_url

    for attempt in range(1, max_retries + 1):
        try:
            page.goto(page_url, wait_until="networkidle", timeout=30000)
            time.sleep(1)
            break
        except PwTimeout:
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)
                break
            except Exception as e:
                if attempt < max_retries:
                    wait = attempt * 5
                    logger.warning("page_fetch_retry", extra={"data": {
                        "page_num": page_num, "attempt": attempt,
                        "max_retries": max_retries, "error_type": type(e).__name__,
                    }})
                    print(f"    Page {page_num}: retry {attempt}/{max_retries} "
                          f"in {wait}s ({type(e).__name__})")
                    time.sleep(wait)
                else:
                    logger.error("page_fetch_failed", extra={"data": {
                        "page_num": page_num, "max_retries": max_retries,
                    }})
                    print(f"    Page {page_num}: FAILED after {max_retries} attempts")
                    return set()
        except Exception as e:
            if attempt < max_retries:
                wait = attempt * 5
                logger.warning("page_fetch_retry", extra={"data": {
                    "page_num": page_num, "attempt": attempt,
                    "max_retries": max_retries, "error_type": type(e).__name__,
                }})
                print(f"    Page {page_num}: retry {attempt}/{max_retries} "
                      f"in {wait}s ({type(e).__name__})")
                time.sleep(wait)
            else:
                logger.error("page_fetch_failed", extra={"data": {
                    "page_num": page_num, "max_retries": max_retries,
                }})
                print(f"    Page {page_num}: FAILED after {max_retries} attempts")
                return set()

    return extract_file_links(page, page_url)


# ─── File Download ────────────────────────────────────────────

def download_file(url, output_path, session):
    """Download a single file. Returns (url, success, message)."""
    filename = output_path.name

    if output_path.exists() and output_path.stat().st_size > 0:
        return url, True, "skip"

    try:
        response = session.get(url, timeout=120)
        if response.status_code != 200:
            logger.error("download_http_error", extra={"data": {
                "url": url, "filename": filename, "status_code": response.status_code,
            }})
            return url, False, f"  HTTP {response.status_code}: {filename}"

        if len(response.content) == 0:
            logger.warning("download_empty", extra={"data": {
                "url": url, "filename": filename,
            }})
            return url, False, f"  Empty file: {filename}"

        with open(output_path, "wb") as f:
            f.write(response.content)

        size = output_path.stat().st_size
        logger.info("download_success", extra={"data": {
            "url": url, "filename": filename, "size_bytes": size,
        }})
        return url, True, f"  Downloaded: {filename} ({size:,} bytes)"

    except requests.exceptions.Timeout:
        logger.error("download_timeout", extra={"data": {
            "url": url, "filename": filename,
        }})
        return url, False, f"  Timeout: {filename}"
    except Exception as e:
        logger.error("download_error", extra={"data": {
            "url": url, "filename": filename,
        }}, exc_info=True)
        return url, False, f"  Error: {filename} — {e}"


def download_batch(file_links, output_dir, session, workers):
    """Download a batch of files using a thread pool.

    Returns (downloaded, skipped, failed) counts.
    """
    downloaded = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for url in file_links:
            # Get original filename, replace .pdf with .mp4
            original_name = os.path.basename(url.split("?")[0])
            mp4_name = re.sub(r'\.pdf$', '.mp4', original_name, flags=re.IGNORECASE)
            output_path = output_dir / mp4_name
            future = pool.submit(download_file, url, output_path, session)
            futures[future] = url

        for future in as_completed(futures):
            url, success, message = future.result()
            if success:
                if message == "skip":
                    skipped += 1
                else:
                    downloaded += 1
                    print(message)
            else:
                failed += 1
                print(message)

    return downloaded, skipped, failed


# ─── Main Download Flow ──────────────────────────────────────

def download_search_results(query, workers, batch_size, dry_run, browser_context):
    """Download all files from a DOJ search query."""

    page = browser_context.new_page()
    Stealth().apply_stealth_sync(page)

    try:
        # Navigate to search page
        print(f"  Navigating to: {SEARCH_URL}")
        page.goto(SEARCH_URL, wait_until="networkidle", timeout=30000)
        time.sleep(2)
        handle_barriers(page)

        # Submit search
        print(f"  Searching: \"{query}\"")
        if not submit_search(page, query):
            return 0, 0, 0

        # Capture the URL after search (contains query params)
        search_result_url = page.url
        logger.info("search_results_url", extra={"data": {
            "url": search_result_url, "query": query,
        }})

        # Discover pagination
        last_page = get_last_page_from_browser(page)
        total_pages = last_page + 1
        logger.info("pagination_discovered", extra={"data": {
            "total_pages": total_pages, "last_page": last_page,
            "query": query,
        }})
        print(f"  Pages: {total_pages} (page 0 to {last_page})")

        # Extract links from the first page (already loaded)
        first_page_links = extract_file_links(page, search_result_url)
        total_results_est = total_pages * 10  # ~10 per page
        print(f"  Estimated results: ~{total_results_est:,}")

        # Set up requests session
        session = None
        if not dry_run:
            VIDEO_DIR.mkdir(parents=True, exist_ok=True)
            session = requests.Session()
            cookies = browser_context.cookies()
            for cookie in cookies:
                session.cookies.set(cookie["name"], cookie["value"],
                                    domain=cookie.get("domain", ""))
            session.headers.update({
                "User-Agent": page.evaluate("() => navigator.userAgent"),
            })

        # Process pages in batches
        total_downloaded = 0
        total_skipped = 0
        total_failed = 0
        total_links = 0

        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            batch_label = f"pages {batch_start}-{batch_end - 1}"

            # Scan this batch of pages for file links
            batch_links = set()
            for page_num in range(batch_start, batch_end):
                if page_num == 0:
                    # Already have first page links
                    batch_links.update(first_page_links)
                else:
                    time.sleep(PAGE_FETCH_DELAY)
                    links = fetch_page_links(page, search_result_url, page_num)
                    batch_links.update(links)

                if page_num == batch_end - 1:
                    print(f"    Scanned {batch_label}: "
                          f"{len(batch_links)} links found")

            batch_links = sorted(batch_links)
            total_links += len(batch_links)

            if dry_run:
                existing = sum(
                    1 for url in batch_links
                    if (VIDEO_DIR / re.sub(
                        r'\.pdf$', '.mp4',
                        os.path.basename(url.split("?")[0]),
                        flags=re.IGNORECASE,
                    )).exists()
                )
                print(f"    Batch links: {len(batch_links)} "
                      f"(already downloaded: {existing})")
            else:
                dl, sk, fl = download_batch(
                    batch_links, VIDEO_DIR, session, workers,
                )
                total_downloaded += dl
                total_skipped += sk
                total_failed += fl
                logger.info("batch_complete", extra={"data": {
                    "batch_label": batch_label,
                    "downloaded": dl, "skipped": sk, "failed": fl,
                }})

            del batch_links

        return total_downloaded, total_skipped, total_failed

    finally:
        page.close()


# ─── Main ────────────────────────────────────────────────────

def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files — Video Downloader (Search-based)",
        epilog="Examples:\n"
               "  python -m src.video_downloader                              # Download all\n"
               '  python -m src.video_downloader --query "No Images Produced" # Custom query\n'
               "  python -m src.video_downloader --workers 10                 # 10 threads\n"
               "  python -m src.video_downloader --batch-size 20              # 20 pages/batch\n"
               "  python -m src.video_downloader --dry-run                    # Count only\n"
               "  python -m src.video_downloader --headless                   # Headless mode\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--query", type=str, default=DEFAULT_QUERY,
        help=f"Search query (default: \"{DEFAULT_QUERY}\")",
    )
    parser.add_argument(
        "--workers", type=int, default=VIDEO_DOWNLOAD_WORKERS,
        help=f"Concurrent download threads (default: {VIDEO_DOWNLOAD_WORKERS})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=VIDEO_BATCH_SIZE,
        help=f"Pages to scan per download batch (default: {VIDEO_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count files and pages without downloading",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser in headless mode (may be blocked by Akamai)",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("Epstein DOJ Files — Video Downloader")
    print("=" * 70)
    print(f"  Query:      \"{args.query}\"")
    print(f"  Workers:    {args.workers}")
    print(f"  Batch size: {args.batch_size} pages")
    print(f"  Output:     {VIDEO_DIR.resolve()}")
    print(f"  Browser:    {'headless' if args.headless else 'headed'}")
    if args.dry_run:
        print("  Mode:       DRY RUN (no downloads)")
    print()

    logger.info("video_downloader_started", extra={"data": {
        "query": args.query, "workers": args.workers,
        "batch_size": args.batch_size, "dry_run": args.dry_run,
        "headless": args.headless,
        "output_dir": str(VIDEO_DIR.resolve()),
    }})

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )

        try:
            downloaded, skipped, failed = download_search_results(
                args.query, args.workers, args.batch_size,
                args.dry_run, context,
            )
        finally:
            context.close()
            browser.close()

    logger.info("video_downloader_complete", extra={"data": {
        "downloaded": downloaded, "skipped": skipped,
        "failed": failed, "dry_run": args.dry_run,
        "query": args.query,
    }})

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"  Dry run complete.")
    else:
        print(f"  Complete!")
        print(f"    Downloaded: {downloaded}")
        print(f"    Skipped:    {skipped}")
        print(f"    Failed:     {failed}")
        print(f"\n  Files saved in: {VIDEO_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
